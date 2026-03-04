/**
 * CcxtAccount — ITradingAccount adapter for CCXT exchanges
 *
 * Direct implementation against ccxt unified API. No SymbolMapper —
 * contract resolution searches exchange.markets on demand.
 * aliceId format: "{exchange}-{market.id}" (e.g. "bybit-BTCUSDT").
 */

import ccxt from 'ccxt'
import type { Exchange, Order as CcxtOrder } from 'ccxt'
import type { Contract } from '../../contract.js'
import type {
  ITradingAccount,
  AccountCapabilities,
  AccountInfo,
  Position,
  Order,
  OrderRequest,
  OrderResult,
  Quote,
  FundingRate,
  OrderBook,
  OrderBookLevel,
} from '../../interfaces.js'

export interface CcxtAccountConfig {
  id?: string
  label?: string
  exchange: string
  apiKey: string
  apiSecret: string
  password?: string
  sandbox: boolean
  demoTrading?: boolean
  defaultMarketType: 'spot' | 'swap'
  options?: Record<string, unknown>
}

// ==================== CCXT market shape ====================

interface CcxtMarket {
  id: string        // exchange-native symbol, e.g. "BTCUSDT"
  symbol: string    // CCXT unified format, e.g. "BTC/USDT:USDT"
  base: string      // e.g. "BTC"
  quote: string     // e.g. "USDT"
  type: string      // "spot" | "swap" | "future" | "option"
  settle?: string   // e.g. "USDT" (for derivatives)
  active?: boolean
  precision?: { price?: number; amount?: number }
}

const MAX_INIT_RETRIES = 5
const INIT_RETRY_BASE_MS = 2000

// ==================== CcxtAccount ====================

export class CcxtAccount implements ITradingAccount {
  readonly id: string
  readonly provider: string  // "ccxt" or the specific exchange name
  readonly label: string

  private exchange: Exchange
  private exchangeName: string
  private defaultMarketType: 'spot' | 'swap'
  private initialized = false

  // orderId → ccxtSymbol cache (CCXT needs symbol to cancel)
  private orderSymbolCache = new Map<string, string>()

  constructor(config: CcxtAccountConfig) {
    this.exchangeName = config.exchange
    this.provider = config.exchange  // use exchange name as provider (e.g. "bybit", "binance")
    this.id = config.id ?? `${config.exchange}-main`
    this.label = config.label ?? `${config.exchange.charAt(0).toUpperCase() + config.exchange.slice(1)} ${config.sandbox ? 'Testnet' : 'Live'}`
    this.defaultMarketType = config.defaultMarketType

    const exchanges = ccxt as unknown as Record<string, new (opts: Record<string, unknown>) => Exchange>
    const ExchangeClass = exchanges[config.exchange]
    if (!ExchangeClass) {
      throw new Error(`Unknown CCXT exchange: ${config.exchange}`)
    }

    this.exchange = new ExchangeClass({
      apiKey: config.apiKey,
      secret: config.apiSecret,
      password: config.password,
      ...(config.options ? { options: config.options } : {}),
    })

    if (config.sandbox) {
      this.exchange.setSandboxMode(true)
    }

    if (config.demoTrading) {
      (this.exchange as unknown as { enableDemoTrading: (enable: boolean) => void }).enableDemoTrading(true)
    }
  }

  // ---- Lifecycle ----

  async init(): Promise<void> {
    let lastError: Error | null = null

    for (let attempt = 1; attempt <= MAX_INIT_RETRIES; attempt++) {
      try {
        await this.exchange.loadMarkets()
        this.initialized = true

        const marketCount = Object.keys(this.exchange.markets).length
        console.log(
          `CcxtAccount[${this.id}]: connected (${this.exchangeName}, ${marketCount} markets loaded)`,
        )
        return
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err))
        if (attempt < MAX_INIT_RETRIES) {
          const delay = INIT_RETRY_BASE_MS * Math.pow(2, attempt - 1)
          console.warn(
            `CcxtAccount[${this.id}]: loadMarkets attempt ${attempt}/${MAX_INIT_RETRIES} failed, retrying in ${delay}ms...`,
          )
          await new Promise(r => setTimeout(r, delay))
        }
      }
    }

    throw new Error(`CcxtAccount[${this.id}]: failed to initialize after ${MAX_INIT_RETRIES} attempts: ${lastError?.message}`)
  }

  async close(): Promise<void> {
    // CCXT exchanges typically don't need explicit closing
  }

  // ---- Contract resolution ----

  async resolveContract(query: Partial<Contract>): Promise<Contract[]> {
    this.ensureInit()

    // Direct aliceId lookup
    if (query.aliceId) {
      const ccxtSymbol = this.aliceIdToCcxt(query.aliceId)
      if (!ccxtSymbol) return []
      const market = this.exchange.markets[ccxtSymbol]
      if (!market) return []
      return [this.marketToContract(market as unknown as CcxtMarket)]
    }

    if (!query.symbol) return []

    const searchBase = query.symbol.toUpperCase()
    const results: Contract[] = []

    for (const market of Object.values(this.exchange.markets) as unknown as CcxtMarket[]) {
      if (market.active === false) continue

      // Match by base asset
      if (market.base.toUpperCase() !== searchBase) continue

      // Filter by secType if specified
      if (query.secType) {
        const marketSecType = this.ccxtTypeToSecType(market.type)
        if (marketSecType !== query.secType) continue
      }

      // Filter by currency if specified
      if (query.currency && market.quote.toUpperCase() !== query.currency.toUpperCase()) continue

      // Default filter: only USDT/USD quoted markets (skip exotic pairs)
      if (!query.currency) {
        const quote = market.quote.toUpperCase()
        if (quote !== 'USDT' && quote !== 'USD' && quote !== 'USDC') continue
      }

      results.push(this.marketToContract(market))
    }

    // Sort: preferred market type first, then USDT > USD > USDC
    const typeOrder = this.defaultMarketType === 'swap'
      ? { swap: 0, future: 1, spot: 2, option: 3 }
      : { spot: 0, swap: 1, future: 2, option: 3 }
    const quoteOrder: Record<string, number> = { USDT: 0, USD: 1, USDC: 2 }

    results.sort((a, b) => {
      const aType = typeOrder[this.secTypeToCcxtType(a.secType) as keyof typeof typeOrder] ?? 99
      const bType = typeOrder[this.secTypeToCcxtType(b.secType) as keyof typeof typeOrder] ?? 99
      if (aType !== bType) return aType - bType
      const aQuote = quoteOrder[a.currency?.toUpperCase() ?? ''] ?? 99
      const bQuote = quoteOrder[b.currency?.toUpperCase() ?? ''] ?? 99
      return aQuote - bQuote
    })

    return results
  }

  // ---- Trading operations ----

  async placeOrder(order: OrderRequest): Promise<OrderResult> {
    this.ensureInit()

    const ccxtSymbol = this.contractToCcxt(order.contract)
    if (!ccxtSymbol) {
      return { success: false, error: 'Cannot resolve contract to CCXT symbol' }
    }

    let size = order.qty

    // Notional → size conversion
    if (!size && order.notional) {
      const ticker = await this.exchange.fetchTicker(ccxtSymbol)
      const price = order.price ?? ticker.last
      if (!price) {
        return { success: false, error: 'Cannot determine price for notional conversion' }
      }
      size = order.notional / price
    }

    if (!size) {
      return { success: false, error: 'Either qty or notional must be provided' }
    }

    try {
      // Set leverage before order if requested
      if (order.leverage && order.leverage > 1) {
        try {
          await this.exchange.setLeverage(order.leverage, ccxtSymbol)
        } catch {
          // Some exchanges don't support setLeverage; ignore
        }
      }

      const params: Record<string, unknown> = {}
      if (order.reduceOnly) params.reduceOnly = true

      const ccxtOrder = await this.exchange.createOrder(
        ccxtSymbol,
        order.type,
        order.side,
        size,
        order.type === 'limit' ? order.price : undefined,
        params,
      )

      // Cache orderId → symbol
      if (ccxtOrder.id) {
        this.orderSymbolCache.set(ccxtOrder.id, ccxtSymbol)
      }

      const status = this.mapOrderStatus(ccxtOrder.status)

      return {
        success: true,
        orderId: ccxtOrder.id,
        message: `Order ${ccxtOrder.id} ${status}`,
        filledPrice: status === 'filled' ? (ccxtOrder.average ?? ccxtOrder.price ?? undefined) : undefined,
        filledQty: status === 'filled' ? (ccxtOrder.filled ?? undefined) : undefined,
      }
    } catch (err) {
      return { success: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  async cancelOrder(orderId: string): Promise<boolean> {
    this.ensureInit()

    try {
      const ccxtSymbol = this.orderSymbolCache.get(orderId)
      await this.exchange.cancelOrder(orderId, ccxtSymbol)
      return true
    } catch {
      return false
    }
  }

  async closePosition(contract: Contract, qty?: number): Promise<OrderResult> {
    this.ensureInit()

    const positions = await this.getPositions()
    const symbol = contract.symbol?.toUpperCase()
    const aliceId = contract.aliceId

    const pos = positions.find(p =>
      (aliceId && p.contract.aliceId === aliceId) ||
      (symbol && p.contract.symbol === symbol),
    )

    if (!pos) {
      return { success: false, error: `No open position for ${aliceId ?? symbol ?? 'unknown'}` }
    }

    return this.placeOrder({
      contract: pos.contract,
      side: pos.side === 'long' ? 'sell' : 'buy',
      type: 'market',
      qty: qty ?? pos.qty,
      reduceOnly: true,
    })
  }

  // ---- Queries ----

  async getAccount(): Promise<AccountInfo> {
    this.ensureInit()

    const [balance, rawPositions] = await Promise.all([
      this.exchange.fetchBalance(),
      this.exchange.fetchPositions(),
    ])

    const bal = balance as unknown as Record<string, Record<string, unknown>>
    const total = parseFloat(String(bal['total']?.['USDT'] ?? bal['total']?.['USD'] ?? 0))
    const free = parseFloat(String(bal['free']?.['USDT'] ?? bal['free']?.['USD'] ?? 0))
    const used = parseFloat(String(bal['used']?.['USDT'] ?? bal['used']?.['USD'] ?? 0))

    let unrealizedPnL = 0
    let realizedPnL = 0
    for (const p of rawPositions) {
      unrealizedPnL += parseFloat(String(p.unrealizedPnl ?? 0))
      realizedPnL += parseFloat(String((p as unknown as Record<string, unknown>).realizedPnl ?? 0))
    }

    return {
      cash: free,
      equity: total,
      unrealizedPnL,
      realizedPnL,
      totalMargin: used,
    }
  }

  async getPositions(): Promise<Position[]> {
    this.ensureInit()

    const raw = await this.exchange.fetchPositions()
    const result: Position[] = []

    for (const p of raw) {
      const market = this.exchange.markets[p.symbol]
      if (!market) continue

      const size = Math.abs(parseFloat(String(p.contracts ?? 0)) * parseFloat(String(p.contractSize ?? 1)))
      if (size === 0) continue

      const markPrice = parseFloat(String(p.markPrice ?? 0))
      const entryPrice = parseFloat(String(p.entryPrice ?? 0))
      const marketValue = size * markPrice
      const costBasis = size * entryPrice
      const unrealizedPnL = parseFloat(String(p.unrealizedPnl ?? 0))

      result.push({
        contract: this.marketToContract(market as unknown as CcxtMarket),
        side: p.side === 'long' ? 'long' : 'short',
        qty: size,
        avgEntryPrice: entryPrice,
        currentPrice: markPrice,
        marketValue,
        unrealizedPnL,
        unrealizedPnLPercent: costBasis > 0 ? (unrealizedPnL / costBasis) * 100 : 0,
        costBasis,
        leverage: parseFloat(String(p.leverage ?? 1)),
        margin: parseFloat(String(p.initialMargin ?? p.collateral ?? 0)),
        liquidationPrice: parseFloat(String(p.liquidationPrice ?? 0)) || undefined,
      })
    }

    return result
  }

  async getOrders(): Promise<Order[]> {
    this.ensureInit()

    const allOrders: CcxtOrder[] = []

    try {
      const open = await this.exchange.fetchOpenOrders()
      allOrders.push(...open)
    } catch {
      // Some exchanges don't support fetchOpenOrders
    }

    try {
      const closed = await this.exchange.fetchClosedOrders(undefined, undefined, 50)
      allOrders.push(...closed)
    } catch {
      // Some exchanges don't support fetchClosedOrders
    }

    const result: Order[] = []

    for (const o of allOrders) {
      const market = this.exchange.markets[o.symbol]
      if (!market) continue

      if (o.id) {
        this.orderSymbolCache.set(o.id, o.symbol)
      }

      result.push({
        id: o.id,
        contract: this.marketToContract(market as unknown as CcxtMarket),
        side: o.side as 'buy' | 'sell',
        type: (o.type ?? 'market') as Order['type'],
        qty: o.amount ?? 0,
        price: o.price ?? undefined,
        leverage: undefined,
        reduceOnly: o.reduceOnly ?? false,
        status: this.mapOrderStatus(o.status),
        filledPrice: o.average ?? undefined,
        filledQty: o.filled ?? undefined,
        filledAt: o.lastTradeTimestamp ? new Date(o.lastTradeTimestamp) : undefined,
        createdAt: new Date(o.timestamp ?? Date.now()),
      })
    }

    return result
  }

  async getQuote(contract: Contract): Promise<Quote> {
    this.ensureInit()

    const ccxtSymbol = this.contractToCcxt(contract)
    if (!ccxtSymbol) throw new Error('Cannot resolve contract to CCXT symbol')

    const ticker = await this.exchange.fetchTicker(ccxtSymbol)
    const market = this.exchange.markets[ccxtSymbol]

    return {
      contract: market
        ? this.marketToContract(market as unknown as CcxtMarket)
        : contract,
      last: ticker.last ?? 0,
      bid: ticker.bid ?? 0,
      ask: ticker.ask ?? 0,
      volume: ticker.baseVolume ?? 0,
      high: ticker.high ?? undefined,
      low: ticker.low ?? undefined,
      timestamp: new Date(ticker.timestamp ?? Date.now()),
    }
  }

  // ---- Capabilities ----

  getCapabilities(): AccountCapabilities {
    return {
      supportsLeverage: true,
      supportsShort: true,
      supportsNotional: false,
      supportsFundingRate: true,
      supportsOrderBook: true,
      supportsMarketClock: false,
      supportsExtendedHours: false,
      supportedSecTypes: ['CRYPTO'],
      supportedOrderTypes: ['market', 'limit'],
    }
  }

  // ---- Optional extensions ----

  async getFundingRate(contract: Contract): Promise<FundingRate> {
    this.ensureInit()

    const ccxtSymbol = this.contractToCcxt(contract)
    if (!ccxtSymbol) throw new Error('Cannot resolve contract to CCXT symbol')

    const funding = await this.exchange.fetchFundingRate(ccxtSymbol)
    const market = this.exchange.markets[ccxtSymbol]

    return {
      contract: market
        ? this.marketToContract(market as unknown as CcxtMarket)
        : contract,
      fundingRate: funding.fundingRate ?? 0,
      nextFundingTime: funding.fundingDatetime ? new Date(funding.fundingDatetime) : undefined,
      previousFundingRate: funding.previousFundingRate ?? undefined,
      timestamp: new Date(funding.timestamp ?? Date.now()),
    }
  }

  async getOrderBook(contract: Contract, limit?: number): Promise<OrderBook> {
    this.ensureInit()

    const ccxtSymbol = this.contractToCcxt(contract)
    if (!ccxtSymbol) throw new Error('Cannot resolve contract to CCXT symbol')

    const book = await this.exchange.fetchOrderBook(ccxtSymbol, limit)
    const market = this.exchange.markets[ccxtSymbol]

    return {
      contract: market
        ? this.marketToContract(market as unknown as CcxtMarket)
        : contract,
      bids: book.bids.map(([p, a]) => [p ?? 0, a ?? 0] as OrderBookLevel),
      asks: book.asks.map(([p, a]) => [p ?? 0, a ?? 0] as OrderBookLevel),
      timestamp: new Date(book.timestamp ?? Date.now()),
    }
  }

  async adjustLeverage(contract: Contract, leverage: number): Promise<{ success: boolean; error?: string }> {
    this.ensureInit()

    const ccxtSymbol = this.contractToCcxt(contract)
    if (!ccxtSymbol) return { success: false, error: 'Cannot resolve contract to CCXT symbol' }

    try {
      await this.exchange.setLeverage(leverage, ccxtSymbol)
      return { success: true }
    } catch (err) {
      return { success: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  // ==================== Internal ====================

  private ensureInit(): void {
    if (!this.initialized) {
      throw new Error(`CcxtAccount[${this.id}] not initialized. Call init() first.`)
    }
  }

  /**
   * Convert a CcxtMarket to a Contract.
   * aliceId = "{exchangeName}-{market.id}"
   */
  private marketToContract(market: CcxtMarket): Contract {
    return {
      aliceId: `${this.exchangeName}-${market.id}`,
      symbol: market.base,
      secType: this.ccxtTypeToSecType(market.type),
      exchange: this.exchangeName,
      currency: market.quote,
      localSymbol: market.symbol,       // CCXT unified symbol, e.g. "BTC/USDT:USDT"
      description: `${market.base}/${market.quote} ${market.type}${market.settle ? ` (${market.settle} settled)` : ''}`,
    }
  }

  /**
   * Resolve a Contract to a CCXT symbol for API calls.
   * Tries: aliceId → localSymbol → search by symbol+secType.
   */
  private contractToCcxt(contract: Contract): string | null {
    // 1. aliceId → market.id → look up in markets
    if (contract.aliceId) {
      const ccxtSymbol = this.aliceIdToCcxt(contract.aliceId)
      if (ccxtSymbol && this.exchange.markets[ccxtSymbol]) return ccxtSymbol
      // aliceId uses market.id, but markets are indexed by ccxt symbol
      // search by market.id
      for (const m of Object.values(this.exchange.markets) as unknown as CcxtMarket[]) {
        if (`${this.exchangeName}-${m.id}` === contract.aliceId) return m.symbol
      }
      return null
    }

    // 2. localSymbol is the CCXT unified symbol
    if (contract.localSymbol && this.exchange.markets[contract.localSymbol]) {
      return contract.localSymbol
    }

    // 3. Search by symbol + secType (resolve to unique)
    if (contract.symbol) {
      const candidates = this.resolveContractSync(contract)
      if (candidates.length === 1) return candidates[0]
      if (candidates.length > 1) {
        // Ambiguous — caller should have resolved first
        return null
      }
    }

    return null
  }

  /** Synchronous search returning CCXT symbols. Used by contractToCcxt. */
  private resolveContractSync(query: Partial<Contract>): string[] {
    if (!query.symbol) return []

    const searchBase = query.symbol.toUpperCase()
    const results: string[] = []

    for (const market of Object.values(this.exchange.markets) as unknown as CcxtMarket[]) {
      if (market.active === false) continue
      if (market.base.toUpperCase() !== searchBase) continue

      if (query.secType) {
        const marketSecType = this.ccxtTypeToSecType(market.type)
        if (marketSecType !== query.secType) continue
      }

      if (query.currency && market.quote.toUpperCase() !== query.currency.toUpperCase()) continue

      if (!query.currency) {
        const quote = market.quote.toUpperCase()
        if (quote !== 'USDT' && quote !== 'USD' && quote !== 'USDC') continue
      }

      results.push(market.symbol)
    }

    return results
  }

  /** Parse aliceId → raw nativeId (market.id) part. */
  private aliceIdToCcxt(aliceId: string): string | null {
    const prefix = `${this.exchangeName}-`
    if (!aliceId.startsWith(prefix)) return null
    return aliceId.slice(prefix.length)
  }

  private ccxtTypeToSecType(type: string): Contract['secType'] {
    switch (type) {
      case 'spot': return 'CRYPTO'
      case 'swap': return 'CRYPTO'  // perpetual swap is still crypto
      case 'future': return 'FUT'
      case 'option': return 'OPT'
      default: return 'CRYPTO'
    }
  }

  private secTypeToCcxtType(secType: Contract['secType']): string {
    switch (secType) {
      case 'CRYPTO': return this.defaultMarketType
      case 'FUT': return 'future'
      case 'OPT': return 'option'
      default: return 'spot'
    }
  }

  private mapOrderStatus(status: string | undefined): Order['status'] {
    switch (status) {
      case 'closed': return 'filled'
      case 'open': return 'pending'
      case 'canceled':
      case 'cancelled': return 'cancelled'
      case 'expired':
      case 'rejected': return 'rejected'
      default: return 'pending'
    }
  }
}
