/**
 * AlpacaAccount — ITradingAccount adapter for Alpaca
 *
 * Direct implementation against @alpacahq/alpaca-trade-api SDK.
 * Supports US equities (STK). Contract resolution uses Alpaca's ticker
 * as nativeId — unambiguous for stocks, extensible when options arrive.
 */

import Alpaca from '@alpacahq/alpaca-trade-api'
import type { Contract, ContractDescription, ContractDetails } from '../../contract.js'
import type {
  ITradingAccount,
  AccountCapabilities,
  AccountInfo,
  Position,
  Order,
  OrderRequest,
  OrderResult,
  Quote,
  MarketClock,
} from '../../interfaces.js'

export interface AlpacaAccountConfig {
  id?: string
  label?: string
  apiKey: string
  secretKey: string
  paper: boolean
}

// ==================== Alpaca SDK raw shapes ====================

interface AlpacaAccountRaw {
  cash: string
  portfolio_value: string
  equity: string
  buying_power: string
  daytrade_count: number
  daytrading_buying_power: string
}

interface AlpacaPositionRaw {
  symbol: string
  side: string
  qty: string
  avg_entry_price: string
  current_price: string
  market_value: string
  unrealized_pl: string
  unrealized_plpc: string
  cost_basis: string
}

interface AlpacaOrderRaw {
  id: string
  symbol: string
  side: string
  type: string
  qty: string | null
  notional: string | null
  limit_price: string | null
  stop_price: string | null
  time_in_force: string
  extended_hours: boolean
  status: string
  filled_avg_price: string | null
  filled_qty: string | null
  filled_at: string | null
  created_at: string
  reject_reason: string | null
}

interface AlpacaSnapshotRaw {
  LatestTrade: { Price: number; Timestamp: string }
  LatestQuote: { BidPrice: number; AskPrice: number; Timestamp: string }
  DailyBar: { Volume: number }
}

interface AlpacaFillActivityRaw {
  activity_type: 'FILL'
  symbol: string
  side: string
  qty: string
  price: string
  cum_qty: string
  leaves_qty: string
  transaction_time: string
  order_id: string
  type: string // 'fill' | 'partial_fill'
}

interface AlpacaClockRaw {
  is_open: boolean
  next_open: string
  next_close: string
  timestamp: string
}

// ==================== AlpacaAccount ====================

export class AlpacaAccount implements ITradingAccount {
  readonly id: string
  readonly provider = 'alpaca'
  readonly label: string

  private client!: InstanceType<typeof Alpaca>
  private readonly config: AlpacaAccountConfig

  /** Cached realized PnL from FILL activities (FIFO lot matching) */
  private realizedPnLCache: { value: number; updatedAt: number } | null = null
  private static readonly REALIZED_PNL_TTL_MS = 60_000

  constructor(config: AlpacaAccountConfig) {
    this.config = config
    this.id = config.id ?? (config.paper ? 'alpaca-paper' : 'alpaca-live')
    this.label = config.label ?? (config.paper ? 'Alpaca Paper' : 'Alpaca Live')
  }

  // ---- Lifecycle ----

  async init(): Promise<void> {
    this.client = new Alpaca({
      keyId: this.config.apiKey,
      secretKey: this.config.secretKey,
      paper: this.config.paper,
    })

    const account = await this.client.getAccount() as AlpacaAccountRaw
    console.log(
      `AlpacaAccount[${this.id}]: connected (paper=${this.config.paper}, equity=$${parseFloat(account.equity).toFixed(2)})`,
    )
  }

  async close(): Promise<void> {
    // Alpaca SDK has no explicit close
  }

  // ---- Contract search (IBKR: reqMatchingSymbols + reqContractDetails) ----

  async searchContracts(pattern: string): Promise<ContractDescription[]> {
    if (!pattern) return []

    // Alpaca tickers are unique for stocks — pattern is treated as exact ticker match
    const ticker = pattern.toUpperCase()
    return [{ contract: this.makeContract(ticker) }]
  }

  async getContractDetails(query: Partial<Contract>): Promise<ContractDetails | null> {
    const symbol = this.resolveSymbol(query as Contract)
    if (!symbol) return null

    return {
      contract: this.makeContract(symbol),
      validExchanges: ['SMART', 'NYSE', 'NASDAQ', 'ARCA'],
      orderTypes: ['market', 'limit', 'stop', 'stop_limit', 'trailing_stop'],
      stockType: 'COMMON',
    }
  }

  // ---- Trading operations ----

  async placeOrder(order: OrderRequest): Promise<OrderResult> {
    const symbol = this.resolveSymbol(order.contract)
    if (!symbol) {
      return { success: false, error: 'Cannot resolve contract to Alpaca symbol' }
    }

    try {
      const alpacaOrder: Record<string, unknown> = {
        symbol,
        side: order.side,
        type: order.type === 'trailing_stop' ? 'trailing_stop' : order.type,
        time_in_force: order.timeInForce ?? 'day',
      }

      if (order.qty != null) {
        alpacaOrder.qty = order.qty
      } else if (order.notional != null) {
        alpacaOrder.notional = order.notional
      }

      if (order.price != null) alpacaOrder.limit_price = order.price
      if (order.stopPrice != null) alpacaOrder.stop_price = order.stopPrice
      if (order.trailingAmount != null) alpacaOrder.trail_price = order.trailingAmount
      if (order.trailingPercent != null) alpacaOrder.trail_percent = order.trailingPercent
      if (order.extendedHours != null) alpacaOrder.extended_hours = order.extendedHours

      const result = await this.client.createOrder(alpacaOrder) as AlpacaOrderRaw
      const isFilled = result.status === 'filled'

      return {
        success: true,
        orderId: result.id,
        filledPrice: isFilled && result.filled_avg_price ? parseFloat(result.filled_avg_price) : undefined,
        filledQty: isFilled && result.filled_qty ? parseFloat(result.filled_qty) : undefined,
      }
    } catch (err) {
      return { success: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  async modifyOrder(orderId: string, changes: Partial<OrderRequest>): Promise<OrderResult> {
    try {
      const patch: Record<string, unknown> = {}
      if (changes.qty != null) patch.qty = changes.qty
      if (changes.price != null) patch.limit_price = changes.price
      if (changes.stopPrice != null) patch.stop_price = changes.stopPrice
      if (changes.trailingAmount != null) patch.trail = changes.trailingAmount
      if (changes.trailingPercent != null) patch.trail = changes.trailingPercent
      if (changes.timeInForce) patch.time_in_force = changes.timeInForce

      const result = await this.client.replaceOrder(orderId, patch) as AlpacaOrderRaw
      const isFilled = result.status === 'filled'

      return {
        success: true,
        orderId: result.id,
        filledPrice: isFilled && result.filled_avg_price ? parseFloat(result.filled_avg_price) : undefined,
        filledQty: isFilled && result.filled_qty ? parseFloat(result.filled_qty) : undefined,
      }
    } catch (err) {
      return { success: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  async cancelOrder(orderId: string): Promise<boolean> {
    try {
      await this.client.cancelOrder(orderId)
      return true
    } catch {
      return false
    }
  }

  async closePosition(contract: Contract, qty?: number): Promise<OrderResult> {
    const symbol = this.resolveSymbol(contract)
    if (!symbol) {
      return { success: false, error: 'Cannot resolve contract to Alpaca symbol' }
    }

    // Partial close → reverse market order
    if (qty != null) {
      const positions = await this.getPositions()
      const pos = positions.find(p => p.contract.symbol === symbol)
      if (!pos) return { success: false, error: `No position for ${symbol}` }

      return this.placeOrder({
        contract,
        side: pos.side === 'long' ? 'sell' : 'buy',
        type: 'market',
        qty,
        timeInForce: 'day',
      })
    }

    // Full close → native Alpaca API
    try {
      const result = await this.client.closePosition(symbol) as AlpacaOrderRaw
      const isFilled = result.status === 'filled'
      return {
        success: true,
        orderId: result.id,
        filledPrice: isFilled && result.filled_avg_price ? parseFloat(result.filled_avg_price) : undefined,
        filledQty: isFilled && result.filled_qty ? parseFloat(result.filled_qty) : undefined,
      }
    } catch (err) {
      return { success: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  // ---- Queries ----

  async getAccount(): Promise<AccountInfo> {
    const [account, positions, realizedPnL] = await Promise.all([
      this.client.getAccount() as Promise<AlpacaAccountRaw>,
      this.client.getPositions() as Promise<AlpacaPositionRaw[]>,
      this.getRealizedPnL(),
    ])

    // Alpaca account API doesn't provide unrealizedPnL — aggregate from positions
    const unrealizedPnL = positions.reduce((sum, p) => sum + parseFloat(p.unrealized_pl), 0)

    return {
      cash: parseFloat(account.cash),
      equity: parseFloat(account.equity),
      unrealizedPnL,
      realizedPnL,
      portfolioValue: parseFloat(account.portfolio_value),
      buyingPower: parseFloat(account.buying_power),
      dayTradeCount: account.daytrade_count,
      dayTradingBuyingPower: parseFloat(account.daytrading_buying_power),
    }
  }

  async getPositions(): Promise<Position[]> {
    const raw = await this.client.getPositions() as AlpacaPositionRaw[]

    return raw.map(p => ({
      contract: this.makeContract(p.symbol),
      side: p.side === 'long' ? 'long' as const : 'short' as const,
      qty: parseFloat(p.qty),
      avgEntryPrice: parseFloat(p.avg_entry_price),
      currentPrice: parseFloat(p.current_price),
      marketValue: Math.abs(parseFloat(p.market_value)),
      unrealizedPnL: parseFloat(p.unrealized_pl),
      unrealizedPnLPercent: parseFloat(p.unrealized_plpc) * 100,
      costBasis: parseFloat(p.cost_basis),
      leverage: 1,
    }))
  }

  async getOrders(): Promise<Order[]> {
    const orders = await this.client.getOrders({
      status: 'all',
      limit: 100,
      until: undefined,
      after: undefined,
      direction: undefined,
      nested: undefined,
      symbols: undefined,
    }) as AlpacaOrderRaw[]

    return orders.map(o => this.mapOrder(o))
  }

  async getQuote(contract: Contract): Promise<Quote> {
    const symbol = this.resolveSymbol(contract)
    if (!symbol) throw new Error('Cannot resolve contract to Alpaca symbol')

    const snapshot = await this.client.getSnapshot(symbol) as AlpacaSnapshotRaw

    return {
      contract: this.makeContract(symbol),
      last: snapshot.LatestTrade.Price,
      bid: snapshot.LatestQuote.BidPrice,
      ask: snapshot.LatestQuote.AskPrice,
      volume: snapshot.DailyBar.Volume,
      timestamp: new Date(snapshot.LatestTrade.Timestamp),
    }
  }

  // ---- Capabilities ----

  getCapabilities(): AccountCapabilities {
    return {
      supportedSecTypes: ['STK'],
      supportedOrderTypes: ['market', 'limit', 'stop', 'stop_limit', 'trailing_stop'],
    }
  }

  async getMarketClock(): Promise<MarketClock> {
    const clock = await this.client.getClock() as AlpacaClockRaw
    return {
      isOpen: clock.is_open,
      nextOpen: new Date(clock.next_open),
      nextClose: new Date(clock.next_close),
      timestamp: new Date(clock.timestamp),
    }
  }

  // ==================== Realized PnL (FILL activities, FIFO) ====================

  /**
   * Get realized PnL from Alpaca FILL activities with TTL cache.
   * Fetches all historical fills, matches buys against sells per symbol using FIFO,
   * and sums the realized profit/loss.
   */
  private async getRealizedPnL(): Promise<number> {
    const now = Date.now()
    if (this.realizedPnLCache && (now - this.realizedPnLCache.updatedAt) < AlpacaAccount.REALIZED_PNL_TTL_MS) {
      return this.realizedPnLCache.value
    }

    try {
      const fills = await this.fetchAllFills()
      const value = AlpacaAccount.computeRealizedPnL(fills)
      this.realizedPnLCache = { value, updatedAt: now }
      return value
    } catch (err) {
      // On error, return cached value if available, otherwise 0
      console.warn(`AlpacaAccount[${this.id}]: failed to fetch FILL activities:`, err)
      return this.realizedPnLCache?.value ?? 0
    }
  }

  /** Paginate through all FILL activities (newest first by default). */
  private async fetchAllFills(): Promise<AlpacaFillActivityRaw[]> {
    const all: AlpacaFillActivityRaw[] = []
    let pageToken: string | undefined

    for (;;) {
      const page = await this.client.getAccountActivities({
        activityTypes: 'FILL',
        pageSize: 100,
        pageToken,
        direction: 'asc', // oldest first → natural FIFO order
        until: undefined,
        after: undefined,
        date: undefined,
      }) as AlpacaFillActivityRaw[]

      if (!page || page.length === 0) break
      all.push(...page)

      // Alpaca pagination: last item's id is the next page_token
      if (page.length < 100) break
      pageToken = (page[page.length - 1] as unknown as { id: string }).id
    }

    return all
  }

  /**
   * FIFO lot matching: track buy lots per symbol, realize PnL on sells.
   * Handles both long-only and short-selling (sell before buy → short lots).
   */
  static computeRealizedPnL(fills: AlpacaFillActivityRaw[]): number {
    // Per-symbol FIFO queue: { qty, price }[]
    // Positive qty = long lot, negative qty = short lot
    const lots = new Map<string, Array<{ qty: number; price: number }>>()
    let totalRealized = 0

    for (const fill of fills) {
      const symbol = fill.symbol
      const price = parseFloat(fill.price)
      const qty = parseFloat(fill.qty)
      const isBuy = fill.side === 'buy'

      if (!lots.has(symbol)) lots.set(symbol, [])
      const queue = lots.get(symbol)!

      // Determine if this fill opens or closes
      // Opening: buy when no short lots (or queue empty), sell when no long lots
      // Closing: buy against short lots, sell against long lots
      let remaining = qty

      while (remaining > 0 && queue.length > 0) {
        const front = queue[0]
        const isClosing = isBuy ? front.qty < 0 : front.qty > 0

        if (!isClosing) break // Same direction → this fill opens new lots

        const matchQty = Math.min(remaining, Math.abs(front.qty))

        if (front.qty > 0) {
          // Closing long: sell at `price`, entry was `front.price`
          totalRealized += matchQty * (price - front.price)
        } else {
          // Closing short: buy at `price`, entry was `front.price`
          totalRealized += matchQty * (front.price - price)
        }

        remaining -= matchQty
        front.qty += isBuy ? matchQty : -matchQty // shrink lot toward 0

        if (Math.abs(front.qty) < 1e-10) queue.shift() // lot fully consumed
      }

      // Remaining qty opens new lots
      if (remaining > 0) {
        queue.push({ qty: isBuy ? remaining : -remaining, price })
      }
    }

    return Math.round(totalRealized * 100) / 100 // round to cents
  }

  // ==================== Internal ====================

  /** Extract native symbol from aliceId, or null if not ours. */
  private parseAliceId(aliceId: string): string | null {
    const prefix = `${this.provider}-`
    if (!aliceId.startsWith(prefix)) return null
    return aliceId.slice(prefix.length)
  }

  /** Build a fully qualified Contract for an Alpaca ticker. */
  private makeContract(ticker: string): Contract {
    return {
      aliceId: `${this.provider}-${ticker}`,
      symbol: ticker,
      secType: 'STK',
      exchange: 'SMART',
      currency: 'USD',
    }
  }

  /**
   * Resolve a Contract to an Alpaca ticker symbol.
   * Accepts: aliceId, or symbol (+ optional secType check).
   */
  private resolveSymbol(contract: Contract): string | null {
    if (contract.aliceId) {
      return this.parseAliceId(contract.aliceId)
    }
    if (contract.symbol) {
      // If secType is specified and not STK, not our domain
      if (contract.secType && contract.secType !== 'STK') return null
      return contract.symbol.toUpperCase()
    }
    return null
  }

  private mapOrder(o: AlpacaOrderRaw): Order {
    const symbol = o.symbol
    return {
      id: o.id,
      contract: this.makeContract(symbol),
      side: o.side as 'buy' | 'sell',
      type: o.type as Order['type'],
      qty: parseFloat(o.qty ?? o.notional ?? '0'),
      price: o.limit_price ? parseFloat(o.limit_price) : undefined,
      stopPrice: o.stop_price ? parseFloat(o.stop_price) : undefined,
      timeInForce: o.time_in_force as Order['timeInForce'],
      extendedHours: o.extended_hours,
      status: this.mapOrderStatus(o.status),
      filledPrice: o.filled_avg_price ? parseFloat(o.filled_avg_price) : undefined,
      filledQty: o.filled_qty ? parseFloat(o.filled_qty) : undefined,
      filledAt: o.filled_at ? new Date(o.filled_at) : undefined,
      createdAt: new Date(o.created_at),
      rejectReason: o.reject_reason ?? undefined,
    }
  }

  private mapOrderStatus(alpacaStatus: string): Order['status'] {
    switch (alpacaStatus) {
      case 'filled':
        return 'filled'
      case 'new':
      case 'accepted':
      case 'pending_new':
      case 'accepted_for_bidding':
        return 'pending'
      case 'canceled':
      case 'expired':
      case 'replaced':
        return 'cancelled'
      case 'partially_filled':
        return 'partially_filled'
      case 'done_for_day':
      case 'suspended':
      case 'rejected':
        return 'rejected'
      default:
        return 'pending'
    }
  }
}
