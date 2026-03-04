/**
 * Mock ITradingAccount for testing.
 *
 * All methods are vi.fn() so callers can override return values or inspect calls.
 */

import { vi } from 'vitest'
import type { Contract } from '../contract.js'
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
} from '../interfaces.js'

// ==================== Defaults ====================

export const DEFAULT_ACCOUNT_INFO: AccountInfo = {
  cash: 100_000,
  equity: 105_000,
  unrealizedPnL: 5_000,
  realizedPnL: 1_000,
  portfolioValue: 105_000,
  buyingPower: 200_000,
}

export const DEFAULT_CAPABILITIES: AccountCapabilities = {
  supportsLeverage: false,
  supportsShort: false,
  supportsNotional: true,
  supportsFundingRate: false,
  supportsOrderBook: false,
  supportsMarketClock: true,
  supportsExtendedHours: true,
  supportedSecTypes: ['STK'],
  supportedOrderTypes: ['market', 'limit', 'stop', 'stop_limit'],
}

export function makeContract(overrides: Partial<Contract> = {}): Contract {
  return {
    aliceId: 'mock-AAPL',
    symbol: 'AAPL',
    secType: 'STK',
    exchange: 'NASDAQ',
    currency: 'USD',
    ...overrides,
  }
}

export function makePosition(overrides: Partial<Position> = {}): Position {
  const contract = makeContract(overrides.contract)
  return {
    contract,
    side: 'long',
    qty: 10,
    avgEntryPrice: 150,
    currentPrice: 160,
    marketValue: 1600,
    unrealizedPnL: 100,
    unrealizedPnLPercent: 6.67,
    costBasis: 1500,
    leverage: 1,
    ...overrides,
    // Ensure nested contract override works
    ...(overrides.contract ? { contract: { ...contract, ...overrides.contract } } : {}),
  }
}

export function makeOrder(overrides: Partial<Order> = {}): Order {
  return {
    id: 'order-1',
    contract: makeContract(),
    side: 'buy',
    type: 'market',
    qty: 10,
    status: 'filled',
    filledPrice: 150,
    filledQty: 10,
    createdAt: new Date('2025-01-01'),
    ...overrides,
  }
}

export function makeOrderResult(overrides: Partial<OrderResult> = {}): OrderResult {
  return {
    success: true,
    orderId: 'order-1',
    filledPrice: 150,
    filledQty: 10,
    ...overrides,
  }
}

// ==================== MockTradingAccount ====================

export interface MockTradingAccountOptions {
  id?: string
  provider?: string
  label?: string
  capabilities?: Partial<AccountCapabilities>
  positions?: Position[]
  orders?: Order[]
  accountInfo?: Partial<AccountInfo>
}

export class MockTradingAccount implements ITradingAccount {
  readonly id: string
  readonly provider: string
  readonly label: string

  private _capabilities: AccountCapabilities
  private _positions: Position[]
  private _orders: Order[]
  private _accountInfo: AccountInfo

  // Spied methods
  init = vi.fn<() => Promise<void>>().mockResolvedValue(undefined)
  close = vi.fn<() => Promise<void>>().mockResolvedValue(undefined)

  resolveContract = vi.fn<(query: Partial<Contract>) => Promise<Contract[]>>()
    .mockResolvedValue([makeContract()])

  placeOrder = vi.fn<(order: OrderRequest) => Promise<OrderResult>>()
    .mockResolvedValue(makeOrderResult())

  cancelOrder = vi.fn<(orderId: string) => Promise<boolean>>()
    .mockResolvedValue(true)

  closePosition = vi.fn<(contract: Contract, qty?: number) => Promise<OrderResult>>()
    .mockResolvedValue(makeOrderResult())

  getQuote = vi.fn<(contract: Contract) => Promise<Quote>>()
    .mockResolvedValue({
      contract: makeContract(),
      last: 160,
      bid: 159.9,
      ask: 160.1,
      volume: 1_000_000,
      timestamp: new Date(),
    })

  getMarketClock = vi.fn<() => Promise<MarketClock>>()
    .mockResolvedValue({
      isOpen: true,
      nextClose: new Date('2025-01-01T21:00:00Z'),
    })

  adjustLeverage = vi.fn<(contract: Contract, leverage: number) => Promise<{ success: boolean; error?: string }>>()
    .mockResolvedValue({ success: true })

  constructor(options: MockTradingAccountOptions = {}) {
    this.id = options.id ?? 'mock-paper'
    this.provider = options.provider ?? 'mock'
    this.label = options.label ?? 'Mock Paper Account'
    this._capabilities = { ...DEFAULT_CAPABILITIES, ...options.capabilities }
    this._positions = options.positions ?? []
    this._orders = options.orders ?? []
    this._accountInfo = { ...DEFAULT_ACCOUNT_INFO, ...options.accountInfo }
  }

  getCapabilities(): AccountCapabilities {
    return this._capabilities
  }

  getAccount = vi.fn<() => Promise<AccountInfo>>()
    .mockImplementation(async () => this._accountInfo)

  getPositions = vi.fn<() => Promise<Position[]>>()
    .mockImplementation(async () => this._positions)

  getOrders = vi.fn<() => Promise<Order[]>>()
    .mockImplementation(async () => this._orders)

  // ---- Test helpers ----

  setPositions(positions: Position[]): void {
    this._positions = positions
  }

  setOrders(orders: Order[]): void {
    this._orders = orders
  }

  setAccountInfo(info: Partial<AccountInfo>): void {
    this._accountInfo = { ...this._accountInfo, ...info }
  }
}
