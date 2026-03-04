import { describe, it, expect, vi, beforeEach } from 'vitest'
import { MaxPositionSizeGuard } from './max-position-size.js'
import { MaxLeverageGuard } from './max-leverage.js'
import { CooldownGuard } from './cooldown.js'
import { SymbolWhitelistGuard } from './symbol-whitelist.js'
import { createGuardPipeline } from './guard-pipeline.js'
import { resolveGuards, registerGuard } from './registry.js'
import type { GuardContext, OperationGuard } from './types.js'
import type { Operation } from '../git/types.js'
import type { AccountInfo, Position } from '../interfaces.js'
import { MockTradingAccount, makePosition } from '../__test__/mock-account.js'

// ==================== Helpers ====================

function makeContext(overrides: {
  operation?: Operation
  positions?: Position[]
  account?: Partial<AccountInfo>
} = {}): GuardContext {
  return {
    operation: overrides.operation ?? {
      action: 'placeOrder',
      params: { symbol: 'AAPL', side: 'buy', type: 'market', qty: 10 },
    },
    positions: overrides.positions ?? [],
    account: {
      cash: 100_000,
      equity: 100_000,
      unrealizedPnL: 0,
      realizedPnL: 0,
      ...overrides.account,
    },
  }
}

// ==================== MaxPositionSizeGuard ====================

describe('MaxPositionSizeGuard', () => {
  it('allows order within limit', () => {
    const guard = new MaxPositionSizeGuard({ maxPercentOfEquity: 25 })
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'AAPL', side: 'buy', type: 'market', notional: 20_000 },
      },
      account: { equity: 100_000 },
    })

    expect(guard.check(ctx)).toBeNull()
  })

  it('rejects order exceeding limit', () => {
    const guard = new MaxPositionSizeGuard({ maxPercentOfEquity: 25 })
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'AAPL', side: 'buy', type: 'market', notional: 30_000 },
      },
      account: { equity: 100_000 },
    })

    const result = guard.check(ctx)
    expect(result).not.toBeNull()
    expect(result).toContain('30.0%')
    expect(result).toContain('limit: 25%')
  })

  it('considers existing position value', () => {
    const guard = new MaxPositionSizeGuard({ maxPercentOfEquity: 25 })
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'AAPL', side: 'buy', type: 'market', notional: 10_000 },
      },
      positions: [makePosition({ contract: { symbol: 'AAPL' }, marketValue: 20_000 })],
      account: { equity: 100_000 },
    })

    const result = guard.check(ctx)
    expect(result).not.toBeNull()
    // 20k existing + 10k new = 30k = 30%
    expect(result).toContain('30.0%')
  })

  it('uses default 25% if no option provided', () => {
    const guard = new MaxPositionSizeGuard({})
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'AAPL', side: 'buy', type: 'market', notional: 26_000 },
      },
      account: { equity: 100_000 },
    })
    expect(guard.check(ctx)).not.toBeNull()
  })

  it('skips non-placeOrder operations', () => {
    const guard = new MaxPositionSizeGuard({ maxPercentOfEquity: 1 })
    const ctx = makeContext({
      operation: { action: 'closePosition', params: { symbol: 'AAPL' } },
    })
    expect(guard.check(ctx)).toBeNull()
  })

  it('allows when addedValue cannot be estimated (qty-based, no existing position)', () => {
    const guard = new MaxPositionSizeGuard({ maxPercentOfEquity: 1 })
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'NEW_STOCK', side: 'buy', type: 'market', qty: 100 },
      },
    })
    expect(guard.check(ctx)).toBeNull()
  })
})

// ==================== MaxLeverageGuard ====================

describe('MaxLeverageGuard', () => {
  it('allows leverage within limit', () => {
    const guard = new MaxLeverageGuard({ maxLeverage: 10 })
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'BTCUSDT', side: 'buy', type: 'market', qty: 1, leverage: 5 },
      },
    })
    expect(guard.check(ctx)).toBeNull()
  })

  it('rejects leverage exceeding limit', () => {
    const guard = new MaxLeverageGuard({ maxLeverage: 10 })
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'BTCUSDT', side: 'buy', type: 'market', qty: 1, leverage: 15 },
      },
    })

    const result = guard.check(ctx)
    expect(result).not.toBeNull()
    expect(result).toContain('15x')
    expect(result).toContain('10x')
  })

  it('checks adjustLeverage operations', () => {
    const guard = new MaxLeverageGuard({ maxLeverage: 10 })
    const ctx = makeContext({
      operation: {
        action: 'adjustLeverage',
        params: { symbol: 'BTCUSDT', newLeverage: 20 },
      },
    })
    expect(guard.check(ctx)).not.toBeNull()
  })

  it('supports per-symbol overrides', () => {
    const guard = new MaxLeverageGuard({
      maxLeverage: 10,
      symbolOverrides: { BTCUSDT: 3 },
    })

    const btcCtx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'BTCUSDT', side: 'buy', type: 'market', qty: 1, leverage: 5 },
      },
    })
    expect(guard.check(btcCtx)).toContain('5x exceeds limit 3x')

    const ethCtx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'ETHUSDT', side: 'buy', type: 'market', qty: 1, leverage: 5 },
      },
    })
    expect(guard.check(ethCtx)).toBeNull()
  })

  it('skips operations without leverage', () => {
    const guard = new MaxLeverageGuard({ maxLeverage: 1 })
    const ctx = makeContext({
      operation: {
        action: 'placeOrder',
        params: { symbol: 'AAPL', side: 'buy', type: 'market', qty: 1 },
      },
    })
    expect(guard.check(ctx)).toBeNull()
  })
})

// ==================== CooldownGuard ====================

describe('CooldownGuard', () => {
  it('allows first trade', () => {
    const guard = new CooldownGuard({ minIntervalMs: 60_000 })
    const ctx = makeContext()
    expect(guard.check(ctx)).toBeNull()
  })

  it('rejects rapid repeat trade for same symbol', () => {
    const guard = new CooldownGuard({ minIntervalMs: 60_000 })
    const ctx = makeContext()

    guard.check(ctx) // first — allowed
    const result = guard.check(ctx) // second — rejected
    expect(result).not.toBeNull()
    expect(result).toContain('Cooldown active')
    expect(result).toContain('AAPL')
  })

  it('allows trade for different symbol', () => {
    const guard = new CooldownGuard({ minIntervalMs: 60_000 })

    guard.check(makeContext({
      operation: { action: 'placeOrder', params: { symbol: 'AAPL', side: 'buy', type: 'market', qty: 1 } },
    }))

    const result = guard.check(makeContext({
      operation: { action: 'placeOrder', params: { symbol: 'GOOG', side: 'buy', type: 'market', qty: 1 } },
    }))
    expect(result).toBeNull()
  })

  it('skips non-placeOrder operations', () => {
    const guard = new CooldownGuard({ minIntervalMs: 60_000 })
    const ctx = makeContext({
      operation: { action: 'closePosition', params: { symbol: 'AAPL' } },
    })
    expect(guard.check(ctx)).toBeNull()
  })
})

// ==================== SymbolWhitelistGuard ====================

describe('SymbolWhitelistGuard', () => {
  it('allows whitelisted symbols', () => {
    const guard = new SymbolWhitelistGuard({ symbols: ['AAPL', 'GOOG'] })
    const ctx = makeContext()
    expect(guard.check(ctx)).toBeNull()
  })

  it('rejects non-whitelisted symbols', () => {
    const guard = new SymbolWhitelistGuard({ symbols: ['GOOG'] })
    const ctx = makeContext()
    expect(guard.check(ctx)).toContain('not in the allowed list')
  })

  it('throws on construction without symbols', () => {
    expect(() => new SymbolWhitelistGuard({})).toThrow('non-empty "symbols"')
    expect(() => new SymbolWhitelistGuard({ symbols: [] })).toThrow('non-empty "symbols"')
  })

  it('allows operations without a symbol param', () => {
    const guard = new SymbolWhitelistGuard({ symbols: ['AAPL'] })
    const ctx = makeContext({
      operation: { action: 'cancelOrder', params: { orderId: '123' } },
    })
    expect(guard.check(ctx)).toBeNull()
  })
})

// ==================== Guard Pipeline ====================

describe('createGuardPipeline', () => {
  it('returns dispatcher directly when no guards', () => {
    const dispatcher = vi.fn().mockResolvedValue({ success: true })
    const account = new MockTradingAccount()
    const pipeline = createGuardPipeline(dispatcher, account, [])

    // Should be the same function reference
    expect(pipeline).toBe(dispatcher)
  })

  it('passes through when all guards allow', async () => {
    const dispatcher = vi.fn().mockResolvedValue({ success: true })
    const account = new MockTradingAccount()
    const allowGuard: OperationGuard = { name: 'allow-all', check: () => null }

    const pipeline = createGuardPipeline(dispatcher, account, [allowGuard])
    const op: Operation = { action: 'placeOrder', params: { symbol: 'AAPL', side: 'buy', type: 'market', qty: 1 } }
    const result = await pipeline(op)

    expect(dispatcher).toHaveBeenCalledWith(op)
    expect(result).toEqual({ success: true })
  })

  it('blocks when a guard rejects', async () => {
    const dispatcher = vi.fn().mockResolvedValue({ success: true })
    const account = new MockTradingAccount()
    const denyGuard: OperationGuard = { name: 'deny-all', check: () => 'Denied!' }

    const pipeline = createGuardPipeline(dispatcher, account, [denyGuard])
    const op: Operation = { action: 'placeOrder', params: { symbol: 'AAPL', side: 'buy', type: 'market', qty: 1 } }
    const result = await pipeline(op) as Record<string, unknown>

    expect(dispatcher).not.toHaveBeenCalled()
    expect(result.success).toBe(false)
    expect(result.error).toContain('[guard:deny-all]')
    expect(result.error).toContain('Denied!')
  })

  it('stops at first rejecting guard', async () => {
    const dispatcher = vi.fn().mockResolvedValue({ success: true })
    const account = new MockTradingAccount()
    const guardA: OperationGuard = { name: 'A', check: vi.fn().mockReturnValue(null) }
    const guardB: OperationGuard = { name: 'B', check: vi.fn().mockReturnValue('Blocked by B') }
    const guardC: OperationGuard = { name: 'C', check: vi.fn().mockReturnValue(null) }

    const pipeline = createGuardPipeline(dispatcher, account, [guardA, guardB, guardC])
    const op: Operation = { action: 'placeOrder', params: { symbol: 'AAPL', side: 'buy', type: 'market', qty: 1 } }
    await pipeline(op)

    expect(guardA.check).toHaveBeenCalled()
    expect(guardB.check).toHaveBeenCalled()
    expect(guardC.check).not.toHaveBeenCalled()
  })

  it('fetches positions and account info for guard context', async () => {
    const dispatcher = vi.fn().mockResolvedValue({ success: true })
    const account = new MockTradingAccount()
    account.setPositions([makePosition()])

    let capturedCtx: GuardContext | undefined
    const spyGuard: OperationGuard = {
      name: 'spy',
      check: (ctx) => { capturedCtx = ctx; return null },
    }

    const pipeline = createGuardPipeline(dispatcher, account, [spyGuard])
    await pipeline({ action: 'placeOrder', params: { symbol: 'AAPL', side: 'buy', type: 'market', qty: 1 } })

    expect(capturedCtx).toBeDefined()
    expect(capturedCtx!.positions).toHaveLength(1)
    expect(capturedCtx!.account.equity).toBe(105_000)
  })
})

// ==================== Registry ====================

describe('resolveGuards', () => {
  it('resolves builtin guard types', () => {
    const guards = resolveGuards([
      { type: 'max-leverage', options: { maxLeverage: 5 } },
      { type: 'symbol-whitelist', options: { symbols: ['AAPL'] } },
    ])
    expect(guards).toHaveLength(2)
    expect(guards[0].name).toBe('max-leverage')
    expect(guards[1].name).toBe('symbol-whitelist')
  })

  it('skips unknown guard types with a warning', () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const guards = resolveGuards([{ type: 'nonexistent' }])
    expect(guards).toHaveLength(0)
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('nonexistent'))
    warnSpy.mockRestore()
  })

  it('returns empty for empty config', () => {
    expect(resolveGuards([])).toEqual([])
  })
})

describe('registerGuard', () => {
  it('registers a custom guard type', () => {
    registerGuard({
      type: 'test-custom',
      create: () => ({ name: 'test-custom', check: () => null }),
    })

    const guards = resolveGuards([{ type: 'test-custom' }])
    expect(guards).toHaveLength(1)
    expect(guards[0].name).toBe('test-custom')
  })
})
