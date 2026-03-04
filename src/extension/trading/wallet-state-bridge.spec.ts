import { describe, it, expect, beforeEach } from 'vitest'
import { createWalletStateBridge } from './wallet-state-bridge.js'
import { MockTradingAccount, makePosition, makeOrder } from './__test__/mock-account.js'

describe('createWalletStateBridge', () => {
  let account: MockTradingAccount

  beforeEach(() => {
    account = new MockTradingAccount()
  })

  it('returns a function', () => {
    const bridge = createWalletStateBridge(account)
    expect(typeof bridge).toBe('function')
  })

  it('assembles GitState from account data', async () => {
    account.setAccountInfo({ cash: 50_000, equity: 55_000, unrealizedPnL: 3_000, realizedPnL: 800 })
    account.setPositions([makePosition()])
    account.setOrders([
      makeOrder({ id: 'o1', status: 'filled' }),
      makeOrder({ id: 'o2', status: 'pending' }),
      makeOrder({ id: 'o3', status: 'cancelled' }),
    ])

    const bridge = createWalletStateBridge(account)
    const state = await bridge()

    expect(state.cash).toBe(50_000)
    expect(state.equity).toBe(55_000)
    expect(state.unrealizedPnL).toBe(3_000)
    expect(state.realizedPnL).toBe(800)
    expect(state.positions).toHaveLength(1)
    expect(state.pendingOrders).toHaveLength(1)
    expect(state.pendingOrders[0].id).toBe('o2')
  })

  it('calls all three account methods in parallel', async () => {
    const bridge = createWalletStateBridge(account)
    await bridge()

    expect(account.getAccount).toHaveBeenCalledTimes(1)
    expect(account.getPositions).toHaveBeenCalledTimes(1)
    expect(account.getOrders).toHaveBeenCalledTimes(1)
  })

  it('returns empty pendingOrders when no orders are pending', async () => {
    account.setOrders([
      makeOrder({ status: 'filled' }),
      makeOrder({ id: 'o2', status: 'cancelled' }),
    ])

    const bridge = createWalletStateBridge(account)
    const state = await bridge()

    expect(state.pendingOrders).toHaveLength(0)
  })
})
