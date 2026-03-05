import { describe, it, expect } from 'vitest'
import { AlpacaAccount } from './AlpacaAccount.js'

/** Helper to build a fill activity record. */
function fill(symbol: string, side: 'buy' | 'sell', qty: number, price: number, index = 0) {
  return {
    activity_type: 'FILL' as const,
    symbol,
    side,
    qty: String(qty),
    price: String(price),
    cum_qty: String(qty),
    leaves_qty: '0',
    transaction_time: `2025-01-01T00:00:0${index}Z`,
    order_id: `order-${index}`,
    type: 'fill',
  }
}

describe('AlpacaAccount.computeRealizedPnL', () => {
  it('returns 0 for empty fills', () => {
    expect(AlpacaAccount.computeRealizedPnL([])).toBe(0)
  })

  it('returns 0 when only buys (no closes)', () => {
    const fills = [
      fill('AAPL', 'buy', 10, 150, 0),
      fill('GOOG', 'buy', 5, 2800, 1),
    ]
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(0)
  })

  it('computes profit on simple buy then sell', () => {
    const fills = [
      fill('AAPL', 'buy', 10, 150, 0),
      fill('AAPL', 'sell', 10, 160, 1),
    ]
    // (160 - 150) * 10 = 100
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(100)
  })

  it('computes loss on simple buy then sell', () => {
    const fills = [
      fill('AAPL', 'buy', 10, 150, 0),
      fill('AAPL', 'sell', 10, 140, 1),
    ]
    // (140 - 150) * 10 = -100
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(-100)
  })

  it('handles partial close (sell less than bought)', () => {
    const fills = [
      fill('AAPL', 'buy', 10, 150, 0),
      fill('AAPL', 'sell', 4, 160, 1),
    ]
    // (160 - 150) * 4 = 40
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(40)
  })

  it('handles FIFO across multiple buy lots', () => {
    const fills = [
      fill('AAPL', 'buy', 5, 100, 0),
      fill('AAPL', 'buy', 5, 120, 1),
      fill('AAPL', 'sell', 7, 130, 2),
    ]
    // FIFO: first lot 5@100 → (130-100)*5 = 150
    //        second lot 2@120 → (130-120)*2 = 20
    // total = 170
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(170)
  })

  it('handles multiple symbols independently', () => {
    const fills = [
      fill('AAPL', 'buy', 10, 150, 0),
      fill('GOOG', 'buy', 2, 2800, 1),
      fill('AAPL', 'sell', 10, 160, 2),
      fill('GOOG', 'sell', 2, 2700, 3),
    ]
    // AAPL: (160-150)*10 = 100
    // GOOG: (2700-2800)*2 = -200
    // total = -100
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(-100)
  })

  it('handles short selling (sell then buy)', () => {
    const fills = [
      fill('AAPL', 'sell', 10, 160, 0),
      fill('AAPL', 'buy', 10, 150, 1),
    ]
    // Short: entry 160, exit 150 → (160-150)*10 = 100 profit
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(100)
  })

  it('handles short selling at a loss', () => {
    const fills = [
      fill('AAPL', 'sell', 10, 150, 0),
      fill('AAPL', 'buy', 10, 160, 1),
    ]
    // Short: entry 150, exit 160 → (150-160)*10 = -100 loss
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(-100)
  })

  it('handles multiple round trips', () => {
    const fills = [
      fill('AAPL', 'buy', 10, 100, 0),
      fill('AAPL', 'sell', 10, 110, 1),
      fill('AAPL', 'buy', 10, 105, 2),
      fill('AAPL', 'sell', 10, 115, 3),
    ]
    // Trip 1: (110-100)*10 = 100
    // Trip 2: (115-105)*10 = 100
    // total = 200
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(200)
  })

  it('rounds to cents', () => {
    const fills = [
      fill('AAPL', 'buy', 3, 10.333, 0),
      fill('AAPL', 'sell', 3, 10.667, 1),
    ]
    // (10.667 - 10.333) * 3 = 1.002
    expect(AlpacaAccount.computeRealizedPnL(fills)).toBe(1)
  })
})
