import { fetchJson } from './client'
import type { TradingAccount, AccountInfo, Position, WalletCommitLog, ReconnectResult } from './types'

// ==================== Unified Trading API ====================

export const tradingApi = {
  // ==================== Accounts ====================

  async listAccounts(): Promise<{ accounts: TradingAccount[] }> {
    return fetchJson('/api/trading/accounts')
  },

  async equity(): Promise<{ totalEquity: number; totalCash: number; totalUnrealizedPnL: number; totalRealizedPnL: number; accounts: Array<{ id: string; label: string; equity: number; cash: number }> }> {
    return fetchJson('/api/trading/equity')
  },

  // ==================== Per-account ====================

  async reconnectAccount(accountId: string): Promise<ReconnectResult> {
    const res = await fetch(`/api/trading/accounts/${accountId}/reconnect`, { method: 'POST' })
    return res.json()
  },

  async accountInfo(accountId: string): Promise<AccountInfo> {
    return fetchJson(`/api/trading/accounts/${accountId}/account`)
  },

  async positions(accountId: string): Promise<{ positions: Position[] }> {
    return fetchJson(`/api/trading/accounts/${accountId}/positions`)
  },

  async orders(accountId: string): Promise<{ orders: unknown[] }> {
    return fetchJson(`/api/trading/accounts/${accountId}/orders`)
  },

  async marketClock(accountId: string): Promise<{ isOpen: boolean; nextOpen: string; nextClose: string }> {
    return fetchJson(`/api/trading/accounts/${accountId}/market-clock`)
  },

  async walletLog(accountId: string, limit = 20, symbol?: string): Promise<{ commits: WalletCommitLog[] }> {
    const params = new URLSearchParams({ limit: String(limit) })
    if (symbol) params.set('symbol', symbol)
    return fetchJson(`/api/trading/accounts/${accountId}/wallet/log?${params}`)
  },

  async walletShow(accountId: string, hash: string): Promise<unknown> {
    return fetchJson(`/api/trading/accounts/${accountId}/wallet/show/${hash}`)
  },
}
