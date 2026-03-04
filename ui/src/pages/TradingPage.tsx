import { useConfigPage } from '../hooks/useConfigPage'
import { SaveIndicator } from '../components/SaveIndicator'
import { Toggle } from '../components/Toggle'
import { Section, Field, inputClass } from '../components/form'
import { GuardsSection, CRYPTO_GUARD_TYPES, SECURITIES_GUARD_TYPES, type GuardEntry } from '../components/guards'
import { SDKSelector, CRYPTO_SDK_OPTIONS, SECURITIES_SDK_OPTIONS } from '../components/SDKSelector'
import { ReconnectButton } from '../components/ReconnectButton'
import type { AppConfig } from '../api'

// ==================== Config types ====================

interface CryptoConfig {
  provider: {
    type: 'ccxt' | 'none'
    exchange?: string
    apiKey?: string
    apiSecret?: string
    password?: string
    sandbox?: boolean
    demoTrading?: boolean
    defaultMarketType?: 'spot' | 'swap'
  }
  guards: GuardEntry[]
}

interface SecuritiesConfig {
  provider: {
    type: 'alpaca' | 'none'
    apiKey?: string
    secretKey?: string
    paper?: boolean
  }
  guards: GuardEntry[]
}

// ==================== Page ====================

export function TradingPage({ tab }: { tab: string }) {
  return (
    <div className="flex flex-col flex-1 min-h-0">
      {/* Header */}
      <div className="shrink-0 border-b border-border">
        <div className="px-4 md:px-6 py-4">
          <h2 className="text-base font-semibold text-text">Trading</h2>
          <p className="text-[12px] text-text-muted mt-1">
            {tab === 'accounts'
              ? 'Configure exchange and broker connections.'
              : 'Trading guards validate operations before execution.'}
          </p>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto px-4 md:px-6 py-5">
        <div className="max-w-[640px] space-y-10">
          {tab === 'accounts' && <AccountsTab />}
          {tab === 'guards' && <GuardsTab />}
        </div>
      </div>
    </div>
  )
}

// ==================== Accounts Tab ====================

function AccountsTab() {
  return (
    <>
      <CryptoAccountSection />
      <div className="border-t border-border" />
      <SecuritiesAccountSection />
    </>
  )
}

// ---- Crypto Account ----

function CryptoAccountSection() {
  const { config, status, loadError, updateConfig, updateConfigImmediate, retry } =
    useConfigPage<CryptoConfig>({
      section: 'crypto',
      extract: (full: AppConfig) => (full as Record<string, unknown>).crypto as CryptoConfig,
    })

  const enabled = config?.provider.type !== 'none'

  const handleToggle = (on: boolean) => {
    if (on) {
      updateConfigImmediate({ provider: { ...config!.provider, type: 'ccxt' } })
    } else {
      updateConfigImmediate({ provider: { type: 'none' } })
    }
  }

  if (loadError) return <p className="text-[13px] text-red">Failed to load crypto configuration.</p>
  if (!config) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h3 className="text-[14px] font-semibold text-text">Crypto Trading</h3>
        <SaveIndicator status={status} onRetry={retry} />
      </div>

      <Section title="Trading Interface">
        <div className="flex items-center justify-between mb-4">
          <div>
            <p className="text-[13px] text-text">Enable Crypto Trading</p>
            <p className="text-[11px] text-text-muted/60">
              When disabled, the crypto trading engine and all related tools are unloaded.
            </p>
          </div>
          <Toggle checked={enabled} onChange={handleToggle} />
        </div>

        {enabled && (
          <div className="mt-1">
            <p className="text-[12px] text-text-muted mb-3">Select a trading SDK to connect with your exchange.</p>
            <SDKSelector
              options={CRYPTO_SDK_OPTIONS}
              selected="ccxt"
              onSelect={() => {/* future: switch SDK */}}
            />
          </div>
        )}
      </Section>

      {enabled && (
        <ExchangeSection
          config={config}
          onChange={updateConfig}
          onChangeImmediate={updateConfigImmediate}
        />
      )}
    </div>
  )
}

// ---- Securities Account ----

function SecuritiesAccountSection() {
  const { config, status, loadError, updateConfig, updateConfigImmediate, retry } =
    useConfigPage<SecuritiesConfig>({
      section: 'securities',
      extract: (full: AppConfig) => (full as Record<string, unknown>).securities as SecuritiesConfig,
    })

  const enabled = config?.provider.type !== 'none'

  const handleToggle = (on: boolean) => {
    if (on) {
      updateConfigImmediate({ provider: { ...config!.provider, type: 'alpaca' } })
    } else {
      updateConfigImmediate({ provider: { type: 'none' } })
    }
  }

  if (loadError) return <p className="text-[13px] text-red">Failed to load securities configuration.</p>
  if (!config) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h3 className="text-[14px] font-semibold text-text">Securities Trading</h3>
        <SaveIndicator status={status} onRetry={retry} />
      </div>

      <Section title="Trading Interface">
        <div className="flex items-center justify-between mb-4">
          <div>
            <p className="text-[13px] text-text">Enable Securities Trading</p>
            <p className="text-[11px] text-text-muted/60">
              When disabled, the securities trading engine and all related tools are unloaded.
            </p>
          </div>
          <Toggle checked={enabled} onChange={handleToggle} />
        </div>

        {enabled && (
          <div className="mt-1">
            <p className="text-[12px] text-text-muted mb-3">Select a broker SDK to connect with your brokerage.</p>
            <SDKSelector
              options={SECURITIES_SDK_OPTIONS}
              selected="alpaca"
              onSelect={() => {/* future: switch SDK */}}
            />
          </div>
        )}
      </Section>

      {enabled && (
        <BrokerSection
          config={config}
          onChange={updateConfig}
          onChangeImmediate={updateConfigImmediate}
        />
      )}
    </div>
  )
}

// ==================== Guards Tab ====================

function GuardsTab() {
  return (
    <>
      <CryptoGuardsSection />
      <SecuritiesGuardsSection />
    </>
  )
}

function CryptoGuardsSection() {
  const { config, loadError, updateConfig, updateConfigImmediate } =
    useConfigPage<CryptoConfig>({
      section: 'crypto',
      extract: (full: AppConfig) => (full as Record<string, unknown>).crypto as CryptoConfig,
    })

  const enabled = config?.provider.type !== 'none'

  if (loadError || !config) return null
  if (!enabled) {
    return (
      <div className="space-y-3">
        <h3 className="text-[14px] font-semibold text-text">Crypto Guards</h3>
        <p className="text-[13px] text-text-muted">
          Enable crypto trading in the Accounts tab to configure guards.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <h3 className="text-[14px] font-semibold text-text">Crypto Guards</h3>
      <GuardsSection
        guards={config.guards || []}
        guardTypes={CRYPTO_GUARD_TYPES}
        description="Guards validate operations before they reach the exchange. Guards run in order — first rejection stops the operation."
        onChange={(guards) => updateConfig({ guards })}
        onChangeImmediate={(guards) => updateConfigImmediate({ guards })}
      />
    </div>
  )
}

function SecuritiesGuardsSection() {
  const { config, loadError, updateConfig, updateConfigImmediate } =
    useConfigPage<SecuritiesConfig>({
      section: 'securities',
      extract: (full: AppConfig) => (full as Record<string, unknown>).securities as SecuritiesConfig,
    })

  const enabled = config?.provider.type !== 'none'

  if (loadError || !config) return null
  if (!enabled) {
    return (
      <div className="space-y-3 mt-8">
        <h3 className="text-[14px] font-semibold text-text">Securities Guards</h3>
        <p className="text-[13px] text-text-muted">
          Enable securities trading in the Accounts tab to configure guards.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-3 mt-8">
      <h3 className="text-[14px] font-semibold text-text">Securities Guards</h3>
      <GuardsSection
        guards={config.guards || []}
        guardTypes={SECURITIES_GUARD_TYPES}
        description="Guards validate operations before they reach the broker. Guards run in order — first rejection stops the operation."
        onChange={(guards) => updateConfig({ guards })}
        onChangeImmediate={(guards) => updateConfigImmediate({ guards })}
      />
    </div>
  )
}

// ==================== Exchange Section (CCXT-specific) ====================

interface ExchangeSectionProps {
  config: CryptoConfig
  onChange: (patch: Partial<CryptoConfig>) => void
  onChangeImmediate: (patch: Partial<CryptoConfig>) => void
}

function ExchangeSection({ config, onChange, onChangeImmediate }: ExchangeSectionProps) {
  const provider = config.provider

  const patchProvider = (field: string, value: unknown, immediate: boolean) => {
    const patch = {
      provider: { ...provider, type: 'ccxt' as const, [field]: value },
    }
    immediate ? onChangeImmediate(patch) : onChange(patch)
  }

  return (
    <Section
      title="Exchange Connection"
      description="CCXT exchange credentials. Save your config, then click Reconnect to apply."
    >
      <Field label="Exchange">
        <input
          className={inputClass}
          value={provider.exchange || ''}
          onChange={(e) => patchProvider('exchange', e.target.value.trim(), false)}
          placeholder="bybit"
        />
      </Field>
      <Field label="API Key">
        <input
          className={inputClass}
          type="password"
          value={provider.apiKey || ''}
          onChange={(e) => patchProvider('apiKey', e.target.value, false)}
          placeholder="Not configured"
        />
      </Field>
      <Field label="API Secret">
        <input
          className={inputClass}
          type="password"
          value={provider.apiSecret || ''}
          onChange={(e) => patchProvider('apiSecret', e.target.value, false)}
          placeholder="Not configured"
        />
      </Field>
      <Field label="Password (optional)">
        <input
          className={inputClass}
          type="password"
          value={provider.password || ''}
          onChange={(e) => patchProvider('password', e.target.value, false)}
          placeholder="Required by some exchanges (e.g. OKX)"
        />
      </Field>
      <Field label="Market Type">
        <select
          className={inputClass}
          value={provider.defaultMarketType || 'swap'}
          onChange={(e) => patchProvider('defaultMarketType', e.target.value, true)}
        >
          <option value="swap">Perpetual Swap</option>
          <option value="spot">Spot</option>
        </select>
      </Field>
      <div className="mb-3">
        <label className="flex items-center gap-2.5 cursor-pointer mb-2">
          <Toggle
            checked={provider.sandbox ?? false}
            onChange={(v) => patchProvider('sandbox', v, true)}
          />
          <span className="text-[13px] text-text">Sandbox Mode</span>
        </label>
        <label className="flex items-center gap-2.5 cursor-pointer mb-2">
          <Toggle
            checked={provider.demoTrading ?? true}
            onChange={(v) => patchProvider('demoTrading', v, true)}
          />
          <span className="text-[13px] text-text">Demo Trading</span>
        </label>
      </div>
      <ReconnectButton provider="ccxt" />
    </Section>
  )
}

// ==================== Broker Section (Alpaca-specific) ====================

interface BrokerSectionProps {
  config: SecuritiesConfig
  onChange: (patch: Partial<SecuritiesConfig>) => void
  onChangeImmediate: (patch: Partial<SecuritiesConfig>) => void
}

function BrokerSection({ config, onChange, onChangeImmediate }: BrokerSectionProps) {
  const patchProvider = (field: string, value: unknown, immediate: boolean) => {
    const patch = {
      provider: { ...config.provider, type: 'alpaca' as const, [field]: value },
    }
    immediate ? onChangeImmediate(patch) : onChange(patch)
  }

  return (
    <Section
      title="Broker Connection"
      description="Alpaca brokerage credentials. Save your config, then click Reconnect to apply."
    >
      <Field label="API Key">
        <input
          className={inputClass}
          type="password"
          value={config.provider.apiKey || ''}
          onChange={(e) => patchProvider('apiKey', e.target.value, false)}
          placeholder="Not configured"
        />
      </Field>
      <Field label="Secret Key">
        <input
          className={inputClass}
          type="password"
          value={config.provider.secretKey || ''}
          onChange={(e) => patchProvider('secretKey', e.target.value, false)}
          placeholder="Not configured"
        />
      </Field>
      <label className="flex items-center gap-2.5 cursor-pointer mb-2">
        <Toggle
          checked={config.provider.paper ?? true}
          onChange={(v) => patchProvider('paper', v, true)}
        />
        <span className="text-[13px] text-text">Paper Trading</span>
      </label>
      <p className="text-[11px] text-text-muted/60">
        When enabled, orders are routed to Alpaca's paper trading environment. Disable for live trading.
      </p>
      <ReconnectButton provider="alpaca" />
    </Section>
  )
}
