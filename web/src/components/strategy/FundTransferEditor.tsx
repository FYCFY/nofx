import { ArrowRightLeft } from 'lucide-react'
import type { FundTransferConfig } from '../../types'

interface FundTransferEditorProps {
  config?: FundTransferConfig
  onChange: (config: FundTransferConfig) => void
  disabled?: boolean
  language: string
}

const defaultFundTransferConfig: FundTransferConfig = {
  enabled: false,
  mode: 'futures_to_spot',
  trigger_time: '00:00',
  target_futures_available_balance: 0,
  min_transfer_amount: 0.01,
}

export function FundTransferEditor({
  config,
  onChange,
  disabled,
  language,
}: FundTransferEditorProps) {
  const current = config || defaultFundTransferConfig

  const t = (key: string) => {
    const translations: Record<string, Record<string, string>> = {
      title: { zh: '资金划转', en: 'Fund Transfer' },
      desc: { zh: '策略级资金调度，会作用于所有绑定该策略的交易员', en: 'Strategy-level transfer settings affect all traders using this strategy' },
      enabled: { zh: '启用自动划转', en: 'Enable auto transfer' },
      mode: { zh: '划转模式', en: 'Transfer mode' },
      modeFuturesToSpot: { zh: '仅 合约 -> 现货', en: 'Futures -> Spot only' },
      modeBidirectional: { zh: '双向划转', en: 'Bidirectional' },
      triggerTime: { zh: '触发时间（北京时间）', en: 'Trigger time (Beijing)' },
      targetBalance: { zh: '目标合约可用余额 (USDT)', en: 'Target futures available balance (USDT)' },
      minAmount: { zh: '最小划转金额 (USDT)', en: 'Minimum transfer amount (USDT)' },
      futuresToSpotHint: { zh: '当合约可用余额高于目标时，自动划转到现货', en: 'Transfers to spot when futures available exceeds target' },
      bidirectionalHint: { zh: '高于目标划转到现货，低于目标从现货补到合约', en: 'Above target transfers to spot; below target refills from spot' },
    }
    return translations[key]?.[language] || key
  }

  const updateField = <K extends keyof FundTransferConfig>(
    key: K,
    value: FundTransferConfig[K]
  ) => {
    if (!disabled) {
      onChange({ ...current, [key]: value })
    }
  }

  const modeHint = current.mode === 'bidirectional' ? t('bidirectionalHint') : t('futuresToSpotHint')

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <ArrowRightLeft className="w-4 h-4" style={{ color: '#F0B90B' }} />
        <div>
          <div className="text-sm font-medium" style={{ color: '#EAECEF' }}>{t('title')}</div>
          <div className="text-xs" style={{ color: '#848E9C' }}>{t('desc')}</div>
        </div>
      </div>

      <label className="flex items-center gap-2 text-sm" style={{ color: '#EAECEF' }}>
        <input
          type="checkbox"
          checked={current.enabled}
          onChange={(e) => updateField('enabled', e.target.checked)}
          disabled={disabled}
          className="accent-yellow-500"
        />
        {t('enabled')}
      </label>

      <div className="grid grid-cols-1 gap-3">
        <div>
          <label className="block text-xs mb-1" style={{ color: '#848E9C' }}>{t('mode')}</label>
          <select
            value={current.mode}
            onChange={(e) => updateField('mode', e.target.value as FundTransferConfig['mode'])}
            disabled={disabled}
            className="w-full px-3 py-2 rounded text-sm"
            style={{ background: '#0B0E11', border: '1px solid #2B3139', color: '#EAECEF' }}
          >
            <option value="futures_to_spot">{t('modeFuturesToSpot')}</option>
            <option value="bidirectional">{t('modeBidirectional')}</option>
          </select>
          <p className="text-xs mt-1" style={{ color: '#848E9C' }}>{modeHint}</p>
        </div>

        <div>
          <label className="block text-xs mb-1" style={{ color: '#848E9C' }}>{t('triggerTime')}</label>
          <input
            type="time"
            value={current.trigger_time}
            onChange={(e) => updateField('trigger_time', e.target.value)}
            disabled={disabled}
            className="w-full px-3 py-2 rounded text-sm"
            style={{ background: '#0B0E11', border: '1px solid #2B3139', color: '#EAECEF' }}
          />
        </div>

        <div>
          <label className="block text-xs mb-1" style={{ color: '#848E9C' }}>{t('targetBalance')}</label>
          <input
            type="number"
            value={current.target_futures_available_balance}
            onChange={(e) => updateField('target_futures_available_balance', Number(e.target.value) || 0)}
            disabled={disabled}
            min={0}
            step={0.01}
            className="w-full px-3 py-2 rounded text-sm"
            style={{ background: '#0B0E11', border: '1px solid #2B3139', color: '#EAECEF' }}
          />
        </div>

        <div>
          <label className="block text-xs mb-1" style={{ color: '#848E9C' }}>{t('minAmount')}</label>
          <input
            type="number"
            value={current.min_transfer_amount}
            onChange={(e) => updateField('min_transfer_amount', Number(e.target.value) || 0)}
            disabled={disabled}
            min={0}
            step={0.01}
            className="w-full px-3 py-2 rounded text-sm"
            style={{ background: '#0B0E11', border: '1px solid #2B3139', color: '#EAECEF' }}
          />
        </div>
      </div>
    </div>
  )
}

