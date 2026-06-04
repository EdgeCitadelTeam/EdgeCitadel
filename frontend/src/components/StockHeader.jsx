import { Bell, TrendingUp } from 'lucide-react'
import clsx from 'clsx'

export default function StockHeader({
  symbol = 'AAPL',
  companyName = 'Apple Inc.',
  price = 213.55,
  change = 2.14,
  changePercent = 1.01,
  marketState = 'Live market preview',
}) {
  const positive = change >= 0

  return (
    <section className="rounded-xl border border-surface-200 bg-surface-50 p-4 shadow-sm">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <span className="rounded-lg bg-accent/15 px-2.5 py-1 text-sm font-semibold text-accent-light">
              {symbol}
            </span>
            <span className="text-xs text-gray-500">{marketState}</span>
          </div>
          <h2 className="mt-3 text-2xl font-semibold text-gray-100">{companyName}</h2>
          <p className="mt-1 max-w-2xl text-sm text-gray-400">
            Research cockpit for price action, technical context, market events, and agent-generated alerts.
          </p>
        </div>

        <div className="flex items-center gap-3">
          <div className="text-right">
            <div className="text-3xl font-semibold tabular-nums text-gray-100">
              ${price.toFixed(2)}
            </div>
            <div
              className={clsx(
                'mt-1 inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium',
                positive ? 'bg-emerald-500/15 text-emerald-300' : 'bg-red-500/15 text-red-300'
              )}
            >
              <TrendingUp size={12} className={positive ? '' : 'rotate-180'} />
              {positive ? '+' : ''}{change.toFixed(2)} ({positive ? '+' : ''}{changePercent.toFixed(2)}%)
            </div>
          </div>
          <button className="rounded-lg border border-surface-200 bg-surface px-3 py-2 text-xs text-gray-300 transition-colors hover:border-accent/60 hover:text-accent-light">
            <span className="inline-flex items-center gap-1.5">
              <Bell size={14} /> Watch
            </span>
          </button>
        </div>
      </div>
    </section>
  )
}
