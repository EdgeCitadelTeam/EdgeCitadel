import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { mockQuotes } from '../data/financeMockData'

function Change({ value }) {
  return <span className={clsx(value >= 0 ? 'text-green-400' : 'text-red-400')}>{value >= 0 ? '+' : ''}{value.toFixed(2)}%</span>
}

export default function WatchlistPanel() {
  const watchlist = useAppStore((s) => s.watchlist)
  const selectedSymbol = useAppStore((s) => s.selectedSymbol)
  const setSelectedSymbol = useAppStore((s) => s.setSelectedSymbol)

  return (
    <aside className="bg-surface-50 border border-surface-200 rounded-xl overflow-hidden">
      <div className="px-3 py-2 border-b border-surface-200">
        <h3 className="text-xs font-semibold text-gray-300">Watchlist</h3>
        <p className="text-[10px] text-gray-600">Demo quotes until live data endpoints are connected.</p>
      </div>
      <div className="divide-y divide-surface-200">
        {watchlist.map((symbol) => {
          const quote = mockQuotes[symbol]
          return (
            <button
              key={symbol}
              onClick={() => setSelectedSymbol(symbol)}
              className={clsx(
                'w-full text-left px-3 py-2.5 hover:bg-surface-100 transition-colors',
                selectedSymbol === symbol && 'bg-accent/10'
              )}
            >
              <div className="flex items-center justify-between gap-2">
                <div>
                  <div className="text-sm font-semibold text-gray-100">{symbol}</div>
                  <div className="text-[10px] text-gray-500 truncate max-w-32">{quote?.name}</div>
                </div>
                <div className="text-right">
                  <div className="text-sm text-gray-200 font-mono">${quote?.price.toFixed(2)}</div>
                  <div className="text-[11px]"><Change value={quote?.changePercent || 0} /></div>
                </div>
              </div>
              <div className="mt-1 flex items-center justify-between text-[10px] text-gray-500">
                <span>{quote?.rating || '—'}</span>
                <span>{quote?.alertCount || 0} alerts</span>
              </div>
            </button>
          )
        })}
      </div>
    </aside>
  )
}
