import { Clock, Database } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { mockQuotes } from '../data/financeMockData'

export default function StockHeader() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol)
  const quote = mockQuotes[selectedSymbol]

  if (!quote) {
    return <div className="bg-surface-50 border border-surface-200 rounded-xl p-4 text-sm text-gray-500">Select a ticker to begin research.</div>
  }

  const up = quote.change >= 0
  return (
    <section className="bg-surface-50 border border-surface-200 rounded-xl p-4">
      <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
        <div>
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-2xl font-semibold text-gray-100">{quote.symbol}</h2>
            <span className="text-sm text-gray-400">{quote.name}</span>
            <span className="text-[10px] uppercase tracking-[0.14em] bg-accent/15 text-accent-light rounded px-2 py-0.5">{quote.rating}</span>
          </div>
          <div className="mt-2 flex items-end gap-3">
            <span className="text-3xl font-mono text-gray-100">${quote.price.toFixed(2)}</span>
            <span className={clsx('text-sm font-medium pb-1', up ? 'text-green-400' : 'text-red-400')}>
              {up ? '+' : ''}{quote.change.toFixed(2)} ({up ? '+' : ''}{quote.changePercent.toFixed(2)}%)
            </span>
          </div>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
          <Metric label="Market cap" value={quote.marketCap} />
          <Metric label="Volume" value={quote.volume} />
          <Metric label="Avg volume" value={quote.averageVolume} />
          <Metric label="52W range" value={`$${quote.week52Low}-$${quote.week52High}`} />
        </div>
      </div>
      <div className="mt-3 flex flex-wrap items-center gap-3 text-[10px] text-gray-500">
        <span>Pre-market ${quote.preMarket}</span>
        <span>After-hours ${quote.afterHours}</span>
        <span className="flex items-center gap-1"><Clock size={11} /> {quote.updatedAt}</span>
        <span className="flex items-center gap-1"><Database size={11} /> {quote.source}</span>
      </div>
    </section>
  )
}

function Metric({ label, value }) {
  return <div><div className="text-gray-500">{label}</div><div className="mt-0.5 text-gray-200 font-mono">{value}</div></div>
}
