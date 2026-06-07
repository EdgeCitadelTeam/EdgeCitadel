import clsx from 'clsx'
import { marketIndexes, sectorPerformance } from '../data/financeMockData'
import StockWorkspace from './StockWorkspace'

function Change({ value }) {
  return <span className={clsx(value >= 0 ? 'text-green-400' : 'text-red-400')}>{value >= 0 ? '+' : ''}{value.toFixed(2)}%</span>
}

export default function MarketOverview() {
  return (
    <div className="flex-1 overflow-y-auto p-3 md:p-4 space-y-4">
      <section className="grid md:grid-cols-4 gap-3">
        {marketIndexes.map((idx) => (
          <div key={idx.symbol} className="bg-surface-50 border border-surface-200 rounded-xl p-3">
            <div className="flex items-center justify-between"><div className="text-sm font-semibold text-gray-100">{idx.symbol}</div><Change value={idx.changePercent} /></div>
            <div className="text-[10px] text-gray-500 mt-1">{idx.label}</div>
            <div className="mt-2 text-lg font-mono text-gray-200">${idx.price}</div>
          </div>
        ))}
      </section>
      <section className="bg-surface-50 border border-surface-200 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-gray-200 mb-3">Sector Heatmap</h3>
        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-2">
          {sectorPerformance.map((sector) => (
            <div key={sector.name} className={clsx('rounded-lg p-3 border', sector.changePercent >= 0 ? 'bg-green-500/10 border-green-500/20' : 'bg-red-500/10 border-red-500/20')}>
              <div className="text-xs text-gray-300">{sector.name}</div>
              <div className="mt-1 text-lg font-mono"><Change value={sector.changePercent} /></div>
            </div>
          ))}
        </div>
      </section>
      <StockWorkspace />
    </div>
  )
}
