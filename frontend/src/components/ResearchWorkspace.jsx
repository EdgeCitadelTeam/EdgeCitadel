import { useState } from 'react'
import StockHeader from './StockHeader'
import PriceChartPanel from './PriceChartPanel'

const WATCHLIST = [
  { symbol: 'AAPL', name: 'Apple Inc.', price: 213.55, change: 2.14, changePercent: 1.01 },
  { symbol: 'NVDA', name: 'NVIDIA Corporation', price: 147.32, change: 4.82, changePercent: 3.36 },
  { symbol: 'MSFT', name: 'Microsoft Corporation', price: 469.12, change: -1.38, changePercent: -0.29 },
]

export default function ResearchWorkspace() {
  const [selected, setSelected] = useState(WATCHLIST[0])

  return (
    <div className="flex-1 overflow-auto bg-surface p-4">
      <div className="mx-auto flex max-w-7xl flex-col gap-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-xl font-semibold text-gray-100">Research Workspace</h1>
            <p className="mt-1 text-sm text-gray-500">First-screen focus: stock identity, live price context, technical charting, and event markers.</p>
          </div>
          <div className="flex rounded-xl border border-surface-200 bg-surface-50 p-1">
            {WATCHLIST.map((stock) => (
              <button
                key={stock.symbol}
                onClick={() => setSelected(stock)}
                className={`rounded-lg px-3 py-2 text-xs font-medium transition-colors ${selected.symbol === stock.symbol ? 'bg-accent text-white' : 'text-gray-400 hover:text-gray-200'}`}
              >
                {stock.symbol}
              </button>
            ))}
          </div>
        </div>

        <StockHeader
          symbol={selected.symbol}
          companyName={selected.name}
          price={selected.price}
          change={selected.change}
          changePercent={selected.changePercent}
        />
        <PriceChartPanel symbol={selected.symbol} />
      </div>
    </div>
  )
}
