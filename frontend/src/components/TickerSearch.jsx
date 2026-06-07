import { Search } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { mockQuotes } from '../data/financeMockData'

export default function TickerSearch() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol)
  const setSelectedSymbol = useAppStore((s) => s.setSelectedSymbol)
  const watchlist = useAppStore((s) => s.watchlist)

  const symbols = Array.from(new Set([...watchlist, ...Object.keys(mockQuotes)]))

  return (
    <div className="relative">
      <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
      <select
        value={selectedSymbol || ''}
        onChange={(e) => setSelectedSymbol(e.target.value)}
        className="w-full bg-surface-100 border border-surface-200 rounded-lg pl-9 pr-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-accent/60"
      >
        {symbols.map((symbol) => (
          <option key={symbol} value={symbol}>{symbol} · {mockQuotes[symbol]?.name || 'Tracked equity'}</option>
        ))}
      </select>
    </div>
  )
}
