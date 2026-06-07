import TickerSearch from './TickerSearch'
import WatchlistPanel from './WatchlistPanel'
import StockHeader from './StockHeader'
import PriceChartPanel from './PriceChartPanel'
import StockAnalysisCard from './StockAnalysisCard'
import ResearchProvenancePanel from './ResearchProvenancePanel'
import FundamentalsPanel from './FundamentalsPanel'
import NewsAndEventsTimeline from './NewsAndEventsTimeline'
import { stockAnalysis } from '../data/financeMockData'

export default function StockWorkspace() {
  return (
    <div className="flex-1 overflow-y-auto p-3 md:p-4">
      <div className="grid xl:grid-cols-[260px_minmax(0,1fr)] gap-4">
        <div className="space-y-4">
          <TickerSearch />
          <WatchlistPanel />
          <ResearchProvenancePanel analysis={stockAnalysis} />
        </div>
        <main className="space-y-4 min-w-0">
          <StockHeader />
          <div className="grid 2xl:grid-cols-[minmax(0,1.4fr)_minmax(360px,0.8fr)] gap-4">
            <PriceChartPanel />
            <StockAnalysisCard analysis={stockAnalysis} />
          </div>
          <FundamentalsPanel />
          <NewsAndEventsTimeline />
        </main>
      </div>
    </div>
  )
}
