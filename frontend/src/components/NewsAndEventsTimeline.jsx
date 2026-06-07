import clsx from 'clsx'
import { newsEvents } from '../data/financeMockData'

const typeClass = {
  earnings: 'bg-purple-500/20 text-purple-300',
  news: 'bg-blue-500/20 text-blue-300',
  filing: 'bg-amber-500/20 text-amber-300',
}

export default function NewsAndEventsTimeline() {
  return (
    <section className="bg-surface-50 border border-surface-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-200">News, Filings & Events</h3>
      <p className="text-[10px] text-gray-600 mb-3">Timeline of catalysts and research inputs.</p>
      <div className="space-y-3">
        {newsEvents.map((event) => (
          <div key={`${event.type}-${event.title}`} className="relative pl-5">
            <div className="absolute left-1 top-1.5 w-2 h-2 rounded-full bg-accent" />
            <div className="text-sm text-gray-300">{event.title}</div>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px] text-gray-500">
              <span className={clsx('px-1.5 py-0.5 rounded', typeClass[event.type])}>{event.type}</span>
              <span>{event.source}</span>
              <span>{event.time}</span>
              <span>{event.sentiment}</span>
              <span>{event.impact} impact</span>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
