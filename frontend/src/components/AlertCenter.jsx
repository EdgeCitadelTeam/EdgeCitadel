import clsx from 'clsx'
import { alerts } from '../data/financeMockData'

const severityClass = {
  high: 'bg-red-500/20 text-red-300',
  medium: 'bg-yellow-500/20 text-yellow-300',
  low: 'bg-blue-500/20 text-blue-300',
}

export default function AlertCenter() {
  return (
    <div className="flex-1 overflow-y-auto p-3 md:p-4 space-y-4">
      <section className="bg-surface-50 border border-surface-200 rounded-xl p-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-gray-100">Alert Center</h2>
            <p className="text-xs text-gray-500">Rules for price, volume, sentiment, agent rating changes, and portfolio exposure.</p>
          </div>
          <button className="bg-accent hover:bg-accent-dark text-white px-3 py-1.5 rounded text-xs">New rule</button>
        </div>
      </section>
      <section className="grid gap-3">
        {alerts.map((alert) => (
          <div key={alert.id} className="bg-surface-50 border border-surface-200 rounded-xl p-4">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-mono text-gray-100">{alert.symbol}</span>
              <span className={clsx('px-2 py-0.5 rounded text-[10px] uppercase', severityClass[alert.severity])}>{alert.severity}</span>
              <span className={clsx('px-2 py-0.5 rounded text-[10px]', alert.status === 'open' ? 'bg-green-500/20 text-green-300' : 'bg-surface-100 text-gray-500')}>{alert.status}</span>
              <span className="ml-auto text-[10px] text-gray-600">{alert.time}</span>
            </div>
            <div className="mt-2 text-sm text-gray-300">{alert.rule}</div>
            <div className="mt-1 text-xs text-gray-500">{alert.trigger}</div>
          </div>
        ))}
      </section>
    </div>
  )
}
