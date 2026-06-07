import { Area, AreaChart, Bar, BarChart, CartesianGrid, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { mockCandles } from '../data/financeMockData'

const RANGES = ['1M', '6M', 'YTD', '1Y', '5Y']

export default function PriceChartPanel() {
  const chartRange = useAppStore((s) => s.chartRange)
  const setChartRange = useAppStore((s) => s.setChartRange)

  return (
    <section className="bg-surface-50 border border-surface-200 rounded-xl p-4 min-h-[360px]">
      <div className="flex items-center justify-between gap-3 mb-3">
        <div>
          <h3 className="text-sm font-semibold text-gray-200">Price Action</h3>
          <p className="text-[10px] text-gray-600">Close price with 20/50-week moving averages and volume.</p>
        </div>
        <div className="flex items-center gap-1">
          {RANGES.map((range) => (
            <button
              key={range}
              onClick={() => setChartRange(range)}
              className={clsx(
                'px-2 py-1 rounded text-[10px] transition-colors',
                chartRange === range ? 'bg-accent text-white' : 'bg-surface-100 text-gray-500 hover:text-gray-300'
              )}
            >
              {range}
            </button>
          ))}
        </div>
      </div>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={mockCandles} margin={{ left: -12, right: 8, top: 10, bottom: 0 }}>
            <defs>
              <linearGradient id="priceFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#38bdf8" stopOpacity={0.35} />
                <stop offset="100%" stopColor="#38bdf8" stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
            <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 10 }} minTickGap={22} />
            <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} domain={['dataMin - 4', 'dataMax + 4']} />
            <Tooltip contentStyle={{ background: '#111827', border: '1px solid #374151', color: '#e5e7eb' }} />
            <Area type="monotone" dataKey="close" stroke="#38bdf8" strokeWidth={2} fill="url(#priceFill)" />
            <Line type="monotone" dataKey="ma20" stroke="#f59e0b" strokeWidth={1.5} dot={false} />
            <Line type="monotone" dataKey="ma50" stroke="#a78bfa" strokeWidth={1.5} dot={false} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
      <div className="h-20 mt-2">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={mockCandles} margin={{ left: -12, right: 8, top: 0, bottom: 0 }}>
            <XAxis dataKey="date" hide />
            <YAxis hide />
            <Tooltip contentStyle={{ background: '#111827', border: '1px solid #374151', color: '#e5e7eb' }} />
            <Bar dataKey="volume" fill="#4b5563" radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="flex items-center gap-4 mt-2 text-[10px] text-gray-500">
        <span><span className="inline-block w-2 h-2 rounded-full bg-sky-400 mr-1" />Close</span>
        <span><span className="inline-block w-2 h-2 rounded-full bg-amber-500 mr-1" />MA20</span>
        <span><span className="inline-block w-2 h-2 rounded-full bg-violet-400 mr-1" />MA50</span>
      </div>
    </section>
  )
}
