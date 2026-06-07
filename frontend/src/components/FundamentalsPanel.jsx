import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { fundamentals, peers } from '../data/financeMockData'

export default function FundamentalsPanel() {
  return (
    <section className="grid lg:grid-cols-2 gap-4">
      <div className="bg-surface-50 border border-surface-200 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-gray-200">Fundamentals</h3>
        <p className="text-[10px] text-gray-600 mb-3">Revenue, EPS, margin, and free cash flow trends.</p>
        <div className="h-56">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={fundamentals} margin={{ left: -12, right: 8 }}>
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis dataKey="period" tick={{ fill: '#6b7280', fontSize: 10 }} />
              <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} />
              <Tooltip contentStyle={{ background: '#111827', border: '1px solid #374151' }} />
              <Legend wrapperStyle={{ fontSize: 10 }} />
              <Bar dataKey="revenue" fill="#38bdf8" name="Revenue ($B)" />
              <Bar dataKey="fcf" fill="#34d399" name="FCF ($B)" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className="bg-surface-50 border border-surface-200 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-gray-200">Valuation & Peers</h3>
        <p className="text-[10px] text-gray-600 mb-3">Peer snapshot for growth, margins, and forward multiples.</p>
        <div className="h-32 mb-3">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={fundamentals} margin={{ left: -12, right: 8 }}>
              <XAxis dataKey="period" tick={{ fill: '#6b7280', fontSize: 10 }} />
              <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} />
              <Tooltip contentStyle={{ background: '#111827', border: '1px solid #374151' }} />
              <Line type="monotone" dataKey="margin" stroke="#f59e0b" strokeWidth={2} name="Net margin %" />
              <Line type="monotone" dataKey="eps" stroke="#a78bfa" strokeWidth={2} name="EPS" />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="text-gray-500"><tr><th className="text-left py-1">Peer</th><th>Growth</th><th>GM</th><th>Fwd P/E</th><th>EV/S</th><th>Agent</th></tr></thead>
            <tbody className="text-gray-300">
              {peers.map((p) => <tr key={p.symbol} className="border-t border-surface-200"><td className="py-1 font-mono">{p.symbol}</td><td className="text-center">{p.growth}%</td><td className="text-center">{p.grossMargin}%</td><td className="text-center">{p.forwardPE}x</td><td className="text-center">{p.evSales}x</td><td className="text-center">{p.rating}</td></tr>)}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  )
}
