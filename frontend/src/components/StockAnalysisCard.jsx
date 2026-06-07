import clsx from 'clsx'

function ratingClass(rating) {
  if (/buy/i.test(rating)) return 'bg-green-500/20 text-green-300'
  if (/sell|under/i.test(rating)) return 'bg-red-500/20 text-red-300'
  if (/hold|neutral/i.test(rating)) return 'bg-yellow-500/20 text-yellow-300'
  return 'bg-blue-500/20 text-blue-300'
}

export default function StockAnalysisCard({ analysis }) {
  if (!analysis) return null
  return (
    <section className="bg-surface-50 border border-surface-200 rounded-xl p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-gray-200">Agent Investment View</h3>
          <p className="text-[10px] text-gray-600">Structured thesis card generated from the stock-analysis schema.</p>
        </div>
        <span className={clsx('px-2 py-1 rounded text-xs font-semibold', ratingClass(analysis.rating))}>{analysis.rating}</span>
      </div>
      <div className="grid grid-cols-3 gap-3 mt-4">
        <Kpi label="Target" value={`$${analysis.target_price}`} />
        <Kpi label="Upside" value={`${analysis.upside_percent}%`} positive />
        <Kpi label="Confidence" value={`${analysis.confidence}%`} />
      </div>
      <p className="mt-4 text-sm text-gray-300 leading-relaxed">{analysis.summary}</p>
      <div className="mt-4 bg-surface-100/60 rounded-lg p-3">
        <div className="text-xs text-gray-500 mb-1">Base thesis</div>
        <div className="text-sm text-gray-300">{analysis.thesis}</div>
      </div>
      <div className="grid md:grid-cols-3 gap-3 mt-4 text-xs">
        <List title="Key drivers" items={analysis.key_drivers} color="text-green-300" />
        <List title="Risks" items={analysis.risks} color="text-red-300" />
        <List title="Catalysts" items={analysis.catalysts} color="text-sky-300" />
      </div>
    </section>
  )
}

function Kpi({ label, value, positive }) {
  return <div className="bg-surface-100 rounded-lg p-3"><div className="text-[10px] text-gray-500 uppercase tracking-wide">{label}</div><div className={clsx('mt-1 text-lg font-mono', positive ? 'text-green-300' : 'text-gray-100')}>{value}</div></div>
}

function List({ title, items = [], color }) {
  return <div><div className={clsx('font-medium mb-1', color)}>{title}</div><ul className="space-y-1 text-gray-400 list-disc list-inside">{items.map((item) => <li key={item}>{item}</li>)}</ul></div>
}
