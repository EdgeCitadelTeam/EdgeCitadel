import clsx from 'clsx'
import { positions } from '../data/financeMockData'

export default function PortfolioWorkspace() {
  const totalWeight = positions.reduce((sum, p) => sum + p.weight, 0)
  return (
    <div className="flex-1 overflow-y-auto p-3 md:p-4 space-y-4">
      <section className="grid md:grid-cols-4 gap-3">
        <Metric label="Market value" value="$1,002,400" />
        <Metric label="Daily P&L" value="+$8,420" positive />
        <Metric label="Beta" value="1.14" />
        <Metric label="Max drawdown" value="-11.8%" negative />
      </section>
      <section className="bg-surface-50 border border-surface-200 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-gray-200">Positions</h3>
        <p className="text-[10px] text-gray-600 mb-3">Portfolio risk view establishes the UI contract for live positions and scenarios.</p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="text-gray-500"><tr><th className="text-left py-2">Symbol</th><th className="text-right">Weight</th><th className="text-right">Value</th><th className="text-right">P&L</th><th className="text-right">Risk</th></tr></thead>
            <tbody>
              {positions.map((position) => (
                <tr key={position.symbol} className="border-t border-surface-200 text-gray-300">
                  <td className="py-2 font-mono">{position.symbol}</td>
                  <td className="text-right">{position.weight}%</td>
                  <td className="text-right">{position.marketValue}</td>
                  <td className={clsx('text-right', position.pnl >= 0 ? 'text-green-400' : 'text-red-400')}>{position.pnl >= 0 ? '+' : ''}{position.pnl}%</td>
                  <td className="text-right">{position.risk}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      <section className="grid md:grid-cols-2 gap-4">
        <div className="bg-surface-50 border border-surface-200 rounded-xl p-4">
          <h3 className="text-sm font-semibold text-gray-200 mb-3">Concentration</h3>
          <div className="space-y-2">
            {positions.map((p) => <div key={p.symbol}><div className="flex justify-between text-xs text-gray-400"><span>{p.symbol}</span><span>{p.weight}%</span></div><div className="h-2 bg-surface-100 rounded"><div className="h-2 bg-accent rounded" style={{ width: `${(p.weight / totalWeight) * 100}%` }} /></div></div>)}
          </div>
        </div>
        <div className="bg-surface-50 border border-surface-200 rounded-xl p-4">
          <h3 className="text-sm font-semibold text-gray-200 mb-3">Scenario Analysis</h3>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <Scenario name="Rates +50 bps" impact="-2.4%" />
            <Scenario name="AI capex slowdown" impact="-6.8%" severe />
            <Scenario name="USD -3%" impact="+1.1%" positive />
            <Scenario name="Oil shock" impact="-0.9%" />
          </div>
        </div>
      </section>
    </div>
  )
}

function Metric({ label, value, positive, negative }) {
  return <div className="bg-surface-50 border border-surface-200 rounded-xl p-4"><div className="text-xs text-gray-500">{label}</div><div className={clsx('mt-2 text-2xl font-mono', positive ? 'text-green-400' : negative ? 'text-red-400' : 'text-gray-100')}>{value}</div></div>
}

function Scenario({ name, impact, positive, severe }) {
  return <div className={clsx('rounded-lg p-3 border', positive ? 'bg-green-500/10 border-green-500/20 text-green-300' : severe ? 'bg-red-500/10 border-red-500/20 text-red-300' : 'bg-surface-100 border-surface-200 text-gray-300')}><div>{name}</div><div className="mt-1 font-mono">{impact}</div></div>
}
