import { AlertTriangle, Database, Link as LinkIcon } from 'lucide-react'

export default function ResearchProvenancePanel({ analysis }) {
  const sources = analysis?.sources || []
  return (
    <section className="bg-surface-50 border border-surface-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-200 flex items-center gap-2"><Database size={14} /> Provenance</h3>
      <p className="text-[10px] text-gray-600 mt-1">Source and data-freshness trail for the latest research output.</p>
      <div className="mt-3 space-y-2">
        {sources.map((source) => (
          <div key={`${source.title}-${source.type}`} className="bg-surface-100/60 rounded-lg p-2 text-xs">
            <div className="flex items-center gap-2 text-gray-300"><LinkIcon size={12} /> {source.title}</div>
            <div className="mt-1 text-gray-500">{source.publisher} · {source.type} · {source.published_at}</div>
          </div>
        ))}
      </div>
      <div className="mt-3 rounded-lg border border-yellow-500/20 bg-yellow-500/10 p-2 text-xs text-yellow-200 flex gap-2">
        <AlertTriangle size={13} className="shrink-0 mt-0.5" />
        Live provider integration is pending; values in this PR are demo fixtures that establish the UI contract.
      </div>
    </section>
  )
}
