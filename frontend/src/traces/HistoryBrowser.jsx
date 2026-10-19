import { useEffect, useState } from 'react'
import { navigateTrace } from './navigation'
import { traceErrorText } from './ObservationInspector'
import { requireProtocol } from './protocol'

function receiptTime(value) {
  const date = new Date(value)
  return value === null || Number.isNaN(date.getTime()) ? 'Core time unavailable' : date.toISOString()
}

export default function HistoryBrowser({ api, route, onDenied }) {
  const [query, setQuery] = useState({ cursor: null, previous: [], snapshot: null, revision: 0 })
  const [page, setPage] = useState({ query: null, loading: true, error: null, result: null })
  useEffect(() => {
    const controller = new AbortController()
    setPage({ query, loading: true, error: null, result: null })
    async function load() {
      try {
        const result = await api.history(route.run, { cursor: query.cursor, limit: 20 }, controller.signal)
        if (controller.signal.aborted) return
        requireProtocol(!query.snapshot || result.snapshot_cursor === query.snapshot)
        requireProtocol(!result.next_cursor || result.next_cursor !== query.cursor)
        setPage({ query, loading: false, error: null, result })
      } catch (error) {
        if (controller.signal.aborted) return
        if (error.code === 'not_authorized') onDenied()
        else setPage({ query, loading: false, error, result: null })
      }
    }
    void load()
    return () => controller.abort()
  }, [api, route.run, query, onDenied])

  // A changed request cannot expose the previous page during effect startup.
  const loading = page.query !== query || page.loading
  const result = loading ? null : page.result
  const error = loading ? null : page.error
  return <section className="trace-history" aria-label="Retained history">
    <div className="trace-history-heading">
      <h3>Retained snapshots</h3>
      <button disabled={loading} onClick={() => setQuery({ cursor: null, previous: [], snapshot: null, revision: query.revision + 1 })}>Refresh history</button>
    </div>
    <p className="trace-muted">Newest first. Times use the Core clock, not execution time. Refresh to include newer evidence; selecting a snapshot pauses the graph.</p>
    {loading && <p role="status">Loading retained history…</p>}
    {error && <p role="alert">{traceErrorText(error)} Refresh history to start a new browse range; the selected graph stays unchanged.</p>}
    {result && <>
      <ol className="trace-history-list">
        {result.items.map(item => <li key={item.position}>
          <button disabled={!item.at} aria-pressed={Boolean(item.at && route.at === item.at)}
            onClick={() => navigateTrace({ ...route, at: item.at, event: null })}>
            <strong>Snapshot {item.position}</strong>
            <span>{receiptTime(item.received_at_ms)}</span>
            {item.is_retained_base && <span>Retained base · earlier history expired</span>}
            {item.trace_state !== 'present' && <span>{item.trace_state === 'expired' ? 'Run expired at this point' : 'Run absent at this point'} · graph unavailable</span>}
          </button>
        </li>)}
      </ol>
      {!result.items.length && <p>{result.next_cursor ? 'No run changes in this scanned range. Continue to older snapshots.' : 'No retained snapshots in this range.'}</p>}
      <div className="trace-pagination">
        <button disabled={!query.previous.length} onClick={() => setQuery({ ...query, cursor: query.previous.at(-1), previous: query.previous.slice(0, -1) })}>Newer history page</button>
        <span>Page {query.previous.length + 1}</span>
        <button disabled={!result.next_cursor} onClick={() => setQuery({ ...query, cursor: result.next_cursor, snapshot: result.snapshot_cursor, previous: [...query.previous, query.cursor ?? result.snapshot_cursor] })}>Older history page</button>
      </div>
      {!result.next_cursor && <p className="trace-muted">Beginning of retained history reached. Earlier evidence may be outside retention.</p>}
    </>}
  </section>
}
