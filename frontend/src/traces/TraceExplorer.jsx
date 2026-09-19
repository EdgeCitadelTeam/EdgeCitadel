import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react'
import { createTraceApi } from './api'
import { createTraceSession } from './session'
import { navigateTrace, useTraceRoute } from './navigation'
import ExecutionMap from './ExecutionMap'
import HistoryBrowser from './HistoryBrowser'
import ObservationInspector, { traceErrorText } from './ObservationInspector'
import { readable } from './layout'
import './trace.css'

const idle = Object.freeze({ mode: 'loading', graph: null, error: null })
const noSubscribe = () => () => {}
const idleSnapshot = () => idle

function Coverage({ coverage }) {
  if (!coverage) return null
  return <div className="trace-coverage" aria-label="Evidence coverage">
    <span>Evidence coverage</span>
    {coverage.partial && <strong>Partial instrumentation</strong>}
    {coverage.catching_up && <strong>Collection catching up</strong>}
    {coverage.gap && <strong>Known evidence gap</strong>}
    {coverage.unknown_sources && <strong>Source set not closed</strong>}
    {coverage.unsupported_families.map(kind => <strong key={kind}>{kind} not supported</strong>)}
    {!coverage.partial && !coverage.catching_up && !coverage.gap && !coverage.unknown_sources && <strong>Reconciled for known sources</strong>}
  </div>
}

function RunList({ api, route, onDenied }) {
  const [page, setPage] = useState({ items: [], next: null, loading: true, error: null })
  const request = useRef(null)
  const load = useCallback(async cursor => {
    request.current?.abort()
    const controller = new AbortController(); request.current = controller
    setPage(old => ({ ...old, loading: true, error: null }))
    try {
      const result = await api.list({ task_id: route.task, cursor, limit: 20 }, controller.signal)
      if (!controller.signal.aborted) setPage({ items: result.items, next: result.next_cursor, loading: false, error: null })
    } catch (error) {
      if (controller.signal.aborted) return
      if (error.code === 'not_authorized') onDenied()
      else setPage({ items: [], next: null, loading: false, error })
    }
  }, [api, route.task, onDenied])
  useEffect(() => { void load(null); return () => request.current?.abort() }, [load])
  return <aside className="trace-run-browser" aria-label="Run browser">
    <div className="trace-run-browser-head"><h2>Runs</h2><button onClick={() => load(null)}>Refresh runs</button></div>
    <p className="trace-muted">Outcomes reflect the list snapshot. Open a run for live evidence.</p>
    {route.task && <div className="trace-note"><p>Runs containing task {route.task}</p><button onClick={() => navigateTrace({ ...route, task: null })}>All runs</button></div>}
    {page.loading && <p role="status">Loading runs…</p>}
    {page.error && <p role="alert">{traceErrorText(page.error)}</p>}
    {!page.loading && !page.error && !page.items.length && <p>{page.next ? 'No matches on this page. Continue scanning older runs.' : 'No matching retained runs. Check collection status or refresh after activity.'}</p>}
    <ul className="trace-run-list">{page.items.map(run => <li key={run.trace_id}><button aria-pressed={route.run === run.trace_id}
      onClick={() => navigateTrace({ run: run.trace_id, task: route.task, step: route.task ? 'task:' + route.task : null })}>
      <strong>{run.root_agent_id ?? 'Owner not observed'}</strong><span>{run.outcome ? readable(run.outcome) : 'Outcome not established'}</span>
      <code>{run.trace_id.slice(0, 12)}</code><span>{run.coverage.partial ? 'Partial evidence' : 'Known-source evidence'}</span>
    </button></li>)}</ul>
    {page.next && <button disabled={page.loading} onClick={() => load(page.next)}>Older runs</button>}
  </aside>
}

function RunView({ api, route, onDenied }) {
  const [session, setSession] = useState(null)
  const [historyOpen, setHistoryOpen] = useState(false)
  useEffect(() => {
    const next = createTraceSession(api)
    setSession(next)
    void next.open(route.run, { at: route.at })
    return () => next.dispose()
  }, [api, route.run, route.at])
  const state = useSyncExternalStore(session?.subscribe ?? noSubscribe, session?.getSnapshot ?? idleSnapshot, idleSnapshot)
  const relevant = state.traceId === route.run && state.historicalAt === route.at
  const graph = relevant ? state.graph : null
  useEffect(() => { if (state.error?.code === 'not_authorized') onDenied() }, [state.error, onDenied])
  const selected = graph?.nodes.find(node => node.id === route.step)
  const select = id => navigateTrace({ ...route, step: id, event: null })
  return <section className="trace-run-view" aria-label="Selected run">
    <div className="trace-run-heading"><div><h2>Execution map</h2><code>{route.run}</code></div><strong role="status">{relevant ? readable(state.mode) : 'Loading'}</strong></div>
    <div className="trace-controls">
      {route.at ? <button onClick={() => navigateTrace({ ...route, at: null, event: null })}>Resume live</button>
        : <button disabled={!graph} onClick={() => navigateTrace({ ...route, at: graph.at, event: null })}>Pause live</button>}
      <button aria-expanded={historyOpen} onClick={() => setHistoryOpen(!historyOpen)}>{historyOpen ? 'Hide retained history' : 'Browse retained history'}</button>
      {state.newerAvailable && <strong>Newer evidence available</strong>}
    </div>
    {historyOpen && <HistoryBrowser api={api} route={route} onDenied={onDenied} />}
    {state.error && <div className="trace-error" role="alert"><p>{traceErrorText(state.error)}</p>
      <button onClick={() => route.at ? navigateTrace({ ...route, at: null, event: null }) : session?.resume()}>Open latest snapshot</button>
      {route.at && state.error.retryable && <button onClick={() => session?.open(route.run, { at: route.at })}>Retry this snapshot</button>}
    </div>}
    {!graph && !state.error && <p className="trace-empty" role="status">{state.traceState === 'expired' || state.traceState === 'absent' ? 'This run is no longer present in the current projection.' : state.progress ? `Loading ${state.progress.loaded} of ${state.progress.total ?? 'unknown'} steps…` : 'Loading execution evidence…'}</p>}
    {graph && <><Coverage coverage={graph.coverage} />
      {route.step && !selected && <p className="trace-note" role="status">The selected step is not present in this snapshot. Its selection is preserved for another view.</p>}
      <div className="trace-workbench"><ExecutionMap graph={graph} selected={route.step} onSelect={select} />
        {selected ? <ObservationInspector key={`${graph.at}/${selected.id}`} api={api} graph={graph} node={selected} route={route} onDenied={onDenied} onSelect={select} />
          : <aside className="trace-inspector"><h2>Follow the evidence</h2><p>Select a step to see its owner, permission decisions, recorded relationships and observations.</p><p className="trace-note">Outcome and evidence coverage are independent. Missing instrumentation is not proof of execution failure.</p></aside>}
      </div>
    </>}
  </section>
}

function ConnectedExplorer({ credential, route, onDenied }) {
  const [api, setApi] = useState(null)
  useEffect(() => {
    const next = createTraceApi(credential); setApi(next)
    return () => next.dispose()
  }, [credential])
  if (!api) return <p role="status">Connecting read access…</p>
  return <div className="trace-content"><RunList api={api} route={route} onDenied={onDenied} />
    {route.run && !route.invalid ? <RunView key={route.run} api={api} route={route} onDenied={onDenied} /> : <section className="trace-empty"><h2>Select a run</h2><p>{route.invalid ? 'The saved address contains an invalid run, step or snapshot. Select a retained run from the list.' : 'Open a run to follow its observed steps and inspect the evidence.'}</p></section>}
  </div>
}

export default function TraceExplorer({ credential, onCredential }) {
  const route = useTraceRoute()
  const [draft, setDraft] = useState('')
  const [error, setError] = useState(null)
  const [theme, setTheme] = useState('dark')
  const onDenied = useCallback(() => {
    onCredential(current => current === credential ? null : current)
    setError('Access denied. Check the fleet read credential and ask your operator to allow this dashboard’s address.')
  }, [credential, onCredential])
  function connect(event) {
    event.preventDefault()
    if (!/^[A-Za-z0-9_-]{32,256}$(?![\s\S])/.test(draft)) { setError('Enter a valid fleet read credential.'); return }
    onCredential(draft); setDraft(''); setError(null)
  }
  return <div className="trace-explorer" data-theme={theme}>
    <header className="trace-heading"><div><h1>Execution map</h1><p>Follow observed execution across the fleet.</p></div><div className="trace-header-actions">
      <button onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? 'Light map theme' : 'Dark map theme'}</button>
      {credential && <button onClick={() => { onCredential(null); setError(null) }}>Disconnect read access</button>}
    </div></header>
    <p className="trace-access-note">Fleet-wide read access. Detailed records remain on their source Edges.</p>
    {credential ? <ConnectedExplorer key={credential} credential={credential} route={route} onDenied={onDenied} /> : <form className="trace-connect" onSubmit={connect}>
      <h2>Connect to execution evidence</h2><p>Use the fleet read credential supplied by your operator. Access stays in memory; enter it again after reloading this page.</p>
      <label>Fleet read credential<input type="password" autoComplete="off" spellCheck={false} value={draft} maxLength={256} onChange={event => setDraft(event.target.value)} required /></label>
      <button type="submit">Connect read access</button>{error && <p role="alert">{error}</p>}
    </form>}
  </div>
}
