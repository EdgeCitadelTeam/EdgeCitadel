import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react'
import { createTraceApi } from './api'
import { createTraceSession } from './session'
import { navigateTrace, useTraceRoute } from './navigation'
import CommunicationMap from './CommunicationMap'
import CommunicationDetails from './CommunicationDetails'
import { eventKey } from './communication'
import { useCommunicationSnapshot } from './useCommunicationSnapshot'
import HistoryBrowser from './HistoryBrowser'
import { traceErrorText } from './ObservationInspector'
import { readable } from './layout'
import './trace.css'
import registryApi from '../api/client'

const idle = Object.freeze({ mode: 'loading', graph: null, error: null })
const noSubscribe = () => () => {}
const idleSnapshot = () => idle

function Coverage({ coverage, nodes }) {
  if (!coverage) return null
  return <div className="trace-coverage" aria-label="Evidence coverage">
    <span>Evidence coverage</span>
    <details className="trace-family-coverage"><summary>Coverage by family</summary>
      <table><thead><tr><th>Family</th><th>Retained observations</th><th>Instrumentation</th></tr></thead><tbody>
        {['run', 'task', 'dispatch', 'permission', 'model', 'tool', 'transport', 'broker'].map(family => {
          const count = nodes.filter(node => node.kind === family).length
          return <tr key={family}><th>{readable(family)}</th><td>{count ? `${count} loaded steps` : 'Not observed in loaded graph'}</td><td>{coverage.unsupported_families.includes(family) ? 'Not instrumented on a reporting source' : 'Completeness unknown'}</td></tr>
        })}
      </tbody></table>
      <p>Counts reflect the selected snapshot and loaded branches. Missing instrumentation is not successful execution.</p>
      {coverage.gap && <p>Lost or rejected evidence is reported for this run; its family cannot be established.</p>}
      <p>Expired payloads are reported as expired history. Unconfigured infrastructure collectors report a separate not-configured observation.</p>
    </details>
    {coverage.partial && <strong>Partial instrumentation</strong>}
    {coverage.catching_up && <strong>Collection catching up</strong>}
    {coverage.gap && <strong>Known evidence gap</strong>}
    {coverage.unknown_sources && <strong>Source set not closed</strong>}
    {coverage.unsupported_families.map(kind => <strong key={kind}>{kind} not supported</strong>)}
    {!coverage.partial && !coverage.catching_up && !coverage.gap && !coverage.unknown_sources && <strong>Reconciled for known sources</strong>}
  </div>
}

function Freshness({ freshness }) {
  if (!freshness) return null
  const label = {
    unavailable: 'Collection unavailable — the global view may be stale.',
    unknown: 'Collection availability has not been observed.',
    collecting: 'Collector connected. Source coverage is reported separately.',
  }[freshness.collector_state]
  return <div className="trace-coverage" aria-label="Collection status" role="status">
    <strong>{label}</strong>
    {freshness.projection_cursor < freshness.ingest_cursor && <span>Projection catching up with collected evidence.</span>}
    <span>Collection status at last check; independent of the selected snapshot and execution outcome.</span>
  </div>
}

function RunList({ api, route, onDenied }) {
  const [historyOpen, setHistoryOpen] = useState(false)
  const [page, setPage] = useState({ items: [], next: null, loading: true, error: null })
  const request = useRef(null)
  const load = useCallback(async cursor => {
    request.current?.abort()
    const controller = new AbortController(); request.current = controller
    setPage(old => ({ ...old, loading: true, error: null }))
    try {
      const result = await api.list({ cursor, limit: 20 }, controller.signal)
      if (!controller.signal.aborted) setPage(old => ({ items: cursor ? [...old.items, ...result.items] : result.items, next: result.next_cursor, loading: false, error: null }))
    } catch (error) {
      if (controller.signal.aborted) return
      if (error.code === 'not_authorized') onDenied()
      else setPage({ items: [], next: null, loading: false, error })
    }
  }, [api, onDenied])
  useEffect(() => { void load(null); return () => request.current?.abort() }, [load])
  return <aside className="trace-run-browser" aria-label="Run browser">
    <div className="trace-run-browser-head"><h2>Trace history</h2><button onClick={() => load(null)}>Refresh runs</button></div>
    <p className="trace-muted">Select a trace to follow its conversation.</p>
    {page.loading && <p role="status">Loading runs…</p>}
    {page.error && <p role="alert">{traceErrorText(page.error)}</p>}
    {!page.loading && !page.error && !page.items.length && <p>{page.next ? 'No matches on this page. Continue scanning older runs.' : 'No retained runs. Check collection status or refresh after activity.'}</p>}
    <ul className="trace-run-list">{page.items.map(run => <li key={run.trace_id}><button aria-pressed={route.run === run.trace_id}
      onClick={() => navigateTrace({ run: run.trace_id })}>
      <strong>{run.task_name}</strong><span>{run.root_agent_id ?? 'Owner not observed'}</span><span>{run.outcome ? readable(run.outcome) : 'Outcome not established'}</span>
      <code>{run.trace_id.slice(0, 12)}</code><span>{run.coverage.partial ? 'Partial evidence' : 'Known-source evidence'}</span>
    </button></li>)}</ul>
    {page.next && <button disabled={page.loading} onClick={() => load(page.next)}>Load more runs</button>}
    {route.run && !route.invalid && <div className="flow-history-snapshots"><button aria-expanded={historyOpen} onClick={() => setHistoryOpen(!historyOpen)}>{historyOpen ? 'Hide retained history' : 'Browse retained history'}</button>
      {historyOpen && <HistoryBrowser key={route.run} api={api} route={route} onDenied={onDenied} />}</div>}
  </aside>
}

function RunView({ api, route, onDenied }) {
  const [session, setSession] = useState(null)
  const [topology, setTopology] = useState(null)
  const [topologySelection, setTopologySelection] = useState(null)
  useEffect(() => {
    let active = true
    registryApi.getRegistry().then(rows => {
      if (active) setTopology({ at: new Date().toISOString(), agents: rows.map(row => {
        const metadata = row.card?.metadata ?? {}
        return { id: row.agent_id, nodeId: metadata['edgecitadel.node_id'], hostName: metadata['edgecitadel.host_name'], mode: metadata['edgecitadel.messaging_mode'], leaf: metadata['edgecitadel.leaf_id'], domain: metadata['edgecitadel.jetstream_domain'], description: row.card?.description, state: row.agent_state, address: metadata['edgecitadel.nats_address'], coreAddress: metadata['edgecitadel.core_address'] }
      }) })
    }).catch(() => { if (active) setTopology({ agents: [], error: 'Current registry unavailable.' }) })
    return () => { active = false }
  }, [])
  const [diagnostics, setDiagnostics] = useState(false)
  useEffect(() => {
    const next = createTraceSession(api)
    setSession(next)
    void next.open(route.run, { at: route.at })
    return () => next.dispose()
  }, [api, route.run, route.at])
  const state = useSyncExternalStore(session?.subscribe ?? noSubscribe, session?.getSnapshot ?? idleSnapshot, idleSnapshot)
  const relevant = state.traceId === route.run && state.historicalAt === route.at
  const snapshot = useCommunicationSnapshot(api, relevant ? state.graph : null, onDenied)
  const graph = relevant && state.graph ? snapshot.view?.graph : null
  const projection = snapshot.view?.projection
  const [expanded, setExpanded] = useState(false)
  const returnFocus = useRef(null)
  useEffect(() => { if (state.error?.code === 'not_authorized') onDenied() }, [state.error, onDenied])
  const linkedSelection = projection && (projection.eventSelection.get(route.event) || projection.messages.find(message => message.id === route.message) || projection.nodeSelection.get(route.step) || projection.flows.find(flow => flow.id === route.step) || projection.agents.find(agent => `agent:${agent.id}` === route.step) || (route.step === 'unassociated' ? projection.evidence : null))
  const selected = topologySelection ?? (linkedSelection?.kind === 'agent' ? { ...linkedSelection, topologyCard: topology?.agents.find(agent => agent.id === linkedSelection.id), topologyAt: topology?.at } : linkedSelection)
  const select = (item, element) => {
    setTopologySelection(item.kind === 'topology' ? item : null)
    if (element?.closest('.communication-map')) returnFocus.current = element
    navigateTrace({ ...route, message: item.kind === 'message' ? item.id : null, step: item.kind === 'task' ? `task:${item.id}` : item.kind === 'agent' ? `agent:${item.id}` : item.kind === 'evidence' ? 'unassociated' : item.kind === 'flow' ? item.id : null, event: null })
  }
  const closeDetails = () => {
    setTopologySelection(null)
    navigateTrace({ ...route, step: null, message: null, event: null })
    const selectedCard = selected?.kind === 'message' ? [...document.querySelectorAll('[data-message-id]')].find(element => element.dataset.messageId === selected.id) : null
    const target = selectedCard ?? (returnFocus.current?.isConnected ? returnFocus.current : document.querySelector('.flow-agent'))
    target?.focus({ preventScroll: true })
  }
  return <section className="trace-run-view" aria-label="Selected run">
    <div className="flow-run-toolbar"><span className="flow-live-state" role="status">{route.at ? 'Snapshot' : relevant ? readable(state.mode) : 'Loading'}</span>
      <details className="flow-run-options"><summary>Run options</summary><div className="flow-options-menu">
        <code>{route.run}</code>
        {route.at ? <button onClick={() => navigateTrace({ ...route, at: null, event: null })}>Resume live</button> : <button disabled={!graph} onClick={() => navigateTrace({ ...route, at: graph.at, event: null })}>Pause live</button>}
        <button aria-expanded={diagnostics} onClick={() => setDiagnostics(!diagnostics)}>Diagnostics</button>
      </div></details>
      {state.newerAvailable && <span>Newer evidence available</span>}
    </div>
    {snapshot.error && <p role="alert">{traceErrorText(snapshot.error)} <button onClick={snapshot.more}>Retry evidence</button></p>}
    {state.error && <div className="trace-error" role="alert"><p>{traceErrorText(state.error)}</p>
      <button onClick={() => route.at ? navigateTrace({ ...route, at: null, event: null }) : session?.resume()}>Open latest snapshot</button>
      {route.at && state.error.retryable && <button onClick={() => session?.open(route.run, { at: route.at })}>Retry this snapshot</button>}
    </div>}
    {!graph && !state.error && <p className="trace-empty" role="status">{state.traceState === 'expired' || state.traceState === 'absent' ? 'This run is no longer present in the current projection.' : state.progress ? `Loading ${state.progress.loaded} of ${state.progress.total ?? 'unknown'} steps…` : 'Loading execution evidence…'}</p>}
    {graph && <>
      {state.freshness?.collector_state === 'unavailable' && <p className="trace-note" role="status">Collection unavailable — this view may be stale.</p>}
      {diagnostics && <div className="flow-diagnostics"><Freshness freshness={state.freshness} /><Coverage coverage={graph.coverage} nodes={graph.nodes} /><p>{projection.events.length} observations loaded</p></div>}
      {(route.step || route.message || route.event) && !selected && <p className="trace-note" role="status">The selected step is not present in this snapshot. Its selection is preserved for another view.</p>}
      {(snapshot.loading || snapshot.view.next) && <p className="trace-muted" role="status">{snapshot.loading ? 'Loading communication…' : 'More evidence available'}{snapshot.view.next && <button disabled={snapshot.loading} onClick={snapshot.more}>Continue loading evidence</button>}</p>}
      {selected && <button className="flow-detail-backdrop" aria-label="Close details backdrop" tabIndex={-1} onClick={closeDetails} />}
      <div className={`communication-workbench${selected ? ' has-details' : ''}${expanded ? ' expanded' : ''}`}><CommunicationMap topology={topology} projection={projection} selected={selected} onSelect={select} />
        {selected && <CommunicationDetails key={`${route.run}/${route.at ?? 'live'}`} selection={selected} projection={projection} onNavigate={direction => {
          const interactions = projection.interactions
          const index = interactions.findIndex(row => (row.message ?? row.task).id === selected.id)
          const next = direction === 'first' ? 0 : direction === 'last' ? interactions.length - 1 : index + direction
          const row = interactions[next]
          if (row) select(row.message ?? row.task)
        }} onSelect={select} route={route} expanded={expanded} onExpand={() => setExpanded(!expanded)} onClose={closeDetails} partial={snapshot.loading || Boolean(snapshot.view.next)} onEvent={event => navigateTrace({ ...route, event: eventKey(event) })} />}
      </div>
    </>}
  </section>
}

function ConnectedExplorer({ route, onDenied }) {
  const [api, setApi] = useState(null)
  useEffect(() => {
    const next = createTraceApi(); setApi(next)
    return () => next.dispose()
  }, [])
  if (!api) return <p role="status">Connecting read access…</p>
  return <div className="trace-content flow-content"><RunList api={api} route={route} onDenied={onDenied} />
    {route.run && !route.invalid ? <RunView key={`${route.run}/${route.at ?? "live"}`} api={api} route={route} onDenied={onDenied} /> : <section className="trace-empty"><h2>Select a run</h2><p>{route.invalid ? 'The saved address contains an invalid run, step or snapshot. Select a retained run from the list.' : 'Open a run to follow its observed steps and inspect the evidence.'}</p></section>}
  </div>
}

export default function TraceExplorer() {
  const route = useTraceRoute()
  const [error, setError] = useState(null)
  const [theme, setTheme] = useState('dark')
  const [historyVisible, setHistoryVisible] = useState(false)
  useEffect(() => { setHistoryVisible(false) }, [route.run, route.at])
  const onDenied = useCallback(() => setError('Execution data is unavailable. Check access to this dashboard.'), [])
  return <div className={`trace-explorer${historyVisible ? ' history-visible' : ''}`} data-theme={theme}>
    <header className="trace-heading"><div><h1>Agent flow</h1></div><div className="trace-header-actions">
      <button className="flow-history-toggle" aria-expanded={historyVisible} onClick={() => setHistoryVisible(!historyVisible)}>Trace history</button>
      <button onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? 'Light map theme' : 'Dark map theme'}</button>
    </div></header>
    {error && <p role="alert">{error}</p>}
    {!error && <ConnectedExplorer route={route} onDenied={onDenied} />}
  </div>
}
