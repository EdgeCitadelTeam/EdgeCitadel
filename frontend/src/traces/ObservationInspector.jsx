import { useEffect, useState } from 'react'
import { navigateTrace } from './navigation'
import { nodeTitle, readable } from './layout'

export const eventKey = event => `${event.node_id}/${event.source_epoch}/${event.event_id}`
export const traceErrorText = error => ({
  not_authorized: 'Access denied. Check the fleet read credential and this dashboard’s allowed address.',
  not_found: 'This run is not available in the selected snapshot.',
  history_expired: 'This snapshot is outside the retained history. Open the latest run to continue.',
  generation_changed: 'The Core rebuilt its projection. Open a new snapshot to continue.',
  cursor_scope_mismatch: 'This saved position belongs to different filters or access. Start from a new snapshot.',
  invalid_cursor: 'This saved position is invalid. Start from a new snapshot.',
  unavailable: 'The Core is unavailable or still collecting data. Try again.',
  oversize_response: 'This response exceeds the supported size. Ask your operator to inspect the run.',
  invalid_response: 'The Core returned inconsistent evidence. The view has stopped updating.',
  invalid_request: 'The requested run or filter is invalid.',
}[error?.code] || 'The evidence could not be loaded. Try again.')

export default function ObservationInspector({ api, graph, node, route, onDenied, onSelect }) {
  const [page, setPage] = useState({ events: [], next: null, loading: true, error: null })
  const [loadCursor, setLoadCursor] = useState(null)
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    const controller = new AbortController()
    async function load() {
      setPage(old => ({ ...old, loading: true, error: null }))
      try {
        const response = await api.events(graph.trace_id, { as_of: graph.at, node_id: node.id, after: loadCursor, limit: 100 }, controller.signal)
        if (controller.signal.aborted) return
        if (response.as_of !== graph.at || response.projection_generation !== graph.projection_generation) throw { code: 'invalid_response' }
        setPage(old => ({ events: loadCursor ? [...old.events, ...response.events] : response.events, next: response.next_cursor, loading: false, error: null }))
      } catch (error) {
        if (controller.signal.aborted) return
        if (error.code === 'not_authorized') onDenied()
        else setPage(old => ({ ...old, loading: false, error }))
      }
    }
    void load()
    return () => controller.abort()
  }, [api, graph.trace_id, graph.at, graph.projection_generation, node.id, loadCursor, attempt, onDenied])
  const selectedEvent = page.events.find(event => eventKey(event) === route.event)
  // Reloading an event link follows bounded pages until its exact source tuple
  // is found. A view change unmounts this inspector and cancels the search.
  useEffect(() => {
    if (route.event && !selectedEvent && page.next && !page.loading && !page.error) setLoadCursor(page.next)
  }, [route.event, selectedEvent, page.next, page.loading, page.error])
  const relationships = graph.edges.filter(edge => edge.from === node.id || edge.to === node.id)
  return <aside className="trace-inspector" aria-label="Selected step details">
    <p className="trace-muted">Selected step</p><h2>{nodeTitle(node, graph.trace_id)}</h2>
    <p className="trace-owner">{node.agent_id ?? 'Owner not observed'}</p>
    <p className={`trace-state state-${node.state}`}>{readable(node.state)}{node.conflict ? ' · conflicting terminal evidence' : ''}</p>
    {['permission', 'dispatch'].includes(node.kind) && <p className="trace-note">An allowed decision is authorization evidence, not proof that a tool or task executed.</p>}
    <dl>
      <dt>Evidence</dt><dd>{readable(node.evidence_kind)}</dd>
      <dt>Operation / policy</dt><dd>{node.operation ?? 'Not reported'}</dd>
      <dt>Task</dt><dd>{node.task_id ?? 'Native observation without a task'}</dd>
      <dt>Outcome candidates</dt><dd>{node.outcome_candidate_count}</dd>
    </dl>
    {relationships.length > 0 && <section><h3>Recorded relationships</h3><ul className="trace-relations">
      {relationships.map(edge => {
        const other = graph.nodes.find(item => item.id === (edge.from === node.id ? edge.to : edge.from))
        return <li key={edge.id}><span>{edge.from === node.id ? 'To' : 'From'} · {readable(edge.kind)} · {edge.status}</span>
          <button onClick={() => onSelect(other.id)}>{nodeTitle(other, graph.trace_id)} · {other.agent_id ?? 'Owner unknown'}</button></li>
      })}
    </ul></section>}
    <section><h3>Observations at this snapshot</h3>
      <p className="trace-muted">Source times may differ across hosts. Detailed content stays on the source Edge.</p>
      {page.loading && <p role="status">Loading observations…</p>}
      {page.error && <div role="alert"><p>{traceErrorText(page.error)}</p><button onClick={() => setAttempt(attempt + 1)}>Retry observations</button></div>}
      {!page.loading && page.events.length === 0 && !page.error && <p>{page.next ? 'No matching observations on this page.' : 'No retained observations for this step.'}</p>}
      <ol className="trace-observations">{page.events.map(event => <li key={eventKey(event)}>
        <button aria-pressed={route.event === eventKey(event)} onClick={() => navigateTrace({ ...route, event: eventKey(event) })}>
          <strong>{readable(event.phase)}</strong><span>{event.node_id} · {readable(event.evidence_kind)}</span><time>{event.occurred_at}</time>
        </button>
      </li>)}</ol>
      {page.next && !page.loading && <button onClick={() => setLoadCursor(page.next)}>More observations</button>}
      {route.event && !selectedEvent && !page.loading && !page.next && <p role="status">The linked observation is not present in this snapshot.</p>}
      {selectedEvent && <section className="trace-event-detail" aria-label="Observation details">
        <h3>{readable(selectedEvent.kind)} · {readable(selectedEvent.phase)}</h3>
        <dl><dt>Occurred at (source clock)</dt><dd>{selectedEvent.occurred_at}</dd>
          <dt>Collection time</dt><dd>Not available</dd>
          <dt>Duration</dt><dd>{selectedEvent.duration_ms === null ? 'Not reported' : `${selectedEvent.duration_ms} ms`}</dd>
          <dt>Execution attempt</dt><dd>{selectedEvent.execution_attempt_id ?? 'Not reported'}</dd>
          <dt>Source / epoch</dt><dd>{selectedEvent.node_id} / {selectedEvent.source_epoch}</dd>
          <dt>Skill ID</dt><dd>{selectedEvent.attributes.skill_id ?? 'Not reported'}</dd>
          <dt>Grant version</dt><dd>{selectedEvent.attributes.grant_version ?? 'Not reported'}</dd>
          <dt>Policy version</dt><dd>{selectedEvent.attributes.policy_version ?? 'Not reported'}</dd>
          <dt>Local record</dt><dd>{selectedEvent.attributes.local_content_ref ? 'Local-only reference; not fetched by this dashboard' : 'Not reported'}</dd>
          <dt>Usage</dt><dd>{selectedEvent.attributes.input_tokens !== undefined || selectedEvent.attributes.output_tokens !== undefined
            ? `Input: ${selectedEvent.attributes.input_tokens ?? 'unknown'}; output: ${selectedEvent.attributes.output_tokens ?? 'unknown'}` : 'Not reported'}</dd>
        </dl>
        <details><summary>Structured evidence</summary><pre>{JSON.stringify(selectedEvent, null, 2)}</pre></details>
      </section>}
    </section>
    <details><summary>Step identity</summary><code>{node.id}</code></details>
  </aside>
}
