import fixture from '../../../agent-runtime/tests/fixtures/traces/read.v1.json'

export const clone = value => structuredClone(value)
export const graph = () => clone(fixture.expected_graph)
export const traceId = fixture.expected_graph.trace_id
export const token = label => `${label}.${'a'.repeat(43)}`
export const deferred = () => {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
export function change(overrides = {}) {
  return {
    cursor: token('change2'), at: token('graph2'), mode: 'patch', trace_state: 'present',
    ingest_high_watermark: 50, upsert_nodes: [], remove_node_ids: [], upsert_edges: [], remove_edge_ids: [],
    coverage: graph().coverage, ...overrides,
  }
}
export const changes = (rows = [], through = token('through')) => ({
  schema_version: 1, kind: 'trace_changes', trace_id: traceId, projection_generation: graph().projection_generation,
  changes: rows, next_cursor: null, through_cursor: through,
})
export const heartbeat = (through = token('heartbeat')) => ({
  schema_version: 1, kind: 'trace_heartbeat', trace_id: traceId, projection_generation: graph().projection_generation,
  through_cursor: through, freshness: graph().freshness,
})
export const message = row => ({
  schema_version: 1, kind: 'trace_change', trace_id: traceId, projection_generation: graph().projection_generation, change: row,
})
export const readError = code => ({
  schema_version: 1, kind: 'trace_error', code, retained_from: null,
  retryable: code === 'unavailable', resnapshot_required: ['generation_changed', 'history_expired'].includes(code),
})
export { fixture }
