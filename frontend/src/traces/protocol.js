import Ajv from 'ajv/dist/2020'
import addFormats from 'ajv-formats'
import readSchema from '../../../schemas/trace-read.v1.json'
import eventSchema from '../../../schemas/trace-event.v1.json'

export const MAX_RESPONSE_BYTES = 2 * 1024 * 1024
const ajv = new Ajv({ strict: false, allErrors: false })
addFormats(ajv)
ajv.addSchema(eventSchema)
const validate = ajv.compile(readSchema)

// Fixed diagnostics never expose response text, credentials or private records.
export class TraceReadError extends Error {
  constructor(code, { retryable = false, resnapshotRequired = false, retainedFrom = null } = {}) {
    super(code)
    this.name = 'TraceReadError'
    this.code = code
    this.retryable = retryable
    this.resnapshotRequired = resnapshotRequired
    this.retainedFrom = retainedFrom
  }
}

export function requireProtocol(condition) {
  if (!condition) throw new TraceReadError('invalid_response')
}

export function uniqueRows(rows) {
  const result = new Map(rows.map(row => [row.id, row]))
  requireProtocol(result.size === rows.length)
  return result
}

export function validateGraph(nodes, edges) {
  const ids = uniqueRows(nodes)
  uniqueRows(edges)
  const parents = new Map()
  for (const edge of edges) {
    requireProtocol(ids.has(edge.from) && ids.has(edge.to))
    if (edge.kind === 'join' || edge.status !== 'resolved') continue
    requireProtocol(!parents.has(edge.to) || parents.get(edge.to) === edge.from)
    parents.set(edge.to, edge.from)
  }
  const finished = new Set()
  for (const start of parents.keys()) {
    const path = new Set()
    let current = start
    while (parents.has(current) && !finished.has(current)) {
      requireProtocol(!path.has(current))
      path.add(current)
      current = parents.get(current)
    }
    for (const id of path) finished.add(id)
  }
}

export function readResponse(value, { kind, traceId, generation } = {}) {
  requireProtocol(validate(value))
  if (value.kind === 'trace_error') {
    requireProtocol(value.resnapshot_required === ['generation_changed', 'history_expired'].includes(value.code))
    throw new TraceReadError(value.code, {
      retryable: value.retryable,
      resnapshotRequired: value.resnapshot_required,
      retainedFrom: value.retained_from,
    })
  }
  requireProtocol(!kind || value.kind === kind)
  requireProtocol(!traceId || value.trace_id === traceId)
  if (generation && value.projection_generation !== generation) {
    throw new TraceReadError('generation_changed', { resnapshotRequired: true })
  }
  if (value.kind === 'trace_graph') {
    validateGraph(value.nodes, value.edges)
    const ids = new Set(value.nodes.map(node => node.id))
    requireProtocol(value.expansions.every(item => ids.has(item.node_id)))
    requireProtocol(value.total_nodes === null || value.total_nodes >= ids.size)
    requireProtocol(value.page_kind !== 'snapshot' || value.total_nodes === null ||
      value.total_nodes === ids.size || value.expansions.length > 0)
    for (const node of value.nodes) {
      requireProtocol(!node.conflict || node.outcome_candidate_count >= 2)
      if (node.kind === 'task' && node.original_state !== null) {
        requireProtocol(node.state === (node.original_state === 'cancelled' ? 'canceled' : node.original_state))
      }
    }
  }
  if (value.kind === 'trace_changes') {
    requireProtocol(value.next_cursor === null || value.next_cursor === value.through_cursor)
  }
  if (value.kind === 'trace_history') {
    requireProtocol(value.retained_from <= value.upper_position)
    let previous = value.upper_position + 1
    for (const item of value.items) {
      requireProtocol(item.position >= value.retained_from && item.position < previous)
      requireProtocol((item.trace_state === 'present') === (item.at !== null))
      requireProtocol(item.is_retained_base === (item.position === value.retained_from))
      previous = item.position
    }
  }
  if (value.kind === 'trace_events') {
    requireProtocol(value.events.every(event => event.trace_id === value.trace_id))
  }
  const changes = value.kind === 'trace_changes' ? value.changes :
    value.kind === 'trace_change' ? [value.change] : []
  for (const change of changes) {
    requireProtocol(change.mode === 'clear'
      ? change.trace_state !== 'present' && change.at === null
      : change.trace_state === 'present' && change.at !== null)
    if (change.mode !== 'patch') {
      requireProtocol(['upsert_nodes', 'upsert_edges', 'remove_node_ids', 'remove_edge_ids']
        .every(key => change[key].length === 0))
    }
    uniqueRows(change.upsert_nodes)
    uniqueRows(change.upsert_edges)
  }
  return value
}
