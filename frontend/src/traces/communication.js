import { sha256 } from '@noble/hashes/sha256'
import { bytesToHex } from '@noble/hashes/utils'

export const eventKey = event => `${event.node_id}/${event.source_epoch}/${event.event_id}`
const key = (family, parts) => `${family}:${bytesToHex(sha256(JSON.stringify(parts)))}`
export function eventNodeId(event) {
  const { kind, attributes: a = {} } = event
  if (kind === 'task') return `task:${event.task_id}`
  if (['transport', 'broker', 'infrastructure'].includes(kind)) return key(kind, [event.node_id, event.source_epoch, event.event_id])
  if (kind === 'run') return key(event.execution_attempt_id ? 'attempt' : 'run_observation', [event.node_id, event.source_epoch, event.execution_attempt_id || event.event_id])
  const family = ['model', 'tool'].includes(kind) ? 'span' : kind
  if (!['span', 'permission', 'dispatch'].includes(family)) return null
  return key(family, [event.node_id, event.source_epoch, event.trace_id, event.execution_attempt_id || '', family === 'span' ? event.span_id : a.dispatch_id, ...(kind === 'permission' ? [a.policy] : [])])
}
export function decodeContent(value) {
  if (typeof value !== 'string') return value
  try { return JSON.parse(value) } catch { return value }
}
function requestText(value, depth = 0) {
  if (depth > 4) return null
  const decoded = decodeContent(value)
  if (typeof decoded === 'string') return decoded.trim() || null
  if (decoded && typeof decoded === 'object' && !Array.isArray(decoded)) {
    for (const name of ['title', 'request', 'body', 'prompt', 'input']) {
      const text = requestText(decoded[name], depth + 1)
      if (text) return text
    }
  }
  return null
}
export function shortTaskName(value) {
  const text = requestText(value)
  if (!text) return null
  const words = text.split(/\s+/), name = words.slice(0, 8).join(' ')
  return name.slice(0, 64).trimEnd() + (words.length > 8 || name.length > 64 ? '…' : '')
}
export function bodyTitle(events) {
  for (const event of events) {
    const name = shortTaskName(event.content?.fields)
    if (name) return name
  }
  return null
}
const unique = values => [...new Set(values.filter(value => value !== null && value !== undefined && value !== ''))]
const consensus = values => { const choices = unique(values); return choices.length === 1 ? choices[0] : null }

// Only local sequence and explicit cause edges impose order. Independent sources
// use a stable identity tie-break; this ordering makes no concurrency claim.
export function orderEvents(events) {
  const byKey = new Map(events.map(event => [eventKey(event), event]))
  const outgoing = new Map(), degrees = new Map([...byKey.keys()].map(id => [id, 0]))
  const link = (from, to) => {
    if (!byKey.has(from) || from === to) return
    const edges = outgoing.get(from) ?? new Set()
    if (!edges.has(to)) { edges.add(to); degrees.set(to, degrees.get(to) + 1) }
    outgoing.set(from, edges)
  }
  const sources = new Map()
  for (const event of byKey.values()) {
    const source = `${event.node_id}/${event.source_epoch}`
    const rows = sources.get(source) ?? []; rows.push(event); sources.set(source, rows)
    for (const cause of event.causes ?? []) link(eventKey(cause), eventKey(event))
  }
  for (const rows of sources.values()) {
    rows.sort((a, b) => a.source_seq - b.source_seq || eventKey(a).localeCompare(eventKey(b)))
    rows.slice(1).forEach((event, index) => link(eventKey(rows[index]), eventKey(event)))
  }
  const ready = [...degrees.keys()].filter(id => !degrees.get(id)).sort(), ordered = []
  while (ready.length) {
    const id = ready.shift(); ordered.push(byKey.get(id))
    for (const next of outgoing.get(id) ?? []) {
      degrees.set(next, degrees.get(next) - 1)
      if (!degrees.get(next)) { ready.push(next); ready.sort() }
    }
  }
  const seen = new Set(ordered.map(eventKey))
  const cyclic = [...byKey.keys()].filter(id => !seen.has(id)).sort()
  return { events: [...ordered, ...cyclic.map(id => byKey.get(id))], cyclic }
}

function executionAttempts(events) {
  const attempts = new Map()
  for (const event of events.filter(e => ['run', 'model', 'tool'].includes(e.kind))) {
    const id = JSON.stringify([event.node_id, event.source_epoch, event.execution_attempt_id ?? 'Attempt not recorded'])
    if (!attempts.has(id)) attempts.set(id, { id, events: [], steps: new Map() })
    const attempt = attempts.get(id), stepId = eventNodeId(event)
    attempt.events.push(event)
    if (!attempt.steps.has(stepId)) attempt.steps.set(stepId, { id: stepId, title: event.attributes?.name ?? event.attributes?.model ?? event.kind, events: [] })
    attempt.steps.get(stepId).events.push(event)
  }
  return [...attempts.values()].map(attempt => ({ ...attempt, steps: [...attempt.steps.values()].map(step => ({ ...step,
    state: unique(step.events.map(e => e.phase).filter(phase => !['started', 'requested'].includes(phase))).join(' · ') || 'started',
    durations: unique(step.events.map(e => e.duration_ms)),
  })) }))
}

export function projectCommunication(graph, input) {
  const ordered = orderEvents(input), events = ordered.events
  const agents = new Map(), tasks = new Map(), messages = new Map(), nodeSelection = new Map(), eventSelection = new Map()
  const addAgent = id => { if (id && !agents.has(id)) agents.set(id, { id, title: id, events: [], nodes: [], kind: 'agent' }) }
  const root = graph.nodes.find(node => node.id === `run:${graph.trace_id}`)
  addAgent(root?.agent_id)
  for (const event of events) {
    const id = event.attributes?.message_id
    if (!id) continue
    if (!messages.has(id)) messages.set(id, { id, kind: 'message', events: [] })
    messages.get(id).events.push(event)
  }
  for (const message of messages.values()) {
    const fields = { sender: message.events.map(e => e.attributes?.sender_id), recipient: message.events.map(e => e.attributes?.recipient_id), type: message.events.map(e => e.attributes?.message_type), taskId: message.events.map(e => e.task_id) }
    message.conflicts = Object.keys(fields).filter(name => unique(fields[name]).length > 1)
    for (const [name, values] of Object.entries(fields)) message[name] = consensus(values)
    message.title = `${message.type ?? 'Unknown type'} · ${message.id.slice(0, 8)}`
    message.confirmed = message.events.some(e => ['caller_accepted', 'durably_accepted'].includes(e.phase))
    message.received = message.events.some(e => e.phase === 'received')
    message.failed = message.events.some(e => /failed|rejected/.test(e.phase))
    message.state = message.conflicts.length ? 'Conflicting evidence' : message.failed ? 'Failure recorded' : message.confirmed ? 'Acceptance recorded' : message.received ? 'Receipt recorded' : 'Publication recorded'
    message.drawable = Boolean(message.sender && message.recipient && message.type && !message.conflicts.length)
    if (message.drawable) { addAgent(message.sender); addAgent(message.recipient) }
  }
  const ensureTask = id => {
    if (!id) return null
    if (!tasks.has(id)) tasks.set(id, { id, kind: 'task', events: [], nodes: [], messages: [], attempts: [] })
    return tasks.get(id)
  }
  for (const event of events) { ensureTask(event.task_id)?.events.push(event); if (['run', 'model', 'tool'].includes(event.kind)) addAgent(event.agent_id) }
  for (const node of graph.nodes) { ensureTask(node.task_id)?.nodes.push(node); addAgent(node.agent_id) }
  for (const message of messages.values()) ensureTask(message.taskId)?.messages.push(message)
  for (const task of tasks.values()) {
    const node = task.nodes.find(n => n.kind === 'task')
    task.agentId = node?.agent_id ?? consensus(task.events.filter(e => ['run', 'task'].includes(e.kind)).map(e => e.agent_id))
    // A command's explicit recipient is usable when the task has no owner claim.
    task.agentId ??= consensus(task.messages.filter(m => m.type === 'command' && m.drawable).map(m => m.recipient))
    addAgent(task.agentId)
    task.title = bodyTitle(task.messages.filter(m => m.type === 'command').flatMap(m => m.events)) || shortTaskName(node?.operation) || `Task ${task.id.slice(0, 8)}`
    task.state = node?.state ?? 'Outcome not observed'
    task.conflict = Boolean(node?.conflict)
    task.failures = new Set([...task.nodes.filter(n => n.kind === 'tool' && (n.state === 'failed' || n.original_state === 'failed')).map(n => n.id), ...task.events.filter(e => e.kind === 'tool' && e.phase === 'failed').map(eventNodeId)]).size
    task.attempts = executionAttempts(task.events)
    for (const node of task.nodes) nodeSelection.set(node.id, task)
  }
  for (const agent of agents.values()) {
    agent.events = events.filter(e => e.agent_id === agent.id)
    agent.attempts = executionAttempts(agent.events)
    agent.nodes = graph.nodes.filter(n => n.agent_id === agent.id)
    agent.state = unique(agent.nodes.filter(n => n.kind === 'run' && !n.task_id).map(n => n.state)).join(' · ')
    for (const node of agent.nodes) if (!nodeSelection.has(node.id)) nodeSelection.set(node.id, agent)
  }
  const unassociated = []
  for (const event of events) {
    const message = messages.get(event.attributes?.message_id)
    const selected = message ?? tasks.get(event.task_id) ?? agents.get(event.agent_id)
    if (selected) {
      eventSelection.set(eventKey(event), selected)
      if (eventNodeId(event)) nodeSelection.set(eventNodeId(event), selected)
    }
    if (!selected || (message && !message.drawable)) unassociated.push(event)
  }
  const evidence = { id: 'unassociated', kind: 'evidence', title: 'Unassociated evidence', events: unassociated, nodes: graph.nodes.filter(n => !nodeSelection.has(n.id)) }
  for (const node of evidence.nodes) nodeSelection.set(node.id, evidence)
  for (const event of unassociated) if (!eventSelection.has(eventKey(event))) eventSelection.set(eventKey(event), evidence)
  // Task grouping supplies a reading order, not a timeline across hosts.
  const interactions = [], placed = new Set()
  function appendTask(task) {
    const visible = task.messages.filter(m => m.drawable && m.type !== 'progress')
    const commands = visible.filter(m => m.type === 'command'), rest = visible.filter(m => m.type !== 'command')
    for (const message of commands) interactions.push({ id: message.id, message })
    if (commands.length) interactions[interactions.length - 1].task = task
    else interactions.push({ id: `task:${task.id}`, task })
    for (const message of rest) interactions.push({ id: message.id, message })
  }
  for (const message of messages.values()) {
    const task = tasks.get(message.taskId)
    if (task) { if (!placed.has(task.id)) { placed.add(task.id); appendTask(task) } }
    else if (message.drawable) interactions.push({ id: message.id, message })
  }
  for (const task of tasks.values()) if (!placed.has(task.id)) appendTask(task)
  const flows = new Map()
  for (const message of messages.values()) {
    if (!message.drawable || message.type === 'progress') continue
    const id = key('flow', [message.sender, message.recipient, message.type])
    if (!flows.has(id)) flows.set(id, { id, kind: 'flow', sender: message.sender, recipient: message.recipient, type: message.type, messages: [], events: [] })
    flows.get(id).messages.push(message)
    flows.get(id).events.push(...message.events)
  }
  for (const flow of flows.values()) {
    const name = flow.type === 'command' ? 'request' : flow.type === 'result' ? 'return' : flow.type
    flow.title = `${flow.messages.length} ${name}${flow.messages.length === 1 ? '' : 's'}`
  }
  return { flows: [...flows.values()], agents: [...agents.values()], tasks: [...tasks.values()], messages: [...messages.values()], interactions, events, evidence, nodeSelection, eventSelection, cycles: ordered.cyclic, graphEdges: graph.edges }
}
