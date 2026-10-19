import { sha256 } from '@noble/hashes/sha256'
import { bytesToHex } from '@noble/hashes/utils'

export const TERMINAL_STATES = new Set([
  'completed',
  'failed',
  'canceled',
  'rejected',
])

export class TaskObservationError extends Error {
  constructor(message) {
    super(message)
    this.name = 'TaskObservationError'
  }
}

export function canonicalJson(value) {
  if (
    value === null ||
    typeof value === 'boolean' ||
    typeof value === 'string'
  ) {
    return JSON.stringify(value)
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TaskObservationError('canonical JSON rejects non-finite number')
    }
    return JSON.stringify(value)
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`
  }
  if (typeof value === 'object') {
    const keys = Object.keys(value).sort()
    const fields = keys.map(
      (key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`,
    )
    return `{${fields.join(',')}}`
  }
  throw new TaskObservationError('canonical JSON rejects unsupported value')
}

export function sha256Canonical(value) {
  const bytes = new TextEncoder().encode(canonicalJson(value))
  return bytesToHex(sha256(bytes))
}

export function requestFingerprint(command) {
  const contextId = command.context_id ?? command.task_id
  const hopCount = Number.isInteger(command.hop_count) ? command.hop_count : 0
  return sha256Canonical({
    type: command.type,
    sender_id: command.sender_id,
    recipient_id: command.recipient_id,
    task_id: command.task_id,
    context_id: contextId,
    hop_count: hopCount,
    payload: command.payload,
  })
}

export function terminalIdentity(task, event) {
  return {
    sender_id: event.sender_id,
    recipient_id: event.recipient_id,
    task_id: event.task_id,
    request_fingerprint: task.request_fingerprint,
    terminal_state: event.task_state,
    canonical_terminal_payload_hash: sha256Canonical(event.payload),
  }
}

const LEGAL_NEXT = {
  none: new Set(['submitted']),
  submitted: new Set([
    'working',
    'input-required',
    'auth-required',
    'completed',
    'failed',
    'canceled',
    'rejected',
  ]),
  working: new Set([
    'working',
    'input-required',
    'auth-required',
    'completed',
    'failed',
    'canceled',
    'rejected',
  ]),
  'input-required': new Set([
    'working',
    'completed',
    'failed',
    'canceled',
    'rejected',
  ]),
  'auth-required': new Set([
    'working',
    'completed',
    'failed',
    'canceled',
    'rejected',
  ]),
}

function recordViolation(task, kind, event) {
  task.contract_violations.push({
    kind,
    envelope_id: event.id,
    observation_index: event.observation_index,
    from_state: task.task_state,
    to_state: event.task_state,
  })
}

function terminalConflict(first, candidate) {
  if (first.terminal_state !== candidate.terminal_state) {
    return 'conflicting_terminal'
  }
  if (
    first.canonical_terminal_payload_hash !==
    candidate.canonical_terminal_payload_hash
  ) {
    return 'conflicting_terminal_payload_hash'
  }
  const identityKeys = [
    'sender_id',
    'recipient_id',
    'task_id',
    'request_fingerprint',
  ]
  const changed = identityKeys.some((key) => first[key] !== candidate[key])
  return changed ? 'conflicting_terminal_identity' : null
}

export function reduceObservedState(current, event) {
  const task = Object.assign({}, current, {
    contract_violations: current.contract_violations.slice(),
  })
  task.last_observation_index = event.observation_index
  task.last_ts = event.timestamp
  task.last_payload = event.payload

  const incoming = event.task_state
  if (!incoming) return task

  if (TERMINAL_STATES.has(task.task_state)) {
    if (!TERMINAL_STATES.has(incoming)) {
      recordViolation(task, 'invalid_transition', event)
      return task
    }
    const candidate = terminalIdentity(task, event)
    const conflict = terminalConflict(task.terminal_identity, candidate)
    if (conflict) {
      recordViolation(task, conflict, event)
    } else {
      task.terminal_replay_count += 1
    }
    return task
  }

  const legal = LEGAL_NEXT[task.task_state]
  if (!legal || !legal.has(incoming)) {
    recordViolation(task, 'invalid_transition', event)
    return task
  }

  task.task_state = incoming
  if (TERMINAL_STATES.has(incoming)) {
    task.terminal_identity = terminalIdentity(task, event)
    task.result = event.payload
  }
  return task
}

function initialTask(event) {
  return {
    task_id: event.task_id,
    context_id: event.context_id ?? null,
    sender_id: event.sender_id,
    recipient_id: event.recipient_id ?? null,
    task_state: 'none',
    request_fingerprint: null,
    terminal_identity: null,
    terminal_replay_count: 0,
    contract_violations: [],
    first_observation_index: event.observation_index,
    last_observation_index: event.observation_index,
    first_ts: event.timestamp,
    last_ts: event.timestamp,
    body: null,
    result: null,
    last_payload: event.payload,
  }
}

export function deriveTasks(messages) {
  const seenIndices = new Set()
  for (const message of messages) {
    const index = message.observation_index
    if (!Number.isInteger(index) || index <= 0) {
      throw new TaskObservationError(
        'observation_index must be a positive integer',
      )
    }
    if (seenIndices.has(index)) {
      throw new TaskObservationError(`duplicate observation_index ${index}`)
    }
    seenIndices.add(index)
  }

  const ordered = messages
    .slice()
    .sort((left, right) => left.observation_index - right.observation_index)
  const byTask = new Map()
  for (const message of ordered) {
    if (!message.task_id) continue
    if (!byTask.has(message.task_id)) {
      byTask.set(message.task_id, initialTask(message))
    }
    let task = byTask.get(message.task_id)
    if (message.type === 'command') {
      if (message.context_id == null || !Number.isInteger(message.hop_count)) {
        recordViolation(task, 'legacy_correlation_missing', message)
      }
      if (task.request_fingerprint === null) {
        task.request_fingerprint = requestFingerprint(message)
      }
      task.body = message.payload?.body ?? task.body
      task.sender_id = message.sender_id
      task.recipient_id = message.recipient_id ?? task.recipient_id
    }
    const observed =
      message.type === 'command' && !message.task_state
        ? Object.assign({}, message, { task_state: 'submitted' })
        : message
    task = reduceObservedState(task, observed)
    byTask.set(message.task_id, task)
  }
  return Array.from(byTask.values()).sort(
    (left, right) => right.last_observation_index - left.last_observation_index,
  )
}
