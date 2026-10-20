import { describe, expect, it } from 'vitest'
import {
  deriveTasks,
  reduceObservedState,
  requestFingerprint,
  TaskObservationError,
} from './taskReducer'

const TERMINALS = ['completed', 'failed', 'canceled', 'rejected']

const LEGAL_TRANSITIONS = [
  ['none', 'submitted'],
  ['submitted', 'working'],
  ['submitted', 'input-required'],
  ['submitted', 'auth-required'],
  ['submitted', 'completed'],
  ['submitted', 'failed'],
  ['submitted', 'canceled'],
  ['submitted', 'rejected'],
  ['working', 'working'],
  ['working', 'input-required'],
  ['working', 'auth-required'],
  ['working', 'completed'],
  ['working', 'failed'],
  ['working', 'canceled'],
  ['working', 'rejected'],
  ['input-required', 'working'],
  ['input-required', 'completed'],
  ['input-required', 'failed'],
  ['input-required', 'canceled'],
  ['input-required', 'rejected'],
  ['auth-required', 'working'],
  ['auth-required', 'completed'],
  ['auth-required', 'failed'],
  ['auth-required', 'canceled'],
  ['auth-required', 'rejected'],
]

const INVALID_TRANSITIONS = [
  ['none', 'working'],
  ['submitted', 'submitted'],
  ['working', 'submitted'],
  ['input-required', 'input-required'],
  ['input-required', 'auth-required'],
  ['auth-required', 'auth-required'],
  ['auth-required', 'input-required'],
]

function event(overrides = {}) {
  const index = overrides.observation_index ?? 1
  return {
    id: `wire-${index}`,
    observation_index: index,
    type: 'task.progress',
    sender_id: 'shell-1',
    recipient_id: 'aggregator',
    task_id: 'task-1',
    context_id: 'context-1',
    hop_count: 0,
    task_state: 'working',
    timestamp: `2026-07-25T12:00:${String(index).padStart(2, '0')}.000Z`,
    payload: { body: 'edgecitadel:nonce-1' },
    ...overrides,
  }
}

function command(overrides = {}) {
  return event({
    id: 'command-1',
    observation_index: 1,
    type: 'command',
    sender_id: 'aggregator',
    recipient_id: 'shell-1',
    task_state: undefined,
    payload: { body: 'nonce-1' },
    ...overrides,
  })
}

function taskAt(taskState) {
  const request = command()
  return {
    task_id: 'task-1',
    context_id: 'context-1',
    sender_id: 'aggregator',
    recipient_id: 'shell-1',
    task_state: taskState,
    request_fingerprint: requestFingerprint(request),
    terminal_identity: null,
    terminal_replay_count: 0,
    contract_violations: [],
    first_observation_index: 1,
    last_observation_index: 1,
    first_ts: request.timestamp,
    last_ts: request.timestamp,
    body: 'nonce-1',
    result: null,
    last_payload: request.payload,
  }
}

describe('observed task state transitions', () => {
  it.each(LEGAL_TRANSITIONS)('%s -> %s is legal', (from, to) => {
    const next = reduceObservedState(
      taskAt(from),
      event({ observation_index: 2, task_state: to }),
    )

    expect(next.task_state).toBe(to)
    expect(next.contract_violations).toEqual([])
  })

  it.each(INVALID_TRANSITIONS)('%s -> %s is rejected', (from, to) => {
    const next = reduceObservedState(
      taskAt(from),
      event({ observation_index: 2, task_state: to }),
    )

    expect(next.task_state).toBe(from)
    expect(next.contract_violations).toMatchObject([
      { kind: 'invalid_transition', envelope_id: 'wire-2', observation_index: 2 },
    ])
  })

  it.each(TERMINALS)('%s accepts an identical terminal replay', (terminal) => {
    const first = reduceObservedState(
      taskAt('submitted'),
      event({ observation_index: 2, task_state: terminal }),
    )
    const replay = reduceObservedState(
      first,
      event({ id: 'terminal-replay', observation_index: 3, task_state: terminal }),
    )

    expect(replay.task_state).toBe(terminal)
    expect(replay.terminal_replay_count).toBe(1)
    expect(replay.contract_violations).toEqual([])
  })

  it.each(TERMINALS)('%s dominates a later working update', (terminal) => {
    const first = reduceObservedState(
      taskAt('submitted'),
      event({ observation_index: 2, task_state: terminal }),
    )
    const later = reduceObservedState(first, event({ observation_index: 3 }))

    expect(later.task_state).toBe(terminal)
    expect(later.contract_violations.at(-1)).toMatchObject({
      kind: 'invalid_transition',
      observation_index: 3,
    })
  })

  it('records a changed terminal state', () => {
    const first = reduceObservedState(
      taskAt('submitted'),
      event({ observation_index: 2, task_state: 'completed' }),
    )
    const later = reduceObservedState(
      first,
      event({ observation_index: 3, task_state: 'failed' }),
    )

    expect(later.contract_violations.at(-1).kind).toBe('conflicting_terminal')
  })

  it('records a changed terminal payload', () => {
    const first = reduceObservedState(
      taskAt('submitted'),
      event({ observation_index: 2, task_state: 'completed' }),
    )
    const later = reduceObservedState(
      first,
      event({
        observation_index: 3,
        task_state: 'completed',
        payload: { body: 'edgecitadel:changed' },
      }),
    )

    expect(later.contract_violations.at(-1).kind).toBe(
      'conflicting_terminal_payload_hash',
    )
  })

  it('records a changed terminal identity', () => {
    const first = reduceObservedState(
      taskAt('submitted'),
      event({ observation_index: 2, task_state: 'completed' }),
    )
    const later = reduceObservedState(
      first,
      event({
        observation_index: 3,
        task_state: 'completed',
        sender_id: 'shell-2',
      }),
    )

    expect(later.contract_violations.at(-1).kind).toBe(
      'conflicting_terminal_identity',
    )
  })
})

describe('task observation derivation', () => {
  const completedHistory = () => [
    command(),
    event({ id: 'result-1', observation_index: 2, type: 'result', task_state: 'completed' }),
  ]

  it('derives the same result from either input ordering', () => {
    expect(deriveTasks(completedHistory())).toEqual(
      deriveTasks(completedHistory().reverse()),
    )
  })

  it('does not use envelope timestamps as state ordering', () => {
    const history = completedHistory()
    history[0].timestamp = '2099-01-01T00:00:00.000Z'
    history[1].timestamp = '2000-01-01T00:00:00.000Z'

    expect(deriveTasks(history)[0].task_state).toBe('completed')
  })

  it('leaves state unchanged when an observation has no task state', () => {
    const current = taskAt('working')
    const next = reduceObservedState(
      current,
      event({ observation_index: 2, task_state: undefined }),
    )

    expect(next.task_state).toBe('working')
  })

  it('projects a command without task state to submitted', () => {
    expect(deriveTasks([command()])[0]).toMatchObject({
      task_state: 'submitted',
      body: 'nonce-1',
    })
  })

  it('rejects invalid observation indices', () => {
    expect(() => deriveTasks([command({ observation_index: undefined })])).toThrow(
      TaskObservationError,
    )
    expect(() => deriveTasks([command({ observation_index: 1.5 })])).toThrow(
      TaskObservationError,
    )
    expect(() => deriveTasks([command(), event({ observation_index: 1 })])).toThrow(
      TaskObservationError,
    )
  })

  it('isolates a legacy direct command and preserves its compatible fingerprint', () => {
    const legacy = command({
      task_id: 'task-legacy',
      context_id: undefined,
      hop_count: undefined,
      payload: { body: 'legacy' },
    })
    const correlated = command({
      id: 'command-2',
      observation_index: 2,
      task_id: 'task-correlated',
      context_id: 'context-2',
      hop_count: 0,
    })

    const tasks = deriveTasks([legacy, correlated])
    const legacyTask = tasks.find((task) => task.task_id === 'task-legacy')

    expect(legacyTask).toMatchObject({
      request_fingerprint:
        '8183b9af0b66433c284834a046bc57d9092be346e9ac58c082b439018cd6bafa',
      contract_violations: [{ kind: 'legacy_correlation_missing' }],
    })
    expect(tasks.map((task) => task.task_id)).toContain('task-correlated')
  })
})
