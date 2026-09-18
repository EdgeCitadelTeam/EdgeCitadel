import { afterEach, expect, it, vi } from 'vitest'
import { createTraceSession } from './session'
import { TraceReadError } from './protocol'
import { change, changes, deferred, graph, heartbeat, message, token, traceId } from './testFixtures'

const sessions = []
afterEach(() => { for (const session of sessions.splice(0)) session.dispose() })
const until = check => vi.waitFor(check, { interval: 1, timeout: 1000 })
function setup() {
  const channels = []
  const api = {
    graph: vi.fn().mockResolvedValue(graph()),
    changes: vi.fn(async (id, after) => ({ ...changes([], after), trace_id: id })),
    subscribe: vi.fn(async function* (id, after, signal) {
      let waiting, error, closed = false
      const queue = []
      const wake = () => { waiting?.(); waiting = null }
      const channel = {
        emit(value) { queue.push(value); wake() },
        fail(value) { error = value; wake() },
        get closed() { return closed },
      }
      const abort = () => channel.fail(new DOMException('Aborted', 'AbortError'))
      signal.addEventListener('abort', abort, { once: true })
      channels.push(channel)
      try {
        while (true) {
          if (error) throw error
          if (!queue.length) { await new Promise(resolve => { waiting = resolve }); continue }
          yield queue.shift()
        }
      } finally { closed = true; signal.removeEventListener('abort', abort) }
    }),
  }
  const session = createTraceSession(api, { retryDelay: 1, historyPoll: 10 })
  sessions.push(session)
  return { api, session, channels }
}
async function live(context) {
  await context.session.open(traceId)
  await until(() => expect(context.channels.length).toBe(1))
}

it('applies one atomic patch and reconnects from its applied cursor, never a heartbeat', async () => {
  const context = setup(), { api, session, channels } = context
  await live(context)
  const row = change({ upsert_nodes: [{ ...graph().nodes[0], state: 'completed', original_state: 'completed' }] })
  channels[0].emit(message(row))
  await until(() => expect(session.getSnapshot().resumeCursor).toBe(row.cursor))
  channels[0].emit(heartbeat(token('farAhead')))
  await until(() => expect(session.getSnapshot().freshness).toEqual(graph().freshness))
  channels[0].fail(new TraceReadError('unavailable', { retryable: true }))
  await until(() => expect(channels.length).toBe(2))
  expect(api.changes.mock.calls.at(-1)[1]).toBe(row.cursor)
  expect(api.subscribe.mock.calls.at(-1)[1]).toBe(row.cursor)
  expect(session.getSnapshot().graph.at).toBe(row.at)
})

it('finishes every replacement page before publishing or acknowledging subsequent changes', async () => {
  const context = setup(), { api, session, channels } = context
  const replacement = deferred()
  const first = graph()
  const next = { ...first, at: token('replacement'), resume_cursor: token('historicalResume'), ingest_high_watermark: 60 }
  api.graph.mockResolvedValueOnce(first)
    .mockResolvedValueOnce({ ...next, nodes: next.nodes.slice(0, 1), edges: [], expansions: [{ node_id: next.nodes[0].id, cursor: token('expand'), remaining_nodes: next.nodes.length - 1 }] })
    .mockReturnValueOnce(replacement.promise)
  await live(context)
  const snapshots = []
  session.subscribe(() => snapshots.push(session.getSnapshot()))
  const row = change({ mode: 'snapshot', at: next.at, ingest_high_watermark: 60 })
  channels[0].emit(message(row))
  await until(() => expect(api.graph).toHaveBeenCalledTimes(3))
  const following = change({ cursor: token('third'), at: token('thirdGraph'), ingest_high_watermark: 61 })
  channels[0].emit(message(following))
  expect(session.getSnapshot().graph).toEqual(first)
  expect(session.getSnapshot().resumeCursor).toBe(first.resume_cursor)
  replacement.resolve({ ...next, page_kind: 'expansion', expansions: [] })
  await until(() => expect(session.getSnapshot().resumeCursor).toBe(following.cursor))
  expect(snapshots.some(state => state.resumeCursor === row.cursor && state.graph.nodes.length === next.nodes.length)).toBe(true)
  expect(snapshots.some(state => state.resumeCursor === next.resume_cursor)).toBe(false)
  expect(snapshots.every(state => state.graph.nodes.length === next.nodes.length)).toBe(true)
})

it('keeps the old applied cursor when replacement loading fails and retries persisted catch-up', async () => {
  const context = setup(), { api, session, channels } = context
  await live(context)
  api.graph.mockRejectedValueOnce(new TraceReadError('unavailable', { retryable: true }))
  channels[0].emit(message(change({ mode: 'snapshot' })))
  await until(() => expect(channels.length).toBe(2))
  expect(api.changes.mock.calls.at(-1)[1]).toBe(graph().resume_cursor)
  expect(session.getSnapshot().graph.at).toBe(graph().at)
})

it('does not let a superseded run load or failure overwrite the selected run', async () => {
  const { api, session } = setup()
  const old = deferred()
  const otherId = 'b'.repeat(32)
  const other = { ...graph(), trace_id: otherId }
  api.graph.mockReturnValueOnce(old.promise).mockResolvedValueOnce(other)
  const opening = session.open(traceId)
  await session.open(otherId)
  old.reject(new TraceReadError('not_authorized'))
  await opening
  expect(session.getSnapshot().traceId).toBe(otherId)
  expect(session.getSnapshot().graph).toEqual(other)
  expect(api.graph.mock.calls[0][2].aborted).toBe(true)
})

it('freezes historical graph and cursor while indicating newer evidence', async () => {
  const { api, session, channels } = setup()
  api.changes.mockResolvedValue(changes([change()]))
  await session.open(traceId, { at: graph().at })
  await until(() => expect(session.getSnapshot().newerAvailable).toBe(true))
  expect(session.getSnapshot().mode).toBe('historical')
  expect(session.getSnapshot().graph.at).toBe(graph().at)
  expect(session.getSnapshot().resumeCursor).toBe(graph().resume_cursor)
  expect(channels).toHaveLength(0)
  expect(Object.keys(api).sort()).toEqual(['changes', 'graph', 'subscribe'])
})

it('pauses the actual retained snapshot and explicitly resnapshots on resume', async () => {
  const context = setup(), { api, session, channels } = context
  await live(context)
  await session.pause()
  expect(api.graph.mock.calls.at(-1)[1]).toEqual({ at: graph().at })
  await until(() => expect(channels[0].closed).toBe(true))
  expect(session.getSnapshot().mode).toBe('historical')
  await session.resume()
  expect(api.graph.mock.calls.at(-1)[1]).toEqual({})
  await until(() => expect(channels.length).toBe(2))
})

it.each([
  ['not_authorized', 'inaccessible'], ['not_found', 'inaccessible'],
  ['generation_changed', 'resnapshot-required'], ['history_expired', 'resnapshot-required'],
])('clears protected detail on %s without silently jumping to a different snapshot', async (code, mode) => {
  const context = setup(), { session, channels } = context
  await live(context)
  channels[0].fail(new TraceReadError(code, { resnapshotRequired: mode === 'resnapshot-required' }))
  await until(() => expect(session.getSnapshot().mode).toBe(mode))
  expect(session.getSnapshot().graph).toBeNull()
  expect(session.getSnapshot().resumeCursor).toBeNull()
  expect(channels).toHaveLength(1)
})

it('rejects a generation change carried by a heartbeat', async () => {
  const context = setup(), { session, channels } = context
  await live(context)
  channels[0].emit({ ...heartbeat(), projection_generation: 'different' })
  await until(() => expect(session.getSnapshot().mode).toBe('resnapshot-required'))
  expect(session.getSnapshot().graph).toBeNull()
})

it('clears arbitrary graphs and admits a later incarnation without old nodes', async () => {
  const context = setup(), { session, channels } = context
  await live(context)
  const clear = change({ mode: 'clear', trace_state: 'expired', at: null })
  channels[0].emit(message(clear))
  await until(() => expect(session.getSnapshot().graph).toBeNull())
  expect(session.getSnapshot().resumeCursor).toBe(clear.cursor)
  const reincarnated = change({ cursor: token('newRun'), upsert_nodes: [graph().nodes[0]] })
  channels[0].emit(message(reincarnated))
  await until(() => expect(session.getSnapshot().graph?.nodes).toHaveLength(1))
  expect(session.getSnapshot().graph.edges).toEqual([])
})

it('advances empty continuation pages only through their served HTTP boundary', async () => {
  const { api, session, channels } = setup()
  api.changes.mockResolvedValueOnce({ ...changes([], token('scan1')), next_cursor: token('scan1') })
    .mockResolvedValueOnce(changes([change()], token('scan2')))
  await session.open(traceId)
  await until(() => expect(channels).toHaveLength(1))
  expect(api.changes.mock.calls[1][1]).toBe(token('scan1'))
  expect(api.subscribe.mock.calls[0][1]).toBe(token('scan2'))
  expect(session.getSnapshot().graph.at).toBe(token('graph2'))
})
