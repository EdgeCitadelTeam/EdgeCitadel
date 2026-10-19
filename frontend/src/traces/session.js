import { loadSnapshot, patchGraph } from './graph'
import { requireProtocol, TraceReadError } from './protocol'

function delay(ms, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(new DOMException('Aborted', 'AbortError')) }
    const timer = setTimeout(() => { signal.removeEventListener('abort', abort); resolve() }, ms)
    if (signal.aborted) abort()
    else signal.addEventListener('abort', abort, { once: true })
  })
}

// A session owns exactly one run/view and one ordered update loop. React may
// subscribe with useSyncExternalStore; switching views aborts all prior work.
export function createTraceSession(api, { retryDelay = 500, historyPoll = 2000 } = {}) {
  let state = Object.freeze({ mode: 'idle', traceId: null, graph: null, resumeCursor: null, error: null, newerAvailable: false, progress: null })
  let controller = null, epoch = 0, closed = false
  const listeners = new Set()
  const publish = patch => {
    state = Object.freeze({ ...state, ...patch })
    for (const listener of listeners) listener()
  }
  const current = id => !closed && id === epoch && !controller?.signal.aborted
  const fault = (error, id) => {
    if (!current(id)) return
    const failure = error instanceof TraceReadError ? error : new TraceReadError('unavailable', { retryable: true })
    const inaccessible = failure.code === 'not_authorized' || failure.code === 'not_found'
    publish({
      mode: inaccessible ? 'inaccessible' : failure.resnapshotRequired ? 'resnapshot-required' : 'error',
      error: failure, progress: null,
      ...(inaccessible || failure.resnapshotRequired ? { graph: null, resumeCursor: null, newerAvailable: false } : {}),
    })
    controller.abort()
  }
  function scope(page, id) {
    requireProtocol(current(id))
    requireProtocol(page.trace_id === state.traceId)
    if (page.projection_generation !== state.generation) {
      throw new TraceReadError('generation_changed', { resnapshotRequired: true })
    }
  }
  async function apply(change, id, signal) {
    if (change.cursor === state.resumeCursor) return
    let graph
    if (change.mode === 'clear') graph = null
    else if (change.mode === 'snapshot') {
      graph = await loadSnapshot(api, state.traceId, change.at, signal)
      if (!current(id)) return
      requireProtocol(graph.projection_generation === state.generation && graph.ingest_high_watermark === change.ingest_high_watermark)
      graph = { ...graph, resume_cursor: change.cursor }
    } else {
      // A later incarnation can appear after a clear, starting with an empty map.
      const base = state.graph ?? {
        schema_version: 1, kind: 'trace_graph', page_kind: 'snapshot',
        trace_id: state.traceId, projection_generation: state.generation,
        nodes: [], edges: [], expansions: [], freshness: state.freshness,
      }
      graph = patchGraph(base, change)
    }
    if (current(id)) publish({ graph, resumeCursor: change.cursor, traceState: change.trace_state, progress: null })
  }
  async function catchUp(id, signal) {
    let hasMore = true
    while (current(id) && hasMore) {
      const before = state.resumeCursor
      const page = await api.changes(state.traceId, before, signal)
      if (!current(id)) return
      scope(page, id)
      for (const change of page.changes) {
        await apply(change, id, signal)
        if (!current(id)) return
      }
      hasMore = page.next_cursor !== null
      requireProtocol(!hasMore || page.through_cursor !== before)
      // Every commit through this HTTP boundary has now been fully applied.
      publish({ resumeCursor: page.through_cursor })
    }
  }
  async function follow(id, signal) {
    let attempt = 0
    while (current(id)) {
      try {
        await catchUp(id, signal)
        if (!current(id)) return
        publish({ mode: 'live', error: null })
        for await (const message of api.subscribe(state.traceId, state.resumeCursor, signal)) {
          if (!current(id)) return
          scope(message, id)
          if (message.kind === 'trace_change') await apply(message.change, id, signal)
          else {
            // A heartbeat is not an application ACK. In particular, it cannot
            // advance past a snapshot replacement that is still being expanded.
            publish({ freshness: message.freshness })
          }
          attempt = 0
        }
        if (current(id)) throw new TraceReadError('unavailable', { retryable: true })
      } catch (error) {
        if (!current(id)) return
        if (!(error instanceof TraceReadError) || !error.retryable) { fault(error, id); return }
        publish({ mode: 'reconnecting', error })
        try { await delay(Math.min(5000, retryDelay * 2 ** Math.min(attempt++, 4)), signal) }
        catch { return }
      }
    }
  }
  async function watchHistory(id, signal) {
    // Only observe whether newer evidence exists. Never mutate the historical
    // graph or its event cursor, and never invoke an execution endpoint.
    let cursor = state.resumeCursor
    try {
      while (current(id)) {
        const page = await api.changes(state.traceId, cursor, signal)
        if (!current(id)) return
        scope(page, id)
        if (page.changes.length) { publish({ newerAvailable: true }); return }
        requireProtocol(!page.next_cursor || page.through_cursor !== cursor)
        cursor = page.through_cursor
        if (!page.next_cursor) await delay(historyPoll, signal)
      }
    } catch (error) {
      if (!current(id)) return
      // Retryable collection failure leaves the chosen historical view frozen.
      if (error instanceof TraceReadError && error.retryable) publish({ error })
      else fault(error, id)
    }
  }
  async function open(traceId, { at = null } = {}) {
    if (closed) throw new Error('trace_session_closed')
    controller?.abort()
    controller = new AbortController()
    const { signal } = controller
    const id = ++epoch
    publish({ mode: 'loading', traceId, graph: null, resumeCursor: null, generation: null, freshness: null,
      error: null, newerAvailable: false, progress: null, historicalAt: at, traceState: null })
    try {
      const graph = await loadSnapshot(api, traceId, at, signal, progress => { if (current(id)) publish({ progress }) })
      if (!current(id)) return
      publish({ graph, resumeCursor: graph.resume_cursor, generation: graph.projection_generation,
        freshness: graph.freshness, mode: at ? 'historical' : 'reconnecting', traceState: 'present', progress: null })
      void (at ? watchHistory(id, signal) : follow(id, signal))
    } catch (error) { fault(error, id) }
  }
  return Object.freeze({
    getSnapshot: () => state,
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener) },
    open,
    pause: () => state.graph ? open(state.traceId, { at: state.graph.at }) : Promise.resolve(),
    resume: () => state.traceId ? open(state.traceId) : Promise.resolve(),
    dispose() {
      closed = true; ++epoch; controller?.abort()
      publish({ mode: 'closed', graph: null, resumeCursor: null, error: null, freshness: null, progress: null })
      listeners.clear()
    },
  })
}
