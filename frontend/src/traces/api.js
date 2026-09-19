import { MAX_RESPONSE_BYTES, readResponse, requireProtocol, TraceReadError } from './protocol'

const abortError = () => new DOMException('Aborted', 'AbortError')
const unavailable = () => new TraceReadError('unavailable', { retryable: true })
function requireTraceId(value) {
  if (typeof value !== 'string' || !/^[0-9a-f]{32}$(?![\s\S])/.test(value)) throw new TraceReadError('invalid_request')
}
const MAX_QUEUED_BYTES = 4 * 1024 * 1024
const MAX_QUEUED_MESSAGES = 16

async function boundedJSON(response) {
  if (Number(response.headers.get('Content-Length')) > MAX_RESPONSE_BYTES) {
    await response.body?.cancel()
    throw new TraceReadError('oversize_response')
  }
  requireProtocol(response.body)
  const reader = response.body.getReader()
  const chunks = []
  let size = 0
  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      size += value.byteLength
      if (size > MAX_RESPONSE_BYTES) throw new TraceReadError('oversize_response')
      chunks.push(value)
    }
  } catch (error) {
    await reader.cancel().catch(() => {})
    throw error
  } finally {
    reader.releaseLock()
  }
  const bytes = new Uint8Array(size)
  let offset = 0
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength }
  try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) }
  catch { throw new TraceReadError('invalid_response') }
}

// One instance per entered credential. It never uses browser storage or cookies,
// follows redirects, or sends credentials to URLs supplied by trace records.
export function createTraceApi(credential, {
  fetchImpl = globalThis.fetch,
  WebSocketImpl = globalThis.WebSocket,
  origin = globalThis.location.origin,
  requestTimeout = 10000,
  heartbeatTimeout = 25000,
} = {}) {
  requireProtocol(typeof credential === 'string' && /^[A-Za-z0-9_-]{32,256}$(?![\s\S])/.test(credential))
  const base = new URL(origin)
  requireProtocol(['http:', 'https:'].includes(base.protocol) && !base.username && !base.password)
  let token = credential
  const active = new Set()

  function own(signal) {
    if (!token) throw new TraceReadError('not_authorized')
    const controller = new AbortController()
    const abort = () => controller.abort()
    if (signal?.aborted) controller.abort()
    signal?.addEventListener('abort', abort, { once: true })
    active.add(controller)
    return {
      controller,
      release() { active.delete(controller); signal?.removeEventListener('abort', abort) },
    }
  }

  function dispose() {
    token = null
    for (const controller of active) controller.abort()
    active.clear()
  }

  async function request(path, params, expected, signal) {
    if (expected.kind !== 'trace_list') requireTraceId(expected.traceId)
    const { controller, release } = own(signal)
    const timer = setTimeout(() => controller.abort(), requestTimeout)
    try {
      const url = new URL('/api/traces' + path, base)
      for (const [key, value] of Object.entries(params)) {
        if (value !== null && value !== undefined) url.searchParams.set(key, String(value))
      }
      const response = await fetchImpl(url.href, {
        method: 'GET', headers: { Authorization: `Bearer ${token}` },
        credentials: 'omit', cache: 'no-store', redirect: 'error', signal: controller.signal,
      })
      if (response.status === 401 || response.status === 403) {
        await response.body?.cancel()
        dispose()
        throw new TraceReadError('not_authorized')
      }
      // Reverse-proxy failures can be HTML. They are retryable, never rendered.
      if (response.status >= 500 && response.status !== 503) {
        await response.body?.cancel()
        throw unavailable()
      }
      const value = readResponse(await boundedJSON(response), expected)
      controller.signal.throwIfAborted()
      requireProtocol(response.ok)
      return value
    } catch (error) {
      if (error instanceof TraceReadError) throw error
      if (signal?.aborted) throw abortError()
      if (!token) throw new TraceReadError('not_authorized')
      throw unavailable()
    } finally { clearTimeout(timer); release() }
  }

  async function* subscribe(traceId, after, signal) {
    requireTraceId(traceId)
    const { controller, release } = own(signal)
    const url = new URL('/ws/traces/' + encodeURIComponent(traceId), base)
    url.protocol = base.protocol === 'https:' ? 'wss:' : 'ws:'
    url.searchParams.set('after', after)
    let socket, timer, failure, wake
    let queue = [], bytes = 0
    const notify = () => { wake?.(); wake = null }
    const stop = (error, discard = true) => {
      if (failure) return
      failure = error
      if (discard) { queue = []; bytes = 0 }
      clearTimeout(timer)
      socket?.close()
      notify()
    }
    const aborted = () => stop(token ? abortError() : new TraceReadError('not_authorized'))
    const arm = () => { clearTimeout(timer); timer = setTimeout(() => stop(unavailable()), heartbeatTimeout) }
    controller.signal.addEventListener('abort', aborted, { once: true })
    try {
      if (controller.signal.aborted) throw abortError()
      socket = new WebSocketImpl(url.href)
      socket.onopen = () => {
        if (failure) return
        socket.send(JSON.stringify({ type: 'authenticate', token }))
        arm()
      }
      socket.onmessage = ({ data }) => {
        if (failure) return
        if (typeof data !== 'string') return stop(new TraceReadError('invalid_response'))
        const size = new TextEncoder().encode(data).byteLength
        if (size > MAX_RESPONSE_BYTES) return stop(new TraceReadError('oversize_response'))
        if (queue.length >= MAX_QUEUED_MESSAGES || bytes + size > MAX_QUEUED_BYTES) return stop(unavailable())
        queue.push({ data, size }); bytes += size
        arm(); notify()
      }
      socket.onerror = () => stop(unavailable())
      socket.onclose = ({ code }) => {
        if (code === 4401 || code === 4403) {
          stop(new TraceReadError('not_authorized')); dispose()
        } else stop(unavailable(), false)
      }
      arm()
      while (true) {
        if (failure && !queue.length) throw failure
        if (!queue.length) { await new Promise(resolve => { wake = resolve }); continue }
        const item = queue.shift(); bytes -= item.size
        let value
        try { value = JSON.parse(item.data) }
        catch { throw new TraceReadError('invalid_response') }
        try { value = readResponse(value, { traceId }) }
        catch (error) { if (error.code === 'not_authorized') dispose(); throw error }
        requireProtocol(['trace_change', 'trace_heartbeat'].includes(value.kind))
        yield value
      }
    } finally {
      clearTimeout(timer)
      controller.signal.removeEventListener('abort', aborted)
      if (socket) {
        socket.onopen = socket.onmessage = socket.onerror = socket.onclose = null
        socket.close()
      }
      queue = []; release()
    }
  }

  return Object.freeze({
    list: (params = {}, signal) => request('', params, { kind: 'trace_list' }, signal),
    graph: (traceId, params = {}, signal) => request('/' + encodeURIComponent(traceId), params, { kind: 'trace_graph', traceId }, signal),
    history: (traceId, params = {}, signal) => request('/' + encodeURIComponent(traceId) + '/history', params, { kind: 'trace_history', traceId }, signal),
    events: (traceId, params, signal) => request('/' + encodeURIComponent(traceId) + '/events', params, { kind: 'trace_events', traceId }, signal),
    changes: (traceId, after, signal) => request('/' + encodeURIComponent(traceId) + '/changes', { after }, { kind: 'trace_changes', traceId }, signal),
    subscribe, dispose,
  })
}
