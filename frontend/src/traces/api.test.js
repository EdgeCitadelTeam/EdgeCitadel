import { afterEach, expect, it, vi } from 'vitest'
import { createTraceApi } from './api'
import { MAX_RESPONSE_BYTES } from './protocol'
import { deferred, graph, heartbeat, readError, token, traceId } from './testFixtures'

const CREDENTIAL = 'private-fleet-test-credential-123456789'
const json = (body, status = 200) => new Response(JSON.stringify(body), { status })
class Socket {
  static instances = []
  constructor(url) { this.url = url; this.sent = []; Socket.instances.push(this) }
  send(value) { this.sent.push(value) }
  close() { this.closed = true }
  open() { this.onopen?.() }
  receive(value) { this.onmessage?.({ data: JSON.stringify(value) }) }
  end(code = 1006) { this.onclose?.({ code }) }
}
afterEach(() => { Socket.instances = []; vi.useRealTimers() })
const apiWith = (options = {}) => createTraceApi(CREDENTIAL, { origin: 'https://core.example', WebSocketImpl: Socket, ...options })

it('uses read-only authenticated same-origin requests without URL/storage credentials', async () => {
  const fetchImpl = vi.fn().mockResolvedValue(json(graph()))
  const stored = vi.spyOn(Storage.prototype, 'setItem')
  const api = apiWith({ fetchImpl })
  expect(await api.graph(traceId)).toEqual(graph())
  const [url, options] = fetchImpl.mock.calls[0]
  expect(url).toBe(`https://core.example/api/traces/${traceId}`)
  expect(url).not.toContain(CREDENTIAL)
  expect(options).toMatchObject({ method: 'GET', credentials: 'omit', redirect: 'error', cache: 'no-store', headers: { Authorization: `Bearer ${CREDENTIAL}` } })
  expect(stored).not.toHaveBeenCalled()
  api.dispose()
})

it('clears credentials and aborts other requests on denial, without revealing response content', async () => {
  const pending = deferred()
  const fetchImpl = vi.fn().mockReturnValueOnce(pending.promise).mockResolvedValueOnce(new Response('PRIVATE_SENTINEL', { status: 403 }))
  const api = apiWith({ fetchImpl })
  const first = api.graph(traceId)
  await expect(api.graph(traceId)).rejects.toThrow('not_authorized')
  expect(fetchImpl.mock.calls[0][1].signal.aborted).toBe(true)
  pending.resolve(json(graph())) // Even a transport ignoring cancellation cannot return old data.
  await expect(first).rejects.toThrow('not_authorized')
  await expect(api.graph(traceId)).rejects.toThrow('not_authorized')
  expect(fetchImpl).toHaveBeenCalledTimes(2)
})

it('bounds streamed response bytes even without Content-Length', async () => {
  const canceled = vi.fn()
  const body = new ReadableStream({
    start(controller) { controller.enqueue(new Uint8Array(MAX_RESPONSE_BYTES + 1)) },
    cancel: canceled,
  })
  const api = apiWith({ fetchImpl: vi.fn().mockResolvedValue(new Response(body)) })
  await expect(api.graph(traceId)).rejects.toThrow('oversize_response')
  expect(canceled).toHaveBeenCalledOnce()
  api.dispose()
})

it('uses fixed errors for malformed content and proxy failures', async () => {
  for (const [response, error] of [[new Response('PRIVATE_SENTINEL'), 'invalid_response'], [new Response('PRIVATE_SENTINEL', { status: 502 }), 'unavailable']]) {
    const api = apiWith({ fetchImpl: vi.fn().mockResolvedValue(response) })
    await expect(api.graph(traceId)).rejects.toThrow(error)
    api.dispose()
  }
})

it('authenticates WS in its first frame and preserves a queued server error across close', async () => {
  const api = apiWith()
  const iterator = api.subscribe(traceId, token('resume'))
  const next = iterator.next()
  const socket = Socket.instances[0]
  socket.open()
  expect(socket.url).toContain('wss://core.example/ws/traces/')
  expect(socket.url).not.toContain(CREDENTIAL)
  expect(JSON.parse(socket.sent[0])).toEqual({ type: 'authenticate', token: CREDENTIAL })
  socket.receive(readError('history_expired'))
  socket.end(1008)
  await expect(next).rejects.toMatchObject({ code: 'history_expired', resnapshotRequired: true })
  expect(socket.closed).toBe(true)
  api.dispose()
})

it('disconnects a slow consumer at the message bound and allows replay from its own cursor', async () => {
  const api = apiWith()
  const iterator = api.subscribe(traceId, token('applied'))
  const first = iterator.next()
  const socket = Socket.instances[0]
  socket.open(); socket.receive(heartbeat())
  await first
  for (let i = 0; i < 17; i++) socket.receive(heartbeat())
  expect(socket.closed).toBe(true)
  await expect(iterator.next()).rejects.toMatchObject({ code: 'unavailable', retryable: true })
  const retry = api.subscribe(traceId, token('applied'))
  const next = retry.next()
  expect(Socket.instances[1].url).toContain(encodeURIComponent(token('applied')))
  api.dispose()
  await expect(next).rejects.toThrow('not_authorized')
})

it('bounds aggregate WS bytes independently of message count', async () => {
  const api = apiWith()
  const iterator = api.subscribe(traceId, token('applied'))
  const first = iterator.next()
  const socket = Socket.instances[0]
  socket.receive(heartbeat()); await first
  for (let i = 0; i < 3; i++) socket.onmessage({ data: 'x'.repeat(MAX_RESPONSE_BYTES) })
  await expect(iterator.next()).rejects.toMatchObject({ code: 'unavailable', retryable: true })
  api.dispose()
})

it('terminates silent sockets and cancels ownership on abort', async () => {
  vi.useFakeTimers()
  const api = apiWith({ heartbeatTimeout: 20 })
  const iterator = api.subscribe(traceId, token('applied'))
  const rejection = expect(iterator.next()).rejects.toThrow('unavailable')
  await vi.advanceTimersByTimeAsync(21)
  await rejection
  expect(Socket.instances[0].closed).toBe(true)
  const controller = new AbortController()
  const second = api.subscribe(traceId, token('applied'), controller.signal)
  const aborted = expect(second.next()).rejects.toMatchObject({ name: 'AbortError' })
  controller.abort(); await aborted
  api.dispose()
})


it('rejects malformed URL run identities before making any authenticated request', async () => {
  const fetchImpl = vi.fn()
  const api = apiWith({ fetchImpl })
  await expect(api.graph('..')).rejects.toThrow('invalid_request')
  await expect(api.graph(traceId + '\n')).rejects.toThrow('invalid_request')
  await expect(api.subscribe('../private', token('cursor')).next()).rejects.toThrow('invalid_request')
  expect(fetchImpl).not.toHaveBeenCalled()
  expect(Socket.instances).toHaveLength(0)
  api.dispose()
})
