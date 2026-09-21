import { StrictMode } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, expect, it, vi } from 'vitest'
import TraceExplorer from './TraceExplorer'
import { createTraceApi } from './api'
import { changes, deferred, fixture, graph, heartbeat, traceId } from './testFixtures'
import { navigateTrace, parseTraceRoute } from './navigation'

vi.mock('./api', () => ({ createTraceApi: vi.fn() }))
const instances = []
beforeEach(() => {
  instances.length = 0
  window.history.replaceState({}, '', '#execution')
  createTraceApi.mockImplementation(() => {
    let closed = false
    const check = () => { if (closed) throw new Error('Disposed API reused') }
    const api = {
      list: vi.fn(async () => { check(); return { items: [{ trace_id: traceId, root_agent_id: 'Test owner', outcome: null, coverage: graph().coverage }], next_cursor: null } }),
      graph: vi.fn(async () => { check(); return graph() }),
      changes: vi.fn(async (id, after) => { check(); return changes([], after) }),
      subscribe: vi.fn(async function* (id, after, signal) {
        check()
        if (!signal.aborted) await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }))
      }),
      events: vi.fn(async () => ({ as_of: graph().at, projection_generation: graph().projection_generation, events: [], next_cursor: null })),
      dispose: vi.fn(() => { closed = true }),
    }
    instances.push(api)
    return api
  })
})
function Owner() { return <TraceExplorer /> }
async function connect() { await screen.findByText('Test owner') }

it('recreates owned resources under StrictMode and freezes/restores a saved view without persisting access', async () => {
  const storage = vi.spyOn(Storage.prototype, 'setItem')
  const view = render(<StrictMode><Owner /></StrictMode>)
  await connect()
  expect(instances[0].dispose).toHaveBeenCalled()
  fireEvent.click(screen.getByRole('button', { name: /Test owner/ }))
  await screen.findByRole('button', { name: 'Pause live' })
  await waitFor(() => expect(screen.getByRole('button', { name: 'Pause live' })).toBeEnabled())
  fireEvent.click(screen.getByRole('button', { name: 'Pause live' }))
  await screen.findByRole('button', { name: 'Resume live' })
  expect(parseTraceRoute(window.location.hash).at).toBe(graph().at)
  await waitFor(() => expect(instances.at(-1).graph).toHaveBeenLastCalledWith(traceId, { at: graph().at }, expect.any(AbortSignal)))
  expect(storage).not.toHaveBeenCalled()
  expect(window.location.hash).not.toContain('a'.repeat(40))
  view.unmount()
  expect(instances.at(-1).dispose).toHaveBeenCalled()
  render(<Owner />)
  await screen.findByText('Test owner')
  expect(screen.queryByLabelText('Fleet read credential')).not.toBeInTheDocument()
})

it('rejects malformed history links instead of silently opening live evidence', async () => {
  window.history.replaceState({}, '', `#execution?run=${traceId}&at=broken`)
  render(<Owner />)
  await connect()
  expect(screen.getByText(/saved address contains an invalid/)).toBeInTheDocument()
  expect(instances.at(-1).graph).not.toHaveBeenCalled()
})

it('cancels stale inspector work and follows an exact observation link across sparse pages', async () => {
  render(<Owner />)
  await connect()
  const api = instances.at(-1), old = deferred(), event = fixture.input_events[0]
  api.events.mockReturnValueOnce(old.promise).mockResolvedValueOnce({ as_of: graph().at, projection_generation: graph().projection_generation, events: [], next_cursor: 'next' })
    .mockResolvedValueOnce({ as_of: graph().at, projection_generation: graph().projection_generation, events: [event], next_cursor: null })
  act(() => navigateTrace({ run: traceId, step: graph().nodes[0].id }))
  await waitFor(() => expect(api.events).toHaveBeenCalledTimes(1))
  act(() => navigateTrace({ run: traceId, step: graph().nodes[1].id, event: `${event.node_id}/${event.source_epoch}/${event.event_id}` }))
  await screen.findByLabelText('Observation details')
  expect(api.events.mock.calls[0][2].aborted).toBe(true)
  await act(async () => old.reject({ code: 'not_authorized' }))
  expect(screen.queryByLabelText('Fleet read credential')).not.toBeInTheDocument()
  expect(api.events.mock.calls.at(-1)[1]).toEqual({ as_of: graph().at, node_id: graph().nodes[1].id, after: 'next', limit: 100 })
})


it('shows collection outage and recovery from heartbeats without changing execution evidence', async () => {
  render(<Owner />)
  await connect()
  const offline = deferred(), online = deferred()
  instances.at(-1).subscribe.mockImplementation(async function* (id, after, signal) {
    await offline.promise
    yield { ...heartbeat(), freshness: { ...graph().freshness, collector_state: 'unavailable' } }
    await online.promise
    yield { ...heartbeat(), freshness: { ...graph().freshness, collector_state: 'collecting' } }
    if (!signal.aborted) await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }))
  })
  act(() => navigateTrace({ run: traceId }))
  await screen.findByText('Collection availability has not been observed.')
  const before = document.querySelectorAll('[data-node-id]').length
  await act(async () => offline.resolve())
  await screen.findByText(/Collection unavailable/)
  expect(document.querySelectorAll('[data-node-id]').length).toBe(before)
  await act(async () => online.resolve())
  await screen.findByText('Collector connected. Source coverage is reported separately.')
  expect(screen.queryByText(/Collection unavailable/)).not.toBeInTheDocument()
  expect(document.querySelectorAll('[data-node-id]').length).toBe(before)
})
