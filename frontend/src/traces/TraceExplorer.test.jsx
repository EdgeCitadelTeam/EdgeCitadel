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
      list: vi.fn(async () => { check(); return { items: [{ trace_id: traceId, task_name: 'Review deployment configuration', root_agent_id: 'Test owner', outcome: null, coverage: graph().coverage }], next_cursor: null } }),
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
  await connect()
  expect(screen.queryByLabelText('Fleet read credential')).not.toBeInTheDocument()
})

it('rejects malformed history links instead of silently opening live evidence', async () => {
  window.history.replaceState({}, '', `#execution?run=${traceId}&at=broken`)
  render(<Owner />)
  await connect()
  expect(screen.getByText(/saved address contains an invalid/)).toBeInTheDocument()
  expect(instances.at(-1).graph).not.toHaveBeenCalled()
})

it('cancels old snapshot work and restores an exact event across sparse pages', async () => {
  render(<Owner />)
  await connect()
  const api = instances.at(-1), old = deferred(), event = fixture.input_events[0]
  api.events.mockReturnValueOnce(old.promise).mockResolvedValueOnce({ as_of: graph().at, projection_generation: graph().projection_generation, events: [], next_cursor: 'next' })
    .mockResolvedValueOnce({ as_of: graph().at, projection_generation: graph().projection_generation, events: [event], next_cursor: null })
  act(() => navigateTrace({ run: traceId, step: graph().nodes[0].id }))
  await waitFor(() => expect(api.events).toHaveBeenCalledTimes(1))
  act(() => navigateTrace({ run: traceId, at: graph().at, event: `${event.node_id}/${event.source_epoch}/${event.event_id}` }))
  await screen.findByLabelText('Communication details')
  expect(screen.getByRole('tab', { name: 'Evidence' })).toHaveAttribute('aria-selected', 'true')
  expect(api.events.mock.calls[0][2].aborted).toBe(true)
  await act(async () => old.reject({ code: 'not_authorized' }))
  expect(screen.queryByText('Execution data is unavailable. Check access to this dashboard.')).not.toBeInTheDocument()
  expect(api.events.mock.calls.at(-1)[1]).toEqual({ as_of: graph().at, after: 'next', limit: 200 })
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
  await screen.findByLabelText('Agent communication map')
  fireEvent.click(screen.getByText('Run options'))
  fireEvent.click(screen.getByRole('button', { name: 'Diagnostics' }))
  await screen.findByText('Collection availability has not been observed.')
  const before = document.querySelectorAll('[data-node-id]').length
  await act(async () => offline.resolve())
  await waitFor(() => expect(screen.getAllByText(/Collection unavailable/).length).toBeGreaterThan(0))
  expect(document.querySelectorAll('[data-node-id]').length).toBe(before)
  await act(async () => online.resolve())
  await screen.findByText('Collector connected. Source coverage is reported separately.')
  expect(screen.queryByText(/Collection unavailable/)).not.toBeInTheDocument()
  expect(document.querySelectorAll('[data-node-id]').length).toBe(before)
})

it('keeps cross-page detail navigation and return focus aligned with message cards', async () => {
  const { communicationFixture } = await import('./communicationFixtures')
  const { g, events, event } = communicationFixture()
  for (let index = 0; index < 52; index++) events.push(event(`extra-${index}`, `extra-message-${index}`, 'command', 'received'))
  render(<Owner />)
  await connect()
  const api = instances.at(-1)
  api.graph.mockResolvedValue(g)
  api.events.mockResolvedValue({ as_of: g.at, projection_generation: g.projection_generation, events, next_cursor: null })
  act(() => navigateTrace({ run: traceId }))
  await screen.findByLabelText('Agent communication map')
  await waitFor(() => expect(document.querySelectorAll('[data-message-id]')).toHaveLength(50))
  fireEvent.click(document.querySelector('[data-message-id="t1-request"]'))
  await screen.findByRole('dialog')
  fireEvent.keyDown(screen.getByRole('dialog'), { key: 'End' })
  await waitFor(() => expect(document.querySelector('[data-message-id="extra-message-51"]')).toBeInTheDocument())
  fireEvent.click(screen.getByRole('button', { name: 'Close details', exact: true }))
  expect(document.querySelector('[data-message-id="extra-message-51"]')).toHaveFocus()
})

it('lists all retained runs without task filtering and keeps earlier pages visible', async () => {
  window.history.replaceState({}, '', `#execution?task=some-task`)
  render(<Owner />)
  await connect()
  const api = instances.at(-1)
  expect(api.list.mock.calls[0][0]).toEqual({ cursor: null, limit: 20 })
  const first = { trace_id: traceId, task_name: 'First run', root_agent_id: 'same-owner', coverage: graph().coverage }
  api.list.mockResolvedValueOnce({ items: [first], next_cursor: 'older' })
  fireEvent.click(screen.getByRole('button', { name: 'Refresh runs' }))
  await screen.findByText('First run')
  api.list.mockResolvedValueOnce({ items: [{ ...first, trace_id: 'b'.repeat(32), task_name: 'Second run', root_agent_id: 'same-owner' }], next_cursor: null })
  fireEvent.click(screen.getByRole('button', { name: 'Load more runs' }))
  await screen.findByText('Second run')
  expect(screen.getByText('First run')).toBeInTheDocument()
  expect(screen.queryByRole('textbox')).not.toBeInTheDocument()
})
