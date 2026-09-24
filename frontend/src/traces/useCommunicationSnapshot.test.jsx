import { act, renderHook, waitFor } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import { useCommunicationSnapshot } from './useCommunicationSnapshot'
import { graph, deferred, token } from './testFixtures'
import { communicationFixture } from './communicationFixtures'

const page = (g, events = [], next = null) => ({ as_of: g.at, projection_generation: g.projection_generation, events, next_cursor: next })
it('keeps a consistent previous view until the new first page is ready and aborts stale pages', async () => {
  const g = graph(), next = { ...g, at: token('next') }, pending = deferred(), later = deferred()
  const api = { events: vi.fn().mockResolvedValueOnce(page(g)).mockReturnValueOnce(pending.promise).mockReturnValueOnce(later.promise) }, denied = vi.fn()
  const view = renderHook(({ graph }) => useCommunicationSnapshot(api, graph, denied), { initialProps: { graph: g } })
  await waitFor(() => expect(view.result.current.view?.graph).toBe(g))
  view.rerender({ graph: next })
  expect(view.result.current.view.graph).toBe(g)
  await act(async () => pending.resolve(page(next, [], 'more')))
  expect(view.result.current.view.graph).toBe(next)
  const signal = api.events.mock.calls.at(-1)[2]
  view.unmount()
  expect(signal.aborted).toBe(true)
  await act(async () => later.reject({ code: 'not_authorized' }))
  expect(denied).not.toHaveBeenCalled()
})
it('automatically loads five snapshot pages then explicitly continues without duplicating messages', async () => {
  const { g, events } = communicationFixture()
  let count = 0
  const api = { events: vi.fn(async () => page(g, [events[0]], ++count < 7 ? `p${count}` : null)) }
  const denied = vi.fn(), view = renderHook(() => useCommunicationSnapshot(api, g, denied))
  await waitFor(() => expect(view.result.current.loading).toBe(false))
  expect(api.events).toHaveBeenCalledTimes(5)
  expect(view.result.current.view.projection.messages).toHaveLength(1)
  expect(view.result.current.view.next).toBe('p5')
  await act(async () => view.result.current.more())
  expect(api.events).toHaveBeenCalledTimes(7)
  expect(view.result.current.view.next).toBeNull()
  for (const [, params] of api.events.mock.calls) expect(params).toMatchObject({ as_of: g.at, limit: 200 })
})
it('rejects mixed snapshots and clears retained evidence on authorization loss', async () => {
  const g = graph(), denied = vi.fn(), api = { events: vi.fn().mockResolvedValueOnce(page(g, [], 'more')).mockRejectedValueOnce({ code: 'not_authorized' }) }
  const view = renderHook(() => useCommunicationSnapshot(api, g, denied))
  await waitFor(() => expect(denied).toHaveBeenCalled())
  expect(view.result.current.view).toBeNull()
  view.unmount()
  api.events.mockResolvedValueOnce(page({ ...g, at: token('wrong') }))
  const mismatch = renderHook(() => useCommunicationSnapshot(api, g, denied))
  await waitFor(() => expect(mismatch.result.current.error?.code).toBe('invalid_response'))
  expect(mismatch.result.current.view).toBeNull()
})
