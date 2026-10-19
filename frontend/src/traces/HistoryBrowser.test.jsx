import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import HistoryBrowser from './HistoryBrowser'
import { clone, deferred, fixture, token, traceId } from './testFixtures'
import { parseTraceRoute } from './navigation'

const history = () => clone(fixture.responses.find(value => value.kind === 'trace_history'))
const route = { run: traceId, at: null, step: 'task:selected', event: 'selected-event' }

it('opens never-visited server snapshots and disables retired boundaries without inventing a graph', async () => {
  const result = history()
  const api = { history: vi.fn().mockResolvedValue(result) }
  render(<HistoryBrowser api={api} route={route} onDenied={vi.fn()} />)
  const oldest = await screen.findByRole('button', { name: /Snapshot 2/ })
  expect(screen.getByRole('button', { name: /Snapshot 7/ })).toBeDisabled()
  fireEvent.click(oldest)
  expect(parseTraceRoute(window.location.hash)).toMatchObject({ run: traceId, at: result.items[2].at, step: route.step, event: null })
  expect(api.history).toHaveBeenCalledTimes(1)
})

it('uses frozen cursors for backward/forward pages and only refresh explicitly admits a new range', async () => {
  const first = history(), next = token('older')
  first.items = first.items.slice(0, 1); first.next_cursor = next
  const second = { ...history(), items: history().items.slice(1) }
  const api = { history: vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second).mockResolvedValueOnce(first).mockResolvedValue(history()) }
  render(<HistoryBrowser api={api} route={route} onDenied={vi.fn()} />)
  await screen.findByRole('button', { name: /Snapshot 8/ })
  fireEvent.click(screen.getByRole('button', { name: 'Older history page' }))
  await screen.findByRole('button', { name: /Snapshot 2/ })
  expect(api.history.mock.calls[1][1].cursor).toBe(next)
  fireEvent.click(screen.getByRole('button', { name: 'Newer history page' }))
  await screen.findByRole('button', { name: /Snapshot 8/ })
  expect(api.history.mock.calls[2][1].cursor).toBe(first.snapshot_cursor)
  fireEvent.click(screen.getByRole('button', { name: 'Refresh history' }))
  await screen.findByRole('button', { name: /Snapshot 2/ })
  expect(api.history.mock.calls[3][1].cursor).toBeNull()
})

it('rejects shifted page ranges and does not change the selected graph on expiry', async () => {
  const first = { ...history(), next_cursor: token('older') }
  const api = { history: vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce({ ...history(), snapshot_cursor: token('wrong') }).mockRejectedValue({ code: 'history_expired' }) }
  render(<HistoryBrowser api={api} route={route} onDenied={vi.fn()} />)
  await screen.findByRole('button', { name: /Snapshot 8/ })
  const before = window.location.hash
  fireEvent.click(screen.getByRole('button', { name: 'Older history page' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('inconsistent evidence')
  expect(screen.queryByRole('button', { name: /Snapshot 8/ })).not.toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Refresh history' }))
  await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('outside the retained history'))
  expect(window.location.hash).toBe(before)
})

it('ignores a canceled denial and clears access for a current denial', async () => {
  const old = deferred(), denied = vi.fn()
  const api = { history: vi.fn().mockReturnValueOnce(old.promise).mockRejectedValue({ code: 'not_authorized' }) }
  const view = render(<HistoryBrowser api={api} route={route} onDenied={denied} />)
  view.unmount()
  await act(async () => old.reject({ code: 'not_authorized' }))
  expect(denied).not.toHaveBeenCalled()
  render(<HistoryBrowser api={api} route={route} onDenied={denied} />)
  await waitFor(() => expect(denied).toHaveBeenCalledTimes(1))
})

it('does not crash on an unavailable Core clock value', async () => {
  const result = history(); result.items[0].received_at_ms = Number.MAX_SAFE_INTEGER
  render(<HistoryBrowser api={{ history: vi.fn().mockResolvedValue(result) }} route={route} onDenied={vi.fn()} />)
  await screen.findByText('Core time unavailable')
})

it('removes old page controls while the next request is pending', async () => {
  const next = deferred()
  const api = { history: vi.fn().mockResolvedValueOnce({ ...history(), next_cursor: token('older') }).mockReturnValueOnce(next.promise) }
  const view = render(<HistoryBrowser api={api} route={route} onDenied={vi.fn()} />)
  await screen.findByRole('button', { name: /Snapshot 8/ })
  fireEvent.click(screen.getByRole('button', { name: 'Older history page' }))
  expect(screen.queryByRole('button', { name: 'Older history page' })).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /Snapshot 8/ })).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Refresh history' })).toBeDisabled()
  view.unmount()
})
