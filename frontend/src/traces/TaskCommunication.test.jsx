import { render, screen, waitFor } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import TaskCommunication from './TaskCommunication'
import { api } from '../api/client'

vi.mock('../api/client', () => ({ api: { queryMessages: vi.fn() } }))
it('shows the real command and response in order and aborts its owned request', async () => {
  api.queryMessages.mockResolvedValue([
    { id: 'result', type: 'result', sender_id: 'hermes', recipient_id: 'codex', timestamp: '2026-09-20T00:00:02Z', payload: { body: 'Finished checking.' } },
    { id: 'command', type: 'command', sender_id: 'codex', recipient_id: 'hermes', timestamp: '2026-09-20T00:00:01Z', payload: { body: 'Check the task.' } },
  ])
  const view = render(<TaskCommunication taskId="task-a" historical={false} />)
  await screen.findByText('Finished checking.')
  expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual([
    expect.stringContaining('Check the task.'), expect.stringContaining('Finished checking.'),
  ])
  const [params, signal] = api.queryMessages.mock.calls.at(-1)
  expect(params).toEqual({ task_id: 'task-a', limit: 100 })
  view.unmount()
  expect(signal.aborted).toBe(true)
})
it('does not present current messages as a historical snapshot', async () => {
  api.queryMessages.mockClear()
  render(<TaskCommunication taskId="task-a" historical />)
  await waitFor(() => expect(screen.getByText(/Resume live/)).toBeVisible())
  expect(api.queryMessages).not.toHaveBeenCalled()
})
