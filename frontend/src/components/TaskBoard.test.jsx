import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import TaskBoard from './TaskBoard'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'

vi.mock('../api/client', () => ({
  api: {
    queryMessages: vi.fn(),
  },
}))
vi.mock('./TaskCard', () => ({
  default: ({ task }) => <div>{`${task.task_id}:${task.task_state}`}</div>,
}))

function deferred() {
  let resolve
  let reject
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve
    reject = nextReject
  })
  return { promise, resolve, reject }
}

function command() {
  return {
    id: 'command-1',
    observation_index: 1,
    type: 'command',
    sender_id: 'aggregator',
    recipient_id: 'shell-1',
    task_id: 'task-1',
    context_id: 'context-1',
    hop_count: 0,
    timestamp: '2026-07-25T12:00:00.000Z',
    payload: { body: 'nonce-1' },
  }
}

function completedHistory() {
  return [
    command(),
    {
      id: 'result-1',
      observation_index: 2,
      type: 'result',
      sender_id: 'shell-1',
      recipient_id: 'aggregator',
      task_id: 'task-1',
      context_id: 'context-1',
      hop_count: 0,
      task_state: 'completed',
      timestamp: '2026-07-25T12:00:01.000Z',
      payload: { body: 'edgecitadel:nonce-1' },
    },
  ]
}

describe('TaskBoard request ordering', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useAppStore.setState({
      selectedAgent: null,
      notifications: [],
      addNotification: vi.fn(),
    })
  })

  it('retains the newest task response when an earlier request resolves late', async () => {
    const first = deferred()
    const second = deferred()
    api.queryMessages.mockImplementationOnce(() => first.promise)
    api.queryMessages.mockImplementationOnce(() => second.promise)

    render(<TaskBoard />)
    await waitFor(() => expect(api.queryMessages).toHaveBeenCalledTimes(1))
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }))
    await waitFor(() => expect(api.queryMessages).toHaveBeenCalledTimes(2))

    second.resolve(completedHistory())
    await screen.findByText('task-1:completed')
    first.resolve([command()])
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(screen.getByText('task-1:completed')).toBeInTheDocument()
  })

  it('retains valid tasks and reports malformed observation history', async () => {
    const invalid = deferred()
    api.queryMessages.mockResolvedValueOnce(completedHistory())
    api.queryMessages.mockImplementationOnce(() => invalid.promise)
    const addNotification = useAppStore.getState().addNotification

    render(<TaskBoard />)
    await screen.findByText('task-1:completed')
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }))
    await waitFor(() => expect(api.queryMessages).toHaveBeenCalledTimes(2))

    invalid.resolve([
      command(),
      { ...command(), id: 'duplicate-index', task_id: 'task-2' },
    ])

    await waitFor(() => {
      expect(addNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'error',
          title: 'Task observation rejected',
        }),
      )
    })
    expect(screen.getByText('task-1:completed')).toBeInTheDocument()
  })
})
