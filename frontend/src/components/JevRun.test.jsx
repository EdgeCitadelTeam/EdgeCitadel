import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import CommandInput from './CommandInput'
import JevRun from './JevRun'
import MessageBubble from './MessageBubble'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'

vi.mock('../api/client', () => ({ api: { sendCommand: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn(), success: vi.fn() } }))
const runId = '3034c407-c7a1-487a-8be2-37d05539b3fc'
const taskId = 'baf8810c-0ea3-4f06-b189-bbcc6d37bfa2'

beforeEach(() => {
  vi.clearAllMocks()
  useAppStore.setState({ agents: [{ agent_id: 'jev', agent_state: 'online' },
    { agent_id: 'worker', agent_state: 'online' }], selectedAgent: 'worker',
    showTestAgents: false, trackedTaskId: null, pendingCommands: {} })
  api.sendCommand.mockResolvedValue({ task_id: taskId })
})

afterEach(() => vi.unstubAllGlobals())

it('submits JEV with a stable request ID after an unconfirmed response and retains direct commands', async () => {
  vi.stubGlobal('crypto', { getRandomValues: crypto.getRandomValues.bind(crypto) })
  api.sendCommand.mockRejectedValueOnce(new Error('network'))
  render(<CommandInput />)
  fireEvent.change(screen.getByRole('textbox', { name: 'Command body' }), { target: { value: 'explain gravity' } })
  fireEvent.click(screen.getByRole('button', { name: '交给 JEV' }))
  await waitFor(() => expect(screen.getByRole('button', { name: '交给 JEV' })).not.toBeDisabled())
  const first = api.sendCommand.mock.calls[0]
  expect(first[2].request_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
  expect(first).toEqual(['jev', 'explain gravity', { request_id: expect.any(String) }, 'jev.run'])
  fireEvent.click(screen.getByRole('button', { name: '交给 JEV' }))
  await waitFor(() => expect(api.sendCommand).toHaveBeenCalledTimes(2))
  expect(api.sendCommand.mock.calls[1]).toEqual(first)
  await waitFor(() => expect(screen.getByRole('textbox')).toHaveValue(''))
  fireEvent.change(screen.getByRole('textbox'), { target: { value: 'direct' } })
  fireEvent.click(screen.getByRole('button', { name: 'Send command' }))
  await waitFor(() => expect(api.sendCommand).toHaveBeenLastCalledWith('worker', 'direct'))
})

it('shows offline state and prevents JEV submission and resume', () => {
  useAppStore.setState({ agents: [] })
  render(<><CommandInput /><JevRun run={{ run_id: runId, outcome: 'interrupted', resumable: true }} /></>)
  expect(screen.getByRole('button', { name: '交给 JEV' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '继续' })).toBeDisabled()
  expect(screen.getAllByText('JEV offline')).toHaveLength(2)
})

it('renders result, actual executor and task link; resume references the existing run', async () => {
  render(<MessageBubble message={{ sender_id: 'jev', type: 'result', payload: {
    body: 'Waiting interrupted', run_id: runId, outcome: 'interrupted', resumable: true,
    steps: [{ executor: 'jim-eq-hermes', task_id: taskId, observed_state: 'running' }],
  } }} />)
  expect(screen.getByText('Waiting interrupted')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: /jim-eq-hermes/ }))
  expect(window.location.hash).toContain(`task=${taskId}`)
  fireEvent.click(screen.getByRole('button', { name: '继续' }))
  await waitFor(() => expect(api.sendCommand).toHaveBeenCalledWith('jev', '继续已有运行', { run_id: runId }, 'jev.resume'))
})
