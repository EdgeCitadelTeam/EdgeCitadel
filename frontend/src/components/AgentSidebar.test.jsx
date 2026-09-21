import { act, fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, expect, it, vi } from 'vitest'
import AgentSidebar from './AgentSidebar'
import HeaderBar from './HeaderBar'
import useAppStore from '../stores/appStore'
import { applyRealtimeEvent } from '../hooks/realtimeEvents'

vi.mock('../api/client', () => ({ api: {
  listAgents: vi.fn(async () => []), systemStatus: vi.fn(async () => ({})),
} }))
const real = { agent_id: 'real-worker', agent_state: 'online', card: {} }
const fixture = { agent_id: 'fixture-worker', agent_state: 'online', card: { metadata: { 'runtime.deployment': 'test' } } }

beforeEach(() => {
  useAppStore.setState({ agents: [], selectedAgent: null, showTestAgents: false })
})

it('filters live registrations immediately and toggles without waiting for polling', async () => {
  render(<><HeaderBar /><AgentSidebar /></>)
  await act(async () => {})
  act(() => {
    useAppStore.getState().upsertAgent(real)
    applyRealtimeEvent({ event: 'agent_registered', data: fixture }, useAppStore.getState())
  })
  expect(screen.getByText('real-worker')).toBeVisible()
  expect(screen.queryByText('fixture-worker')).not.toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Show test data' }))
  expect(screen.getByText('fixture-worker')).toBeVisible()
  fireEvent.click(screen.getByText('fixture-worker'))
  expect(useAppStore.getState().selectedAgent).toBe('fixture-worker')
  fireEvent.click(screen.getByRole('button', { name: 'Hide test data' }))
  expect(screen.queryByText('fixture-worker')).not.toBeInTheDocument()
  expect(useAppStore.getState().selectedAgent).toBeNull()
  expect(useAppStore.getState().agents).toHaveLength(2)
})

it('preserves test provenance on synthetic live and recovered streams', () => {
  const actions = useAppStore.getState()
  applyRealtimeEvent({ event: 'message', data: {
    type: 'task.progress', task_id: 'test-stream', sender_id: 'real-worker',
    deployment: 'test', payload: { message: 'part' },
  } }, actions)
  actions.seedStreamFromHistory('recovered-test', 'real-worker', 'part', null, null, 'test')
  expect(useAppStore.getState().realtimeMessages.filter(row => row.task_id.endsWith('test') || row.task_id === 'test-stream'))
    .toEqual(expect.arrayContaining([expect.objectContaining({ task_id: 'test-stream', deployment: 'test' }), expect.objectContaining({ task_id: 'recovered-test', deployment: 'test' })]))
})
