import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import App from './App'
import HeaderBar from './components/HeaderBar'
import useWebSocket from './hooks/useWebSocket'
import useAppStore from './stores/appStore'

vi.mock('./hooks/useWebSocket', () => ({
  default: vi.fn(),
}))
vi.mock('./Layout', () => ({
  default: () => <main>layout</main>,
}))
vi.mock('./components/Toast', () => ({
  default: () => null,
}))
vi.mock('./api/client', () => ({
  api: {
    systemStatus: vi.fn().mockResolvedValue({
      nats_connected: true,
      jetstream_stream_ok: true,
    }),
  },
}))

describe('application product contract', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useAppStore.setState({
      selectedAgent: null,
      agents: [],
      notifications: [],
      systemStatus: null,
      wsConnected: true,
      showTestAgents: false,
      sidebarOpen: false,
    })
  })

  it('keeps one fleet stream after agent selection', () => {
    useAppStore.setState({ selectedAgent: 'shell-1', notifications: [] })

    render(<App />)

    expect(useWebSocket).toHaveBeenCalledTimes(1)
    expect(useWebSocket).toHaveBeenCalledWith()
  })

  it('uses the EdgeCitadel product name', () => {
    render(<HeaderBar />)

    expect(
      screen.getByRole('heading', { name: 'EdgeCitadel' }),
    ).toBeInTheDocument()
  })

  it('does not claim an unsupported light theme', () => {
    render(<HeaderBar />)

    expect(screen.getAllByRole('button')).toHaveLength(2)
    expect(
      screen.getByRole('button', { name: 'Open agent list' }),
    ).toBeInTheDocument()
    expect(
      screen.getByRole('button', { name: 'Show test data' }),
    ).toBeInTheDocument()
  })
})
