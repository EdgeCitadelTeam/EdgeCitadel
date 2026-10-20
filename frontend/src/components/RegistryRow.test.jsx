import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import RegistryRow from './RegistryRow'

describe('RegistryRow', () => {
  it('passes the agent state to its accessible status', () => {
    render(
      <table><tbody><RegistryRow row={{
        agent_id: 'shell-1', agent_state: 'online',
        card: { metadata: { 'runtime.roles': ['worker'], 'runtime.kind': 'native' } },
        queue: { pending: 0, ack_pending: 0 }, poison_count: 0,
      }} onClick={() => undefined} showTestAgents={false} /></tbody></table>,
    )
    expect(screen.getByRole('status', { name: 'Status: online' })).toBeInTheDocument()
  })
})
