import { fireEvent, render, screen } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import CommunicationMap from './CommunicationMap'
import CommunicationDetails from './CommunicationDetails'
import { projectCommunication, eventKey } from './communication'
import { communicationFixture } from './communicationFixtures'
import { parseTraceRoute, navigateTrace } from './navigation'

it('selects messages, tasks and agents with keyboard-accessible buttons and paginates whole messages', () => {
  const { g, events, event } = communicationFixture()
  for (let i = 0; i < 52; i++) events.push(event(`t${i + 3}`, `m${i}`, 'command', 'received'))
  const p = projectCommunication(g, events), select = vi.fn()
  const view = render(<CommunicationMap projection={p} onSelect={select} />)
  expect(view.container.querySelectorAll('[data-agent-id]')).toHaveLength(2)
  expect(view.container.querySelectorAll('[data-arrow-id]')).toHaveLength(1)
  expect(view.container.querySelectorAll('[data-message-id]')).toHaveLength(50)
  fireEvent.click(view.container.querySelector('[data-message-id]'))
  expect(select.mock.calls.at(-1)[0]).toBe(p.messages[0])
  view.rerender(<CommunicationMap projection={p} selected={p.messages[0]} onSelect={select} />)
  expect(view.container.querySelectorAll('[data-arrow-id]')).toHaveLength(1)
  fireEvent.click(screen.getByRole('button', { name: 'Next interactions' }))
  expect(view.container.querySelectorAll('[data-message-id]')).toHaveLength(6)
  expect(screen.getByRole('list', { name: 'Communication list' })).toBeVisible()
  expect(screen.queryByRole('textbox')).not.toBeInTheDocument()
})
it('opens overview for communication, expands long content and supports all tabs plus original event evidence', () => {
  const { g, events } = communicationFixture()
  events[0].content = { status: 'available', fields: { body: JSON.stringify('Long request. '.repeat(120)) } }
  events.push({ ...events[0], event_id: 'broker', source_seq: 100, kind: 'broker', content: { status: 'available', fields: { boundaries: 'broker-only-evidence' } } })
  const p = projectCommunication(g, events), close = vi.fn()
  const props = { selection: p.messages[0], projection: p, route: { message: p.messages[0].id, at: g.at }, onClose: close, onExpand: vi.fn(), onEvent: vi.fn(), partial: true }
  const view = render(<CommunicationDetails {...props} />)
  expect(screen.getByRole('tab', { name: 'Overview' })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByText(/Evidence loading is incomplete/)).toBeVisible()
  expect(screen.queryByText('broker-only-evidence')).not.toBeInTheDocument()
  const summary = screen.getByText(/Expand content/)
  fireEvent.click(summary)
  expect(summary.closest('details')).toHaveAttribute('open')
  fireEvent.keyDown(screen.getByRole('tab', { name: 'Overview' }), { key: 'ArrowRight' })
  expect(screen.getByRole('tab', { name: 'Execution' })).toHaveFocus()
  fireEvent.click(screen.getByRole('tab', { name: 'Evidence' }))
  expect(screen.getByText(/Source sequence establishes/)).toBeVisible()
  fireEvent.keyDown(screen.getByLabelText('Communication details'), { key: 'Escape' })
  expect(close).toHaveBeenCalled()
  view.unmount()
  render(<CommunicationDetails {...props} route={{ event: eventKey(events[0]), at: g.at }} />)
  expect(screen.getByRole('tab', { name: 'Evidence' })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByText(eventKey(events[0])).closest('details')).toHaveAttribute('open')
})
it('roundtrips an aggregate message link while preserving original route fields', () => {
  const { g } = communicationFixture()
  navigateTrace({ run: g.trace_id, at: g.at, message: 'message-123', step: 'task:abc' })
  expect(parseTraceRoute(window.location.hash)).toMatchObject({ run: g.trace_id, at: g.at, message: 'message-123', step: 'task:abc', invalid: false })
})

it('shows configured endpoints on core, leaf and agent cards without inventing agent listeners', () => {
  const { g, events } = communicationFixture()
  const projection = projectCommunication(g, events)
  const topology = { agents: projection.agents.map(agent => ({ id: agent.id, nodeId: 'host-one', hostName: 'Host one', mode: 'nats_leaf', address: '127.0.0.1:4223', coreAddress: '192.0.2.10:4222' })) }
  topology.agents[1].address = undefined // An offline agent shares its host's current transport.
  const view = render(<CommunicationMap projection={projection} topology={topology} onSelect={vi.fn()} />)
  expect(screen.getByText('192.0.2.10:4222')).toBeVisible()
  expect(screen.getByText('127.0.0.1:4223')).toBeVisible()
  expect(screen.getAllByText('via NATS 127.0.0.1:4223')).toHaveLength(2)
  view.rerender(<CommunicationMap projection={projection} onSelect={vi.fn()} />)
  expect(screen.queryByText('192.0.2.10:4222')).not.toBeInTheDocument()
  expect(screen.getAllByText('IP / port not recorded')).toHaveLength(5)
})
