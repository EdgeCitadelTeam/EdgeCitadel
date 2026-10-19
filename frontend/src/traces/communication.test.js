import { expect, it } from 'vitest'
import { bodyTitle, eventKey, eventNodeId, orderEvents, projectCommunication } from './communication'
import { communicationFixture } from './communicationFixtures'

it('combines duplicate delivery, broker and confirmation without changing result semantics or task identity', () => {
  const { g, events, event } = communicationFixture()
  events.push(event('t1', 't1-return', 'result', 'received', { kind: 'broker' }), events[0])
  const p = projectCommunication(g, events)
  expect(p.agents.map(a => a.id)).toEqual(['Codex', 'Hermes'])
  expect(p.agents[0].state).toBe('interrupted')
  expect(p.tasks).toHaveLength(2)
  expect(p.tasks.every(t => t.state === 'completed')).toBe(true)
  expect(p.messages).toHaveLength(4)
  expect(p.messages.find(m => m.id === 't1-return').events).toHaveLength(3)
  expect(p.interactions.filter(row => row.message)).toHaveLength(4)
  expect(p.interactions.filter(row => row.task)).toHaveLength(2)
})
it('preserves attempts and failed tools, collapses progress, and maps original nodes/events to their task', () => {
  const { g, events, event } = communicationFixture()
  const steps = ['a1', 'a2'].map(id => event('t1', null, null, 'failed', { kind: 'tool', span_id: id, execution_attempt_id: id, attributes: { name: 'shell' }, agent_id: 'Hermes' }))
  g.nodes.push(...steps.map(e => ({ id: eventNodeId(e), task_id: 't1', kind: 'tool', state: 'failed' })))
  events.push(...steps, event('t1', 'progress', 'progress', 'publish_accepted'))
  const p = projectCommunication(g, events), task = p.tasks.find(t => t.id === 't1')
  expect(task.attempts.map(a => JSON.parse(a.id)[2])).toEqual(['a1', 'a2'])
  expect(task.failures).toBe(2)
  expect(p.interactions).toHaveLength(4)
  expect(p.nodeSelection.get(eventNodeId(steps[0]))).toBe(task)
  expect(p.eventSelection.get(eventKey(steps[0]))).toBe(task)
})
it('does not invent recipients and preserves conflicting evidence', () => {
  const { g, events, event } = communicationFixture()
  events.push(event(null, 'unknown', 'command', 'publish_started', { attributes: { message_id: 'unknown', sender_id: 'Codex', message_type: 'command' } }))
  events.push(event('t1', 't1-request', 'command', 'received', { attributes: { message_id: 't1-request', sender_id: 'Third', recipient_id: 'Hermes' } }))
  const p = projectCommunication(g, events)
  expect(p.messages.find(m => m.id === 'unknown').recipient).toBeNull()
  expect(p.messages.find(m => m.id === 't1-request').conflicts).toEqual(['sender'])
  expect(p.evidence.events).toHaveLength(4)
  expect(p.interactions.filter(row => row.message)).toHaveLength(3)
})
it('uses source sequence even when arrival/time is reversed, includes additional Agents and never fabricates a missing return', () => {
  const { g, events, event } = communicationFixture()
  const third = event('t3', 'third', 'command', 'received', { attributes: { message_id: 'third', message_type: 'command', sender_id: 'Hermes', recipient_id: 'Third' } })
  events.push(third)
  expect(orderEvents([...events].reverse()).events.map(eventKey)).toEqual(orderEvents(events).events.map(eventKey))
  const p = projectCommunication(g, [...events].reverse())
  expect(p.agents.map(a => a.id)).toEqual(['Codex', 'Hermes', 'Third'])
  expect(p.tasks.find(t => t.id === 't3').messages).toHaveLength(1)
})

 it('names tasks from nested command requests and shortens long instructions', () => {
  const { g, events } = communicationFixture()
  events[0].content = { fields: { body: JSON.stringify({ body: 'Review the deployment configuration for production' }) } }
  const p = projectCommunication(g, events)
  expect(p.tasks[0].title).toBe('Review the deployment configuration for production')
  expect(p.tasks[1].title).toBe('Task t2')
  expect(bodyTitle([{ content: { fields: { request: 'Investigate why the production deployment fails after the database migration finishes' } } }])).toBe('Investigate why the production deployment fails after the…')
})
