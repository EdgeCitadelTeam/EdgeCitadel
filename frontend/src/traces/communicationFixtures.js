import { graph, fixture } from './testFixtures'

export const communicationFixture = () => {
  const g = { ...graph(), nodes: [
    { id: `run:${graph().trace_id}`, kind: 'run', agent_id: 'Codex', state: 'interrupted' },
    ...['t1', 't2'].map(id => ({ id: `task:${id}`, task_id: id, kind: 'task', agent_id: 'Hermes', state: 'completed' })),
  ], edges: [] }
  let seq = 0
  const event = (task, message, type, phase, extra = {}) => ({ ...fixture.input_events[0], node_id: 'source', source_epoch: 'epoch', event_id: `e${++seq}`, source_seq: seq, kind: 'transport', agent_id: null, task_id: task, execution_attempt_id: null, phase, attributes: { message_id: message, message_type: type, sender_id: type === 'command' ? 'Codex' : 'Hermes', recipient_id: type === 'command' ? 'Hermes' : 'Codex' }, ...extra })
  const events = ['t1', 't2'].flatMap(task => [event(task, `${task}-request`, 'command', 'publish_started'), event(task, `${task}-request`, 'command', 'received'), event(task, `${task}-return`, 'result', 'publish_accepted'), event(task, `${task}-return`, 'result', 'caller_accepted')])
  return { g, events, event }
}
