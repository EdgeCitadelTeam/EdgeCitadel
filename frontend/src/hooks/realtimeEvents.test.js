import { describe, expect, it, vi } from 'vitest'
import { applyRealtimeEvent } from './realtimeEvents'

function actions() {
  return Object.fromEntries(
    [
      'addRealtimeMessage',
      'appendStreamDelta',
      'finalizeStream',
      'updateAgentStatus',
      'upsertAgent',
      'upsertRegistryRow',
      'removeAgent',
      'addNotification',
    ].map((name) => [name, vi.fn()]),
  )
}

describe('applyRealtimeEvent', () => {
  it('applies an offline status and notifies', () => {
    const next = actions()
    applyRealtimeEvent(
      { event: 'agent_status_change', data: { agent_id: 'shell-1', agent_state: 'offline' } },
      next,
    )
    expect(next.updateAgentStatus).toHaveBeenCalledWith('shell-1', 'offline')
    expect(next.addNotification).toHaveBeenCalledWith(expect.objectContaining({ type: 'warning' }))
  })

  it('upserts a registered agent into fleet and registry', () => {
    const next = actions()
    const card = { metadata: { 'runtime.deployment': 'test' } }
    applyRealtimeEvent({ event: 'agent_registered', data: { agent_id: 'shell-1', card } }, next)
    expect(next.upsertAgent).toHaveBeenCalledWith(expect.objectContaining({ agent_state: 'online' }))
    expect(next.upsertRegistryRow).toHaveBeenCalledWith(expect.objectContaining({ agent_state: 'online' }))
  })

  it('removes a deleted agent from both projections', () => {
    const next = actions()
    applyRealtimeEvent({ event: 'agent_deleted', data: { agent_id: 'shell-1' } }, next)
    expect(next.removeAgent).toHaveBeenCalledWith('shell-1')
  })

  it('adds a normal message', () => {
    const next = actions()
    const data = { type: 'command', id: 'wire-1' }
    applyRealtimeEvent({ event: 'message', data }, next)
    expect(next.addRealtimeMessage).toHaveBeenCalledWith(data)
  })

  it('turns progress into a stream delta without adding a raw bubble', () => {
    const next = actions()
    applyRealtimeEvent({
      event: 'message',
      data: { type: 'task.progress', task_id: 'task-1', sender_id: 'shell-1', payload: { message: 'part', skill_id: 'echo' } },
    }, next)
    expect(next.appendStreamDelta).toHaveBeenCalledWith('task-1', 'shell-1', 'part', 'echo')
    expect(next.addRealtimeMessage).not.toHaveBeenCalled()
  })

  it('finalizes and adds a terminal result', () => {
    const next = actions()
    const data = { type: 'result', task_id: 'task-1', id: 'wire-2' }
    applyRealtimeEvent({ event: 'message', data }, next)
    expect(next.finalizeStream).toHaveBeenCalledWith('task-1', data)
    expect(next.addRealtimeMessage).toHaveBeenCalledWith(data)
  })
})
