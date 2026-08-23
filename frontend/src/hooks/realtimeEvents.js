export function applyRealtimeEvent(frame, actions) {
  const data = frame && frame.data
  if (!frame || !data) return

  if (frame.event === 'message') {
    if (data.type === 'task.progress') {
      const delta = data.payload?.message ?? data.payload?.delta ?? ''
      actions.appendStreamDelta(data.task_id, data.sender_id, delta, data.payload?.skill_id)
      return
    }
    if (data.type === 'result' && data.task_id) actions.finalizeStream(data.task_id, data)
    if (data.type !== 'heartbeat' && data.type !== 'register') actions.addRealtimeMessage(data)
    return
  }

  if (frame.event === 'agent_status_change') {
    actions.updateAgentStatus(data.agent_id, data.agent_state)
    if (data.agent_state === 'offline') {
      actions.addNotification({ type: 'warning', title: 'Agent Offline', message: `${data.agent_id} went offline` })
    }
    return
  }

  if (frame.event === 'agent_registered') {
    const fleetRow = { agent_id: data.agent_id, card: data.card, agent_state: 'online' }
    const registryRow = {
      ...fleetRow, last_heartbeat: null, last_register: null,
      deployment: data.card?.metadata?.['runtime.deployment'] ?? null,
      queue: { pending: 0, ack_pending: 0 }, poison_count: 0,
    }
    actions.upsertAgent(fleetRow)
    actions.upsertRegistryRow(registryRow)
    actions.addNotification({ type: 'info', title: 'Agent Registered', message: `${data.agent_id} connected` })
    return
  }

  if (frame.event === 'agent_deleted') actions.removeAgent(data.agent_id)
}
