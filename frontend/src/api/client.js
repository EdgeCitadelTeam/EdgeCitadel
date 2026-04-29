// v0.1 messaging contract — canonical /api endpoints (Tasks 6 + 14).
// Uses native fetch; previous axios surface (`agentApi`, `messageApi`, ...)
// is replaced by a single `api` object with the methods listed below.

const API_BASE = import.meta.env.VITE_API_URL || '/api'

async function req(path, opts = {}) {
  const r = await fetch(`${API_BASE}${path}`, opts)
  if (!r.ok) {
    const detail = await r.text().catch(() => '')
    throw new Error(`${r.status} ${detail}`)
  }
  if (r.status === 204) return null
  return r.json()
}

export const api = {
  // System
  systemStatus: () => req('/system/status'),

  // Agents
  listAgents: () => req('/agents'),
  getAgent: (id) => req(`/agents/${encodeURIComponent(id)}`),
  getAgentCard: (id) => req(`/agents/${encodeURIComponent(id)}/card`),
  getAgentQueue: (id) => req(`/agents/${encodeURIComponent(id)}/queue`),
  deleteAgent: (id) =>
    req(`/agents/${encodeURIComponent(id)}`, { method: 'DELETE' }),

  // Commands (returns {task_id, recipient_id, accepted_at})
  sendCommand: (agentId, body, args) =>
    req(`/command/${encodeURIComponent(agentId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ body, ...(args ? { args } : {}) }),
    }),

  // Messages — accepts {agent_id, task_id, context_id, type, deployment,
  //                     exclude_deployment, since_ts, limit}
  queryMessages: (params = {}) => {
    const filtered = Object.fromEntries(
      Object.entries(params).filter(([, v]) => v !== undefined && v !== null && v !== '')
    )
    const qs = new URLSearchParams(filtered).toString()
    return req(`/messages${qs ? `?${qs}` : ''}`)
  },

  // Poison events — agent_id optional
  queryPoison: (agentId, limit = 100) => {
    const params = { limit }
    if (agentId) params.agent_id = agentId
    const qs = new URLSearchParams(params).toString()
    return req(`/poison?${qs}`)
  },
}

export default api
