// v0.1 messaging contract — canonical /api endpoints (Tasks 6 + 14).
// Uses native fetch; previous axios surface (`agentApi`, `messageApi`, ...)
// is replaced by a single `api` object with the methods listed below.

const API_BASE = import.meta.env.VITE_API_URL || '/api'


function buildQuery(params = {}) {
  const filtered = Object.fromEntries(
    Object.entries(params)
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => [k, Array.isArray(v) ? v.join(',') : v])
  )
  return new URLSearchParams(filtered).toString()
}

async function req(path, opts = {}) {
  const r = await fetch(`${API_BASE}${path}`, opts)
  if (!r.ok) {
    const detail = await r.text().catch(() => '')
    throw new Error(`${r.status} ${detail}`)
  }
  if (r.status === 204) return null
  return r.json()
}

export function getCandles(symbol, range, interval) {
  const qs = buildQuery({ symbol, range, interval })
  return req(`/market/candles${qs ? `?${qs}` : ''}`)
}

export function getTechnicalIndicators(symbol, range, indicators = []) {
  const qs = buildQuery({ symbol, range, indicators })
  return req(`/market/technical-indicators${qs ? `?${qs}` : ''}`)
}

export const api = {
  // System
  systemStatus: () => req('/system/status'),

  // Market data
  getCandles,
  getTechnicalIndicators,

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
    const qs = buildQuery(filtered)
    return req(`/messages${qs ? `?${qs}` : ''}`)
  },

  // Poison events — agent_id optional
  queryPoison: (agentId, limit = 100) => {
    const params = { limit }
    if (agentId) params.agent_id = agentId
    const qs = buildQuery(params)
    return req(`/poison?${qs}`)
  },

  // Registry — fleet snapshot used by the Registry tab
  getRegistry: () => req('/registry'),
}

export default api
