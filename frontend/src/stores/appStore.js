import { create } from 'zustand'
import { api } from '../api/client'

const MAX_REALTIME_MESSAGES = 500

const upsertByAgentId = (rows, incoming) => {
  const index = rows.findIndex((row) => row.agent_id === incoming.agent_id)
  if (index < 0) return rows.concat([incoming])
  return rows.map((row, rowIndex) => {
    if (rowIndex !== index) return row
    const merged = Object.assign({}, row, incoming)
    if (Object.hasOwn(row, 'queue')) merged.queue = row.queue
    if (Object.hasOwn(row, 'poison_count')) merged.poison_count = row.poison_count
    return merged
  })
}

const useAppStore = create((set, get) => ({
  // Agents — items shaped like {agent_id, card, agent_state, last_heartbeat,
  // last_register, deployment, heartbeat_interval_sec}
  agents: [],
  selectedAgent: null,

  // Navigation
  activeTab: 'chat',

  // Real-time messages — canonical envelopes
  realtimeMessages: [],
  trackedTaskId: null,
  wsConnected: false,

  // Filters
  messageTypeFilter: null,
  logLevelFilter: null,
  taskStatusFilter: null,

  // Pending commands awaiting reply, keyed by task_id -> { target, sentAt }
  pendingCommands: {},

  // System
  systemStatus: null,
  notifications: [],

  // Test data toggle (persisted)
  showTestAgents: JSON.parse(localStorage.getItem('showTestAgents') || 'false'),

  // Mobile sidebar
  sidebarOpen: false,

  // v0.1 messaging surfaces
  agentQueue: {}, // agentQueue[agentId] = {pending, ack_pending, num_waiting}
  poisonEvents: {}, // poisonEvents[agentId] = [advisories]

  // Registry tab state — array of RegistryEntry rows. Patched in place
  // by WS events; replaced wholesale on /api/registry refetch.
  registry: [],
  setRegistry: (rows) => set({ registry: rows || [] }),
  patchRegistryRow: (agentId, partial) => set((state) => ({
    registry: state.registry.map((r) =>
      r.agent_id === agentId ? { ...r, ...partial } : r),
  })),
  removeRegistryRow: (agentId) => set((state) => ({
    registry: state.registry.filter((r) => r.agent_id !== agentId),
  })),

  // Actions
  setAgents: (agents) => set({ agents }),

  upsertAgent: (incoming) => set((state) => ({
    agents: upsertByAgentId(state.agents, incoming),
  })),

  upsertRegistryRow: (incoming) => set((state) => ({
    registry: upsertByAgentId(state.registry, incoming),
  })),

  setSelectedAgent: (agent) => set({ selectedAgent: agent }),

  updateAgentStatus: (agentId, agentState) => set((state) => ({
    agents: state.agents.map((row) => row.agent_id === agentId ? { ...row, agent_state: agentState } : row),
    registry: state.registry.map((row) => row.agent_id === agentId ? { ...row, agent_state: agentState } : row),
  })),

  removeAgent: (agentId) => set((state) => ({
    agents: state.agents.filter((row) => row.agent_id !== agentId),
    registry: state.registry.filter((row) => row.agent_id !== agentId),
    selectedAgent: state.selectedAgent === agentId ? null : state.selectedAgent,
  })),

  addPendingCommand: (taskId, target) =>
    set((state) => ({
      pendingCommands: {
        ...state.pendingCommands,
        [taskId]: { target, sentAt: Date.now() },
      },
    })),

  removePendingCommand: (taskId) =>
    set((state) => {
      const next = { ...state.pendingCommands }
      delete next[taskId]
      return { pendingCommands: next }
    }),

  addRealtimeMessage: (message) =>
    set((state) => {
      const msgs = [message, ...state.realtimeMessages]
      // Clear pending if a non-command envelope arrives for this task_id
      const pending = { ...state.pendingCommands }
      if (
        message.task_id &&
        pending[message.task_id] &&
        message.type !== 'command'
      ) {
        delete pending[message.task_id]
      }
      return {
        realtimeMessages: msgs.slice(0, MAX_REALTIME_MESSAGES),
        pendingCommands: pending,
      }
    }),

  setTrackedTaskId: (taskId) => set({ trackedTaskId: taskId }),

  // Phase 2.5 — Streaming bubble reducers.
  // Synthetic streaming bubbles live in the existing realtimeMessages array
  // with streaming: true. When the canonical result envelope arrives,
  // finalizeStream swaps the synthetic for the real one.
  appendStreamDelta: (taskId, senderId, delta, skillId) => set((state) => {
    const idx = state.realtimeMessages.findIndex(
      (m) => m.task_id === taskId && m.streaming === true
    )
    if (idx >= 0) {
      const existing = state.realtimeMessages[idx]
      return {
        realtimeMessages: [
          ...state.realtimeMessages.slice(0, idx),
          {
            ...existing,
            content: existing.content + delta,
            last_delta_at: Date.now(),
          },
          ...state.realtimeMessages.slice(idx + 1),
        ],
      }
    }
    const synth = {
      id: `stream-${taskId}`,
      task_id: taskId,
      sender_id: senderId,
      type: 'result',
      streaming: true,
      skill_id: skillId,
      content: delta,
      timestamp: new Date().toISOString(),
      last_delta_at: Date.now(),
    }
    return {
      realtimeMessages: [synth, ...state.realtimeMessages].slice(0, MAX_REALTIME_MESSAGES),
    }
  }),

  // Page-refresh recovery: seed a synthetic streaming bubble from
  // already-persisted task.progress chunks when no live one exists yet.
  // Idempotent — does nothing if a synthetic for this task is already in
  // realtimeMessages, so subsequent live deltas extend the same bubble via
  // appendStreamDelta. `lastDeltaIso` is the timestamp of the freshest
  // chunk used for stall detection.
  seedStreamFromHistory: (taskId, senderId, content, skillId, lastDeltaIso) =>
    set((state) => {
      const exists = state.realtimeMessages.some(
        (m) => m.task_id === taskId && m.streaming === true
      )
      if (exists || !content) return {}
      const synth = {
        id: `stream-${taskId}`,
        task_id: taskId,
        sender_id: senderId,
        type: 'result',
        streaming: true,
        skill_id: skillId,
        content,
        timestamp: lastDeltaIso || new Date().toISOString(),
        last_delta_at: lastDeltaIso ? new Date(lastDeltaIso).getTime() : Date.now(),
      }
      return {
        realtimeMessages: [synth, ...state.realtimeMessages]
          .slice(0, MAX_REALTIME_MESSAGES),
      }
    }),

  finalizeStream: (taskId, resultEnvelope) => set((state) => ({
    realtimeMessages: state.realtimeMessages.map((m) =>
      m.task_id === taskId && m.streaming
        ? { ...resultEnvelope, streaming: false }
        : m
    ),
  })),

  setActiveTab: (tab) => set({ activeTab: tab }),
  setMessageTypeFilter: (filter) => set({ messageTypeFilter: filter }),
  setLogLevelFilter: (filter) => set({ logLevelFilter: filter }),
  setTaskStatusFilter: (filter) => set({ taskStatusFilter: filter }),
  setSystemStatus: (status) => set({ systemStatus: status }),
  setWsConnected: (connected) => set({ wsConnected: connected }),

  setShowTestAgents: (show) => {
    localStorage.setItem('showTestAgents', JSON.stringify(show))
    set({ showTestAgents: show })
  },

  setSidebarOpen: (open) => set({ sidebarOpen: open }),

  addNotification: (notification) =>
    set((state) => ({
      notifications: [
        { id: Date.now(), timestamp: new Date(), ...notification },
        ...state.notifications,
      ].slice(0, 50),
    })),

  clearRealtimeMessages: () => set({ realtimeMessages: [] }),

  // v0.1: queue depth / poison advisories
  fetchAgentQueue: async (agentId) => {
    if (!agentId) return
    try {
      const queue = await api.getAgentQueue(agentId)
      set((state) => ({
        agentQueue: { ...state.agentQueue, [agentId]: queue },
      }))
    } catch (e) {
      // Surface a notification but don't throw — consumer not yet bootstrapped is normal.
      console.warn(`getAgentQueue ${agentId}: ${e.message}`)
    }
  },

  fetchPoisonEvents: async (agentId) => {
    if (!agentId) return
    try {
      const events = await api.queryPoison(agentId)
      set((state) => ({
        poisonEvents: { ...state.poisonEvents, [agentId]: events || [] },
      }))
    } catch (e) {
      console.warn(`queryPoison ${agentId}: ${e.message}`)
    }
  },
}))

export default useAppStore
