import { create } from 'zustand'
import { api } from '../api/client'

const MAX_REALTIME_MESSAGES = 500

const useAppStore = create((set, get) => ({
  // Agents — items shaped like {agent_id, card, agent_state, last_heartbeat,
  // last_register, deployment, heartbeat_interval_sec}
  agents: [],
  selectedAgent: null,

  // Navigation
  activeTab: 'chat',

  // Real-time messages — canonical envelopes
  realtimeMessages: [],
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

  // Theme
  darkMode: true,

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

  setSelectedAgent: (agent) => set({ selectedAgent: agent }),

  updateAgentStatus: (agentId, agentState) =>
    set((state) => ({
      agents: state.agents.map((a) =>
        a.agent_id === agentId ? { ...a, agent_state: agentState } : a
      ),
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
  setDarkMode: (dark) => set({ darkMode: dark }),

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
