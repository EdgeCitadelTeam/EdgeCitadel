import { create } from 'zustand'

const MAX_REALTIME_MESSAGES = 500

const useAppStore = create((set, get) => ({
  // Agents
  agents: [],
  selectedAgent: null,

  // Navigation
  activeTab: 'chat',

  // Real-time
  realtimeMessages: [],
  wsConnected: false,

  // Filters
  messageTypeFilter: null,
  logLevelFilter: null,
  taskStatusFilter: null,

  // Pending commands awaiting reply (correlation_id -> { target, text, sentAt })
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

  // Actions
  setAgents: (agents) => set({ agents }),

  setSelectedAgent: (agent) => set({ selectedAgent: agent }),

  updateAgentStatus: (agentId, status) =>
    set((state) => ({
      agents: state.agents.map((a) =>
        a.id === agentId ? { ...a, status } : a
      ),
    })),

  addPendingCommand: (correlationId, target) =>
    set((state) => ({
      pendingCommands: { ...state.pendingCommands, [correlationId]: { target, sentAt: Date.now() } },
    })),

  removePendingCommand: (correlationId) =>
    set((state) => {
      const next = { ...state.pendingCommands }
      delete next[correlationId]
      return { pendingCommands: next }
    }),

  addRealtimeMessage: (message) =>
    set((state) => {
      const msgs = [message, ...state.realtimeMessages]
      // Clear pending if this is a reply
      const pending = { ...state.pendingCommands }
      if (message.correlation_id && pending[message.correlation_id] && message.message_type !== 'command') {
        delete pending[message.correlation_id]
      }
      return { realtimeMessages: msgs.slice(0, MAX_REALTIME_MESSAGES), pendingCommands: pending }
    }),

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
}))

export default useAppStore
