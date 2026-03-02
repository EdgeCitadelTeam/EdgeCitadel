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

  // System
  systemStatus: null,
  notifications: [],

  // Theme
  darkMode: true,

  // Actions
  setAgents: (agents) => set({ agents }),

  setSelectedAgent: (agent) => set({ selectedAgent: agent }),

  updateAgentStatus: (agentId, status) =>
    set((state) => ({
      agents: state.agents.map((a) =>
        a.id === agentId ? { ...a, status } : a
      ),
    })),

  addRealtimeMessage: (message) =>
    set((state) => {
      const msgs = [message, ...state.realtimeMessages]
      return { realtimeMessages: msgs.slice(0, MAX_REALTIME_MESSAGES) }
    }),

  setActiveTab: (tab) => set({ activeTab: tab }),
  setMessageTypeFilter: (filter) => set({ messageTypeFilter: filter }),
  setLogLevelFilter: (filter) => set({ logLevelFilter: filter }),
  setTaskStatusFilter: (filter) => set({ taskStatusFilter: filter }),
  setSystemStatus: (status) => set({ systemStatus: status }),
  setWsConnected: (connected) => set({ wsConnected: connected }),
  setDarkMode: (dark) => set({ darkMode: dark }),

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
