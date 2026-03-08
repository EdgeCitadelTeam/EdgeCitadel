import axios from 'axios'
import useAppStore from '../stores/appStore'

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || '/api',
})

// Automatically add exclude_test param when test data is hidden
api.interceptors.request.use((config) => {
  if (!useAppStore.getState().showTestAgents) {
    config.params = { ...config.params, exclude_test: true }
  }
  return config
})

export const agentApi = {
  list: () => api.get('/agents'),
  get: (id) => api.get(`/agents/${id}`),
  create: (data) => api.post('/agents', data),
  update: (id, data) => api.patch(`/agents/${id}`, data),
  delete: (id) => api.delete(`/agents/${id}`),
  stats: (id) => api.get(`/agents/${id}/stats`),
}

export const messageApi = {
  list: (params) => api.get('/messages', { params }),
  conversations: (params) => api.get('/messages/conversations', { params }),
  flow: (params) => api.get('/messages/flow', { params }),
}

export const taskApi = {
  list: (params) => api.get('/tasks', { params }),
  create: (data) => api.post('/tasks', data),
  update: (id, data) => api.patch(`/tasks/${id}`, data),
  trace: (id) => api.get(`/tasks/${id}/trace`),
}

export const logApi = {
  list: (params) => api.get('/logs', { params }),
}

export const commandApi = {
  send: (agentName, data) => api.post(`/command/${agentName}`, data),
  broadcast: (data) => api.post('/broadcast', data),
}

export const systemApi = {
  status: () => api.get('/system/status'),
  topology: (params) => api.get('/system/topology', { params }),
}

export default api
