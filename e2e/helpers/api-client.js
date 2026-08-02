const axios = require('axios');

if (!process.env.AGG_URL) {
  throw new Error('AGG_URL is required');
}

const BASE_URL = `${process.env.AGG_URL}/api`;

class TestAPIClient {
  constructor() {
    this.client = axios.create({
      baseURL: BASE_URL,
      timeout: 10_000,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Health
  async health() {
    return this.client.get('/health');
  }

  // Agents
  async listAgents() {
    return this.client.get('/agents');
  }

  async getAgent(agentId) {
    return this.client.get(`/agents/${agentId}`);
  }

  async createAgent(data) {
    return this.client.post('/agents', data);
  }

  async updateAgent(agentId, data) {
    return this.client.patch(`/agents/${agentId}`, data);
  }

  async deleteAgent(agentId) {
    return this.client.delete(`/agents/${agentId}`);
  }

  async getAgentStats(agentId) {
    return this.client.get(`/agents/${agentId}/stats`);
  }

  // Messages
  async listMessages(params = {}) {
    return this.client.get('/messages', { params });
  }

  async getConversations() {
    return this.client.get('/messages/conversations');
  }

  async getFlow(params = {}) {
    return this.client.get('/messages/flow', { params });
  }

  // Tasks
  async listTasks(params = {}) {
    return this.client.get('/tasks', { params });
  }

  async createTask(data) {
    return this.client.post('/tasks', data);
  }

  async updateTask(taskId, data) {
    return this.client.patch(`/tasks/${taskId}`, data);
  }

  async getTaskTrace(taskId) {
    return this.client.get(`/tasks/${taskId}/trace`);
  }

  // Logs
  async listLogs(params = {}) {
    return this.client.get('/logs', { params });
  }

  // Commands
  async sendCommand(agentName, data) {
    return this.client.post(`/command/${agentName}`, data);
  }

  async broadcast(data) {
    return this.client.post('/broadcast', data);
  }

  // System
  async getSystemStatus() {
    return this.client.get('/system/status');
  }

  async getTopology(params = {}) {
    return this.client.get('/system/topology', { params });
  }
}

module.exports = { TestAPIClient };
