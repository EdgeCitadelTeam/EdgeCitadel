const { test, expect } = require('../helpers/fixtures');
const { makeAgent } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Agent Heartbeat', () => {
  test('Heartbeat sets agent status to online', async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await pollUntil(async () => {
      try {
        return (await apiClient.getAgent(agent.name)).data;
      } catch {
        return null;
      }
    }, { label: 'agent exists' });

    await mqttClient.sendHeartbeat(agent.name);
    await sleep(500);

    const res = await apiClient.getAgent(agent.name);
    expect(res.data.status).toBe('online');
  });

  test('Heartbeat updates CPU/memory metrics', async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await pollUntil(async () => {
      try {
        return (await apiClient.getAgent(agent.name)).data;
      } catch {
        return null;
      }
    }, { label: 'agent exists' });

    await mqttClient.sendHeartbeat(agent.name, {
      cpu_percent: 78.5,
      memory_percent: 91.2,
    });

    const result = await pollUntil(async () => {
      const res = await apiClient.getAgent(agent.name);
      if (res.data.cpu_percent === 78.5) return res.data;
      return null;
    }, { label: 'metrics updated' });

    expect(result.cpu_percent).toBe(78.5);
    expect(result.memory_percent).toBe(91.2);
  });

  test('Heartbeat updates last_heartbeat timestamp', async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await pollUntil(async () => {
      try {
        return (await apiClient.getAgent(agent.name)).data;
      } catch {
        return null;
      }
    }, { label: 'agent exists' });

    const before = new Date().toISOString();
    await mqttClient.sendHeartbeat(agent.name);

    const result = await pollUntil(async () => {
      const res = await apiClient.getAgent(agent.name);
      if (res.data.last_heartbeat && res.data.last_heartbeat >= before) return res.data;
      return null;
    }, { label: 'heartbeat timestamp updated' });

    expect(result.last_heartbeat).toBeTruthy();
  });

  test('Heartbeat triggers agent_status_change WebSocket event', async ({ mqttClient, wsClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await sleep(500);
    wsClient.clearEvents();

    await mqttClient.sendHeartbeat(agent.name);

    const event = await wsClient.waitForEvent(
      (e) => e.type === 'agent_status_change' && e.agent_id === agent.name,
      10_000
    );
    expect(event).toBeTruthy();
    expect(event.status).toBe('online');
  });

  test('Heartbeat for unknown agent auto-creates it', async ({ mqttClient, apiClient }) => {
    const agentName = `unknown-${Date.now()}`;
    await mqttClient.sendHeartbeat(agentName, { cpu_percent: 30, memory_percent: 50 });

    const result = await pollUntil(async () => {
      try {
        return (await apiClient.getAgent(agentName)).data;
      } catch {
        return null;
      }
    }, { label: 'auto-created agent' });

    expect(result.id).toBe(agentName);
    expect(result.status).toBe('online');
  });
});
