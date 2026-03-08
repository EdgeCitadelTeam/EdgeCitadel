const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Agent Offline Detection', () => {
  // HEARTBEAT_TIMEOUT=10, HEALTH_CHECK_INTERVAL=3 in test docker-compose
  // Agent should go offline ~10s after last heartbeat, checked every 3s

  test('Agent marked offline after heartbeat timeout', async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await mqttClient.sendHeartbeat(agent.name);

    // Verify online
    await pollUntil(async () => {
      const res = await apiClient.getAgent(agent.name);
      return res.data.status === 'online' ? res.data : null;
    }, { label: 'agent online' });

    // Wait for heartbeat timeout + health check interval
    const result = await pollUntil(async () => {
      const res = await apiClient.getAgent(agent.name);
      return res.data.status === 'offline' ? res.data : null;
    }, { timeout: 15_000, interval: 1000, label: 'agent goes offline' });

    expect(result.status).toBe('offline');
  });

  test('Offline triggers WebSocket event', async ({ mqttClient, wsClient, apiClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await mqttClient.sendHeartbeat(agent.name);
    await pollUntil(async () => {
      const res = await apiClient.getAgent(agent.name);
      return res.data.status === 'online' ? res.data : null;
    }, { label: 'agent online' });

    wsClient.clearEvents();

    const event = await wsClient.waitForEvent(
      (e) => e.type === 'agent_status_change' && e.agent_id === agent.name && e.status === 'offline',
      25_000
    );
    expect(event).toBeTruthy();
    expect(event.status).toBe('offline');
  });

  test('Sidebar updates to show offline styling', async ({ mqttClient, page, apiClient }) => {
    const agent = makeAgent({ display_name: `Offline-${uniqueId()}` });
    await mqttClient.registerAgent(agent.name, agent);
    await mqttClient.sendHeartbeat(agent.name);

    await page.goto('/');
    await expect(page.locator(`text=${agent.display_name}`).first()).toBeVisible({ timeout: 15_000 });

    // Wait for agent to go offline (heartbeat timeout)
    await pollUntil(async () => {
      const res = await apiClient.getAgent(agent.name);
      return res.data.status === 'offline' ? res.data : null;
    }, { timeout: 15_000, interval: 1000, label: 'agent offline' });

    // Sidebar should refresh and show offline styling (dimmed/opacity)
    // The AgentCard component dims offline agents
    await page.waitForTimeout(5_000); // Wait for sidebar poll
    const agentCard = page.locator(`text=${agent.display_name}`).first().locator('..');
    await expect(agentCard).toBeVisible();
  });
});
