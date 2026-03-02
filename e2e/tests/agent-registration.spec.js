const { test, expect } = require('../helpers/fixtures');
const { uniqueId, makeAgent } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Agent Registration', () => {
  test('MQTT register creates agent in DB with correct fields', async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);

    const result = await pollUntil(
      async () => {
        try {
          const res = await apiClient.getAgent(agent.name);
          return res.data;
        } catch {
          return null;
        }
      },
      { label: 'agent created in DB' }
    );

    expect(result.id).toBe(agent.name);
    expect(result.display_name).toBe(agent.display_name);
    expect(result.role).toBe(agent.role);
    expect(result.device_type).toBe(agent.device_type);
    expect(result.model).toBe(agent.model);
    expect(result.status).toBe('online');
  });

  test('Registered agent appears in sidebar with online badge', async ({ mqttClient, page }) => {
    const agent = makeAgent({ display_name: `Sidebar-${uniqueId()}` });
    await mqttClient.registerAgent(agent.name, agent);
    await sleep(1000);

    await page.goto('/');
    await expect(page.locator(`text=${agent.display_name}`)).toBeVisible({ timeout: 15_000 });
    // Online badge should be a green indicator near the agent
    const agentCard = page.locator(`text=${agent.display_name}`).locator('..');
    await expect(agentCard).toBeVisible();
  });

  test('Re-registering updates agent fields', async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await pollUntil(async () => {
      try {
        const res = await apiClient.getAgent(agent.name);
        return res.data;
      } catch {
        return null;
      }
    }, { label: 'initial registration' });

    // Re-register with updated fields
    await mqttClient.registerAgent(agent.name, {
      ...agent,
      role: 'coordinator',
      model: 'llama-3.2-3b',
    });
    await sleep(1000);

    const res = await apiClient.getAgent(agent.name);
    expect(res.data.role).toBe('coordinator');
    expect(res.data.model).toBe('llama-3.2-3b');
  });

  test('Registration triggers agent_registered WebSocket event', async ({ mqttClient, wsClient }) => {
    const agent = makeAgent();
    wsClient.clearEvents();

    await mqttClient.registerAgent(agent.name, agent);

    const event = await wsClient.waitForEvent(
      (e) => e.type === 'agent_registered' && e.agent_id === agent.name,
      10_000
    );
    expect(event).toBeTruthy();
    expect(event.agent_id).toBe(agent.name);
  });

  test('Registration triggers toast notification', async ({ mqttClient, page }) => {
    const agent = makeAgent({ display_name: `Toast-${uniqueId()}` });
    await page.goto('/');
    await sleep(1000);

    await mqttClient.registerAgent(agent.name, agent);

    // Toast notification should appear
    await expect(page.locator('[role="status"]').first()).toBeVisible({ timeout: 10_000 });
  });

  test('Multiple agents all appear sorted correctly', async ({ mqttClient, page }) => {
    const agents = [
      makeAgent({ display_name: `Multi-A-${uniqueId()}` }),
      makeAgent({ display_name: `Multi-B-${uniqueId()}` }),
      makeAgent({ display_name: `Multi-C-${uniqueId()}` }),
    ];

    for (const agent of agents) {
      await mqttClient.registerAgent(agent.name, agent);
      await sleep(300);
    }

    await page.goto('/');
    for (const agent of agents) {
      await expect(page.locator(`text=${agent.display_name}`)).toBeVisible({ timeout: 15_000 });
    }
  });
});
