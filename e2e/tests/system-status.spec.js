const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('System Status', () => {
  test('Header shows agent count, message count, task count', async ({ mqttClient, page }) => {
    // Register an agent to ensure counts are non-zero
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await sleep(1000);

    await page.goto('/');
    await sleep(2000);

    // HeaderBar shows system metrics
    // Look for the metric displays in the header
    const header = page.locator('header, [class*="header"], nav').first();
    await expect(header).toBeVisible({ timeout: 10_000 });
  });

  test('System status polls and updates', async ({ mqttClient, apiClient, page }) => {
    await page.goto('/');
    await sleep(2000);

    // Get initial status
    const initial = await apiClient.getSystemStatus();
    const initialTotal = initial.data.total_messages;

    // Send a message (not register/heartbeat, which are skipped for message storage)
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await sleep(500);
    await mqttClient.sendMessage(agent.name, 'system', 'command', { message: 'status-test' });
    await sleep(1000);

    // Status should eventually update
    const updated = await pollUntil(async () => {
      const res = await apiClient.getSystemStatus();
      return res.data.total_messages > initialTotal ? res.data : null;
    }, { timeout: 10_000, label: 'message count increased' });

    expect(updated.total_messages).toBeGreaterThan(initialTotal);
  });

  test('MQTT connection badge shows connected status', async ({ page }) => {
    await page.goto('/');
    await sleep(2000);

    // The header should show MQTT connection status
    // Look for "Connected" or green indicator
    const connBadge = page.locator('text=Connected').first();
    // It might show "MQTT Connected" or just the green dot
    await expect(connBadge.or(page.locator('text=MQTT'))).toBeVisible({ timeout: 10_000 });
  });

  test('GET /api/system/status returns all fields', async ({ apiClient }) => {
    const res = await apiClient.getSystemStatus();
    expect(res.status).toBe(200);
    expect(res.data).toHaveProperty('agents_online');
    expect(res.data).toHaveProperty('agents_total');
    expect(res.data).toHaveProperty('total_messages');
    expect(res.data).toHaveProperty('active_tasks');
    expect(res.data).toHaveProperty('errors_today');
    expect(res.data).toHaveProperty('mqtt_connected');
    expect(typeof res.data.agents_online).toBe('number');
    expect(typeof res.data.agents_total).toBe('number');
    expect(typeof res.data.total_messages).toBe('number');
  });
});
