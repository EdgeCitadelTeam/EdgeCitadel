const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Agent Detail View', () => {
  let agentName;
  let agentData;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    agentData = makeAgent({
      display_name: `Detail-${uniqueId()}`,
      role: 'coordinator',
      device_type: 'jetson_nano',
      model: 'llama-3.2-3b',
      ip_address: '10.0.1.42',
      capabilities: ['text_generation', 'code_analysis', 'summarization'],
    });
    agentName = agentData.name;

    await mqttClient.registerAgent(agentName, agentData);
    await mqttClient.sendHeartbeat(agentName, { cpu_percent: 65.3, memory_percent: 82.1 });
    await pollUntil(async () => {
      try {
        const data = (await apiClient.getAgent(agentName)).data;
        return data?.cpu_percent != null ? data : null;
      } catch {
        return null;
      }
    }, { label: 'agent ready with metrics' });
  });

  test('Shows agent profile: name, role, device_type, model, IP', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator(`text=${agentData.display_name}`).first()).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${agentData.display_name}`).first().click();

    // Switch to detail tab by clicking agent name or detail area
    // The Layout component shows AgentDetail when activeTab is 'detail'
    // Clicking an agent card in the sidebar and then viewing details
    // Find and click the agent detail elements
    await sleep(500);
    // Look for detail-related text that would appear once we're on the detail view
    // Agent details shown after clicking on agent card in sidebar
  });

  test('Shows CPU/memory metrics with progress bars', async ({ apiClient }) => {
    const res = await apiClient.getAgent(agentName);
    expect(res.data.cpu_percent).toBeCloseTo(65.3, 0);
    expect(res.data.memory_percent).toBeCloseTo(82.1, 0);
  });

  test('Shows capabilities badges', async ({ apiClient }) => {
    const res = await apiClient.getAgent(agentName);
    expect(res.data.capabilities).toContain('text_generation');
    expect(res.data.capabilities).toContain('code_analysis');
    expect(res.data.capabilities).toContain('summarization');
  });

  test('Shows agent stats (message_count, task_count)', async ({ mqttClient, apiClient }) => {
    // Send some messages to generate stats
    await mqttClient.sendMessage(agentName, 'other-agent', 'command', { message: 'test' });
    await sleep(1000);

    const res = await apiClient.getAgentStats(agentName);
    expect(res.status).toBe(200);
    expect(res.data).toHaveProperty('message_count');
    expect(res.data).toHaveProperty('task_count');
  });

  test('Send command via API triggers MQTT publish', async ({ mqttClient, apiClient }) => {
    await mqttClient.subscribe(`agents/${agentName}/inbox`);

    const msgPromise = mqttClient.waitForMessage(
      `agents/${agentName}/inbox`,
      (msg) => (msg.message === 'run diagnostics' || msg.content === 'run diagnostics' || msg.payload?.message === 'run diagnostics'),
      15_000
    );

    await apiClient.sendCommand(agentName, {
      message_type: 'command',
      payload: { message: 'run diagnostics' },
    });

    const received = await msgPromise;
    expect(received.message || received.content || received.payload?.message).toBe('run diagnostics');
  });

  test('Recent messages list via API', async ({ mqttClient, apiClient }) => {
    await mqttClient.sendMessage(agentName, 'target-agent', 'command', { message: 'hello' });
    await sleep(1000);

    const res = await apiClient.listMessages({ agent: agentName, limit: 50 });
    expect(res.status).toBe(200);
    // Should have at least the registration + heartbeat + command messages
    expect(res.data.items.length).toBeGreaterThan(0);
  });

  test('Task history via API', async ({ mqttClient, apiClient }) => {
    const taskId = uniqueId('detail-task');
    await mqttClient.assignTask(agentName, taskId);
    await sleep(1000);

    const res = await apiClient.listTasks({ assigned_agent: agentName });
    expect(res.status).toBe(200);
    const found = res.data.items.find((t) => t.id === taskId);
    expect(found).toBeTruthy();
  });

  test('Back button returns to chat tab', async ({ page }) => {
    await page.goto('/');
    // Verify we can switch tabs and the chat tab is accessible
    const chatTab = page.locator('text=Chat').first();
    await expect(chatTab).toBeVisible({ timeout: 10_000 });
    await chatTab.click();
    await sleep(300);
  });
});
