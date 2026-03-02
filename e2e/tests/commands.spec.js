const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Commands', () => {
  let agentName;
  let agentData;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    agentData = makeAgent({ display_name: `Cmd-${uniqueId()}` });
    agentName = agentData.name;
    await mqttClient.registerAgent(agentName, agentData);
    await pollUntil(async () => {
      try {
        return (await apiClient.getAgent(agentName)).data;
      } catch {
        return null;
      }
    }, { label: 'agent ready' });
  });

  test('UI command sends MQTT message to agents/inbox/{name}', async ({ mqttClient, page }) => {
    await mqttClient.subscribe(`agents/inbox/${agentName}`);

    await page.goto('/');
    await sleep(2000);

    // Select agent in sidebar
    await expect(page.locator(`text=${agentData.display_name}`)).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${agentData.display_name}`).click();
    await sleep(500);

    // Find command input and send button
    const commandInput = page.locator('input[placeholder*="command"], input[placeholder*="message"], textarea').last();
    const cmdText = `do-task-${uniqueId()}`;

    const msgPromise = mqttClient.waitForMessage(
      `agents/inbox/${agentName}`,
      (msg) => msg.payload?.message === cmdText,
      10_000
    );

    await commandInput.fill(cmdText);
    // Press Enter or click send button
    await commandInput.press('Enter');

    const received = await msgPromise;
    expect(received.payload.message).toBe(cmdText);
  });

  test('Send button disabled when no target or text', async ({ page }) => {
    await page.goto('/');
    await sleep(1000);

    // Look for the send button - it should be disabled when no agent is selected or no text
    const sendBtn = page.locator('button:has-text("Send"), button[type="submit"]').last();
    if (await sendBtn.isVisible()) {
      await expect(sendBtn).toBeDisabled();
    }
  });

  test('Toast shows sent confirmation on success', async ({ page, mqttClient }) => {
    await page.goto('/');
    await sleep(2000);

    await expect(page.locator(`text=${agentData.display_name}`)).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${agentData.display_name}`).click();
    await sleep(500);

    const commandInput = page.locator('input[placeholder*="command"], input[placeholder*="message"], textarea').last();
    await commandInput.fill(`toast-test-${uniqueId()}`);
    await commandInput.press('Enter');

    // Toast notification should appear
    await expect(page.locator('[role="status"]').first()).toBeVisible({ timeout: 10_000 });
  });

  test('POST /api/broadcast publishes to agents/broadcast topic', async ({ mqttClient, apiClient }) => {
    await mqttClient.subscribe('agents/broadcast');

    const msgPromise = mqttClient.waitForMessage(
      'agents/broadcast',
      (msg) => msg.payload?.message === 'attention all agents',
      10_000
    );

    await apiClient.broadcast({
      message_type: 'command',
      payload: { message: 'attention all agents' },
    });

    const received = await msgPromise;
    expect(received.payload.message).toBe('attention all agents');
  });

  test('Error handling for command to nonexistent agent', async ({ apiClient }) => {
    // The command endpoint should still publish even if agent doesn't exist in DB
    // (MQTT is fire-and-forget), or return an error if it validates
    try {
      const res = await apiClient.sendCommand(`nonexistent-${uniqueId()}`, {
        message_type: 'command',
        payload: { message: 'hello' },
      });
      // If it succeeds, that's acceptable (MQTT publish doesn't require recipient)
      expect(res.status).toBe(200);
    } catch (err) {
      // If it validates agent existence first, a 404 is acceptable
      expect([404, 500]).toContain(err.response.status);
    }
  });
});
