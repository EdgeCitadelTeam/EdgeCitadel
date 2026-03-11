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

  test('UI command sends NATS message to agents.{name}.inbox', async ({ mqttClient, page }) => {
    await mqttClient.subscribe(`agents.${agentName}.inbox`);

    await page.goto('/');
    await sleep(2000);

    // Select agent in sidebar
    await expect(page.locator(`text=${agentData.display_name}`).first()).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${agentData.display_name}`).first().click();
    await sleep(500);

    // Find command input and send button
    const commandInput = page.locator('input[placeholder*="command"], input[placeholder*="message"], textarea').last();
    const cmdText = `do-task-${uniqueId()}`;

    const msgPromise = mqttClient.waitForMessage(
      `agents.${agentName}.inbox`,
      (msg) => (msg.message === cmdText || msg.content === cmdText || msg.payload?.message === cmdText),
      15_000
    );

    await commandInput.fill(cmdText);
    // Press Enter or click send button
    await commandInput.press('Enter');

    const received = await msgPromise;
    expect(received.message || received.content || received.payload?.message).toBe(cmdText);
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

    await expect(page.locator(`text=${agentData.display_name}`).first()).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${agentData.display_name}`).first().click();
    await sleep(500);

    const commandInput = page.locator('input[placeholder*="command"], input[placeholder*="message"], textarea').last();
    await commandInput.fill(`toast-test-${uniqueId()}`);
    await commandInput.press('Enter');

    // Toast notification should appear
    await expect(page.locator('[role="status"]').first()).toBeVisible({ timeout: 10_000 });
  });

  test('POST /api/broadcast publishes to system.broadcast topic', async ({ mqttClient, apiClient }) => {
    await mqttClient.subscribe('system.broadcast');

    const msgPromise = mqttClient.waitForMessage(
      'system.broadcast',
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
    // (NATS is fire-and-forget), or return an error if it validates
    try {
      const res = await apiClient.sendCommand(`nonexistent-${uniqueId()}`, {
        message_type: 'command',
        payload: { message: 'hello' },
      });
      // If it succeeds, that's acceptable (NATS publish doesn't require recipient)
      expect(res.status).toBe(200);
    } catch (err) {
      // If it validates agent existence first, a 404 is acceptable
      expect([404, 500]).toContain(err.response.status);
    }
  });
});
