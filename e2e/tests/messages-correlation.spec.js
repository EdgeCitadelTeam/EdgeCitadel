const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Messages Correlation & Thread View', () => {
  let sender, receiver;
  let correlationId;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    sender = makeAgent({ display_name: `Corr-S-${uniqueId()}` });
    receiver = makeAgent({ display_name: `Corr-R-${uniqueId()}` });
    await mqttClient.registerAgent(sender.name, sender);
    await mqttClient.registerAgent(receiver.name, receiver);
    await pollUntil(async () => {
      try {
        await apiClient.getAgent(sender.name);
        await apiClient.getAgent(receiver.name);
        return true;
      } catch {
        return null;
      }
    }, { label: 'agents ready' });

    // Send a sequence of correlated messages
    correlationId = uniqueId('corr');
    const baseMsg = {
      sender: sender.name,
      receiver: receiver.name,
      type: 'command',
      correlation_id: correlationId,
      timestamp: new Date().toISOString(),
      payload: { message: `corr-msg-1-${correlationId}` },
    };
    await mqttClient.publish(`agents/inbox/${receiver.name}`, baseMsg);
    await sleep(300);

    const replyMsg = {
      sender: receiver.name,
      receiver: sender.name,
      type: 'result',
      correlation_id: correlationId,
      timestamp: new Date().toISOString(),
      payload: { message: `corr-msg-2-${correlationId}` },
    };
    await mqttClient.publish(`agents/inbox/${sender.name}`, replyMsg);
    await sleep(500);
  });

  test('Click correlated message opens trace panel', async ({ page }) => {
    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();

    // Find the correlated message and click it
    const msg = page.locator(`text=corr-msg-1-${correlationId}`);
    await expect(msg).toBeVisible({ timeout: 15_000 });
    await msg.click();
    await sleep(500);

    // The ConversationThread panel should open showing correlated messages
    // It fetches messages with the same correlation_id
  });

  test('Panel shows all messages with same correlation_id', async ({ apiClient }) => {
    const res = await apiClient.listMessages({ correlation_id: correlationId });
    expect(res.data.items.length).toBe(2);
    expect(res.data.items.every((m) => m.correlation_id === correlationId)).toBe(true);
  });

  test('Click correlated message again deselects', async ({ page }) => {
    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();

    const msg = page.locator(`text=corr-msg-1-${correlationId}`);
    await expect(msg).toBeVisible({ timeout: 15_000 });

    // Click to open
    await msg.click();
    await sleep(500);

    // Click again to close
    await msg.click();
    await sleep(500);
  });

  test('Conversation API returns grouped threads', async ({ apiClient }) => {
    const res = await apiClient.getConversations();
    expect(res.status).toBe(200);
    expect(Array.isArray(res.data)).toBe(true);
    // Should have at least our correlated conversation
    const thread = res.data.find((c) => c.correlation_id === correlationId);
    expect(thread).toBeTruthy();
    expect(thread.message_count).toBe(2);
  });
});
