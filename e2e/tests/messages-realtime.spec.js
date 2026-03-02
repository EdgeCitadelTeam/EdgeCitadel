const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Messages Real-time Updates', () => {
  let sender, receiver;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    sender = makeAgent({ display_name: `RT-Sender-${uniqueId()}` });
    receiver = makeAgent({ display_name: `RT-Recv-${uniqueId()}` });
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
  });

  test('New MQTT message appears without page refresh', async ({ mqttClient, page }) => {
    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();
    await sleep(2000); // Wait for WebSocket connection

    const msgText = `realtime-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: msgText });

    // Should appear via WebSocket without refresh
    await expect(page.locator(`text=${msgText}`)).toBeVisible({ timeout: 15_000 });
  });

  test('Auto-scroll follows new messages', async ({ mqttClient, page }) => {
    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();
    await sleep(2000);

    // Send multiple messages to trigger scrolling
    for (let i = 0; i < 5; i++) {
      await mqttClient.sendMessage(sender.name, receiver.name, 'command', {
        message: `scroll-msg-${i}-${uniqueId()}`,
      });
      await sleep(300);
    }

    const lastMsg = `scroll-last-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: lastMsg });

    // Latest message should be visible (auto-scrolled into view)
    await expect(page.locator(`text=${lastMsg}`)).toBeVisible({ timeout: 15_000 });
  });

  test('Agent filter applies to real-time messages', async ({ mqttClient, page }) => {
    await page.goto('/');
    await sleep(2000);

    // Select sender agent in sidebar
    await expect(page.locator(`text=${sender.display_name}`)).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${sender.display_name}`).click();
    await sleep(500);

    // Send a message from the selected agent
    const filteredMsg = `filtered-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: filteredMsg });

    // The message from this agent should appear
    await expect(page.locator(`text=${filteredMsg}`)).toBeVisible({ timeout: 15_000 });
  });

  test('Historical and realtime messages deduplicated', async ({ mqttClient, apiClient, page }) => {
    const msgText = `dedup-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: msgText });
    await sleep(1000);

    // Verify in API
    await pollUntil(async () => {
      const res = await apiClient.listMessages({ search: msgText });
      return res.data.items.length > 0 ? true : null;
    }, { label: 'message in API' });

    // Load page (will get historical + potentially realtime duplicate)
    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();
    await sleep(2000);

    // The message should appear exactly once
    const matches = page.locator(`text=${msgText}`);
    const count = await matches.count();
    expect(count).toBe(1);
  });
});
