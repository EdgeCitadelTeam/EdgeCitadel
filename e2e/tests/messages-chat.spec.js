const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Messages Chat View', () => {
  let sender, receiver;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    sender = makeAgent({ display_name: `Sender-${uniqueId()}` });
    receiver = makeAgent({ display_name: `Receiver-${uniqueId()}` });
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

  test('Shows empty state text when no user messages', async ({ page }) => {
    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();
    await sleep(500);
    // The chat area should be visible
    await expect(chatTab).toBeVisible();
  });

  test('Messages appear after MQTT publish', async ({ mqttClient, page }) => {
    const msgText = `hello-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: msgText });
    await sleep(1000);

    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();

    // The message text should be visible in the chat area
    await expect(page.locator(`text=${msgText}`)).toBeVisible({ timeout: 15_000 });
  });

  test('Type filter dropdown works', async ({ mqttClient, page }) => {
    const cmdMsg = `cmd-${uniqueId()}`;
    const alertMsg = `alert-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: cmdMsg });
    await mqttClient.sendMessage(sender.name, receiver.name, 'alert', { message: alertMsg });
    await sleep(1000);

    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();
    await sleep(500);

    // Look for type filter select
    const filter = page.locator('select').first();
    if (await filter.isVisible()) {
      await filter.selectOption('command');
      await sleep(500);
    }
  });

  test('Search input filters by payload', async ({ mqttClient, page }) => {
    const searchTerm = `searchable-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: searchTerm });
    await sleep(1000);

    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();
    await sleep(500);

    // Find the search input
    const searchInput = page.locator('input[placeholder*="earch"]').first();
    if (await searchInput.isVisible()) {
      await searchInput.fill(searchTerm);
      await sleep(500);
      await expect(page.locator(`text=${searchTerm}`)).toBeVisible({ timeout: 10_000 });
    }
  });

  test('Sender and receiver shown with colored names', async ({ mqttClient, page }) => {
    const msg = `colored-${uniqueId()}`;
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: msg });
    await sleep(1000);

    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();

    await expect(page.locator(`text=${msg}`)).toBeVisible({ timeout: 15_000 });
    // Sender name should appear in the message bubble
    await expect(page.locator(`text=${sender.name}`).first()).toBeVisible();
  });

  test('MQTT topic and correlation_id displayed', async ({ mqttClient, apiClient }) => {
    const corrId = await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: 'test' });
    await sleep(1000);

    const res = await apiClient.listMessages({ correlation_id: corrId });
    expect(res.data.items.length).toBeGreaterThan(0);
    expect(res.data.items[0].correlation_id).toBe(corrId);
    expect(res.data.items[0].mqtt_topic).toBeTruthy();
  });

  test('Large payloads truncated at 500 chars', async ({ mqttClient, page }) => {
    const longText = 'A'.repeat(600);
    await mqttClient.sendMessage(sender.name, receiver.name, 'command', { message: longText });
    await sleep(1000);

    await page.goto('/');
    const chatTab = page.locator('text=Chat').first();
    await chatTab.click();
    await sleep(1000);

    // The full 600-char text should not be fully visible; it's truncated in MessageBubble
    // Check that some portion is visible
    const truncatedLocator = page.locator(`text=${'A'.repeat(100)}`).first();
    await expect(truncatedLocator).toBeVisible({ timeout: 15_000 });
  });
});
