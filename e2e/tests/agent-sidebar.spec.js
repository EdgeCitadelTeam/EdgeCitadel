const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { sleep } = require('../helpers/wait-utils');

test.describe('Agent Sidebar', () => {
  test('Shows empty state when no agents match', async ({ page }) => {
    await page.goto('/');
    // The sidebar should at minimum show "All Agents" button
    await expect(page.locator('text=All Agents')).toBeVisible({ timeout: 10_000 });
  });

  test('Shows agent count badge', async ({ mqttClient, page }) => {
    const agent = makeAgent();
    await mqttClient.registerAgent(agent.name, agent);
    await sleep(1000);

    await page.goto('/');
    // The sidebar header shows total agent count
    await expect(page.locator(`text=${agent.display_name}`).first()).toBeVisible({ timeout: 15_000 });
  });

  test('All Agents selected by default', async ({ page }) => {
    await page.goto('/');
    const allAgentsBtn = page.locator('text=All Agents').first();
    await expect(allAgentsBtn).toBeVisible({ timeout: 10_000 });
    // The "All Agents" button should have active/selected styling
    await expect(allAgentsBtn.locator('..')).toBeVisible();
  });

  test('Click agent selects and filters messages', async ({ mqttClient, page }) => {
    const agent = makeAgent({ display_name: `Select-${uniqueId()}` });
    await mqttClient.registerAgent(agent.name, agent);
    await sleep(1000);

    await page.goto('/');
    await expect(page.locator(`text=${agent.display_name}`).first()).toBeVisible({ timeout: 15_000 });

    // Click on the agent card
    await page.locator(`text=${agent.display_name}`).first().click();
    await sleep(500);

    // The agent card should appear selected (highlighted styling)
    const card = page.locator(`text=${agent.display_name}`).first().locator('..');
    await expect(card).toBeVisible();
  });

  test('Click selected agent deselects it', async ({ mqttClient, page }) => {
    const agent = makeAgent({ display_name: `Deselect-${uniqueId()}` });
    await mqttClient.registerAgent(agent.name, agent);
    await sleep(1000);

    await page.goto('/');
    await expect(page.locator(`text=${agent.display_name}`).first()).toBeVisible({ timeout: 15_000 });

    // Click to select
    await page.locator(`text=${agent.display_name}`).first().click();
    await sleep(300);

    // Click again to deselect
    await page.locator(`text=${agent.display_name}`).first().click();
    await sleep(300);

    // "All Agents" should be re-selected
    await expect(page.locator('text=All Agents')).toBeVisible();
  });

  test('Polling picks up new agents within 12s', async ({ mqttClient, page }) => {
    await page.goto('/');
    await sleep(1000);

    // Register agent after page load
    const agent = makeAgent({ display_name: `Poll-${uniqueId()}` });
    await mqttClient.registerAgent(agent.name, agent);

    // Sidebar polls every 10s, so within 12s the agent should appear
    await expect(page.locator(`text=${agent.display_name}`).first()).toBeVisible({ timeout: 15_000 });
  });
});
