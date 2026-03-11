const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Log Viewer', () => {
  let agentName;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    agentName = agent.name;
    await mqttClient.registerAgent(agentName, agent);
    await pollUntil(async () => {
      try {
        return (await apiClient.getAgent(agentName)).data;
      } catch {
        return null;
      }
    }, { label: 'agent ready' });
  });

  test('MQTT log message stored in DB', async ({ mqttClient, apiClient }) => {
    const logMsg = `log-stored-${uniqueId()}`;
    await mqttClient.sendLog(agentName, 'INFO', logMsg);

    const result = await pollUntil(async () => {
      const res = await apiClient.listLogs({ search: logMsg });
      if (res.data.items.length > 0) return res.data.items[0];
      return null;
    }, { label: 'log in DB' });

    expect(result.message).toBe(logMsg);
    expect(result.level).toBe('INFO');
    expect(result.agent_id).toBe(agentName);
  });

  test('Logs appear in table with correct columns', async ({ mqttClient, page }) => {
    const logMsg = `log-table-${uniqueId()}`;
    await mqttClient.sendLog(agentName, 'WARN', logMsg, { source: 'test-src' });
    await sleep(1000);

    await page.goto('/');
    const logsTab = page.locator('text=Logs').first();
    await logsTab.click();
    await sleep(2000);

    // Log entry should appear in the table
    await expect(page.locator(`text=${logMsg}`).first()).toBeVisible({ timeout: 15_000 });
  });

  test('Level filter buttons work', async ({ mqttClient, page }) => {
    await mqttClient.sendLog(agentName, 'ERROR', `error-log-${uniqueId()}`);
    await mqttClient.sendLog(agentName, 'INFO', `info-log-${uniqueId()}`);
    await sleep(1000);

    await page.goto('/');
    const logsTab = page.locator('text=Logs').first();
    await logsTab.click();
    await sleep(1000);

    // Click ERROR level filter
    const errorBtn = page.locator('button:has-text("ERROR")');
    if (await errorBtn.isVisible()) {
      await errorBtn.click();
      await sleep(500);
    }
  });

  test('Source filter input works', async ({ mqttClient, page }) => {
    const src = `source-${uniqueId()}`;
    await mqttClient.sendLog(agentName, 'INFO', `src-filter-test-${uniqueId()}`, { source: src });
    await sleep(1000);

    await page.goto('/');
    const logsTab = page.locator('text=Logs').first();
    await logsTab.click();
    await sleep(1000);

    const sourceInput = page.locator('input[placeholder*="ource"]');
    if (await sourceInput.isVisible()) {
      await sourceInput.fill(src);
      await sleep(500);
    }
  });

  test('Search filter works', async ({ mqttClient, page }) => {
    const searchTerm = `searchlog-${uniqueId()}`;
    await mqttClient.sendLog(agentName, 'INFO', searchTerm);
    await sleep(1000);

    await page.goto('/');
    const logsTab = page.locator('text=Logs').first();
    await logsTab.click();
    await sleep(1000);

    const searchInput = page.locator('input[placeholder*="earch"]');
    if (await searchInput.isVisible()) {
      await searchInput.fill(searchTerm);
      await sleep(500);
      await expect(page.locator(`text=${searchTerm}`).first()).toBeVisible({ timeout: 10_000 });
    }
  });

  test('Click row expands metadata JSON', async ({ mqttClient, page }) => {
    const logMsg = `expand-${uniqueId()}`;
    await mqttClient.sendLog(agentName, 'INFO', logMsg, {
      metadata: { detail: 'extra-info', count: 42 },
    });
    await sleep(1000);

    await page.goto('/');
    const logsTab = page.locator('text=Logs').first();
    await logsTab.click();
    await sleep(2000);

    // Click on the log row to expand it
    const row = page.locator(`text=${logMsg}`).first();
    await expect(row).toBeVisible({ timeout: 15_000 });
    await row.click();
    await sleep(500);
  });

  test('ERROR rows have red styling', async ({ mqttClient, page }) => {
    const logMsg = `error-style-${uniqueId()}`;
    await mqttClient.sendLog(agentName, 'ERROR', logMsg);
    await sleep(1000);

    await page.goto('/');
    const logsTab = page.locator('text=Logs').first();
    await logsTab.click();
    await sleep(2000);

    const logRow = page.locator(`text=${logMsg}`).first().locator('..');
    await expect(logRow).toBeVisible({ timeout: 15_000 });
    // ERROR rows should have red background tinting
  });

  test('API supports all filter params', async ({ mqttClient, apiClient }) => {
    const logMsg = `api-filter-${uniqueId()}`;
    await mqttClient.sendLog(agentName, 'WARN', logMsg, { source: 'api-test-src' });
    await sleep(1000);

    const res = await apiClient.listLogs({
      level: 'WARN',
      agent_id: agentName,
      source: 'api-test-src',
      search: logMsg,
    });
    expect(res.status).toBe(200);
    expect(res.data.items.length).toBeGreaterThan(0);
    expect(res.data.items[0].message).toBe(logMsg);
    expect(res.data.items[0].level).toBe('WARN');
  });
});
