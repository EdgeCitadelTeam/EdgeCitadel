const { test, expect } = require('@playwright/test');
const path = require('node:path');
const { mkdirSync } = require('node:fs');

test.skip(!process.env.EDGECITADEL_DETAILED_TRACE_ID, 'Requires a retained detailed-trace acceptance run on jim-eq');

test('retained messaging lanes, model tool linkage, content, history and keyboard access', async ({ page, request }) => {
  const trace = process.env.EDGECITADEL_DETAILED_TRACE_ID;
  expect(new URL(process.env.APP_URL).hostname).toBe('jim-eq');
  const response = await request.get(`${process.env.APP_URL}/api/traces/${trace}`);
  expect(response.ok()).toBeTruthy();
  const graph = await response.json();
  const tool = graph.nodes.find(node => node.kind === 'tool');
  expect(tool).toBeTruthy();
  const parent = graph.edges.find(edge => edge.to === tool.id && graph.nodes.some(node => node.id === edge.from && node.kind === 'model'));
  expect(parent).toBeTruthy();
  await page.goto(`/#execution?run=${trace}`);
  await expect(page.getByRole('button', { name: 'Pause live' })).toBeEnabled();
  await page.getByText('Coverage by family', { exact: true }).click();
  await expect(page.getByRole('table')).toContainText('Completeness unknown');
  await expect(page.getByRole('table')).toContainText('transport');
  await page.getByText('Evidence lanes', { exact: true }).click();
  const messaging = page.getByRole('checkbox', { name: /Messaging and brokers/ });
  await expect(messaging).not.toBeChecked();
  await messaging.check();
  const broker = graph.nodes.find(node => node.kind === 'broker');
  await expect(page.locator(`[data-node-id="${broker.id}"]`)).toBeVisible();
  await page.locator(`[data-node-id="${tool.id}"]`).click();
  await expect(page.getByLabel('Selected step details')).toBeVisible();
  await page.locator('.trace-observations button').last().click();
  await expect(page.getByLabel('Retained content')).toContainText('available');
  await page.getByRole('button', { name: 'Pause live' }).click();
  await page.locator('.trace-observations button').last().click();
  await expect(page.getByLabel('Retained content')).toContainText('available');
  const historical = page.url();
  expect(historical).toContain('at=');
  await page.reload();
  await expect(page.getByLabel('Retained content')).toContainText('available');
  expect(page.url()).toBe(historical);
  await page.getByRole('button', { name: 'Resume live' }).click();
  await expect(page.getByRole('button', { name: 'Pause live' })).toBeEnabled();
  await page.locator('[data-node-id]').first().focus();
  await page.keyboard.press('Home');
  await expect(page.locator('[data-map-index="0"]')).toBeFocused();
  const output = path.resolve(__dirname, '../../local-docs/research/detailed-trace-evidence');
  mkdirSync(output, { recursive: true });
  for (const width of [1440, 320]) {
    await page.setViewportSize({ width, height: 1000 });
    if (width < 768) await expect.poll(() => page.locator('.fixed.top-12').evaluate(element => element.getBoundingClientRect().right)).toBeLessThanOrEqual(0);
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth);
    expect(overflow).toBeFalsy();
    await page.screenshot({ animations: 'disabled', path: path.join(output, `trace-${width}.png`) });
  }
});
