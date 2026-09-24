const { test, expect } = require('@playwright/test');
const path = require('node:path');
const fs = require('node:fs');

test.skip(process.env.EDGECITADEL_COMMUNICATION_E2E !== '1', 'Requires authorized jim-eq deployment and retained trace');
const trace = process.env.EDGECITADEL_COMMUNICATION_TRACE || 'aa824f1af85d444fa7f3bdebd5b8874b';
const artifacts = path.resolve(__dirname, `../../local-docs/architecture-reviews/${process.env.EDGECITADEL_UI_ARTIFACTS || 'communication-option1'}/${trace}`);
test('selected communication, deep links, finite motion and responsive panels', async ({ page }) => {
  expect(new URL(process.env.APP_URL).hostname).toBe('jim-eq');
  fs.mkdirSync(artifacts, { recursive: true });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(`/#execution?run=${trace}`);
  const browser = page.getByLabel('Run browser');
  await expect(browser).toBeVisible();
  await expect(page.getByRole('button', { name: 'Open agent list' })).toHaveCount(0);
  const rail = await browser.boundingBox();
  const run = await page.getByLabel('Selected run', { exact: true }).boundingBox();
  expect(rail.x + rail.width).toBeLessThanOrEqual(run.x);
  expect(Math.abs(rail.y - run.y)).toBeLessThan(2);
  const retained = [];
  let cursor = null;
  do {
    const response = await page.request.get('/api/traces', { params: { limit: 20, ...(cursor ? { cursor } : {}) } });
    expect(response.ok()).toBe(true);
    const data = await response.json();
    retained.push(...data.items.map(run => run.trace_id));
    cursor = data.next_cursor;
  } while (cursor);
  while (await browser.getByRole('button', { name: 'Load more runs' }).count()) {
    await browser.getByRole('button', { name: 'Load more runs' }).click();
    await expect(browser.getByRole('status')).toHaveCount(0);
  }
  await expect(browser.locator('.trace-run-list button')).toHaveCount(retained.length);
  for (const id of retained) await expect(browser.getByText(id.slice(0, 12), { exact: true })).toBeVisible();
  await expect(page.getByRole('textbox')).toHaveCount(0);
  const map = page.getByLabel('Agent communication map');
  await expect(map.locator('[data-agent-id]')).toHaveCount(2);
  await expect(map.locator('[data-arrow-id]')).toHaveCount(1);
  await expect(map.locator('[data-message-id]')).toHaveCount(4);
  await expect(map.locator('.topology-host')).toHaveCount(trace === '13ebbd37f28e481ebfd4b23706e7fad8' ? 1 : 2);
  await expect(map.locator('.topology-core .flow-network-address')).toContainText(':4222');
  for (const card of await map.locator('[data-agent-id]').all()) {
    await expect(card.locator('.flow-network-address')).toContainText('via NATS 127.0.0.1:4223');
  }
  const first = map.locator('[data-message-id]').first();
  await first.click();
  await expect(map.locator('[data-arrow-id]')).toHaveCount(1);
  await expect(map.locator('.selected-message-label')).toHaveCSS('fill', 'rgb(237, 240, 248)');
  await expect(page.locator('[data-detail-message]')).toHaveCount(1);
  expect((await page.getByRole('dialog').boundingBox()).width).toBe(360);
  await page.reload();
  await expect(map.locator('[data-arrow-id]')).toHaveCount(1);
  await expect(page.getByRole('tab', { name: 'Overview', exact: true })).toHaveAttribute('aria-selected', 'true');
  await page.getByRole('button', { name: 'Close details', exact: true }).click();
  await first.focus();
  await page.keyboard.press('ArrowDown');
  await expect(map.locator('[data-arrow-id]')).toHaveAttribute('stroke-dasharray', '6 5');
  await page.getByRole('button', { name: 'Close details', exact: true }).click();
  await expect(map.locator('[data-message-id]').nth(1)).toBeFocused();
  await first.click();
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('ArrowDown');
  await expect(map.locator('[data-message-id]').nth(2)).toHaveAttribute('aria-pressed', 'true');
  await page.keyboard.press('End');
  await expect(map.locator('[data-message-id]').last()).toHaveAttribute('aria-pressed', 'true');
  await page.getByRole('button', { name: 'Close details', exact: true }).click();
  for (const width of [1440, 1024, 736, 320]) {
    await page.setViewportSize({ width, height: 900 });
    for (const theme of ['dark', 'light']) {
      const current = await page.locator('.trace-explorer').getAttribute('data-theme');
      if (current !== theme) await page.getByRole('button', { name: theme === 'dark' ? 'Dark map theme' : 'Light map theme' }).click();
      await expect(map.locator('[data-arrow-id]')).toHaveCount(1);
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
      await page.locator('.trace-heading').scrollIntoViewIfNeeded();
      await page.screenshot({ path: path.join(artifacts, `${width}-${theme}.png`), fullPage: true });
      await first.click();
      await expect(page.getByRole('dialog')).toBeVisible();
      expect((await page.getByRole('dialog').boundingBox()).width).toBe(width <= 700 ? width : width <= 1150 ? 440 : 360);
      await page.getByRole('tab', { name: 'Evidence', exact: true }).click();
      await page.locator('.communication-evidence summary').first().click();
      await page.locator('.communication-evidence').first().getByRole('button', { name: 'Link to observation' }).click();
      await page.reload();
      await expect(page.locator('.communication-evidence[open]')).toHaveCount(1);
      await page.getByRole('button', { name: 'Close details', exact: true }).click();
    }
  }
  await page.setViewportSize({ width: 1440, height: 900 });
  await first.click();
  await page.getByRole('button', { name: 'Expand panel' }).click();
  expect((await page.getByRole('dialog').boundingBox()).width).toBeLessThanOrEqual(620);
  await page.getByRole('button', { name: 'Replay motion' }).click();
  await expect(page.locator('.communication-motion')).toHaveCSS('opacity', '0', { timeout: 5000 });
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await expect(page.locator('.communication-motion')).toHaveCSS('display', 'none');
  await page.screenshot({ path: path.join(artifacts, 'selected-expanded.png'), fullPage: true });
  expect(errors).toEqual([]);
  fs.writeFileSync(path.join(artifacts, 'ui-acceptance.json'), JSON.stringify({ trace, widths: [1440, 1024, 736, 320], themes: ['dark', 'light'], testedAt: new Date().toISOString(), errors, note: 'UI verification of an existing retained run; does not establish Case A/B acceptance.' }, null, 2));
});
