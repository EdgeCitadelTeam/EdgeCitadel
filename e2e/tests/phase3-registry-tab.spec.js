const { test, expect } = require('@playwright/test');

const APP = process.env.APP_URL;

test.describe('Phase 3 — Registry tab', () => {
  test('Registry tab renders fleet table', async ({ page }) => {
    await page.goto(APP);

    await page.keyboard.press('5');
    await expect(page.getByText('Registry')).toBeVisible();

    await expect(page.locator('table')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('td', { hasText: 'shell-1' })).toBeVisible({ timeout: 10000 });
  });

  test('Test data toggle reveals deployment=test rows', async ({ page }) => {
    await page.goto(APP);
    await page.keyboard.press('5');

    await expect(page.locator('th', { hasText: 'Deployment' })).not.toBeVisible();
    await page.getByRole('button', { name: 'Show test data' }).click();
    await expect(page.locator('th', { hasText: 'Deployment' })).toBeVisible();
  });

  test('Click row drills into AgentDetail; back returns to Registry', async ({ page }) => {
    await page.goto(APP);
    await page.keyboard.press('5');
    await page.locator('td', { hasText: 'shell-1' }).first().click();
    await expect(page.getByRole('button', { name: 'Back to all' })).toBeVisible({ timeout: 5000 });
    await page.getByRole('button', { name: 'Back to all' }).click();
    await page.keyboard.press('5');
    await expect(page.locator('table')).toBeVisible();
  });
});
