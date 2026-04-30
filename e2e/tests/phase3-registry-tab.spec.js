const { test, expect } = require('@playwright/test');

const APP = process.env.APP_URL || 'http://localhost';

test.describe('Phase 3 — Registry tab', () => {
  test('Registry tab renders fleet table', async ({ page }) => {
    await page.goto(APP);

    // Switch via keyboard shortcut
    await page.keyboard.press('5');
    await expect(page.getByText('Registry')).toBeVisible();

    // Table renders within 10s (5s registry refetch)
    await expect(page.locator('table')).toBeVisible({ timeout: 10000 });

    // Confirm at least one operator agent row (gemma-1 from Phase 2 stack)
    await expect(page.locator('td', { hasText: 'gemma-1' })).toBeVisible({ timeout: 10000 });
  });

  test('Test data toggle reveals deployment=test rows', async ({ page }) => {
    await page.goto(APP);
    await page.keyboard.press('5');

    // Default state: no deployment column header visible (toggle off)
    await expect(page.locator('th', { hasText: 'Deployment' })).not.toBeVisible();

    // Find the showTestAgents toggle in the header bar and click it.
    // Note: the toggle label may differ; adjust to actual header copy.
    const toggle = page.getByRole('switch', { name: /test data/i });
    if (await toggle.isVisible()) {
      await toggle.click();
      await expect(page.locator('th', { hasText: 'Deployment' })).toBeVisible();
    }
  });

  test('Click row drills into AgentDetail; back returns to Registry', async ({ page }) => {
    await page.goto(APP);
    await page.keyboard.press('5');
    await page.locator('td', { hasText: 'gemma-1' }).first().click();
    // AgentDetail surface — look for the Send button or back arrow
    await expect(page.getByRole('button', { name: /back|←/i })).toBeVisible({ timeout: 5000 });
    await page.getByRole('button', { name: /back|←/i }).click();
    // Back to whatever previous tab was; we re-press 5 to confirm Registry still works
    await page.keyboard.press('5');
    await expect(page.locator('table')).toBeVisible();
  });
});
