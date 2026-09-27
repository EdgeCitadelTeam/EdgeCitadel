const { test, expect } = require('@playwright/test');

test('Main navigation and keyboard shortcuts expose four primary views', async ({ page }) => {
  await page.goto('/');
  const nav = page.getByRole('navigation', { name: 'Main navigation' });
  await expect(nav.getByRole('button')).toHaveCount(4);
  for (const [key, name] of [['1', 'Chat'], ['2', 'Flow'], ['3', 'Agent Flow'], ['4', 'Registry']]) {
    await page.keyboard.press(key);
    await expect(nav.getByRole('button', { name, exact: true })).toHaveAttribute('aria-current', 'page');
  }
  await nav.getByRole('button', { name: 'Chat', exact: true }).click();
  const input = page.locator('input').first();
  await input.focus();
  await page.keyboard.press('3');
  await expect(nav.getByRole('button', { name: 'Chat', exact: true })).toHaveAttribute('aria-current', 'page');
});
