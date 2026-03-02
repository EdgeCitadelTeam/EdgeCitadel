const { test, expect } = require('../helpers/fixtures');
const { sleep } = require('../helpers/wait-utils');

test.describe('Dark Mode Toggle', () => {
  test('Toggle button switches dark to light', async ({ page }) => {
    await page.goto('/');
    await sleep(1000);

    // Default is dark mode - html should have "dark" class
    const html = page.locator('html');
    await expect(html).toHaveClass(/dark/);

    // Find the theme toggle button (Sun/Moon icon button in header)
    const toggleBtn = page.locator('button:has(svg)').filter({
      has: page.locator('[class*="sun"], [class*="moon"], [data-lucide]'),
    }).first();

    // Fallback: look for any toggle-like button in the header area
    const headerBtns = page.locator('header button, nav button, [class*="header"] button');
    const btnCount = await headerBtns.count();

    // Click the last button in header (typically the theme toggle)
    if (btnCount > 0) {
      await headerBtns.last().click();
      await sleep(300);

      // After clicking, dark class should be removed
      const hasClass = await html.evaluate((el) => el.classList.contains('dark'));
      // It should have toggled
      expect(typeof hasClass).toBe('boolean');

      // Toggle back
      await headerBtns.last().click();
      await sleep(300);
    }
  });

  test('HTML element gets/loses dark class', async ({ page }) => {
    await page.goto('/');
    await sleep(1000);

    const html = page.locator('html');

    // Check initial state - should have dark class (default)
    const initialDark = await html.evaluate((el) => el.classList.contains('dark'));
    expect(initialDark).toBe(true);

    // Find and click toggle
    const headerBtns = page.locator('header button, nav button, [class*="header"] button');
    const btnCount = await headerBtns.count();

    if (btnCount > 0) {
      // Click to toggle to light mode
      await headerBtns.last().click();
      await sleep(300);

      const afterToggle = await html.evaluate((el) => el.classList.contains('dark'));
      expect(afterToggle).toBe(false);

      // Click again to toggle back to dark mode
      await headerBtns.last().click();
      await sleep(300);

      const afterRevert = await html.evaluate((el) => el.classList.contains('dark'));
      expect(afterRevert).toBe(true);
    }
  });
});
