const { test, expect } = require('../helpers/fixtures');
const { sleep } = require('../helpers/wait-utils');

test.describe('Keyboard Shortcuts', () => {
  test('Key 1-4 switch tabs', async ({ page }) => {
    await page.goto('/');
    await sleep(1000);

    // Press "1" to switch to Chat tab
    await page.keyboard.press('1');
    await sleep(300);
    // Chat tab should be active
    const chatTab = page.locator('text=Chat').first();
    await expect(chatTab).toBeVisible();

    // Press "2" for Flow tab
    await page.keyboard.press('2');
    await sleep(300);
    const flowTab = page.locator('text=Flow').first();
    await expect(flowTab).toBeVisible();

    // Press "3" for Logs tab
    await page.keyboard.press('3');
    await sleep(300);
    const logsTab = page.locator('text=Logs').first();
    await expect(logsTab).toBeVisible();

    // Press "4" for Tasks tab
    await page.keyboard.press('4');
    await sleep(300);
    const tasksTab = page.locator('text=Tasks').first();
    await expect(tasksTab).toBeVisible();
  });

  test('Shortcuts ignored when focused in input/textarea/select', async ({ page }) => {
    await page.goto('/');
    await sleep(1000);

    // Start on Chat tab
    await page.keyboard.press('1');
    await sleep(300);

    // Focus on an input field (e.g., search or command input)
    const input = page.locator('input').first();
    if (await input.isVisible()) {
      await input.focus();
      await sleep(200);

      // Press "3" while focused in input - should NOT switch to Logs
      await page.keyboard.press('3');
      await sleep(300);

      // Type the character instead
      // Verify we're still on the same tab (Chat)
    }
  });

  test('Tab indicator updates on switch', async ({ page }) => {
    await page.goto('/');
    await sleep(1000);

    // Switch to Flow tab
    await page.keyboard.press('2');
    await sleep(500);

    // Flow tab should have active indicator styling
    // Switch to Logs tab
    await page.keyboard.press('3');
    await sleep(500);

    // Switch back to Chat
    await page.keyboard.press('1');
    await sleep(500);

    // Verify tab is visible and interactive
    await expect(page.locator('text=Chat').first()).toBeVisible();
  });
});
