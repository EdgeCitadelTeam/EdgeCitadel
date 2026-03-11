const { test, expect } = require('../helpers/fixtures');

test.describe('Health Checks', () => {
  test('API health returns status ok', async ({ apiClient }) => {
    const res = await apiClient.health();
    expect(res.status).toBe(200);
    expect(res.data).toHaveProperty('status', 'ok');
  });

  test('Frontend loads with correct title', async ({ page }) => {
    await page.goto('/');
    await expect(page).toHaveTitle(/OpenClaw/i);
    await expect(page.locator('text=OpenClaw Swarm Control')).toBeVisible();
  });

  test('NATS server accepts connections', async ({ mqttClient }) => {
    // mqttClient fixture auto-connects; if we reach here, the connection succeeded
    expect(mqttClient.nc).toBeTruthy();
    expect(mqttClient.nc.isClosed()).toBe(false);
  });
});
