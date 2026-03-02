// @ts-check
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: false,
  workers: 1,
  retries: 1,
  timeout: 60_000,
  expect: {
    timeout: 15_000,
  },
  reporter: [
    ['html', { open: 'never' }],
    ['list'],
  ],
  globalSetup: './global-setup.js',
  globalTeardown: './global-teardown.js',
  use: {
    baseURL: 'http://localhost:13000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    actionTimeout: 10_000,
    navigationTimeout: 30_000,
  },
  projects: [
    {
      name: 'chromium',
      use: {
        browserName: 'chromium',
        viewport: { width: 1440, height: 900 },
      },
    },
  ],
});
