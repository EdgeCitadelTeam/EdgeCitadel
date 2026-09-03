// @ts-check
const { defineConfig } = require('@playwright/test');

if (!process.env.APP_URL || !process.env.AGG_URL) {
  throw new Error('APP_URL and AGG_URL are required');
}

module.exports = defineConfig({
  testDir: './tests',
  // These suites exercise Managed Agents and upstream services intentionally absent
  // from the hermetic Core + shell fixture. Run them with
  // `npm run test:external-plugins` against an explicitly prepared stack.
  testIgnore: [
    '**/phase2-gemma-smoke.spec.js',
    '**/phase2.5-streaming-and-memory.spec.js',
    '**/phase6-hermes-bridge.spec.js',
    '**/streaming-fragmentation-regression.spec.js',
  ],
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 60_000,
  use: {
    baseURL: process.env.APP_URL,
    trace: 'off',
    video: 'off',
    screenshot: 'off',
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
