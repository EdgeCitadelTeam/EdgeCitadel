// Minimal Playwright config for running phase smoke specs against the
// already-running dev stack. Skips globalSetup/globalTeardown (which
// would spin up a separate test stack on :13000) and points at the
// dev stack on :80. Used for ad-hoc walkthrough verification.
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  testMatch: /phase\d+.*-smoke\.spec\.js/,
  workers: 1,
  retries: 0,
  timeout: 90_000,
  reporter: [['list']],
  use: {
    baseURL: process.env.AGG_URL || 'http://localhost',
  },
  projects: [
    { name: 'chromium', use: { browserName: 'chromium' } },
  ],
});
