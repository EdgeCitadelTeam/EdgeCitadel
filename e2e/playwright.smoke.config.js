// Minimal Playwright config for running phase smoke specs against the
// already-running dev stack. Skips globalSetup/globalTeardown (which
// would spin up a separate test stack on :13000) and points at the
// dev stack on :80. Used for ad-hoc walkthrough verification.
//
// testMatch covers both naming styles: legacy `phaseN-...-smoke.spec.js`
// (Phase 1 + 2) and `phase3+` specs without the -smoke suffix.
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  testMatch: /phase\d+-.*\.spec\.js/,
  workers: 1,
  retries: 0,
  timeout: 120_000,
  reporter: [['list']],
  use: {
    baseURL: process.env.AGG_URL || 'http://localhost',
  },
  projects: [
    { name: 'chromium', use: { browserName: 'chromium' } },
  ],
});
