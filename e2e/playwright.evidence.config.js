const path = require('node:path')
const { defineConfig } = require('@playwright/test')
const base = require('./playwright.config')

const evidenceDir = process.env.EVIDENCE_DIR
if (!evidenceDir) throw new Error('EVIDENCE_DIR is required')

module.exports = defineConfig(Object.assign({}, base, {
  testMatch: ['operator-journey.spec.js'],
  workers: 1,
  retries: 0,
  reporter: [
    ['list'],
    ['json', { outputFile: path.join(evidenceDir, 'playwright-results.json') }],
  ],
  use: Object.assign({}, base.use, { trace: 'on', video: 'on', screenshot: 'off' }),
  projects: [
    { name: 'desktop', use: { browserName: 'chromium', viewport: { width: 1440, height: 900 } } },
    { name: 'mobile', use: { browserName: 'chromium', viewport: { width: 390, height: 844 } } },
  ],
}))
