const base = require('@playwright/test');
const { TestNATSClient } = require('./nats-client');
const { TestAPIClient } = require('./api-client');
const { TestWSClient } = require('./ws-client');

/**
 * Extended Playwright test with custom fixtures for NATS, API, and WebSocket clients.
 * Each fixture auto-connects before the test and disconnects after.
 *
 * The `mqttClient` fixture name is kept for backward compatibility with existing test files.
 * It uses NATS (TestNATSClient) under the hood.
 */
const test = base.test.extend({
  mqttClient: async ({}, use) => {
    const client = new TestNATSClient();
    await client.connect();
    await use(client);
    await client.disconnect();
  },

  apiClient: async ({}, use) => {
    const client = new TestAPIClient();
    await use(client);
  },

  wsClient: async ({}, use) => {
    const client = new TestWSClient();
    await client.connect('/stream');
    await use(client);
    await client.disconnect();
  },

  wsLogClient: async ({}, use) => {
    const client = new TestWSClient();
    await client.connect('/logs');
    await use(client);
    await client.disconnect();
  },
});

const { expect } = base;

module.exports = { test, expect };
