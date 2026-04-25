const base = require('@playwright/test');
const crypto = require('crypto');
const { TestAPIClient } = require('./api-client');
const { TestWSClient } = require('./ws-client');

/**
 * Build a v0.1 canonical envelope (A2A-aligned).
 *
 * Required: type, sender_id
 * Optional: recipient_id, task_id, body (becomes payload.body), payload (verbatim)
 *
 * `payload` takes precedence over `body`. If neither is supplied, payload is {}.
 *
 * The timestamp uses Date.prototype.toISOString() which already emits
 * `YYYY-MM-DDTHH:MM:SS.sssZ` matching the canonical regex enforced by the
 * aggregator validator.
 */
function buildCanonicalEnvelope({ type, sender_id, recipient_id, task_id, body, payload } = {}) {
  if (!type) throw new Error('buildCanonicalEnvelope: type is required');
  if (!sender_id) throw new Error('buildCanonicalEnvelope: sender_id is required');
  return {
    v: 1,
    id: crypto.randomUUID(),
    type,
    sender_id,
    ...(recipient_id ? { recipient_id } : {}),
    ...(task_id ? { task_id } : {}),
    timestamp: new Date().toISOString(),
    payload: payload ?? (body !== undefined ? { body } : {}),
  };
}

/**
 * Extended Playwright test with custom fixtures for API and WebSocket clients.
 * Each fixture auto-connects before the test and disconnects after.
 *
 * MQTT helpers were removed in v0.1: agents publish to NATS via JetStream,
 * not to MQTT topics. Tests that need to inject envelopes should drive the
 * pipeline through the HTTP API or run a real adapter as part of the stack.
 */
const test = base.test.extend({
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

module.exports = { test, expect, buildCanonicalEnvelope };
