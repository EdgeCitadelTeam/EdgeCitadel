const { test, expect } = require('@playwright/test');
const { connect } = require('@nats-io/transport-node');
const { randomUUID } = require('node:crypto');

const API = process.env.AGG_URL || 'http://localhost';

async function publishOneHeartbeat() {
  if (!process.env.NATS_TOKEN) throw new Error('isolated launcher did not provide NATS_TOKEN');
  const nc = await connect({
    servers: process.env.NATS_URL || 'nats://localhost:4222',
    token: process.env.NATS_TOKEN,
  });
  const now = () => new Date().toISOString();
  const card = {
    name: 'tester-1',
    description: 'system reconciliation fixture',
    version: '0',
    url: 'u',
    provider: { organization: 'x' },
    capabilities: {
      streaming: false,
      extensions: [{ uri: 'https://edgecitadel.local/ext/nats-binding/v1' }],
    },
    securitySchemes: {},
    metadata: {
      'runtime.kind': 'native',
      'runtime.roles': ['worker'],
      'runtime.conformance': 'L1',
      'runtime.heartbeat_interval_sec': 10,
      'runtime.deployment': 'test',
    },
  };
  try {
    await nc.publish('agents.tester-1.register', Buffer.from(JSON.stringify({
      v: 1,
      id: randomUUID(),
      type: 'register',
      sender_id: 'tester-1',
      timestamp: now(),
      payload: card,
    })));
    await nc.publish('agents.tester-1.heartbeat', Buffer.from(JSON.stringify({
      v: 1,
      id: randomUUID(),
      type: 'heartbeat',
      sender_id: 'tester-1',
      timestamp: now(),
      payload: {},
    })));
    await nc.flush();
  } finally {
    await nc.close();
  }
}

test.describe('Phase 3 — system-owned presence reconciliation', () => {
  test.beforeAll(async () => {
    await publishOneHeartbeat();
  });

  test('stale Agent becomes offline and Core rejects new work', async ({ request }) => {
    let state;
    for (let attempt = 0; attempt < 40; attempt++) {
      const response = await request.get(`${API}/api/agents/tester-1`);
      if (response.ok()) {
        state = (await response.json()).agent_state;
        if (state === 'offline') break;
      }
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    expect(state).toBe('offline');

    const command = await request.post(
      `${API}/api/command/tester-1?sender_id=test-runner`,
      { data: { body: 'hello' } },
    );
    expect(command.status()).toBe(409);
    expect((await command.json()).detail).toContain('offline');
  });
});
