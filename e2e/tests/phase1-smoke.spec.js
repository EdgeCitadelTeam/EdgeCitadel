const { test, expect } = require('@playwright/test');
const crypto = require('node:crypto');

const API = process.env.AGG_URL;

test.describe('Phase 1 smoke — canonical envelope round trip', () => {
  test('system status has no mqtt_connected', async ({ request }) => {
    const r = await request.get(`${API}/api/system/status`);
    expect(r.ok()).toBe(true);
    const body = await r.json();
    expect(body).not.toHaveProperty('mqtt_connected');
    expect(body).toHaveProperty('nats_connected');
    expect(body).toHaveProperty('jetstream_stream_ok');
  });

  test('shell-1 registered with A2A card', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/shell-1/card`);
    expect(r.ok()).toBe(true);
    const card = await r.json();
    expect(card.name).toBe('shell-1');
    expect(card.metadata['runtime.kind']).toBe('native');
    expect(card.metadata['runtime.roles']).toContain('worker');
    expect(
      card.capabilities.extensions.some(
        (e) => e.uri === 'https://edgecitadel.local/ext/nats-binding/v1',
      ),
    ).toBe(true);
  });

  test('POST /command returns task_id and result arrives', async ({ request }) => {
    const nonce = crypto.randomUUID();
    const post = await request.post(`${API}/api/command/shell-1`, {
      data: { body: nonce },
    });
    expect(post.status()).toBe(202);
    const { task_id } = await post.json();
    expect(task_id).toMatch(/^[0-9a-f-]{36}$/);

    let result;
    for (let i = 0; i < 30; i++) {
      await new Promise((r) => setTimeout(r, 500));
      const q = await request.get(
        `${API}/api/messages?task_id=${task_id}&type=result`,
      );
      const rows = await q.json();
      if (rows.length) {
        result = rows[0];
        break;
      }
    }
    expect(result).toBeDefined();
    expect(result.task_state).toBe('completed');
    expect(result.payload.body).toBe(`edgecitadel:${nonce}`);
    expect(result).not.toHaveProperty('receiver_id');
    expect(result).not.toHaveProperty('message_type');
  });

  test('queue endpoint returns pending/ack_pending integers', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/shell-1/queue`);
    expect(r.ok()).toBe(true);
    const body = await r.json();
    expect(Number.isInteger(body.pending)).toBe(true);
    expect(Number.isInteger(body.ack_pending)).toBe(true);
  });

  test('subject inventory coverage — DB contains each persisted type', async ({ request }) => {
    const r = await request.get(`${API}/api/messages?limit=500`);
    const rows = await r.json();
    const types = new Set(rows.map((x) => x.type));
    for (const t of ['command', 'result']) {
      expect(types.has(t), `missing type=${t}`).toBe(true);
    }
  });
});
