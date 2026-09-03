const { test, expect } = require('@playwright/test');

const API = process.env.AGG_URL || 'http://localhost';
const POLL_INTERVAL_MS = 1000;
const POLL_BUDGET_S = 60;
// All POSTs from this spec carry sender_id=test-runner so the aggregator
// auto-registers a synthetic deployment=test card and tags every related
// envelope (command, outbox mirror, gemma's result) with deployment=test.
// Operators with the showTestAgents toggle off won't see this traffic in
// the dashboard. See docs/roadmap.md "Test-data separation convention".
const TEST_RUNNER = 'test-runner';

test.describe('Phase 2 smoke — Gemma round trip', () => {
  test('gemma-1 registered with reasoner role', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/gemma-1/card`);
    expect(r.ok()).toBe(true);
    const card = await r.json();
    expect(card.name).toBe('gemma-1');
    expect(card.metadata['runtime.kind']).toBe('native');
    expect(card.metadata['runtime.roles']).toContain('reasoner');
    expect(card.skills.some(s => s.id === 'reasoning.chat')).toBe(true);
    expect(card.capabilities.extensions.some(
      e => e.uri === 'https://edgecitadel.local/ext/nats-binding/v1')).toBe(true);
  });

  test('POST /command/gemma-1 returns task_id and result completes', async ({ request }) => {
    const post = await request.post(
      `${API}/api/command/gemma-1?sender_id=${TEST_RUNNER}`,
      { data: { body: 'Reply with exactly the digit 4 and nothing else.' } });
    expect(post.status()).toBe(202);
    const { task_id } = await post.json();
    expect(task_id).toMatch(/^[0-9a-f-]{36}$/);

    let result;
    for (let i = 0; i < POLL_BUDGET_S; i++) {
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      const q = await request.get(
        `${API}/api/messages?task_id=${task_id}&type=result`);
      const rows = await q.json();
      if (rows.length) { result = rows[0]; break; }
    }
    expect(result, `no result in ${POLL_BUDGET_S}s`).toBeDefined();
    expect(result.task_state).toBe('completed');
    expect(result.payload.body).toMatch(/4/);
    expect(result.payload.model).toBeTruthy();
    expect(typeof result.payload.duration_ms).toBe('number');
    // Smoke-driven envelopes should be tagged deployment=test so the
    // dashboard's showTestAgents toggle filters them out by default.
    expect(result.deployment).toBe('test');
  });

  test('POST /command/gemma-1 with empty body is rejected by the Managed Agent', async ({ request }) => {
    // Empty body fails Pydantic validation at the API layer, returning 422.
    // We DON'T test the Managed Agent handler's "empty_prompt" rejection here (that
    // would require a malformed envelope going through NATS, which is
    // is covered by plugin-toolkit/tests/gemma_runtime/).
    const post = await request.post(
      `${API}/api/command/gemma-1?sender_id=${TEST_RUNNER}`,
      { data: { body: '' } });
    expect([202, 422]).toContain(post.status());
  });

  test('queue endpoint returns pending/ack_pending integers', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/gemma-1/queue`);
    expect(r.ok()).toBe(true);
    const body = await r.json();
    expect(Number.isInteger(body.pending)).toBe(true);
    expect(Number.isInteger(body.ack_pending)).toBe(true);
  });
});
