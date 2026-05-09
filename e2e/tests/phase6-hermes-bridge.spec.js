// Phase 6 E2E — Hermes bridge adapter.
// Requires: hermes serve running on :8642 (or a mock fixture), and
// the bridge adapter running with HERMES_TOKEN configured.
// Skipped automatically if us-mac-hermes is not registered within 10s.

import { test, expect } from '@playwright/test';

const HERMES_AGENT_ID = 'us-mac-hermes';

async function waitForAgent(request, agentId, timeoutMs = 10000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const resp = await request.get(`/api/agents/${agentId}/card`);
    if (resp.ok()) return await resp.json();
    await new Promise(r => setTimeout(r, 500));
  }
  return null;
}

test.describe('Phase 6 — Hermes bridge', () => {

  test('us-mac-hermes registers with bridge runtime kind', async ({ request }) => {
    const card = await waitForAgent(request, HERMES_AGENT_ID);
    test.skip(!card, 'us-mac-hermes not online — install hermes locally and start the bridge adapter to run this spec');
    expect(card.metadata['runtime.kind']).toBe('bridge');
    expect(card.metadata['runtime.upstream']).toBe('hermes-agent');
    expect(card.metadata['runtime.tags']).toContain('external-memory');
    expect(card.capabilities.streaming).toBe(true);
  });

  test('command roundtrip yields a non-empty result envelope', async ({ request }) => {
    const card = await waitForAgent(request, HERMES_AGENT_ID);
    test.skip(!card, 'us-mac-hermes not online');
    // Send a smoke command via the test harness — implementation depends on
    // the existing aggregator dispatch endpoint; mirror the gemma E2E pattern.
    const resp = await request.post('/api/agents/us-mac-hermes/dispatch', {
      data: { body: 'reply with the word teal', context_id: 'e2e-ctx-1' },
    });
    expect(resp.ok()).toBeTruthy();
    const result = await resp.json();
    expect(result.task_state).toBe('completed');
    expect(result.payload.body).toBeTruthy();
    expect(result.payload.upstream).toBe('hermes-agent');
  });

  test('multi-turn within a context_id shows continuity (Hermes upstream memory)',
    async ({ request }) => {
      const card = await waitForAgent(request, HERMES_AGENT_ID);
      test.skip(!card, 'us-mac-hermes not online');
      const ctx = `e2e-ctx-${Date.now()}`;

      const turn1 = await request.post('/api/agents/us-mac-hermes/dispatch',
        { data: { body: 'My favourite colour is teal. Reply with "ok".',
                   context_id: ctx } });
      expect(turn1.ok()).toBeTruthy();

      const turn2 = await request.post('/api/agents/us-mac-hermes/dispatch',
        { data: { body: 'What is my favourite colour? Reply with the colour name only.',
                   context_id: ctx } });
      expect(turn2.ok()).toBeTruthy();
      const r2 = await turn2.json();
      expect(r2.payload.body.toLowerCase()).toContain('teal');
    });

  test('aggregator does not record turns in conversation_turns for us-mac-hermes',
    async ({ request }) => {
      const card = await waitForAgent(request, HERMES_AGENT_ID);
      test.skip(!card, 'us-mac-hermes not online');
      // After the previous tests sent commands, the conversations endpoint
      // should still report zero turns for us-mac-hermes (memory ownership rule).
      const resp = await request.get('/api/conversations?agent_id=us-mac-hermes');
      expect(resp.ok()).toBeTruthy();
      const rows = await resp.json();
      expect(Array.isArray(rows)).toBe(true);
      expect(rows.length).toBe(0);
    });

});
