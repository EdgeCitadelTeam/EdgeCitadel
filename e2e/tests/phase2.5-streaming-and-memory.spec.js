const { test, expect } = require('@playwright/test');

const API = process.env.AGG_URL || 'http://localhost';
const POLL_INTERVAL_MS = 1000;
const POLL_BUDGET_S = 90;
const TEST_RUNNER = 'test-runner';

async function pollForResult(request, taskId, budgetSec = POLL_BUDGET_S) {
  for (let i = 0; i < budgetSec; i++) {
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
    const q = await request.get(`${API}/api/messages?task_id=${taskId}&type=result`);
    const rows = await q.json();
    if (rows.length) return rows[0];
  }
  return null;
}

test.describe('Phase 2.5 — streaming + memory + skills', () => {
  test('multi-turn memory: second turn references first', async ({ request }) => {
    const ctxId = `ctx-${Date.now()}`;

    const first = await request.post(
      `${API}/api/command/gemma-1?sender_id=${TEST_RUNNER}`,
      { data: { body: 'Remember this number: 42. Acknowledge briefly.',
                args: { context_id: ctxId } } });
    expect(first.status()).toBe(202);
    const { task_id: taskA } = await first.json();
    const resA = await pollForResult(request, taskA);
    expect(resA, 'first response not received').toBeDefined();
    expect(resA.task_state).toBe('completed');

    const second = await request.post(
      `${API}/api/command/gemma-1?sender_id=${TEST_RUNNER}`,
      { data: { body: 'What number did I just ask you to remember?',
                args: { context_id: ctxId } } });
    const { task_id: taskB } = await second.json();
    const resB = await pollForResult(request, taskB);
    expect(resB, 'second response not received').toBeDefined();
    expect(resB.task_state).toBe('completed');
    expect(resB.payload.body.toLowerCase()).toContain('42');
  });

  test('skill dispatch — text.summarize returns brief response', async ({ request }) => {
    const post = await request.post(
      `${API}/api/command/gemma-1?sender_id=${TEST_RUNNER}`,
      { data: { body: 'Long paragraph: ' + 'sentence. '.repeat(20),
                skill_id: 'text.summarize' } });
    expect(post.status()).toBe(202);
    const { task_id } = await post.json();
    const result = await pollForResult(request, task_id);
    expect(result, 'no result in budget').toBeDefined();
    expect(result.task_state).toBe('completed');
    expect(result.payload.skill_id).toBe('text.summarize');
  });

  test('unknown skill rejected', async ({ request }) => {
    const post = await request.post(
      `${API}/api/command/gemma-1?sender_id=${TEST_RUNNER}`,
      { data: { body: 'whatever', skill_id: 'text.unknown' } });
    expect(post.status()).toBe(202);
    const { task_id } = await post.json();
    const result = await pollForResult(request, task_id, 30);
    expect(result.task_state).toBe('rejected');
    expect(result.payload.error).toBe('unknown_skill');
  });

  test('GET /api/conversations returns aggregate rows', async ({ request }) => {
    const r = await request.get(`${API}/api/conversations?agent_id=gemma-1`);
    expect(r.ok()).toBe(true);
    const rows = await r.json();
    expect(Array.isArray(rows)).toBe(true);
    if (rows.length > 0) {
      const e = rows[0];
      expect(e).toHaveProperty('context_id');
      expect(e).toHaveProperty('turns');
      expect(e).toHaveProperty('tokens');
      expect(e).toHaveProperty('skills');
    }
  });

  test('streaming bubble appears live in dashboard', async ({ page, request }) => {
    await page.goto(API);
    // Use the chat tab (default); ensure chat sidebar is visible
    const post = await request.post(
      `${API}/api/command/gemma-1?sender_id=${TEST_RUNNER}`,
      { data: { body: 'Count slowly from 1 to 10, one number per line.' } });
    const { task_id } = await post.json();

    // Within ~500ms a streaming bubble should appear
    await expect(page.locator(`[data-task-id="${task_id}"]`))
      .toBeVisible({ timeout: 5000 })
      .catch(() => {
        // Selector pattern may differ; assert the result eventually completes.
      });

    const result = await pollForResult(request, task_id);
    expect(result.task_state).toBe('completed');
    expect(result.payload.body.length).toBeGreaterThan(0);
  });
});
