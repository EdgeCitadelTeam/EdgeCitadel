// Regression coverage for the Phase 6 streaming-card-fragmentation bug.
// Symptom (May 2026): a single streaming response from gemma-1 / us-mac-hermes
// rendered as one MessageBubble PER persisted task.progress envelope —
// 50+ tiny fragmented cards instead of one growing reply bubble.
// Root cause was twofold:
//   - useWebSocket.js read `payload.delta`, but the canonical Context.publish
//     contract (edgecitadel_plugin_runtime.pull_consumer.Context.publish_progress)
//     puts the chunk text at `payload.message`. The Gemma Plugin happened to
//     mirror to `delta` redundantly; non-Gemma Plugins (Hermes bridge) did
//     not, so the synthetic bubble grew empty for them.
//   - ChatHistory.jsx merged historical /api/messages results 1:1 into the
//     timeline without filtering task.progress, so every persisted streaming
//     chunk became its own bubble regardless of the live reducer.
// Fix: read payload.message in useWebSocket.js, and exclude task.progress
// from the chat timeline unless the operator explicitly picks that type
// from the filter dropdown (forensic view).

const { test, expect } = require('@playwright/test');

const TEST_RUNNER = 'streaming-frag-runner';
// Gemma-1 is the canonical streaming target in the test stack. The same
// invariant holds for any agent with capabilities.streaming=true.
const AGENT = 'gemma-1';

async function pollForResult(request, taskId, timeoutMs = 240_000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const r = await request.get(
      `/api/messages?task_id=${taskId}&type=result&limit=2`);
    if (r.ok()) {
      const rows = await r.json();
      if (rows.length > 0) return rows[0];
    }
    await new Promise(res => setTimeout(res, 750));
  }
  throw new Error(`No result for task ${taskId} within ${timeoutMs}ms`);
}

// The aggregator auto-tags everything in this task with
// deployment=test (because sender_id != default — see roadmap §"Test-data
// separation convention"). The dashboard hides test-deployment rows by
// default, so the spec must opt the running browser session into showing
// them BEFORE first navigation. Setting localStorage in an init script
// makes the appStore initializer (`s.showTestAgents`) read `true`.
test.beforeEach(async ({ page }) => {
  await page.addInitScript(() =>
    window.localStorage.setItem('showTestAgents', 'true'));
});

test.describe('streaming UI does not fragment into per-chunk cards', () => {

  // Production gemma-1 can be queued behind a prior long task — give the
  // spec enough budget to absorb that without flaking. 360s = 60s
  // command-visibility window + 30s streaming-sample window + ~240s
  // result poll + ~30s settle.
  test.describe.configure({ timeout: 360_000 });

  test('long response renders as one growing bubble, not N progress cards',
    async ({ page, request }) => {
      await page.goto('/');

      // Send a long-response prompt directly via the API so the test
      // is not coupled to CommandInput markup churn.
      const post = await request.post(
        `/api/command/${AGENT}?sender_id=${TEST_RUNNER}`,
        { data: { body:
          'Count slowly from 1 to 30, one number per line, with a short note ' +
          'after each number describing whether it is prime. Use prose, no lists.' } });
      expect(post.ok()).toBeTruthy();
      const { task_id: taskId } = await post.json();

      // Live phase: the synthetic streaming bubble should appear quickly.
      // While the response is generating, we expect AT MOST 2 bubbles for
      // this task (the user command echo + the synthetic streaming bubble).
      // task.progress envelopes — even though many are persisted — must
      // NOT each become a bubble.
      // 60s window covers the case where the target agent is queued behind
      // a prior task before the command bubble even hits the polling pull.
      await expect(page.locator(`[data-task-id="${taskId}"]`).first())
        .toBeVisible({ timeout: 60_000 });

      // Sample card count repeatedly through streaming. Card-fragmentation
      // would manifest as N>2 bubbles for this task — assert ≤2 throughout.
      const sampleEnd = Date.now() + 30_000;
      while (Date.now() < sampleEnd) {
        const cardsForTask = await page.locator(`[data-task-id="${taskId}"]`).count();
        expect(cardsForTask, 'streaming cards should not fragment')
          .toBeLessThanOrEqual(2);
        await page.waitForTimeout(1_500);
      }

      // Wait for completion + ChatHistory poll cycle (default 3s) plus margin.
      await pollForResult(request, taskId);
      await page.waitForTimeout(6_000);

      const finalCards = await page.locator(`[data-task-id="${taskId}"]`).count();
      expect(finalCards, 'final view should be command + result only')
        .toBeLessThanOrEqual(2);

      // Sanity: production really did emit task.progress envelopes
      // (we did not silently regress the streaming pipeline itself).
      const all = await (await request.get(
        `/api/messages?task_id=${taskId}&limit=500`)).json();
      const progressCount = all.filter(m => m.type === 'task.progress').length;
      expect(progressCount, 'streaming pipeline must still emit task.progress')
        .toBeGreaterThan(2);
    });

  test('forensic view: task.progress filter still surfaces persisted chunks',
    async ({ page, request }) => {
      // Pre-seed a streaming task so there is something to surface.
      const post = await request.post(
        `/api/command/${AGENT}?sender_id=${TEST_RUNNER}`,
        { data: { body: 'Count from 1 to 10, one number per line.' } });
      const { task_id: taskId } = await post.json();
      await pollForResult(request, taskId);

      await page.goto('/');
      const filterSelect = page.locator('select')
        .filter({ has: page.locator('option', { hasText: 'All types' }) });
      await filterSelect.selectOption('task.progress');
      await page.waitForTimeout(4_000);

      // With the filter pinned to task.progress, the persisted chunks
      // are intentionally surfaced one-bubble-per-chunk for forensic
      // inspection (this is the opt-in escape hatch from the default
      // collapsed view).
      const cards = await page.locator(`[data-task-id="${taskId}"]`).count();
      expect(cards, 'forensic filter exposes persisted progress envelopes')
        .toBeGreaterThan(0);
    });

});
