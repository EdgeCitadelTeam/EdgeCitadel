const { test, expect } = require('@playwright/test');
const { execFileSync, spawn } = require('node:child_process');
const path = require('node:path');
const { readFileSync, writeFileSync, mkdirSync } = require('node:fs');
const { createInterface } = require('node:readline');

// Explicitly opt in to the existing authorized server; never start a local stack.
test.skip(process.env.EDGECITADEL_TRACE_UI_E2E !== '1', 'Requires the jim-eq trace deployment');
let run, task;
const evidence = path.resolve(__dirname, '../../local-docs/architecture-reviews/end-to-end-flow/execution');
const artifactPrefix = process.env.EDGECITADEL_TRACE_UI_EVIDENCE_PREFIX || 'm6-ui';
if (!/^[a-z0-9-]{1,64}$/.test(artifactPrefix)) throw new Error('Invalid trace UI evidence prefix');
let credential;
test.beforeAll(async () => {
  expect(new URL(process.env.APP_URL).hostname).toBe('jim-eq');
  mkdirSync(evidence, { recursive: true });
  credential = execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `python3 -c 'from pathlib import Path; print(next(line.split("=",1)[1] for line in Path("/root/.edgecitadel/core/.env").read_text().splitlines() if line.startswith("EDGECITADEL_TRACE_READ_TOKEN=")))'`], { encoding: 'utf8' }).trim();
  // Discover an existing completed Hermes run rather than pinning an expiring ID.
  const read = async suffix => {
    const response = await fetch(`${process.env.APP_URL}/api/traces${suffix}`, { headers: { Authorization: `Bearer ${credential}` } });
    if (!response.ok) throw new Error('jim-eq trace fixture read unavailable');
    return response.json();
  };
  const list = await read('?agent_id=jim-eq-hermes&limit=100');
  for (const item of list.items) {
    const graph = await read('/' + item.trace_id);
    const match = graph.nodes.find(node => node.kind === 'task' && node.agent_id === 'jim-eq-hermes' && node.state === 'completed');
    if (match) { run = item.trace_id; task = match.task_id; break; }
  }
  if (!run) throw new Error('The jim-eq fixture needs a retained completed Hermes trace');
});
test.afterAll(() => { credential = null; });
async function connect(page) {
  await page.getByLabel('Fleet read credential').fill(credential);
  await page.getByRole('button', { name: 'Connect read access', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Disconnect read access' })).toBeVisible();
}
async function openRun(page) {
  await page.goto(`/#execution?run=${run}`);
  await connect(page);
  await expect(page.getByRole('button', { name: 'Pause live' })).toBeEnabled();
  await expect(page.locator('[data-node-id]').first()).toBeVisible();
}

test('real retained run: selection, exact observation URL, history reload, themes and narrow panning', async ({ page }) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await openRun(page);
  const selected = page.locator(`[data-node-id="task:${task}"]`);
  await selected.click();
  await expect(page.getByLabel('Selected step details')).toBeVisible();
  await expect(page.locator('.trace-observations button').first()).toBeVisible();
  await page.locator('.trace-observations button').first().click();
  await expect(page.getByLabel('Observation details')).toBeVisible();
  const selectedEvent = new URLSearchParams(new URL(page.url()).hash.slice(11)).get('event');
  expect(selectedEvent).toMatch(/^[a-z0-9_-]+\/[0-9a-f-]+\/[0-9a-f-]+$/);
  await page.getByRole('button', { name: 'Pause live' }).click();
  await expect(page.getByRole('button', { name: 'Resume live' })).toBeVisible();
  const frozen = page.url();
  expect(new URLSearchParams(new URL(frozen).hash.slice(11)).get('at')).toBeTruthy();
  await page.reload();
  await expect(page.getByLabel('Fleet read credential')).toBeVisible();
  await connect(page);
  await expect(page.getByLabel('Selected step details')).toBeVisible();
  expect(page.url()).toBe(frozen);
  await page.getByRole('button', { name: 'Resume live' }).click();
  await expect(page.getByRole('button', { name: 'Pause live' })).toBeEnabled();
  await page.getByRole('button', { name: 'Text view', exact: true }).click();
  await expect(page.getByRole('list', { name: 'Execution step list' })).toBeVisible();
  await page.getByRole('button', { name: 'Map view', exact: true }).click();
  await selected.focus();
  await page.keyboard.press('Home');
  await expect(page.locator('[data-map-index="0"]')).toBeFocused();
  for (const width of [1440, 768, 320]) {
    await page.setViewportSize({ width, height: 1000 });
    for (const theme of ['dark', 'light']) {
      const current = await page.locator('.trace-explorer').getAttribute('data-theme');
      if (current !== theme) await page.getByRole('button', { name: /map theme/ }).click();
      await page.locator('.trace-explorer').evaluate(element => { element.scrollTop = 0; });
      const bounds = await page.evaluate(() => ({ viewport: innerWidth, document: document.documentElement.scrollWidth }));
      expect(bounds.document).toBeLessThanOrEqual(bounds.viewport);
      await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-${theme}-${width}.png`) });
    }
    await selected.scrollIntoViewIfNeeded();
    const size = await selected.boundingBox();
    expect(size.width).toBe(204);
    await selected.click();
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-map-${width}.png`) });
    await page.getByLabel('Selected step details').scrollIntoViewIfNeeded();
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-inspector-${width}.png`) });
  }
  const storage = await page.evaluate(() => [...Object.values(localStorage), ...Object.values(sessionStorage)].join(''));
  expect(storage.includes(credential)).toBe(false);
  expect(page.url().includes(credential)).toBe(false);
  expect(errors).toEqual([]);
});

test('task lookup, Flow entry, tab shortcuts and browser back preserve navigation and memory access', async ({ page }) => {
  await page.goto('/#flow');
  await page.getByRole('button', { name: 'Open execution map' }).click();
  await connect(page);
  await page.evaluate(({ task }) => { location.hash = `execution?task=${task}&step=task%3A${task}`; }, { task });
  const runButton = page.locator('.trace-run-list button').filter({ hasText: run.slice(0, 12) });
  await expect(runButton).toBeVisible();
  await runButton.click();
  await expect(page.getByLabel('Selected step details')).toBeVisible();
  await page.locator('.trace-heading h1').click();
  await page.keyboard.press('2');
  await expect(page.getByText('Communication topology')).toBeVisible();
  await page.goBack();
  await expect(page.getByLabel('Selected step details')).toBeVisible();
  await expect(page.getByLabel('Fleet read credential')).toHaveCount(0);
  await page.getByRole('button', { name: 'Disconnect read access' }).click();
  await expect(page.getByLabel('Fleet read credential')).toBeVisible();
  await expect(page.getByLabel('Selected step details')).toHaveCount(0);
});

test('fresh Hermes execution updates the real browser, reconnects and preserves exact frozen history', async ({ page }) => {
  test.setTimeout(180_000);
  const directory = `/root/edgecitadel-m6-ui-20260918/run-${Date.now()}`;
  const ssh = command => execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', command], { encoding: 'utf8' });
  ssh(`install -d -m 700 ${directory}`);
  const source = readFileSync(path.resolve(__dirname, '../helpers/trace-live-task.py'), 'utf8');
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-task.py`], { input: source });
  const child = spawn('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `/root/.edgecitadel/supervisor/bin/python ${directory}/verify-task.py ${directory}`], { stdio: ['ignore', 'pipe', 'pipe'] });
  let trace, settled, processError = false;
  child.stderr.on('data', () => { processError = true; });
  const finished = new Promise(resolve => child.on('exit', resolve));
  const lines = createInterface({ input: child.stdout });
  lines.on('line', line => {
    const value = JSON.parse(line);
    if (value.stage === 'bound') trace = value.trace_id;
    if (value.acknowledgment_exact) settled = value;
  });
  try {
    await expect.poll(() => trace, { timeout: 20_000 }).toBeTruthy();
    await page.addInitScript(() => {
      const NativeSocket = window.WebSocket;
      window.traceTestSockets = [];
      window.WebSocket = class extends NativeSocket {
        constructor(url, protocols) {
          super(url, protocols);
          if (new URL(url).pathname.startsWith('/ws/traces/')) window.traceTestSockets.push(this);
        }
      };
    });
    await page.goto(`/#execution?run=${trace}`);
    await connect(page);
    await expect(page.getByRole('button', { name: 'Pause live' })).toBeEnabled();
    const original = await page.locator('[data-node-id]').evaluateAll(nodes => nodes.map(node => ({ id: node.dataset.nodeId, left: node.style.left, top: node.style.top })));
    await page.getByRole('button', { name: 'Pause live' }).click();
    const frozen = page.url();
    await expect(page.locator('.trace-run-heading > strong')).toHaveText('historical');
    await page.getByRole('button', { name: 'Resume live' }).click();
    await expect(page.locator('.trace-run-heading > strong')).toHaveText('live');
    ssh(`touch ${directory}/client-ready`);
    await expect.poll(() => settled, { timeout: 140_000 }).toBeTruthy();
    expect(await finished).toBe(0);
    expect(processError).toBe(false);
    expect(settled.all_core_settled).toBe(true);
    const completed = page.locator(`[data-node-id="task:${settled.task_id}"]`);
    await expect(completed).toHaveClass(/state-completed/);
    for (const node of original) {
      const current = await page.locator(`[data-node-id="${node.id}"]`).evaluate(element => ({ left: element.style.left, top: element.style.top }));
      expect(current).toEqual({ left: node.left, top: node.top });
    }
    const beforeReconnect = await page.evaluate(() => {
      const count = window.traceTestSockets.length;
      window.traceTestSockets.at(-1).close();
      return count;
    });
    await expect.poll(() => page.evaluate(() => window.traceTestSockets.length)).toBeGreaterThan(beforeReconnect);
    await expect(page.locator('.trace-run-heading > strong')).toHaveText('live', { timeout: 30_000 });
    await page.evaluate(url => { location.hash = new URL(url).hash; }, frozen);
    await expect(page.locator('.trace-run-heading > strong')).toHaveText('historical');
    await expect(page.getByText('Newer evidence available', { exact: true })).toBeVisible();
    await expect(page.locator('[data-node-id]')).toHaveCount(original.length);
    await page.getByRole('button', { name: 'Resume live' }).click();
    await expect(completed).toHaveClass(/state-completed/);
    writeFileSync(path.join(evidence, `${artifactPrefix}-live-jim-eq.json`), JSON.stringify({ target: 'jim-eq', trace_id: trace, task_id: settled.task_id, event_count: settled.event_count, exact_source_core_settlement: true, browser_live_completed: true, existing_positions_stable: true, frozen_history: true, newer_indicator: true, explicit_resume: true, closed_socket_reconnected_live: true }, null, 2) + '\n');
  } finally { lines.close(); }
});

test('server history discovers unvisited snapshots, preserves selection on refresh and reloads read-only', async ({ page }) => {
  const writes = [];
  page.on('request', request => {
    if (new URL(request.url()).pathname.startsWith('/api/') && request.method() !== 'GET') writes.push(request.method());
  });
  await openRun(page);
  const liveCount = await page.locator('[data-node-id]').count();
  await page.getByRole('button', { name: 'Browse retained history' }).click();
  const history = page.getByRole('region', { name: 'Retained history' });
  await expect(history.getByRole('button', { name: /Snapshot/ }).first()).toBeVisible();
  let pages = 0;
  for (;;) {
    const snapshots = history.locator('.trace-history-list button:enabled');
    if (await snapshots.count()) {
      await snapshots.last().click();
      await expect(page.locator('.trace-run-heading > strong')).toHaveText('historical');
    }
    const older = history.getByRole('button', { name: 'Older history page' });
    if (await older.isDisabled()) break;
    expect(++pages).toBeLessThan(30);
    await Promise.all([
      page.waitForResponse(response => new URL(response.url()).pathname === `/api/traces/${run}/history` && response.status() === 200),
      older.click(),
    ]);
    await expect(history.getByText(/Loading retained history/)).toHaveCount(0);
    await expect(history.getByRole('button', { name: 'Older history page' })).toBeVisible();
  }
  await expect(page.locator('[data-node-id]')).toHaveCount(2);
  expect(liveCount).toBeGreaterThan(2);
  await expect(page.getByText('Newer evidence available', { exact: true })).toBeVisible();
  const frozen = page.url();
  await history.getByRole('button', { name: 'Refresh history' }).click();
  await expect(history.getByRole('button', { name: /Snapshot/ }).first()).toBeVisible();
  expect(page.url()).toBe(frozen);
  await expect(page.locator('[data-node-id]')).toHaveCount(2);
  for (const width of [1440, 320]) {
    await page.setViewportSize({ width, height: 1000 });
    if (width < 768) await expect(page.getByText('All Agents', { exact: true })).not.toBeInViewport();
    await history.scrollIntoViewIfNeeded();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-history-${width}.png`) });
  }
  await page.reload();
  await connect(page);
  await expect(page.locator('.trace-run-heading > strong')).toHaveText('historical');
  await expect(page.locator('[data-node-id]')).toHaveCount(2);
  expect(page.url()).toBe(frozen);
  expect(writes).toEqual([]);
});

test('S4 denied dispatch has permission evidence and no child execution', async ({ page }) => {
  test.setTimeout(180_000);
  const directory = `/root/edgecitadel-s4-20260919/run-${Date.now()}`;
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `install -d -m 700 ${directory}`]);
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-denial.py`], {
    input: readFileSync(path.resolve(__dirname, '../helpers/trace-denied-dispatch.py')),
  });
  const result = JSON.parse(execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    `/root/.edgecitadel/supervisor/bin/python ${directory}/verify-denial.py ${directory}`],
  { encoding: 'utf8', timeout: 150_000 }));
  expect(result.denied_before_child_creation).toBe(true);
  expect(result.no_tasks_on_core_or_leaf).toBe(true);
  expect(result.retry_no_new_events).toBe(true);
  expect(result.all_core_settled).toBe(true);
  expect(result.session_closed_and_connector_revoked).toBe(true);
  writeFileSync(path.join(evidence, `${artifactPrefix}-denied-dispatch.json`), JSON.stringify(result, null, 2) + '\n');
  await page.goto(`/#execution?run=${result.trace_id}`);
  await connect(page);
  const permission = page.locator('[data-node-id^="permission:"]');
  const dispatch = page.locator('[data-node-id^="dispatch:"]');
  await expect(permission).toHaveClass(/state-denied/);
  await expect(dispatch).toHaveClass(/state-denied/);
  await expect(page.locator('[data-node-id^="task:"]')).toHaveCount(0);
  await permission.click();
  await expect(page.getByLabel('Selected step details')).toBeVisible();
  await page.locator('.trace-observations button').first().click();
  await page.getByText('Structured evidence', { exact: true }).click();
  await expect(page.getByLabel('Observation details')).toContainText(result.dispatch_id);
  await expect(page.getByLabel('Observation details')).toContainText('native_connector_capability');
  await expect(page.getByLabel('Observation details')).toContainText('permission_denied');
  await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-denial.png`) });
});

test('S6 collector outage leaves execution running and recovers retained evidence', async ({ page }) => {
  test.setTimeout(240_000);
  const directory = `/root/edgecitadel-s6-20260919/run-${Date.now()}`;
  const ssh = command => execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', command], { encoding: 'utf8' });
  ssh(`install -d -m 700 ${directory}`);
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-task.py`], {
    input: readFileSync(path.resolve(__dirname, '../helpers/trace-live-task.py')),
  });
  const child = spawn('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    `/root/.edgecitadel/supervisor/bin/python ${directory}/verify-task.py ${directory} --collector-outage`],
  { stdio: ['ignore', 'pipe', 'pipe'] });
  let trace, stopped, completed, settled, processError = false;
  child.stderr.on('data', () => { processError = true; });
  const finished = new Promise(resolve => child.on('exit', resolve));
  const lines = createInterface({ input: child.stdout });
  lines.on('line', line => {
    const value = JSON.parse(line);
    if (value.stage === 'bound') trace = value.trace_id;
    if (value.stage === 'collector_stopped') stopped = true;
    if (value.stage === 'completed_while_offline') completed = value;
    if (value.acknowledgment_exact) settled = value;
  });
  try {
    await expect.poll(() => trace, { timeout: 20_000 }).toBeTruthy();
    await page.goto(`/#execution?run=${trace}`);
    await connect(page);
    await expect(page.getByLabel('Collection status')).toContainText('Collector connected');
    const before = await page.locator('[data-node-id]').count();
    expect(before).toBeGreaterThan(0);
    ssh(`touch ${directory}/client-ready`);
    await expect.poll(() => stopped, { timeout: 20_000 }).toBe(true);
    await expect(page.getByLabel('Collection status')).toContainText('Collection unavailable', { timeout: 25_000 });
    ssh(`touch ${directory}/outage-observed`);
    await expect.poll(() => completed, { timeout: 140_000 }).toBeTruthy();
    expect(completed.uncollected_events).toBeGreaterThan(0);
    await expect(page.locator('[data-node-id]')).toHaveCount(before);
    await expect(page.locator('[data-node-id^="task:"]')).toHaveCount(0);
    await expect(page.getByLabel('Collection status')).toContainText('Collection unavailable');
    await page.getByLabel('Collection status').scrollIntoViewIfNeeded();
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-collector-offline.png`) });
    ssh(`touch ${directory}/resume-collector`);
    await expect.poll(() => settled, { timeout: 130_000 }).toBeTruthy();
    expect(await finished).toBe(0);
    expect(processError).toBe(false);
    expect(settled.execution_completed_with_collector_stopped).toBe(true);
    expect(settled.all_core_settled).toBe(true);
    await expect(page.locator(`[data-node-id="task:${settled.task_id}"]`)).toHaveClass(/state-completed/, { timeout: 25_000 });
    await expect(page.getByLabel('Collection status')).toContainText('Collector connected', { timeout: 25_000 });
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-collector-recovered.png`) });
    writeFileSync(path.join(evidence, `${artifactPrefix}-collector-outage.json`), JSON.stringify({ ...settled,
      uncollected_events_during_outage: completed.uncollected_events,
      browser_warned_stale: true, browser_no_fabricated_child: true, browser_recovered_completed_task: true,
    }, null, 2) + '\n');
  } finally {
    // Release every owned handshake on assertion failure; the server helper's
    // finally block restores collection before this test can finish.
    ssh(`touch ${directory}/client-ready ${directory}/outage-observed ${directory}/resume-collector`);
    await finished;
    lines.close();
    const status = await fetch(`${process.env.APP_URL}/api/system/status`).then(response => response.json());
    expect(status.telemetry.state).toBe('running');
    expect(status.nats_connected).toBe(true);
  }
});

test('S1 three real Hermes workers show parallel activity and explicit root completion', async ({ page }) => {
  test.skip(process.env.EDGECITADEL_TRACE_S1_E2E !== '1', 'Requires private Hermes runtime overlay and owned worker provisioning');
  test.setTimeout(360_000);
  const directory = `/root/edgecitadel-s1-20260919/browser-${Date.now()}`;
  const ssh = command => execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', command], { encoding: 'utf8' });
  ssh(`install -d -m 700 ${directory}`);
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-workers.py`], {
    input: readFileSync(path.resolve(__dirname, '../helpers/trace-multi-worker.py')),
  });
  const child = spawn('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    `/root/.edgecitadel/supervisor/bin/python ${directory}/verify-workers.py ${directory} --browser`],
  { stdio: ['ignore', 'pipe', 'pipe'] });
  let trace, dispatched, settled, processError = false;
  child.stderr.on('data', () => { processError = true; });
  const finished = new Promise(resolve => child.on('exit', resolve));
  const lines = createInterface({ input: child.stdout });
  lines.on('line', line => {
    const value = JSON.parse(line);
    if (value.stage === 'bound') trace = value.trace_id;
    if (value.stage === 'dispatched') dispatched = value.task_ids;
    if (value.stage === 'complete') settled = value;
  });
  try {
    await expect.poll(() => trace, { timeout: 180_000 }).toBeTruthy();
    await page.goto(`/#execution?run=${trace}`);
    await connect(page);
    await expect(page.locator(`[data-node-id="run:${trace}"]`)).toHaveClass(/state-running/);
    ssh(`touch ${directory}/client-ready`);
    await expect.poll(() => dispatched, { timeout: 15_000 }).toBeTruthy();
    await expect(page.locator('[data-node-id^="task:"].state-running')).toHaveCount(3, { timeout: 30_000 });
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-s1-running.png`) });
    await expect.poll(() => settled, { timeout: 170_000 }).toBeTruthy();
    expect(await finished).toBe(0);
    expect(processError).toBe(false);
    expect(settled.actual_model_tool_events_per_worker).toBe(true);
    expect(settled.terminal_results_verified_locally).toBe(true);
    expect(settled.three_tool_actions_overlapped).toBe(true);
    expect(settled.all_core_settled).toBe(true);
    expect(settled.owned_workers_removed).toBe(true);
    await expect(page.locator('[data-node-id^="task:"].state-completed')).toHaveCount(3);
    await expect(page.locator(`[data-node-id="run:${trace}"]`)).toHaveClass(/state-completed/);
    const response = await fetch(`${process.env.APP_URL}/api/traces/${trace}`, { headers: { Authorization: `Bearer ${credential}` } });
    expect(response.ok).toBe(true);
    const graph = await response.json();
    expect(graph.expansions).toEqual([]);
    for (const taskId of settled.task_ids) {
      expect(graph.edges.some(edge => edge.from === `run:${trace}` && edge.to === `task:${taskId}` && edge.status === 'resolved')).toBe(true);
      expect(graph.nodes.some(node => node.task_id === taskId && node.kind === 'model' && node.state === 'finished')).toBe(true);
      expect(graph.nodes.some(node => node.task_id === taskId && node.kind === 'tool' && node.state === 'finished')).toBe(true);
    }
    const childTask = page.locator(`[data-node-id="task:${settled.task_ids[0]}"]`);
    await childTask.click();
    await expect(page.getByLabel('Selected step details')).toContainText(settled.workers[0]);
    await page.getByRole('button', { name: 'Text view', exact: true }).click();
    await expect(page.getByRole('list', { name: 'Execution step list' })).toContainText(settled.workers[2]);
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-s1-completed.png`) });
    writeFileSync(path.join(evidence, `${artifactPrefix}-s1.json`), JSON.stringify({ ...settled, browser_three_running_children: true, resolved_root_child_links: true, browser_completed_root: true }, null, 2) + '\n');
  } finally {
    ssh(`touch ${directory}/client-ready`);
    await finished;
    lines.close();
  }
});
