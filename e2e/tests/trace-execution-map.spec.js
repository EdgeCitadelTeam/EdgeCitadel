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
});
async function requireRetainedRun() {
  if (run) return;
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
}
test.afterAll(() => { credential = null; });
async function connect(page) {
  await page.getByLabel('Fleet read credential').fill(credential);
  await page.getByRole('button', { name: 'Connect read access', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Disconnect read access' })).toBeVisible();
}
async function openRun(page) {
  await requireRetainedRun();
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
  await requireRetainedRun();
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
  const child = spawn('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-task.py ${directory}`], { stdio: ['ignore', 'pipe', 'pipe'] });
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
    `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-denial.py ${directory}`],
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
    `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-task.py ${directory} --collector-outage`],
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
  test.skip(process.env.EDGECITADEL_TRACE_S1_E2E !== '1', 'Requires dedicated-UID Leaf and owned Hermes worker provisioning');
  test.setTimeout(360_000);
  const directory = `/root/edgecitadel-s1-20260919/browser-${Date.now()}`;
  const ssh = command => execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', command], { encoding: 'utf8' });
  ssh(`install -d -m 700 ${directory}`);
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-workers.py`], {
    input: readFileSync(path.resolve(__dirname, '../helpers/trace-multi-worker.py')),
  });
  const child = spawn('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-workers.py ${directory} --browser`],
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
    await expect.poll(() => {
      if (!trace && (child.exitCode !== null || child.signalCode !== null)) throw new Error('Owned worker helper exited before binding; inspect its private logs');
      return trace;
    }, { timeout: 180_000 }).toBeTruthy();
    await page.goto(`/#execution?run=${trace}`);
    await connect(page);
    await expect(page.locator(`[data-node-id="run:${trace}"]`)).toHaveClass(/state-running/);
    ssh(`touch ${directory}/client-ready`);
    await expect.poll(() => dispatched, { timeout: 15_000 }).toBeTruthy();
    await expect(page.locator('[data-node-id^="task:"].state-running')).toHaveCount(3, { timeout: 30_000 });
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-s1-running.png`) });
    await expect.poll(() => {
      if (!settled && (child.exitCode !== null || child.signalCode !== null)) throw new Error('Owned worker helper exited before settlement; inspect its private logs');
      return settled;
    }, { timeout: 170_000 }).toBeTruthy();
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

test('retained branches group repeated operations and reveal exact steps read-only', async ({ page }) => {
  const read = async suffix => {
    const response = await fetch(`${process.env.APP_URL}/api/traces${suffix}`, { headers: { Authorization: `Bearer ${credential}` } });
    expect(response.ok).toBe(true);
    return response.json();
  };
  const list = await read('?limit=100');
  let fixture;
  for (const item of list.items.filter(item => item.root_agent_id?.startsWith('trace-s1-'))) {
    const candidate = await read('/' + item.trace_id);
    if (candidate.nodes.filter(node => node.kind === 'task').length === 3 && candidate.nodes.filter(node => node.kind === 'model').length >= 6) { fixture = candidate; break; }
  }
  expect(fixture).toBeTruthy();
  const taskNode = fixture.nodes.find(node => node.kind === 'task');
  const models = fixture.nodes.filter(node => node.kind === 'model' && node.task_id === taskNode.task_id);
  const selected = models.sort((a, b) => a.id.localeCompare(b.id)).at(-1);
  const writes = [];
  page.on('request', request => {
    if (new URL(request.url()).pathname.startsWith('/api/') && request.method() !== 'GET') writes.push(request.method());
  });
  await page.goto(`/#execution?run=${fixture.trace_id}`);
  await connect(page);
  await page.getByLabel('Find a step').fill('no matching operation');
  const branches = page.getByLabel('Branch browser', { exact: true });
  await branches.getByRole('button', { name: 'Browse branches and repeated steps' }).click();
  await expect(branches).toContainText('3 task branches');
  const taskBranch = branches.getByRole('list', { name: 'Task branches', exact: true }).locator(':scope > li').filter({ has: page.locator(`[data-branch-id="${taskNode.task_id}"]`) });
  await taskBranch.locator(':scope > button').click();
  const operation = taskBranch.getByRole('list', { name: 'Operation groups' }).locator(':scope > li').filter({ has: page.getByRole('button', { name: new RegExp(`${selected.operation} · ${models.length} steps`) }) });
  await operation.locator(':scope > button').focus();
  await page.keyboard.press('Enter');
  const exactStep = operation.locator(`[data-step-id="${selected.id}"]`);
  await exactStep.focus();
  await page.keyboard.press('Enter');
  await expect(page.getByLabel('Find a step')).toHaveValue('');
  await expect(page.locator(`[data-node-id="${selected.id}"]`)).toHaveAttribute('aria-pressed', 'true');
  await expect(page.getByLabel('Selected step details')).toContainText(taskNode.agent_id);
  await expect(exactStep).toHaveAttribute('aria-pressed', 'true');
  await expect(operation.locator(':scope > button')).toHaveAttribute('aria-expanded', 'true');
  for (const width of [1440, 320]) {
    await page.setViewportSize({ width, height: 1000 });
    if (width === 320) await expect(page.getByRole('button', { name: 'All Agents', exact: true })).not.toBeInViewport();
    await branches.scrollIntoViewIfNeeded();
    const bounds = await page.evaluate(() => ({ viewport: innerWidth, width: document.documentElement.scrollWidth }));
    expect(bounds.width).toBeLessThanOrEqual(bounds.viewport);
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-branches-${width}.png`) });
  }
  expect(writes).toEqual([]);
});

test('hostile metadata is rejected or inert and local references are never fetched', async ({ page }) => {
  test.setTimeout(180_000);
  const directory = `/root/edgecitadel-hostile-20260919/run-${Date.now()}`;
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `install -d -m 700 ${directory}`]);
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-metadata.py`], {
    input: readFileSync(path.resolve(__dirname, '../helpers/trace-hostile-metadata.py')),
  });
  const result = JSON.parse(execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-metadata.py ${directory}`], { encoding: 'utf8', timeout: 150_000 }));
  expect(result.rejected_inputs).toBe(5);
  expect(result.rejections_did_not_append).toBe(true);
  expect(result.source_core_exact).toBe(true);
  expect(result.all_core_settled).toBe(true);
  expect(result.owned_connector_revoked).toBe(true);
  const forbiddenRequests = [], writes = [];
  page.on('request', request => {
    if (request.url().includes(result.local_reference) || request.url().includes('trace-metadata-probe')) forbiddenRequests.push(request.url());
    if (new URL(request.url()).pathname.startsWith('/api/') && request.method() !== 'GET') writes.push(request.method());
  });
  await page.goto(`/#execution?run=${result.trace_id}`);
  await connect(page);
  for (const label of result.labels) {
    await page.locator('[data-node-id]').filter({ hasText: label }).click();
    await page.locator('.trace-observations button').first().click();
    await expect(page.getByLabel('Observation details')).toContainText('Local-only reference; not fetched by this dashboard');
    await expect(page.getByLabel('Selected step details').locator('a')).toHaveCount(0);
  }
  await page.getByLabel('Observation details').scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-local-reference.png`) });
  // Deliberately tamper with this browser's read response only. The source schema
  // already rejected this HTML; this separately checks the client trust boundary.
  const hostile = '<img src=x onerror="window.traceInjected=true">';
  let tampered = false;
  await page.route(`**/api/traces/${result.trace_id}*`, async route => {
    if (new URL(route.request().url()).pathname !== `/api/traces/${result.trace_id}`) return route.continue();
    const response = await route.fetch();
    const body = await response.json();
    body.nodes.find(node => node.kind === 'tool').operation = hostile;
    tampered = true;
    await route.fulfill({ response, json: body });
  });
  await page.reload();
  await connect(page);
  await expect(page.getByRole('alert')).toContainText('Core returned inconsistent evidence');
  expect(tampered).toBe(true);
  await expect(page.locator('[data-node-id]')).toHaveCount(0);
  await expect(page.locator('img[src="x"]')).toHaveCount(0);
  expect(await page.evaluate(() => window.traceInjected)).toBeUndefined();
  expect(forbiddenRequests).toEqual([]);
  expect(writes).toEqual([]);
  writeFileSync(path.join(evidence, `${artifactPrefix}-hostile-metadata.json`), JSON.stringify({ ...result,
    browser_local_reference_labeled: true, browser_never_fetched_reference: true,
    browser_labels_inert: true, tampered_read_rejected: true, zero_execution_requests: true,
  }, null, 2) + '\n');
});

test('large retained run expands beyond 500 nodes and exposes every step', async ({ page }) => {
  test.skip(process.env.EDGECITADEL_TRACE_LARGE_E2E !== '1', 'Explicit large fixture opt-in required');
  test.setTimeout(300_000);
  const retained = process.env.EDGECITADEL_TRACE_LARGE_FIXTURE;
  if (retained && !/^\/root\/edgecitadel-large-20260919\/run-[0-9]+$/.test(retained)) throw new Error('Invalid large fixture directory');
  const directory = retained || `/root/edgecitadel-large-20260919/run-${Date.now()}`;
  if (!retained) {
    execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `install -d -m 700 ${directory}`]);
    execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-large.py`], {
      input: readFileSync(path.resolve(__dirname, '../helpers/trace-large-run.py')),
    });
  }
  const result = JSON.parse(execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    retained ? `cat ${directory}/result.json` : `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-large.py ${directory}`], { encoding: 'utf8', timeout: 200_000 }));
  expect(result.source_core_exact).toBe(true);
  expect(result.all_core_settled).toBe(true);
  expect(result.event_count).toBe(1202);
  expect(result.owned_connector_revoked).toBe(true);
  const writes = [], errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('request', request => {
    if (new URL(request.url()).pathname.startsWith('/api/') && request.method() !== 'GET') writes.push(request.method());
  });
  // Observe a cloned Fetch body: CDP cannot reliably retrieve bodies already
  // consumed by the application's bounded stream reader.
  await page.addInitScript(traceId => {
    const fetch = window.fetch.bind(window);
    window.traceGraphReads = [];
    window.fetch = async (...args) => {
      const response = await fetch(...args);
      if (new URL(response.url).pathname === `/api/traces/${traceId}`) {
        window.traceGraphReads.push(response.clone().json().catch(() => ({ capture_error: true })));
      }
      return response;
    };
  }, result.trace_id);
  const cdp = await page.context().newCDPSession(page);
  await page.goto(`/#execution?run=${result.trace_id}`);
  const heapBefore = await cdp.send('Runtime.getHeapUsage');
  const started = performance.now();
  await connect(page);
  await expect(page.getByRole('button', { name: 'Pause live' })).toBeEnabled({ timeout: 30_000 });
  await expect(page.locator('[data-node-id]')).toHaveCount(100);
  const coldLoadMs = performance.now() - started;
  const heapLoaded = await cdp.send('Runtime.getHeapUsage');
  const pages = await page.evaluate(() => Promise.all(window.traceGraphReads));
  expect(pages.every(value => !value.capture_error)).toBe(true);
  const first = pages.find(value => value.page_kind === 'snapshot');
  expect(first.total_nodes).toBeGreaterThan(500);
  expect(first.nodes.length).toBeLessThanOrEqual(500);
  expect(first.expansions.length).toBeGreaterThan(0);
  expect(pages.some(value => value.page_kind === 'expansion')).toBe(true);
  const nodes = new Map(pages.flatMap(value => value.nodes).map(node => [node.id, node]));
  const edges = new Map(pages.flatMap(value => value.edges).map(edge => [edge.id, edge]));
  expect(edges.size).toBe(601);
  expect([...edges.values()].every(edge => edge.status === 'resolved' && nodes.has(edge.from) && nodes.has(edge.to))).toBe(true);
  expect(nodes.size).toBe(first.total_nodes);
  expect([...nodes.values()].filter(node => node.kind === 'tool')).toHaveLength(600);
  expect(new Set(pages.map(value => value.at)).size).toBe(1);
  const seen = new Set();
  let pageCount = 0;
  while (true) {
    const ids = await page.locator('[data-node-id]').evaluateAll(items => items.map(item => item.dataset.nodeId));
    expect(ids.length).toBeLessThanOrEqual(100);
    for (const id of ids) { expect(seen.has(id)).toBe(false); seen.add(id); }
    pageCount++;
    const next = page.getByRole('button', { name: 'Next steps', exact: true });
    if (await next.isDisabled()) break;
    await next.click();
    await expect(page.locator('[data-node-id]').first()).not.toHaveAttribute('data-node-id', ids[0]);
  }
  expect([...seen].sort()).toEqual([...nodes.keys()].sort());
  const last = [...seen].at(-1);
  await page.locator(`[data-node-id="${last}"]`).click();
  await expect(page.getByLabel('Selected step details')).toContainText(last);
  await page.getByRole('button', { name: 'Text view', exact: true }).click();
  await expect(page.getByRole('list', { name: 'Execution step list' })).toBeVisible();
  await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-large-last-page.png`) });
  expect(writes).toEqual([]);
  expect(errors).toEqual([]);
  const { span_ids, ...summary } = result;
  expect(span_ids).toHaveLength(600);
  writeFileSync(path.join(evidence, `${artifactPrefix}-large-run.json`), JSON.stringify({ ...summary,
    graph_nodes: nodes.size, graph_edges: edges.size, graph_responses: pages.length, map_pages: pageCount,
    all_steps_reachable: true, cold_load_ms: coldLoadMs,
    js_heap_before_bytes: heapBefore.usedSize, js_heap_loaded_bytes: heapLoaded.usedSize,
    measurement_scope: 'single cold load and unforced-GC heap samples including cloned-response observer; not commit-to-render latency or retained-memory bounds',
  }, null, 2) + '\n');
  await cdp.detach();
});

test('keyboard focus survives a real live insertion across a map page boundary', async ({ page }) => {
  test.skip(process.env.EDGECITADEL_TRACE_LARGE_E2E !== '1', 'Explicit large fixture opt-in required');
  test.setTimeout(240_000);
  const directory = `/root/edgecitadel-large-20260919/run-${Date.now()}`;
  const ssh = command => execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', command], { encoding: 'utf8' });
  ssh(`install -d -m 700 ${directory}`);
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-large.py`], {
    input: readFileSync(path.resolve(__dirname, '../helpers/trace-large-run.py')),
  });
  const child = spawn('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-large.py ${directory} --focus-update`], { stdio: ['ignore', 'pipe', 'pipe'] });
  let trace, report, exitCode, processError = false;
  child.stderr.on('data', () => { processError = true; });
  const finished = new Promise(resolve => child.on('exit', code => { exitCode = code; resolve(code); }));
  const lines = createInterface({ input: child.stdout });
  lines.on('line', line => {
    const value = JSON.parse(line);
    if (value.stage === 'ready') trace = value.trace_id;
    if (value.all_core_settled) report = value;
  });
  try {
    await expect.poll(() => {
      if (exitCode !== undefined) throw new Error('Owned focus helper exited before readiness');
      return trace;
    }, { timeout: 120_000 }).toBeTruthy();
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(`/#execution?run=${trace}`);
    await connect(page);
    await expect(page.getByRole('option', { name: 'All owners (602)', exact: true })).toHaveCount(1, { timeout: 30_000 });
    const focused = page.locator('[data-node-id]').nth(99);
    const identity = await focused.getAttribute('data-node-id');
    expect(identity).toMatch(/^span:[0-9a-f]{64}$/);
    await page.locator('[data-node-id]').first().focus();
    await page.keyboard.press('End');
    await expect(focused).toBeFocused();
    ssh(`printf '%s' '${identity}' > ${directory}/focused-step`);
    await expect.poll(() => report, { timeout: 60_000 }).toBeTruthy();
    expect(await finished).toBe(0);
    expect(processError).toBe(false);
    expect(report.event_count).toBe(1204);
    expect(report.source_core_exact).toBe(true);
    expect(report.owned_connector_revoked).toBe(true);
    await expect(page.getByText('Showing 101–200 of 603 steps', { exact: true })).toBeVisible();
    await expect(page.locator(`[data-node-id="${identity}"]`)).toBeFocused();
    await page.keyboard.press('Enter');
    await expect(page.getByLabel('Selected step details')).toContainText(identity);
    const outline = await page.locator(`[data-node-id="${identity}"]`).evaluate(element => getComputedStyle(element).outlineStyle);
    expect(outline).not.toBe('none');
    await page.screenshot({ path: path.join(evidence, `${artifactPrefix}-live-focus.png`) });
    const { span_ids, ...summary } = report;
    expect(span_ids).toHaveLength(601);
    writeFileSync(path.join(evidence, `${artifactPrefix}-live-focus.json`), JSON.stringify({ ...summary,
      focus_preserved_across_page_boundary: true, keyboard_selection_after_update: true,
      reduced_motion: true, visible_focus_outline: true,
    }, null, 2) + '\n');
  } finally {
    // Release a waiting owned helper even if browser setup/assertions fail.
    ssh(`test -f ${directory}/focused-step || printf '%s' 'span:${'f'.repeat(64)}' > ${directory}/focused-step`);
    await finished;
    lines.close();
  }
});

test('live burst above 500 nodes catches up through ordered patches without graph refetch', async ({ page }) => {
  test.skip(process.env.EDGECITADEL_TRACE_LARGE_E2E !== '1', 'Explicit large fixture opt-in required');
  test.setTimeout(240_000);
  const directory = `/root/edgecitadel-large-20260919/run-${Date.now()}`;
  const ssh = command => execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', command], { encoding: 'utf8' });
  ssh(`install -d -m 700 ${directory}`);
  execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq', `cat > ${directory}/verify-large.py`], {
    input: readFileSync(path.resolve(__dirname, '../helpers/trace-large-run.py')),
  });
  const child = spawn('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
    `/var/lib/edgecitadel-core/state/supervisor/bin/python ${directory}/verify-large.py ${directory} --burst-update`], { stdio: ['ignore', 'pipe', 'pipe'] });
  let trace, report, exitCode, processError = false, released = false;
  const graphRequests = [], errors = [], writes = [];
  child.stderr.on('data', () => { processError = true; });
  const finished = new Promise(resolve => child.on('exit', code => { exitCode = code; resolve(code); }));
  const lines = createInterface({ input: child.stdout });
  lines.on('line', line => {
    const value = JSON.parse(line);
    if (value.stage === 'ready') trace = value.trace_id;
    if (value.all_core_settled) report = value;
  });
  try {
    await expect.poll(() => {
      if (exitCode !== undefined) throw new Error('Owned burst helper exited before readiness');
      return trace;
    }, { timeout: 120_000 }).toBeTruthy();
    await page.addInitScript(traceId => {
      window.traceBurstChanges = new Map();
      window.traceBurstSockets = [];
      window.traceBurstReadErrors = 0;
      const remember = change => window.traceBurstChanges.set(change.cursor, change.mode);
      const NativeSocket = window.WebSocket;
      window.WebSocket = class extends NativeSocket {
        constructor(url, protocols) {
          super(url, protocols);
          if (new URL(url).pathname !== `/ws/traces/${traceId}`) return;
          window.traceBurstSockets.push(this);
          this.addEventListener('message', event => {
            const message = JSON.parse(event.data);
            if (message.kind === 'trace_change') remember(message.change);
          });
        }
      };
      const fetch = window.fetch.bind(window);
      window.fetch = async (...args) => {
        const response = await fetch(...args);
        if (new URL(response.url).pathname === `/api/traces/${traceId}/changes`) {
          // Count replay as well as socket delivery without exposing signed cursors.
          response.clone().json().then(value => value.changes?.forEach(remember)).catch(() => { window.traceBurstReadErrors++; });
        }
        return response;
      };
    }, trace);
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => {
      const url = new URL(request.url());
      if (released && url.pathname === `/api/traces/${trace}`) graphRequests.push(url.pathname);
      if (url.pathname.startsWith('/api/') && request.method() !== 'GET') writes.push(request.method());
    });
    await page.goto(`/#execution?run=${trace}`);
    await connect(page);
    await expect(page.getByRole('option', { name: 'All owners (502)', exact: true })).toHaveCount(1, { timeout: 30_000 });
    await expect.poll(() => page.evaluate(() => window.traceBurstSockets.some(socket => socket.readyState === 1))).toBe(true);
    await page.evaluate(() => window.traceBurstChanges.clear());
    const started = performance.now();
    released = true;
    ssh(`touch ${directory}/burst-ready`);
    await page.getByLabel('Find a step').fill(`run:${trace}`);
    await expect(page.locator(`[data-node-id="run:${trace}"]`)).toHaveClass(/state-completed/, { timeout: 60_000 });
    const releaseToFinalDisplayMs = performance.now() - started;
    await expect(page.getByRole('option', { name: 'All owners (602)', exact: true })).toHaveCount(1);
    await expect.poll(() => report, { timeout: 60_000 }).toBeTruthy();
    expect(await finished).toBe(0);
    expect(processError).toBe(false);
    expect(report.event_count).toBe(1202);
    expect(report.source_core_exact).toBe(true);
    expect(report.owned_connector_revoked).toBe(true);
    const modes = await page.evaluate(() => [...window.traceBurstChanges.values()]);
    expect(await page.evaluate(() => window.traceBurstReadErrors)).toBe(0);
    expect(modes.length).toBeGreaterThanOrEqual(201);
    expect(modes.every(mode => mode === 'patch')).toBe(true);
    expect(graphRequests).toEqual([]);
    expect(writes).toEqual([]);
    expect(errors).toEqual([]);
    await page.getByLabel('Find a step').fill('');
    const seen = new Set();
    while (true) {
      const nodes = await page.locator('[data-node-id]').evaluateAll(items => items.map(item => ({ id: item.dataset.nodeId, state: item.querySelector('.trace-node-state').textContent })));
      for (const node of nodes) {
        expect(seen.has(node.id)).toBe(false);
        expect(['finished', 'completed']).toContain(node.state);
        seen.add(node.id);
      }
      const next = page.getByRole('button', { name: 'Next steps', exact: true });
      if (await next.isDisabled()) break;
      await next.click();
      await expect(page.locator('[data-node-id]').first()).not.toHaveAttribute('data-node-id', nodes[0].id);
    }
    expect(seen.size).toBe(602);
    const { span_ids, ...summary } = report;
    expect(span_ids).toHaveLength(600);
    writeFileSync(path.join(evidence, `${artifactPrefix}-live-burst.json`), JSON.stringify({ ...summary,
      distinct_patch_commits: modes.length, graph_refetches: graphRequests.length,
      all_final_nodes_reachable: true, release_to_final_display_ms: releaseToFinalDisplayMs,
      timing_scope: 'single harness release-to-final-render sample including emission, collection, projection, transport and observer overhead; not commit-to-render p95',
    }, null, 2) + '\n');
  } finally {
    ssh(`touch ${directory}/burst-ready`);
    await finished;
    lines.close();
  }
});
