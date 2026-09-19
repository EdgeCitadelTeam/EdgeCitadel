// Run only on jim-eq. Node forwards frame acknowledgments to a private receiver.
const { readFileSync, writeFileSync } = require('node:fs');
const { hostname } = require('node:os');
const path = require('node:path');
const { chromium } = require('playwright');
const { monitorPage, captureFailure } = require('./trace-browser-diagnostics');

async function main() {
  if (hostname().toLowerCase() !== 'jim-eq') throw new Error('jim-eq only');
  const directory = process.argv[2];
  if (!path.isAbsolute(directory)) throw new Error('absolute output directory required');
  const config = JSON.parse(readFileSync(path.join(directory, 'scope.json')));
  const token = readFileSync('/root/.edgecitadel/core/.env', 'utf8').split('\n')
    .find(line => line.startsWith('EDGECITADEL_TRACE_READ_TOKEN=')).split('=')[1];
  const request = async (suffix, body) => {
    const response = await fetch(config.receiver.url + suffix, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: 'Bearer ' + config.receiver.token },
      body: body === undefined ? undefined : body,
      signal: AbortSignal.timeout(3000),
    });
    if (!response.ok) throw new Error('receiver rejected request: ' + response.status);
    return response.json();
  };
  const browser = await chromium.launch({ executablePath: '/snap/bin/chromium', headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  let pageErrors = 0;
  let writes = 0;
  const lanes = [];
  let stage = 'setup';
  let currentSample = null;
  try {
    const laneCount = config.workload.mode === 'baseline' ? config.workload.sampled_agents.length : 1;
    for (let lane = 0; lane < laneCount; lane++) {
      const page = await browser.newPage({ viewport: { width: 1440, height: 1200 }, reducedMotion: 'reduce' });
      page.on('pageerror', () => pageErrors++);
      const laneState = { page, trace: config.scope.trace_id || null, samples: 0, diagnostics: monitorPage(page) };
      lanes.push(laneState);
      page.on('request', request => {
        if (request.url().includes('/api/') && request.method() !== 'GET') writes++;
      });
      const trace = config.scope.trace_id || null;
      await page.goto('http://127.0.0.1/#execution' + (trace ? '?run=' + trace : ''));
      await page.getByLabel('Fleet read credential').fill(token);
      await page.getByRole('button', { name: 'Connect read access', exact: true }).click();
      await page.getByRole('button', { name: 'Disconnect read access', exact: true }).waitFor();
      if (trace) await page.locator('[data-node-id]').first().waitFor();
    }
    stage = 'ready';
    await request('/ready', '');
    const seen = new Set();
    const deadline = performance.now() + config.workload.browser_timeout_s * 1000;
    while (seen.size < config.samples) {
      if (performance.now() > deadline) throw new Error('pilot browser timeout');
      stage = 'next';
      const next = await request('/next');
      if (!next) { await new Promise(resolve => setTimeout(resolve, 25)); continue; }
      if (seen.has(next.event_id)) throw new Error('receiver repeated acknowledged identity');
      currentSample = { event_id: next.event_id, node_id: next.node_id, trace_id: next.trace_id, state: next.state, lane: next.lane };
      const lane = lanes[next.lane];
      if (!lane) throw new Error('undeclared browser lane');
      const page = lane.page;
      stage = 'navigate';
      if (next.trace_id && lane.trace !== next.trace_id) {
        await page.evaluate(trace => { location.hash = '#execution?run=' + trace; }, next.trace_id);
        await page.locator('.trace-run-heading code').filter({ hasText: next.trace_id }).waitFor();
        lane.trace = next.trace_id;
      }
      // Predeclared terminal samples remain eligible even after pagination grows.
      // Include reveal/filter cost in the conservative display-latency bound.
      stage = 'filter';
      if (config.workload.mode !== 'closed_loop') {
        await page.getByLabel('Find a step', { exact: true }).fill(next.node_id);
      }
      const node = page.locator(`[data-node-id="${next.node_id}"].state-${next.state}`);
      stage = 'visible';
      await node.waitFor({ state: 'visible', timeout: 20000 });
      stage = 'frame';
      await node.scrollIntoViewIfNeeded();
      await node.evaluate(element => new Promise((resolve, reject) => {
        const state = element.className;
        requestAnimationFrame(() => requestAnimationFrame(() => {
          const rect = element.getBoundingClientRect();
          const cx = rect.x + rect.width / 2, cy = rect.y + rect.height / 2;
          if (!element.isConnected || element.className !== state || document.visibilityState !== 'visible' ||
              rect.width <= 0 || rect.height <= 0 || cx < 0 || cy < 0 || cx >= innerWidth || cy >= innerHeight ||
              !element.contains(document.elementFromPoint(cx, cy))) {
            reject(new Error('step was not visible and stable at frame acknowledgment'));
          } else resolve();
        }));
      }));
      stage = 'ack';
      await request('/ack', JSON.stringify({ event_id: next.event_id }));
      seen.add(next.event_id);
      lane.samples++;
    }
    stage = 'report';
    if (pageErrors || writes) throw new Error('browser errors or execution writes');
    for (const [index, lane] of lanes.entries()) {
      await lane.page.screenshot({ path: path.join(directory, index ? `pilot-${index}.png` : 'pilot.png') });
    }
    writeFileSync(path.join(directory, 'browser.json'), JSON.stringify({
      samples: seen.size, lanes: lanes.map(lane => ({ samples: lane.samples })), page_errors: [], execution_writes: writes, chromium: browser.version(),
      frame_policy: 'visible stable step across double animation frames; host receipt upper bound',
    }, null, 2));
  } catch (error) {
    // Preserve the primary failure if diagnostics fail; never turn it into a pass.
    try {
      const failure = await captureFailure(lanes, { stage, sample: currentSample, page_errors: pageErrors, execution_writes: writes });
      writeFileSync(path.join(directory, 'browser-failure.json'), JSON.stringify(failure, null, 2), { mode: 0o600, flag: 'wx' });
    } catch { console.error('browser failure diagnostics unavailable'); }
    throw error;
  } finally { await browser.close(); }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
