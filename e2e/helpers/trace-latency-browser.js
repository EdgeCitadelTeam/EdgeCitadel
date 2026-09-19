// Run only on jim-eq. Node forwards frame acknowledgments to a private receiver.
const { readFileSync, writeFileSync } = require('node:fs');
const { hostname } = require('node:os');
const path = require('node:path');
const { chromium } = require('playwright');

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
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 }, reducedMotion: 'reduce' });
  const errors = [];
  let writes = 0;
  page.on('pageerror', error => errors.push(error.message));
  page.on('request', request => {
    if (request.url().includes('/api/') && request.method() !== 'GET') writes++;
  });
  try {
    await page.goto('http://127.0.0.1/#execution?run=' + config.scope.trace_id);
    await page.getByLabel('Fleet read credential').fill(token);
    await page.getByRole('button', { name: 'Connect read access', exact: true }).click();
    await page.getByRole('button', { name: 'Pause live', exact: true }).waitFor();
    await page.locator('[data-node-id]').first().waitFor();
    await request('/ready', '');
    const seen = new Set();
    const deadline = performance.now() + config.workload.browser_timeout_s * 1000;
    while (seen.size < config.samples) {
      if (performance.now() > deadline) throw new Error('pilot browser timeout');
      const next = await request('/next');
      if (!next) { await new Promise(resolve => setTimeout(resolve, 25)); continue; }
      if (seen.has(next.event_id)) throw new Error('receiver repeated acknowledged identity');
      // Predeclared terminal samples remain eligible even after pagination grows.
      // Include reveal/filter cost in the conservative display-latency bound.
      if (config.workload.mode === 'open_loop_terminal') {
        await page.getByLabel('Find a step', { exact: true }).fill(next.node_id);
      }
      const node = page.locator(`[data-node-id="${next.node_id}"].state-${next.state}`);
      await node.waitFor({ state: 'visible', timeout: 20000 });
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
      await request('/ack', JSON.stringify({ event_id: next.event_id }));
      seen.add(next.event_id);
    }
    if (errors.length || writes) throw new Error('browser errors or execution writes');
    await page.screenshot({ path: path.join(directory, 'pilot.png') });
    writeFileSync(path.join(directory, 'browser.json'), JSON.stringify({
      samples: seen.size, page_errors: errors, execution_writes: writes, chromium: browser.version(),
      frame_policy: 'visible stable step across double animation frames; host receipt upper bound',
    }, null, 2));
  } finally { await browser.close(); }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
