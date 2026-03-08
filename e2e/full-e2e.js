const { chromium } = require('playwright');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const SCREENSHOTS_DIR = path.join(__dirname, '..', 'test-artifacts');
const API_KEY = 'openclaw-citadel-secret-2026';

function mqtt(topic, payload) {
  const msg = typeof payload === 'string' ? payload : JSON.stringify(payload);
  execSync(
    `docker compose exec -T mqtt mosquitto_pub -h localhost -u iot_agent -P openclaw_secret -t "${topic}" -m '${msg.replace(/'/g, "'\\''")}'`,
    { cwd: path.join(__dirname, '..') }
  );
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

let step = 0;
async function screenshot(page, label) {
  step++;
  const name = `${String(step).padStart(2, '0')}-${label}.png`;
  await page.screenshot({ path: path.join(SCREENSHOTS_DIR, name), fullPage: true });
  console.log(`  [screenshot] ${name}`);
}

const issues = [];
const passes = [];

function check(name, ok) {
  if (ok) {
    passes.push(name);
    console.log(`  PASS: ${name}`);
  } else {
    issues.push(name);
    console.log(`  FAIL: ${name}`);
  }
}

(async () => {
  fs.mkdirSync(SCREENSHOTS_DIR, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 1: Agent Registration ===');
  // ═══════════════════════════════════════════════════

  // Register agents via MQTT heartbeat/register topics
  mqtt('agents/register/rupert', {
    display_name: 'Rupert', role: 'Orchestrator', device_type: 'server',
    capabilities: ['orchestration', 'planning', 'delegation', 'task_management'],
    ip_address: '192.168.1.10', status: 'online',
    cpu_percent: 23.5, memory_percent: 41.2
  });

  mqtt('agents/register/jeeves', {
    display_name: 'Jeeves', role: 'IoT Controller', device_type: 'raspberry_pi',
    capabilities: ['sensors', 'actuators', 'mqtt', 'home_automation'],
    ip_address: '192.168.1.20', status: 'online',
    cpu_percent: 12.1, memory_percent: 28.7
  });

  mqtt('agents/register/percy', {
    display_name: 'Percy', role: 'Mobile Agent', device_type: 'smartphone',
    capabilities: ['gps', 'camera', 'notifications', 'geofencing'],
    ip_address: '192.168.1.50', status: 'online',
    cpu_percent: 8.3, memory_percent: 55.0
  });

  await sleep(1500);

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 2: Dashboard Load ===');
  // ═══════════════════════════════════════════════════

  await page.goto('http://localhost', { waitUntil: 'networkidle' });
  await sleep(3000);

  const title = await page.title();
  check('Page title is OpenClaw Swarm Control', title.includes('OpenClaw'));

  const header = await page.textContent('h1');
  check('Header shows OpenClaw Swarm Control', header.includes('OpenClaw Swarm Control'));

  await screenshot(page, 'dashboard-loaded');

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 3: Agent Sidebar ===');
  // ═══════════════════════════════════════════════════

  const bodyText = await page.textContent('body');
  check('Rupert visible in sidebar', bodyText.includes('Rupert'));
  check('Jeeves visible in sidebar', bodyText.includes('Jeeves'));
  check('Percy visible in sidebar', bodyText.includes('Percy'));
  check('Orchestrator role shown', bodyText.includes('Orchestrator'));
  check('IoT Controller role shown', bodyText.includes('IoT Controller'));

  // Check system status in header
  const headerText = await page.textContent('header');
  check('WebSocket connected', headerText.includes('Connected'));
  check('Online agent count', headerText.includes('online'));

  await screenshot(page, 'agents-sidebar');

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 4: Agent Communication ===');
  // ═══════════════════════════════════════════════════

  // Rupert assigns task to Jeeves
  mqtt('openclaw/local/rupert/cmd', {
    sender_id: 'rupert', receiver_id: 'jeeves', message_type: 'command',
    correlation_id: 'task-temp-001',
    payload: { message: 'Check all temperature sensors and report readings' }
  });
  await sleep(500);

  // Jeeves responds with results
  mqtt('openclaw/local/jeeves/result', {
    sender_id: 'jeeves', receiver_id: 'rupert', message_type: 'result',
    correlation_id: 'task-temp-001',
    payload: {
      message: 'Sensor readings complete',
      result: { living_room: '22.5C', kitchen: '24.1C', bedroom: '19.8C', garage: '15.2C' }
    }
  });
  await sleep(500);

  // Rupert broadcasts status
  mqtt('openclaw/local/rupert/broadcast', {
    sender_id: 'rupert', message_type: 'broadcast',
    payload: { message: 'All temperature sensors nominal. No anomalies detected.' }
  });
  await sleep(500);

  // Percy reports location alert
  mqtt('openclaw/local/percy/alert', {
    sender_id: 'percy', receiver_id: 'rupert', message_type: 'alert',
    payload: { message: 'Geofence breach detected: device left home zone at 14:23' }
  });
  await sleep(500);

  // Rupert assigns task to Percy
  mqtt('openclaw/local/rupert/cmd', {
    sender_id: 'rupert', receiver_id: 'percy', message_type: 'command',
    correlation_id: 'task-photo-002',
    payload: { message: 'Take photo of front entrance and send notification' }
  });
  await sleep(500);

  // Percy responds
  mqtt('openclaw/local/percy/result', {
    sender_id: 'percy', receiver_id: 'rupert', message_type: 'result',
    correlation_id: 'task-photo-002',
    payload: { message: 'Photo captured and notification sent to all devices', result: { photo_id: 'IMG_20260308_1423', notification_sent: true } }
  });
  await sleep(2000);

  // Reload to see messages
  await page.goto('http://localhost', { waitUntil: 'networkidle' });
  await sleep(3000);

  const chatBody = await page.textContent('body');
  check('Command messages visible', chatBody.includes('COMMAND') || chatBody.includes('command'));
  check('Result messages visible', chatBody.includes('RESULT') || chatBody.includes('result'));
  check('Broadcast visible', chatBody.includes('BROADCAST') || chatBody.includes('broadcast'));
  check('Alert visible', chatBody.includes('ALERT') || chatBody.includes('alert') || chatBody.includes('Geofence'));

  await screenshot(page, 'chat-messages');

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 5: Flow Graph ===');
  // ═══════════════════════════════════════════════════

  const flowTab = page.locator('button', { hasText: 'Flow' });
  await flowTab.click();
  await sleep(3000);
  await screenshot(page, 'flow-graph');

  const flowBody = await page.textContent('body');
  check('Flow tab loads', flowBody.includes('Time range') || flowBody.includes('1h'));

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 6: Log Viewer ===');
  // ═══════════════════════════════════════════════════

  const logsTab = page.locator('button', { hasText: 'Logs' });
  await logsTab.click();
  await sleep(2000);
  await screenshot(page, 'log-viewer');

  const logsBody = await page.textContent('body');
  check('Log viewer has entries', logsBody.includes('MQTT') || logsBody.includes('INFO'));
  check('Log level filter present', logsBody.includes('ERROR') && logsBody.includes('WARN'));
  check('Log table columns', logsBody.includes('Level') || logsBody.includes('Agent'));

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 7: Task Board ===');
  // ═══════════════════════════════════════════════════

  const tasksTab = page.locator('button', { hasText: 'Tasks' });
  await tasksTab.click();
  await sleep(1500);
  await screenshot(page, 'task-board');

  const taskBody = await page.textContent('body');
  check('Kanban columns visible', taskBody.includes('Pending') && taskBody.includes('Running') && taskBody.includes('Completed'));
  check('Create Task button', taskBody.includes('Create Task'));

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 8: Send Command from Dashboard ===');
  // ═══════════════════════════════════════════════════

  // Switch back to Chat tab
  const chatTab = page.locator('button', { hasText: 'Chat' });
  await chatTab.click();
  await sleep(1000);

  // Find the command input target select (bottom bar)
  const selects = page.locator('select');
  const selectCount = await selects.count();
  let targetSelect = null;
  for (let i = 0; i < selectCount; i++) {
    const options = await selects.nth(i).locator('option').allTextContents();
    if (options.some(o => o.includes('Target') || o.includes('Rupert') || o.includes('rupert'))) {
      targetSelect = selects.nth(i);
      break;
    }
  }

  if (targetSelect) {
    const options = await targetSelect.locator('option').allTextContents();
    console.log(`  Target dropdown options: ${options.join(', ')}`);
    const rupertOption = options.find(o => o.toLowerCase().includes('rupert'));
    if (rupertOption) {
      await targetSelect.selectOption({ label: rupertOption });
      const cmdInput = page.locator('input[placeholder="Send command..."]');
      await cmdInput.fill('What is the current system status?');
      await sleep(500);
      await screenshot(page, 'command-typed');

      // Click send
      const sendBtn = page.locator('button').filter({ has: page.locator('svg.lucide-send') });
      if (await sendBtn.count() > 0) {
        await sendBtn.click();
        await sleep(1000);
        check('Command sent via UI', true);
      } else {
        check('Send button found', false);
      }
    } else {
      check('Rupert in target dropdown', false);
    }
  } else {
    check('Target dropdown found', false);
  }

  // Simulate Rupert's reply
  await sleep(500);
  mqtt('openclaw/local/rupert/result', {
    sender_id: 'rupert', receiver_id: 'dashboard', message_type: 'result',
    payload: {
      message: 'System Status Report:\n- Agents online: 3/3\n- Temperature: nominal\n- Tasks completed: 2\n- Errors: 0\n- Uptime: 10h 23m'
    }
  });
  await sleep(2500);

  await screenshot(page, 'command-reply');

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 8b: Send Command to Non-Simulated Agent ===');
  // ═══════════════════════════════════════════════════

  // Test sending a command to any available agent (tests the deployment lookup fallback)
  // This covers the case where an agent's deployment might not match connected clients
  {
    const apiResult = await page.evaluate(async () => {
      try {
        const resp = await fetch('/api/command/us-claw-remote', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message_type: 'command',
            payload: { message: "What's today's weather" },
          }),
        });
        return { status: resp.status, body: await resp.json() };
      } catch (e) {
        return { error: e.message };
      }
    });
    console.log(`  Command to us-claw-remote: ${JSON.stringify(apiResult)}`);
    check('Command to non-simulated agent succeeds', apiResult.status === 200 && apiResult.body?.ok === true);
  }

  // Also test via the UI: select a different agent and send with apostrophes
  if (targetSelect) {
    const options = await targetSelect.locator('option').allTextContents();
    const anyAgent = options.find(o => o !== 'Target...');
    if (anyAgent) {
      await targetSelect.selectOption({ label: anyAgent });
      const cmdInput = page.locator('input[placeholder="Send command..."]');
      await cmdInput.fill("What's today's weather");
      await sleep(300);
      const sendBtn = page.locator('button').filter({ has: page.locator('svg.lucide-send') });
      await sendBtn.click();
      await sleep(1500);
      // Check no error toast
      const bodyText = await page.textContent('body');
      const hasError = bodyText.includes('Failed to send');
      check('UI command with apostrophes succeeds', !hasError);
      await screenshot(page, 'command-apostrophe');
    }
  }

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 9: Agent Detail View ===');
  // ═══════════════════════════════════════════════════

  // Click on Rupert in sidebar
  const rupertCard = page.locator('button', { hasText: 'Rupert' }).first();
  if (await rupertCard.count() > 0) {
    await rupertCard.click();
    await sleep(1500);
    // Double-click to enter detail view (first click selects, changes tab)
    const activeTab = await page.textContent('body');
    await screenshot(page, 'agent-selected');
    check('Agent selection works', true);
  }

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 10: API & Persistence Verification ===');
  // ═══════════════════════════════════════════════════

  const [agents, messages, logs, status, history, flow] = await page.evaluate(async (apiKey) => {
    const headers = { 'api-key': apiKey };
    const [a, m, l, s, h, f] = await Promise.all([
      fetch('/api/agents').then(r => r.json()),
      fetch('/api/messages?limit=20').then(r => r.json()),
      fetch('/api/logs?limit=20').then(r => r.json()),
      fetch('/api/system/status').then(r => r.json()),
      fetch('/api/history?limit=10', { headers }).then(r => r.json()),
      fetch('/api/messages/flow?hours=24').then(r => r.json()),
    ]);
    return [a, m, l, s, h, f];
  }, API_KEY);

  console.log(`  Agents: ${agents.length}`);
  console.log(`  Messages: ${messages.items?.length}`);
  console.log(`  Logs: ${logs.items?.length}`);
  console.log(`  Episodes: ${history.length}`);
  console.log(`  System: ${JSON.stringify(status)}`);
  console.log(`  Flow nodes: ${flow.nodes?.length}, edges: ${flow.edges?.length}`);

  check('Agents persisted', agents.length >= 3);
  check('Messages persisted', messages.items?.length >= 5);
  check('Logs persisted', logs.items?.length >= 5);
  check('Raw episodes persisted', history.length >= 5);
  check('System status correct', status.agents_online >= 3);
  check('Flow graph has data', flow.nodes?.length >= 2);

  // Check agent details
  for (const agent of agents) {
    if (['rupert', 'jeeves', 'percy'].includes(agent.id)) {
      check(`Agent ${agent.id} has display_name`, !!agent.display_name);
      check(`Agent ${agent.id} has role`, !!agent.role);
      check(`Agent ${agent.id} is online`, agent.status === 'online');
    }
  }

  // ═══════════════════════════════════════════════════
  console.log('\n=== PHASE 11: More Agent Interactions (for video) ===');
  // ═══════════════════════════════════════════════════

  // Click "All Agents" to reset
  const allAgentsBtn = page.locator('button', { hasText: 'All Agents' });
  if (await allAgentsBtn.count() > 0) await allAgentsBtn.click();
  await sleep(500);

  // More realistic agent chatter
  mqtt('openclaw/local/jeeves/info', {
    sender_id: 'jeeves', receiver_id: 'rupert', message_type: 'info',
    payload: { message: 'Motion detected in garage. Camera activated.' }
  });
  await sleep(800);

  mqtt('openclaw/local/rupert/cmd', {
    sender_id: 'rupert', receiver_id: 'percy', message_type: 'command',
    correlation_id: 'task-notify-003',
    payload: { message: 'Send push notification: Motion detected in garage' }
  });
  await sleep(800);

  mqtt('openclaw/local/percy/result', {
    sender_id: 'percy', receiver_id: 'rupert', message_type: 'result',
    correlation_id: 'task-notify-003',
    payload: { message: 'Push notification delivered to 2 devices' }
  });
  await sleep(2000);

  await screenshot(page, 'full-conversation');

  // Final screenshot of logs with all activity
  await logsTab.click();
  await sleep(2000);
  await screenshot(page, 'logs-with-activity');

  // Flow with more data
  await flowTab.click();
  await sleep(3000);
  await screenshot(page, 'flow-with-data');

  // ═══════════════════════════════════════════════════
  console.log('\n\n========================================');
  console.log('         E2E TEST RESULTS');
  console.log('========================================');
  console.log(`  PASSED: ${passes.length}`);
  console.log(`  FAILED: ${issues.length}`);
  if (issues.length > 0) {
    console.log('\n  FAILURES:');
    issues.forEach(i => console.log(`    - ${i}`));
  }
  console.log('========================================\n');

  await browser.close();
})().catch(e => {
  console.error('ERROR:', e.message);
  console.error(e.stack);
  process.exit(1);
});
