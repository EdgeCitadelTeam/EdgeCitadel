const { chromium } = require('playwright');
const { execSync } = require('child_process');
const path = require('path');

const DOCS_DIR = path.join(__dirname, '..', 'docs');
const MQTT_OPTS = '-h localhost -u iot_agent -P openclaw_secret';

function mqtt(topic, payload) {
  const msg = typeof payload === 'string' ? payload : JSON.stringify(payload);
  execSync(
    `docker compose exec -T mqtt mosquitto_pub ${MQTT_OPTS} -t "${topic}" -m '${msg.replace(/'/g, "'\\''")}'`,
    { cwd: path.join(__dirname, '..') }
  );
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

(async () => {
  // Register agents first
  console.log('Registering agents...');
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
  await sleep(1000);

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    recordVideo: { dir: DOCS_DIR, size: { width: 1440, height: 900 } }
  });
  const page = await context.newPage();

  // ── Load dashboard and wait for WS ──
  console.log('Loading dashboard...');
  await page.goto('http://localhost', { waitUntil: 'networkidle' });
  await sleep(3000);

  // ── SCENE 1: Multi-agent conversation streams in ──
  console.log('Scene 1: Agent conversation streaming in...');

  // Rupert starts a task - asks Jeeves to check sensors
  mqtt('openclaw/local/rupert/cmd', {
    sender_id: 'rupert', receiver_id: 'jeeves', message_type: 'command',
    correlation_id: 'task-sensor-001',
    payload: { message: 'Run full sensor diagnostic on all rooms' }
  });
  await sleep(2000);

  // Jeeves acknowledges
  mqtt('openclaw/local/jeeves/result', {
    sender_id: 'jeeves', receiver_id: 'rupert', message_type: 'info',
    correlation_id: 'task-sensor-001',
    payload: { message: 'Starting sensor sweep across 4 rooms...' }
  });
  await sleep(2000);

  // Jeeves reports result
  mqtt('openclaw/local/jeeves/result', {
    sender_id: 'jeeves', receiver_id: 'rupert', message_type: 'result',
    correlation_id: 'task-sensor-001',
    payload: {
      message: 'Sensor diagnostic complete',
      result: { living_room: '22.5C / 45%', kitchen: '24.1C / 52%', bedroom: '19.8C / 40%', garage: '15.2C / 38%' }
    }
  });
  await sleep(2000);

  // Rupert delegates to Percy
  mqtt('openclaw/local/rupert/cmd', {
    sender_id: 'rupert', receiver_id: 'percy', message_type: 'command',
    correlation_id: 'task-notify-002',
    payload: { message: 'Send daily summary to all mobile devices' }
  });
  await sleep(2000);

  // Percy responds
  mqtt('openclaw/local/percy/result', {
    sender_id: 'percy', receiver_id: 'rupert', message_type: 'result',
    correlation_id: 'task-notify-002',
    payload: { message: 'Daily summary pushed to 3 devices successfully' }
  });
  await sleep(2000);

  // Jeeves detects anomaly
  mqtt('openclaw/local/jeeves/alert', {
    sender_id: 'jeeves', receiver_id: 'rupert', message_type: 'alert',
    payload: { message: 'Motion detected in garage - camera activated' }
  });
  await sleep(2000);

  // Rupert broadcasts to all
  mqtt('openclaw/local/rupert/broadcast', {
    sender_id: 'rupert', message_type: 'broadcast',
    payload: { message: 'Security alert acknowledged. Monitoring garage camera feed.' }
  });
  await sleep(2000);

  // Percy reports geofence
  mqtt('openclaw/local/percy/info', {
    sender_id: 'percy', receiver_id: 'rupert', message_type: 'info',
    payload: { message: 'All registered devices within home geofence. No unauthorized movement.' }
  });
  await sleep(3000);

  // ── SCENE 2: Send command from dashboard ──
  console.log('Scene 2: Send command from dashboard...');

  // Select Rupert in the target dropdown
  const selects = page.locator('select');
  const selectCount = await selects.count();
  for (let i = 0; i < selectCount; i++) {
    const options = await selects.nth(i).locator('option').allTextContents();
    if (options.some(o => o.includes('Rupert') || o.includes('rupert'))) {
      await selects.nth(i).selectOption({ label: options.find(o => o.toLowerCase().includes('rupert')) });
      break;
    }
  }

  // Type command
  const cmdInput = page.locator('input[placeholder="Send command..."]');
  if (await cmdInput.count() > 0) {
    await cmdInput.click();
    // Type slowly for the video
    for (const char of 'What is the current system status?') {
      await cmdInput.press(char === ' ' ? 'Space' : char);
      await sleep(50);
    }
    await sleep(1000);

    // Click send
    const sendBtn = page.locator('button').filter({ has: page.locator('svg.lucide-send') });
    if (await sendBtn.count() > 0) {
      await sendBtn.click();
      await sleep(1500);
    }
  }

  // Simulate Rupert's reply via MQTT
  mqtt('openclaw/local/rupert/result', {
    sender_id: 'rupert', receiver_id: 'dashboard', message_type: 'result',
    payload: {
      message: 'System Status Report:\n- Agents online: 3/3 (Rupert, Jeeves, Percy)\n- Temperature: all nominal\n- Tasks completed today: 5\n- Errors: 0\n- Uptime: 12h 45m'
    }
  });
  await sleep(3000);

  // ── SCENE 3: Browse other tabs ──
  console.log('Scene 3: Flow graph...');
  const flowTab = page.locator('button', { hasText: 'Flow' });
  await flowTab.click();
  await sleep(4000);

  console.log('Scene 4: Log viewer...');
  const logsTab = page.locator('button', { hasText: 'Logs' });
  await logsTab.click();
  await sleep(3000);

  console.log('Scene 5: Task board...');
  const tasksTab = page.locator('button', { hasText: 'Tasks' });
  await tasksTab.click();
  await sleep(3000);

  // Back to chat to show full conversation
  console.log('Scene 6: Back to chat...');
  const chatTab = page.locator('button', { hasText: 'Chat' });
  await chatTab.click();
  await sleep(3000);

  // ── SCENE 4: Click agent in sidebar ──
  console.log('Scene 7: Agent detail...');
  const rupertBtn = page.locator('button', { hasText: 'Rupert' }).first();
  if (await rupertBtn.count() > 0) {
    await rupertBtn.click();
    await sleep(3000);
  }

  // Final pause
  await sleep(2000);

  console.log('Closing browser...');
  await page.close();
  await context.close();
  await browser.close();

  // Find the recorded video and move it
  const { readdirSync, renameSync, existsSync } = require('fs');
  const files = readdirSync(DOCS_DIR).filter(f => f.endsWith('.webm'));
  if (files.length > 0) {
    const src = path.join(DOCS_DIR, files[files.length - 1]);
    const dest = path.join(DOCS_DIR, 'demo-raw.webm');
    renameSync(src, dest);
    console.log(`Video saved: ${dest}`);
    console.log('Converting to mp4...');
    try {
      execSync(`ffmpeg -y -i "${dest}" -c:v libx264 -pix_fmt yuv420p -r 30 "${path.join(DOCS_DIR, 'demo.mp4')}"`, { stdio: 'inherit' });
      console.log('MP4 created: docs/demo.mp4');
    } catch (e) {
      console.log('ffmpeg conversion failed, webm still available');
    }
    // Generate GIF
    try {
      execSync(`ffmpeg -y -i "${dest}" -vf "fps=10,scale=720:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=128[p];[s1][p]paletteuse=dither=bayer:bayer_scale=3" "${path.join(DOCS_DIR, 'demo.gif')}"`, { stdio: 'inherit' });
      console.log('GIF created: docs/demo.gif');
    } catch (e) {
      console.log('GIF generation failed');
    }
  } else {
    console.log('No video file found');
  }
})().catch(e => {
  console.error('ERROR:', e.message);
  process.exit(1);
});
