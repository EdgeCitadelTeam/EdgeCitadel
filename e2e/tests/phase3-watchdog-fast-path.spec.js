const { test, expect } = require('@playwright/test');
const { spawn } = require('node:child_process');
const path = require('node:path');

const API = process.env.AGG_URL || 'http://localhost';
const POLL_INTERVAL_MS = 1000;
const POLL_BUDGET_S = 90;

// tester-1 fixture process
let testerProc = null;

function spawnTester() {
  // Minimal Python one-liner that registers tester-1 with a 10s heartbeat,
  // sends one heartbeat, then exits. The watchdog should flag offline
  // within 25s + 5s tolerance + 5s loop cadence ≈ 35s.
  const script = `
import asyncio, json, os, uuid
from datetime import datetime, timezone
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect(servers=[os.environ['NATS_URL']],
                     token=os.environ.get('NATS_TOKEN'))
    now = lambda: datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
    card = {'name': 'tester-1', 'description': 'phase3 e2e fixture',
            'version': '0', 'url': 'u', 'provider': {'organization': 'x'},
            'capabilities': {}, 'securitySchemes': {},
            'metadata': {'runtime.kind': 'native', 'runtime.roles': ['worker'],
                         'runtime.heartbeat_interval_sec': 10,
                         'runtime.deployment': 'test'}}
    reg = {'v': 1, 'id': str(uuid.uuid4()), 'type': 'register',
           'sender_id': 'tester-1', 'timestamp': now(), 'payload': card}
    await nc.publish('agents.tester-1.register', json.dumps(reg).encode())
    hb = {'v': 1, 'id': str(uuid.uuid4()), 'type': 'heartbeat',
          'sender_id': 'tester-1', 'timestamp': now(), 'payload': {}}
    await nc.publish('agents.tester-1.heartbeat', json.dumps(hb).encode())
    await nc.flush(); await nc.close()

asyncio.run(main())
`;
  return spawn('python3', ['-c', script], {
    env: { ...process.env,
           NATS_URL: process.env.NATS_URL || 'nats://localhost:4222' },
    stdio: 'inherit',
  });
}

test.describe('Phase 3 — watchdog fast-path synthesises recipient_offline', () => {
  test.beforeAll(async () => {
    // Spawn tester-1 register+heartbeat once
    testerProc = spawnTester();
    await new Promise(r => testerProc.on('exit', r));
  });

  test('tester-1 visible in /api/registry', async ({ request }) => {
    let found = false;
    for (let i = 0; i < 20; i++) {
      const r = await request.get(`${API}/api/registry?deployment=test`);
      const rows = await r.json();
      if (rows.find(x => x.agent_id === 'tester-1')) { found = true; break; }
      await new Promise(s => setTimeout(s, 500));
    }
    expect(found, 'tester-1 not in registry within 10s').toBe(true);
  });

  test('command to silent tester-1 → recipient_offline result within 90s', async ({ request }) => {
    // Send a command (sender=test-runner, auto-registered by aggregator
    // with deployment=test).
    const post = await request.post(
      `${API}/api/command/tester-1?sender_id=test-runner`,
      { data: { body: 'hello' } });
    expect(post.status()).toBe(202);
    const { task_id } = await post.json();

    // Poll for the synthesised result. Watchdog default check cadence is
    // 5s, threshold for 10s interval = 25s + 5s tolerance = 30s. Total
    // worst case ≈ 35s; we give 90s budget.
    let result;
    for (let i = 0; i < POLL_BUDGET_S; i++) {
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      const q = await request.get(
        `${API}/api/messages?task_id=${task_id}&type=result`);
      const rows = await q.json();
      if (rows.length) { result = rows[0]; break; }
    }
    expect(result, `no result within ${POLL_BUDGET_S}s`).toBeDefined();
    expect(result.task_state).toBe('failed');
    expect(result.payload.error).toBe('recipient_offline');
    expect(result.sender_id).toBe('watchdog-1');
    expect(['heartbeat_staleness', 'sticky_offline', 'max_deliveries_advisory'])
      .toContain(result.payload.trigger);
  });
});
