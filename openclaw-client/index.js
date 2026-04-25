/**
 * openclaw-client v0.1
 *
 * Connects to NATS using the account-scoped OPENCLAW_TOKEN.
 * Publishes register + heartbeats on plain NATS.
 * Forwards commands to the aggregator HTTP endpoint (not direct JetStream
 * publish) — see ADR-0005.
 */
import 'dotenv/config';
import { connect } from '@nats-io/transport-node';
import { v4 as uuid } from 'uuid';
import {
  buildRegisterEnvelope, buildHeartbeatEnvelope, buildStatusEnvelope,
  validateEnvelope, nowIso
} from './src/nats-session.js';

const {
  NATS_URL = 'nats://localhost:4222',
  OPENCLAW_TOKEN,
  OPENCLAW_SESSION_ID = `sess-${uuid().slice(0, 8)}`,
  OPENCLAW_AGENT_ID = `openclaw-${OPENCLAW_SESSION_ID}`,
  HEARTBEAT_INTERVAL_SEC = '30'
} = process.env;

if (!OPENCLAW_TOKEN) {
  console.error('OPENCLAW_TOKEN is required (not NATS_TOKEN; see ADR-0005).');
  process.exit(1);
}

async function main() {
  const nc = await connect({ servers: NATS_URL, token: OPENCLAW_TOKEN });
  console.log(`[openclaw] connected as ${OPENCLAW_AGENT_ID}`);

  const enc = data => new TextEncoder().encode(JSON.stringify(data));

  const reg = buildRegisterEnvelope({
    agentId: OPENCLAW_AGENT_ID,
    sessionId: OPENCLAW_SESSION_ID,
    heartbeatIntervalSec: Number(HEARTBEAT_INTERVAL_SEC)
  });
  const v = validateEnvelope(reg);
  if (!v.ok) { console.error('register invalid:', v.error); process.exit(2); }

  await nc.publish(`agents.${OPENCLAW_AGENT_ID}.register`, enc(reg));

  const hbInterval = setInterval(() => {
    const hb = buildHeartbeatEnvelope(OPENCLAW_AGENT_ID);
    nc.publish(`agents.${OPENCLAW_AGENT_ID}.heartbeat`, enc(hb));
  }, Number(HEARTBEAT_INTERVAL_SEC) * 1000);

  const shutdown = async () => {
    clearInterval(hbInterval);
    const off = buildStatusEnvelope(OPENCLAW_AGENT_ID, 'offline', 'shutdown');
    await nc.publish(`agents.${OPENCLAW_AGENT_ID}.status`, enc(off));
    await nc.drain();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // subscribe to own results (plain NATS — mirrored from aggregator)
  const sub = nc.subscribe(`openclaw.${OPENCLAW_SESSION_ID}.results.*`);
  (async () => {
    for await (const m of sub) {
      try {
        const env = JSON.parse(new TextDecoder().decode(m.data));
        console.log('[openclaw] result:', env.task_id, env.task_state,
                    env.payload?.body?.slice?.(0, 120));
      } catch (e) {
        console.warn('[openclaw] non-JSON result:', e.message);
      }
    }
  })();
}

main().catch(err => { console.error(err); process.exit(3); });
