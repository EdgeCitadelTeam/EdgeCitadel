#!/usr/bin/env node
const { connect, StringCodec } = require('nats');
const { execFile } = require('child_process');
const os = require('os');

const sc = StringCodec();

const ID      = process.env.AGENT_ID;
if (!ID) { console.error('AGENT_ID required'); process.exit(1); }

const DISPLAY = process.env.AGENT_DISPLAY   || ID.replace(/-/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
const ROLE    = process.env.AGENT_ROLE       || 'Agent';
const DEVICE  = process.env.AGENT_DEVICE_TYPE|| 'server';
const HOST    = process.env.CITADEL_HOST     || process.env.NATS_HOST || '127.0.0.1';
const PORT    = parseInt(process.env.CITADEL_PORT || process.env.NATS_PORT || '4222');
const NATS_TK = process.env.NATS_TOKEN       || '';
const HB_SEC  = parseInt(process.env.HEARTBEAT_SEC || '30');
const OPENCLAW_BIN = process.env.OPENCLAW_BIN || 'openclaw';
const AGENT_TIMEOUT = parseInt(process.env.AGENT_TIMEOUT || '600');
const T       = `[${ID}]`;

function getIP() {
    for (const ifs of Object.values(os.networkInterfaces()))
        for (const i of ifs) if (i.family==='IPv4' && !i.internal) return i.address;
    return '127.0.0.1';
}

let nc = null;
let hbTimer = null;

function heartbeat() {
    if (!nc) return;
    const la = os.loadavg()[0], cpus = os.cpus().length||1;
    const tm = os.totalmem(), fm = os.freemem();
    nc.publish(`agents.${ID}.heartbeat`, sc.encode(JSON.stringify({
        agent_id:ID, sender_id:ID, display_name:DISPLAY, role:ROLE, device_type:DEVICE,
        status:'online',
        cpu_percent: Math.round((la/cpus)*1000)/10,
        memory_percent: Math.round(((tm-fm)/tm)*1000)/10,
        ip_address: getIP(),
        capabilities: ['chat','task_execution','nats_listener'],
        timestamp: new Date().toISOString()
    })));
}

async function start() {
    console.log(T, `Connecting to NATS at ${HOST}:${PORT}...`);

    const opts = {
        servers: `${HOST}:${PORT}`,
        reconnect: true,
        maxReconnectAttempts: -1,
        reconnectTimeWait: 5000,
    };
    if (NATS_TK) opts.token = NATS_TK;
    nc = await connect(opts);

    console.log(T, `Connected to ${HOST}:${PORT}`);

    // Register
    nc.publish(`agents.${ID}.register`, sc.encode(JSON.stringify({
        agent_id:ID, sender_id:ID, display_name:DISPLAY, role:ROLE, device_type:DEVICE,
        capabilities:['chat','task_execution','nats_listener'],
        ip_address:getIP(), status:'online', timestamp:new Date().toISOString()
    })));

    heartbeat();
    hbTimer = setInterval(heartbeat, HB_SEC*1000);

    // Subscribe to inbox
    const inboxSub = nc.subscribe(`agents.${ID}.inbox`);
    // Subscribe to broadcasts
    const broadcastSub = nc.subscribe('system.broadcast');

    console.log(T, 'Online. Listening for commands.');

    // Handle inbox messages
    (async () => {
        for await (const msg of inboxSub) {
            try {
                const m = JSON.parse(sc.decode(msg.data));
                const from = m.from||m.sender_id||'unknown';
                const content = m.content||m.message||'';
                const corrId = m.correlationId||m.correlation_id||'';
                const msgType = m.type||m.message_type||'';
                // Skip own messages and responses (prevent reply loops)
                if (from===ID || !content.trim()) continue;
                if (msgType==='response' || msgType==='result') continue;
                console.log(T, `${from}: ${content.substring(0,120)}`);
                callAgent(from, content, corrId, msg.reply || null);
            } catch(e) { console.error(T, 'Parse:', e.message); }
        }
    })();

    // Handle broadcast messages
    (async () => {
        for await (const msg of broadcastSub) {
            try {
                const m = JSON.parse(sc.decode(msg.data));
                const from = m.from||m.sender_id||'unknown';
                const content = m.content||m.message||'';
                if (from===ID || !content.trim()) continue;
                console.log(T, `[broadcast] ${from}: ${content.substring(0,120)}`);
            } catch(e) { /* ignore */ }
        }
    })();

    // Wait for close
    await nc.closed();
    console.log(T, 'NATS connection closed');
}

function callAgent(from, content, corrId, natsReplySubject) {
    const prompt = `[NATS from ${from}] ${content}`;
    const sessionId = `nats-${from}-${ID}`;
    const args = [
        'agent',
        '-m', prompt,
        '--session-id', sessionId,
        '--json',
        '--timeout', String(AGENT_TIMEOUT),
    ];

    console.log(T, `Calling openclaw agent (session: ${sessionId})...`);

    execFile(OPENCLAW_BIN, args, {
        timeout: (AGENT_TIMEOUT + 30) * 1000,
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env, NO_COLOR: '1' },
    }, (err, stdout, stderr) => {
        if (stderr) {
            for (const line of stderr.split('\n').filter(l => l.trim())) {
                console.log(T, `[openclaw] ${line}`);
            }
        }
        if (err) {
            console.error(T, `Agent error: ${err.message}`);
            reply(from, `[Error: agent failed to process message — ${err.message}]`, corrId, natsReplySubject);
            return;
        }

        try {
            const result = JSON.parse(stdout);
            const inner = result.result || result;
            const texts = (inner.payloads || [])
                .map(p => p.text)
                .filter(t => t && t.trim());
            const responseText = texts.join('\n\n');
            if (responseText.trim()) {
                reply(from, responseText, corrId, natsReplySubject);
            } else {
                console.log(T, 'Agent returned empty response');
            }
        } catch(e) {
            const text = stdout.trim();
            if (text) {
                reply(from, text, corrId, natsReplySubject);
            } else {
                console.error(T, 'Failed to parse agent output:', e.message);
            }
        }
    });
}

function reply(to, content, corrId, natsReplySubject) {
    if (!nc) return;
    const msg = {
        from:ID, to, sender_id:ID, receiver_id:to,
        type:'response', message_type:'result',
        content, message:content,
        correlationId:corrId||'', correlation_id:corrId||'',
        timestamp:new Date().toISOString()
    };
    const encoded = sc.encode(JSON.stringify(msg));

    // 1. Publish on own outbox (aggregator picks this up for chat history)
    nc.publish(`agents.${ID}.outbox`, encoded);

    // 2. Deliver reply to sender's inbox so they actually receive it
    if (to && to !== ID && to !== 'dashboard' && to !== 'system') {
        nc.publish(`agents.${to}.inbox`, encoded);
        console.log(T, `-> ${to} (inbox + outbox): ${content.substring(0,120)}`);
    } else {
        console.log(T, `-> ${to} (outbox): ${content.substring(0,120)}`);
    }

    // 3. Support NATS request-reply pattern (if sender used nc.request())
    if (natsReplySubject) {
        nc.publish(natsReplySubject, encoded);
        console.log(T, `-> ${to} (request-reply): responded`);
    }
}

async function shutdown() {
    if(hbTimer) clearInterval(hbTimer);
    if (nc) {
        nc.publish(`agents.${ID}.status`, sc.encode(JSON.stringify({
            sender_id:ID, status:'offline', timestamp:new Date().toISOString()
        })));
        await nc.flush();
        await nc.close();
    }
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(T, `Listener starting | ${DISPLAY} (${ROLE}) | ${HOST}:${PORT} | openclaw agent CLI mode`);
start().catch(e => { console.error(T, 'Fatal:', e.message); process.exit(1); });
