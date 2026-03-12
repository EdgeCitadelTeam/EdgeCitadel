#!/usr/bin/env node
const mqtt = require('mqtt');
const { execFile } = require('child_process');
const os = require('os');

const ID      = process.env.AGENT_ID;
if (!ID) { console.error('AGENT_ID required'); process.exit(1); }

const DISPLAY = process.env.AGENT_DISPLAY   || ID.replace(/-/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
const ROLE    = process.env.AGENT_ROLE       || 'Agent';
const DEVICE  = process.env.AGENT_DEVICE_TYPE|| 'server';
const HOST    = process.env.CITADEL_HOST     || process.env.MQTT_HOST || '127.0.0.1';
const PORT    = parseInt(process.env.CITADEL_PORT || process.env.MQTT_PORT || '1883');
const NATS_TOKEN = process.env.NATS_TOKEN    || process.env.MQTT_PASS || '';
const HB_SEC  = parseInt(process.env.HEARTBEAT_SEC || '30');
const OPENCLAW_BIN = process.env.OPENCLAW_BIN || 'openclaw';
const AGENT_TIMEOUT = parseInt(process.env.AGENT_TIMEOUT || '600');
const T       = `[${ID}]`;

function getIP() {
    for (const ifs of Object.values(os.networkInterfaces()))
        for (const i of ifs) if (i.family==='IPv4' && !i.internal) return i.address;
    return '127.0.0.1';
}

let client = null;
let hbTimer = null;

function heartbeat() {
    if (!client) return;
    const la = os.loadavg()[0], cpus = os.cpus().length||1;
    const tm = os.totalmem(), fm = os.freemem();
    client.publish(`agents/${ID}/heartbeat`, JSON.stringify({
        agent_id:ID, sender_id:ID, display_name:DISPLAY, role:ROLE, device_type:DEVICE,
        status:'online',
        cpu_percent: Math.round((la/cpus)*1000)/10,
        memory_percent: Math.round(((tm-fm)/tm)*1000)/10,
        ip_address: getIP(),
        capabilities: ['chat','task_execution','mqtt_listener'],
        timestamp: new Date().toISOString()
    }), {qos: 0});
}

function start() {
    console.log(T, `Connecting to EdgeCitadel broker at ${HOST}:${PORT}...`);

    const opts = {
        reconnectPeriod: 5000,
        connectTimeout: 10000,
        username: 'mqtt-agent',
        password: NATS_TOKEN,
    };

    client = mqtt.connect(`mqtt://${HOST}:${PORT}`, opts);

    client.on('connect', () => {
        console.log(T, `Connected to ${HOST}:${PORT}`);

        // Register (retained so new subscribers see it)
        client.publish(`agents/${ID}/register`, JSON.stringify({
            agent_id:ID, sender_id:ID, display_name:DISPLAY, role:ROLE, device_type:DEVICE,
            capabilities:['chat','task_execution','mqtt_listener'],
            ip_address:getIP(), status:'online', timestamp:new Date().toISOString()
        }), {retain: true, qos: 1});

        heartbeat();
        hbTimer = setInterval(heartbeat, HB_SEC*1000);

        // Subscribe to inbox
        client.subscribe(`agents/${ID}/inbox`, {qos: 1});
        // Subscribe to broadcasts
        client.subscribe('system/broadcast', {qos: 0});

        console.log(T, 'Online. Listening for commands.');
    });

    client.on('message', (topic, payload) => {
        try {
            const m = JSON.parse(payload.toString());

            if (topic === `agents/${ID}/inbox`) {
                const from = m.from||m.sender_id||'unknown';
                const inner = (typeof m.payload === 'object' && m.payload) ? m.payload : {};
                const content = m.content||m.message||inner.message||inner.content||inner.command||'';
                const corrId = m.correlationId||m.correlation_id||'';
                const msgType = m.type||m.message_type||'';
                // Skip own messages and responses (prevent reply loops)
                if (from===ID || !content.trim()) return;
                if (msgType==='response' || msgType==='result') return;
                console.log(T, `${from}: ${content.substring(0,120)}`);
                callAgent(from, content, corrId);
            } else if (topic === 'system/broadcast') {
                const from = m.from||m.sender_id||'unknown';
                const content = m.content||m.message||'';
                if (from===ID || !content.trim()) return;
                console.log(T, `[broadcast] ${from}: ${content.substring(0,120)}`);
            }
        } catch(e) { console.error(T, 'Parse:', e.message); }
    });

    client.on('error', (err) => {
        console.error(T, 'Broker error:', err.message);
    });

    client.on('close', () => {
        console.log(T, 'Broker connection closed');
    });

    client.on('reconnect', () => {
        console.log(T, 'Reconnecting to broker...');
    });
}

function callAgent(from, content, corrId) {
    const prompt = `[MQTT from ${from}] ${content}`;
    const sessionId = `mqtt-${from}-${ID}`;
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
            reply(from, `[Error: agent failed to process message — ${err.message}]`, corrId);
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
                reply(from, responseText, corrId);
            } else {
                console.log(T, 'Agent returned empty response');
            }
        } catch(e) {
            const text = stdout.trim();
            if (text) {
                reply(from, text, corrId);
            } else {
                console.error(T, 'Failed to parse agent output:', e.message);
            }
        }
    });
}

function reply(to, content, corrId) {
    if (!client) return;
    const msg = {
        from:ID, to, sender_id:ID, receiver_id:to,
        type:'response', message_type:'result',
        content, message:content,
        correlationId:corrId||'', correlation_id:corrId||'',
        timestamp:new Date().toISOString()
    };
    const encoded = JSON.stringify(msg);

    // 1. Publish on own outbox (aggregator picks this up for chat history)
    client.publish(`agents/${ID}/outbox`, encoded, {qos: 1});

    // 2. Deliver reply to sender's inbox so they actually receive it
    if (to && to !== ID && to !== 'dashboard' && to !== 'system') {
        client.publish(`agents/${to}/inbox`, encoded, {qos: 1});
        console.log(T, `-> ${to} (inbox + outbox): ${content.substring(0,120)}`);
    } else {
        console.log(T, `-> ${to} (outbox): ${content.substring(0,120)}`);
    }
}

function shutdown() {
    if(hbTimer) clearInterval(hbTimer);
    if (client) {
        client.publish(`agents/${ID}/status`, JSON.stringify({
            sender_id:ID, status:'offline', timestamp:new Date().toISOString()
        }), {qos: 1}, () => {
            client.end(false, {}, () => {
                process.exit(0);
            });
        });
        // Force exit after 5s if graceful close hangs
        setTimeout(() => process.exit(0), 5000);
    } else {
        process.exit(0);
    }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(T, `Listener starting | ${DISPLAY} (${ROLE}) | ${HOST}:${PORT} | openclaw agent CLI mode`);
start();
