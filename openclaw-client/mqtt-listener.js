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
const USER    = process.env.MQTT_USER        || ID;
const PASS    = process.env.MQTT_PASS        || '';
const HB_SEC  = parseInt(process.env.HEARTBEAT_SEC || '30');
const OPENCLAW_BIN = process.env.OPENCLAW_BIN || 'openclaw';
const AGENT_TIMEOUT = parseInt(process.env.AGENT_TIMEOUT || '600');
const T       = `[${ID}]`;

function getIP() {
    for (const ifs of Object.values(os.networkInterfaces()))
        for (const i of ifs) if (i.family==='IPv4' && !i.internal) return i.address;
    return '127.0.0.1';
}

const client = mqtt.connect(`mqtt://${HOST}:${PORT}`, {
    username: USER, password: PASS,
    clientId: `${ID}-${Date.now()}`,
    reconnectPeriod: 5000,
    will: { topic: `agents/status/${ID}`,
            payload: JSON.stringify({status:'offline',timestamp:new Date().toISOString()}),
            qos:1, retain:true }
});

let hbTimer = null;

function heartbeat() {
    const la = os.loadavg()[0], cpus = os.cpus().length||1;
    const tm = os.totalmem(), fm = os.freemem();
    client.publish(`agents/heartbeat/${ID}`, JSON.stringify({
        agent_id:ID, display_name:DISPLAY, role:ROLE, device_type:DEVICE,
        status:'online',
        cpu_percent: Math.round((la/cpus)*1000)/10,
        memory_percent: Math.round(((tm-fm)/tm)*1000)/10,
        ip_address: getIP(),
        capabilities: ['chat','task_execution','mqtt_listener'],
        timestamp: new Date().toISOString()
    }), {qos:1});
}

client.on('connect', () => {
    console.log(T, `Connected to ${HOST}:${PORT}`);
    client.subscribe(`agents/inbox/${ID}`, {qos:1});
    client.subscribe('agents/broadcast', {qos:1});
    // Register
    client.publish(`agents/register/${ID}`, JSON.stringify({
        agent_id:ID, display_name:DISPLAY, role:ROLE, device_type:DEVICE,
        capabilities:['chat','task_execution','mqtt_listener'],
        ip_address:getIP(), status:'online', timestamp:new Date().toISOString()
    }), {qos:1});
    heartbeat();
    if (hbTimer) clearInterval(hbTimer);
    hbTimer = setInterval(heartbeat, HB_SEC*1000);
    console.log(T, 'Online. Listening for commands.');
});

client.on('message', (topic, message) => {
    try {
        const m = JSON.parse(message.toString());
        const from = m.from||m.sender_id||'unknown';
        const content = m.content||m.message||'';
        const corrId = m.correlationId||m.correlation_id||'';
        if (from===ID || !content.trim()) return;
        console.log(T, `${from}: ${content.substring(0,120)}`);
        callAgent(from, content, corrId);
    } catch(e) { console.error(T, 'Parse:', e.message); }
});

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
            // openclaw prints info/warnings to stderr, log them
            for (const line of stderr.split('\n').filter(l => l.trim())) {
                console.log(T, `[openclaw] ${line}`);
            }
        }
        if (err) {
            console.error(T, `Agent error: ${err.message}`);
            // If the agent failed entirely, send an error reply
            reply(from, `[Error: agent failed to process message — ${err.message}]`, corrId);
            return;
        }

        try {
            const result = JSON.parse(stdout);
            // openclaw agent --json returns { payloads: [{ text, mediaUrl }], meta: {...} }
            const texts = (result.payloads || [])
                .map(p => p.text)
                .filter(t => t && t.trim());
            const responseText = texts.join('\n\n');
            if (responseText.trim()) {
                reply(from, responseText, corrId);
            } else {
                console.log(T, 'Agent returned empty response');
            }
        } catch(e) {
            // stdout might not be valid JSON (e.g. if --json isn't supported or mixed output)
            // Try to extract useful text from stdout
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
    const msg = {
        from:ID, to, sender_id:ID, receiver_id:to,
        type:'response', message_type:'result',
        content, message:content,
        correlationId:corrId||'', correlation_id:corrId||'',
        timestamp:new Date().toISOString()
    };
    client.publish(`agents/inbox/${to}`, JSON.stringify(msg), {qos:1});
    client.publish('agents/mirror', JSON.stringify(msg), {qos:1});
    console.log(T, `-> ${to}: ${content.substring(0,80)}`);
}

client.on('error', e=>console.error(T,'MQTT:',e.message));
client.on('offline', ()=>{ if(hbTimer){clearInterval(hbTimer);hbTimer=null;} console.log(T,'Offline, reconnecting...'); });

function shutdown() {
    if(hbTimer) clearInterval(hbTimer);
    client.publish(`agents/status/${ID}`,JSON.stringify({status:'offline',timestamp:new Date().toISOString()}),
        {qos:1,retain:true}, ()=>{client.end();process.exit(0);});
    setTimeout(()=>process.exit(0),2000);
}
process.on('SIGINT',shutdown); process.on('SIGTERM',shutdown);
console.log(T, `Listener starting | ${DISPLAY} (${ROLE}) | ${HOST}:${PORT} | openclaw agent CLI mode`);
