#!/usr/bin/env node
const mqtt = require('mqtt');
const http = require('http');
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

// P2P delegation config
const API_URL = process.env.CITADEL_API_URL || `http://${HOST}`;
const MAX_DELEGATION_DEPTH = parseInt(process.env.MAX_DELEGATION_DEPTH || '3');
const DELEGATION_TIMEOUT = parseInt(process.env.DELEGATION_TIMEOUT || '90') * 1000;
const ROSTER_REFRESH_SEC = parseInt(process.env.ROSTER_REFRESH_SEC || '60');
const MAX_CHAIN_DEPTH = MAX_DELEGATION_DEPTH + 2; // hard safety limit across agents

// State
let client = null;
let hbTimer = null;
let rosterTimer = null;
let agentRoster = {}; // { agentId: { display_name, role, status, capabilities } }
let activeDelegations = 0;
const MAX_CONCURRENT_DELEGATIONS = 5;
const _chainHashes = new Map(); // chainId -> Set<hash>

function getIP() {
    for (const ifs of Object.values(os.networkInterfaces()))
        for (const i of ifs) if (i.family==='IPv4' && !i.internal) return i.address;
    return '127.0.0.1';
}

// ── Agent Roster ──

function fetchRoster() {
    return new Promise((resolve) => {
        const url = `${API_URL}/api/agents`;
        http.get(url, { timeout: 5000 }, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                try {
                    const agents = JSON.parse(data);
                    const roster = {};
                    for (const a of agents) {
                        if (a.id && a.id !== ID && a.status === 'online') {
                            roster[a.id] = {
                                display_name: a.display_name || a.id,
                                role: a.role || '',
                                capabilities: a.capabilities || '',
                            };
                        }
                    }
                    agentRoster = roster;
                    const names = Object.keys(roster);
                    if (names.length > 0) {
                        console.log(T, `Roster: ${names.join(', ')} (${names.length} peers online)`);
                    }
                } catch(e) {
                    console.error(T, `Roster parse error: ${e.message}`);
                }
                resolve();
            });
        }).on('error', (e) => {
            console.error(T, `Roster fetch error: ${e.message}`);
            resolve();
        });
    });
}

function buildRosterString() {
    const entries = Object.entries(agentRoster);
    if (entries.length === 0) return '';
    return entries.map(([id, info]) =>
        `- ${id}: ${info.display_name} (${info.role || 'agent'})`
    ).join('\n');
}

// ── Delegation Parsing ──

const DELEGATE_RE = /\[DELEGATE:([^\]]+)\]\s*([\s\S]*?)(?=\[DELEGATE:|$)/g;

function parseDelegations(text) {
    const delegations = [];
    let match;
    DELEGATE_RE.lastIndex = 0;
    while ((match = DELEGATE_RE.exec(text)) !== null) {
        const target = match[1].trim().toLowerCase();
        const message = match[2].trim();
        if (!message) continue;
        if (target === ID) {
            console.log(T, `Skipping self-delegation`);
            continue;
        }
        if (!agentRoster[target]) {
            console.log(T, `Skipping delegation to unknown agent: ${target}`);
            continue;
        }
        delegations.push({ target, message });
    }
    return delegations;
}

function stripDelegations(text) {
    return text.replace(DELEGATE_RE, '').trim();
}

// ── Loop Detection ──

function hashContent(text) {
    let h = 0;
    const s = text.substring(0, 200);
    for (let i = 0; i < s.length; i++) {
        h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    }
    return h.toString(36);
}

function isLoopDetected(chainId, content) {
    if (!chainId) return false;
    if (!_chainHashes.has(chainId)) _chainHashes.set(chainId, new Set());
    const hashes = _chainHashes.get(chainId);
    const h = hashContent(content);
    if (hashes.has(h)) return true;
    hashes.add(h);
    if (_chainHashes.size > 50) {
        const oldest = _chainHashes.keys().next().value;
        _chainHashes.delete(oldest);
    }
    return false;
}

// ── Delegation Execution ──

function executeDelegation(target, message, chainId, chainDepth) {
    return new Promise((resolve, reject) => {
        if (activeDelegations >= MAX_CONCURRENT_DELEGATIONS) {
            reject(new Error('Too many concurrent delegations'));
            return;
        }
        activeDelegations++;

        const delegCorrId = `deleg-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
        const timer = setTimeout(() => {
            cleanup();
            reject(new Error(`Delegation to ${target} timed out`));
        }, DELEGATION_TIMEOUT);

        const handler = (_topic, payload) => {
            try {
                const m = JSON.parse(payload.toString());
                const inCorrId = m.correlationId || m.correlation_id || '';
                if (inCorrId === delegCorrId) {
                    cleanup();
                    const content = m.content || m.message || '';
                    resolve({ from: target, content });
                }
            } catch(e) { /* ignore parse errors from other messages */ }
        };

        function cleanup() {
            clearTimeout(timer);
            activeDelegations--;
            if (client) client.removeListener('message', handler);
        }

        client.on('message', handler);

        const msg = {
            from: ID, to: target, sender_id: ID, receiver_id: target,
            type: 'delegation', message_type: 'command',
            content: message, message,
            correlationId: delegCorrId, correlation_id: delegCorrId,
            chain_id: chainId, chain_depth: chainDepth,
            delegation: true,
            timestamp: new Date().toISOString(),
        };
        const encoded = JSON.stringify(msg);

        // Publish to target's inbox and own outbox (for dashboard visibility)
        client.publish(`agents/${target}/inbox`, encoded, { qos: 1 });
        client.publish(`agents/${ID}/outbox`, encoded, { qos: 1 });
        console.log(T, `[delegate] -> ${target}: ${message.substring(0, 120)}`);
    });
}

// ── Heartbeat ──

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
        capabilities: ['chat','task_execution','mqtt_listener','delegation'],
        timestamp: new Date().toISOString()
    }), {qos: 0});
}

// ── Main ──

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

        // Register
        client.publish(`agents/${ID}/register`, JSON.stringify({
            agent_id:ID, sender_id:ID, display_name:DISPLAY, role:ROLE, device_type:DEVICE,
            capabilities:['chat','task_execution','mqtt_listener','delegation'],
            ip_address:getIP(), status:'online', timestamp:new Date().toISOString()
        }), {retain: true, qos: 1});

        heartbeat();
        hbTimer = setInterval(heartbeat, HB_SEC*1000);

        // Fetch roster then subscribe
        fetchRoster().then(() => {
            rosterTimer = setInterval(fetchRoster, ROSTER_REFRESH_SEC * 1000);
        });

        // Subscribe to inbox and broadcasts
        client.subscribe(`agents/${ID}/inbox`, {qos: 1});
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

                // Extract delegation chain metadata
                const chainId = m.chain_id || '';
                const chainDepth = m.chain_depth || 0;

                // Reject if chain depth exceeds safety limit
                if (chainDepth > MAX_CHAIN_DEPTH) {
                    console.log(T, `Rejecting: chain_depth ${chainDepth} exceeds limit`);
                    reply(from, '[Error: delegation chain too deep]', corrId, { chainId, chainDepth });
                    return;
                }

                console.log(T, `${from}: ${content.substring(0,120)}${chainDepth > 0 ? ` (chain depth ${chainDepth})` : ''}`);
                callAgent(from, content, corrId, { chainId, chainDepth });
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

// ── LLM Agent Call with Delegation Loop ──

function callAgent(from, content, corrId, opts = {}) {
    const chainId = opts.chainId || `chain-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const chainDepth = opts.chainDepth || 0;
    const round = opts.round || 0;
    const delegationContext = opts.delegationContext || '';

    // Build prompt with delegation context
    const rosterStr = buildRosterString();
    let prompt;
    if (round === 0) {
        prompt = `[Message from ${from}] ${content}`;
        if (rosterStr && chainDepth < MAX_DELEGATION_DEPTH) {
            prompt += `\n\n[AVAILABLE AGENTS]\n${rosterStr}\n\nYou can delegate tasks to other agents by writing [DELEGATE:agent_id] followed by a message. Only delegate when the task requires capabilities you don't have. Example: [DELEGATE:jeeves] Check the temperature sensors`;
        }
    } else {
        prompt = `[Message from ${from}] ${content}\n\n[DELEGATION RESULTS]\n${delegationContext}\n\nSynthesize the delegation results and provide your final answer.`;
        if (round < MAX_DELEGATION_DEPTH && rosterStr) {
            prompt += ` You may delegate again if needed (${MAX_DELEGATION_DEPTH - round} rounds remaining).`;
        } else {
            prompt += ' Do NOT use [DELEGATE:...] syntax. Provide your final answer now.';
        }
    }

    const sessionId = `mqtt-${from}-${ID}`;
    const args = [
        'agent',
        '-m', prompt,
        '--session-id', sessionId,
        '--json',
        '--timeout', String(AGENT_TIMEOUT),
    ];

    console.log(T, `Calling openclaw agent (session: ${sessionId}, round: ${round})...`);

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
            reply(from, `[Error: agent failed to process message — ${err.message}]`, corrId, { chainId, chainDepth });
            return;
        }

        // Parse LLM response
        let responseText = '';
        try {
            const result = JSON.parse(stdout);
            const inner = result.result || result;
            const texts = (inner.payloads || [])
                .map(p => p.text)
                .filter(t => t && t.trim());
            responseText = texts.join('\n\n');
        } catch(e) {
            responseText = stdout.trim();
        }

        if (!responseText.trim()) {
            console.log(T, 'Agent returned empty response');
            return;
        }

        // Check for delegations
        const delegations = parseDelegations(responseText);

        if (delegations.length === 0 || round >= MAX_DELEGATION_DEPTH) {
            // No delegations or depth exceeded — send final reply
            const finalText = stripDelegations(responseText) || responseText;
            reply(from, finalText, corrId, { chainId, chainDepth });
            return;
        }

        // Execute delegations
        console.log(T, `${delegations.length} delegation(s) in round ${round + 1}`);

        const delegPromises = delegations.map((d) => {
            if (isLoopDetected(chainId, d.message)) {
                console.log(T, `Loop detected for delegation to ${d.target}, skipping`);
                return Promise.resolve({ from: d.target, content: '[Loop detected, skipped]' });
            }
            return executeDelegation(d.target, d.message, chainId, chainDepth + 1)
                .catch((e) => ({ from: d.target, content: `[Error: ${e.message}]` }));
        });

        Promise.allSettled(delegPromises).then((results) => {
            const context = results.map((r) => {
                const val = r.status === 'fulfilled' ? r.value : { from: 'unknown', content: `[Error: ${r.reason?.message || 'unknown'}]` };
                return `From ${val.from}: ${val.content}`;
            }).join('\n\n');

            // Re-invoke LLM with delegation results
            callAgent(from, content, corrId, {
                chainId,
                chainDepth,
                round: round + 1,
                delegationContext: context,
            });
        });
    });
}

// ── Reply ──

function reply(to, content, corrId, opts = {}) {
    if (!client) return;
    const msg = {
        from:ID, to, sender_id:ID, receiver_id:to,
        type:'response', message_type:'result',
        content, message:content,
        correlationId:corrId||'', correlation_id:corrId||'',
        timestamp:new Date().toISOString()
    };
    if (opts.chainId) msg.chain_id = opts.chainId;
    if (opts.chainDepth !== undefined) msg.chain_depth = opts.chainDepth;
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

// ── Shutdown ──

function shutdown() {
    if(hbTimer) clearInterval(hbTimer);
    if(rosterTimer) clearInterval(rosterTimer);
    if (client) {
        client.publish(`agents/${ID}/status`, JSON.stringify({
            sender_id:ID, status:'offline', timestamp:new Date().toISOString()
        }), {qos: 1}, () => {
            client.end(false, {}, () => {
                process.exit(0);
            });
        });
        setTimeout(() => process.exit(0), 5000);
    } else {
        process.exit(0);
    }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(T, `Listener starting | ${DISPLAY} (${ROLE}) | ${HOST}:${PORT} | P2P delegation enabled (max depth: ${MAX_DELEGATION_DEPTH})`);
start();
