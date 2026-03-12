# Future Potential

EdgeCitadel's current infrastructure — NATS with JetStream, MQTT adapter, P2P delegation, task system, real-time dashboard — is roughly 30% utilized. This document covers what the existing setup unlocks without major rearchitecting.

---

## 1. Crash Recovery via JetStream Replay

**Current state:** The `CONVERSATIONS` JetStream stream captures every message to disk, but the aggregator never reads from it. If the aggregator restarts, in-flight messages are lost.

**What it enables:** A durable consumer can resume from the last acknowledged sequence on startup — zero message loss across restarts.

**How it works:**

```python
# In aggregator.py connect()
sub = await self.js.subscribe(
    "agents.>",
    durable="aggregator-agents",    # survives restarts
    deliver_policy=DeliverPolicy.LAST_PER_SUBJECT,
)
async for msg in sub.messages:
    await self._on_agent_message(msg.subject, msg.data.decode())
    await msg.ack()  # mark as processed
```

On restart, NATS delivers only unacknowledged messages — the aggregator picks up exactly where it left off.

**Effort:** ~20 lines changed in `aggregator.py`.

> **Reference:** [JetStream Consumers](https://docs.nats.io/nats-concepts/jetstream/consumers) | [nats-py JetStream](https://github.com/nats-io/nats.py#jetstream)

---

## 2. Agent Discovery via K/V Watch

**Current state:** The `AGENT_STATE` K/V bucket is written on every heartbeat but nothing reads it. Agents fetch the roster via HTTP polling (`GET /api/agents` every 60 seconds).

**What it enables:** Agents could watch the K/V bucket for changes and get notified instantly when peers come online or go offline — no polling, no HTTP dependency, no aggregator dependency.

**How it works:**

```python
# Watch for agent state changes
kv = await js.key_value("AGENT_STATE")
watcher = await kv.watchall()
async for entry in watcher:
    if entry.value:
        state = json.loads(entry.value)
        print(f"Agent {entry.key}: {state['action']} at {state['last_seen']}")
```

This means agents could discover peers even if the aggregator is down.

**Effort:** Requires agents to use a native NATS client (nats.ws or nats.js) instead of MQTT.

> **Reference:** [NATS K/V Store](https://docs.nats.io/nats-concepts/jetstream/key-value-store) | [K/V Watch Example](https://natsbyexample.com/examples/kv/watch/go)

---

## 3. Native Request-Reply for P2P

**Current state:** P2P delegation uses pub/sub with manual correlation ID matching — publish to inbox, install a temporary message handler, wait for a reply matching the correlation ID.

**What it enables:** NATS request-reply eliminates all the matching logic. The requester publishes with an auto-generated reply subject; the responder replies to that subject; the requester gets the response directly.

**Current implementation (40+ lines):**

```javascript
// mqtt-listener.js executeDelegation() - correlation matching
const delegCorrId = `deleg-${Date.now()}-...`;
const handler = (_topic, payload) => {
    const m = JSON.parse(payload.toString());
    if ((m.correlationId || m.correlation_id) === delegCorrId) {
        cleanup();
        resolve({ from: target, content: m.content || m.message });
    }
};
client.on('message', handler);
client.publish(`agents/${target}/inbox`, encoded);
```

**With NATS request-reply (~5 lines):**

```javascript
// Using nats.js native client
const response = await nc.request(
    `agents.${target}.inbox`,
    encode(message),
    { timeout: 90_000 }
);
const result = JSON.parse(response.data);
```

No correlation IDs, no temporary handlers, no race conditions.

**Effort:** Requires switching the agent listener from `mqtt.js` to `nats` npm package. The MQTT adapter doesn't support request-reply.

> **Reference:** [NATS Request-Reply](https://docs.nats.io/nats-concepts/core-nats/reqreply) | [nats.js Request](https://github.com/nats-io/nats.js#request-reply)

---

## 4. Horizontal Scaling via Queue Groups

**Current state:** One aggregator instance processes all messages.

**What it enables:** Multiple aggregator instances subscribe to the same subjects as a queue group. NATS load-balances messages across them automatically — each message is delivered to exactly one instance.

```python
# Each aggregator instance joins the same queue group
sub = await self.nc.subscribe("agents.>", queue="aggregators")
```

```
Message arrives on agents.jeeves.heartbeat
  ├── Aggregator A (queue: "aggregators") ← NATS picks one
  ├── Aggregator B (queue: "aggregators")
  └── Aggregator C (queue: "aggregators")
```

If one instance crashes, the others continue processing. Same pattern works for agents — five Jeeves instances on a Pi cluster can share the `agents.jeeves.inbox` queue.

**Effort:** One line change per subscription. Requires shared database (e.g., PostgreSQL instead of SQLite) for multi-instance aggregators.

> **Reference:** [NATS Queue Groups](https://docs.nats.io/nats-concepts/core-nats/queue) | [Queue Subscribe Example](https://natsbyexample.com/examples/messaging/queue-group/python)

---

## 5. Edge Computing with Leaf Nodes

**Current state:** Single NATS server on one machine. All agents connect directly.

**What it enables:** A Raspberry Pi at a remote site runs a NATS leaf node that connects to the central cluster. If the network goes down, the leaf queues messages locally and forwards them when connectivity returns — true offline-first edge computing.

**Central server config:**

```
# nats.conf (central)
leafnodes {
    port: 7422
}
```

**Edge device config:**

```
# nats-leaf.conf (Raspberry Pi)
server_name: "edge-garage"
leafnodes {
    remotes [
        { url: "nats://100.97.29.74:7422" }
    ]
}
jetstream {
    store_dir: "/data/jetstream"
    max_mem: 32MB
    max_file: 128MB
}
```

```
┌──────────────────┐         ┌──────────────────┐
│  Central NATS    │◄───────►│  Edge NATS (Pi)   │
│  (Mac Mini)      │ Tailscale│  leaf node        │
│  port 4222+7422  │         │  port 4222        │
└──────────────────┘         └──────────────────┘
       ▲                            ▲
       │                            │
  Aggregator               IoT sensors/agents
  Dashboard                (connect locally)
```

Agents at the edge connect to their local NATS. Messages automatically route to the central server. If the link drops, JetStream buffers messages on both sides.

**Effort:** Add leaf node config to `nats.conf`, deploy NATS on edge devices.

> **Reference:** [NATS Leaf Nodes](https://docs.nats.io/running-a-nats-service/configuration/leafnodes) | [Edge Architecture](https://docs.nats.io/nats-concepts/overview#scalability-and-the-edge)

---

## 6. Token Streaming in the Dashboard

**Current state:** The task system has `tasks.{id}.stream` subjects and the WebSocket supports `token_stream` events, but nothing uses them.

**What it enables:** When an agent's LLM generates a response, tokens stream to the dashboard character-by-character — like ChatGPT's typing effect but for multi-agent conversations.

**Agent-side (in mqtt-listener.js):**

```javascript
// Stream tokens as the LLM generates them
const child = execFile(OPENCLAW_BIN, ['agent', '-m', prompt, '--stream']);
child.stdout.on('data', (chunk) => {
    client.publish(`tasks/${corrId}/stream`, JSON.stringify({
        task_id: corrId,
        token: chunk.toString(),
        agent_id: ID,
    }));
});
```

**Dashboard-side (WebSocket event):**

```javascript
// useWebSocket.js already handles token_stream events
case 'token_stream':
    // Append token to the in-progress message bubble
    appendToken(event.data.task_id, event.data.token);
    break;
```

During P2P delegation, the requesting agent could also subscribe to the streaming subject to monitor the delegate's progress in real-time.

**Effort:** Modify `callAgent()` to use streaming exec, add token append logic to `MessageBubble.jsx`.

---

## 7. Multi-Agent Workflow DAGs

**Current state:** P2P delegation supports linear chains (A→B→C) and parallel fan-out (A→B+C). The task system tracks state transitions.

**What it enables:** Directed acyclic graph (DAG) workflows where tasks have dependencies:

```
┌──────────────────┐
│ Task 1: Research  │ (Rupert + Jeeves in parallel)
│ - gather data     │
└────────┬─────────┘
         │ depends on
┌────────▼─────────┐
│ Task 2: Write     │ (Percy drafts report)
│ - compose report  │
└────────┬─────────┘
         │ depends on
┌────────▼─────────┐
│ Task 3: Review    │ (Rupert reviews)
│ - quality check   │
└──────────────────┘
```

**Implementation:** Add a `blocked_by` field to the task schema. When a task completes, check if any blocked tasks are now unblocked and assign them automatically.

```python
# In database.py
"CREATE TABLE IF NOT EXISTS tasks (
    ...
    blocked_by TEXT DEFAULT '',  -- comma-separated task IDs
)"
```

The JetStream stream provides the complete audit trail for the entire workflow.

**Effort:** Add `blocked_by` to tasks table, add unblock logic in the aggregator's task completion handler.

---

## 8. Per-Agent Authentication and ACLs

**Current state:** Single `NATS_TOKEN` for all agents — any agent can read/write any subject.

**What it enables:** Each agent gets its own credentials with subject-level permissions:

```
# nats.conf
accounts {
    AGENTS {
        users: [
            {
                user: "jeeves",
                password: "$JEEVES_TOKEN",
                permissions: {
                    publish: ["agents.jeeves.>", "tasks.>"]
                    subscribe: ["agents.jeeves.inbox", "system.broadcast"]
                }
            },
            {
                user: "rupert",
                password: "$RUPERT_TOKEN",
                permissions: {
                    publish: ["agents.>", "tasks.>", "system.>"]
                    subscribe: ["agents.>", "system.>"]
                }
            }
        ]
    }
}
```

This prevents a compromised IoT device from impersonating other agents or eavesdropping on their messages. Only the orchestrator (Rupert) can broadcast and subscribe to all agent subjects.

**Effort:** Rewrite `nats.conf` authorization section, update join.sh to provision per-agent tokens.

> **Reference:** [NATS Authorization](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/authorization) | [NATS Accounts](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/accounts)

---

## 9. Webhook and External Service Integration

**Current state:** The aggregator processes all messages internally.

**What it enables:** Independent NATS subscribers that trigger external actions — no changes to the aggregator:

| Subject Pattern | Trigger | Action |
|---|---|---|
| `agents.*.outbox` (type=alert) | Agent raises alert | Send Slack notification |
| `tasks.*.failed` | Task fails | Create GitHub issue |
| `agents.*.status` (offline) | Agent goes offline | Page on-call via PagerDuty |
| `system.broadcast` | System announcement | Forward to email |

**Example (Node.js webhook subscriber):**

```javascript
const nc = await connect({ servers: 'nats://localhost:4222' });
const sub = nc.subscribe('agents.*.outbox');
for await (const msg of sub) {
    const data = JSON.parse(msg.data);
    if (data.message_type === 'alert') {
        await fetch(SLACK_WEBHOOK, {
            method: 'POST',
            body: JSON.stringify({ text: `Alert from ${data.sender_id}: ${data.message}` }),
        });
    }
}
```

Each integration is a standalone process. Add or remove integrations without touching the aggregator.

**Effort:** ~30 lines per integration. No aggregator changes needed.

---

## 10. Persistent Agent Memory via K/V

**Current state:** Agent conversation context lives in `openclaw agent --session-id` sessions. No shared memory across agents.

**What it enables:** The JetStream K/V store could serve as shared agent memory accessible to any agent:

```python
# Jeeves stores a sensor baseline
await kv.put("sensors.living_room.baseline", b"22.5")

# Rupert reads it for decision-making
entry = await kv.get("sensors.living_room.baseline")
baseline = float(entry.value)

# Percy watches for changes
watcher = await kv.watch("sensors.>")
async for entry in watcher:
    print(f"Sensor update: {entry.key} = {entry.value}")
```

This gives agents long-term memory that survives restarts, is accessible network-wide, and supports real-time change notifications.

**Effort:** Add K/V read/write helpers to the agent listener. Requires native NATS client.

> **Reference:** [NATS K/V Store](https://docs.nats.io/nats-concepts/jetstream/key-value-store) | [K/V Operations](https://natsbyexample.com/examples/kv/intro/python)

---

## 11. MCP Tool Server per Agent

**Current state:** Agents use `[DELEGATE:agent] message` text convention for P2P. The LLM infers what to delegate from natural language.

**What it enables:** Each agent exposes an MCP (Model Context Protocol) tool server that advertises its capabilities as structured tools:

```json
{
    "name": "query_sensors",
    "description": "Read IoT sensor data from Jeeves",
    "input_schema": {
        "type": "object",
        "properties": {
            "room": { "type": "string", "enum": ["living_room", "kitchen", "bedroom"] },
            "metric": { "type": "string", "enum": ["temperature", "humidity"] }
        }
    }
}
```

Instead of the LLM guessing `[DELEGATE:jeeves] What's the temperature in the kitchen?`, it would invoke a typed tool call with validated parameters. The openclaw CLI already supports MCP tool servers.

**Effort:** Write an MCP server per agent type, configure openclaw to connect to peer agents' MCP servers.

> **Reference:** [Model Context Protocol](https://modelcontextprotocol.io/) | [MCP Tool Servers](https://modelcontextprotocol.io/docs/concepts/tools)

---

## 12. Multi-Site Federation

**Current state:** The database has a `deployment` field on messages (always "local"). Single NATS server.

**What it enables:** Multiple EdgeCitadel sites connected via NATS gateway connections:

```
┌─────────────────┐     Gateway     ┌─────────────────┐
│   Home Site     │◄──────────────►│   Office Site    │
│   NATS cluster  │                │   NATS cluster   │
│   Rupert+Jeeves │                │   Percy+Watson   │
└─────────────────┘                └─────────────────┘
```

**Gateway config:**

```
# Home nats.conf
gateway {
    name: "home"
    port: 7222
    gateways: [
        { name: "office", url: "nats://office-ip:7222" }
    ]
}
```

Agents at home can communicate with agents at the office transparently. The `deployment` field distinguishes local vs remote messages. Each site has its own dashboard showing both local and federated agents.

**Effort:** Add gateway config, update aggregator to populate the deployment field from message origin.

> **Reference:** [NATS Gateways](https://docs.nats.io/running-a-nats-service/configuration/gateways) | [Super Clusters](https://docs.nats.io/nats-concepts/overview#scalability-and-the-edge)

---

## Priority Ranking

| Priority | Feature | Effort | Impact |
|---|---|---|---|
| 1 | JetStream replay (crash recovery) | ~20 lines | High — zero message loss |
| 2 | Token streaming | Medium | High — much better UX |
| 3 | Request-reply P2P | Medium | Medium — simpler delegation code |
| 4 | Webhook integrations | Small per hook | Medium — extensibility |
| 5 | K/V agent discovery | Medium | Medium — removes HTTP dependency |
| 6 | Per-agent auth | Config change | Medium — security hardening |
| 7 | Leaf nodes (edge) | Config + deploy | High for edge use cases |
| 8 | Workflow DAGs | Schema + logic | High for complex tasks |
| 9 | MCP tool servers | Per-agent server | High for reliability |
| 10 | Queue group scaling | 1-line change | High at scale |
| 11 | Persistent agent memory | K/V helpers | Medium |
| 12 | Multi-site federation | Gateway config | High for multi-site |
