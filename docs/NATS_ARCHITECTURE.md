# EdgeCitadel NATS Architecture

## Previous Architecture (MQTT Hub Relay)

```
Jeeves (RPi)     Percy (EC2)     Dashboard (browser)
     \               |               /
      \              |              /
       ▼             ▼             ▼
     ┌─────────────────────────────────┐
     │      Mosquitto MQTT Broker      │
     │      (single Docker container)  │
     └────────────────┬────────────────┘
                      │
                      ▼
               ┌─────────────┐
               │  Aggregator  │  ← paho-mqtt background thread
               │  (FastAPI)   │    asyncio.run_coroutine_threadsafe()
               │  + SQLite    │    to bridge to async
               └─────────────┘
```

**Problems:**
- **Single point of failure** — broker dies, all agents disconnect
- **Hub relay** — every agent-to-agent message routes through the aggregator
- **Race conditions** — paho-mqtt callbacks fire from background threads, racing on SQLite writes
- **GIL deadlock** — thread-bridging between paho-mqtt and asyncio
- **No streaming** — MQTT pub/sub has no native token streaming for LLM responses
- **No session replay** — conversation history only in SQLite, no stream-based catch-up
- **No peer discovery** — agents don't know about each other without querying the aggregator API

### Message flow (old)

```
Dashboard → POST /api/command/jeeves
  → Aggregator publishes to MQTT topic "agents/inbox/jeeves"
    → Aggregator also publishes to "openclaw/{dep}/jeeves/cmd"
      → Jeeves receives on MQTT subscription
        → Jeeves replies to "openclaw/{dep}/jeeves/result"
          → Aggregator receives, stores in SQLite, broadcasts to WebSocket
            → Dashboard sees the reply
```

Agent A could never directly message Agent B. All messages were mediated.

---

## Current Architecture (NATS Direct P2P)

```
Mac Mini                  Raspberry Pi              EC2
┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│ Rupert Agent      │     │ Jeeves Agent      │     │ Percy Agent       │
│ (nats-listener.js)│     │ (nats-listener.js)│     │ (nats-listener.js)│
└────────┬──────────┘     └────────┬──────────┘     └────────┬──────────┘
         │                         │                          │
         ▼                         ▼                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     NATS Server (nats:4222)                          │
│  ┌────────────────┐  ┌──────────────────┐  ┌──────────────────────┐ │
│  │ Core pub/sub   │  │ JetStream        │  │ KV: AGENT_STATE      │ │
│  │ (fire & forget)│  │ CONVERSATIONS    │  │  jeeves → {...}      │ │
│  │                │  │ stream           │  │  rupert → {...}      │ │
│  │                │  │ (durable replay) │  │  percy  → {...}      │ │
│  └────────────────┘  └──────────────────┘  └──────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
         ▲                                            ▲
         │ subscribes to agents.> (passive observer)  │
         │                                            │
  ┌──────┴───────┐                             ┌──────┴──────┐
  │  Aggregator  │                             │  Dashboard  │
  │  (FastAPI)   │ ──── WebSocket ────────────►│  (React)    │
  │  + SQLite    │                             │             │
  └──────────────┘                             └─────────────┘
```

**Key change:** The aggregator is a **passive observer**, not a message relay. Agents talk directly to each other through NATS subjects.

---

## Comparison: Hub Relay vs. Direct P2P

### Latency

| Path | Hub relay (old) | Direct P2P (new) |
|------|-----------------|-------------------|
| Agent → Agent (same host) | ~2-4 ms (agent → MQTT → aggregator Python → MQTT → agent) | ~50-100 μs (agent → NATS → agent) |
| Agent → Agent (cross-host, Tailscale) | ~10-50 ms (+ Python relay hop) | ~1-5 ms (NATS only) |
| 100-token LLM stream | ~200 ms (each token relayed through Python) | ~10 ms (direct NATS delivery) |

Direct P2P is **10-30x faster** per message.

### Throughput

| Component | msgs/sec |
|-----------|----------|
| Core NATS 1:1 pub/sub | ~4,900,000 |
| NATS request-reply (50 clients) | ~132,000 |
| JetStream async publish | ~400,000 |
| Python/FastAPI relay (realistic ceiling) | ~5,000-15,000 |

The old relay approach capped throughput at **0.1-0.3%** of what NATS can natively handle. At 50 agents × 100 msg/sec, the Python relay was already at capacity.

### Reliability

| Concern | Hub relay (old) | Direct P2P (new) |
|---------|-----------------|-------------------|
| Single point of failure | Aggregator crash = all communication stops | Only NATS server needed; aggregator crash doesn't affect agent messaging |
| Message loss on observer downtime | N/A (aggregator was in the path) | JetStream replay catches up on missed messages |
| HA option | Requires aggregator clustering (complex) | NATS 3-node cluster (built-in) |

### Observability

Both approaches provide **equivalent observability**. In the new architecture, the aggregator subscribes to `agents.>` as a passive wildcard listener — it receives every message on every agent subject, stores in SQLite, and broadcasts to the dashboard WebSocket. The difference is that observation is **decoupled from delivery** — the aggregator can crash and restart without any messages between agents being lost or delayed.

### Scalability

| Metric | Hub relay (old) | Direct P2P (new) |
|--------|-----------------|-------------------|
| Max agents before bottleneck | ~50 (Python throughput limit) | ~1,000+ (NATS native) |
| Burst handling | No headroom beyond steady-state | 3-4 orders of magnitude of headroom |
| Fan-out (1 message to N agents) | O(N) through Python | O(N) native in NATS |

---

## How Agents Communicate (New Architecture)

### Subject Namespace

| Subject | Purpose | Persistence |
|---------|---------|-------------|
| `agents.{name}.heartbeat` | Liveness pings + system metrics | Core NATS (ephemeral) |
| `agents.{name}.register` | Agent registration on startup | JetStream (durable) |
| `agents.{name}.inbox` | Incoming messages to the agent | JetStream (durable) |
| `agents.{name}.outbox` | Agent's published responses | JetStream (durable) |
| `agents.{name}.status` | Status changes (online/offline) | Core NATS (ephemeral) |
| `agents.{name}.log` | Structured log entries | JetStream (durable) |
| `tasks.{id}.assign` | Task assignment | JetStream (durable) |
| `tasks.{id}.progress` | Task progress updates | Core NATS (ephemeral) |
| `tasks.{id}.stream` | LLM token streaming | Core NATS (ephemeral) |
| `tasks.{id}.complete` | Task completion | JetStream (durable) |
| `tasks.{id}.failed` | Task failure | JetStream (durable) |
| `system.broadcast` | System-wide announcements | Core NATS (ephemeral) |

### Flow 1: Dashboard → Agent

User types a command targeting agent "jeeves" in the dashboard.

```
1. Dashboard UI
   → POST /api/command/jeeves { "message": "check disk usage" }

2. Aggregator (main.py)
   → Injects sender_id="dashboard", correlation_id=uuid
   → Publishes to NATS: agents.jeeves.inbox

3. Two things happen simultaneously:

   3a. Aggregator (passive observer on agents.>)
       → Receives agents.jeeves.inbox message
       → Stores in SQLite (message + log + auto-creates task)
       → Broadcasts to dashboard WebSocket
       → Dashboard shows "sent" message

   3b. Jeeves (nats-listener.js, subscribed to agents.jeeves.inbox)
       → Parses message, extracts content
       → Calls openclaw agent CLI
       → Publishes response to agents.jeeves.outbox

4. Aggregator (passive observer)
   → Receives agents.jeeves.outbox message
   → Stores in SQLite, broadcasts to dashboard WebSocket
   → Dashboard shows jeeves' reply
```

### Flow 2: Agent → Agent (direct P2P)

Rupert (orchestrator) asks Jeeves (IoT) to run a health check.

```
1. Rupert decides to contact Jeeves
   → Looks up "jeeves" in AGENT_STATE KV bucket (peer discovery)
   → Confirms jeeves is online with capabilities: ["system_health", ...]

2. Rupert publishes to:
   → agents.rupert.outbox  (own outbox, so aggregator can log it)
   → agents.jeeves.inbox   (direct delivery to jeeves)

3. Three things happen simultaneously:

   3a. Aggregator (passive observer on agents.>)
       → Receives both messages
       → Stores conversation in SQLite
       → Dashboard shows the exchange in real-time

   3b. Jeeves (subscribed to agents.jeeves.inbox)
       → Receives command directly from NATS
       → Runs health check via openclaw CLI
       → Publishes response to agents.jeeves.outbox

   3c. Rupert (can optionally subscribe to agents.jeeves.outbox)
       → Receives jeeves' response directly
       → Or uses NATS request-reply for synchronous request/response

4. Aggregator receives jeeves' outbox response
   → Stores, broadcasts to dashboard
   → Full conversation visible in UI
```

### Flow 3: Broadcast to all agents

```
1. Dashboard or orchestrator publishes to system.broadcast
   → All agents subscribed to system.broadcast receive it
   → Aggregator receives it too (subscribed to system.>)
```

### Agent Discovery via NATS KV

Agents discover peers through the `AGENT_STATE` JetStream KV bucket — shared state on the NATS server accessible to all connected clients.

```
NATS Server
┌─────────────────────────────┐
│  AGENT_STATE KV bucket       │
│  ┌─────────────────────────┐ │
│  │ jeeves → {              │ │
│  │   "role": "iot",        │ │
│  │   "status": "online",   │ │
│  │   "capabilities": [...],│ │
│  │   "last_seen": "..."    │ │
│  │ }                       │ │
│  │ rupert → { ... }        │ │
│  │ percy  → { ... }        │ │
│  └─────────────────────────┘ │
└─────────────────────────────┘
      ▲         ▲         ▲
  put own   get/watch  put own
   entry    all keys    entry
     │         │          │
   Jeeves    Rupert     Percy
```

**On startup**, each agent:
1. Writes its own entry: `kv.put(ID, { role, capabilities, status, ... })`
2. Lists all peers: `kv.keys()` → `['jeeves', 'rupert', 'percy']`
3. Watches for changes: `kv.watchAll()` → real-time push when agents join/leave

**No aggregator dependency** — discovery works even if the aggregator is down. Entries can have TTL so stale agents auto-expire.

---

## Security

| Layer | Mechanism |
|-------|-----------|
| Transport encryption | Tailscale WireGuard (all inter-node traffic) |
| NATS authentication | Token-based auth (`--auth` flag, `NATS_TOKEN` env var) |
| Future | NATS Accounts + NKeys + JWT for per-agent subject permissions |

---

## JetStream Configuration Notes

- **CONVERSATIONS stream**: `sync_interval: "always"` recommended per [Jepsen NATS 2.12.1 analysis](https://jepsen.io/analyses/nats-2.12.1) — default 2-minute sync can lose acknowledged writes on OS crash
- **AGENT_STATE KV bucket**: entries should have TTL matching heartbeat timeout so stale agents auto-expire
- **Core NATS** (no JetStream) for ephemeral data: heartbeats, token streaming, status updates — at-most-once is acceptable for these
