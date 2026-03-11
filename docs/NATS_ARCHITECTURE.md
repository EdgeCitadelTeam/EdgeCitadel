# NATS Leaf Node Architecture for EdgeCitadel

## Current Architecture (MQTT Central Broker)

```
Mac Mini (Rupert)     Raspberry Pi (Jeeves)     EC2 (Percy)
       \                    |                    /
        \                   |                   /
         ▼                  ▼                  ▼
       ┌─────────────────────────────────────────┐
       │         Mosquitto MQTT Broker           │
       │         (single Docker container)        │
       └──────────────────┬──────────────────────┘
                          │
                          ▼
                    ┌───────────┐
                    │ Aggregator│ ← paho-mqtt background thread
                    │ (FastAPI) │   asyncio.run_coroutine_threadsafe()
                    │ + SQLite  │   to bridge to async
                    └───────────┘
```

**Problems:**
- **Single point of failure** — broker dies, all agents disconnect
- **Race conditions** — paho-mqtt callbacks fire from background threads, racing on SQLite writes
- **GIL deadlock** — thread-bridging between paho-mqtt and asyncio (commit d183737)
- **No P2P** — Rupert→Jeeves always routes through the central broker
- **No streaming** — MQTT pub/sub has no native token streaming for LLM responses
- **No session replay** — conversation history only in SQLite, no stream-based catch-up

## New Architecture (NATS Leaf Nodes)

```
Mac Mini                  Raspberry Pi              EC2
┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│ ┌───────────────┐ │     │ ┌───────────────┐ │     │ ┌───────────────┐ │
│ │ NATS Leaf     │ │     │ │ NATS Leaf     │ │     │ │ NATS Leaf     │ │
│ │ :4222         │◄├────►├►│ :4222         │◄├────►├►│ :4222         │ │
│ └───────┬───────┘ │     │ └───────┬───────┘ │     │ └───────┬───────┘ │
│         │         │     │         │         │     │         │         │
│ ┌───────▼───────┐ │     │ ┌───────▼───────┐ │     │ ┌───────▼───────┐ │
│ │ Rupert Agent  │ │     │ │ Jeeves Agent  │ │     │ │ Percy Agent   │ │
│ └───────────────┘ │     │ └───────────────┘ │     │ └───────────────┘ │
│ ┌───────────────┐ │     │                   │     │                   │
│ │ Aggregator    │ │     │                   │     │                   │
│ │ + SQLite      │ │     │                   │     │                   │
│ └───────────────┘ │     │                   │     │                   │
└───────────────────┘     └───────────────────┘     └───────────────────┘
         Tailscale encrypted overlay (existing, no new infra)
```

**Each node runs its own NATS leaf node (~20 MB binary, 32 MB RAM, ARM64).**

### Key Components

#### 1. Transport: NATS Leaf Nodes
- Each physical device runs a NATS leaf node process
- Leaf nodes connect to each other over Tailscale (unicast, no multicast needed)
- Local agents connect to `localhost:4222` — sub-ms local latency
- Cross-node messages hop one leaf-to-leaf connection (~1-5ms on Tailscale)

#### 2. Persistence: JetStream Streams
- `CONVERSATIONS` stream on subject `conversations.>` — all agent messages
- `AGENT_STATE` K/V bucket — replaces SQLite agents table for live state
- Ordered consumers for the aggregator to write to SQLite (read-replica for dashboard)
- Session replay: any agent can rewind to sequence N to resume a conversation

#### 3. Streaming: NATS Request-Reply + SSE
- Agent A publishes task request to `agents.{name}.tasks`
- Agent B streams LLM tokens back on `tasks.{task_id}.stream`
- Aggregator subscribes to `tasks.>` and forwards to WebSocket for dashboard

#### 4. Subject Namespace

| Subject Pattern | Purpose | Replaces |
|---|---|---|
| `agents.{name}.heartbeat` | Agent liveness | `agents/heartbeat/{name}` |
| `agents.{name}.register` | Agent registration | `agents/register/{name}` |
| `agents.{name}.inbox` | Commands to agent | `agents/inbox/{name}`, `openclaw/{d}/{a}/cmd` |
| `agents.{name}.outbox` | Agent responses | `openclaw/{d}/{a}/result` |
| `conversations.{session}.{agent}` | Conversation messages | `openclaw/{d}/{a}/chat` |
| `tasks.{task_id}.assign` | Task assignment | Topic-embedded task messages |
| `tasks.{task_id}.stream` | LLM token streaming | (new, not possible with MQTT) |
| `system.broadcast` | Broadcast to all | `agents/broadcast/*` |
| `system.logs.{level}` | Structured logs | Log messages embedded in MQTT |

## Architecture Comparison

| | MQTT (Current) | NATS Leaf Nodes | Zenoh P2P | libp2p |
|---|---|---|---|---|
| **Topology** | Central broker | Distributed brokers | Brokerless P2P | Brokerless P2P |
| **SPOF** | Yes (Mosquitto) | No (any node can die) | No | No |
| **Latency (local)** | ~1ms (through broker) | <0.1ms (local NATS) | <0.1ms (direct) | ~1ms (DHT lookup) |
| **Latency (cross-node)** | ~5ms (through broker) | ~1-5ms (leaf-to-leaf) | ~0.5-2ms (direct) | ~5-10ms (relay) |
| **Persistence** | None (broker-level only) | JetStream (built-in) | None (build yourself) | None |
| **Streaming** | No | Yes (ordered consumers) | Yes (queryable) | Yes (yamux streams) |
| **Session replay** | No | Yes (stream sequence) | No | No |
| **Python async** | No (thread bridge hack) | Yes (native nats.py) | Yes (zenoh-python) | Thin bindings |
| **Backpressure** | No | Yes (flow control) | Manual | Manual |
| **Auth** | Password file | Accounts + NKeys + JWT | TLS mutual auth | Noise protocol |
| **Observability** | Mosquitto logs only | nats top, metrics, traces | Limited | Limited |
| **Binary size** | ~10MB | ~20MB | ~15MB | ~30MB+ |
| **ARM64 support** | Yes | Yes | Yes | Rust only |
| **Community** | Large (IoT standard) | Large (CNCF graduated) | Growing (Eclipse) | Large (Web3) |
| **Maturity** | 25+ years | 10+ years, CNCF | 5 years | 8 years |

### When to choose what

- **NATS Leaf Nodes** — You need persistence, streaming, session replay, and operational simplicity. Best for EdgeCitadel's multi-turn LLM conversations.
- **Zenoh P2P** — You need absolute minimum latency and zero infrastructure. Best for real-time robotics or stateless consensus. Consider as a future upgrade if agent count grows past 20+.
- **libp2p** — You need decentralized identity and DHT-based discovery across untrusted networks. Overkill for Tailscale (already trusted overlay).
- **Stay on MQTT** — You only do fire-and-forget telemetry with no streaming or session needs.

## Migration Path

### Phase 1: NATS Infrastructure
- Add NATS server to docker-compose.yml (replaces Mosquitto)
- Configure JetStream with conversation stream
- Verify leaf node connectivity over Tailscale

### Phase 2: Aggregator Migration
- Replace paho-mqtt with nats.py in aggregator.py
- Native async subscriptions (no more thread bridging)
- Map MQTT topic patterns to NATS subjects
- Keep SQLite as read-replica for dashboard queries

### Phase 3: Streaming & Sessions
- Add JetStream ordered consumers for conversation persistence
- Implement token streaming via NATS request-reply
- Add session replay endpoint for agents rejoining conversations

### Phase 4: Agent Migration
- Update agent clients to use NATS (nats.py / nats.go)
- Implement A2A task protocol over NATS subjects
- Remove Mosquitto dependency entirely
