# Architecture: Hybrid NATS + MQTT

EdgeCitadel runs a single **NATS 2.10+** server that serves two protocols simultaneously:

- **Native NATS** (port 4222) — used by the aggregator for full JetStream features
- **MQTT 3.1.1 adapter** (port 1883) — used by IoT agents (Raspberry Pi, ESP32, smartphones)

This gives the best of both worlds: enterprise-grade streaming and persistence for the backend, simple pub/sub for constrained devices.

## Why Hybrid

| Concern | NATS (aggregator) | MQTT (agents) |
|---|---|---|
| Persistent streams | JetStream `CONVERSATIONS` stream | N/A (NATS persists for them) |
| K/V state store | `AGENT_STATE` bucket | N/A |
| Request-reply | Native support | Correlation ID over pub/sub |
| IoT compatibility | Not practical on ESP32 | First-class, tiny footprint |
| Auth | Token-based | Same token as MQTT password |

## Topic Translation

NATS uses dot-separated subjects. MQTT uses slash-separated topics. The NATS server translates automatically:

```
MQTT:  agents/jeeves/heartbeat
NATS:  agents.jeeves.heartbeat
       ↕ automatic translation ↕
```

The aggregator's `publish()` method also converts slashes to dots, so REST API callers can use either format.

## Subject Structure

```
agents.{name}.register     # Agent registration with capabilities
agents.{name}.heartbeat    # Periodic health check (CPU, memory, IP)
agents.{name}.inbox        # Commands TO the agent
agents.{name}.outbox       # Results FROM the agent
agents.{name}.status       # Status changes (online/offline)
agents.{name}.log          # Agent log entries

tasks.{id}.assign          # Task assignment
tasks.{id}.progress        # Task progress update
tasks.{id}.stream          # Token streaming
tasks.{id}.complete        # Task completion with result
tasks.{id}.failed          # Task failure with error

system.broadcast           # Broadcast to all agents
```

## JetStream Persistence

The `CONVERSATIONS` stream captures all subjects matching `agents.>`, `tasks.>`, `system.>`:

- Retention: max 10,000 messages
- Storage: file-backed (survives restarts)
- Replay: ordered consumer for conversation history

The `AGENT_STATE` K/V bucket stores live state per agent:

```json
{
  "agent_id": "jeeves",
  "action": "heartbeat",
  "last_seen": 1710000000
}
```

## Authentication

Single `NATS_TOKEN` environment variable:

- **NATS clients**: pass as connection token
- **MQTT clients**: pass as password (username is arbitrary, e.g. `mqtt-agent`)
- **No auth in test stack**: test-nats.conf omits the authorization block

## Docker Compose Stack

```
┌─────────────────────────────────────────────┐
│  nginx (:80)                                │
│  ├── /api/*  → aggregator:8000              │
│  ├── /ws/*   → aggregator:8000              │
│  └── /*      → dashboard:80                 │
├─────────────────────────────────────────────┤
│  aggregator (FastAPI, nats-py)              │
│  └── SQLite /data/openclaw.db               │
├─────────────────────────────────────────────┤
│  dashboard (React, Vite, Nginx)             │
├─────────────────────────────────────────────┤
│  nats (:4222 NATS, :1883 MQTT, :8222 HTTP)  │
│  └── JetStream /data/jetstream/             │
└─────────────────────────────────────────────┘
```

## Data Flow

```
IoT Agent (mqtt.js)                    Aggregator (nats-py)
     │                                      │
     ├── MQTT publish ──→ NATS server ──→ subscription ──→ parse + store
     │                         │                              │
     │                         │                         WebSocket broadcast
     │                         │                              │
     │                    JetStream                     React Dashboard
     │                    persists                           │
     │                         │                         REST API queries
     └── MQTT subscribe ←─────┘                              │
                                                         SQLite DB
```
