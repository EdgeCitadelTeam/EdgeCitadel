# Messaging

## Message Types

| Type | Direction | Description |
|---|---|---|
| `register` | Agent → Server | Agent announces itself with capabilities |
| `heartbeat` | Agent → Server | Periodic health check (CPU, memory, IP) |
| `command` | Dashboard/Agent → Agent | Task or instruction sent to an agent |
| `result` | Agent → Dashboard/Agent | Response to a command |
| `broadcast` | Agent → All | System-wide announcement |
| `info` | Agent → Server | Informational message |
| `alert` | Agent → Server | Alert/warning message |
| `delegation` | Agent → Agent | P2P task delegation (see [P2P Delegation](06-p2p-delegation.md)) |
| `task_assign` | Server → Agent | Task assignment notification |
| `task_progress` | Agent → Server | Task progress update |
| `task_complete` | Agent → Server | Task completed with result |
| `task_failed` | Agent → Server | Task failed with error |

## Command-Reply Flow

```
Dashboard                  Aggregator              Agent (Jeeves)
    │                          │                        │
    ├─ POST /command/jeeves ──→│                        │
    │  {message, correlation_id}                        │
    │                          ├─ publish ──────────────→│
    │                          │  agents.jeeves.inbox    │
    │                          │                        │
    │                          │  (agent calls LLM)     │
    │                          │                        │
    │                          │←── publish ────────────┤
    │                          │  agents.jeeves.outbox   │
    │                          │                        │
    │←── WebSocket event ──────┤  (stored in DB)        │
    │  {message, correlation_id}                        │
```

The dashboard groups command + result by `correlation_id` and shows them as a conversation pair.

## Message Format

All messages are JSON with these common fields:

```json
{
  "sender_id": "rupert",
  "receiver_id": "jeeves",
  "type": "command",
  "message_type": "command",
  "content": "Check the temperature sensors",
  "message": "Check the temperature sensors",
  "correlation_id": "1710000000000-abc12345",
  "timestamp": "2026-03-12T10:00:00.000Z",
  "payload": {
    "message": "Check the temperature sensors"
  }
}
```

## Deduplication

The aggregator prevents duplicate messages using a bounded cache:

- **Correlation-based**: Same `correlation_id + sender + receiver + type` within 5 seconds is deduplicated
- **Content-based**: Result messages with same `sender + receiver + content hash` within 5 seconds
- **Cache size**: 500 entries max, 10-second TTL cleanup

## Message Storage

Messages are stored in the SQLite `messages` table:

| Column | Type | Description |
|---|---|---|
| id | INTEGER | Auto-increment primary key |
| deployment | TEXT | Deployment name (default: "local") |
| sender_id | TEXT | Sending agent ID |
| receiver_id | TEXT | Receiving agent ID |
| message_type | TEXT | Message type (command, result, etc.) |
| payload | TEXT | JSON payload |
| correlation_id | TEXT | Links related messages |
| timestamp | TEXT | ISO 8601 timestamp |

Heartbeat and register messages are **not** stored in the messages table (they update the agents table instead).

## Raw Episodes

Every message is also stored as a raw episode in the `episodes` table for audit purposes:

| Column | Type | Description |
|---|---|---|
| deployment | TEXT | Deployment name |
| topic | TEXT | NATS subject |
| payload | TEXT | Raw JSON payload |
| ts | INTEGER | Unix timestamp |

## Querying Messages

```bash
# All messages (latest 50)
curl http://localhost/api/messages?limit=50

# Filter by agent
curl http://localhost/api/messages?agent=jeeves

# Filter by type
curl http://localhost/api/messages?type=command

# Full-text search
curl http://localhost/api/messages?search=temperature

# By correlation ID (get conversation thread)
curl http://localhost/api/messages?correlation_id=task-001

# Exclude test data
curl http://localhost/api/messages?exclude_test=true
```

## Communication Flow (Topology)

```bash
# Get message flow graph (nodes + edges)
curl http://localhost/api/messages/flow?hours=24

# Response:
{
  "nodes": [
    {"id": "rupert"},
    {"id": "jeeves"}
  ],
  "edges": [
    {"source": "rupert", "target": "jeeves", "count": 5}
  ]
}
```

## Broadcasting

Send a message to all agents:

```bash
curl -X POST http://localhost/api/broadcast \
  -H "Content-Type: application/json" \
  -d '{"sender_id": "rupert", "message": "All systems nominal"}'
```

Agents subscribed to `system/broadcast` receive the message.
