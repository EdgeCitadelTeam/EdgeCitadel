# API Reference

Base URL: `http://<host>/api`

This is the EdgeCitadel Aggregator HTTP surface for v0.1 of the agent messaging
contract. All envelope rows returned by `/api/messages` conform to
[`schemas/envelope.v1.json`](../schemas/envelope.v1.json) and all agent records
embed an A2A v1.0 Agent Card from [`schemas/agent-card.v1.json`](../schemas/agent-card.v1.json).

The aggregator owns the canonical command-dispatch path: dashboards and
operators call `POST /api/command/{id}` rather than publishing to NATS
directly. Direct JetStream publish is reserved for the openclaw browser flow
(Task 14, session-token endpoint) and adapter-to-adapter delegation.

## Endpoint Index

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/system/status` | NATS + JetStream health |
| `GET` | `/api/agents` | List agent records (excluding `aggregator`) |
| `GET` | `/api/agents/{id}` | Single agent record |
| `GET` | `/api/agents/{id}/card` | A2A Agent Card payload only |
| `GET` | `/api/agents/{id}/queue` | JetStream consumer pending counts |
| `DELETE` | `/api/agents/{id}` | Forget cached card |
| `POST` | `/api/command/{id}` | Dispatch a `command` envelope, returns `task_id` |
| `GET` | `/api/messages` | Filtered envelope rows |
| `GET` | `/api/poison` | Recent JetStream MAX_DELIVERIES poison events |
| `GET` | `/api/registry` | Fleet snapshot for Registry tab (card + queue + poison count) |

The legacy v0 surface — `/api/agents/{id}/heartbeat`, `/api/messages` with
`receiver_id`/`message_type`/`correlation_id` parameters, the
`mqtt_connected` field on system status, the `/deployments` and `/publish`
families, and the `/health`, `/tasks`, `/logs`, `/messages/conversations`,
`/messages/flow`, `/system/topology`, and `/broadcast` routes — has been
removed. Adapters use the canonical NATS surface (`docs/05-messaging.md`)
directly.

---

## GET /api/system/status

Returns NATS connection status, JetStream stream readiness, and the
aggregator build version.

**Response 200**
```json
{
  "nats_connected": true,
  "jetstream_stream_ok": true,
  "version": "0.1.0"
}
```

- `nats_connected` — whether the aggregator's NATS client is currently
  connected.
- `jetstream_stream_ok` — whether `stream_info("AGENT_INBOX")` succeeded on
  the most recent call. Returns `false` when NATS is connected but the
  stream is missing or unreachable.
- `version` — aggregator build version.

The legacy `mqtt_connected` field has been removed; MQTT is now an optional
NATS-side translation, not a separate broker (see `docs/05-messaging.md`).

---

## GET /api/conversations

Conversation snapshot — one row per `(agent_id, context_id)` with aggregate fields. Used by future "active conversations" dashboard view.

**Query params:**
- `agent_id` (optional) — filter to one agent.

**Response:** `200 OK`
```json
[
  {
    "context_id": "ctx-abc",
    "agent_id": "gemma-1",
    "turns": 14,
    "tokens": 3210,
    "first_seen": "2026-04-30T08:01:00.000Z",
    "last_seen":  "2026-04-30T08:14:32.000Z",
    "skills": ["reasoning.chat", "text.summarize"]
  }
]
```

---

## GET /api/agents

List of all known agents excluding the aggregator's self-cached entry.

**Response 200**
```json
[
  {
    "agent_id": "shell-1",
    "card": {
      "name": "shell-1",
      "description": "Generic shell adapter",
      "version": "0.1.0",
      "url": "nats://edgecitadel/agents.shell-1.inbox",
      "provider": {"organization": "EdgeCitadel"},
      "capabilities": {"streaming": false},
      "securitySchemes": {},
      "metadata": {
        "runtime.kind": "native",
        "runtime.roles": ["worker"],
        "runtime.heartbeat_interval_sec": 30
      }
    },
    "agent_state": "online",
    "last_heartbeat": "2026-04-23T10:05:12.412Z",
    "last_register": "2026-04-23T10:00:00.000Z",
    "deployment": null,
    "heartbeat_interval_sec": 30
  }
]
```

Each entry combines the cached A2A Agent Card with aggregator-tracked
metadata: `agent_state`, `last_heartbeat`, `last_register`, `deployment`,
and `heartbeat_interval_sec`.

---

## GET /api/agents/{id}

Single agent record. Same shape as one element of `GET /api/agents`.

**Response 200** — entry as above.
**Response 404** — `{"detail": "agent not found"}`.

---

## GET /api/agents/{id}/card

Returns just the A2A Agent Card payload (no aggregator metadata).

**Response 200**
```json
{
  "name": "shell-1",
  "description": "Generic shell adapter",
  "version": "0.1.0",
  "url": "nats://edgecitadel/agents.shell-1.inbox",
  "provider": {"organization": "EdgeCitadel"},
  "capabilities": {"streaming": false},
  "securitySchemes": {},
  "metadata": {
    "runtime.kind": "native",
    "runtime.roles": ["worker"],
    "runtime.heartbeat_interval_sec": 30
  }
}
```

**Response 404** — `{"detail": "agent not found"}`.

---

## GET /api/agents/{id}/queue

JetStream consumer info for the unique `AGENT_INBOX` consumer whose filter is
`agents.{id}.inbox`. Used by the dashboard to surface inbox depth and ack
pressure without depending on a producer-specific durable name.

**Response 200**
```json
{
  "pending": 3,
  "ack_pending": 1,
  "num_waiting": 0
}
```

- `pending` — `num_pending` from the consumer.
- `ack_pending` — `num_ack_pending` from the consumer.
- `num_waiting` — `num_waiting` if available, else `0`.

**Response 404** — consumer not found (`{"detail": "consumer not found: ..."}`).
**Response 503** — JetStream not yet initialized; the aggregator hasn't
finished startup or NATS is unreachable.

---

## DELETE /api/agents/{id}

Forget an agent's cached card and aggregator metadata. The agent will
reappear on its next `register` envelope.

**Response 204** — deleted.
**Response 400** — `{"detail": "cannot delete self"}` for `id == "aggregator"`.
**Response 404** — `{"detail": "agent not found"}`.

---

## POST /api/command/{id}

Canonical command dispatch. The aggregator generates a fresh `task_id`,
publishes a `command` envelope to `agents.{id}.inbox` via JetStream with
`Nats-Msg-Id = envelope.id` for idempotency, and mirrors the envelope to
`agents.aggregator.outbox` for the audit trail.

The frontend uses the returned `task_id` to subscribe to subsequent
`task.progress` and `result` envelopes for the same task.

**Request body**
```json
{
  "body": "echo hi",
  "args": {"timeout_sec": 30}
}
```

- `body` (string, required) — the command body, interpreted by the recipient
  adapter.
- `args` (object, optional) — adapter-specific arguments.

**Response 202**
```json
{
  "task_id": "5d1a3e1c-9e47-4bd7-8c6a-2f0e3a1c9b88",
  "recipient_id": "shell-1",
  "accepted_at": "2026-04-24T12:34:56.789Z"
}
```

- `task_id` — UUID4 generated by the aggregator. Use this to filter
  `/api/messages?task_id=...` and to correlate streamed progress events.
- `recipient_id` — echoed for convenience.
- `accepted_at` — envelope `timestamp`.

**Response 422** — request body fails Pydantic validation (e.g. missing
`body`).

The published envelope conforms to the `command` shape in
[`schemas/envelope.v1.json`](../schemas/envelope.v1.json):

```json
{
  "v": 1,
  "id": "<envelope-uuid>",
  "type": "command",
  "sender_id": "aggregator",
  "recipient_id": "shell-1",
  "task_id": "<task-uuid>",
  "timestamp": "2026-04-24T12:34:56.789Z",
  "payload": {"body": "echo hi", "args": {"timeout_sec": 30}}
}
```

---

## GET /api/messages

Returns filtered envelope rows from the outbox-mirrored audit log
(see ADR-0006). All filters AND together; `agent_id` matches sender OR
recipient.

**Query parameters**

| Name | Type | Default | Notes |
|---|---|---|---|
| `agent_id` | string | — | matches `sender_id` OR `recipient_id` |
| `task_id` | string | — | exact match |
| `context_id` | string | — | exact match |
| `type` | string | — | `register`, `heartbeat`, `status`, `command`, `result`, `delegation`, `cancel`, `log`, `broadcast`, `task.progress` |
| `since_ts` | ISO-8601 string | — | only rows with `timestamp >= since_ts`; useful for isolating a live benchmark run |
| `deployment` | string | — | exact deployment match |
| `exclude_deployment` | string | — | exclude an exact deployment |
| `limit` | int | 500 | result cap |

**Response 200**
```json
[
  {
    "id": "ec6d…",
    "v": 1,
    "type": "command",
    "sender_id": "aggregator",
    "recipient_id": "shell-1",
    "task_id": "5d1a…",
    "context_id": null,
    "task_state": null,
    "agent_state": null,
    "hop_count": null,
    "timestamp": "2026-04-24T12:34:56.789Z",
    "payload": {"body": "echo hi"},
    "deployment": "default"
  }
]
```

Rows are returned newest-first. Envelope shape matches
[`schemas/envelope.v1.json`](../schemas/envelope.v1.json) with the
columnar fields lifted into top-level keys for easy filtering.

The legacy `receiver_id`, `message_type`, and `correlation_id` query
parameters and columns are gone. Use `recipient_id`, `type`, and `task_id`
respectively.

---

## GET /api/poison

Recent JetStream `MAX_DELIVERIES` advisory events captured from
`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>`. Used by the
dashboard to surface inbox messages an adapter has repeatedly failed to
process.

**Query parameters**

| Name | Type | Default | Notes |
|---|---|---|---|
| `agent_id` | string | — | filter by recipient agent |
| `limit` | int | 100 | result cap |

**Response 200**
```json
[
  {
    "id": 14,
    "agent_id": "shell-1",
    "consumer": "shell-1_inbox",
    "task_id": "5d1a…",
    "original_sender": "aggregator",
    "detected_at": "2026-04-24T12:35:11.020Z",
    "advisory_json": "{...full advisory body...}"
  }
]
```

Rows are newest-first.

---

## GET /api/registry

Fleet snapshot consumed by the dashboard's Registry tab. Joins the
`agents` table with JetStream consumer info and a poison-event count.

**Query params:**
- `deployment` (optional) — filter to one deployment string.

**Response:** `200 OK`
```json
[
  {
    "agent_id": "gemma-1",
    "card": { "..." : "..." },
    "agent_state": "online",
    "last_heartbeat": "2026-04-29T10:15:23.412Z",
    "last_register": "2026-04-29T08:02:11.001Z",
    "deployment": "default",
    "heartbeat_interval_sec": 30,
    "queue": {"pending": 0, "ack_pending": 1},
    "poison_count": 0
  }
]
```

- `card` — full A2A Agent Card as stored by the aggregator.
- `agent_state` — last known runtime state (`online`, `offline`, `busy`, `error`).
- `last_heartbeat` / `last_register` — ISO 8601 UTC timestamps from the most recent envelope of each type.
- `deployment` — value of `metadata["runtime.deployment"]` from the Agent Card, or `null`.
- `heartbeat_interval_sec` — declared heartbeat interval used by the watchdog staleness calculation.
- `queue` — live JetStream consumer counts (`pending`, `ack_pending`). `null` if the consumer does not exist yet.
- `poison_count` — number of MAX_DELIVERIES advisory rows for this agent in the current 24-hour window.

**Response 200** — always an array (empty array when no agents are registered).

---

## WebSocket events

The aggregator broadcasts real-time agent and task events over the WebSocket endpoint
(`ws://<host>/ws`). Each frame is a JSON object with an `event` field indicating the
type and an `data` payload.

| Event | Payload | Notes |
|---|---|---|
| `agent_registered` | `{agent_id, card, agent_state}` | Fired when a new `register` envelope is received. Dashboard adds or refreshes the agent row. |
| `agent_state_changed` | `{agent_id, agent_state}` | Fired on `status` envelope or watchdog heartbeat-timeout. |
| `agent_deleted` | `{agent_id}` | Fired when `DELETE /api/agents/{id}` succeeds. Registry tab removes the row. |
| `task_progress` | `{task_id, task_state, payload}` | Forwarded from `agents.{id}.task_progress.{task_id}` subjects. |
| `result` | `{task_id, task_state, sender_id, payload}` | Fired when a `result` envelope lands on any inbox the aggregator observes. |
