# Messaging (v0.1)

## Status

v0.1 messaging is **NATS JetStream over the canonical envelope** at [`schemas/envelope.v1.json`](../schemas/envelope.v1.json). Plain NATS carries the ephemeral fleet traffic (registration, heartbeat, status, log, broadcast, task progress, outbox mirror); a single JetStream stream `AGENT_INBOX` carries the durable per-agent command/result/delegation/cancel traffic.

MQTT ingress is **deploy-time opt-in only** (default off). When enabled, it exists exclusively to onboard constrained IoT devices that cannot speak NATS; the internal fleet (aggregator, native adapters, openclaw browser client) does not use MQTT under any deployment. See ADR-0004 (forthcoming, Task 15) and the `EC_ENABLE_MQTT` flag below.

This document is the **operational reference** — what subjects exist, how the stream and per-agent consumer are configured, how operators verify the system is healthy. The **wire contract** (envelope shape, required-by-type matrix, lifecycle rules) lives in [`docs/agent-contract.md`](agent-contract.md). When the two disagree, the contract wins.

---

## Subject inventory

All envelopes are JSON objects validated against [`schemas/envelope.v1.json`](../schemas/envelope.v1.json).

| Subject | Transport | Envelope `type` | Direction | Purpose |
|---|---|---|---|---|
| `agents.{id}.register` | Plain NATS | `register` | Agent → fleet | Agent announces itself with its A2A Agent Card. Idempotent: re-publishes overwrite the cached card. |
| `agents.{id}.heartbeat` | Plain NATS | `heartbeat` | Agent → fleet | Periodic liveness signal. Interval is per-agent, declared in the Agent Card under `metadata["runtime.heartbeat_interval_sec"]`. |
| `agents.{id}.status` | Plain NATS | `status` | Agent → fleet | Agent runtime state changes. Carries `agent_state ∈ {online, offline, busy, error}`. |
| `agents.{id}.inbox` | **JetStream `AGENT_INBOX`** | `command`, `result`, `delegation`, `cancel` | Any sender → agent | Durable per-agent work queue. At-least-once delivery, FIFO per agent (see consumer config). |
| `agents.{id}.outbox` | Plain NATS | mirror of inbox publishes | Agent (self) → aggregator audit | Per ADR-0006, every adapter mirrors its outbound `agents.{recipient}.inbox` publishes here so the aggregator can populate dashboard conversation views without attaching a second consumer to the WorkQueue stream. |
| `agents.{id}.log` | Plain NATS | `log` | Agent → fleet | Optional structured log envelopes. Persisted by the aggregator. |
| `agents.{id}.task_progress.{task_id}` | Plain NATS | `task.progress` | Streaming agents → aggregator + dashboard | In-flight token deltas during inference. Payload `{delta, skill_id?}`; hybrid 8-tokens-or-100ms cadence. Note the dual naming: subject uses `task_progress` (underscore), envelope `type` uses `task.progress` (A2A dotted form). Intentional. |
| `memory.turns.get` | Plain NATS | (request-reply) | Adapter (any) → aggregator | Fetch prior turns within token budget. Request: `{context_id, agent_id, token_budget?}`. Response: `{turns: [...], total_tokens}`. |
| `memory.turns.put` | Plain NATS | (request-reply) | Adapter (any) → aggregator | Persist a single turn. Request: `{context_id, agent_id, role, content, skill_id?}`. Response: `{id, token_count}`. |
| `memory.turns.delete` | Plain NATS | (request-reply) | Adapter or operator → aggregator | Forget all turns for a context_id. Request: `{context_id}`. Response: `{deleted_count}`. |
| `system.broadcast` | Plain NATS | `broadcast` | Any → all | Fleet-wide announcements. Aggregator publishes `payload: {action: "request_register"}` here on restart to solicit re-registration. |
| `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>` | JetStream advisory | (advisory, not envelope) | JetStream → aggregator + watchdog | Poison-message advisory. Raised when a consumer hits `max_deliver`. Aggregator persists it (surfaced via `GET /api/poison`); watchdog (`watchdog-1`) also subscribes (Phase 3.1) to synthesise `recipient_offline` failures for tasks the heartbeat-staleness and sticky-offline paths missed. Two plain-NATS subscribers — no JetStream consumer-slot conflict. |

**Routing rule:** only `agents.{id}.inbox` is durable (JetStream). Everything else is plain NATS fire-and-forget.

**`payload.extra.upstream`** (optional). Bridge adapters (`runtime.kind: bridge`) SHOULD set this to the upstream agent product identifier — e.g., Hermes' bridge sets `"hermes-agent"`. Native adapters (Gemma, Watchdog) omit this field. See [agent-contract.md §"Bridge adapters"](agent-contract.md#bridge-adapters).

**Watchdog (`watchdog-1`) subject summary:**

| Role | Subjects |
|---|---|
| Subscribes to | `agents.*.register`, `agents.*.outbox`, `agents.*.heartbeat`, `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>` |
| Publishes to | `agents.watchdog-1.register`, `agents.watchdog-1.heartbeat`, `agents.watchdog-1.status`, `agents.watchdog-1.outbox`, `agents.watchdog-1.log`; synthesised `result` envelopes → `agents.{original_sender}.inbox` (JetStream) |

---

## JetStream `AGENT_INBOX` stream config

Configured idempotently by [`aggregator/jetstream_bootstrap.py`](../aggregator/jetstream_bootstrap.py) `ensure_stream`. The aggregator calls it at startup; each adapter calls it lazily on first connect (create-if-not-exists).

| Field | Value | Rationale |
|---|---|---|
| `name` | `AGENT_INBOX` | Single stream covers the entire fleet. |
| `subjects` | `["agents.*.inbox"]` | Wildcard matches every agent's inbox without per-agent provisioning. |
| `retention` | `WorkQueuePolicy` | Each message is removed once any consumer acks it. Audit history lives in the outbox mirror (ADR-0006), not in the stream. |
| `discard` | `DiscardNew` | When the stream fills, reject new publishes rather than evict old ones. Backpressure beats silent loss. |
| `max_age` | `24h` (86400 s) | Bound on undelivered work. A message older than 24 h is presumed stale. |
| `max_bytes` | `1 GiB` | Sized for a single Mac Mini fleet (<50 agents). |
| `max_msg_size` | `1 MiB` | Matches the NATS server default. Oversized envelopes are rejected at publish time. Bodies above ~256 KB SHOULD use `payload.body_ref` pointing at object storage. |
| `duplicate_window` | `5 min` | When publishers set `Nats-Msg-Id`, the broker dedupes within this window. Operator double-clicks and reconnect-time republishes are no-ops. |

See [ADR-0002](adr/0002-nats-jetstream-workqueue.md) for the full rationale and the considered alternatives.

---

## Per-agent durable consumer config

Configured by `ensure_consumer` in the same bootstrap module. One consumer per agent; created on the agent's first connect and persisted across restarts.

| Field | Value | Rationale |
|---|---|---|
| `durable_name` | `{agent_id}_inbox` | Stable per-agent identifier; consumer state survives broker and adapter restarts. |
| `filter_subject` | `agents.{agent_id}.inbox` | Each consumer sees only its own agent's traffic. WorkQueue's disjoint-filter rule is satisfied trivially because no two consumers' filters overlap. |
| `ack_policy` | `explicit` | The adapter must `ack()` after the work is complete. No auto-ack. |
| `ack_wait` | `300 s` (default) | Floor, not ceiling. Long-running tasks call `in_progress()` periodically to extend the deadline. |
| `max_ack_pending` | `1` | **The FIFO knob.** At any time, the agent has at most one un-acked message in flight; the next message arrives only after the previous is acked. Per-agent serialization is enforced by the broker, not the adapter. |
| `max_deliver` | `3` | Three failed delivery attempts and the broker emits a max-deliveries advisory on `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>`. The aggregator captures it as a `poison_events` row. |

**Aggregator's own consumer is the exception.** It does not need FIFO over its inbox (no shared mutable state per envelope). It uses `max_ack_pending: 100` and `ack_wait: 60s` on the same stream.

See [ADR-0002](adr/0002-nats-jetstream-workqueue.md) §"Decision Outcome".

---

## Canonical envelope shape

Every message published to `agents.*`, `tasks.*`, or `system.*` is a JSON object that validates against [`schemas/envelope.v1.json`](../schemas/envelope.v1.json). The schema is **strict** — unknown top-level fields are rejected.

Baseline required fields (every envelope): `v` (= `1`), `id` (UUID4), `type`, `sender_id`, `timestamp` (ISO 8601 UTC, ms precision, `Z` suffix), `payload` (object).

Type-specific required fields (e.g., `recipient_id` and `task_id` on `command`, `agent_state` on `status`, `hop_count` and `context_id` on `delegation`) live in the **required-by-type matrix** in [`docs/agent-contract.md`](agent-contract.md) §1.6. The contract document is the single source of truth for envelope grammar; this doc deliberately does not duplicate it.

Hard size limit: 1 MiB per envelope (NATS default + stream `max_msg_size`).

---

## Publisher semantics

### JetStream publishes (`agents.{recipient}.inbox`)

- MUST set the `Nats-Msg-Id: <envelope.id>` header on every publish. The broker's 5-minute `duplicate_window` uses this header to dedupe redelivered messages.
- MUST mirror to the publisher's own outbox: every successful `js.publish("agents.{recipient}.inbox", env, ...)` pairs with `nc.publish("agents.{self}.outbox", env)` on plain NATS. This is enforced by the shared adapter wrapper in `adapters/_common/pull_consumer.py` (Task 10) — adapters do not call `js.publish` directly. See [ADR-0006](adr/0006-outbox-mirror-authoritative.md) for why the mirror lives on plain NATS rather than JetStream.

### Plain NATS publishes (everything else)

- `heartbeat`, `status`, `log`, `broadcast`, `task.progress`, and the outbox mirror itself do **not** require `Nats-Msg-Id`. They are ephemeral; the broker does not store them and dedup does not apply.
- Publish-and-forget: `await nc.publish(subject, json.dumps(env).encode())`.

---

## Stream-full backpressure

`discard: new` means the broker rejects publishes once the stream reaches `max_bytes`; old messages are not evicted to make room. The publisher sees a publish error rather than silent loss.

Recommended client behavior:

- **`command` / `result` / `delegation` / `cancel`** — log the publish error and retry with exponential backoff. The envelope's `id` is stable across retries, so the dedup window absorbs duplicates.
- **`heartbeat` / `status` / `log` / `task.progress` / `broadcast`** — these go over plain NATS, so stream-full backpressure does not apply. Plain NATS publishes never block; the broker either delivers or drops based on subscriber state.

Stream depth is observable via `js.stream_info()` (or `nats stream info AGENT_INBOX`) and exposed to the dashboard via `GET /api/system/queue`.

---

## MQTT ingress

**Status: deploy-time opt-in. Off by default.**

- To enable: set `EC_ENABLE_MQTT=1` and re-render `nats/nats.conf` (the rendering script ships in Task 15). Without that flag, port 1883 is not exposed and the in-broker MQTT 3.1.1 adapter is not activated.
- **Use case:** constrained IoT sensors that can only speak MQTT 3.1.1 (e.g., ESP8266/ESP32 firmware). A designated gateway agent normalizes MQTT-origin payloads into canonical A2A envelopes before they enter the JetStream inbox.
- **The internal fleet does NOT use MQTT under any deployment.** Aggregator, native adapters (shell, AG2, LangGraph), and the openclaw browser client speak NATS directly. MQTT is the IoT-onboarding seam, not a transport for first-party components.

See ADR-0004 (forthcoming, Task 15) for the formal decision record.

---

## Legacy MQTT topics — DELETED

v0.1 is a clean rebuild. The following are gone with no alias-fallback shim:

- **`citadel/agents/{id}/...` slash topics.** Replaced by the dotted `agents.{id}.{leaf}` NATS subject family.
- **paho-mqtt JS / Python clients.** The openclaw client is rewritten in `@nats-io/nats` (Task 13); the aggregator's MQTT subscription path is removed entirely.
- **Field aliases on the legacy informal envelope.** None of `receiver_id`, `message_type`, `correlation_id`, `causation_id`, `chain_id`, or `assigned_agent` are accepted. The canonical fields are `recipient_id`, `type`, `task_id`, `context_id`, `hop_count` — strict; unknown fields are rejected.
- **`mqtt_connected` field in `/api/system/status`.** Removed. The replacement fields are `nats_connected` and `jetstream_stream_ok`.

If a producer ships an envelope with any of the legacy field names, the aggregator's validator (Task 3) rejects it at the boundary.

---

## Verification

How an operator confirms messaging is healthy:

- **Stream is live:**
  ```
  docker compose exec nats nats stream info AGENT_INBOX
  ```
  Confirms the stream exists with the expected subject filter, retention, and discard policy.

- **Aggregator sees broker and stream:**
  ```
  curl http://localhost:8000/api/system/status
  ```
  Expect `nats_connected: true` and `jetstream_stream_ok: true`.

- **Per-agent consumer state:**
  ```
  curl http://localhost:8000/api/agents/<id>/queue
  docker compose exec nats nats consumer info AGENT_INBOX <id>_inbox
  ```
  Shows `pending` (un-delivered), `ack_pending` (delivered, awaiting ack), and `redelivered` counts.

- **Recent poison events:**
  ```
  curl 'http://localhost:8000/api/poison?agent_id=<id>'
  ```
  Returns rows persisted from the `MAX_DELIVERIES` advisory subject.

- **End-to-end smoke (Task 17):**
  ```
  cd e2e && npm test -- phase1-smoke.spec.js
  ```
  Exercises register → command → result round-trip plus subject-inventory coverage.

---

## References

- [`docs/agent-contract.md`](agent-contract.md) — the v0.1 wire contract (envelope, lifecycle, Agent Card).
- [`schemas/envelope.v1.json`](../schemas/envelope.v1.json) — strict JSON Schema for envelopes.
- [`schemas/agent-card.v1.json`](../schemas/agent-card.v1.json) — A2A v1.0 Agent Card schema.
- [ADR-0002](adr/0002-nats-jetstream-workqueue.md) — JetStream WorkQueue choice and consumer config rationale.
- [ADR-0006](adr/0006-outbox-mirror-authoritative.md) — outbox mirror on plain NATS as authoritative for dashboard views.
- [`aggregator/jetstream_bootstrap.py`](../aggregator/jetstream_bootstrap.py) — `ensure_stream` / `ensure_consumer` source.
- [`docs/superpowers/specs/2026-04-23-agent-messaging-design.md`](superpowers/specs/2026-04-23-agent-messaging-design.md) — full design spec.
