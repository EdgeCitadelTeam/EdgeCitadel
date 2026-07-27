# Messaging (v0.1)

## Status

v0.1 messaging is **NATS JetStream over the canonical envelope** at [`schemas/envelope.v1.json`](../schemas/envelope.v1.json). Plain NATS carries the ephemeral fleet traffic (registration, heartbeat, status, log, broadcast, task progress, outbox mirror); a single JetStream stream `AGENT_INBOX` carries the durable per-agent command/result/delegation/cancel traffic.

MQTT ingress is **deploy-time opt-in only** (default off). When enabled, it exists exclusively to onboard constrained IoT devices that cannot speak NATS; the internal fleet (aggregator, native adapters, openclaw browser client) does not use MQTT under any deployment. See ADR-0004 (forthcoming, Task 15) and the `EC_ENABLE_MQTT` flag below.

This document is the **operational reference** — what subjects exist, how the stream and per-agent consumer are configured, how operators verify the system is healthy. The **base wire contract** (envelope shape, required-by-type matrix, lifecycle rules) lives in [`docs/agent-contract.md`](agent-contract.md). When the two disagree, the base contract wins except for the task-correlation projection explicitly defined as a normative supplement below.

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

Type-specific required fields (e.g., `recipient_id` and `task_id` on `command`, `agent_state` on `status`, `hop_count` and `context_id` on `delegation`) live in the **required-by-type matrix** in [`docs/agent-contract.md`](agent-contract.md) §1.6. That contract and `envelope.v1.json` remain the source of truth for base envelope grammar. The correlation projection below is the narrow exception for `command`, `delegation`, `cancel`, and `result`.

Hard size limit: 1 MiB per envelope (NATS default + stream `max_msg_size`).

The injected task executor also limits canonical JSON structures to 128 nested object/array containers. An inbound request beyond that limit is terminated as redacted `nesting_too_deep` poison; an over-depth handler payload is a malformed handler return and becomes the generic ledgered `handler_failed` terminal.

### Task correlation supplement

For `command`, `delegation`, `cancel`, and `result`, [`schemas/task-correlation.v1.json`](../schemas/task-correlation.v1.json) is a normative supplement to the base envelope contract and prevails for correlation-projection constraints. A receiver first validates the complete wire document against `envelope.v1.json`, then validates only the seven-field correlation projection: `type`, `sender_id`, `recipient_id`, `task_id`, `context_id`, `hop_count`, and `payload`. Projection defaults are never written back to the wire document.

- `id` is the UUIDv4 identity of one wire envelope. A transport retry of that envelope retains its `id` for broker deduplication; `id` does not identify the logical task, and repeated terminal publications may have distinct envelope IDs.
- `task_id` is the UUIDv4 identity of one logical task and remains stable across delivery attempts and semantic retries of that task. Correlated UUIDv4 values (`task_id`, `context_id`, and `payload.parent_task_id`) use canonical lowercase text; validation rejects case aliases rather than normalizing them.
- `context_id` groups the root request and its descendants. Existing direct `command` and `result` producers may omit it; their validation projection uses `context_id = task_id`.
- `hop_count` is zero for a direct request. Existing direct `command` and `result` producers may omit it; their validation projection uses `hop_count = 0`.
- A `command` is direct: its projected `hop_count` is zero and it has no parent. A child receives a `delegation` with a fresh lowercase UUIDv4 `task_id`, preserves its parent's `context_id`, sets `payload.parent_task_id` to the parent task's UUIDv4, and increments `hop_count` by one. Delegations and delegated results must carry all three fields explicitly. A result is delegated when it has `payload.parent_task_id` or a positive `hop_count`; an explicit `context_id` alone remains compatible with a direct result.
- A direct `cancel` receives the same projection defaults (`context_id = task_id`, `hop_count = 0`) but remains a policy event rather than an executable request.

The task-aware injected executor requires a worker to accept an executable request only when `recipient_id` equals its configured agent ID. Every canonical worker terminal reverses the request direction: terminal `sender_id` is the delivery worker, terminal `recipient_id` is the request sender, and `task_id`, normalized `context_id`, and normalized `hop_count` preserve the request correlation. A delegated terminal also preserves `payload.parent_task_id`. The compatibility `PullConsumer(handler=...)` path remains unchanged: its direct results are base-compatible, may carry only an optional `context_id`, and still omit `hop_count` and `payload.parent_task_id`.

The canonical request fingerprint is SHA-256 over canonical JSON containing exactly the seven projection fields listed above. Mapping keys are sorted, whitespace is omitted, UTF-8 is preserved, and non-finite numbers are rejected. Wire metadata such as `id`, `timestamp`, and `task_state` is excluded. Fingerprints apply only to executable `command` and `delegation` requests. A `cancel` resolves the existing task record through cancellation policy instead of creating a changed request fingerprint. `task.progress` retains base-envelope validation, is correlated by `task_id` at the observer, and is never request-fingerprinted. Experiment run IDs and trial IDs are harness metadata; they are not envelope correlation fields or logical task identity.

Only `completed`, `failed`, `canceled`, and `rejected` are terminal task states. One logical terminal outcome is identified by `(sender_id, recipient_id, task_id, request_fingerprint, terminal_state, canonical_terminal_payload_hash)`. Repeats with the same logical identity and content are idempotent even when envelope IDs, publication attempts, or wire deliveries differ. The worker outcome key `(worker_agent_id, task_id)` owns one request sender and fingerprint; a second sender or different fingerprint for that key is rejected with `task_state: rejected` and `payload.error: "task_id_collision"`. A later terminal with a different state or payload hash is a contract violation, not another successful outcome.

The aggregator audit mirror uses `messages.id` as its idempotency key. A replay
increments `duplicate_count` and does not add a visible row; this is mirror
replay metadata, not a broker delivery count. `observation_index` is SQLite
insertion order and is the only dashboard task-state ordering input. Envelope
`timestamp` remains display metadata.

### Task-aware executor ordering

`adapters/_common/task_executor.py` owns the transport-neutral task decision. An injected receiver passes the original delivery bytes to it and performs no acknowledgement or result classification of its own. The executor applies these eight stages:

1. Require the delivery's worker identity to match the configured executor before decoding or ledger access.
2. Strictly decode UTF-8 JSON, reject duplicate keys and non-finite values, validate the base envelope, apply detached direct-correlation defaults, or terminate the delivery as poison.
3. Fingerprint executable requests and emit their request-attempt record, enforce recipient binding, and route cancellation through policy without fingerprint or ledger mutation.
4. Look up `(worker_agent_id, task_id)`; register and reuse a matching cached outcome, or publish a fresh non-ledger collision rejection without exposing the cached payload.
5. On a miss, evaluate execution policy, run an accepted handler once, and convert an ordinary handler failure into a deterministic terminal.
6. Canonicalize and prepare one stable terminal envelope and payload hash, resolving a prepare race to the persisted winner or collision.
7. Emit the final ledger decision, publish through the injected `TerminalPublisher`, validate the current receipt, and mark accepted publication when the ledger is enabled.
8. Commit the inbound delivery only after accepted terminal publication and the applicable publication mark.

A semantic retry may carry a new wire `id` after the broker duplicate window expires. If its normalized sender and request fingerprint match, it registers a new request attempt and republishes the original terminal `id` without executing the handler again. Recipient mismatch, collision, and cancellation outcomes are intentionally non-ledger paths. Cancellation acceptance and knowledge of an already observed terminal belong to the injected policy; the executor does not claim durable cancellation state.

Terminal and progress routing are injected separately. `TerminalPublisher` carries canonical terminal results, while `ProgressPublisher` carries `task.progress` with the bound task, context, hop, sender, and recipient correlation. This prevents an all-durable experiment from accidentally using the compatibility path's plain-NATS progress helper.

`DisabledOutcomeStore` is reserved for the declared EdgeCitadel experimental ablations. It still canonicalizes a prepared result, but it retains nothing, skips publication marking, and intentionally re-executes a redelivery. Primary-mode configuration legality is enforced by the experiment harness, not by the executor interface.

The crash hooks have deliberately scoped applicability. For the publication hooks, "accepted" means the boundary declared by the injected transport: relay terminal acceptance for Central relay, plain Core send/flush for Core-only, and JetStream PubAck for EdgeCitadel and All-durable.

| Crash point | Executor path condition | Central relay | Core-only | EdgeCitadel | All-durable |
|---|---|---|---|---|---|
| `after-receive-before-handler` | Accepted executable ledger miss, immediately before handler entry. | Applies. | Applies. | Applies. | Applies. |
| `after-side-effect-before-ledger-prepare` | Deterministic fixture handler only; the executor cannot infer an external side effect. | Applies. | Applies. | Applies. | Applies. |
| `during-handler-exception-conversion` | After an ordinary handler exception, before terminal UUID/time generation. Control-flow `BaseException` values escape. | Applies. | Applies. | Applies. | Applies. |
| `after-ledger-prepare-before-result-publish` | Executable outcomes after enabled or disabled preparation; not cancellation, collision, or recipient mismatch. | Applies. | Applies. | Applies. | Applies. |
| `after-result-publish-before-publish-mark` | Every accepted terminal publication, including non-ledger outcomes. | Applies. | Applies. | Applies. | Applies. |
| `after-publish-mark-before-inbound-commit` | Enabled outcomes after marking, and the equivalent disabled-ledger stage; not non-ledger outcomes. | Applies before relay HTTP return. | Transport-inapplicable: plain Core has no inbound-finalization acknowledgement boundary. | Applies before JetStream inbound ACK. | Applies before JetStream inbound ACK. |

These boundaries do not imply general exactly-once side effects. The artifact serializes first execution with one worker and `max_ack_pending=1`; `TaskExecutor` does not coordinate simultaneous first executions. An external side effect can still occur before ledger preparation and repeat after a crash, so application-level idempotency keys remain necessary.

---

## Publisher semantics

The following subject rules describe the production compatibility split plane. Benchmark receivers inject terminal and progress publishers selected by the active transport mode; they do not inherit the compatibility outbox mirror implicitly. In the all-durable mode, injected progress publication awaits a JetStream acknowledgement. Other benchmark modes use the acceptance and durability semantics declared by their transport implementation.

### JetStream publishes (`agents.{recipient}.inbox`)

- MUST set the `Nats-Msg-Id: <envelope.id>` header on every publish. The broker's 5-minute `duplicate_window` uses this header to dedupe redelivered messages.
- Compatibility production adapters MUST mirror to the publisher's own outbox: every successful `js.publish("agents.{recipient}.inbox", env, ...)` pairs with `nc.publish("agents.{self}.outbox", env)` on plain NATS. This is enforced by the shared adapter wrapper in `adapters/_common/pull_consumer.py` — compatibility handlers do not call `js.publish` directly. See [ADR-0006](adr/0006-outbox-mirror-authoritative.md) for why the mirror lives on plain NATS rather than JetStream.

### Plain NATS publishes (everything else)

- On the production compatibility split plane, `heartbeat`, `status`, `log`, `broadcast`, `task.progress`, and the outbox mirror itself do **not** require `Nats-Msg-Id`. They are ephemeral; the broker does not store them and dedup does not apply.
- Publish-and-forget: `await nc.publish(subject, json.dumps(env).encode())`.

---

## Stream-full backpressure

`discard: new` means the broker rejects publishes once the stream reaches `max_bytes`; old messages are not evicted to make room. The publisher sees a publish error rather than silent loss.

Recommended client behavior:

- **`command` / `result` / `delegation` / `cancel`** — log the publish error and retry with exponential backoff. The envelope's `id` is stable across retries, so the dedup window absorbs duplicates.
- **`heartbeat` / `status` / `log` / `task.progress` / `broadcast`** — on the production compatibility split plane these go over plain NATS, so stream-full backpressure does not apply. Injected benchmark publishers follow their selected mode; notably, all-durable progress can encounter JetStream backpressure.

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

## Phase 4 surfaces

**Phase 4 binding location** (per ADR-0010 and ADR-0011): the A2A HTTP+SSE binding (reserved in ADR-0003) is implemented by the external A2A ingress gateway (`a2a-gateway/`, Phase 4.4) — purely an external-edge surface. In-fleet delegation does not traverse A2A HTTP; it uses the NATS-native `delegation` envelope on `agents.{recipient}.inbox` via the shared L2 helpers at `adapters/_common/l2_orchestrator.py`. Tool exposure to MCP-aware clients (Claude Desktop, Cursor, Hermes, AG2's MCPToolkit) is provided by the MCP server (`mcp-server/`, Phase 4.3, agent `mcp-1`).

---

## References

- [`docs/agent-contract.md`](agent-contract.md) — the v0.1 wire contract (envelope, lifecycle, Agent Card).
- [`schemas/envelope.v1.json`](../schemas/envelope.v1.json) — strict JSON Schema for envelopes.
- [`schemas/task-correlation.v1.json`](../schemas/task-correlation.v1.json) — strict logical task-correlation projection.
- [`schemas/agent-card.v1.json`](../schemas/agent-card.v1.json) — A2A v1.0 Agent Card schema.
- [ADR-0002](adr/0002-nats-jetstream-workqueue.md) — JetStream WorkQueue choice and consumer config rationale.
- [ADR-0006](adr/0006-outbox-mirror-authoritative.md) — outbox mirror on plain NATS as authoritative for dashboard views.
- [`aggregator/jetstream_bootstrap.py`](../aggregator/jetstream_bootstrap.py) — `ensure_stream` / `ensure_consumer` source.
- [`docs/superpowers/specs/2026-04-23-agent-messaging-design.md`](superpowers/specs/2026-04-23-agent-messaging-design.md) — full design spec.
