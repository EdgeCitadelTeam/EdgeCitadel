# Agent Messaging Design — EdgeCitadel v0.1

Status: design-complete, pending user review
Date: 2026-04-23 (rev 5)
Branch: `feat/agent-contract-v0.1`
Author: collaborative brainstorm (see `~/workplace/edge-research-notes/agent-contract-execution-plan.md` for the prior execution plan this spec supersedes)
Target spec: `docs/agent-contract.md`

Revision history:
- rev 1 (initial): three-layer design, migration-compatible
- rev 2 (clean rebuild): drop migration, wipe DB, canonical names only
- rev 3 (scenarios): subject inventory, lifecycle, error flows, cancel, ack semantics
- rev 4 (durability): Nats-Msg-Id dedup, stream backpressure, payload shape, structured args
- rev 5 (integration): aggregator durable intake, register validation, adapter layout,
  retirement flow, schema provenance, v0.2 roadmap consolidation

## Summary

v0.1 is a **clean rebuild** of EdgeCitadel's messaging layer. No backward compatibility
with the mid-2024-era MQTT+slash-topic codebase. Canonical field names from day one.
Strict envelope validation. SQLite DB is wiped and recreated on first boot. The design
has three layers:

- **Transport:** NATS JetStream WorkQueue, one stream over `agents.*.inbox`, one durable pull
  consumer per agent with `max_ack_pending=1`. Server-enforced sequential processing per agent.
- **Semantic:** A2A v1.0 task lifecycle vocabulary on every envelope (`task_id`, `context_id`,
  `task_state`, `hop_count`). A2A v1.0 Agent Card shape for `register`.
- **Adapter:** nats-py async pull-consumer loop for Phase 2–3 (shell, Gemma, watchdog). For
  Phase 4, wrap each AG2 agent in `A2aAgentServer` for HTTP+SSE streaming, bridged to NATS.

openclaw-client is rewritten in nats.js with canonical field names only. Shell adapter is
rewritten in nats-py async and supersedes `273e80a`. MQTT block is removed from `nats.conf`
on day one.

## Problem

The existing mid-2024-era codebase has three problems this spec resolves together:

1. **Undefined busy-agent semantics.** When A sends a message to B while B is executing:
   the shell adapter has a dead `error: busy` branch; paho's single-threaded callback
   incidentally serializes; any nats-py adapter (AG2, watchdog) would process
   concurrently by default. HiveMind ([arXiv 2604.17111](https://arxiv.org/html/2604.17111))
   documents 72–100% failure rate for uncoordinated LLM agents under contention.
2. **Heterogeneous transport and field names.** MQTT + slash-topics in clients; NATS + dots
   in aggregator. Aliases like `from`/`to`/`receiver_id`/`message_type` everywhere.
3. **No durability.** Adapter crash = in-flight work lost; no observability into queue depth;
   no poison-message detection.

Rather than layer a migration on top, v0.1 rebuilds the messaging layer from scratch with
a coherent transport (NATS+JetStream only), a coherent vocabulary (A2A v1.0 on every
envelope), and strict validation. Nothing in production depends on the legacy shapes.

## Goals

1. One message in flight per `agent_id` at any time, enforced by the broker.
2. Messages survive adapter crashes: redeliver on `ack_wait` expiry, cap at `max_deliver`.
3. Poison messages are observable via JetStream advisories, not silent.
4. Queue depth per agent is observable from the aggregator.
5. Envelope vocabulary is compatible with the A2A v1.0 task lifecycle so future external
   agents can interop without envelope translation.
6. AG2 v0.12 adapter works end-to-end for L2 conformance (delegation + `context_id` +
   `hop_count` loop protection).
7. openclaw-client uses NATS natively; no MQTT bridge in the primary path.

## Non-goals (v0.1)

- JetStream clustering (single-node broker is fine until a second persistent host joins).
- Per-agent JWT auth (parked; shared `NATS_TOKEN` for v0.1).
- Token-streaming over NATS (SSE over HTTP via A2A in Phase 4 instead).
- MQTT 5.0 features (blocked on NATS's adapter being 3.1.1 only; not worth a second broker).
- Horizontal scale of a single `agent_id` across worker replicas (achievable by relaxing
  `max_ack_pending`; not a v0.1 requirement).
- P2P transport (Zenoh) — v0.3+ consideration.

## Design

### Transport layer: JetStream WorkQueue

Single stream covers all agent inboxes. One durable pull consumer per agent enforces the
one-at-a-time property server-side.

```
stream AGENT_INBOX:
  subjects:         ["agents.*.inbox"]
  retention:        WorkQueuePolicy
  storage:          file
  max_age:          24h
  max_bytes:        1GB           # prevent unbounded growth on aggregator node
  max_msg_size:     1MB           # per-message cap; LLM tokens stream over SSE
  discard:          new           # reject new publishes when full (do NOT drop queued work)
  duplicate_window: 5m            # server-side dedup via Nats-Msg-Id header
```

Publishers MUST set the `Nats-Msg-Id` header on every JetStream publish to the envelope's
`id` field. JetStream deduplicates within `duplicate_window`, so a message whose publish
client retries (network flap) or whose adapter crashes-before-ack does not execute twice.

Per-agent consumer (created at agent startup):

```
consumer {agent_id}_inbox:
  durable_name:    "{agent_id}_inbox"
  filter_subject:  "agents.{agent_id}.inbox"
  ack_policy:      explicit
  max_ack_pending: 1       # THE serialization guarantee
  ack_wait:        300s    # tune per agent_card.heartbeat_interval * N
  max_deliver:     3
```

Adapter loop (pseudocode). The contract: `handle_envelope` MUST complete the full task
(including publishing the `result` envelope) before returning. Ack happens only after
a successful return. For long tasks, `handle_envelope` periodically calls
`msg.in_progress()` to extend `ack_wait`.

```python
js = await nc.jetstream()
sub = await js.pull_subscribe(
    subject=f"agents.{AGENT_ID}.inbox",
    durable=f"{AGENT_ID}_inbox",
    config=ConsumerConfig(max_ack_pending=1, ack_wait=300, max_deliver=3),
)
while not shutdown:
    msgs = await sub.fetch(batch=1, timeout=30)
    for msg in msgs:
        env = json.loads(msg.data)
        try:
            # handle_envelope must:
            #   1. process the command/delegation/cancel
            #   2. publish the result envelope to the sender's inbox
            #   3. call msg.in_progress() every (ack_wait/3)s if work is long
            #   4. return only when done
            await handle_envelope(env, msg)
            await msg.ack()
        except TransientError:
            await msg.nak()        # redeliver after backoff
        except FatalError:
            await msg.term()       # poison — move to DLQ via advisory
```

Advisory subscriber (aggregator-side, informational):

```
$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>
```

Logged as poison-message events in the dashboard's agent registry view. No separate DLQ
stream needed in v0.1.

**Delegation ack semantics.** When an adapter receives a `command` or `delegation`, it
does NOT ack immediately. Ack happens only after the adapter publishes the final
`result` envelope. Rationale: `max_ack_pending=1` only provides serial-execution
guarantees if the message stays unacked for the whole task. Otherwise it becomes
at-most-one-delivery, not serial.

Adapters MUST call `msg.in_progress()` periodically to extend `ack_wait` for tasks
longer than the configured `ack_wait`. Recommended cadence: every `ack_wait / 3`
seconds. Per-adapter `ack_wait` defaults:

| Adapter            | `ack_wait` | Notes                                                |
|--------------------|------------|------------------------------------------------------|
| shell              | 30s        | Subprocess timeout bounds it                         |
| gemma / LLM        | 300s       | Ollama latency; `in_progress` ticks every 100s       |
| AG2 (with delegation) | 600s    | Chain latency; `in_progress` ticks every 200s        |
| watchdog           | n/a        | Does not hold inbox consumer                         |

**Aggregator observation.** The aggregator observes fleet traffic via **plain NATS
subscriptions**, not JetStream consumers. Plain subscribers see every message published
on `agents.>`, `tasks.>`, and `system.>` regardless of whether that message was also
persisted to JetStream for durable delivery. JetStream's `AGENT_INBOX` handles
routing to the recipient; the aggregator's plain subscriber handles audit/dashboard.
The two do not conflict: WorkQueue retention removes messages from the stream on ack,
but the original NATS publish already fanned out to plain subscribers.

### Semantic layer: A2A lifecycle vocabulary

Canonical envelope fields for v0.1. Strict: schema rejects unknown fields at the top level;
deprecated names (`receiver_id`, `message_type`, `content`, `from`, `to`) are not accepted.

| Field         | Required for              | Semantics                                    |
|---------------|---------------------------|----------------------------------------------|
| `v`           | all                       | Envelope version integer. v0.1 = `1`.        |
| `id`          | all                       | UUID4 per message.                           |
| `type`        | all                       | One of `register`, `heartbeat`, `status`, `command`, `result`, `delegation`, `cancel`, `log`, `broadcast`, `task.progress`. |
| `sender_id`   | all                       | Publisher's agent ID.                        |
| `recipient_id`| `command`, `result`, `delegation`, `cancel` | Addressee's agent ID.       |
| `timestamp`   | all                       | ISO 8601 UTC with ms precision, `Z` suffix.  |
| `task_id`     | `command`, `result`, `delegation`, `cancel`, `task.progress` | A2A task ID (UUID4). Generated by the originator of the task; echoed unchanged by all responders. New `task_id` for each delegation hop (child task ≠ parent task); use `context_id` to group them. |
| `context_id`  | `delegation` (required); `result`, `task.progress`, `cancel` (SHOULD propagate if the task is part of a chain) | A2A context ID; shared across a delegation chain. |
| `task_state`  | `result` (required), `task.progress` (required), `status` (optional) | A2A task state enum (see below). |
| `hop_count`   | `delegation` (required)   | Integer, 0 at root, +1 per hop; refuse at ≥8. |
| `payload`     | all                       | Type-specific body. Always an object. See payload shape below. |

**Payload shape.** `payload` is a type-agnostic object. Conventional fields:

| Field           | Present in                     | Semantics                                           |
|-----------------|--------------------------------|-----------------------------------------------------|
| `body`          | `command`, `result`, `delegation`, `broadcast`, `log` | Human-readable text (prompt, reply, log message). |
| `args`          | `command`, `delegation` (optional) | Structured arguments object, agent-specific schema. |
| `error`         | `result` (when `task_state: failed` or `rejected`) | Short machine-readable error tag.         |
| `reason`        | `cancel`, `status` (optional)  | Human-readable reason string.                       |
| `progress`      | `task.progress` (optional)     | Integer 0–100 for known-fraction work.              |
| `message`       | `task.progress` (optional)     | Human-readable progress description.                |
| `task_id`       | `cancel` (required)            | The task_id to cancel. Echoed in top-level `task_id` also for routing. |

Unknown payload fields are preserved through the aggregator DB but not interpreted.
Adapters MAY define additional fields within their own `args` object.

`task_state` enum (A2A v1.0):

```
submitted | working | input-required | completed | failed | canceled | rejected | auth-required
```

Terminal states: `completed`, `failed`, `canceled`, `rejected`. Intermediate progress uses
`type: task.progress` with `task_state: working` (or `input-required`, `auth-required`).
Failure details go in `payload.error`.

### Agent Card and A2A extensions

The `register` envelope payload is the full A2A v1.0 Agent Card JSON. Our legacy
`schemas/agent-card.v1.json` is superseded; v0.1 preserves EdgeCitadel-specific fields
(`runtime.roles`, `tags`, `deployment`) under the Agent Card's free-form `metadata` map.

**Mandatory A2A fields** (per v1.0 spec): `name`, `description`, `version`, `url`,
`provider`, `capabilities`, `securitySchemes`.

**Optional but required by our convention**: `skills[]`, `defaultInputModes`,
`defaultOutputModes`, `additionalInterfaces`, `capabilities.extensions`, `metadata`.

**NATS transport binding as an A2A profile extension.** EdgeCitadel mints the URI
`https://edgecitadel.local/ext/nats-binding/v1` and declares it on every Agent Card:

```json
"capabilities": {
  "streaming": true,
  "extensions": [{
    "uri": "https://edgecitadel.local/ext/nats-binding/v1",
    "description": "NATS JetStream transport binding for EdgeCitadel fleet.",
    "required": false,
    "params": {"subject_prefix": "agents.{agent_id}"}
  }]
},
"additionalInterfaces": [
  {"url": "nats://edgecitadel/agents.{agent_id}.inbox", "transport": "nats-jsonrpc"}
]
```

This is a legitimate use of A2A's documented extension mechanism, not a deviation —
A2A v1.0 explicitly supports custom bindings via `additionalInterfaces` and custom
capabilities via extension URIs.

**EdgeCitadel metadata vocabulary** (all under Agent Card `metadata` map):

| Key                  | Required | Values |
|----------------------|----------|--------|
| `runtime.roles`      | yes      | Array of role strings: `worker`, `reasoner`, `watchdog`, `orchestrator` |
| `runtime.tags`       | no       | Free-form tags (model family, deployment, capabilities) |
| `runtime.deployment` | no       | Deployment identifier, e.g., `mac-mini-studio-01` |
| `runtime.kind`       | yes      | `native` (speaks A2A/NATS directly) or `bridge` (facade for an external runtime) |
| `runtime.upstream`   | required if `runtime.kind = bridge` | Identifier of the upstream runtime, e.g., `nous-hermes-agent`, `openclaw-legacy-mqtt` |

`runtime.kind` and `runtime.upstream` are informational — they do not change NATS routing
but let operators and the aggregator reason about failure modes.

**Building the Agent Card.** AG2's `A2aAgentServer` auto-generates only `name` and
`description`; everything else must be supplied explicitly. A shared factory
(`adapters/_common/agent_card.py`) builds the full card from a per-agent config file and
passes it into `A2aAgentServer(agent, agent_card=card)`.

### Adapter layer

**Phase 2–3 adapter (plain NATS pull consumer):**

Thin async loop as above. One message at a time by consumer config; no in-process queue
needed. Same binary handles echo, Gemma (wrapping Ollama HTTP), and watchdog (subscribes to
`agents.*.heartbeat` instead of an inbox).

**Phase 4 adapter (AG2 + A2A wrapper):**

For AG2 agents specifically:
1. AG2's `ConversableAgent` is wrapped with `A2aAgentServer(agent, agent_card=card).build()`
   — exposes the agent as an HTTP+SSE A2A endpoint on a local port. The Agent Card is
   built by our shared factory (see previous section), not auto-generated by AG2.
2. A small bridge process runs alongside:
   - NATS pull consumer on `agents.{agent_id}.inbox` (same config as above).
   - On each message, POST to the local A2A server as `message/send`.
   - Consume SSE stream from A2A server; translate `task_state: working` → NATS
     `type: task.progress`; terminal state → NATS `type: result` with
     `task_state ∈ {completed, failed, rejected}`.
3. Both the A2A HTTP endpoint at `/.well-known/agent-card.json` AND the NATS
   `agents.{agent_id}.register` publish serve the same Agent Card JSON.

**AG2-specific constraints:**
- `A2aRemoteAgent.run()` fails; use `a_run()` exclusively. Our adapter code is async-first.
- AG2's `A2aAgentServer` in v0.12 auto-populates only `name` and `description`; everything
  else comes from our factory.
- AG2 group-chat API: use `autogen.agentchat.group` with `AutoPattern` / `RoundRobinPattern`
  and `register_hand_off(OnCondition(target=AgentTarget(...)))`. Swarm is deprecated.
- Pin: `ag2>=0.12,<0.13`. Expect breaking changes across minor bumps until v1.0.

**A2A version compatibility:** if we later upgrade A2A, set `enable_v0_3_compat=True` on the
server to keep older clients working during migration windows. Current spec target is
A2A v1.0; no v0.3 clients expected at launch.

**Loop protection:**

`context_id` carries a hop counter in envelope field `hop_count`, incremented
at each delegation. Refuse delegation at `hop_count >= 8`. This is an envelope-level
mechanism; not per-process visit-sets (which don't detect cross-process cycles).

### openclaw-client rewrite

openclaw-client is rewritten in `@nats-io/nats`. Legacy paho-mqtt code is deleted, not
wrapped. The rewrite uses:
- `connect({ servers, token: NATS_TOKEN })` for plain NATS (heartbeats, status).
- JetStream `js.publish(subject, payload)` for durable messages (commands, results,
  delegations).
- Subject form `agents.{id}.inbox` / `agents.{id}.heartbeat` / `agents.{id}.register`.
- Canonical envelope fields only — no aliases. `recipient_id`, `type`, `task_id`, etc.
- `register` publishes an A2A v1.0 Agent Card on startup; aggregator caches it.

E2E test fixtures and assertions are rewritten to match the new envelope shape. Specs
that asserted on legacy fields (`receiver_id`, `message_type`) are updated or pruned.

### MQTT removal

`mqtt { port: 1883, ... }` block is removed from `nats.conf` on v0.1 first boot. Port
1883 in docker-compose.yml is removed. No MQTT publisher or subscriber remains in the
EdgeCitadel-internal fleet.

### Shell adapter rewrite

The shell adapter is rewritten in nats-py async. Env vars `CITADEL_HOST` / `CITADEL_PORT`
are replaced by `NATS_URL`. The rewrite uses the same pull-consumer pattern as every other
v0.1 adapter. `273e80a`'s shell adapter is deleted, not ported.

### Database wipe

The existing SQLite DB at `/data/openclaw.db` is wiped on v0.1 first boot. New schema
uses canonical column names: `recipient_id` (not `receiver_id`), `type` (not
`message_type`), matching envelope field names. Indexes recreated accordingly. No
migration script; the deployment note says "expect to lose dev message history."

### Bridge pattern for external runtimes

Not every agent we onboard is an AG2 `ConversableAgent`. Some upstream runtimes — Nous
Research's Hermes Agent (ACP-native, Feb 2026), legacy openclaw-MQTT agents during
transition, any future non-A2A framework — require a translator process.

**Canonical bridge topology:**

```
[External runtime] <-- upstream protocol --> [EdgeCitadel bridge] <-- NATS JetStream --> [EdgeCitadel fleet]
```

The bridge is, from NATS's perspective, an ordinary agent:
- Owns a stable `agent_id` (e.g., `hermes-nous-01`, `openclaw-legacy-42`).
- Holds a durable pull consumer on `agents.{agent_id}.inbox` with `max_ack_pending=1`.
- Publishes its own Agent Card to `agents.{agent_id}.register` with:
  - `metadata.runtime.kind = "bridge"`
  - `metadata.runtime.upstream = "nous-hermes-agent"` (or equivalent)
  - `skills[]` reflecting what the upstream runtime actually supports

**Responsibilities of the bridge:**
1. Translate inbound A2A envelopes → upstream protocol calls (e.g., ACP `session/new`,
   ACP `session/prompt`).
2. Translate upstream lifecycle events → A2A `task_state` updates on NATS. Unmappable
   upstream states default to `failed` with a clear reason in the result payload.
3. Maintain a `{A2A task_id → upstream session_id}` map for the duration of each task.
4. Terminate trust at the bridge (v0.1): the bridge is trusted by NATS; it is the bridge's
   responsibility to authenticate to the upstream runtime out-of-band.
5. Propagate `context_id` and `hop_count` unchanged in both directions so loop protection
   remains coherent end-to-end.

**Bridge failure semantics (v0.1):** if the bridge crashes mid-task, the NATS message is
unacked and JetStream redelivers per `ack_wait`. Upstream session state may be lost; on
redelivery the bridge either resumes (if the upstream protocol supports session resume) or
marks the task `failed` with reason `bridge_restart_lost_session` so the sender can retry.
Checkpointing `{task_id → session_id}` to JetStream KV for bridge crash-recovery is v0.2.

**Applicability note:** "openclaw/hermes" in EdgeCitadel conversations most likely refers
to Nous Research's Hermes Agent (the OpenClaw alternative runtime). If instead it means
something repo-specific that a future reader understands, re-scope this section.

## Subject inventory

The complete v0.1 subject map. All use NATS dot-form (no MQTT slash-form).

| Subject                                  | Published by     | Consumed by                      | Persistence           | Envelope `type`  |
|------------------------------------------|------------------|----------------------------------|-----------------------|------------------|
| `agents.{id}.register`                   | Agent (self)     | Aggregator (plain sub)           | Plain NATS (ephemeral) + aggregator cache | `register`       |
| `agents.{id}.heartbeat`                  | Agent (self)     | Watchdog + aggregator            | Plain NATS (ephemeral) | `heartbeat`      |
| `agents.{id}.status`                     | Agent (self)     | Watchdog + aggregator            | Plain NATS (ephemeral) | `status`         |
| `agents.{id}.log`                        | Agent (self)     | Aggregator                       | Plain NATS (ephemeral) | `log`            |
| `agents.{id}.inbox`                      | Any sender       | Agent's durable consumer         | **JetStream `AGENT_INBOX`** | `command`, `result`, `delegation`, `cancel` |
| `agents.{id}.task_progress.{task_id}`    | Agent processing a task | Subscribers interested in live progress (aggregator, Phase 4 SSE bridge) | Plain NATS (ephemeral) | `task.progress`  |
| `agents.{id}.outbox`                     | Agent (self)     | Aggregator (audit)               | Plain NATS (ephemeral) | Mirror of what `{id}` is sending |
| `system.broadcast`                       | Any              | All agents                       | Plain NATS (ephemeral) | `broadcast`      |
| `tasks.{task_id}.{event}`                | Aggregator       | Dashboard (WS)                   | Plain NATS            | Task board events|
| `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>` | JetStream (internal) | Aggregator | Plain NATS            | Poison-message alerts |

**Routing rule:** only `agents.{id}.inbox` is persistent (goes through JetStream's
`AGENT_INBOX` stream). Everything else is plain NATS fire-and-forget. This keeps
JetStream's durable work cheap — heartbeats, logs, and progress updates don't need
redelivery.

**`type: log` storage.** Log envelopes published to `agents.{id}.log` are observed by
the aggregator's plain NATS subscriber and written to the `messages` DB table with
`type = 'log'`. Log retention follows the same DB retention as other message types
(see "Retention" below). Adapters that want sustained structured logging should
**also** log to their own stdout/stderr (Docker captures those) — the NATS log
subject is for coarse-grained fleet-wide observability, not full log aggregation.

**Retention.** The aggregator's SQLite `messages` table grows unbounded in v0.1.
Operators are expected to run a periodic cleanup (`DELETE FROM messages WHERE
timestamp < datetime('now', '-7 days')`) as a cron job; a built-in retention task
is v0.2 work. JetStream streams retain per their configured `max_age` / `max_bytes`.

**Sender-side publish semantics:**
- For `command` / `delegation` / `cancel` / `result`: use JetStream
  `js.publish("agents.{recipient}.inbox", env, headers={"Nats-Msg-Id": env["id"]})`.
  This gives at-least-once delivery ack + server-side dedup within `duplicate_window`.
- For everything else: plain `nc.publish(subject, env)`.
- Agents MUST publish to their own `outbox` mirror whenever they publish to another
  agent's inbox, so observers can see the exchange without scraping JetStream.

**Publish-failure retry.** If `js.publish` raises or returns an error (broker down,
stream full, no responder), sender retries with exponential backoff (recommended:
3 attempts, 100ms → 300ms → 1s). After final failure, sender either (a) surfaces the
error to its own caller (if driving an HTTP request), or (b) publishes an internal
`type: log` with level `error` so the aggregator surfaces the stuck message.

**Stream-full behavior.** `AGENT_INBOX` uses `discard: new`, so when full, `js.publish`
returns an error rather than silently dropping older queued work. The sender's retry
loop plus the operator's queue-depth observability is the backpressure story for v0.1.

## Agent lifecycle

### Register

On connect, agent publishes `type: register` to `agents.{self}.register`. Payload is
the full A2A v1.0 Agent Card. Aggregator subscribes to `agents.*.register`, validates
the payload against `schemas/agent-card.v1.json` (A2A v1.0 shape + EdgeCitadel
metadata), and caches cards in memory (in v0.1). Invalid cards are rejected with an
error log; the agent is treated as "unregistered" and commands to it will fail via
the watchdog synthesized-failure path.

**`sender_id` must equal the Agent Card's `agent_id`.** A register whose envelope
`sender_id` doesn't match its payload's declared identity is rejected — this is the
v0.1 minimum defense against impersonation before per-agent JWT (v0.2).

The dashboard reads cards from `GET /api/agents` on the aggregator.

**Late subscribers.** NATS core does not support retained messages. A client that
connects after the register publish will not see it. Resolution in v0.1:
- Aggregator's `GET /api/agents/{id}/card` and `GET /api/agents` (list) serve the
  cached Agent Card JSON for any agent it has seen.
- New clients (dashboard, other adapters) call that HTTP endpoint on startup rather
  than relying on NATS retention.
- If the aggregator itself restarts, it loses its cache. It solicits re-registration
  by publishing `type: broadcast` with `payload: {action: "request_register"}` on
  `system.broadcast`. All online agents re-publish their cards on receipt. Deadline:
  agents reply within 5s; aggregator considers any card it hasn't received by then
  "agent offline until heartbeat."

v0.2 replaces this with a JetStream KV bucket (`AGENT_CARDS`, key = `agent_id`) so
card discovery is durable and aggregator-crash-safe without the re-registration
dance.

### Heartbeat

Agents publish `type: heartbeat` to `agents.{self}.heartbeat` at an interval declared
in their Agent Card under `metadata["runtime.heartbeat_interval_sec"]`. Default 30s.
Allowed range: 10s–300s. The watchdog clamps out-of-range values to this interval
before computing the 3× offline threshold.

Payload: `{cpu_percent?, memory_percent?, power_source?, battery_percent?}` — all
optional, all informational.

### Status

Agents publish `type: status` to `agents.{self}.status` when state changes:
`{task_state: online | offline | busy | error, reason?}`. The watchdog or operator
may also publish status-of-others using the watchdog's own `sender_id` (never
impersonate — see watchdog rule).

Final message before shutdown SHOULD be `{task_state: offline, reason: "shutdown"}`.

### Restart / re-registration

Durable consumers are named `{agent_id}_inbox` and MUST be created idempotently
(create-if-not-exists). On restart, an agent reuses its existing durable consumer;
any unacked messages in-flight when it crashed will redeliver on the first fetch.
JetStream `duplicate_window` ensures a redelivered-and-reprocessed message is
deduplicated by `Nats-Msg-Id` if the adapter re-publishes the same result within
5 minutes.

On every restart (including clean shutdown and restart), the agent:
1. Connects to NATS, acquires or reuses its durable consumer.
2. Re-publishes its Agent Card on `agents.{self}.register`.
3. Publishes `status: online`.
4. Begins the fetch loop.

### Graceful shutdown

On SIGTERM or SIGINT:
1. Stop fetching new messages from the consumer (break the loop after current fetch
   returns).
2. If a task is in-flight, finish it: publish `result`, ack the inbox message, then
   shut down. Bounded by `ack_wait`; if the task exceeds the timeout the shutdown
   proceeds anyway and the next run redelivers.
3. Publish `status: offline, payload.reason: "shutdown"`.
4. Disconnect. Durable consumer persists in JetStream for next startup.

### Multi-instance same `agent_id`

Running two processes with the same `AGENT_ID` is discouraged but not blocked at the
transport layer. Both processes bind to the same durable consumer; JetStream delivers
each message to exactly one of them (work-sharing). Heartbeats and register publishes
are last-write-wins at the aggregator cache. Operators who want hot-standby should
accept that the aggregator's dashboard will flicker between the two processes'
heartbeats.

For v0.1, document: "one `agent_id` = one process." Multi-instance / hot-standby is
not a supported configuration.

### Retiring an agent

When an agent is permanently removed from the fleet:
1. Stop the process.
2. Operator runs `nats consumer rm AGENT_INBOX {agent_id}_inbox` to free JetStream
   quota.
3. Operator runs `curl -X DELETE /api/agents/{agent_id}` on the aggregator to drop
   the cached card.
4. Any messages still pending for that consumer are orphaned in the stream until
   `max_age` expires (default 24h).

Automated retirement (triggered by watchdog after N days offline) is v0.2.

## Error and timeout flows

### Recipient offline

If A publishes `command` to `agents.B.inbox` and B is offline, the message sits in
`AGENT_INBOX` until:
- B reconnects and consumes it, OR
- `ack_wait` expires `max_deliver` times (default 3 × 300s = 15min) and the message
  is terminated via the MAX_DELIVERIES advisory.

**Synthesized failure.** To prevent A from waiting forever, the watchdog publishes a
`type: result` envelope (with its own `sender_id: watchdog-1` and `task_id` matching
A's original) carrying `task_state: failed, payload.error: "recipient_offline"` when
either:
- The recipient has been `offline` in the watchdog's view for > (N × heartbeat_interval)
  at the time of the publish (watchdog notices at publish time via its own NATS trace), or
- A MAX_DELIVERIES advisory fires for a message addressed to an offline recipient.

The watchdog's synthesized result is published on `agents.{A}.inbox` the same way a
real result would be. A consumes it and knows B couldn't be reached.

### Cancellation

A2A v1.0 supports `tasks/cancel`. EdgeCitadel models this as `type: cancel` on
`agents.{B}.inbox` with `payload: {task_id: <id-to-cancel>, reason?: string}`. The
envelope is delivered through the same JetStream path. On receipt:
- If B is still working on `task_id`, B attempts a best-effort cancel (e.g.,
  interrupt the Ollama request, kill the subprocess, set a cancel flag in the AG2
  agent's state).
- B publishes `type: result` with `task_id: <id-to-cancel>, task_state: canceled,
  payload.reason: <reason>`.
- If B has already completed the task, the cancel is a no-op; B publishes a
  `task_state: rejected` result with reason `"already_completed"`.

Cancel is best-effort. v0.1 does not guarantee cancellation takes effect before
partial side-effects (e.g., subprocess writes).

### Chain-level cancellation

If A delegates `task_id: T` to B, and B delegates sub-task `task_id: T2` (same
`context_id`), cancelling `T` at A does NOT automatically cancel `T2`. The adapter
that is handling T is responsible for propagating cancel to its child delegations
if it wants to. v0.1 documents this behavior; does not enforce it.

## Aggregator's fleet identity

The aggregator participates in the fleet as a real agent:
- `agent_id: aggregator` (reserved name, not reassignable)
- Publishes its own Agent Card on startup at `agents.aggregator.register`
- `metadata.runtime.roles: ["aggregator"]`, `runtime.kind: "native"`
- Holds a durable JetStream consumer on `agents.aggregator.inbox` named
  `aggregator_inbox` with `max_ack_pending: 100` (no serial constraint — aggregator
  only correlates and writes to DB / pushes to WS; it doesn't "execute tasks").
  This is required so that `result` envelopes returned to the aggregator (when it
  is the `sender_id` of an original HTTP-driven command) are delivered durably even
  if the aggregator restarts.
- Plain NATS subscribers on `agents.>`, `tasks.>`, `system.>` for dashboard audit;
  these see every publish regardless of JetStream persistence.
- When the aggregator publishes commands to other agents on behalf of an HTTP caller,
  it uses `sender_id: aggregator` — it does NOT impersonate the caller. The HTTP
  caller's identity lives in `payload.on_behalf_of` if present.
- Aggregator excludes its own `sender_id` from the cards cache when it sees its own
  `register` publish reflected back, so `GET /api/agents` lists peers only.
- `task_id` generation: when an HTTP caller POSTs a command, the aggregator generates
  a new UUID4 as `task_id` and returns it in the HTTP response so the caller can
  poll / stream on it.

## Message size and schema evolution

**Max message size.** JetStream stream `AGENT_INBOX` is declared with
`max_msg_size: 1MB`. Sufficient for LLM replies up to ~250k tokens of JSON overhead.
Token-level streaming goes over the A2A SSE path (Phase 4), not through JetStream.
Any adapter producing results larger than 1MB MUST chunk at the application level;
no v0.1 adapter does.

**Schema version pinning.** `envelope.v == 1` is required. v0.1 agents reject any
envelope with `v != 1` at the validator. Schema evolution to `v: 2` requires a
lockstep upgrade of all publishers and consumers; there is no dual-version support
in v0.1. This is intentional — v0.1 is a clean rebuild and the fleet is small enough
to upgrade atomically.

**Schema provenance.** Both `schemas/envelope.v1.json` and `schemas/agent-card.v1.json`
are **vendored** copies. The Agent Card schema is derived from A2A v1.0 with
EdgeCitadel's metadata vocabulary spelled out explicitly. `scripts/update-a2a-schema.sh`
(to be written in Session 1.2) pulls the latest A2A schema, diffs against our
vendored copy, and surfaces the diff for human review. We do not fetch A2A schemas at
runtime — supply chain and reproducibility require vendoring.

**Outbound envelope validation.** Adapters MUST validate envelopes against
`schemas/envelope.v1.json` before publishing. This is not cosmetic: the aggregator's
inbound validator will drop malformed envelopes silently (to preserve its own liveness
under attack), so an adapter that publishes garbage will see its messages vanish
without obvious cause. The `adapters/_common/pull_consumer.py` wraps every publish
helper with validation.

## Verification

Per session, minimum verification:

1. **JetStream stream + consumer live:** `nats stream info AGENT_INBOX` shows the stream;
   `nats consumer info AGENT_INBOX {agent_id}_inbox` shows `num_pending` and `num_ack_pending`.
2. **Sequential processing:** send two commands in quick succession to one agent; confirm
   consumer reports one pending while the first is processing.
3. **Crash recovery:** kill adapter mid-task; confirm the unacked message redelivers on
   restart.
4. **Poison detection:** send a command that deterministically fails 3x; confirm advisory
   on `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>` and dashboard surfaces it.
5. **Queue depth observable:** new aggregator endpoint `/api/agents/{id}/queue` returning
   `{pending, ack_pending}` from JetStream; dashboard displays.
6. **Strict envelope validation:** publishing a legacy-shape message (e.g., with
   `receiver_id` instead of `recipient_id`) is rejected by the validator and dropped; the
   event logs the reason. No silent acceptance.
7. **openclaw-client:** rewritten Playwright specs pass end-to-end against the new envelope.
8. **Fresh DB schema:** `sqlite3 openclaw.db ".schema messages"` shows canonical column
   names (`recipient_id`, `type`); no `receiver_id` / `message_type` columns remain.
9. **AG2 adapter (Phase 4):** planner→worker delegation with shared `context_id`, visible
   chain in dashboard, loop protection refuses at `hop_count=8`.
10. **Cancel round-trip:** A sends `command` to B (long-running); A sends `cancel` with
    the same `task_id`; B publishes `result` with `task_state: canceled` within a
    bounded window (best-effort).
11. **Recipient-offline synthesized failure:** publish a `command` to an unregistered
    `agent_id`; after `ack_wait * max_deliver` (15min default) the watchdog publishes
    a `task_state: failed, payload.error: "recipient_offline"` result; A observes it.
12. **Aggregator restart discovery:** restart aggregator with 2 agents online; confirm
    `GET /api/agents` lists both within 10s via the `request_register` broadcast.
13. **Subject inventory end-to-end:** one E2E spec exercises every subject in the
    Subject Inventory table at least once; aggregator DB rows include every
    `type` enum value.
14. **Idempotency on redelivery:** publish the same `command` twice with the same
    `Nats-Msg-Id` within 5 minutes; confirm JetStream delivers it to the consumer
    only once. Kill adapter after it publishes result but before ack; on redelivery,
    confirm the adapter either dedups at the application layer or publishes a
    duplicate result which JetStream's duplicate_window discards.
15. **Stream-full backpressure:** publish to `AGENT_INBOX` until `max_bytes`; confirm
    next `js.publish` returns an error (not silent drop).
16. **Aggregator durable intake:** stop aggregator, have an agent publish a `result`
    to `agents.aggregator.inbox`, restart aggregator; confirm the result is
    processed (written to DB) on startup, not lost.
17. **`task_id` ownership:** HTTP POST `/api/command/<agent_id>` returns a task_id
    in the response body; subsequent results from the agent echo that exact task_id.
18. **Register payload validation:** publish a register envelope whose `sender_id`
    differs from the payload Agent Card's `agent_id`, or whose card violates the
    schema; confirm the aggregator rejects and does NOT cache; error logged.
19. **Outbound envelope validation:** an adapter that attempts to publish an invalid
    envelope (e.g., missing `task_id` on a `result`) is blocked by the
    `_common/pull_consumer` wrapper with a clear error, not silently sent and dropped.
20. **Retirement cleanup:** after `nats consumer rm` + `DELETE /api/agents/{id}`,
    the agent is gone from `nats consumer ls AGENT_INBOX` and `GET /api/agents`.

## Supported features matrix

| Feature                                      | v0.1 | Notes |
|----------------------------------------------|------|-------|
| Per-agent FIFO enforced by broker            | Yes  | `max_ack_pending=1` |
| Crash-survival / redelivery                  | Yes  | `ack_wait` + `max_deliver` |
| Poison-message detection                     | Yes  | JetStream advisories |
| Queue-depth observability                    | Yes  | `nats consumer info` + `/api/agents/{id}/queue` |
| A2A v1.0 task lifecycle vocabulary           | Yes  | Canonical envelope fields |
| A2A v1.0 Agent Card in `register`            | Yes  | Full card shape; legacy fields in `metadata` |
| AG2 L2 delegation + loop protection          | Yes  | `hop_count` in envelope |
| LLM token streaming                          | Yes (Phase 4) | A2A SSE; not over NATS |
| A2A extension: NATS transport binding        | Yes  | `https://edgecitadel.local/ext/nats-binding/v1` profile extension |
| Bridge pattern for non-A2A runtimes          | Yes  | Bridges declare `runtime.kind=bridge` |
| NATS-native client for all v0.1 agents       | Yes  | openclaw-client rewritten; shell adapter rewritten |
| Strict envelope validation                   | Yes  | Non-conformant messages dropped |
| Cancellation (best-effort)                   | Yes  | `type: cancel`, terminal `task_state: canceled` |
| Synthesized failure for offline recipient    | Yes  | Watchdog publishes `task_state: failed, payload.error: recipient_offline` |
| Intermediate progress events                 | Yes  | `type: task.progress` on dedicated subject |
| Agent Card discovery after aggregator restart | Yes | `request_register` broadcast; v0.2 replaces with JetStream KV |
| Server-side dedup on redelivery              | Yes  | `Nats-Msg-Id` header + `duplicate_window: 5m` |
| Graceful shutdown (offline status + drain)   | Yes  | Adapter lifecycle contract |
| Structured command arguments                 | Yes  | `payload.args` object |
| Stream-full backpressure                     | Yes  | `discard: new`; publish returns error |
| Per-agent JWT auth                           | No   | v0.2; shared `NATS_TOKEN` for v0.1 |
| Multi-worker per `agent_id` (horizontal)     | No   | v0.2; relax `max_ack_pending` |
| JetStream clustering                         | No   | When 2nd persistent node exists; [#7817](https://github.com/nats-io/nats-server/issues/7817) gotcha to resolve first |
| MQTT (any version)                           | No   | Removed entirely; no bridge retained |
| Native A2A wire protocol on external edge    | No   | v0.2 possibility via `A2aAgentServer` HTTP endpoints |
| Zenoh / P2P transport                        | No   | v0.3+; major rewrite |

## Known limitations we are accepting

- **JetStream clustering bug [#7817](https://github.com/nats-io/nats-server/issues/7817)**
  (Feb 2026): workqueue + max-deliver can silently lose messages on 3-replica cluster.
  Single-node deployment is fine; flag before ever clustering.
- **Shared `NATS_TOKEN` means any token-holder can impersonate any `sender_id`.**
  Per-agent JWT auth is v0.2 work. Until then, the fleet boundary is the Tailscale
  tailnet; trust is tailnet-scoped.
- **Multi-instance same `agent_id` is not a supported configuration.** One `agent_id`
  = one process. JetStream tolerates it (work-sharing at inbox level) but aggregator
  cache and heartbeat view become non-deterministic. Hot-standby is a v0.2+ topic.
- **A2A semantic-only borrow:** we adopt A2A vocabulary but not its HTTP+JSON-RPC wire
  form for internal traffic. External A2A interop requires the Phase 4 `A2aAgentServer`
  wrapper.
- **No built-in DLQ in JetStream**; we use advisories + aggregator logging. Acceptable
  for v0.1 volumes.
- **Cancel is best-effort.** Side-effects already committed before cancel arrives
  remain. Partial writes, subprocess output, external API calls may have completed.
- **Chain-level cancellation is not automatic.** Cancelling a parent task does not
  cancel its delegations; the adapter handling the parent must propagate if desired.
- **Cancel on AG2 agents is v0.2.** v0.1 AG2 adapter returns `task_state: rejected,
  payload.reason: "ag2_cancel_not_supported"`. AG2 v0.12 does not expose a clean
  interrupt path through group-chat orchestration.
- **Aggregator cache is lost on aggregator restart.** Mitigated by the
  `request_register` broadcast dance. JetStream KV for card storage is v0.2.
- **AG2 v0.11+ API churn:** ag2 is pre-1.0 with breaking changes across minor bumps.
  Pin tightly and expect refactor work on each upgrade.
- **Max message size is 1MB.** Larger LLM outputs must stream over A2A SSE (Phase 4),
  not through JetStream.

## Impact on the execution plan

The prior `agent-contract-execution-plan.md` is **superseded** — its dual-emit / migration
model no longer applies. v0.1 is a clean rebuild. New phase shape:

### Phase 1 — Messaging foundation (clean rebuild)

1. **1.1 — Envelope schema + strict validation.** `schemas/envelope.v1.json` is the canonical
   A2A-aligned schema (`v`, `id`, `type`, `sender_id`, `recipient_id`, `task_id`,
   `context_id`, `task_state`, `hop_count`, `payload`). Type enum includes `cancel`.
   `additionalProperties: false` at the top level. Aggregator loads the schema and drops
   non-conformant messages with a logged reason.
2. **1.2 — Agent Card schema (A2A v1.0).** `schemas/agent-card.v1.json` replaced with the
   A2A v1.0 Agent Card shape. EdgeCitadel metadata vocabulary (`runtime.kind`,
   `runtime.roles`, `runtime.tags`, `runtime.deployment`, `runtime.upstream`) documented
   and schema-validated inside `metadata`.
3. **1.3 — Aggregator rewrite.** Drop all `receiver_id` / `message_type` / alias-fallback
   reader code. Canonical field readers only. `database.py` schema rewritten: columns
   `recipient_id`, `type`, `task_id`, `context_id`, `task_state` (not `receiver_id`,
   `message_type`). Indexes updated. DB wipe on first boot (new schema; no migration
   script). Aggregator publishes its own `type: register` Agent Card on
   `agents.aggregator.register` at startup. Aggregator subscribes to `agents.*.register`,
   caches cards, and serves `GET /api/agents` and `GET /api/agents/{id}/card`.
   Aggregator handles the `request_register` rebroadcast dance after its own restart.
4. **1.4 — JetStream bootstrap.** `AGENT_INBOX` stream on `agents.*.inbox` with
   `WorkQueuePolicy`. Per-agent consumer helper. Aggregator gains advisory subscriber on
   `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>` and endpoint
   `/api/agents/{id}/queue` returning `{pending, ack_pending}`.
5. **1.5 — Shared adapter common.** `adapters/_common/` package:
   - `pull_consumer.py` — JetStream pull-consumer loop (handles fetch, in_progress,
     ack, nak, term, `Nats-Msg-Id` header on outbound publishes).
   - `agent_card.py` — A2A v1.0 Agent Card factory from per-agent YAML config.
   - `template.py` — skeleton adapter with `handle()` stub.
   - `tests/conformance.py` — envelope accept/reject suite every adapter runs.
6. **1.6 — Shell adapter rewrite.** nats-py async, pull consumer with `max_ack_pending=1`.
   Uses the shared card factory. `273e80a`'s paho adapter is deleted.
7. **1.7 — openclaw-client rewrite.** `@nats-io/nats`. Canonical fields only. Retained
   MQTT block deleted from `nats.conf`; port 1883 removed from docker-compose. E2E test
   fixtures and assertions rewritten for the new envelope shape; specs that tested legacy
   shapes are pruned.
8. **1.8 — Frontend rewrite.** All components reading `receiver_id` / `message_type`
   updated to canonical fields. New queue-depth and poison-message surfaces wired to the
   new aggregator endpoints.
9. **1.9 — End-of-phase smoke test.** Fresh `docker compose down && up --build -d`, empty
   DB, echo shell adapter round-trip through the full rebuilt stack, one representative
   Playwright spec covering dashboard + conversation view.

### Phase 2 — Gemma smoke test

10. **2.1 — Gemma adapter.** `adapters/gemma/gemma_adapter.py` copies the shell adapter's
    pull-consumer shape; replaces the handler body with an HTTP call to Ollama
    (`POST /api/generate`, `stream: false`). Uses shared card factory.
    **Preflight:** verify `$OLLAMA_MODEL` tag exists via `ollama list` / `ollama pull`
    before writing code. Defaults: `gemma4:12b` preferred if present; `gemma3:12b` safe
    floor.

### Phase 3 — Operational hardening

11. **3.1 — Watchdog.** nats-py native. Subscribes `agents.*.heartbeat` and the
    MAX_DELIVERIES advisory subject. Holds a durable consumer on
    `agents.watchdog-1.inbox` following the same convention as all other adapters
    (`max_ack_pending: 1`); default handler rejects unknown commands with
    `task_state: rejected, payload.reason: "unknown_command"`. Publishes
    `status: offline` on `agents.watchdog-1.outbox` after 3× interval miss.
    Publishes **synthesized failure results** on `agents.{sender}.inbox` (via
    JetStream) when a command to an offline agent fails delivery — envelope is
    `type: result, task_state: failed, payload.error: "recipient_offline"`, with
    `sender_id: watchdog-1` and the original `task_id` echoed. Bootstrap default:
    expects heartbeats every 30s from any agent it hasn't seen register yet.
    Its own card declares `runtime.roles: ["watchdog"]`. Does not impersonate
    timed-out agents.
12. **3.2 — Agent registry view (dashboard).** Per-agent panel: card (name, roles, tags,
    deployment), heartbeat freshness, queue depth, poison-message count, online/offline.
    Can be implemented in parallel with 3.1; verification requires 3.1 complete.

### Phase 4 — AG2 + A2A wrapper

13. **4.1 — AG2 adapter L1 scaffold.** Pin `ag2>=0.12,<0.13`. Maps `ConversableAgent`
    send/receive onto NATS via the shared pull-consumer pattern. Uses
    `autogen.agentchat.group` orchestration (not deprecated Swarm). `A2aRemoteAgent`
    async-only (`a_run` everywhere).
14. **4.2 — AG2 L2 delegation + loop protection.** Maps AG2 hand-offs (registered via
    `register_hand_off(OnCondition(target=AgentTarget(...)))`) onto `type: delegation`
    envelopes. `hop_count` increments at each delegation publish; refuse at ≥8.
    **Cancel on AG2 agents in v0.1:** accepted envelope but returns
    `task_state: rejected, payload.reason: "ag2_cancel_not_supported"`. Proper
    cancel propagation through AG2 group-chat state is v0.2 work (AG2's API does
    not expose a clean interrupt path as of v0.12).
15. **4.3 — Delegation chain view (dashboard).** `/api/chains/{context_id}` endpoint +
    chain timeline UI.
16. **4.4 — A2A HTTP wrapper.** `A2aAgentServer(agent, agent_card=card).build()` + NATS
    bridge. Serves `/.well-known/agent-card.json` over HTTP alongside the NATS
    `register` publish. Enables LLM token streaming via SSE.

### Phase 5 — Permanent Gemma fleet member

17. **5.1 — deploy-mac-mini.sh.** One-command Mac Mini deploy. **Preflight documented:**
    (a) `.env` copied to the Mac Mini via Tailscale; (b) `BROKER_HOST` set to the
    broker's tailnet name / LAN IP; (c) operator verifies the shared token has JetStream
    permissions before deploying (one-line `nats consumer add` smoke on the broker host).

### Optional, v0.1+

18. **Bridge adapter for Hermes / ACP runtime.** Only if/when Nous Hermes Agent is
    onboarded. Covered by the bridge pattern in this spec; not required for v0.1
    completion.

**Session budget:** 17 core sessions plus the optional bridge. Expect 1–2 fix sessions
for issues surfaced during verification.

### Documentation step (applies to every session)

Each session in the execution plan adds a **Documentation** action, performed after code
changes are applied and before the commit:

> Invoke the `doc-writer` agent (`.claude/agents/doc-writer.md`) with the session's
> change summary and the list of files touched. Review its proposed edits, apply them,
> and include the doc updates in the same commit as the code change. Then optionally
> invoke `document-standards` for a compliance pass.

The `doc-writer` agent owns the mapping of change-type → doc-to-touch (subjects →
`docs/05-messaging.md`, lifecycle → `docs/agent-contract.md`, API → `docs/08-api-reference.md`,
etc.) and updates `docs/CHANGELOG.md` under `## [Unreleased]`. For hard-to-reverse
decisions (protocol / schema / transport pins), it also drafts a new ADR in
`docs/adr/NNNN-<slug>.md`.

Sessions that produce no user-visible behavior change (pure refactors, internal type
cleanups) may skip the Documentation step; the agent self-reports "no updates needed"
in those cases.

## Extensibility: adding a new agent type

The spec is designed so that adding a new agent type is a configuration + handler-body
change, not an infrastructure change.

### Standard adapter layout

```
adapters/
  _common/
    pull_consumer.py       # shared JetStream pull-consumer loop (in_progress, ack, dedup)
    agent_card.py          # A2A Agent Card factory (reads YAML → builds v1.0 card)
    template.py            # skeleton adapter: fill in handle() and run
    tests/
      conformance.py       # envelope accept/reject suite every adapter runs
  shell/
    adapter.py             # imports pull_consumer, implements handle()
    config.yaml
    tests/
    README.md
  gemma/
    adapter.py
    config.yaml
    tests/
    README.md
  watchdog/
    adapter.py             # heartbeat + synthesized-failure publisher
    config.yaml
    tests/
  ag2/                     # Phase 4
    adapter.py
    config.yaml
    tests/
```

### Adding a new agent

1. Create `adapters/<type>/` following the layout above.
2. Write `config.yaml` declaring `agent_id`, name, description, skills,
   `runtime.roles`, `runtime.tags`, any model-specific fields.
3. Copy `adapters/_common/template.py` as `adapters/<type>/adapter.py` and implement
   the handler body. The handler signature is:
   ```python
   async def handle(env: dict, ctx: Context) -> tuple[dict, str]
       # returns (result_payload, task_state)
       # ctx.in_progress() extends ack_wait for long tasks
       # ctx.publish_progress(body, progress?) emits task.progress envelopes
   ```
4. The shared Agent Card factory (`adapters/_common/agent_card.py`) builds the A2A
   card from the YAML. No envelope-schema changes needed.
5. First boot: the adapter creates its durable JetStream consumer if absent.

An adapter that needs to declare a new A2A extension (e.g., vendor-specific JSON-RPC
method) adds its extension URI to the YAML's `capabilities.extensions[]` list; the
factory passes it through.

For non-A2A-native upstream runtimes, follow the Bridge pattern section instead of
writing a direct adapter.

## Testing strategy

- **Unit tests per adapter:** assert envelope-shape handling, status transitions,
  error paths. Live in `adapters/<type>/tests/`.
- **Integration tests:** `e2e/fleet/` contains Playwright specs that bring up a
  docker-compose stack with 2–3 adapters, publish commands via the aggregator's HTTP
  API, and assert on conversation view + chain view + queue depth.
- **Transport conformance:** a reference test (`adapters/_common/tests/conformance.py`)
  publishes a suite of envelopes (valid, missing required fields, wrong `v`, wrong
  enum) and asserts validator accept/reject. Every adapter's test suite runs this
  against its own NATS connection.
- **Bridge pattern test:** if a bridge is implemented post-v0.1, it runs the same
  conformance suite plus a translator correctness test (upstream session events ↔
  A2A task_state transitions).

Verification commands — see AGENTS.md for canonical invocations.

## Legacy code disposition

Everything below is deleted or rewritten in Phase 1. No code survives the rebuild in
legacy shape.

| What                                         | Disposition      | Session |
|----------------------------------------------|------------------|---------|
| `adapters/shell/shell_adapter.py` (`273e80a`, paho) | Deleted, rewritten nats-py async | 1.6 |
| `openclaw-client/mqtt-listener.js` (paho)    | Deleted, rewritten @nats-io/nats | 1.7 |
| `aggregator/aggregator.py` alias fallbacks (`from`/`to`/`sender`/`assigned_agent`) | Deleted | 1.3 |
| `aggregator/database.py` `messages.receiver_id`, `messages.message_type` columns + indexes | Dropped; new schema with `recipient_id`, `type` | 1.3 |
| `/data/openclaw.db` existing file            | Wiped on first boot | 1.3 / deploy |
| `schemas/agent-card.v1.json` legacy shape    | Replaced with A2A v1.0 Agent Card shape | 1.2 |
| `schemas/envelope.v1.json` permissiveness (`additionalProperties: true`) | Tightened; strict validation | 1.1 |
| `mqtt { port: 1883, ... }` block in `nats.conf` | Removed | 1.7 |
| `ports: - "1883:1883"` in docker-compose.yml | Removed | 1.7 |
| Frontend components reading `receiver_id` / `message_type` | Rewritten for canonical fields | 1.8 |
| E2E specs asserting on legacy envelope shapes | Rewritten or pruned | 1.7 / 1.8 |

## v0.2 roadmap (consolidated)

All items deferred from v0.1 for a cleaner scope. Each is documented in its relevant
section above; this is a consolidated forward-look.

| Area                                    | v0.2 plan                                                |
|-----------------------------------------|----------------------------------------------------------|
| Agent Card storage                      | JetStream KV bucket `AGENT_CARDS` (key=agent_id); removes the aggregator-restart `request_register` dance |
| Authentication                          | Per-agent JWT with per-subject grants; retires shared `NATS_TOKEN` |
| Horizontal scale                        | Multi-worker per `agent_id` by relaxing `max_ack_pending` and declaring queue-group consumers |
| Hot-standby                             | Two processes sharing one `agent_id` with deterministic card/heartbeat arbitration |
| DB retention                            | Built-in retention task in aggregator; retires manual cron |
| Automated retirement                    | Watchdog triggers consumer + card cleanup after N days offline |
| External A2A edge                       | Expose `A2aAgentServer` HTTP endpoints outside the tailnet with OAuth2/mTLS |
| Cancel on AG2 agents                    | Real cancel propagation through AG2 group-chat state |
| Bridge crash-recovery                   | Checkpoint `{task_id → upstream session_id}` to JetStream KV so bridges survive restart |
| Structured/multimodal payloads          | A2A-style `parts[]` on message payloads; image/audio/file attachments |
| Multi-node JetStream clustering         | After [#7817](https://github.com/nats-io/nats-server/issues/7817) is verified fixed |
| Transport binding URI publication       | Move `https://edgecitadel.local/ext/nats-binding/v1` to a stable public domain with a binding spec doc |

## Implementation notes flagged for execution

These are not architectural ambiguities — the design is settled. They are details that
implementers must get right during the relevant session.

- **`hop_count` threading through AG2 hand-offs (Phase 4.2).** `register_hand_off` in
  AG2 v0.12 does not surface the envelope layer; the AG2 adapter MUST increment
  `hop_count` at the NATS publish boundary (in the outbound `type: delegation` envelope),
  not inside AG2's in-process chat history. Verified by sending a two-hop delegation
  and inspecting the envelope on the wire.
- **NATS transport binding URI stability.** `https://edgecitadel.local/ext/nats-binding/v1`
  is the declared extension URI. If EdgeCitadel becomes an open-source project, move
  to a public domain and publish a spec document at that URL. Until then, the URI
  is informational only — A2A consumers on the EdgeCitadel tailnet do not dereference
  it.
- **Bridge reference target.** The bridge pattern applies to any non-A2A-native runtime.
  v0.1 does not ship a bridge; the reference target for the first bridge (post-v0.1)
  is Nous Research's Hermes Agent (ACP-native). The bridge pattern section documents
  the shape; the implementation is a separate deliverable.

## References

- NATS JetStream docs: https://docs.nats.io/nats-concepts/jetstream/streams
- NATS Consumer details: https://docs.nats.io/using-nats/developer/develop_jetstream/consumers
- Synadia JetStream scaling patterns: https://www.synadia.com/blog/jetstream-design-patterns-for-scale
- A2A Protocol spec v1.0.0: https://a2a-protocol.org/latest/specification/
- AG2 A2A integration: https://docs.ag2.ai/latest/docs/user-guide/a2a/
- AG2 v0.9 release (Swarm deprecation): https://docs.ag2.ai/latest/docs/blog/2025/04/28/0.9-Release-Announcement/
- HiveMind — OS-inspired scheduling for LLM agents: https://arxiv.org/html/2604.17111
- ProtocolBench — LLM multi-agent protocol comparison: https://arxiv.org/pdf/2510.17149
- NATS #7817 (clustering gotcha): https://github.com/nats-io/nats-server/issues/7817
- NATS discussion #4750 (MQTT QoS): https://github.com/nats-io/nats-server/discussions/4750
