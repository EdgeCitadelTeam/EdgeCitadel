---
paths:
  - "nats/**"
  - "aggregator/aggregator.py"
  - "aggregator/jetstream_bootstrap.py"
  - "adapters/_common/**"
  - "schemas/envelope.v1.json"
---

# NATS & Messaging Rules (v0.1+)

> Authoritative sources: `docs/agent-contract.md`, `schemas/envelope.v1.json`,
> ADR-0002 (JetStream WorkQueue), ADR-0003 (A2A vocabulary), ADR-0004 (MQTT
> opt-in), ADR-0005 (browser-scoped token), ADR-0006 (outbox mirror),
> ADR-0010 (NATS-native L2 delegation), ADR-0011 (MCP for tool exposure).
> This file is short-form guidance for tools; conflicts in favor of the docs.

## Transport

- NATS-only by default. JetStream is enabled with file storage.
- MQTT ingress is **deploy-time opt-in** (`EC_ENABLE_MQTT=1` +
  `scripts/render-nats-conf.sh`); off in default `docker compose up`.
  Internal fleet does not use MQTT under any deployment.

## Envelope contract

Every message published under `agents.*`, `tasks.*`, or `system.*` follows
the strict schema at `schemas/envelope.v1.json`:

- `v: 1`, `id` (UUID4), `type`, `sender_id`, `timestamp` (`.sssZ`), `payload`
  are always required.
- `type` ∈ `{register, heartbeat, status, command, result, delegation,
  cancel, log, broadcast, task.progress}`.
- `task_state` ∈ A2A v1.0 enum: `submitted | working | input-required |
  completed | failed | canceled | rejected | auth-required`.
- `agent_state` ∈ `{online, offline, busy, error}` and **only** appears on
  `status` envelopes (forbidden elsewhere by schema).
- `task_id` / `context_id` are UUID4. **Never** `correlation_id` /
  `chain_id` / `receiver_id` / `message_type` — those are legacy and rejected
  at the validator layer.

## Subjects

- `agents.{id}.register` — A2A v1.0 Agent Card payload, idempotent.
- `agents.{id}.heartbeat` — periodic liveness (default 30s).
- `agents.{id}.status` — `agent_state` transitions.
- `agents.{id}.inbox` — JetStream `AGENT_INBOX` WorkQueue, durable consumer
  `{id}_inbox` (`max_ack_pending=1`, `ack_wait=300s`, `max_deliver=3`).
- `agents.{id}.outbox` — plain-NATS audit mirror (per ADR-0006).
- `agents.{id}.log` — plain-NATS structured log envelopes (lifecycle,
  errors); see `adapters/_common/template.py:publish_log`.
- `agents.{id}.task_progress.{task_id}` — plain-NATS streaming progress.
- `memory.turns.{get,put,delete}` — Phase 2.5 memory service, request-reply.
  Aggregator is sole subscriber/responder. Adapters use these instead of
  reaching into the aggregator's SQLite directly.
  Bridge adapters (`runtime.kind: bridge`) MUST NOT publish to these
  subjects (per ADR-0009). They retain upstream memory ownership and
  may pass `context_id` to the upstream as an opaque session token.
- `system.broadcast` — fleet-wide.
- `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>` —
  aggregator subscriber for poison-event logging; watchdog also
  subscribes (Phase 3.1) for synthesising recipient_offline failures.
  Two subscribers on plain NATS = no consumer-slot conflict.
- `openclaw.{session_id}.{kind}` — browser ingress; aggregator translates
  `command.{target}` → `agents.{target}.inbox` with server-set `sender_id`.

## Adding new subjects

1. Define the subject in `aggregator/aggregator.py:AggregatorApp.start`.
2. Add a handler on `MessageRouter` (validate via `_parse_and_validate`).
3. Update `docs/05-messaging.md` (subject inventory + semantics).
4. For new envelope types: extend `schemas/envelope.v1.json` (with
   conditional `if/then` rules for required-by-type) and add tests in
   `schemas/tests/test_envelope_schema.py`. Then update the validator
   re-export at `adapters/_common/validator.py`.

## JetStream

- `AGENT_INBOX` config is owned by `aggregator/jetstream_bootstrap.py`:
  `WorkQueuePolicy`, `discard: new`, `max_msg_size: 1MB`,
  `duplicate_window: 5min`. Adapters call the same module on first
  connect (idempotent).
- Per-agent durable consumers: `{agent_id}_inbox`, `max_ack_pending=1`
  (FIFO), `ack_wait_sec=300`, `max_deliver=3`.
- nats-py StreamConfig/ConsumerConfig accepts `max_age`,
  `duplicate_window`, `ack_wait` in **seconds** and converts to ns
  internally during JSON serialization. Don't pre-convert to ns — the
  values overflow uint64.

## Publisher semantics

- JetStream publishes (`agents.{id}.inbox`) MUST set
  `Nats-Msg-Id: <envelope.id>` for the 5-min dedup window.
- Adapters MUST mirror inbox publishes to their own
  `agents.{self}.outbox` via plain NATS (per ADR-0006).
- Plain-NATS publishes (heartbeat, status, log, broadcast, task_progress)
  do not need `Nats-Msg-Id`.

## Phase 4 surfaces

- In-fleet `delegation` envelopes stay on NATS. Use the L2 helpers at
  `adapters/_common/l2_orchestrator.py` (Phase 4.2) — do not route in-fleet
  delegation through A2A HTTP+SSE (ADR-0010).
- External A2A v1.0 HTTP+SSE traffic enters the fabric only through the
  `a2a-gateway` service (Phase 4.4). Adapters MUST NOT serve A2A HTTP
  endpoints directly.
- Tool exposure to MCP-aware clients (Claude Desktop, Cursor, Hermes,
  AG2 MCPToolkit) is provided by the `mcp-server` service (Phase 4.3,
  registered as agent `mcp-1`, `runtime.kind: gateway`). Tools surface
  fleet primitives 1:1 with NATS / aggregator — no business logic is
  duplicated (ADR-0011).

## Auth

- v0.1 broker auth is token-only (`token: $NATS_TOKEN` in `nats.conf`).
  Adapters and aggregator connect with that token.
- The browser openclaw client uses an account-scoped `OPENCLAW_TOKEN`
  (per ADR-0005). The token-based connection model means a single
  user/openclaw account-scoped JWT is v0.2 work — today the aggregator
  translator + the fact that `OPENCLAW_TOKEN` is held only by trusted
  browsers is the actual scoping.
- HTTP-level auth on aggregator endpoints: **none** in v0.1. Designs for
  v0.2 are deferred (see `docs/roadmap.md`).
