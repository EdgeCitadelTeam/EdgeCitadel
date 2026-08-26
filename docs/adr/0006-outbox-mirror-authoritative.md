# ADR-0006: Outbox Mirror is the Authoritative Source for Dashboard Conversation Views

## Status

Accepted

## Date

2026-04-24

## Context and Problem Statement

In v0.1 every adapter consumes its inbox traffic via a *durable, per-agent JetStream pull consumer* on the `EC_INBOX` stream (subject filter `agents.{self}.inbox.>`). The stream is configured with `WorkQueuePolicy` retention so that each message is delivered to exactly one consumer and removed once acked — that property is what gives the contract its at-least-once durable inbox semantics.

JetStream's `WorkQueuePolicy` enforces a **disjoint-filter constraint**: two consumers attached to the same WorkQueue stream cannot have overlapping subject filters. That rule prevents the obvious-looking design — "add a second JetStream consumer that observes `agents.*.inbox.>` for audit" — because such a consumer would overlap with every per-agent consumer and would either be rejected at creation time or break delivery semantics on the per-agent consumers.

Without an audit path, the aggregator (and the dashboard it serves) would have no way to populate conversation views (`/api/messages` filtered by sender / recipient / task) without competing with adapters for inbox messages, which would silently drop deliveries to the real recipient.

The question was where to put the audit tap: on the inbox path itself (impossible per above), on a parallel JetStream stream, by polling consumer state, or by mirroring publishes on a separate plain-NATS subject.

## Decision Drivers

- Must not perturb the inbox WorkQueue's at-least-once delivery to the real recipient.
- Must capture *every* inbox publish a healthy adapter performs (commands, results, delegations, status, task.progress when targeted, cancels) without polling.
- Must let the dashboard render conversation views from a single canonical SQLite table.
- Should also be useful as a debug "network tap" for operators watching live traffic.
- Should not double JetStream storage just for audit.

## Considered Options

1. **Adapters mirror every outbound JetStream-inbox publish to `agents.{self}.outbox` via plain NATS; aggregator subscribes to `agents.*.outbox` and treats those events as authoritative for dashboard views.**
2. **Add a second JetStream stream (e.g. `EC_AUDIT`) that captures the same subjects.** Doubles storage, breaks the dedup boundary, and forces every adapter to publish to both streams.
3. **Query each per-agent consumer's pending state / replay log from the aggregator.** Race-prone (state read != message arrival), no payload visibility before ack, and operationally fragile.
4. **Move the outbox mirror itself onto JetStream** (a separate `EC_OUTBOX` stream). Defeats the original constraint — once it's a JetStream stream you've reintroduced the WorkQueue overlap problem from a different angle, and you've doubled storage.

## Decision Outcome

Chosen option: **Option 1 — outbox mirror on plain NATS, aggregator-authoritative for dashboard views.**

Concretely:

- **Adapter contract.** Every adapter that publishes to another agent's JetStream inbox (`agents.{recipient}.inbox.{type}`) MUST also publish the same envelope to its own outbox subject `agents.{self}.outbox.{type}` via plain (non-JetStream) NATS, on a best-effort fire-and-forget path. This is enforced in `adapters/_common/pull_consumer.py` (Task 10) — adapters do not call `js.publish` directly; they call a wrapper that fans out to inbox + outbox.
- **Aggregator subscription.** The aggregator subscribes to `agents.*.outbox.>` on plain NATS. Each received envelope is written into the canonical `messages` table via `database.insert_message`.
- **Authoritative for dashboard views.** `/api/messages` (filtered by `agent_id` / `task_id` / `context_id`) reads exclusively from the aggregator's `messages` table, which is populated by the outbox subscription. The aggregator does *not* attach a JetStream consumer to the inbox stream.

### Consequences

#### Positive

- **WorkQueue serialization preserved.** Per-agent inbox consumers are the only attachees on `EC_INBOX`; the disjoint-filter constraint is satisfied trivially.
- **Operator debug tap.** `nats sub 'agents.*.outbox.>'` shows live conversation traffic without disturbing delivery, useful during incident response.
- **Single canonical table.** Dashboard queries are simple SQL against `messages`; no cross-stream joining or consumer-state polling.
- **Adapter contract is testable.** Conformance tests (Task 11) can assert that publishing a command via the adapter wrapper produces both an inbox publish and an outbox mirror.

#### Negative

- **Aggregator-down windows lose audit.** While the aggregator is offline, outbox publishes have no subscriber and are dropped from the dashboard view. Durable inbox delivery is unaffected — the recipient still receives the message via JetStream. We accept this trade-off because the dashboard is a view, not a system of record; the agents' own task state is.
- **Every adapter MUST mirror.** A buggy adapter that publishes to inbox without mirroring to outbox produces a silent audit gap. Mitigated by routing all publishes through `adapters/_common/pull_consumer.py` (Task 10) and asserting the contract in the conformance suite (Task 11).

#### Neutral

- **Plain NATS, not JetStream, for the mirror.** Outbox subjects are intentionally not durable; if you want history you query `messages` in the aggregator. This keeps the mirror cheap and avoids reintroducing the WorkQueue overlap problem.

## Pros and Cons of the Options

### 1. Outbox mirror on plain NATS, aggregator-authoritative (chosen)

- Good, because it sidesteps the WorkQueue disjoint-filter constraint cleanly.
- Good, because it doubles as an operator-visible network tap.
- Bad, because aggregator downtime drops the audit slice of that window.

### 2. Second JetStream stream for audit

- Good, because audit history is durable.
- Bad, because storage doubles, dedup boundaries diverge between streams, and adapters now have two publish paths to keep in sync.

### 3. Query per-consumer state from the aggregator

- Good, because no extra publishes from adapters.
- Bad, because consumer state is a poor proxy for "a message was sent" — it's race-prone, lacks payload visibility, and is fragile under reconnection.

### 4. Outbox via JetStream

- Good, because durable outbox history.
- Bad, because it defeats the WorkQueue disjoint-filter constraint that motivated the design in the first place, and doubles storage.

## Links

- [docs/agent-contract.md](../agent-contract.md) — v0.1 contract; outbox subjects are part of the subject inventory.
- [docs/05-messaging.md](../05-messaging.md) — operator-facing subject inventory (will be updated in Task 8).
- [ADR-0001: NATS over MQTT broker](0001-nats-over-mqtt-broker.md)
- [ADR-0003: A2A v1.0 vocabulary adoption](0003-a2a-v1-vocabulary-adoption.md)
