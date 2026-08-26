# ADR-0002: NATS JetStream WorkQueue for Per-Agent Inbox Serialization

## Status

Accepted

## Date

2026-04-24

## Context and Problem Statement

Pre-v0.1 the dev fleet used plain NATS publish for commands targeting an agent's inbox subject (`agents.{agent_id}.inbox`). Two operational defects surfaced as soon as more than one operator drove the system:

1. **Concurrent commands to the same agent interleaved.** A second `task.assign` arriving while the first was mid-execution caused the adapter to spawn a parallel handler. For shell agents this meant two processes editing the same working directory; for LLM-backed agents it meant two completion streams racing onto the same outbox subject.
2. **Messages were dropped on broker restarts.** Plain NATS delivery is fire-and-forget — if the broker restarted while a message was in flight, or if the agent was offline at publish time, the message was lost with no record. Operators only learned of the loss when the expected `result` envelope never arrived.

For v0.1 we need:

- (a) **Per-agent FIFO serialization** at the broker layer, so the application doesn't have to coordinate. An adapter can never see message N+1 for the same agent until message N is acked.
- (b) **At-least-once delivery across broker and agent restarts.** Messages must survive both broker crashes and agent reconnects.
- (c) **Bounded redelivery (poison detection).** A consistently-failing handler must not redeliver forever; the broker should advisory-flag the message after a fixed retry count so operators see it on the dashboard.

The fleet target is <50 agents on a single Mac Mini for the foreseeable future, so single-stream throughput limits and per-server consumer limits are not constraints.

## Decision Drivers

- Per-agent FIFO must be enforced by the broker, not by application-level coordination that won't survive aggregator restart.
- Inbox messages must be durable — a crashed agent reconnecting should resume processing where it left off, with no operator intervention.
- Poison-message detection must be observable from the dashboard without writing custom retry-count tracking in every adapter.
- Solution must not require per-agent stream provisioning — fleet may grow to 50+ agents and stream-per-agent doesn't scale on a single node.
- Operator retries (re-publishing the same envelope) must be idempotent within a small window, so that double-clicks or network blips don't cause double execution.

## Considered Options

1. **Application-level mutex per agent** — keep plain NATS, add an in-process mutex map in the aggregator keyed by `agent_id`.
2. **NATS queue groups** — switch to a queue subscription so the broker load-balances among consumers.
3. **Separate JetStream stream per agent** — one stream `AGENT_INBOX_{agent_id}` per agent.
4. **Single JetStream stream with per-agent durable consumers, `max_ack_pending: 1`** — one `AGENT_INBOX` stream subscribing `agents.*.inbox`, one durable consumer per agent.
5. **Single JetStream stream with higher `max_ack_pending`** — same as Option 4 but allow more in-flight messages per consumer.

## Decision Outcome

Chosen option: **Option 4 — single JetStream stream `AGENT_INBOX` with WorkQueuePolicy and per-agent durable consumers limited to `max_ack_pending: 1`.**

Concretely:

- **Stream `AGENT_INBOX`**:
  - `subjects: ["agents.*.inbox"]` — single stream covers every agent's inbox via wildcard.
  - `retention: WorkQueuePolicy` — message is removed from the stream once any consumer acks it (no replay; the outbox mirror, ADR-0006, provides the audit trail).
  - `discard: new` — when the stream fills, reject new publishes rather than evict old ones; backpressure is preferable to silent loss.
  - `max_msg_size: 1MB` — matches NATS server default; rejects oversized payloads at publish time.
  - `max_age: 24h`, `max_bytes: 1GB` — bounds for a single-Mac-Mini fleet.
  - `duplicate_window: 5min` — when the publisher sets `Nats-Msg-Id`, the broker dedupes redelivered messages within this window. Operator double-clicks and network blips are no-ops.
- **Per-agent durable consumer `{agent_id}_inbox`**:
  - `filter_subject: agents.{agent_id}.inbox` — each consumer sees only its own agent's traffic.
  - `ack_policy: explicit`, `ack_wait: 300s`, `max_ack_pending: 1` — at any time the agent has at most one un-acked message in flight; the next message only arrives after the previous one is acked. This is the FIFO-serialization mechanism.
  - `max_deliver: 3` — three failed delivery attempts and the broker emits a max-deliveries advisory on `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>`. Task 5's `on_advisory` handler captures this and persists a `poison_events` row.
  - Long-running tasks call `in_progress()` periodically to extend `ack_wait`; the 300s default is the floor, not the ceiling.
- **Aggregator's own consumer is the exception.** The aggregator does not need FIFO over its inbox — it's draining results back to HTTP callers and there is no shared mutable state per envelope. Its consumer uses `max_ack_pending: 100` and `ack_wait: 60s`. Same stream, different consumer config.

### Consequences

#### Positive

- **Per-agent FIFO is enforced by the broker, not the application.** No in-process mutex to lose on aggregator restart; no race conditions when two operators publish concurrently.
- **Messages survive broker restart.** File storage on the JetStream server keeps the stream across restarts; agents reconnecting see the un-acked message redelivered.
- **`Nats-Msg-Id` + `duplicate_window` gives a 5-minute idempotency window** for retries. Publishers can safely re-send the same envelope ID without fear of double execution.
- **Poison messages surface automatically** via `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>`. The aggregator's existing `on_advisory` handler writes a `poison_events` row; the dashboard reads from there.
- **One stream covers the entire fleet.** Adding an agent does not require provisioning a new stream — only a new durable consumer, which the bootstrap helper creates on first connect.

#### Negative

- **Adapters MUST keep the message UNACKED for the entire task.** If the adapter acks before the work is done, a crash mid-task means the message is lost. The shared adapter skeleton (Task 10) calls `in_progress()` on a timer and only `ack()`s after the result envelope has been published.
- **WorkQueuePolicy enforces "disjoint subject filters" across consumers on the same stream.** This means we cannot add a second consumer that listens on a wildcard like `agents.*.inbox` for audit purposes — the audit consumer's filter would overlap with every per-agent consumer. This constraint is what motivated ADR-0006's outbox mirror: instead of a second consumer on the inbox stream, agents publish a copy of their inbound traffic to `agents.{agent_id}.outbox` (plain NATS, audit-only) so the dashboard has a canonical audit view without violating WorkQueue's disjoint-filter rule.
- **`max_ack_pending: 1` means an agent's processing throughput is bounded by its single-task latency.** Horizontal replication of one logical agent across two adapters would require relaxing this and is out of scope for v0.1.

#### Neutral

- **Stream metrics live on the broker, not the aggregator.** Queue depth and consumer lag are read via `js.stream_info()` / `js.consumer_info()` rather than from a local counter. Task 6's `/api/system/queue` endpoint exposes these to the dashboard.

## Pros and Cons of the Options

### 1. Application-level mutex per agent

- Good, because no broker-side configuration changes; works with plain NATS.
- Bad, because the mutex map lives in aggregator memory — any aggregator restart drops it and the next two concurrent commands race again.
- Bad, because it doesn't address durability: messages are still lost on broker restart.

### 2. NATS queue groups

- Good, because trivial to configure (queue subscription is one parameter).
- Bad, because queue groups load-balance delivery across subscribers — it's the wrong semantic. Inbox messages target a specific agent, not "any worker who can do this."
- Bad, because plain NATS queue groups are not durable; offline agents miss messages.

### 3. Separate JetStream stream per agent

- Good, because per-stream config gives maximum tenancy isolation.
- Bad, because NATS has a per-server stream limit (~hundreds, depending on storage); a fleet target of 50 today is fine but the design doesn't scale.
- Bad, because the bootstrap helper would have to enumerate every agent at startup; new agents need a new stream provisioned before they can be sent commands.

### 4. Single stream + per-agent durable consumers, `max_ack_pending: 1` (chosen)

- Good, because exactly one stream regardless of fleet size; only consumers proliferate, and they're cheap.
- Good, because `max_ack_pending: 1` is the natural FIFO knob — no application coordination needed.
- Bad, because the WorkQueue disjoint-filter rule blocks adding an audit consumer on the same stream (drove ADR-0006).

### 5. Single stream + higher `max_ack_pending`

- Good, because higher throughput per agent.
- Bad, because higher throughput is the wrong goal here — we explicitly want serialization. With `max_ack_pending > 1`, two messages can be in flight to the same agent simultaneously, which is the original defect.
- Defer: revisit when v0.2 horizontal scale matters.

## Links

- [docs/05-messaging.md](../05-messaging.md) — subject inventory and JetStream consumer table.
- [aggregator/jetstream_bootstrap.py](../../aggregator/jetstream_bootstrap.py) — `ensure_stream` / `ensure_consumer` implementation.
- [ADR-0001: NATS over MQTT broker](0001-nats-over-mqtt-broker.md)
- [ADR-0006: Outbox mirror authoritative](0006-outbox-mirror-authoritative.md) — explains why the audit path lives on a separate subject family rather than a second consumer on `AGENT_INBOX`.
