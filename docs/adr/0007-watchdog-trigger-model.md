# ADR-0007: Watchdog trigger model — heartbeat-staleness fast path with advisory backstop

## Status

Proposed (Phase 3.1)

## Date

2026-04-29

## Context and Problem Statement

The v0.1 messaging spec (`docs/superpowers/specs/2026-04-23-agent-messaging-design.md`, rev 6, lines 759–778) pinned the watchdog's synthesised-failure trigger to the JetStream `MAX_DELIVERIES` advisory only, rejecting heartbeat-staleness as a trigger because it would have required the watchdog to subscribe to per-agent inbox traffic — incompatible with the WorkQueue's disjoint-filter rule on `AGENT_INBOX`.

That decision had a real cost: failure-detection latency for offline recipients equals `ack_wait × max_deliver`, which is 1.5 min for a shell adapter and 15 min for a Gemma-class LLM adapter. Senders (HTTP callers especially) hang for the full window. Phase 3 brainstorm revisited the constraint with one new observation: per ADR-0006, every adapter mirrors its inbox publishes to its own `agents.{self}.outbox` (plain NATS). The watchdog can derive in-flight task state from those mirrors **without** subscribing to inboxes.

## Decision Drivers

- Cut failure-detection latency for offline recipients from 1.5–15 min to under 65 s for default-interval (30 s) agents.
- Maintain spec rev 6's correctness guarantee: the advisory must remain authoritative for any task the fast paths miss (cold-start, watchdog restart gap, dropped outbox traffic).
- Avoid per-agent inbox observation that would conflict with `max_ack_pending=1` serialization.
- Keep the watchdog stateless across restarts — no new persistence layer.

## Considered Options

1. **Advisory-only (spec rev 6).** Synthesise failures only when JetStream fires MAX_DELIVERIES. 1.5–15 min latency; trivial state machine.
2. **Heartbeat-staleness fast path with advisory backstop (this ADR).** Watchdog observes outbox + heartbeat traffic, synthesises immediately when an agent's heartbeat expires past `2 × declared_interval` or when a new command targets an already-flagged offline agent. The advisory remains a defensive backstop.
3. **Heartbeat-only (no advisory).** Skip the advisory subscription. Simpler watchdog, but a watchdog restart leaves a gap during which no synthesis happens — senders hang.

## Decision Outcome

Chosen option: **2 — heartbeat-staleness fast path with advisory backstop.**

Three reinforcing trigger paths produce the same synthesised envelope, sharing one dedup key (`Nats-Msg-Id: watchdog-syn-{task_id}`):

1. **Heartbeat-staleness fast path** (primary). When `now - last_seen[X] > max(2 × declared_interval, 20 s) + 5 s tolerance`, fan out synthesised failures for every entry in `pending_tasks[X]`. ~30–65 s detection for default 30 s interval.
2. **Sticky-offline immediate path.** Once X is in the `offline_agents` set, observing a new `command` or `delegation` to X on the outbox feed → immediate synthesis, no entry added to `pending_tasks`. New commands to a known-dead agent fail in milliseconds.
3. **Advisory backstop** (defensive). The MAX_DELIVERIES advisory subscription remains active. Cold-start or watchdog-restart gaps are covered when JetStream eventually terminates the message after `ack_wait × max_deliver`.

The watchdog rebuilds in-flight state from outbox traffic on restart; no persistence. The advisory backstop guarantees correctness when the fast paths cannot.

### Consequences

#### Positive

- Failure-detection latency drops from 1.5–15 min to ~30–65 s for default-interval agents.
- New commands to known-dead agents fail in milliseconds (sticky-offline path).
- No new persistence; watchdog state is rebuilt from live traffic.

#### Negative

- Watchdog now maintains an in-memory `pending_tasks` map. Memory cost is bounded by the number of in-flight commands fleet-wide (small in practice).
- Two subscribers on MAX_DELIVERIES (aggregator for poison logging, watchdog for synthesis). Both are plain NATS, no consumer-slot conflict.

#### Neutral

- Diverges from spec rev 6's "MAX_DELIVERIES only" pin. The spec doc is updated to point here for the canonical trigger description; rev 6 reasoning remains accurate as a snapshot.

## Pros and Cons of the Options

### Option 1 — Advisory-only

- Good, because trivial state machine and pristine alignment with spec rev 6.
- Bad, because callers wait 1.5–15 min on offline recipients; HTTP callers time out without context.

### Option 2 — Heartbeat-staleness with advisory backstop (chosen)

- Good, because order-of-magnitude latency reduction and instant feedback on commands to known-dead agents.
- Good, because the advisory backstop preserves correctness without requiring watchdog persistence.
- Bad, because adds an in-memory `pending_tasks` map and three handler paths instead of one.

### Option 3 — Heartbeat-only

- Good, because simpler than option 2.
- Bad, because watchdog restart gap leaves senders hanging with no recovery path.

## Related

- Spec: `docs/superpowers/specs/2026-04-29-phase-3-watchdog-and-registry-design.md`
- Plan: `docs/superpowers/plans/2026-04-29-phase-3-watchdog-and-registry.md`
- Supersedes the "MAX_DELIVERIES only" passage in `docs/superpowers/specs/2026-04-23-agent-messaging-design.md` rev 6.
- Builds on ADR-0006 (outbox mirror as authoritative audit path).
