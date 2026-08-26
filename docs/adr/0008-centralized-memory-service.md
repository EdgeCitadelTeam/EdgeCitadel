# ADR-0008: Centralized memory service hosted by aggregator, exposed over NATS

## Status

Proposed (Phase 2.5)

## Date

2026-04-30

## Context and Problem Statement

Phase 2.5 adds conversational memory to the Gemma adapter. The brainstorm considered four DB topologies: per-adapter SQLite, aggregator-hosted SQLite with direct read access, aggregator-hosted SQLite behind a NATS service API, and external KV (Redis / libSQL). Per-adapter would scale poorly as more agents adopt memory (N DB files to back up, schema drift across adapters, no fleet-wide queries). External KV is over-engineered for the single-host v0.2 scale. Direct DB access from adapters violates the existing decoupling rule (adapters talk to NATS only, never to the aggregator's filesystem).

## Decision Drivers

- One backup target for all conversational state.
- One schema migration path; no schema drift across adapters.
- Decoupling: adapters communicate over NATS only.
- New adapters that want memory should be able to opt in by sending three NATS messages — no new infrastructure.
- Forward-compat for v0.3 semantic memory (sqlite-vec) without API breakage.

## Considered Options

1. **Per-adapter SQLite** (each adapter owns its own `data/<id>-memory.db`).
2. **Aggregator-hosted SQLite, direct adapter reads** (adapters open `/data/openclaw.db` themselves).
3. **Aggregator-hosted SQLite, NATS request-reply API** (this ADR's choice).
4. **External KV / document store** (Redis, libSQL/Turso).

## Decision Outcome

Chosen option: **3 — aggregator-hosted SQLite + NATS request-reply API.**

A new `conversation_turns` table lives in `/data/openclaw.db`. Three NATS subjects (`memory.turns.get`, `memory.turns.put`, `memory.turns.delete`) form the API surface. The aggregator owns all writes; SQLite WAL mode permits future read-only consumers without lock contention. A background asyncio task hard-deletes turns whose `context_id` has been idle past 30 days. The schema reserves `turn_embedding BLOB` and `skill_id` columns; the aggregator loads `sqlite-vec` at startup (best-effort) so v0.3 semantic memory is additive — same NATS API extends with a `query_embedding` parameter.

### Consequences

#### Positive

- One file to back up; one schema migration; one place to enforce retention policy.
- New adapters that need memory require zero new infrastructure (just three NATS calls).
- Multi-host extraction path is a clean refactor — the NATS API stays identical, only the storage process moves.
- Schema reserves forward-compat columns from day one.

#### Negative

- Three NATS round-trips per inference (get + 2× put). Acceptable at single-host scale; ~1ms each over loopback.
- SQLite single-writer ceiling. Fine at expected v0.2 throughput (~2 inserts per inference, low concurrency). Signal to watch: `memory.turns.put` p95 latency exceeding 50ms under load — that's the cue to extract the memory service to its own libSQL or Postgres-backed process.

#### Neutral

- Diverges from the implicit "each adapter owns its own state" assumption. Established new pattern: adapter-state queries flow through aggregator-hosted services exposed via NATS.

## Pros and Cons of the Options

### Option 1 — Per-adapter SQLite

- Good, because adapter is fully self-contained.
- Bad, because backup sprawl, schema drift, no fleet-wide queries, dashboard cannot show "all conversations" without N round-trips.

### Option 2 — Aggregator-hosted SQLite, direct reads

- Good, because no NATS round-trip overhead.
- Bad, because adapters reach into aggregator's filesystem; breaks the abstraction; concurrent writes from N adapters need careful WAL coordination.

### Option 3 — Aggregator-hosted SQLite, NATS API (chosen)

- Good, because preserves decoupling, single writer, forward-compat for v0.3, clean extraction path.
- Bad, because adds three NATS round-trips per inference (acceptable at scale).

### Option 4 — External KV (Redis / libSQL)

- Good, because purpose-built for high-throughput KV access.
- Bad, because over-engineered for single-host scale; adds new infrastructure to deploy + back up.

## Related

- Builds on ADR-0006 (outbox-mirror as audit path) — memory is a separate, narrower data store with its own retention policy.
- Forward-compat for v0.3 semantic memory: `sqlite-vec` extension load + `turn_embedding BLOB` column.
