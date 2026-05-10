# ADR-0009: Bridge adapters retain upstream memory ownership

- **Status:** Proposed (Phase 6)
- **Date:** 2026-05-06
- **Supersedes:** none
- **Superseded by:** none
- **Related:** ADR-0008 (centralized memory service)

## Context

ADR-0008 made the aggregator the centralized memory service for adapters that have no memory of their own. The Phase 2.5 Gemma adapter consumes that service via `memory.turns.{get,put,delete}` — fetch on each command, persist user + assistant turns afterwards.

Phase 6 onboards Nous Research's Hermes Agent as the first **bridge** adapter — `runtime.kind: bridge`, `runtime.upstream: hermes-agent`. Hermes Agent ships with its own persistent memory store (SOUL.md, learned skills, session memory under `~/.hermes/`). Forcing Hermes traffic through `memory.turns.*` would either:

1. Duplicate state — Hermes' store and the aggregator's `conversation_turns` table both hold the same conversation, with no synchronization guarantees and double the backup surface, or
2. Require disabling Hermes' memory features — defeats the point of using Hermes.

Future bridge candidates (Claude Code, AG2 orchestrator, external SaaS agents) will share this property: a self-contained agent product with its own state.

## Decision

**Bridge adapters (`runtime.kind: bridge`) MUST NOT call `memory.turns.{get,put,delete}`.** They MAY pass `context_id` to the upstream as an opaque session/run identifier (HTTP header, query param, or whatever the upstream's API expects).

The aggregator's envelope ledger (`messages` table, fed by `agents.{id}.{outbox,log,heartbeat,status}`) remains the canonical cross-agent audit record. Operators querying "what did agent X do" hit `GET /api/messages?agent_id=X` rather than `GET /api/conversations?agent_id=X`.

`runtime.tags: [external-memory]` is the operator-facing signal (surfaced on the agent card and, eventually, the dashboard) that an agent's continuity lives upstream rather than in `conversation_turns`.

## Consequences

- The aggregator's `conversation_turns` table is sparse for fleets that lean on bridge adapters; metrics like `GET /api/conversations` reflect only native-runtime agents.
- Backup story for upstream-owned memory is the operator's responsibility, documented per-adapter (Hermes: `~/.hermes/`).
- Cross-agent shared memory or AG2 delegation chains that thread context across both native and bridge agents must integrate at the envelope ledger (canonical) rather than `conversation_turns` (partial). This ADR locks that direction.
- The `memory.turns.*` API surface remains untouched — bridge adapters don't extend it; they opt out of it.
- A bridge adapter that needs centralized memory in the future (e.g., the upstream lacks its own store) is free to call `memory.turns.*` — the rule says MUST NOT for adapters whose upstream owns memory, not "never". This ADR is permissive on that edge case if it arises.

## Implementation note

The Hermes adapter (`adapters/hermes/`, Phase 6) is the reference implementation. Its `tests/test_adapter_handle.py:test_handle_does_not_publish_to_memory_turns` is a regression guard: it asserts `nc.publish` is never called with a subject matching `memory.turns.*` during a complete `handle()` cycle.
