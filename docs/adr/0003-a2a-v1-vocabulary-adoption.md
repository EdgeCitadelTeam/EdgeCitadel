# ADR-0003: Adopt A2A v1.0 Lifecycle Vocabulary on the EdgeCitadel Envelope

## Status

Accepted

## Date

2026-04-24

## Context and Problem Statement

Pre-v0.1, the EdgeCitadel envelope used legacy ad-hoc field names — `receiver_id`, `message_type`, `correlation_id`, `causation_id`, `chain_id`, `assigned_agent`, plus `content` / `from` / `to` aliases scattered across the codebase. These names predated any external standard and were chosen informally as the messaging foundation grew.

Two pressures made this untenable for v0.1:

1. **Delegation chains and task lifecycle visibility.** As the fleet expanded toward AG2-driven multi-agent workflows, the contract needed first-class concepts for *task identity* (distinct from message identity), *grouping a chain of delegated tasks* (distinct from a single request/reply correlation), and *task lifecycle state* (distinct from agent runtime state). Bolting these onto `correlation_id` / `causation_id` / `chain_id` was possible but produced a vocabulary nobody outside this repo would recognize.
2. **Forward-compatibility with A2A.** The Agent2Agent (A2A) protocol v1.0 had stabilized a vocabulary covering exactly these concepts: `task_id`, `context_id`, `task_state` (8 values), and an Agent Card shape. Aligning EdgeCitadel's envelope with A2A's vocabulary today made future federation — exposing an EdgeCitadel agent to an external A2A client, or vice versa — a wrapping problem rather than a translation problem.

The question was scope: adopt only the field names, only the Agent Card shape, or also the HTTP+SSE transport binding A2A specifies.

## Decision Drivers

- Strict, schema-enforced wire format: producers can't drift, parsers don't carry alias-fallback code.
- Clear separation of *agent runtime state* (`agent_state`) from *task lifecycle state* (`task_state`); no enum collision.
- Forward-compat with A2A v1.0 *without* committing to A2A's HTTP+SSE transport in v0.1 (NATS JetStream is the v0.1 transport).
- Cheap to express EdgeCitadel-specific concerns (`runtime.roles`, `runtime.deployment`) via A2A's own extension mechanism rather than fork the spec.

## Considered Options

1. **Keep legacy field names, add aliases for the new A2A names.** Backward-compatible for legacy code, but doubles the validator surface, lets producers drift, and locks every downstream tool into a forever-deprecated alias table.
2. **Invent fresh EdgeCitadel-native names** (e.g., `task_handle`, `chain_handle`, `lifecycle_state`). Avoids legacy baggage, but produces a vocabulary with zero external recognition — every future integration is a translation.
3. **Adopt A2A v1.0 vocabulary on the envelope (semantic borrow only).** Use `task_id`, `context_id`, `task_state`, `sender_id`/`recipient_id`. Agent Card adopts A2A v1.0 shape. Transport remains NATS JetStream, declared as an A2A extension.
4. **Adopt A2A v1.0 vocabulary AND its HTTP+SSE transport today.** Maximum standards-alignment, but gives up JetStream's durable inbox semantics and forces every adapter to run an HTTP server.

## Decision Outcome

Chosen option: **Option 3 — adopt A2A v1.0 vocabulary on the envelope (semantic borrow only).**

Concretely:

- **Field names.** Envelope uses `sender_id`, `recipient_id`, `task_id`, `context_id`, `task_state`, `agent_state`, `hop_count`. The 8-value `task_state` enum (`submitted`, `working`, `input-required`, `completed`, `failed`, `canceled`, `rejected`, `auth-required`) matches A2A v1.0 verbatim. The 4-value `agent_state` enum (`online`, `offline`, `busy`, `error`) is EdgeCitadel-native and intentionally separate.
- **No legacy aliases.** `receiver_id`, `message_type`, `content`, `from`, `to`, `correlation_id`, `causation_id`, `chain_id`, `assigned_agent` are removed without alias-fallback. The strict schema (`additionalProperties: false`) rejects them at validation time. Producers and consumers update in lockstep on the rebuild branch; there is no migration window.
- **Agent Card.** Card payload follows A2A v1.0 Agent Card JSON shape. EdgeCitadel-specific keys (`runtime.roles`, `runtime.tags`, `runtime.deployment`, `runtime.kind`, `runtime.upstream`, etc.) live under the A2A `metadata` map.
- **Transport binding.** EdgeCitadel mints the URI `https://edgecitadel.local/ext/nats-binding/v1` and declares it as an A2A capability extension on every Agent Card, with `additionalInterfaces` pointing at `nats://edgecitadel/agents.{agent_id}.inbox`. **This is a semantic borrow, not a transport claim** — A2A's HTTP+SSE binding is explicitly NOT used in v0.1; that work is deferred to Phase 4 via the `A2aAgentServer` wrapper around AG2 agents.

### Consequences

#### Positive

- **One vocabulary, one schema.** No alias-fallback code in validators, parsers, or storage. Producers can't drift quietly.
- **Future federation is wrapping, not translation.** When EdgeCitadel exposes an agent to an external A2A client (Phase 4), the wire format already speaks the right names — only transport changes.
- **Clean separation of concerns.** `task_state` (A2A) describes a task's lifecycle; `agent_state` (EdgeCitadel) describes an agent's runtime; `hop_count` is delegation depth. No overloaded fields.
- **Stable subset of A2A v1.0.** EdgeCitadel emits a strictly smaller envelope than full A2A, reducing the surface area that would need change for future federation.

#### Negative

- **Hard rebuild for all producers and consumers.** Every adapter, the aggregator, the frontend, the openclaw client, the e2e tests — all see new field names in one cut. Mitigated by doing this on a clean rebuild branch (no production data preserved; legacy DB is wiped per spec).
- **`task_state` and `agent_state` look similar at a glance.** Reviewers will sometimes need to re-read which is which. Mitigated by schema-level enforcement: each envelope `type` accepts at most one of the two.

#### Neutral

- The decision binds us to A2A v1.0's specific `task_state` enum values. If A2A v1.1 redefines them, we will re-evaluate. The risk is bounded — A2A is a stabilized v1.0 spec with explicit deprecation discipline.

## Pros and Cons of the Options

### 1. Keep legacy names + aliases

- Good, because no producer code changes immediately.
- Bad, because the alias table lives forever, drift is invisible, and future federation requires a translation layer anyway.

### 2. Invent fresh EdgeCitadel-native names

- Good, because the vocabulary fits EdgeCitadel concepts exactly.
- Bad, because nobody outside this repo will recognize the names — every future integration is a translation.

### 3. A2A v1.0 vocabulary, NATS transport (chosen)

- Good, because the vocabulary is externally recognizable, the schema is strictly enforceable, and EdgeCitadel keeps its NATS JetStream durability story via A2A's own extension mechanism.
- Bad, because the rebuild is hard-cut — every component changes at once.

### 4. A2A v1.0 vocabulary AND HTTP+SSE transport

- Good, because maximum standards-alignment.
- Bad, because we lose JetStream's durable inbox + dedup + replay, and every adapter must host an HTTP server. Premature for v0.1.

## Links

- [docs/agent-contract.md](../agent-contract.md) — authoritative v0.1 contract built on this decision.
- [schemas/envelope.v1.json](../../schemas/envelope.v1.json) — strict JSON Schema enforcing the vocabulary.
- [ADR-0001: NATS over MQTT broker](0001-nats-over-mqtt-broker.md)
