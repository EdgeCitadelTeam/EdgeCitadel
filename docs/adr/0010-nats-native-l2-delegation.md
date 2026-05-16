# ADR-0010: In-fleet delegation is NATS-native; HTTP+SSE A2A is external-only

- **Status:** Proposed (Phase 4 umbrella)
- **Date:** 2026-05-16
- **Supersedes:** none
- **Superseded by:** none
- **Related:** ADR-0003 (A2A v1.0 vocabulary), ADR-0006 (outbox mirror), ADR-0009 (bridge memory ownership)

## Context

ADR-0003 adopted A2A v1.0 vocabulary in v0.1 and reserved the A2A HTTP+SSE binding for Phase 4. An initial Phase 4 design (2026-05-09 draft) considered using stock `A2aRemoteAgent` over HTTP+SSE for in-fleet delegation, requiring an A2A gateway to act as the in-fleet delegation hub.

That design introduced three under-specified seams:

1. **Provenance loss.** Peers receive envelopes with `sender_id: a2a-gateway` rather than the real originator. Per-sender authorization, `memory.turns.*` keying, and cancel attribution all break when the actual sender is hidden behind the gateway.
2. **`hop_count` does not cross an A2A boundary natively.** Stock `A2aRemoteAgent` is generic A2A and has no knowledge of EdgeCitadel's `hop_count`. The gateway would have to either invent a custom A2A header or track chain depth itself — both fragile.
3. **Authorization ambiguity.** A gateway that publishes on behalf of arbitrary callers needs an explicit policy about which `sender_id` values it is allowed to mint. The 2026-05-09 design deferred this to v0.3.

HTTP+SSE round-trips for messages already on the same NATS broker are also pure overhead — a fresh process, a fresh socket, a fresh serialization for traffic that JetStream is already routing.

## Decision

**In-fleet delegation MUST use the NATS-native delegation envelope** (`type: delegation`) on JetStream `agents.{recipient}.inbox`, with the existing dedup and at-least-once contract.

- Adapters acting as L2 orchestrators (AG2, future LangGraph / CrewAI / Pydantic-AI / etc.) use shared helpers in `adapters/_common/l2_orchestrator.py`: `publish_delegation`, `await_result_for_task`, `refuse_if_hop_limit`, `propagate_context`. AG2 (Phase 4.2) is the reference implementation; future frameworks reuse the same helpers without re-deriving the contract.
- Stock `A2aRemoteAgent` MAY be used for delegation to **external** (non-fleet) A2A endpoints. That is federation; v0.3+ scope.
- The A2A HTTP+SSE binding (ADR-0003 phase-4 commitment) is implemented by the **external A2A ingress gateway** (Phase 4.4), which translates inbound external A2A traffic into internal NATS commands. The gateway is **not** on the in-fleet delegation path — it is purely external edge.

## Consequences

- One delegation transport to maintain (NATS), one external-protocol surface (the slim A2A gateway), cleanly separated.
- Provenance is preserved by construction: every delegation envelope carries the real `sender_id`, `context_id`, `hop_count`. Peers can apply per-sender authorization, key memory entries correctly, attribute cancels.
- The A2A gateway becomes much smaller (only handles untrusted edge traffic; never internal cascades).
- The L2 substrate (`adapters/_common/l2_orchestrator.py`) becomes the canonical seam for any future orchestrator framework — explicit forward-compat for LangGraph, CrewAI, etc.
- Future federation (an in-fleet adapter driving an external A2A endpoint) is unconstrained: an adapter can spawn an `A2aRemoteAgent` pointed at a foreign URL when v0.3 needs it; this ADR doesn't forbid that direction, only the *internal-cascade-via-HTTP* anti-pattern.

## Alternatives considered

- **Multiplexing A2A gateway as in-fleet delegation hub** (the 2026-05-09 draft). Rejected on overhead, provenance loss, and substrate-plurality grounds (the L2 contract should be transport-uniform across frameworks).
- **Internal A2A egress library** that translates `A2aRemoteAgent.send()` to NATS in-process without HTTP. Rejected because it preserves a dependency on AG2's specific A2A client class for what should be a transport-uniform contract — future frameworks shouldn't need to mock `A2aRemoteAgent` to be L2-conformant.
