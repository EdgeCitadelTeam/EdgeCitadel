# ADR-0011: MCP is the canonical tool-exposure protocol

- **Status:** Proposed (Phase 4 umbrella)
- **Date:** 2026-05-16
- **Supersedes:** none
- **Superseded by:** none
- **Related:** ADR-0003 (A2A v1.0 vocabulary), ADR-0010 (NATS-native L2)

## Context

The fleet's primitives — publish a command, query the roster, read the message ledger, cancel a task — are useful far beyond the dashboard's HTTP surface. The roadmap's parking lot listed "MCP server exposing edge-research tools to Hermes" as v0.3+; promoting it to Phase 4 multiplies the value of every other deliverable in this phase.

MCP-aware clients are now mainstream: Claude Desktop, Cursor, VS Code (Continue), AG2 (`MCPToolkit`), Hermes Agent (built-in MCP client), future agents. Each gains immediate access to the fleet without an EdgeCitadel-specific code path.

A2A *skills* (the L3 conformance level) and MCP *tools* are not redundant — they have different audiences:

- A2A skills describe what an *agent* offers to another *agent* via the A2A protocol.
- MCP tools describe what the *fleet as a whole* offers to a *tool-using consumer* (LLM, dev tool, automation script).
- The same primitive (e.g., "publish a command to gemma-1") can be both an A2A skill on `gemma-1`'s card and an MCP tool on the fleet's MCP server, addressing different consumers.

## Decision

**MCP is the canonical tool-exposure protocol for EdgeCitadel.**

- The MCP server (Phase 4.3) is a first-class fleet subscriber: its own NATS connection, registered as agent `mcp-1` (`runtime.kind: gateway`), heartbeats, audit-trail in the envelope ledger like any other agent.
- The server speaks MCP over both stdio (local Claude Desktop / Cursor mounts) and HTTP+SSE (remote clients including Hermes' built-in MCP client and AG2's `MCPToolkit`).
- Tools and resources surface fleet primitives 1:1 with the existing aggregator and NATS surface; no business logic is duplicated. The MCP server's `delegate` tool uses the same `adapters/_common/l2_orchestrator.py` helpers an AG2 orchestrator uses — one substrate, two consumers.
- The MCP server is **not** the in-fleet delegation hub (per ADR-0010); it's a tool-exposure surface that internally publishes via the same NATS-native delegation contract.
- Per-caller provenance lives in `payload.metadata.caller` (e.g., `claude-desktop`, `cursor`, an ad-hoc uuid for HTTP+SSE sessions). `sender_id` stays at `mcp-1` because the envelope contract requires sender_ids to match cached Agent Cards.

## Consequences

- One new service (`mcp-server/`) to maintain; new docker-compose entry, new launchd/systemd unit.
- AG2's L2 orchestrator (Phase 4.2) can use the MCP server as a tool source — the fleet "drinks its own champagne." This is also a forward-compat pattern: future framework orchestrators (LangGraph, CrewAI) can do the same.
- Hermes (built-in MCP client) can call into the fleet without an EdgeCitadel-specific code path.
- L3 conformance (A2A typed skills) becomes a separate concern — adapters declare A2A skills on their cards independently of MCP exposure. Phase 4 makes L3 implementable; it does not require every adapter to be L3.

## Alternatives considered

- **Folding MCP support into the A2A gateway.** Rejected: different audiences (tool consumers vs. agent peers), different protocols (MCP vs. A2A), different lifecycles.
- **Reusing `/api/*` HTTP endpoints with no MCP layer.** Rejected: external clients cannot discover or mount HTTP-only surfaces as MCP tools; the entire point is that MCP is the protocol that *makes them tools*.
