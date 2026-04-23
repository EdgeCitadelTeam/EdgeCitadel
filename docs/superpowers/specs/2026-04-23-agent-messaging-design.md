# Agent Messaging Design — EdgeCitadel v0.1

Status: draft, pending review
Date: 2026-04-23
Branch: `feat/agent-contract-v0.1`
Author: collaborative brainstorm (see `~/workplace/edge-research-notes/agent-contract-execution-plan.md` for the execution plan this spec extends)
Target spec: `docs/agent-contract.md`

## Summary

Define how agent-to-agent messages are queued, delivered, and lifecycled when the recipient
is mid-task. The design has three layers:

- **Transport:** NATS JetStream WorkQueue, one stream over `agents.*.inbox`, one durable pull
  consumer per agent with `max_ack_pending=1`. Server-enforced sequential processing per agent.
- **Semantic:** Adopt the Google A2A protocol's task lifecycle vocabulary (`task_state`,
  `task_id`, `context_id`) on top of our existing envelope. Borrow the names; keep NATS as the
  wire.
- **Adapter:** For v0.1 Phase 2–3, a thin nats-py pull-consumer loop wrapping `ConversableAgent`.
  For Phase 4, wrap each AG2 agent in `A2aAgentServer` for HTTP+SSE streaming, bridged to NATS.

Openclaw-client migrates from paho-mqtt to nats.js in v0.1. MQTT adapter on NATS is retired
from the primary path.

## Problem

Today, when A sends a message to B while B is executing:
- The shell adapter has a dead `error: busy` code branch; in practice paho's single-threaded
  callback implicitly serializes.
- Any future nats-py adapter (AG2, watchdog) will process messages concurrently by default —
  silent heterogeneity the spec does not address.
- Adapter crash = in-flight work lost; sender has no signal to retry.
- AG2's `ConversableAgent` is not thread-safe.
- HiveMind (arXiv 2604.17111) documents 72–100% failure rate for uncoordinated LLM agents
  under contention vs 0–18% with admission control.

The v0.1 goal is: make sequential processing an enforced property of the system, not an
adapter-local convention, and give operators durability + observability without infrastructure
changes beyond what NATS already supports.

## Goals

1. One message in flight per `agent_id` at any time, enforced by the broker.
2. Messages survive adapter crashes: redeliver on `ack_wait` expiry, cap at `max_deliver`.
3. Poison messages are observable via JetStream advisories, not silent.
4. Queue depth per agent is observable from the aggregator.
5. Envelope vocabulary is compatible with the A2A v1.0 task lifecycle so future external
   agents can interop without envelope translation.
6. AG2 v0.12 adapter works end-to-end for L2 conformance (delegation + chain_id).
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
  subjects:  ["agents.*.inbox"]
  retention: WorkQueuePolicy
  storage:   file
  max_age:   24h
  max_bytes: 1GB          # prevent unbounded growth on aggregator node
  discard:   old          # drop oldest if cap hit
```

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

Adapter loop (pseudocode):

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
            await handle_envelope(env)
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

### Semantic layer: A2A lifecycle vocabulary

Envelope additions (backward-compatible in v0.1; deprecated old names removed in v0.2):

| New field     | Required for              | Maps to / semantics                          |
|---------------|---------------------------|----------------------------------------------|
| `task_id`     | `command`, `result`, `delegation` | Alias of `correlation_id` — identical semantics |
| `context_id`  | `delegation`              | Alias of `chain_id` — identical semantics    |
| `task_state`  | Optional on `result` and intermediate progress messages | A2A task state enum |
| `hop_count`   | `delegation` (required)   | Integer, starts at 0 on root delegation, +1 at each hop, refuse at ≥8 |

`task_state` enum (borrowed from A2A v1.0):

```
submitted | working | input-required | completed | failed | canceled | rejected | auth-required
```

Our existing `type: result` continues to exist; `task_state` refines what kind of result
(`completed` vs `failed` vs `rejected`). The current `error` field in result payload becomes
a detail of `task_state: failed`.

During v0.1 the envelope schema accepts either set. Publishers SHOULD emit canonical (A2A)
names; consumers MUST accept both. v0.2 drops the old names.

### Adapter layer

**Phase 2–3 adapter (plain NATS pull consumer):**

Thin async loop as above. One message at a time by consumer config; no in-process queue
needed. Same binary handles echo, Gemma (wrapping Ollama HTTP), and watchdog (subscribes to
`agents.*.heartbeat` instead of an inbox).

**Phase 4 adapter (AG2 + A2A wrapper):**

For AG2 agents specifically:
1. AG2's `ConversableAgent` is wrapped with `A2aAgentServer(agent).build()` — exposes the
   agent as an HTTP+SSE A2A endpoint on a local port.
2. A small bridge process runs alongside:
   - NATS pull consumer on `agents.{agent_id}.inbox` (same config as above).
   - On each message, POST to the local A2A server as `tasks/send`.
   - Consume SSE stream from A2A server; translate `task_state: working` → NATS
     `type: task.progress`; terminal state → NATS `type: result` with
     `task_state ∈ {completed, failed, rejected}`.
3. Agent Card at `/.well-known/agent-card.json` is served by AG2; the register envelope
   on `agents.{agent_id}.register` is derived from it.

Pin: `ag2>=0.12,<0.13`. AG2's current group-chat API (`autogen.agentchat.group`,
`register_hand_off(OnCondition(target=...))`) replaces the deprecated Swarm API.

**Loop protection:**

`context_id` (née `chain_id`) carries a hop counter in envelope field `hop_count`, incremented
at each delegation. Refuse delegation at `hop_count >= 8`. This is an envelope-level
mechanism; not per-process visit-sets (which don't detect cross-process cycles).

### openclaw-client NATS port

Replace `paho-mqtt` with `@nats-io/nats` in openclaw-client. Changes:
- `mqtt.connect(url, creds)` → `connect({ servers, token: NATS_TOKEN })`.
- Subject form `agents/${id}/inbox` → `agents.${id}.inbox`.
- `client.subscribe(topic, {qos: 1})` → `nc.subscribe(subject)` (no QoS; use JetStream for
  durability where required).
- `client.publish(topic, payload, {qos: 1})` → `js.publish(subject, payload)` for durable
  publishes; plain `nc.publish(subject, payload)` for ephemeral (e.g., heartbeats).
- Retained `register` message → JetStream `kv`-style pattern or republish on reconnect.
  Simplest v0.1: publish on connect; aggregator caches. (Deferred detail: evaluate
  JetStream KV for Agent Card storage in v0.2.)

E2E tests touching openclaw flows will break and need updates. Budget ~1 session for the
test migration in addition to the port.

### MQTT port retirement

`mqtt` block in `nats.conf` retained for one v0.1 release (in case a legacy publisher lags)
but no EdgeCitadel-internal publisher uses it. v0.2 removes the block.

Shell adapter: port from paho to nats-py as part of the Phase 1 work to unify the adapter
story. Current shell adapter env vars `CITADEL_HOST` / `CITADEL_PORT` change to `NATS_URL`.
Existing committed shell adapter (`273e80a`) is superseded.

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
6. **A2A vocab round-trip:** publisher emits `task_id`; aggregator reads both `task_id` and
   `correlation_id` with no warnings.
7. **openclaw-client:** all existing Playwright specs pass after port.
8. **AG2 adapter (Phase 4):** planner→worker delegation with shared `context_id`, visible
   chain in dashboard, loop protection refuses at `hop_count=8`.

## Supported features matrix

| Feature                                      | v0.1 | v0.2 plan | Notes |
|----------------------------------------------|------|-----------|-------|
| Per-agent FIFO enforced by broker            | Yes  | —         | `max_ack_pending=1` |
| Crash-survival / redelivery                  | Yes  | —         | `ack_wait` + `max_deliver` |
| Poison-message detection                     | Yes  | —         | Advisories |
| Queue-depth observability                    | Yes  | —         | `nats consumer info` |
| A2A task lifecycle vocabulary                | Yes  | Rename-only | Aliased envelope fields |
| AG2 L2 delegation + loop protection          | Yes  | —         | `hop_count` in envelope |
| LLM token streaming                          | Yes (Phase 4) | — | A2A SSE, not over NATS |
| Agent Card discovery                         | Yes  | Move to A2A-native `/.well-known/agent-card.json` | v0.1 keeps custom `register`; v0.2 serves A2A Agent Card |
| NATS-native client for all v0.1 agents       | Yes  | —         | openclaw-client ported; shell adapter ported |
| Per-agent JWT auth                           | No   | Planned   | Shared `NATS_TOKEN` for v0.1 |
| Multi-worker per `agent_id` (horizontal)     | No   | Possible  | Relax `max_ack_pending` |
| JetStream clustering                         | No   | When 2nd persistent node exists | [#7817](https://github.com/nats-io/nats-server/issues/7817) gotcha to resolve first |
| MQTT 5.0 features                            | No   | No        | Blocked on NATS adapter |
| Native A2A wire protocol on external edge    | No   | Possible  | Currently NATS-internal; can expose A2A HTTP endpoints |
| Zenoh / P2P transport                        | No   | v0.3+     | Major rewrite |

## Known limitations we are accepting

- **NATS→MQTT delivery is always QoS 0** ([discussion #4750](https://github.com/nats-io/nats-server/discussions/4750)).
  Irrelevant after openclaw-client ports, but a constraint if any external MQTT consumer
  ever attaches.
- **JetStream clustering bug [#7817](https://github.com/nats-io/nats-server/issues/7817)**
  (Feb 2026): workqueue + max-deliver can silently lose messages on 3-replica cluster.
  Single-node deployment is fine; flag before ever clustering.
- **A2A semantic-only borrow:** we adopt A2A vocabulary but not its HTTP+JSON-RPC wire form
  for internal traffic. External A2A interop requires the Phase 4 `A2aAgentServer` wrapper.
- **No built-in DLQ in JetStream**; we use advisories + aggregator logging. Acceptable
  for v0.1 volumes.
- **AG2 v0.11+ API churn:** ag2 is pre-1.0 with breaking changes across minor bumps.
  Pin tightly and expect refactor work on each upgrade.

## Impact on the execution plan

The existing `agent-contract-execution-plan.md` needs these changes:

1. **New Session 1.4 — JetStream bootstrap.** Stream + per-agent consumer declarations.
   Aggregator gains advisory subscriber and `/api/agents/{id}/queue` endpoint.
2. **Session 1.1 extended:** envelope schema adds A2A alias fields (`task_id`, `context_id`,
   `task_state`, `hop_count`) alongside existing fields; validator accepts both. LWT
   guidance paragraph stays.
3. **Session 1.2 replaced:** openclaw-client is ported from paho-mqtt to nats.js. Dual-emit
   of field names is preserved during v0.1; wire-level transport is NATS-only. E2E specs
   updated. Larger than the original dual-emit session; budget 1.5×.
4. **New Session 1.5 — Shell adapter port.** From paho to nats-py. Uses pull consumer with
   `max_ack_pending=1`. Supersedes `273e80a`'s shell adapter.
5. **Session 1.3 moves:** becomes the end-of-Phase-1 smoke test against the new transport.
6. **Phase 2.1 Gemma adapter:** uses nats-py pull-consumer pattern (not the plan's fictional
   `handle_message` swap). Budget ~60 lines (not 30).
7. **Phase 3.1 watchdog:** nats-py native; subscribes `agents.*.heartbeat`; publishes
   status under `agents.watchdog-1.outbox`.
8. **Phase 4 API update:** `register_hand_off(OnCondition(target=AgentTarget|...))` and
   `autogen.agentchat.group.AutoPattern` / `RoundRobinPattern` — not Swarm. Pin
   `ag2>=0.12,<0.13`.
9. **New Session 4.4 — A2A wrapper.** `A2aAgentServer(agent).build()` + NATS bridge bridge
   process per agent. Agent Card served from `/.well-known/agent-card.json`.
10. **Session budget:** realistic is now 13–16 sessions, not 7–10.

## Open questions flagged for spec review

- **Agent Card storage:** today it's retained via the publish-on-connect + aggregator cache.
  JetStream KV would be a cleaner store. Defer to v0.2 decision.
- **`hop_count` initialization:** defined at 0 for a root `command` and incremented at each
  delegation. Verify the AG2 adapter correctly threads it through hand-offs before Phase 4.2.
- **Aggregator dashboard surface for poison messages:** new panel in the agent registry view,
  or inline next to per-agent queue depth. UX decision during Phase 3.2.

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
