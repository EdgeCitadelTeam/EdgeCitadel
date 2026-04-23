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

### Agent Card and A2A extensions

The `register` envelope payload is the full A2A v1.0 Agent Card JSON. Our legacy
`schemas/agent-card.v1.json` is superseded; v0.1 preserves EdgeCitadel-specific fields
(`runtime.roles`, `tags`, `deployment`) under the Agent Card's free-form `metadata` map.

**Mandatory A2A fields** (per v1.0 spec): `name`, `description`, `version`, `url`,
`provider`, `capabilities`, `securitySchemes`.

**Optional but required by our convention**: `skills[]`, `defaultInputModes`,
`defaultOutputModes`, `additionalInterfaces`, `capabilities.extensions`, `metadata`.

**NATS transport binding as an A2A profile extension.** EdgeCitadel mints the URI
`https://edgecitadel.local/ext/nats-binding/v1` and declares it on every Agent Card:

```json
"capabilities": {
  "streaming": true,
  "extensions": [{
    "uri": "https://edgecitadel.local/ext/nats-binding/v1",
    "description": "NATS JetStream transport binding for EdgeCitadel fleet.",
    "required": false,
    "params": {"subject_prefix": "agents.{agent_id}"}
  }]
},
"additionalInterfaces": [
  {"url": "nats://edgecitadel/agents.{agent_id}.inbox", "transport": "nats-jsonrpc"}
]
```

This is a legitimate use of A2A's documented extension mechanism, not a deviation —
A2A v1.0 explicitly supports custom bindings via `additionalInterfaces` and custom
capabilities via extension URIs.

**EdgeCitadel metadata vocabulary** (all under Agent Card `metadata` map):

| Key                  | Required | Values |
|----------------------|----------|--------|
| `runtime.roles`      | yes      | Array of role strings: `worker`, `reasoner`, `watchdog`, `orchestrator` |
| `runtime.tags`       | no       | Free-form tags (model family, deployment, capabilities) |
| `runtime.deployment` | no       | Deployment identifier, e.g., `mac-mini-studio-01` |
| `runtime.kind`       | yes      | `native` (speaks A2A/NATS directly) or `bridge` (facade for an external runtime) |
| `runtime.upstream`   | required if `runtime.kind = bridge` | Identifier of the upstream runtime, e.g., `nous-hermes-agent`, `openclaw-legacy-mqtt` |

`runtime.kind` and `runtime.upstream` are informational — they do not change NATS routing
but let operators and the aggregator reason about failure modes.

**Building the Agent Card.** AG2's `A2aAgentServer` auto-generates only `name` and
`description`; everything else must be supplied explicitly. A shared factory
(`adapters/_common/agent_card.py`) builds the full card from a per-agent config file and
passes it into `A2aAgentServer(agent, agent_card=card)`.

### Adapter layer

**Phase 2–3 adapter (plain NATS pull consumer):**

Thin async loop as above. One message at a time by consumer config; no in-process queue
needed. Same binary handles echo, Gemma (wrapping Ollama HTTP), and watchdog (subscribes to
`agents.*.heartbeat` instead of an inbox).

**Phase 4 adapter (AG2 + A2A wrapper):**

For AG2 agents specifically:
1. AG2's `ConversableAgent` is wrapped with `A2aAgentServer(agent, agent_card=card).build()`
   — exposes the agent as an HTTP+SSE A2A endpoint on a local port. The Agent Card is
   built by our shared factory (see previous section), not auto-generated by AG2.
2. A small bridge process runs alongside:
   - NATS pull consumer on `agents.{agent_id}.inbox` (same config as above).
   - On each message, POST to the local A2A server as `message/send`.
   - Consume SSE stream from A2A server; translate `task_state: working` → NATS
     `type: task.progress`; terminal state → NATS `type: result` with
     `task_state ∈ {completed, failed, rejected}`.
3. Both the A2A HTTP endpoint at `/.well-known/agent-card.json` AND the NATS
   `agents.{agent_id}.register` publish serve the same Agent Card JSON.

**AG2-specific constraints:**
- `A2aRemoteAgent.run()` fails; use `a_run()` exclusively. Our adapter code is async-first.
- AG2's `A2aAgentServer` in v0.12 auto-populates only `name` and `description`; everything
  else comes from our factory.
- AG2 group-chat API: use `autogen.agentchat.group` with `AutoPattern` / `RoundRobinPattern`
  and `register_hand_off(OnCondition(target=AgentTarget(...)))`. Swarm is deprecated.
- Pin: `ag2>=0.12,<0.13`. Expect breaking changes across minor bumps until v1.0.

**A2A version compatibility:** if we later upgrade A2A, set `enable_v0_3_compat=True` on the
server to keep older clients working during migration windows. Current spec target is
A2A v1.0; no v0.3 clients expected at launch.

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

### Bridge pattern for external runtimes

Not every agent we onboard is an AG2 `ConversableAgent`. Some upstream runtimes — Nous
Research's Hermes Agent (ACP-native, Feb 2026), legacy openclaw-MQTT agents during
transition, any future non-A2A framework — require a translator process.

**Canonical bridge topology:**

```
[External runtime] <-- upstream protocol --> [EdgeCitadel bridge] <-- NATS JetStream --> [EdgeCitadel fleet]
```

The bridge is, from NATS's perspective, an ordinary agent:
- Owns a stable `agent_id` (e.g., `hermes-nous-01`, `openclaw-legacy-42`).
- Holds a durable pull consumer on `agents.{agent_id}.inbox` with `max_ack_pending=1`.
- Publishes its own Agent Card to `agents.{agent_id}.register` with:
  - `metadata.runtime.kind = "bridge"`
  - `metadata.runtime.upstream = "nous-hermes-agent"` (or equivalent)
  - `skills[]` reflecting what the upstream runtime actually supports

**Responsibilities of the bridge:**
1. Translate inbound A2A envelopes → upstream protocol calls (e.g., ACP `session/new`,
   ACP `session/prompt`).
2. Translate upstream lifecycle events → A2A `task_state` updates on NATS. Unmappable
   upstream states default to `failed` with a clear reason in the result payload.
3. Maintain a `{A2A task_id → upstream session_id}` map for the duration of each task.
4. Terminate trust at the bridge (v0.1): the bridge is trusted by NATS; it is the bridge's
   responsibility to authenticate to the upstream runtime out-of-band.
5. Propagate `context_id` and `hop_count` unchanged in both directions so loop protection
   remains coherent end-to-end.

**Bridge failure semantics (v0.1):** if the bridge crashes mid-task, the NATS message is
unacked and JetStream redelivers per `ack_wait`. Upstream session state may be lost; on
redelivery the bridge either resumes (if the upstream protocol supports session resume) or
marks the task `failed` with reason `bridge_restart_lost_session` so the sender can retry.
Checkpointing `{task_id → session_id}` to JetStream KV for bridge crash-recovery is v0.2.

**Applicability note:** "openclaw/hermes" in EdgeCitadel conversations most likely refers
to Nous Research's Hermes Agent (the OpenClaw alternative runtime). If instead it means
something repo-specific that a future reader understands, re-scope this section.

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
| Agent Card discovery (A2A-native in v0.1)    | Yes  | Add `/.well-known/agent-card.json` HTTP endpoint (Phase 4) | v0.1 publishes A2A Agent Card JSON via `register`; Phase 4 AG2 adapter also serves HTTP |
| A2A extension: NATS transport binding        | Yes  | —         | `https://edgecitadel.local/ext/nats-binding/v1` profile extension |
| Bridge pattern for non-A2A runtimes          | Yes  | KV-checkpoint for bridge crash-recovery | v0.1 marks `failed` on bridge restart |
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
   guidance paragraph stays. `schemas/agent-card.v1.json` is replaced by A2A v1.0 Agent
   Card shape; our legacy fields move to `metadata`.
3. **Session 1.2 replaced:** openclaw-client is ported from paho-mqtt to nats.js. Dual-emit
   of field names is preserved during v0.1; wire-level transport is NATS-only. E2E specs
   updated. Larger than the original dual-emit session; budget 1.5×.
4. **New Session 1.5 — Shell adapter port.** From paho to nats-py. Uses pull consumer with
   `max_ack_pending=1`. Supersedes `273e80a`'s shell adapter.
5. **New Session 1.6 — Shared Agent Card factory.** `adapters/_common/agent_card.py` that
   builds a v1.0 A2A Agent Card from per-agent YAML config. Used by shell, Gemma, watchdog,
   and the Phase 4 AG2 adapter.
6. **Session 1.3 moves:** becomes the end-of-Phase-1 smoke test against the new transport
   and Agent Card shape.
7. **Phase 2.1 Gemma adapter:** uses nats-py pull-consumer pattern (not the plan's fictional
   `handle_message` swap). Uses shared Agent Card factory. Budget ~60 lines (not 30).
8. **Phase 3.1 watchdog:** nats-py native; subscribes `agents.*.heartbeat`; publishes
   status under `agents.watchdog-1.outbox`. Card declares `metadata.runtime.roles = ["watchdog"]`.
9. **Phase 4 API update:** `register_hand_off(OnCondition(target=AgentTarget|...))` and
   `autogen.agentchat.group.AutoPattern` / `RoundRobinPattern` — not Swarm. Pin
   `ag2>=0.12,<0.13`. `A2aRemoteAgent` async-only (`a_run` not `run`).
10. **New Session 4.4 — A2A wrapper.** `A2aAgentServer(agent, agent_card=card).build()` +
    NATS bridge process per AG2 agent. Serves `/.well-known/agent-card.json` alongside the
    existing `agents.{id}.register` publish.
11. **Optional Phase 5+ — Bridge adapter for Hermes/ACP runtime.** Covered by the bridge
    pattern in this spec; only implemented if/when we onboard Nous Hermes Agent or
    similar. Not required for v0.1 completion.
12. **Session budget:** realistic is now 14–17 sessions, not 7–10.

## Open questions flagged for spec review

- **Agent Card storage durability:** v0.1 uses publish-on-connect + aggregator cache.
  JetStream KV bucket (key = `agent_id`, value = Agent Card JSON) would give durable
  fleet-wide discovery. Defer to v0.2.
- **`hop_count` threading through AG2 hand-offs:** `register_hand_off` in AG2 v0.12 does
  not surface the envelope layer; verify that our adapter increments `hop_count` before
  publishing the outbound delegation envelope on NATS — not after, not inside AG2's chat
  history. Test during Phase 4.2.
- **Aggregator dashboard surface for poison messages:** new panel in the agent registry
  view, or inline next to per-agent queue depth. UX decision during Phase 3.2.
- **NATS transport binding URI stability:** we mint `https://edgecitadel.local/ext/nats-binding/v1`
  as a placeholder. If we later publish EdgeCitadel as an open project, the URI should
  move to a stable public domain and a spec document should live at that URL describing
  the binding semantics. v0.2 cleanup.
- **Hermes/openclaw terminology:** bridge section assumes Nous Research's Hermes Agent
  (ACP-native). If a different runtime is in scope when Phase 5+ onboarding starts,
  the bridge pattern is the same but the upstream protocol details in the bridge
  implementation differ.

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
