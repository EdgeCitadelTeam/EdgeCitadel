# EdgeCitadel Agent Contract (v0.1 draft)

**Status:** Draft. Normative for all new agent implementations; existing agents grandfathered until v0.2.

This document is the single source of truth for what it means to be an agent on EdgeCitadel. Any runtime — AG2, LangGraph, a raw Python script, an ESP32 firmware, a Mac mini running Gemma — that satisfies this contract can join the mesh and interoperate with every other agent.

The contract has three layers. They are independent but must all be implemented for conformance:

1. **Envelope** — the wire format of a single message.
2. **Lifecycle** — the subjects an agent must publish and subscribe to, and the order in which it must do so.
3. **Agent Card** — the self-description an agent publishes at registration so peers know how to address it.

Transport is out of scope here: the contract runs unchanged over the current NATS+MQTT setup ([01-architecture.md](01-architecture.md)) and would run unchanged over Zenoh, Redis pub/sub, or any broker that preserves subject semantics.

---

## 1. Envelope

Every message published to an `agents.*.*`, `tasks.*.*`, or `system.*` subject MUST be a JSON object matching this schema.

### 1.1 Required fields

| Field | Type | Description |
|---|---|---|
| `v` | integer | Envelope version. Current value: `1`. |
| `id` | string (UUID4) | Unique message ID. Used for idempotency. |
| `type` | string | Message type. See §1.3. |
| `sender_id` | string | Agent ID of the publisher. |
| `timestamp` | string (ISO 8601, UTC, ms precision) | Time the message was produced by the sender. |
| `payload` | object | Type-specific body. Schema depends on `type`. |

### 1.2 Optional fields

| Field | Type | When to include |
|---|---|---|
| `recipient_id` | string | For addressed messages (`command`, `result`, `delegation`). Omit for broadcasts and lifecycle messages. |
| `correlation_id` | string | Links a reply to its request. MUST be echoed unchanged on every message in a conversation. |
| `causation_id` | string | ID of the message that directly caused this one. Enables tracing through DAG workflows. |
| `chain_id` | string | Shared across all messages in a delegation chain. Used for loop detection. |
| `ttl_ms` | integer | Sender requests the message be dropped if not delivered within this window. Advisory. Requires cross-mesh clock sync — agents SHOULD run NTP (or equivalent) with ≤1 s skew. If an agent cannot sync its clock, it MUST NOT set `ttl_ms` on outbound messages. |

### 1.3 Canonical `type` values

| `type` | Direction | Required subject | Notes |
|---|---|---|---|
| `register` | agent → broker | `agents.{self}.register` | Payload is the Agent Card (§3). Retained. |
| `heartbeat` | agent → broker | `agents.{self}.heartbeat` | Payload: `{status, cpu_percent, memory_percent, ip_address, power_source?, battery_percent?}`. `power_source ∈ {ac, battery, unknown}`; `battery_percent` (0–100) required when `power_source=battery`. |
| `status` | agent → broker | `agents.{self}.status` | Payload: `{status: online\|offline\|busy\|error, reason?}`. |
| `command` | sender → recipient | `agents.{recipient}.inbox` | Payload: `{body: string, tool_calls?: []}`. |
| `result` | responder → requester | `agents.{requester}.inbox` | Requires `correlation_id`. Payload: `{body, error?}`. |
| `delegation` | agent → agent | `agents.{recipient}.inbox` | Like `command` but `chain_id` required. |
| `log` | agent → broker | `agents.{self}.log` | Payload: `{level, message, context?}`. |
| `broadcast` | sender → all | `system.broadcast` | No `recipient_id`. |
| `task.assign` / `task.progress` / `task.complete` / `task.failed` | various | `tasks.{id}.*` | See [07-task-management.md](07-task-management.md). |

### 1.4 Deprecations from today's format

The current informal format ([05-messaging.md:42](05-messaging.md:42)) has several duplicated fields. The spec deprecates them. Parsers MUST accept both during the migration window; producers SHOULD emit only the canonical form.

| Deprecated | Canonical | Notes |
|---|---|---|
| `message_type` | `type` | Same meaning; drop `message_type` in v0.2. |
| `content`, `message` at top level | `payload.body` | Keep `payload` as the single body container. |
| `receiver_id` | `recipient_id` | Rename; "recipient" is less ambiguous. |

### 1.5 Sender identity

`sender_id` is self-declared and therefore untrusted until bound to transport-layer authentication. Until per-agent auth ships (§6), the contract requires a weaker but enforceable check:

- A receiver (peer or watchdog) MUST reject any message whose `sender_id` does **not** match a currently-retained Agent Card on `agents.{sender_id}.register`. No Card → no identity → drop and log.
- When per-agent credentials are deployed, the broker/aggregator MUST additionally verify that the authenticated principal matches `sender_id`, and reject otherwise.
- Receivers SHOULD log — not silently drop — rejected messages. Identity spoofing attempts are a signal worth keeping.

This does not prevent a credentialed agent from lying about message contents. It prevents uncredentialed senders from impersonating a known peer.

### 1.6 Size and encoding

- UTF-8, no BOM.
- Hard limit: 1 MB per message (NATS default; enforced by the broker).
- If the body exceeds ~256 KB, SHOULD use `payload.body_ref` pointing to a JetStream object store entry instead of inlining.

---

## 2. Lifecycle

Every agent is a state machine. The contract defines five states and the transitions between them. An implementation that respects this state machine is observable and debuggable; one that does not is a black box.

```
          ┌─────────────┐
          │   offline   │  (initial; published on clean shutdown)
          └──────┬──────┘
                 │ connect + auth
                 ▼
          ┌─────────────┐
          │  connecting │
          └──────┬──────┘
                 │ publish register (retained)
                 ▼
          ┌─────────────┐
    ┌────►│   online    │◄────┐
    │     └──────┬──────┘     │
    │            │ receive command / delegation
    │            ▼             │
    │     ┌─────────────┐     │
    │     │    busy     │─────┘ publish result / progress
    │     └──────┬──────┘
    │            │ unrecoverable error
    │            ▼
    │     ┌─────────────┐
    └─────│    error    │  (exponential backoff; see §2.3)
          └─────────────┘
```

**Error state is recoverable, not terminal.** The transition `error → online` is the common case; `error → offline` is what happens if recovery attempts exhaust. See §2.3 for required backoff behavior.

### 2.1 Required subjects

Every conformant agent MUST:

- **Publish** on `agents.{self}.register` exactly once per session, on connect, with the Agent Card as payload. MUST use the broker's retained-message or last-value semantics so late subscribers see it.
- **Publish** on `agents.{self}.heartbeat` at an interval declared in its Agent Card (`heartbeat_interval_sec`, default 30). Missed heartbeats beyond 3× the interval cause the broker to mark the agent offline.
- **Publish** on `agents.{self}.status` when its state changes. Final message before shutdown MUST be `{status: "offline"}`.
- **Subscribe** to `agents.{self}.inbox`. This is where commands, results, and delegations all arrive.
- **Subscribe** to `system.broadcast` unless the Agent Card declares `listens_broadcast: false`.

### 2.2 Optional subjects

Conformance does not require — but the contract reserves — these subjects:

- `agents.{self}.outbox` — a public replay of what `{self}` is sending. Useful for observers; not used for routing.
- `agents.{self}.log` — structured log events.
- `tasks.*` — task lifecycle; see [07-task-management.md](07-task-management.md).

### 2.3 Behavior guarantees

Every conformant agent:

- MUST send `{status: "offline"}` on clean shutdown. The broker uses this to suppress the 3×heartbeat wait.
- MUST echo `correlation_id` unchanged on every reply.
- MUST drop messages whose `chain_id` it has already seen with the same body hash (loop protection).
- SHOULD refuse commands when `busy` by replying with `type: result, payload: {error: "busy", retry_after_ms: ...}` rather than silently queuing.
- MUST NOT assume any particular transport. The envelope is identical whether delivered via NATS, MQTT, or a future substrate.

#### Error-state behavior (normative)

While in `error`, an agent:

- MUST continue to publish `heartbeat` with `status: error` and a short `reason` field so observers can distinguish a stuck agent from a crashed one.
- MUST remain subscribed to `agents.{self}.inbox`. Dropping the subscription during recovery leaves senders blocked on replies they'll never get.
- MUST reply to any `command` or `delegation` arriving during `error` with `type: result, payload: {error: "agent_error", reason, retry_after_ms}`. No silent drops.
- MUST attempt recovery using exponential backoff — SHOULD start at 1 s, cap at 60 s, jitter ±20%.
- After 10 consecutive failed recovery attempts, MUST transition to `offline` (clean shutdown), publish `{status: "offline", reason}`, and exit. Restart is the supervisor's job, not the agent's.

### 2.4 Watchdog role

The contract defines a **Watchdog** as any agent (typically the aggregator, but it can be a standalone process or a peer) that:

- Subscribes to `agents.*.heartbeat` and `agents.*.status`.
- Tracks last-seen timestamp per agent, keyed by the `heartbeat_interval_sec` declared in that agent's retained Card.
- When an agent exceeds 3× its declared interval without a heartbeat, publishes a synthetic `status` message on its behalf: `{sender_id: "watchdog-{wd_id}", type: "status", payload: {subject_agent: "{stale_id}", status: "offline", reason: "heartbeat_timeout"}}`. Note that `sender_id` is the watchdog, not the stale agent — the contract forbids impersonation.
- SHOULD clear the retained Agent Card for timed-out agents so late subscribers don't see ghost peers.

Multiple Watchdogs MAY run concurrently (e.g., one per site in a federated setup). They coordinate via idempotent publishes — duplicate offline declarations are harmless.

Watchdog-role conformance is declared in the Agent Card via `runtime.roles: ["watchdog"]`.

---

## 3. Agent Card

The Agent Card is the payload of the `register` message. It tells the network who this agent is, what it can do, and how to talk to it. A machine-readable directory of live agents is simply the set of currently retained Cards.

### 3.1 Schema

```json
{
  "v": 1,
  "agent_id": "jeeves",
  "display_name": "Jeeves",
  "role": "IoT Controller",
  "device_type": "raspberry_pi",
  "runtime": {
    "framework": "ag2",
    "framework_version": "0.4.1",
    "model": "gemma-3-27b-it",
    "model_host": "local",
    "roles": ["worker"]
  },
  "tags": ["indoor", "low-latency", "temperature-sensing"],
  "capabilities": [
    {
      "name": "query_sensors",
      "description": "Read temperature and humidity from household sensors.",
      "modalities": ["text"],
      "tool_schema_ref": "mcp://jeeves/query_sensors"
    }
  ],
  "endpoints": {
    "inbox": "agents.jeeves.inbox",
    "mcp": "http://192.168.1.20:8765/mcp",
    "a2a": null
  },
  "heartbeat_interval_sec": 30,
  "listens_broadcast": true,
  "chain_depth_limit": 3,
  "timestamp": "2026-04-21T10:00:00.000Z"
}
```

### 3.2 Field meanings

| Field | Purpose |
|---|---|
| `runtime.framework` | Identifies how the agent is built. Used by observability; does not change wire behavior. Examples: `ag2`, `langgraph`, `openclaw`, `custom`. |
| `runtime.model` / `model_host` | What LLM is behind this agent, and whether it's local or remote. Used by the registry to answer "which nodes run Gemma locally?" |
| `runtime.roles` | Contract-defined roles this agent fulfills. Reserved values: `worker` (default), `watchdog`, `planner`. Other values are allowed and ignored by receivers that don't recognize them. |
| `tags[]` | Free-form, lowercase, hyphenated labels. Planners use these for cheap agent selection (e.g., "find me an `outdoor` agent with `low-latency`") without parsing capability schemas. Advisory — receivers MUST NOT use tags for authorization. |
| `capabilities[]` | Typed advertisements. An empty list means "conversational only, no structured tools." |
| `capabilities[].tool_schema_ref` | Optional pointer to an MCP tool schema. When present, the capability is invocable as a typed tool, not just via freeform `command`. |
| `endpoints.mcp` | If the agent exposes an MCP server, peers can call its tools directly instead of routing through `inbox`. |
| `chain_depth_limit` | Max number of delegation hops the agent will accept before refusing. Used to bound cascades. |

### 3.3 Why Card rides on `register`, not a side channel

The Card is the registration. Separating them creates two sources of truth and a window where an agent is "registered but not described." Keeping them fused guarantees: if a peer sees you, it has your Card.

---

## 4. Conformance levels

Not every agent needs every capability. The contract defines three levels.

| Level | Requires | Who this is for |
|---|---|---|
| **L1 — Reachable** | Envelope §1, lifecycle §2, Agent Card §3. Can receive `command`, reply with `result`. | ESP32 sensor, shell-script agent, any minimal worker. |
| **L2 — Collaborative** | L1 + handles `delegation`, enforces `chain_id` loop protection, respects `chain_depth_limit`. | AG2 agents, LangGraph agents, anything that delegates to peers. |
| **L3 — Typed** | L2 + publishes `tool_schema_ref` for each capability + runs an MCP endpoint. | Agents whose capabilities need to be invoked programmatically by planners. |

An implementation declares its level in `runtime.conformance: "L1" | "L2" | "L3"` (optional; default L1).

---

## 5. Reference adapters

The contract is only useful if it's cheap to implement. Each reference adapter is a thin wrapper that puts an existing framework behind the contract.

- **[ag2-adapter](../adapters/ag2/)** (planned) — wraps an AG2 `ConversableAgent`. Maps AG2's `send`/`receive` hooks to `inbox`/`outbox`. Publishes Agent Card from the AG2 agent's `description`.
- **[langgraph-adapter](../adapters/langgraph/)** (planned) — invokes a LangGraph graph per inbound `command`. Emits `task.progress` from node transitions.
- **[shell-adapter](../adapters/shell/)** (planned) — minimal L1 reference. ~100 lines of Python. Runs an arbitrary shell command on each `command`; the stdout becomes the `result.body`. Proof that L1 is trivial.
- **[openclaw-client](../openclaw-client/mqtt-listener.js)** — existing; will be migrated to the formal envelope in v0.2.

Each adapter is small on purpose. If an adapter is more than ~500 lines, the contract is wrong, not the adapter.

---

## 6. Non-goals (v0.1)

Explicitly out of scope for this version, to keep the contract small:

- **Transport selection.** We assume NATS/MQTT today. Swapping substrates is a separate decision.
- **Per-agent authentication.** Single shared token for now. Per-agent JWTs come with the CollabIoT-style onboarding work.
- **Streaming tokens.** The envelope reserves `tasks.{id}.stream` but does not define token-level semantics yet.
- **Cross-site federation.** `deployment` field is reserved but unused.
- **Formal schema validation.** We'll add a JSON Schema + a CI linter after the shape stabilizes.

---

## 7. Migration plan

Steps 2 and 3 run **in parallel, not in sequence**. An external review recommended schema-before-implementation; we do both at once. Schema-first locks the shape before implementation has a chance to reveal over-specification; implementation-first produces ad-hoc JSON that's hard to validate. Building them together — with each catching the other's mistakes — is how the contract actually stabilizes.

1. **Merge this doc.** Freeze v0.1 semantics.
2. **Write the JSON Schemas** (`schemas/envelope.v1.json`, `schemas/agent-card.v1.json`) mirroring §1 and §3. *(parallel with step 3)*
3. **Write the shell-adapter** as the L1 reference. Validate every outbound message against the schema from step 2. Every schema change triggers a re-run; every adapter awkwardness triggers a spec change. *(parallel with step 2)*
4. **Migrate `openclaw-client`** to emit canonical field names (dual-emit during the window, stop emitting deprecated names in v0.2).
5. **Write the AG2 adapter.** First real test of framework-agnosticism.
6. **Deploy a Watchdog** (standalone process or aggregator extension) and turn on schema validation of all incoming messages at the aggregator. Log non-conformant traffic but don't drop yet — the logs tell us where the spec is wrong.

Everything after step 5 is new capability work, not contract work.
