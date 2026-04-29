# Phase 3 Design — Watchdog Adapter + Dashboard Agent Registry

Status: design-complete, pending user review
Date: 2026-04-29
Branch (target): TBD; will branch from `origin/feat/phase1-followups` (PR #6) before Phase 3 work begins
Author: collaborative brainstorm
Builds on:
- `docs/superpowers/specs/2026-04-23-agent-messaging-design.md` (rev 7) — canonical envelope vocabulary, JetStream advisory, heartbeat semantics
- `docs/superpowers/specs/2026-04-24-gemma-adapter-design.md` — adapter common skeleton patterns
- `docs/superpowers/plans/2026-04-23-agent-messaging-v0.1-phase-1.md` — Phase 1 implementation handoff (Phase 3 sketch in §"Phases 2–5")

## Summary

Phase 3 ships **operational hardening** for the v0.1 fleet: a watchdog adapter that detects offline agents and synthesises `recipient_offline` failures so callers don't hang, plus a dashboard registry tab that gives operators a fleet-wide view of card metadata, heartbeat freshness, queue depth, poison count, and online/offline state.

The design is two sessions, one plan, one spec:

- **Phase 3.1 — Watchdog adapter.** New native nats-py adapter at `adapters/watchdog/`. First-class fleet member (`runtime.kind: native, runtime.roles: [watchdog]`). Subscribes to `agents.*.outbox` (audit feed), `agents.*.heartbeat`, and the `MAX_DELIVERIES` advisory. Publishes synthesised `result` envelopes with `task_state: failed, payload.error: "recipient_offline"` when an agent goes silent past `2 × declared heartbeat_interval` (heartbeat-staleness fast path), when a new command targets an already-flagged offline agent (sticky-offline immediate path), or when a JetStream `MAX_DELIVERIES` advisory fires (advisory backstop). All three paths share a common `Nats-Msg-Id: watchdog-syn-{task_id}` dedup key so duplicates are collapsed by JetStream.
- **Phase 3.2 — Dashboard agent registry panel.** New top-level "Registry" tab in the dashboard (5th tab, keyboard `5`). Fleet-wide table sorted by state + heartbeat-age. New aggregator endpoint `GET /api/registry` joins agents, queue depth, and poison count in one snapshot. Live updates via the existing WebSocket bridge (`agent_registered`, `agent_status_change`, new `agent_deleted` event). The sidebar gets a roles-based filter so infrastructure agents (`watchdog`, `aggregator`) only appear in the Registry tab, not in the Chat sidebar — operators delegate work to workers, not to infrastructure.

Failure detection latency drops from `ack_wait × max_deliver` (1.5 min for shell, 15 min for Gemma) to ~30–65 s for default-interval (30 s) agents. The advisory backstop preserves correctness when the fast path misses — e.g., during the watchdog's own restart window.

## Problem

Phase 1 shipped the messaging foundation: strict envelope validation, JetStream WorkQueue, durable per-agent inboxes, MAX_DELIVERIES advisory subscriber for poison logging, `/api/agents`, `/api/agents/{id}/queue`, and `/api/poison`. Phase 2 shipped the Gemma reasoner. PR #6 ships seven Phase 1 follow-ups including the WebSocket bridge, server-side test/prod separation, and test-stack fixes.

Two operational gaps remain:

1. **Senders hang on offline recipients.** When agent A publishes `command` to `agents.B.inbox` and B is offline, the message sits in JetStream until `ack_wait × max_deliver` elapses (15 min for an LLM-class adapter). A's pending future has no completion path; HTTP callers time out with no information about *why*. The aggregator's existing `on_advisory` handler logs poison events for the dashboard, but doesn't synthesise a result for the original sender — so A still hangs.
2. **No fleet-wide operator view.** The dashboard's `AgentSidebar` is the only fleet surface today. It lists agent IDs and state, but doesn't surface heartbeat freshness, queue depth, or poison-event counts in one place. Operators investigating "why is the system slow?" have to click into each agent individually.

Phase 1 anticipated both gaps:
- Spec rev 6 (lines 759–778 of `2026-04-23-agent-messaging-design.md`) reserved the watchdog as a peer agent with `runtime.roles: [watchdog]` and pinned the synthesised-failure trigger to MAX_DELIVERIES advisory only.
- Phase 1 Tasks 6 + 8 shipped the underlying endpoints (`/api/agents/{id}/queue`, `/api/poison`) the registry tab needs.

Phase 3 closes both.

## Approach

### 3.1 — Watchdog adapter trigger model

Spec rev 6 deliberately rejected heartbeat-staleness as a synthesised-failure trigger because it would have required the watchdog to subscribe to per-agent inbox traffic, which conflicts with the WorkQueue's disjoint-filter rule on `AGENT_INBOX`. Phase 3 revisits this constraint: with **outbox-mirror as the authoritative audit path** (per ADR-0006), the watchdog can derive in-flight task state without subscribing to inboxes — every adapter mirrors its inbox publishes to its own `agents.{self}.outbox` (plain NATS), and the watchdog observes those mirrors.

The phase-3 trigger model is three reinforcing paths sharing one dedup key:

1. **Heartbeat-staleness fast path** (primary). When `now - last_seen[X] > max(2 × declared_interval, 20 s) + 5 s tolerance`, fan out synthesised failures for every entry in `pending_tasks[X]`. Default 30 s interval → ~65 s detection.
2. **Sticky-offline immediate path** (B2). Once X is in the `offline_agents` set, observing a new `command` or `delegation` to X on the outbox feed → immediate synthesis, no entry added to `pending_tasks`. New commands to a known-dead agent fail in milliseconds.
3. **Advisory backstop** (defensive). The MAX_DELIVERIES advisory subscription remains active. Any task the fast paths missed (cold-start, watchdog restart gap, dropped outbox messages) gets a synthesised failure when JetStream eventually terminates the message after `ack_wait × max_deliver`.

All three paths build the same envelope shape and publish to `agents.{original_sender}.inbox` with `Nats-Msg-Id: watchdog-syn-{task_id}`. JetStream's 5-minute `duplicate_window` collapses double-fires.

This diverges from spec rev 6 and is recorded in **ADR-NN (Phase 3)** — see [ADRs](#adrs-to-draft).

### 3.2 — Registry panel placement

Three placements were considered: enriching `AgentDetail.jsx` only (per-agent deep-dive); a new top-level "Registry" tab (fleet table); or both (enriched sidebar + AgentDetail). The chosen design is the **new top-level tab** (option B) for one reason: backend infrastructure agents (`watchdog`, `aggregator`) are conceptually different from worker agents an operator delegates to. Mixing them in the chat sidebar is misleading. The Registry tab is the right home for an "everything the broker knows about" view; the Chat sidebar stays curated to delegation targets via a roles-based filter.

## Goals

1. Senders observe a `task_state: failed, payload.error: "recipient_offline"` result within ~65 s of an agent going silent (down from 1.5–15 min via the advisory path).
2. New commands targeting an already-offline agent fail within milliseconds (sticky-offline path).
3. The watchdog has no persistent state; it rebuilds from live traffic on restart, with the advisory backstop covering the cold-start gap.
4. Operators see fleet state — card metadata, heartbeat freshness, queue depth, poison count, online/offline — in one Registry tab, refreshed every ~5 s and patched live via the existing WebSocket bridge.
5. Infrastructure agents (`watchdog`, `aggregator`) are visible in the Registry tab but excluded from the Chat sidebar, so delegation UX is uncluttered.
6. The watchdog is a first-class fleet member with its own A2A Agent Card; synthesised failures are attributed to `sender_id: watchdog-1` for honest provenance.

## Non-goals (Phase 3)

Items deferred from Phase 3 with forward-compat hooks already in place:

- **Persistent watchdog state across restarts.** Bootstrap is "rebuild from current traffic + advisory backstop catches stragglers." v0.2 if persistence becomes needed; the watchdog could query `/api/messages` on boot.
- **Persistent `offline_since` per agent** ("agent X has been down for 4 days"). Requires watchdog persistence + a new `offline_history` table; v0.2.
- **Per-agent disable for synthesised failures.** No use case yet. v0.2 hook: a card-metadata flag (`runtime.synthesise_failures: false`) plus a watchdog filter.
- **Watchdog admin commands** (`list_offline`, `dump_pending_tasks`). Inbox handler rejects all unknowns in v0.1; future commands slot in without schema change.
- **Mac Mini launchd plist for the watchdog.** Already on Phase 5's roadmap (line 136 of `docs/roadmap.md`).
- **Multi-watchdog HA / hot-spare.** v0.2+; durable consumer slot enforces single-inbox-processor today, and outbox/heartbeat/advisory are plain NATS so multiple watchers are tolerated under `Nats-Msg-Id` dedup.
- **Synthesised-failure visualisation on the Flow tab.** Backlog. Synthesised envelopes carry `payload.trigger` and `sender_id: watchdog-1`; frontend can render them with a distinct badge later.
- **Registry table column collapse on narrow viewports.** v0.1 acceptable as horizontal-scroll.
- **Sort/filter persistence in URL or `appStore`.** Component-local state for v0.1.
- **`queue_changed` / `poison_event` WS push events.** 5 s polling is fine for v0.1 fleet sizes (~20 agents max). Bridge already supports custom event types via `_hub_event(name, data)`; future delta is ~20 LOC.
- **Bridge for AG2 / Hermes adapters.** Phase 4 scope. Watchdog observes `agents.*.outbox` regardless of adapter `runtime.kind`; bridge agents will be tracked the same way.

## Design

### Watchdog adapter (Phase 3.1)

#### Identity & configuration

`adapters/watchdog/config.yaml`:
```yaml
agent_id: watchdog-1
name: Watchdog
description: Detects offline agents and synthesises recipient_offline failures
runtime:
  kind: native
  roles: [watchdog]
  heartbeat_interval_sec: 30
  deployment: default
skills: []                  # empty: not a delegation target
capabilities:
  streaming: false
```

The card factory (`adapters/_common/agent_card.build_card()`) reads this YAML and produces an A2A v1.0 Agent Card. The watchdog publishes its card on `agents.watchdog-1.register` at startup and heartbeats every 30 s on `agents.watchdog-1.heartbeat`.

#### Subscriptions

| Subject | Transport | Purpose |
|---|---|---|
| `agents.*.register` | plain NATS | Populate `declared_interval[agent_id]` from the card's `runtime.heartbeat_interval_sec`; idempotent on re-register |
| `agents.*.outbox` | plain NATS | Observe `(task_id, sender, recipient)` tuples on `command` / `delegation`; clear entries on `result` |
| `agents.*.heartbeat` | plain NATS | Update `last_seen[agent_id]`; clear from `offline_agents` |
| `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>` | plain NATS | Backstop synthesis for tasks the fast path missed |
| `agents.watchdog-1.inbox` | JetStream pull consumer (`max_ack_pending=1`, `ack_wait=30 s`, `max_deliver=3`) | Be-a-good-fleet-citizen; default handler rejects unknown commands |

Note: the aggregator already subscribes to the MAX_DELIVERIES advisory for poison logging. The watchdog adds a *second* subscriber on the same subject — both are plain NATS, no consumer-slot conflict.

#### In-memory state

```python
last_seen: dict[str, datetime]              # agent_id -> last heartbeat ts
declared_interval: dict[str, int]           # agent_id -> heartbeat_interval_sec from card (or 30 default)
pending_tasks: dict[str, dict[str, str]]    # recipient_id -> {task_id: original_sender}
offline_agents: set[str]                    # sticky flag, cleared on next heartbeat
```

No persistence. State rebuilds from live traffic on restart.

#### Outbox handler

For each envelope on `agents.*.outbox`:
- `type ∈ {command, delegation}`:
  - If `recipient_id ∈ offline_agents`: synthesise immediate failure (sticky-offline path, B2). Skip adding to `pending_tasks` — already failed.
  - Otherwise: `pending_tasks.setdefault(recipient_id, {})[task_id] = sender_id`.
- `type == result`: `pending_tasks.get(sender_id, {}).pop(task_id, None)`. (The result is FROM the worker; the worker's `agent_id == sender_id` of the result envelope; the relevant `pending_tasks` entry is keyed by the worker. Verify in tests.)
- All other types: no-op.

#### Register handler

For each `agents.X.register` envelope:
- Validate the card via `validator.validate_register()`; drop on validation error.
- `declared_interval[X] = card["metadata"]["runtime.heartbeat_interval_sec"]`.
- Idempotent — a re-register simply overwrites the entry.

#### Heartbeat handler

For each `agents.X.heartbeat` envelope:
- `last_seen[X] = ts`.
- If `X ∈ offline_agents`: `offline_agents.discard(X)`; publish a `log` envelope at `INFO` level on `agents.watchdog-1.log` (`"agent X back online after offline_since=..."`).
- If `X ∉ declared_interval` (no register seen yet): default to 30 s until a register lands. The threshold computation in the staleness loop uses `declared_interval.get(X, 30)`.

#### Staleness check loop

A single asyncio task runs every 5 s:

```python
while not shutdown:
    await asyncio.sleep(5)
    now = datetime.now(timezone.utc)
    for X, ts in list(last_seen.items()):
        if X in offline_agents: continue
        interval = declared_interval.get(X, 30)
        threshold = max(2 * interval, 20) + 5  # +5 s tolerance
        if (now - ts).total_seconds() > threshold:
            offline_agents.add(X)
            await fan_out_failures(X)
```

`fan_out_failures(X)`:
- Snapshot `pending_tasks[X]` (copy then iterate to avoid concurrent mutation).
- For each `(task_id, original_sender)`:
  - Build the synthesised envelope (shape below).
  - JetStream-publish to `agents.{original_sender}.inbox` with `Nats-Msg-Id: watchdog-syn-{task_id}`.
  - Publish a `log` envelope at `WARN` to `agents.watchdog-1.log` for dashboard / Logs tab visibility.
- Drain the entries from `pending_tasks[X]` after fan-out.
- Publish a single `status` envelope to `agents.watchdog-1.outbox` describing the offline transition: `{agent_id: X, agent_state: offline, offline_since: ts}`. **Sender is `watchdog-1`** — not impersonating X. The payload describes the observation.

#### Advisory backstop handler

For each `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.<agent>.<consumer>`:
- Parse `agent_id` from the subject tail.
- Extract `original_sender` and `task_id` from the advisory's headers/payload (same parsing the aggregator already does in `on_advisory`).
- Build the synthesised envelope with `payload.trigger = "max_deliveries_advisory"`.
- Publish with `Nats-Msg-Id: watchdog-syn-{task_id}`.
- If the fast path already fired for this `task_id`, JetStream's 5-min dedup window collapses the double publish — no harm, no state to clean up.

#### Inbox handler

The watchdog holds a durable consumer on `agents.watchdog-1.inbox` per the spec convention (every adapter does this). Default handler rejects all `command` and `delegation` envelopes:

```python
{
  "v": 1, "id": uuid4(), "type": "result",
  "sender_id": "watchdog-1",
  "recipient_id": env["sender_id"],
  "task_id": env["task_id"],
  "context_id": env.get("context_id"),
  "timestamp": now_iso(),
  "task_state": "rejected",
  "payload": {"reason": "unknown_command"}
}
```

Future versions can extend this to support admin commands (`list_offline`, `dump_pending_tasks`) without schema change.

#### Synthesised envelope shape

All three trigger paths produce the same envelope:

```python
{
  "v": 1,
  "id": uuid4(),
  "type": "result",
  "sender_id": "watchdog-1",
  "recipient_id": original_sender,
  "task_id": original_task_id,
  "context_id": original_context_id,   # echoed if known; null otherwise
  "timestamp": now_iso(),
  "task_state": "failed",
  "payload": {
    "error": "recipient_offline",
    "offline_agent_id": X,
    "detected_at": now_iso(),
    "trigger": "heartbeat_staleness" | "sticky_offline" | "max_deliveries_advisory"
  }
}
```

`payload.trigger` is observability metadata — the dashboard can later render the path that produced the failure. JetStream dedup collapses duplicate publishes from different paths via the shared `Nats-Msg-Id`.

#### Restart / bootstrap semantics

On startup:
1. Connect to NATS, register card, start heartbeat task (per `_common/template.py`).
2. Subscribe to outbox / heartbeat / advisory; create durable inbox consumer.
3. Begin the 5 s staleness check loop.
4. All state (`last_seen`, `pending_tasks`, `offline_agents`) starts empty.

For ~30 s after start, the watchdog is in cold-start: it doesn't yet know about agents that aren't actively heartbeating, and `pending_tasks` is empty. The advisory backstop covers any in-flight tasks orphaned during this window. After 30 s, the watchdog has full liveness for any agent that has heartbeated. Steady-state.

No DB seed. No aggregator query on boot. Self-healing via traffic.

#### Edge cases

| Case | Behaviour |
|---|---|
| Agent X recovers mid-fan-out | Real `result` arrives later; aggregator's `pending_tasks` future resolution + JetStream dedup collapse the duplicate. First envelope per `task_id` wins. |
| Sender went offline too | Synthesised failure sits in `agents.{sender}.inbox` until reconnect or `max_age=24 h` expires. Acceptable. |
| Watchdog itself goes offline | Synthesised failures stop arriving. Aggregator's existing `on_advisory` handler still records poison events for the dashboard, so operators see the symptom. Documented limitation. |
| Two watchdog instances accidentally running | Durable consumer slot on `agents.watchdog-1.inbox` enforces one for inbox traffic. Both observe outbox / heartbeat / advisory (plain NATS); duplicate synth publishes collapsed by `Nats-Msg-Id` dedup. Safe but wasteful — startup probe (`nats consumer info AGENT_INBOX watchdog-1_inbox`) is a future hardening step. |
| Cold-start window: command sent at t=0, watchdog starts at t=10 | If agent processes the command, watchdog's outbox sub eventually sees the result and clears state. If agent is offline, MAX_DELIVERIES advisory at t≈900 (worst case for Gemma) catches it via backstop. Acceptable. |

### Aggregator additions (Phase 3.2 backend)

#### `GET /api/registry`

Single snapshot endpoint joining agents + queue depth + poison count.

**Response:**
```json
[
  {
    "agent_id": "gemma-1",
    "card": { ... },
    "agent_state": "online",
    "last_heartbeat": "2026-04-29T10:15:23.412Z",
    "last_register": "2026-04-29T08:02:11.001Z",
    "deployment": "default",
    "heartbeat_interval_sec": 30,
    "queue": {"pending": 0, "ack_pending": 1},
    "poison_count": 0
  }
]
```

**Implementation:**
- `db.list_agents()` — already returns first 6 fields.
- Per agent: JetStream `consumer_info(stream="AGENT_INBOX", consumer=f"{agent_id}_inbox")` → `{num_pending, num_ack_pending}`. Failures (consumer missing) → `{pending: 0, ack_pending: 0}`, no exception.
- Per agent: `SELECT COUNT(*) FROM poison_events WHERE agent_id=?`.
- Total cost for N agents: 1 SQL + N JetStream calls + N SQL counts. ≤5 ms for N≤20. Optimisation (single `GROUP BY` query) is a future delta if N grows.
- Endpoint accepts `?deployment=<filter>` for parity with the existing test-data toggle. Default returns all deployments; frontend filters at presentation time.

`summary` and `description` per `python-backend.md` rule. Pydantic response model in `aggregator/models.py`.

#### `agent_deleted` WebSocket event

`DELETE /api/agents/{id}` already exists (Phase 1 Task 6). It currently doesn't fire a hub event; adding one is ~5 LOC:

```python
await self.router._hub_event("agent_deleted", {"agent_id": agent_id}, agent_id=agent_id)
```

The Registry tab patches its row table on this event.

#### No changes to existing endpoints

`/api/agents`, `/api/agents/{id}/queue`, `/api/poison` continue to work as before. Registry uses `/api/registry` as its primary feed; AgentDetail continues to use the per-agent endpoints it already polls.

### Frontend (Phase 3.2)

#### Sidebar roles-based filter

`AgentSidebar.jsx` filters out infrastructure roles from the chat-target list:

```js
const isOperator = (a) => {
  const roles = a.card?.metadata?.['runtime.roles'] || []
  return !roles.some(r => r === 'aggregator' || r === 'watchdog')
}
const sidebarAgents = agents.filter(isOperator)
```

The Registry tab uses the unfiltered list. Same `/api/agents` source, two consumer filters.

#### Registry tab

- New file: `frontend/src/components/AgentRegistry.jsx`.
- Add to `Layout.jsx`'s `TABS` array as the 5th entry (key `registry`, icon `Server` from `lucide-react`, shortcut `5`). Wire shortcut `5` in `App.jsx`'s key handler.

**Columns** (left → right): Agent ID · Roles · Kind · State · Heartbeat age · Queue (p / ap) · Poison · Deployment (hidden unless `showTestAgents`).

**Default sort:** `offline-first → online`, ties broken by `heartbeat_age desc`. Click a column header to re-sort. Sort state is component-local `useState`.

**Row click:** `setSelectedAgent(row.agent_id) + setActiveTab('detail')` — drills into the existing `AgentDetail.jsx` surface.

**Test-data toggle:** reuses the global `showTestAgents` from `appStore`. When off, hide rows where `deployment === 'test'`.

**Loading / empty / error:** consistent with AgentDetail's existing patterns. Error toast via `react-hot-toast`; table keeps stale rows on screen.

#### Registry tab data lifecycle

```
mount                      → GET /api/registry → render
every 5 s                  → GET /api/registry → reconcile
WS agent_registered        → patch row (add)
WS agent_status_change     → patch row (state)
WS agent_deleted           → patch row (remove)
local 1 s tick             → recompute heartbeat-age from each row's last_heartbeat
WS reconnect               → immediate GET /api/registry to resync
```

Heartbeat-age countdown is local clock-driven; the 5 s refetch keeps `last_heartbeat` itself fresh.

#### Mobile / narrow viewport

Table is horizontally scrollable inside its container (`overflow-x-auto`). Critical columns (ID, State, Heartbeat) are sticky-left via `position: sticky` so row identity stays visible while scrolling right. No column collapse logic in v0.1.

## Verification

Per session, minimum verification:

1. **Watchdog registers:** after `python -m adapter` from `adapters/watchdog/`, `GET /api/agents/watchdog-1` returns the card with `runtime.roles: ["watchdog"]`.
2. **Watchdog heartbeats:** `GET /api/agents/watchdog-1` shows `last_heartbeat` updating every 30 s.
3. **Heartbeat fast-path synthesises failure:** test agent registered with `heartbeat_interval_sec: 10` is killed mid-task; original sender receives a `result` with `task_state: failed, payload.error: "recipient_offline"` within 30 s.
4. **Sticky-offline immediate path:** with the test agent killed and flagged offline, sending a new `command` to it produces a synthesised failure within ms (visible in the Logs tab WARN entry).
5. **Advisory backstop:** with the watchdog stopped, send a command to an unregistered `agent_id`; aggregator's `poison_events` row appears after `ack_wait × max_deliver`. Restart the watchdog; existing in-flight tasks for that agent eventually trigger advisory-driven synth (validates the cold-start window).
6. **Dedup across paths:** trigger heartbeat staleness AND wait for the advisory; assert the original sender receives exactly one `result` envelope per `task_id`.
7. **Inbox unknown command rejected:** publish a `command` to `agents.watchdog-1.inbox`; observe a `result` with `task_state: rejected, payload.reason: "unknown_command"`.
8. **`/api/registry` shape:** `curl http://localhost/api/registry` returns the documented JSON shape with all current agents, queue counts, and poison counts.
9. **Registry tab renders:** dashboard navigation to Registry shows the table; tab key `5` switches to it; click a row drills into AgentDetail; back button returns.
10. **Sidebar filter:** AgentSidebar shows worker/reasoner agents but not `watchdog-1` or `aggregator`; toggle off-by-default.
11. **WebSocket patches:** with the dashboard open on the Registry tab, register a new agent → row appears immediately (no 5 s wait); flip an agent offline → state badge updates immediately.
12. **Test-data toggle parity:** with `showTestAgents` off, registry hides `runtime.deployment: test` rows; toggle on → they appear.

End-to-end Playwright specs codify items 3 and 9–12.

## Testing strategy

### Watchdog adapter unit + integration tests

`adapters/watchdog/tests/`:

| Test | What it covers |
|---|---|
| `test_register_populates_declared_interval` | `agents.X.register` envelope updates `declared_interval[X]` from the card's `runtime.heartbeat_interval_sec` |
| `test_outbox_command_added` | observing `command` on `agents.X.outbox` populates `pending_tasks[X][task_id]` |
| `test_outbox_result_cleared` | `result` from worker outbox clears the entry |
| `test_outbox_delegation_added` | `delegation` envelope behaves like `command` |
| `test_heartbeat_updates_last_seen` | `agents.X.heartbeat` updates `last_seen[X]` |
| `test_heartbeat_clears_offline` | heartbeat from a known-offline X removes X from `offline_agents` |
| `test_staleness_threshold_floor` | 10 s declared interval → effective threshold floor at 25 s (20 + 5) |
| `test_staleness_fan_out` | threshold elapsed → synth result published per pending `task_id` |
| `test_sticky_offline_synthesises_new_command` | (B2) new command to offline X → immediate synth |
| `test_advisory_backstop` | MAX_DELIVERIES advisory → synth with `payload.trigger="max_deliveries_advisory"` |
| `test_dedup_prevents_double_synth` | fast path + advisory → JetStream collapses via `Nats-Msg-Id` |
| `test_inbox_unknown_command_rejected` | command on `agents.watchdog-1.inbox` → result with `task_state="rejected"` |
| `test_card_registration` | card validates against `schemas/agent-card.v1.json`, `runtime.roles=["watchdog"]` |

Conformance suite (`adapters/_common/tests/conformance.py`) is auto-applied: register-card validation, envelope accept/reject, ack semantics.

Tests run against the test NATS broker (the `e2e/test-nats.conf` fix from PR #6).

### Aggregator endpoint tests

`aggregator/tests/test_registry_endpoint.py`:
- Seeded DB with 3 agents (online / offline / error states), 1 poison event, varying deployments.
- `GET /api/registry` returns the expected shape and counts.
- `GET /api/registry?deployment=test` filters correctly.
- Missing JetStream consumer for an agent → graceful `{pending: 0, ack_pending: 0}`, no 500.
- Empty DB → `[]`.

WebSocket integration:
- `agent_registered` event delivered on `register` envelope.
- `agent_status_change` event delivered on `status` envelope.
- `agent_deleted` event delivered on `DELETE /api/agents/{id}`.

### End-to-end (Playwright)

`e2e/specs/phase3-watchdog-fast-path.spec.js`:
1. Stack up. Aggregator + watchdog + test agent (`tester-1`, `runtime.deployment: test`, `heartbeat_interval_sec: 10`) running as a fixture process.
2. Test agent registers + sends one heartbeat. Wait for it to appear in `/api/registry`.
3. POST `/api/command/tester-1` with a long-running payload.
4. Kill the test agent fixture (SIGKILL — no graceful shutdown).
5. Within 30 s (= 2×10 + 5 + 5 cadence), the dashboard's Logs tab shows a WARN entry from `watchdog-1`. The original command's `task_id` resolves to `task_state: failed, payload.error: "recipient_offline"` — visible in TaskBoard.
6. Toast appears in the UI for the WARN log via the existing notification pipeline.

`e2e/specs/phase3-registry-tab.spec.js`:
1. Stack up.
2. Navigate to the Registry tab via tab click + keyboard `5`.
3. Assert table shows online agents (`gemma-1`, `shell-1`, `watchdog-1`, `aggregator`) with state / heartbeat / queue / poison columns rendered.
4. Toggle "Test data" on → test agents appear (those registered with `runtime.deployment: test`).
5. Click a row → drills into AgentDetail; back button returns to Registry tab.
6. Verify column-header sort: click "Heartbeat", confirm rows reorder.

The test agent fixture used in both specs registers with `runtime.deployment: test` so it's filterable from the production-default sidebar (per the test-data convention introduced post-Phase 2 walkthrough).

### Negative / chaos tests

`test_watchdog_restart_resilience` — adapter integration:
1. Two test agents online + heartbeating.
2. Send a command to agent A.
3. Kill agent A AND the watchdog simultaneously.
4. Restart the watchdog.
5. Assert: state rebuilds from outbox + heartbeats; agent A is correctly flagged offline within ~30 s of watchdog restart; advisory backstop synthesises the original task's failure.

`test_two_watchdog_instances_dedup` — manual-only chaos check (not in CI):
1. Run two watchdog processes against the same NATS broker.
2. Trigger heartbeat staleness for an agent.
3. Verify only one synthesised result lands at the original sender (via `Nats-Msg-Id` dedup).

Lives as a doc-test in `adapters/watchdog/README.md`.

### Coverage gates

- All Phase 3.1 unit tests + conformance suite pass before merge.
- Phase 3.2 backend unit tests + at least one of the two E2E specs pass live (per `AGENTS.md` validation policy).
- Spec §"Verification" item 3 verified manually (heartbeat fast-path synth) before merge.

## Documentation impact

Files updated as Phase 3 deliverables:

- `docs/agent-contract.md` — under "Recipient offline", document the three-path trigger model and the failure-detection latency window. Reference the new ADR.
- `docs/05-messaging.md` — expand the watchdog row in the subject inventory: subscribes to `agents.*.outbox` (new), retains heartbeat + advisory subscriptions.
- `docs/08-api-reference.md` — add `GET /api/registry`. Add `agent_deleted` to the WebSocket events table.
- `docs/CHANGELOG.md` — add v0.3 (or unreleased) entry summarising Phase 3.
- `docs/roadmap.md` — mark Phase 3.1 and 3.2 complete; promote any deferred items into the appropriate forward-compat tables.
- `.claude/rules/nats-messaging.md` — note the watchdog as a second subscriber to MAX_DELIVERIES advisory; add `agents.*.outbox` to its subscription list.
- `adapters/watchdog/README.md` — new file. Operational docs (how to run, how to interpret WARN logs, the two-instance chaos doc-test).

ADR drafts:

- `docs/adr/00NN-watchdog-trigger-model.md` — see [ADRs to draft](#adrs-to-draft).

## ADRs to draft

**ADR-NN — Watchdog trigger model: heartbeat-staleness fast path with advisory backstop.**

- **Status:** Proposed (Phase 3.1)
- **Context:** Spec rev 6 (`docs/superpowers/specs/2026-04-23-agent-messaging-design.md`, lines 759–778) explicitly pinned the watchdog's synthesised-failure trigger to the JetStream `MAX_DELIVERIES` advisory only, rejecting heartbeat-staleness as a trigger because it would have required per-agent inbox observation. Phase 3 brainstorm revisited this: with outbox-mirror as authoritative audit (per ADR-0006), the watchdog can derive in-flight task state without subscribing to inboxes.
- **Decision:** Watchdog primary trigger is heartbeat staleness (`now - last_seen > max(2 × declared_interval, 20 s) + 5 s tolerance`), with two reinforcing paths: (a) sticky-offline fast-publish for new commands targeting an already-flagged agent; (b) MAX_DELIVERIES advisory as a backstop for cold-start gaps and tasks not yet observed via outbox. JetStream dedup on `Nats-Msg-Id: watchdog-syn-{task_id}` collapses double-fires across the three paths.
- **Consequences:**
  - Failure detection latency drops from `ack_wait × max_deliver` (1.5–15 min) to `2 × heartbeat_interval + 5–10 s` (~30–65 s for default 30 s interval).
  - In-flight bookkeeping in the watchdog (`pending_tasks` map keyed on outbox observations). State rebuilds from live traffic on restart; no persistence.
  - Spec rev 6 reasoning is preserved for one specific case: the advisory still authoritatively catches everything the fast paths miss (cold-start, watchdog restart gap, dropped outbox messages).
  - First-class `runtime.roles: [watchdog]` identity remains. Synthesised envelopes carry `sender_id: watchdog-1` per spec.

Other Phase 3 decisions handled inline (no ADR required):

- Watchdog as own host process — matches spec rev 6, no divergence.
- Registry tab placement (B) + sidebar roles-based filter — implementation detail in `frontend/src/Layout.jsx`, `AgentSidebar.jsx`.
- `GET /api/registry` endpoint shape — REST surface addition, documented in `docs/08-api-reference.md`.

## Impact on the execution plan

This spec maps to two implementation sessions:

- **Session 3.1 — Watchdog adapter.** New `adapters/watchdog/` directory (adapter.py, config.yaml, tests, README), new ADR draft, doc updates to `agent-contract.md`, `05-messaging.md`, `.claude/rules/nats-messaging.md`. Follows the `adapters/_common/template.py` skeleton.
- **Session 3.2 — Aggregator endpoint + Registry tab.** New `GET /api/registry` endpoint in `aggregator/main.py`, new Pydantic model in `aggregator/models.py`, new `agent_deleted` WS event, new `frontend/src/components/AgentRegistry.jsx`, new tab in `frontend/src/Layout.jsx`, sidebar filter in `AgentSidebar.jsx`, doc updates to `08-api-reference.md`.

Sessions can be merged independently; 3.2 doesn't strictly depend on 3.1 — the Registry tab works against any agent population, including the population without a watchdog row.

The detailed task list (test fixtures, individual code blocks, commit boundaries) is the subject of the implementation plan, drafted via the `superpowers:writing-plans` skill after this spec is approved.
