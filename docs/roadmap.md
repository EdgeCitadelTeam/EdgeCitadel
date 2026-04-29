# EdgeCitadel Roadmap

This file tracks deferred work and the path forward beyond what's currently implemented or scheduled. Two top-level sections:

1. **[Out of scope — deferred enhancements](#out-of-scope--deferred-enhancements)** — items intentionally cut from current specs, with the design hooks already in place to land them later without contract changes.
2. **[Phase handover — delayed-to-later-phases](#phase-handover--delayed-to-later-phases)** — explicit work items pushed to specific future phases, each with the spec entry point.

Last updated: 2026-04-24.

---

## Out of scope — deferred enhancements

Items intentionally NOT in the current spec scope. Each has been brainstormed and consciously deferred; landing them later should be a clean delta, not a redesign.

### Gemma adapter (Phase 2 spec)

| Item | Reason deferred | Forward-compat hook |
|---|---|---|
| **Multi-skill dispatch** (`text.summarize`, `text.classify`, `code.explain`, ...) keyed by `payload.skill_id` | Single-skill is enough to validate the wire contract; per-skill prompt templates compound testing complexity | `skills` array in `config.yaml` is open-ended; a future skill_id dispatcher inside `handle()` adds dispatch without touching the envelope |
| **Conversational memory** keyed by `context_id` (turn history → prepended into prompts) | Smoke scope; memory eviction policy is its own design problem (token budget, summarization, abandonment) | Spec preserves `context_id` from inbound to outbound result; future memory store keys on it |
| **Token streaming** via `task.progress` envelopes (Ollama `stream=true` → batched updates) | Frontend has no WebSocket plumbing today; per-token NATS publishes have measurable cost without user-visible benefit | `capabilities.streaming` is `false`; flipping to `true` is schema-clean. `task.progress` envelope type already exists in v0.1 schema |
| **Non-Ollama backends** (vLLM, llama.cpp, OpenAI-compatible, Anthropic API) | Different failure modes, different auth assumptions, different streaming protocols — each needs its own adapter | Backend is hidden behind `handle()`; a parallel `adapters/openai/` would mirror this directory's shape |
| **Auto-pull on startup** (`ollama pull` if model missing) | 3–8 GB downloads mask "operator forgot to pull" as "adapter is slow"; failure mode confusion | Preflight already detects missing model and exits 2 with a clear message; auto-pull is one branch added |
| **Container packaging** of the Gemma adapter | Ollama runs on the host; containerizing the adapter complicates `OLLAMA_HOST` networking. Phase 5 Mac Mini deploy can run both as host services | Adapter has no Docker assumptions today; future Dockerfile is additive |

### Phase 1 follow-ups (loose ends from the v0.1 messaging rebuild)

Items that surfaced during Phase 1 implementation but were out of plan scope:

| Item | Severity | Notes |
|---|---|---|
| **WebSocket endpoints** (`/ws/stream`, `/ws/agent/<id>`) — `frontend/src/hooks/useWebSocket.js` references them but `aggregator/main.py` doesn't ship them | Medium — frontend currently relies on polling (TaskBoard 5s, AgentDetail queue 5s, poison 30s) | Add `/ws/agent/<id>` to aggregator and wire `task.progress` deliveries to it. Pairs naturally with Phase 2.5 streaming. |
| ~~**Stale `.claude/rules/docker-infra.md` and `nats-messaging.md`**~~ | ~~Low~~ | **Resolved** — both rule files rewritten against shipped v0.1 reality (token-only NATS auth, MQTT off by default, nginx preserve-prefix proxy, aggregator-as-package Dockerfile, JetStream config in seconds). `e2e-testing.md` also refreshed: drops references to deleted helpers (mqtt-client/cleanup/test-data) and the unshipped WS, points at the smoke-config workaround. |
| ~~**`httpx` not pinned in `aggregator/requirements.txt`**~~ | ~~Medium~~ | **Resolved** — `httpx>=0.27` added. FastAPI `TestClient` and any future async HTTP work now have an explicit pin. |
| ~~**`OPENCLAW_API_KEY` env var preserved in `docker-compose.yml`**~~ | ~~Low~~ | **Resolved (retire)** — gone from `.env.example`, `docker-compose.yml`, README, CONTRIBUTING, `docs/02-server-setup.md`, `e2e/docker-compose.test.yml`. Aggregator never read it post-Phase-1; HTTP-level auth is a separate v0.2 design topic. |
| **Legacy `e2e/full-e2e.js`, `e2e/record-demo.js`, `openclaw-client/register.sh`, `openclaw-client/openclaw.conf.example`** still POST to the deleted `/api/publish` endpoint with hardcoded api-keys | Low — non-functional; not invoked by any live workflow | Delete or rewrite to canonical envelope. Doc-cleanup PR. |
| **JetStream test fixture slow-skip (~2 min/test without broker)** | Low — local dev annoyance | Add TCP probe before `nats-py.connect()` so unreachable broker fails fast |
| **`python-backend.md` rule violations**: FastAPI endpoints lack `summary`/`description`; `database.py` has f-string in `PRAGMA table_info({name})` | Low — internal-only, no security risk; PRAGMA is a parameterless SQL form so f-string is acceptable here | Either refactor or update the rule to permit |
| **TaskBoard "Create task" UX removed in Phase 1 Task 16** — no `/api/tasks` endpoint exists in v0.1 | Low — TaskBoard now derives task state from `/api/messages` filtered by `task_id` | Decide: re-add via new endpoint, or stay derived-only. Visible UX change. |
| **Live-stack Phase 1 verification checklist (13 rows in `docs/CHANGELOG.md`)** | Required before tagging v0.1 | Operator step; cannot be automated |
| ~~**`on_register` / `on_heartbeat` don't persist to `messages` table**~~ | ~~Low~~ | **Resolved (option b)** — smoke spec updated in `feat/gemma-adapter-impl` to assert only the persisted types (`command`, `result`); register/heartbeat continue to update the `agents` table only. Reversing this is a future call (per-row cost vs full audit observability). |
| ~~**Phase 1 test stack inherited Phase 1 bugs**~~ | ~~Medium~~ | **Partially resolved** — `e2e/test-nats.conf` rewritten for token-only auth + MQTT off (matching dev contract; also bumped JetStream `max_file` to 2GB so the stream's 1GB `max_bytes` actually fits). `e2e/docker-compose.test.yml` build context fixed to repo root + Dockerfile path explicit + `NATS_TOKEN` propagated + `/api/system/status` healthcheck (replacing retired `/health`). `e2e/test-nginx.conf` `proxy_pass` slash bug fixed. `global-setup.js` API_URL pointed at `/api/system/status`. **Test stack now boots cleanly to the canonical v0.1 status.** Remaining: agent-level smokes (Phase 1 / Phase 2 round-trips) need host-side `shell` and `gemma` adapters pointed at the test broker (port 14222) — the test compose doesn't run them (Gemma can't be containerized cleanly because Ollama lives on the host). Until `global-setup.js` learns to spawn host adapters with `NATS_URL=nats://localhost:14222`, agent-round-trip tests should keep using the `playwright.smoke.config.js` bypass against the dev stack. |
| **`adapters/_common/template.py` builds the registration card from YAML and publishes a `register` envelope, but never mirrors register/heartbeat to outbox** | Low — observability gap | Either persist via `on_register`/`on_heartbeat` (above) or have adapters mirror to outbox |

### Test-data separation convention (introduced after Phase 2 walkthrough)

The `messages` and `agents` tables both have a `deployment` column. As of the
post-Phase-2 walkthrough fixes, the aggregator's `MessageRouter` now resolves
each message's deployment by looking up the sender's (or recipient's) cached
A2A card and reading `metadata.runtime.deployment` (default: `"default"`).
Frontend `AgentSidebar` and `ChatHistory` filter out `deployment === "test"`
unless the user toggles "Test data" on in the header.

**Convention for test runners:**
- Tests that need their own visible-but-hideable agent should register with
  `runtime.deployment: test` in their `config.yaml` (or programmatically).
  All messages they send AND results targeted at them get tagged `test`.
- Tests that drive production agents (the current Playwright smoke does this
  for `gemma-1` / `shell-1`) cannot be tagged this way today — they need
  EITHER (a) a registered test runner agent that publishes the commands,
  OR (b) an envelope-level `deployment` field (schema change). Tracked as a
  follow-up below.

| Item | Severity | Notes |
|---|---|---|
| ~~**Phase 2 Playwright smoke pollutes prod data**~~ | ~~Medium~~ | **Resolved** — `POST /api/command/{agent_id}` accepts `?sender_id=<name>` query param. When non-default, the aggregator auto-registers a synthetic A2A card with `runtime.deployment: test` for that sender (and the outbox mirror moves to `agents.{sender}.outbox`). All envelopes in the resulting task — command, outbox mirror, gemma's result — flow through `_deployment_for()` and tag `deployment=test`. The Phase 2 Playwright smoke now passes `?sender_id=test-runner` and asserts `result.deployment === 'test'`. Smoke turns invisible to the dashboard when the user's `showTestAgents` toggle is off. |
| ~~**`/api/messages` endpoint doesn't accept a `deployment` filter param**~~ | ~~Low~~ | **Resolved** — `db.query_messages()` now accepts `deployment` (allowlist) and `exclude_deployment` (denylist) kwargs; both surfaced as query params on `/api/messages`. Frontend `ChatHistory` now passes `exclude_deployment=test` when `showTestAgents=false` so the server-side LIMIT only sees production rows (instead of post-fetch filter that could lose old prod rows behind a wall of test data). New test at `aggregator/tests/test_database.py::test_query_messages_deployment_filters`. |
| **Logs tab content is sparse** — only lifecycle (register/shutdown) and handler errors publish log envelopes today. Per-command success info isn't logged | Low — by design (verbose) but operators may want it | Optional toggle for verbose mode that publishes a `log` envelope per completed command |

---

## Phase handover — delayed-to-later-phases

Each phase has its own spec/plan cycle when activated. The Phase 1 plan handoff section (`docs/superpowers/plans/2026-04-23-agent-messaging-v0.1-phase-1.md`) is the original source for these.

### Phase 2.5 — Gemma adapter enhancements

When? After Phase 2 ships and we have a baseline of "what users actually ask the LLM agent for". Possibly bundled with Phase 3.

Items:
- Multi-skill dispatch (see deferred table above)
- Conversational memory
- Token streaming via `task.progress`
- WebSocket bridge for live UI updates

Spec file: `docs/superpowers/specs/<date>-gemma-enhancements-design.md` (TBD).

### Phase 3 — Operational hardening

Two sessions, one plan. Builds on Phase 1's heartbeat + advisory infrastructure.

#### Phase 3.1 — Watchdog adapter

Subscribes `agents.*.heartbeat` and `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>`. Tracks per-agent heartbeat freshness; when an agent goes silent past 2× its declared `runtime.heartbeat_interval_sec`, publishes synthesized `result` envelopes for any in-flight commands targeted at it (`task_state: failed`, `payload.error: "recipient_offline"`).

Has its own durable inbox (`agents.watchdog-1.inbox`) with `max_ack_pending: 1`. `runtime.kind: native`, `runtime.roles: [watchdog]`.

Spec file: `docs/superpowers/specs/<date>-watchdog-adapter-design.md` (TBD).

#### Phase 3.2 — Dashboard agent-registry panel

Per-agent UI panel showing card metadata, heartbeat freshness, queue depth, poison count, and online/offline badge. Uses the endpoints already shipped in Phase 1 Task 6 + Task 8 (`/api/agents`, `/api/agents/{id}/queue`, `/api/poison`).

Pairs well with WebSocket bridge (deferred above) so the panel updates live instead of via polling.

Spec file: `docs/superpowers/specs/<date>-agent-registry-panel-design.md` (TBD).

### Phase 4 — AG2 + A2A wrapper

Four sessions, one plan. The first phase that exercises the **bridge** pattern (`runtime.kind: bridge`).

#### Phase 4.1 — AG2 adapter L1 scaffold

Pin `ag2>=0.12,<0.13`. Spend 15 minutes verifying imports (`A2aRemoteAgent`, `A2aAgentServer`, `autogen.agentchat.group.AutoPattern`) against the pinned wheel before writing code. Use `a_run` async-only.

#### Phase 4.2 — AG2 L2 delegation + hop_count loop protection

Refuse delegations at `hop_count >= 8`. Cancel returns `task_state: rejected, payload.reason: "ag2_cancel_not_supported"` (v0.2 limitation; revisit if/when AG2 ships cancel).

#### Phase 4.3 — Dashboard delegation-chain view

`GET /api/chains/{context_id}` endpoint + chain timeline UI. Visualizes a delegation cascade across multiple agents.

#### Phase 4.4 — A2A HTTP wrapper

`A2aAgentServer(agent, agent_card=card)` serving `/.well-known/agent-card.json`. NATS bridge translates SSE → `task.progress` envelopes. Decide `.build()` vs `.serve()` vs `.run()` at pin time.

Spec file: `docs/superpowers/specs/<date>-ag2-a2a-wrapper-design.md` (TBD).

### Phase 5 — Mac Mini deploy

One session, one plan. Production-shaped deployment to the dedicated host.

Items:
- `deploy-mac-mini.sh` script (preflight: `.env` over tailnet, `BROKER_HOST` set, operator verifies `NATS_TOKEN` has JetStream perms via `nats consumer add` smoke).
- launchd / systemd-style services for Ollama + Gemma adapter + watchdog adapter (Phase 3) + openclaw browser launcher.
- Backup strategy for `data/openclaw.db` and `nats/data/jetstream/`.
- Tailnet ACLs for inter-host access.

Spec file: `docs/superpowers/specs/<date>-mac-mini-deploy-design.md` (TBD).

### Optional / parking lot

- **Bridge adapter for Hermes / ACP.** Spec rev 7 §"Bridge pattern" already covers the design. Plan when Nous Research's Hermes Agent is first onboarded.
- **Per-agent JWT auth** (replaces shared `NATS_TOKEN`). v0.2+. Necessary for multi-tenant deployments.
- **JetStream clustering.** Single-node broker is fine until a second persistent host joins (likely v0.3+).
- **P2P transport (Zenoh).** v0.3+ exploration; only relevant if we want agent-to-agent communication that doesn't traverse the central broker.
- **Token streaming over NATS for browser clients.** Currently planned via A2A SSE wrapper at Phase 4. Revisit if we want native NATS streaming for non-browser clients.
