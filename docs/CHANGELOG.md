# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added (Phase 6)
- New `adapters/hermes/` — bridge adapter for Nous Research's Hermes Agent (`us-mac-hermes`).
- ADR-0009: bridge adapters retain upstream memory ownership.
- `scripts/launchd/com.edgecitadel.hermes-{bridge,server}.plist` for Mac auto-start.
- E2E spec `e2e/tests/phase6-hermes-bridge.spec.js`.
- Docs: `agent-contract.md` Bridge subsection, `03-agent-registration.md` Local adapter onboarding, `agent-setup.md` Hermes quickstart.

### Changed
- `task.progress.payload.extra.upstream` — bridge adapters SHOULD set this; native adapters omit it. (`docs/05-messaging.md`).
- `docs/roadmap.md`: Hermes promoted from parking lot to Phase 6; "MCP server exposing edge-research tools to Hermes" logged as v0.3.

### Added (Phase 2.5)

- **Multi-skill dispatch** in Gemma adapter. Four skills routed by
  `payload.skill_id`: `reasoning.chat`, `text.summarize`, `text.classify`
  (JSON output via Ollama `format: "json"` + JSON-schema validation),
  `code.explain`. Skills are data in `adapters/gemma/config.yaml` —
  adding a fifth is a YAML edit, no code change.
- **Conversational memory service** in the aggregator. New
  `conversation_turns` table; three NATS request-reply subjects
  (`memory.turns.get`, `memory.turns.put`, `memory.turns.delete`).
  Token-budget sliding-window eviction at `get` time; 30-day hard-delete
  idle cleanup runs every 5 min. ADR-0008 records the architecture.
- **Token streaming** in Gemma. Ollama `stream: true` → `task.progress`
  envelopes with hybrid 8-tokens-or-100ms flush. Canonical `result`
  envelope still emits with full text.
- **Live UI rendering** in dashboard. Synthetic streaming bubble in
  `ChatHistory.jsx` keyed by `task_id`. Cursor glyph, skill badge with
  per-skill colors, 60s stall detection. Replaced by canonical bubble
  when result lands.
- **`GET /api/conversations`** aggregator endpoint. Group
  `conversation_turns` by `(agent_id, context_id)`; returns turns,
  tokens, first/last seen, skills used.
- **`sqlite-vec` extension** loaded at aggregator startup (best-effort).
  Forward hook for v0.3 semantic memory.
- **ADR-0008** — centralized memory service hosted by aggregator over
  NATS.

### Added (Phase 3)

- **Watchdog adapter** (`adapters/watchdog/`). Native nats-py adapter,
  `runtime.kind: native`, `runtime.roles: [watchdog]`. Synthesises
  `task_state: failed, payload.error: "recipient_offline"` envelopes
  when an agent goes silent past `2 × declared_interval`, when a new
  command targets an already-offline agent, or when JetStream's
  MAX_DELIVERIES advisory fires. ~30–65 s detection latency for
  default-interval agents (down from 1.5–15 min).
- **`GET /api/registry`** aggregator endpoint. Returns one row per
  registered agent with card metadata, JetStream queue depth, and
  poison-event count.
- **`agent_deleted`** WebSocket event broadcast on `DELETE /api/agents/{id}`.
- **Dashboard "Registry" top-level tab** (5th tab, keyboard `5`).
  Sortable fleet table with state, heartbeat freshness, queue depth,
  and poison count.
- **Sidebar roles-based filter.** `AgentSidebar` now hides agents whose
  `runtime.roles` includes `watchdog` or `aggregator`; those appear only
  in the Registry tab.
- **ADR-0007:** records the heartbeat-staleness + advisory-backstop
  trigger model, divergent from the v0.1 messaging spec rev 6's
  advisory-only pin.

### Added — v0.2 Gemma reasoner adapter (Phase 2)
- `adapters/gemma/` — single-shot Ollama-backed reasoner agent
  (`agent_id: gemma-1`, `runtime.kind: native`, `runtime.roles:
  [reasoner]`, skill `reasoning.chat`).
- Wraps `POST /api/generate` (no streaming, no memory in v0.2);
  configurable model/temperature/max_tokens/timeout via per-command
  `payload.args` or env vars.
- Seven typed adapter-level error codes (`unsupported_type`,
  `empty_prompt`, `ollama_unreachable`, `ollama_timeout`,
  `model_not_loaded`, `ollama_inference_error`, `ollama_bad_response`)
  give the dashboard a stable failure vocabulary.
- Fail-fast preflight (`/api/tags` health + model presence check)
  blocks startup with exit code 1 (unreachable) or 2 (model-not-loaded);
  no auto-pull on missing model.
- 11 unit tests + gated live-Ollama integration test +
  `e2e/tests/phase2-gemma-smoke.spec.js`.
- `.env.example` documents `OLLAMA_HOST`, `OLLAMA_PORT`,
  `OLLAMA_MODEL`, `OLLAMA_TIMEOUT_SEC`.

Out of scope (deferred — see `docs/roadmap.md`): multi-skill dispatch,
conversational memory keyed by `context_id`, token streaming via
`task.progress`, WebSocket bridge for live UI updates, non-Ollama
backends, auto-pull, container packaging.

### Added — v0.1 messaging clean rebuild (Phase 1)
- NATS JetStream `AGENT_INBOX` WorkQueue stream with per-agent durable pull
  consumers (`max_ack_pending=1`). Envelope dedup via `Nats-Msg-Id` and
  `duplicate_window: 5m` (ADR-0002).
- A2A v1.0 task lifecycle vocabulary on every envelope (`task_id`, `context_id`,
  `task_state`, `hop_count`). Agent Card shape replaces legacy EdgeCitadel card
  (ADR-0003).
- Outbox mirror (`agents.{id}.outbox`) as authoritative audit path for inbox
  traffic (ADR-0006).
- Aggregator endpoints: `GET /api/agents`, `/agents/{id}/card`,
  `/agents/{id}/queue`, `DELETE /api/agents/{id}`; `POST /api/command/{id}`
  returns `task_id`; `GET /api/messages` / `/api/poison`;
  `POST /api/openclaw/login` for browser session tokens.
- Shared adapter skeleton under `adapters/_common/` (pull consumer, Agent Card
  factory, conformance suite, template).
- Shell adapter rewritten on nats-py async (replaces paho legacy).
- openclaw-client rewritten on `@nats-io/nats` with account-scoped
  `OPENCLAW_TOKEN` and aggregator-mediated publishes (ADR-0005).
- MQTT ingress moved behind deploy-time toggle (`EC_ENABLE_MQTT=1`); off by
  default (ADR-0004). Default `docker compose up -d` does NOT expose port 1883.
- Frontend reads canonical fields; new queue-depth and poison-event surfaces
  in the agent detail panel.
- E2E `phase1-smoke.spec.js` covers the canonical envelope round trip.

### Removed
- `receiver_id`, `message_type`, `correlation_id`, `chain_id`, `content`,
  `from`, `to`, `assigned_agent` aliases and alias-fallback readers.
- `/data/openclaw.db` wiped on first boot; no migration from pre-v0.1 shape.
- paho-mqtt client code (`openclaw-client/mqtt-listener.js`,
  `adapters/shell/shell_adapter.py`).
- MQTT 1883 port exposed by default in `docker-compose.yml`.
- `mqtt_connected` field in `/api/system/status`.
- Legacy E2E specs that asserted on MQTT topics or removed DB columns
  (replaced by `phase1-smoke.spec.js`).

### Phase 1 verification (operator-run)

The following checks should be run against a fresh `docker compose down -v && docker compose up --build -d` stack with the shell adapter started on the host. Each row should pass before tagging v0.1.

| Check | Command | Expected |
|---|---|---|
| Stream live | `docker compose exec nats nats stream info AGENT_INBOX` | shows stream |
| Consumer live | `docker compose exec nats nats consumer info AGENT_INBOX shell-1_inbox` | `num_pending=0` idle |
| Sequential FIFO | send 2 commands back-to-back, inspect mid-flight | `num_ack_pending <= 1` |
| Crash recovery | kill shell adapter mid-task, restart | unacked redelivers on restart |
| Dedup | publish same `Nats-Msg-Id` 3x | handler fires once |
| Queue endpoint | `curl /api/agents/shell-1/queue` | `{pending, ack_pending}` |
| Strict validation | publish envelope with `receiver_id: x` | dropped; logged reason |
| openclaw round-trip | browser → aggregator HTTP → JetStream → shell-1 → result | result visible |
| Fresh DB schema | `sqlite3 data/openclaw.db '.schema messages' \| grep recipient_id` | match; `receiver_id` absent |
| MQTT off by default | `docker compose ps \| grep 1883` | empty |
| No `mqtt_connected` | `curl /api/system/status` | field absent |
| Aggregator restart discovery | restart aggregator, check `/api/agents` | online agents within 10s |
| E2E smoke | `cd e2e && npm test -- phase1-smoke.spec.js` | PASS |

### Added
- P2P agent-to-agent delegation via LLM tool pattern
- Comprehensive documentation for all EdgeCitadel features (docs/ directory)
- Future potential documentation with concrete examples and references
- Architecture Decision Records (ADR) framework
- Claude Code agents for code review, security review, test review, and document standards
- Path-specific rules for Python, React, NATS, E2E, and Docker
- Contributing guide with quality gates and conventions

### Changed
- Migrated from standalone Mosquitto to hybrid NATS+MQTT architecture
- Aggregator now uses native nats-py instead of MQTT client
- CLAUDE.md restructured for conciseness (<120 lines) with progressive disclosure via rules/

### Fixed
- join.sh MQTT connection test: force exit on connect, use grep for robust OK check
- Nested payload parsing in nats-listener inbox handler
- Reply routing: deliver responses to sender's inbox
