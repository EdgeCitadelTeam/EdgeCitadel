# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
