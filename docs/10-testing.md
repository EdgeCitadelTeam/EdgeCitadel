# Testing

End-to-end coverage runs through Playwright against either the dedicated test
stack (full isolation) or the live dev stack (smoke). The legacy
`full-e2e.js` / `record-demo.js` scripts and the MQTT-based helpers were
retired in the v0.1 messaging rebuild — see `docs/roadmap.md` and
`docs/CHANGELOG.md`.

## Suites

| Spec | Tests | Scope |
|---|---|---|
| `phase1-smoke.spec.js` | 5 | v0.1 canonical-envelope round trip (register → command → result → outbox mirror). |
| `phase2-gemma-smoke.spec.js` | 4 | Gemma adapter wire contract (single-skill `reasoning.chat`). |
| `phase2.5-streaming-and-memory.spec.js` | 5 | Multi-skill dispatch, `task.progress` streaming, conversational memory, unknown-skill rejection. |
| `phase3-watchdog-fast-path.spec.js` | 2 | Watchdog SYN dedup + heartbeat-staleness fast path. |
| `phase3-registry-tab.spec.js` | 3 | Dashboard Registry tab, `agent_deleted` WS event, role-based sidebar filter. |
| `dark-mode.spec.js` | 2 | Theme toggle. |
| `keyboard-shortcuts.spec.js` | 3 | Tab-switch shortcuts. |

Total: 7 spec files, 24 tests.

## Run modes

### Full isolated stack

```bash
cd e2e
npm test
```

`global-setup.js` builds and starts `docker-compose.test.yml`, waits for
healthchecks, runs all specs in parallel (3 workers), then `global-teardown.js`
stops the stack. Test ports are remapped so the dev stack on :80 / :4222 stays
untouched.

| Port | Service |
|---|---|
| 13000 | Frontend (nginx) |
| 18000 | Aggregator API |
| 14222 | NATS client |
| 18222 | NATS monitoring |

Phase 2/2.5 round-trip specs require a host-side Gemma adapter pointed at
`NATS_URL=nats://localhost:14222`; the test compose does not run Ollama-backed
adapters (Ollama is host-only). Until `global-setup.js` learns to spawn host
adapters automatically, those specs should be run via the smoke config below.

### Smoke against the dev stack

```bash
cd e2e
npx playwright test --config=playwright.smoke.config.js tests/phase2.5-streaming-and-memory.spec.js
```

`playwright.smoke.config.js` skips global setup/teardown and points at
`http://localhost` (the dev stack). `testMatch` covers any
`phase[\d.]+-*.spec.js`. Used for ad-hoc walkthrough verification when host
adapters are already running against the dev broker on :4222.

## Configuration

| File | Purpose |
|---|---|
| `playwright.config.js` | Full-stack run; baseURL `http://localhost:13000`, 3 workers, 1 retry. |
| `playwright.smoke.config.js` | Dev-stack run; baseURL `http://localhost`, 1 worker, 0 retries. |
| `global-setup.js` / `global-teardown.js` | Test-stack lifecycle. |
| `test-storage-state.json` | Sets `localStorage.showTestAgents = "true"` so test-tagged data is visible. |
| `test-nats.conf` | Token-only auth, MQTT off, 2 GB JetStream `max_file`. |
| `test-nginx.conf` | Test-stack nginx (preserve-prefix proxy). |
| `docker-compose.test.yml` | Test stack: nats + aggregator + dashboard + nginx, isolated ports. |

## Helpers

- `helpers/api-client.js` — REST client (`/api/agents`, `/api/messages`,
  `/api/command/{id}`, `/api/registry`, `/api/conversations`).
- `helpers/fixtures.js` — `buildCanonicalEnvelope({ type, sender_id, ... })`
  emits the v0.1 envelope (UUID id, ISO-8601 timestamp). Same shape as the
  Python adapters and `openclaw-client/src/nats-session.js`.
- `helpers/ws-client.js` — `/ws/stream` and `/ws/agent/{id}` clients.
- `helpers/wait-utils.js` — `pollUntil`, `sleep`.

## Test-data isolation

Both `messages` and `agents` carry a `deployment` column. The aggregator
resolves it from the sender's (or recipient's) cached A2A card
(`metadata.runtime.deployment`, default `"default"`). The dashboard hides
`deployment === "test"` rows unless **Test data** is toggled on in the header.

For tests that need an isolatable identity:

- Static identity: register a card with `runtime.deployment: test`.
- Dashboard-driven commands: pass `?sender_id=<name>` to
  `POST /api/command/{agent_id}`. When the sender is unknown, the aggregator
  auto-registers a synthetic test-deployment card and tags every envelope in
  the resulting task `deployment=test`. The Phase 2 / 2.5 smokes use this.

Server-side filtering: `/api/messages` accepts `deployment` (allowlist) and
`exclude_deployment` (denylist). The dashboard passes
`exclude_deployment=test` when `showTestAgents` is off so the LIMIT only sees
production rows.
