# EdgeCitadel

Hybrid NATS+MQTT agent communication platform. NATS 2.10+ with JetStream for persistence, built-in MQTT adapter for IoT devices. Aggregator (Python/FastAPI) connects via native NATS. IoT agents connect via MQTT on port 1883. React dashboard via WebSocket.

## Key Commands

```bash
# Full stack
docker compose up --build
docker compose down -v          # teardown (destroys data)

# Aggregator (Python 3.12)
cd aggregator && pip install -r requirements.txt
ruff check aggregator/ --fix    # lint
ruff format aggregator/         # format
mypy aggregator/ --strict       # type check
pytest tests/ -x --tb=short     # run tests (prefer single file)

# Frontend (React 18 + Vite 5)
cd frontend && npm ci
npm run lint                    # eslint
npm run build                   # production build
npm run dev                     # dev server :3000

# E2E tests
cd e2e && npm ci && npx playwright test
```

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):

```
<type>(<scope>): <description>

Types: feat|fix|docs|style|refactor|perf|test|chore|ci|build
Scopes: aggregator|frontend|nats|mqtt|dashboard|e2e|client|infra
```

Branch naming: `<type>/<short-description>` (e.g., `feat/jetstream-consumers`)

## Architecture Rules

- **NATS subjects**: `agents.{name}.{action}`, `tasks.{id}.{action}`, `system.broadcast`
- **MQTT topics**: `agents/{name}/{action}` (auto-translated by NATS, slashes become dots)
- **Auth**: Single `NATS_TOKEN` — NATS clients use as token, MQTT clients use as password
- **Ports**: 4222 (NATS native), 1883 (MQTT adapter), 8222 (NATS monitoring HTTP)
- **Database**: SQLite at `/data/openclaw.db`, sync with module-level connection
- **Aggregator**: nats-py async subscriptions run natively in FastAPI event loop — no thread bridging
- **Nginx**: Strips `/api/` prefix before forwarding to aggregator
- **Frontend**: Tailwind CSS (build-time PostCSS), Zustand state, dark theme default

## Quality Gates (Enforced Before Every Commit)

1. **Lint passes** — `ruff check` (Python), `npm run lint` (JS)
2. **Types check** — `mypy --strict` for any modified Python files
3. **Tests pass** — run relevant test file, not full suite
4. **No secrets** — no tokens, passwords, or API keys in committed code
5. **No direct commits to main** — use feature branches + PR
6. **Commit message format** — must follow Conventional Commits

## Code Standards

### Python (aggregator/)
- Pydantic models for all request/response schemas
- Consistent error format: `{"error": str, "detail": str, "status": int}`
- OpenAPI docstrings on every endpoint
- No blocking calls in async handlers
- Type annotations on all function signatures

### JavaScript (frontend/)
- ES modules only (import/export, not require)
- Functional components with hooks (no class components)
- Zustand for state (not Redux, not Context)
- lucide-react for icons
- Tailwind utility classes only (no custom CSS files)
- `axios` for HTTP, native WebSocket for real-time

### NATS/MQTT (messaging)
- All new subjects must be documented in `docs/05-messaging.md`
- JetStream streams must have explicit retention and limits
- MQTT QoS 1 for agent communication (at-least-once)
- Include `correlation_id` for request-reply patterns

## Documentation Requirements

Every PR must include:
- **Feature**: Update relevant `docs/` file, CHANGELOG entry, test coverage
- **Bug fix**: CHANGELOG entry, regression test
- **Architecture change**: ADR in `docs/adr/`, update this file if conventions change
- **API change**: Update `docs/08-api-reference.md`, update Pydantic models
- **Config change**: Update `.env.example`, update `docs/02-server-setup.md`

## Directory Map

```
aggregator/     Python FastAPI aggregator (NATS subscriber, REST API, WebSocket)
frontend/       React 18 dashboard (Vite, Tailwind, Zustand)
openclaw-client/ Node.js MQTT agent listener
nats/           NATS server config (JetStream + MQTT adapter)
nginx/          Reverse proxy config
e2e/            Playwright end-to-end tests
scripts/        Utility scripts (simulation, demo recording)
docs/           Architecture docs, API reference, ADRs
.claude/        Agents, rules, skills, hooks for Claude Code
```

## Critical Gotchas

- SQLite is sync with module-level connection — never use from multiple threads
- nginx strips `/api/` prefix — aggregator routes don't include it
- NATS MQTT adapter translates `/` to `.` — subject and topic must match
- WebSocket timeout is 86400s (24h) — long-lived connections are expected
- Frontend deduplicates messages by key with 10s TTL, max 500 cached
- Test data uses `test-` prefix — filtered in UI via `showTestAgents` toggle
