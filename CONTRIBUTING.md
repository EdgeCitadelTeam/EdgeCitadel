# Contributing to EdgeCitadel

## Quick Start

```bash
git clone <repo-url> && cd EdgeCitadel
cp .env.example .env          # configure NATS_TOKEN, OPENCLAW_TOKEN
docker compose up --build     # start full stack
```

Dashboard: http://localhost (via nginx)
API status: http://localhost/api/system/status
NATS monitoring: http://localhost:8222

## Development Workflow

### 1. Branch from main

```bash
git checkout -b <type>/<short-description>
# Examples:
#   feat/jetstream-consumers
#   fix/mqtt-reconnection
```

### 2. Make changes

Repository policy and quality gates are in `AGENTS.md`. Claude-specific area guides are in `.claude/rules/`:
- `python-backend.md` — aggregator Python code
- `react-frontend.md` — dashboard React components
- `nats-messaging.md` — NATS subjects and message schemas
- `e2e-testing.md` — Playwright end-to-end tests
- `docker-infra.md` — Docker, nginx, NATS config

### 3. Verify quality

```bash
# Python
ruff check aggregator/ --fix
ruff format aggregator/
mypy aggregator/ --strict
pytest tests/ -x --tb=short

# Frontend
cd frontend && npm run lint && npm run build

# E2E (requires running stack)
cd e2e && npx playwright test
```

### 4. Commit with Conventional Commits

```
<type>(<scope>): <description>

Types: feat | fix | docs | style | refactor | perf | test | chore | ci | build
Scopes: aggregator | frontend | nats | mqtt | dashboard | e2e | client | infra
```

Examples:
```
feat(aggregator): add JetStream consumer group support
fix(mqtt): resolve race condition in topic translation
docs(nats): add ADR for subject naming conventions
test(e2e): add agent offline detection tests
```

### 5. Open a PR

PRs must include:
- Clear description of what changed and why
- Test coverage for new behavior

## Code Review Standards

Reviewers check for:

1. **Correctness** — Does it work? Edge cases handled?
2. **NATS contract** — Are all publishers/subscribers consistent?
3. **Database** — Parameterized queries? No concurrent thread access?
4. **Security** — No secrets in code? Input validated?
5. **Tests** — New behavior covered? Existing tests pass?
6. **Simplicity** — Is there a simpler way?

Verdicts: **SHIP** / **FIX-THEN-SHIP** / **RETHINK**

## Project Structure

```
aggregator/      Python FastAPI aggregator
frontend/        React 18 dashboard
openclaw-client/ Node.js NATS agent client
nats/            NATS server config
nginx/           Reverse proxy config
e2e/             Playwright tests
scripts/         Utility scripts
plugin-toolkit/  Plugin authoring SDK, schemas, validation, and tests
plugins/         Independently distributable plugin packages
.agents/         Canonical shared verification skills
.claude/         Claude-specific compatibility and area guides
```
