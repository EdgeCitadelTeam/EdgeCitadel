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
# Python gates are defined in .agents/skills/commit-check/SKILL.md.
uv run --isolated --with-requirements scripts/requirements-test.txt ruff check --target-version py312 aggregator/ scripts/ plugin-toolkit/ plugins/ tests/ deploy/tests/
uv run --isolated --with-requirements scripts/requirements-test.txt ruff format --target-version py312 aggregator/ scripts/ plugin-toolkit/ plugins/ tests/ deploy/tests/ --check
cd aggregator && uv run --isolated --with-requirements requirements-dev.txt python -m pytest -q
cd .. && uv run --isolated --with-requirements scripts/requirements-test.txt python -m pytest -q scripts/tests
./scripts/research/run-python -m pytest tests/ -x --tb=short

# Frontend
cd frontend && npm run lint && npm run build

# Deterministic E2E owns and cleans up a disposable stack.
cd e2e && npm test

# Optional upstream/model-dependent Plugin suites use a prepared external stack.
APP_URL=http://localhost AGG_URL=http://localhost:8000 npm run test:external-plugins
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
plugin-toolkit/  Shared Plugin runtime, SDK, schemas, validation, and tests
plugins/         Installable Agent Plugin packages and implementations
.agents/         Canonical shared verification skills
.claude/         Claude-specific compatibility and area guides
```
