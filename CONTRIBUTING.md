# Contributing to EdgeCitadel

## Quick Start

```bash
git clone <repo-url> && cd EdgeCitadel
cp .env.example .env          # configure generated credentials before startup
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

Repository policy and quality gates are in `AGENTS.md`; repeatable verification
procedures live in `.agents/skills/`. Tool-specific configuration must defer to
those shared sources.

### 3. Verify quality

```bash
# Python gates are defined in .agents/skills/commit-check/SKILL.md.
uv run --isolated --with-requirements scripts/requirements-test.txt ruff check --target-version py312 aggregator/ scripts/ agent-runtime/ agent-packages/ tests/ deploy/tests/ e2e/fixture_agent/
uv run --isolated --with-requirements scripts/requirements-test.txt ruff format --target-version py312 aggregator/ scripts/ agent-runtime/ agent-packages/ tests/ deploy/tests/ e2e/fixture_agent/ --check
cd aggregator && uv run --isolated --with-requirements requirements-dev.txt python -m pytest -q
cd .. && uv run --isolated --with-requirements scripts/requirements-test.txt python -m pytest -q tests scripts/tests deploy/tests schemas/tests

# Frontend
cd frontend && npm run lint && npm test && npm run build

# Deterministic E2E owns and cleans up a disposable stack.
cd e2e && npm test

# Optional upstream/model-dependent Managed Agent suites use a prepared external stack.
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
nats/            NATS server config
nginx/           Reverse proxy config
e2e/             Playwright tests
scripts/         Utility scripts
agent-runtime/   agentd, Agent Package runtime, SDK, validation, and tests
agent-packages/  Installable Agent Packages and examples
plugins/         Native host Plugins for Codex, Claude Code, and Pi
.agents/         Canonical shared verification skills
.claude/         Claude-specific settings, commands, and shared-skill links
```
