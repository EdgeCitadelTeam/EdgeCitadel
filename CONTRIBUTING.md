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

Choose checks for the changed behavior using
[commit-check](.agents/skills/commit-check/SKILL.md). Start with affected tests and
callers; broaden for shared contracts or failures. Reuse passing results when
the tested code and dependencies are unchanged. Documentation-only edits need
content/link review, not application suites.

Examples below select one check, not a sequence to run for every change. Use the
Python environment configured for the relevant subsystem; see `agent-runtime/README.md`
for its source/editable setup.

| Area | Focused command | Working directory |
|---|---|---|
| Python | `python -m pytest -q tests/test_api.py` | `aggregator/` |
| Frontend unit test | `npm test -- src/components/StatusBadge.test.jsx` | `frontend/` |
| E2E helper | `node --test helpers/stack-config.spec.js` | `e2e/` |
| Browser workflow | `npm run test:playwright -- tests/operator-journey.spec.js` | `e2e/` |

`e2e`'s `npm test` runs both helper tests and the full browser suite; use it when
both are relevant. The focused browser command above owns a disposable stack;
there is no need to restart a shared stack first. External model-dependent
Managed Agent suites require a prepared stack and use `npm run test:external-plugins`.
CI and release workflows keep their broader checks.

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
- Relevant verification and any material limitations

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
