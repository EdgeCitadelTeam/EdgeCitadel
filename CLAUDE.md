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

## Worktree Workflow

Every task uses a worktree for isolation. Multiple sessions (human or AI) work in parallel without conflicts.

### Starting a New Task

```bash
scripts/worktree-create.sh <type> <short-description>
# Example: scripts/worktree-create.sh feat jetstream-consumers
# Creates: .claude/worktrees/jetstream-consumers/ on branch feat/jetstream-consumers
```

The script auto-copies `.env` files and installs dependencies (`npm ci`, `pip install`).

### Submitting Work

```bash
cd .claude/worktrees/<name>
git push -u origin <type>/<short-description>
gh pr create --base main
```

### After PR Merge — Cleanup

```bash
scripts/worktree-cleanup.sh <name>        # single worktree
scripts/worktree-cleanup.sh --all         # all merged worktrees
scripts/worktree-cleanup.sh --list        # show status of all worktrees
```

The cleanup script removes the worktree, deletes the local branch, and deletes the remote branch. It warns if the branch is not yet merged.

### PR Feedback After Worktree Removal

When a PR receives review feedback after the worktree was already cleaned up, the branch still exists on the remote. Recreate the worktree from it:

```bash
scripts/worktree-resume.sh feat/jetstream-consumers   # by branch name
scripts/worktree-resume.sh 42                          # by PR number
```

This fetches the branch, recreates the worktree, copies env files, and installs dependencies. You pick up exactly where you left off — make changes, commit, push.

### Multi-Session Parallel Work

- Each session gets its own worktree and branch — git enforces one branch per worktree
- **Never use `git stash`** — stash is shared across all worktrees and causes cross-session contamination
- Always commit work-in-progress to the branch instead
- Each worktree has its own `node_modules`, `.env`, and build artifacts — fully isolated
- Use `--force-with-lease` (not `--force`) when pushing rebased branches

### Worktree Gotchas

- **Shared across worktrees**: object database, refs, git config, stash, hooks
- **Isolated per worktree**: HEAD, index (staging), working directory, bisect/rebase state
- **Branch lock**: a branch checked out in one worktree cannot be checked out in another — use `git worktree prune` if you get stale lock errors
- **Dependency install required**: each worktree is a fresh directory with no `node_modules` — the scripts handle this automatically
- **Port conflicts**: if running dev servers in multiple worktrees, assign different ports

## Critical Gotchas

- SQLite is sync with module-level connection — never use from multiple threads
- nginx strips `/api/` prefix — aggregator routes don't include it
- NATS MQTT adapter translates `/` to `.` — subject and topic must match
- WebSocket timeout is 86400s (24h) — long-lived connections are expected
- Frontend deduplicates messages by key with 10s TTL, max 500 cached
- Test data uses `test-` prefix — filtered in UI via `showTestAgents` toggle
