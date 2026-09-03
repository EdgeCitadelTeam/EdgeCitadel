# edge-research Codex Instructions

## How this file is maintained
- Treat this file as code. PRs that change repo workflow, commands, directories, or quality gates must update it.
- Keep it under 200 lines. Move long workflow detail into `.agents/skills/<name>/SKILL.md`.
- Keep one source of truth. Do not duplicate facts across this file, tool-specific instruction files, or `.agents/skills/`.
- Add "Do Not" entries only for real mistakes that should be prevented next time.

## Engineering behavior
- Think before coding. State assumptions, surface ambiguity, and ask when the request has materially different interpretations.
- Prefer the simplest working design. Do not add features, abstractions, configurability, or speculative error handling that the task does not need.
- Make surgical changes. Touch only what the request requires, match local style, and leave unrelated cleanup for a separate task.
- Clean up only your own mess. Remove imports, variables, functions, or files made obsolete by your change; do not delete pre-existing dead code unless asked.
- Define success criteria for non-trivial work. For fixes, reproduce the failure first when practical; for refactors, verify behavior before and after.
- Every changed line should trace back to the user's request.

## Repo map
- `aggregator/` - Python FastAPI backend, NATS subscriptions, SQLite persistence
- `frontend/` - React/Vite dashboard; the only UI source root
- `e2e/` - Playwright end-to-end tests
- `agent-platform/` - agentd, Agent Package runtime, schemas, SDK protocols, validation, and tests
- `agent-packages/` - Installable Agent Packages and developer examples
- `plugins/` - Native Plugins for Pi, Claude Code, and Codex
- `edgecitadel/` - Python distribution entrypoint; packaged runtime assets are assembled at build time

## Commands
- Newcomer setup: `./scripts/edgecitadel install`
- Enroll a host: `./scripts/edgecitadel invite --node-id <node-id> --host <reachable-host>` then `./scripts/edgecitadel join '<invitation>'`
- Install a Managed Agent: `./scripts/edgecitadel agent install <package-path>`
- Inspect the local service: `./scripts/edgecitadel service status`
- Homebrew formula style: `brew style deploy/homebrew/Formula/edgecitadel.rb`
- Python package: `python -m build` then install the wheel in a clean virtual environment
- Root Python setup: `python3.12 -m venv .venv && .venv/bin/pip install -r scripts/requirements-test.txt`
- Root Python tests: `.venv/bin/python -m pytest -q tests scripts/tests deploy/tests schemas/tests`
- Full stack: `docker compose up --build -d`
- Restart: `docker compose down && docker compose up --build -d`
- Backend setup: `cd aggregator && python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt`
- Backend dev: `aggregator/.venv/bin/uvicorn aggregator.main:app --host 0.0.0.0 --port 8000 --reload`
- Frontend dev: `cd frontend && npm run dev`
- Frontend build: `cd frontend && npm run build`
- Frontend tests: `cd frontend && npm test`
- Deterministic E2E tests: `cd e2e && npm test`; external Managed Agent suites require a prepared stack and run with `APP_URL=... AGG_URL=... npm run test:external-plugins`
- Agent Package checks (smoke): `cd agent-platform && python -m pytest -q && python -m edgecitadel_supervisor validate ../agent-packages/examples/placeholder`; see `agent-platform/README.md` for the full contributor gate.

## Working rules
- Inspect any nested `AGENTS.md` before editing in a subdirectory.
- Keep changes narrow on `main`; prefer feature branches and PRs.
- Conventional Commits: `feat|fix|docs|refactor|perf|test|chore|ci|build(<scope>): <desc>`. Scopes: `aggregator`, `frontend`, `nats`, `mqtt`, `dashboard`, `e2e`, `client`, `infra`.
- Cross-subsystem changes: leave all touched areas consistent in one pass; document verification per touched subsystem.
- Use `--force-with-lease`, never plain `--force`.

## Quality gates
- No secrets, tokens, or local config in committed files.
- Config changes update `.env.example`.
- New host-level dependency (Phase 5+): edit `deploy/manifest.toml` only; deployment automation consumes it.
- Verification: invoke the relevant `verify-*` skill (`verify-frontend`, `verify-backend`, `verify-infra`). Default smoke: `curl http://localhost:8222/healthz` and `curl http://localhost/api/system/status`.
- Curl-only checks are not sufficient for UI or workflow changes. Playwright via `cd e2e && npm test` is the gate.

## Do Not
- Don't add new files at repo root; top-level config only.
- Don't commit `.Codex/settings.local.json`, `.env`, or anything in `data/`.
- Don't treat curl checks as sufficient for UI/workflow changes; Playwright is the gate.
- New entries: see "How this file is maintained" above. Format: `- YYYY-MM-DD Don't <thing>. (incident: <one-line context>)`

## Where to look
- Operational workflows: `.agents/skills/` (verify-*, release, smoke-check, etc.)
- Hook/permission config: `.claude/settings.json`
