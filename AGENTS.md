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
- `openclaw-client/` - Node NATS client for agents
- `e2e/` - Playwright end-to-end tests
- `docs/` - architecture and operations
- `plugin-system/` - Plugin schemas, SDK protocols, no-op supervisor, and hermetic tests
- `plugins/` - Installable EdgeCitadel plugin packages and examples
- Service `dashboard` is current; the old `dashboard/` directory is retired.

## Commands
- Full stack: `docker compose up --build -d`
- Restart: `docker compose down && docker compose up --build -d`
- Backend dev: `cd aggregator && uvicorn main:app --host 0.0.0.0 --port 8000 --reload`
- Frontend dev: `cd frontend && npm run dev`
- Frontend build: `cd frontend && npm run build`
- Client listener: `cd openclaw-client && npm start`
- E2E tests: `cd e2e && npm test`
- Plugin checks: `cd plugin-system && python -m pytest -q && python -m edgecitadel_supervisor validate ../plugins/examples/placeholder`

## Working rules
- Inspect any nested `AGENTS.md` before editing in a subdirectory.
- Keep changes narrow on `main`; prefer feature branches and PRs.
- Conventional Commits: `feat|fix|docs|refactor|perf|test|chore|ci|build(<scope>): <desc>`. Scopes: `aggregator`, `frontend`, `nats`, `mqtt`, `dashboard`, `e2e`, `client`, `infra`.
- Cross-subsystem changes: leave all touched areas consistent in one pass; document verification per touched subsystem.
- Use `--force-with-lease`, never plain `--force`.

## Quality gates
- No secrets, tokens, or local config in committed files.
- Schema/messaging changes update `docs/05-messaging.md` in the same PR.
- Config changes update `.env.example` and relevant setup docs.
- New host-level dependency (Phase 5+): edit `deploy/manifest.toml` only; deploy script and platform setup guides consume it. Do not embed dependency lists in prose.
- Verification: invoke the relevant `verify-*` skill (`verify-frontend`, `verify-backend`, `verify-infra`). Default smoke: `curl http://localhost:8222/healthz` and `curl http://localhost/api/system/status`.
- Curl-only checks are not sufficient for UI or workflow changes. Playwright via `cd e2e && npm test` is the gate.

## Do Not
- Don't edit the retired `dashboard/` directory; UI lives in `frontend/`.
- Don't add new files at repo root; top-level config only, new docs go in `docs/`.
- Don't commit `.Codex/settings.local.json`, `.env`, or anything in `data/`.
- Don't treat curl checks as sufficient for UI/workflow changes; Playwright is the gate.
- New entries: see "How this file is maintained" above. Format: `- YYYY-MM-DD Don't <thing>. (incident: <one-line context>)`

## Where to look
- Architecture: `docs/01-architecture.md`
- Messaging contracts: `docs/05-messaging.md`
- Setup: `docs/agent-setup.md`
- Operational workflows: `.agents/skills/` (verify-*, release, smoke-check, etc.)
- Hook/permission config: `.Codex/settings.json`
