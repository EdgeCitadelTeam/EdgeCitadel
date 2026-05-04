# edge-research — Claude Code Instructions

## How this file is maintained
- **Treat as code.** PRs that change repo workflow (commands, dirs, gates) MUST update this file in the same PR.
- **Grow the Do Not section from real Claude mistakes.** Bad output Claude should have prevented → add a one-line rule with the date.
- **Apply the deletion test.** Each line must answer "would removing this cause a mistake?" Quarterly skim; cut what doesn't.
- **Hard ceiling: 200 lines.** If exceeded, graduate workflow content to `.claude/skills/<name>/SKILL.md` (see Where to look).
- **Single source of truth.** A fact lives in exactly one of: this file, `~/.claude/CLAUDE.md`, `CLAUDE.local.md` (gitignored), `.claude/skills/`, `.claude/settings.json`. No duplicates.

## Repo map
- `aggregator/` — Python FastAPI backend, NATS subscriptions, SQLite persistence
- `frontend/` — React/Vite dashboard (the only UI source root)
- `openclaw-client/` — Node MQTT listener for agents
- `e2e/` — Playwright end-to-end tests
- `docs/` — architecture and operations
- Service `dashboard` is current; the old `dashboard/` directory is retired.

## Commands
- Full stack: `docker compose up --build -d`
- Restart: `docker compose down && docker compose up --build -d`
- Backend dev: `cd aggregator && uvicorn main:app --host 0.0.0.0 --port 8000 --reload`
- Frontend dev: `cd frontend && npm run dev`
- Frontend build: `cd frontend && npm run build`
- Client listener: `cd openclaw-client && npm start`
- E2E tests: `cd e2e && npm test`

## Working rules
- Inspect any nested `CLAUDE.md` before editing in a subdirectory.
- Prefer targeted changes over broad rewrites; preserve existing patterns in the touched area.
- Keep changes narrow on `main`; prefer feature branches and PRs.
- Conventional Commits: `feat|fix|docs|refactor|perf|test|chore|ci|build(<scope>): <desc>`. Scopes: `aggregator`, `frontend`, `nats`, `mqtt`, `dashboard`, `e2e`, `client`, `infra`.
- Cross-subsystem changes: leave all touched areas consistent in one pass; document verification per touched subsystem.
- Use `--force-with-lease`, never plain `--force`.
- **IMPORTANT:** Update this file in the same PR if you change commands, dirs, or quality gates.

## Quality gates
- No secrets, tokens, or local config in committed files.
- Schema/messaging changes update `docs/05-messaging.md` in the same PR.
- Config changes update `.env.example` and relevant setup docs.
- Verification: invoke the relevant `verify-*` skill (`verify-frontend`, `verify-backend`, `verify-infra`). Default smoke: `curl http://localhost:8222/healthz` and `curl http://localhost/api/system/status`.
- Curl-only checks are NOT sufficient for UI or workflow changes — Playwright via `cd e2e && npm test` is the gate.

## Do Not
- Don't edit the retired `dashboard/` directory — UI lives in `frontend/`.
- Don't add new files at repo root — top-level config only; new docs go in `docs/`.
- Don't commit `.claude/settings.local.json`, `.env`, or anything in `data/`.
- Don't treat curl checks as sufficient for UI/workflow changes — Playwright is the gate.
- New entries: see "How this file is maintained" above. Format: `- YYYY-MM-DD Don't <thing>. (incident: <one-line context>)`

## Where to look
- Architecture: `docs/01-architecture.md`
- Messaging contracts: `docs/05-messaging.md`
- Setup: `docs/agent-setup.md`
- Operational workflows: `.claude/skills/` (verify-*, release, smoke-check, etc.)
- Hook/permission config: `.claude/settings.json`
