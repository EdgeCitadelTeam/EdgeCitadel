# Agent Setup

## Canonical Layout

- `AGENTS.md` is the canonical, team-shared instruction file for coding agents.
- `CLAUDE.md` remains in the repo root only so Claude Code can load the shared guidance through an import.
- Nested `AGENTS.md` files in active subprojects add local rules without duplicating the root file.

## Claude Files

- Shared Claude project settings live in `.claude/settings.json`.
- Personal Claude overrides belong in `.claude/settings.local.json`, which is intentionally not committed.
- Existing project subagents remain in `.claude/agents/`.

## Codex Files

- Codex reads `AGENTS.md` from the repo root down to the current working directory.
- No repo-tracked Codex rules are required for this project.
- Personal Codex fallback or home-level preferences should stay in each developer's local Codex config.

## Verification

- Codex from repo root: `codex --ask-for-approval never "Summarize the current instructions."`
- Codex from a subdirectory: `codex --cd frontend --ask-for-approval never "Show which instruction files are active."`
- Claude Code: open the repo and confirm `CLAUDE.md` loads the imported `AGENTS.md` guidance and still exposes `.claude/agents/`.
- For repo-structure, shared config, Docker wiring, or agent-workflow changes, restart the stack with `docker compose down && docker compose up --build -d` and run at least one smoke check such as `curl http://localhost:8222/healthz` or `curl http://localhost/api/system/status`.
- For frontend, browser-flow, or operator-workflow changes, also run actual Playwright verification from `e2e/`, for example `npm test -- tests/health.spec.js tests/dashboard-command-pipeline.spec.js` or the narrowest relevant spec set.

## Notes

- Prefer `frontend/` for UI work. The runtime service is still named `dashboard`, but its source lives in `frontend/`.
- Keep local-only secrets, machine-specific permissions, and experiments out of committed agent config.
