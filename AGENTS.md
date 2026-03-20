# EdgeCitadel Agent Guide

This repository supports both Codex and Claude Code.

## Canonical Instructions

- Treat this file as the team-shared source of truth for coding-agent guidance.
- `CLAUDE.md` exists only as a Claude compatibility wrapper that imports this file.
- Add narrower `AGENTS.md` files inside active subprojects when local rules differ.

## Project Priorities

- Keep the active product paths healthy: `aggregator/`, `frontend/`, `openclaw-client/`, and `e2e/`.
- Optimize for small, reviewable changes that preserve runtime behavior unless the task explicitly changes behavior.
- Prefer explicit documentation of workflow and verification changes so future agents inherit the same operating model.

## Repo Map

- `aggregator/`: Python FastAPI backend, NATS subscriptions, SQLite persistence.
- `frontend/`: active React/Vite dashboard. Use this for UI work.
- `openclaw-client/`: Node MQTT listener for agents.
- `e2e/`: Playwright end-to-end tests and test stack.
- `docs/`: architecture and operational documentation.
- `dashboard`: current Docker/nginx service name for the UI, backed by `frontend/`.

## Working Rules

- Inspect the nearest `AGENTS.md` before editing in a subdirectory.
- Prefer targeted changes over broad rewrites.
- Preserve user changes already in the worktree.
- Follow existing patterns in the touched area before introducing new abstractions.
- Keep docs and commands aligned with the current code, not stale historical structure.
- Update the closest relevant docs when a change alters workflow, architecture, setup, or verification expectations.

## Coordination Rules

- Root `AGENTS.md` defines repository-wide workflow, coordination, and verification policy.
- Nested `AGENTS.md` files should add local constraints only; they should not duplicate or contradict root policy.
- Cross-cutting changes that touch backend, frontend, client, infra, or tests should leave all affected areas in a consistent state in one pass when feasible.
- When a change spans multiple subsystems, document the verification performed for each touched subsystem, not just the one where the edit started.
- Treat `frontend/` as the only UI source root. The service name `dashboard` is current, but the old `dashboard/` directory has been retired.

## Commands

- Full stack: `docker compose up --build -d`
- Full stack restart: `docker compose down && docker compose up --build -d`
- Backend dev: `cd aggregator && uvicorn main:app --host 0.0.0.0 --port 8000 --reload`
- Frontend dev: `cd frontend && npm run dev`
- Frontend build: `cd frontend && npm run build`
- Client listener: `cd openclaw-client && npm start`
- E2E tests: `cd e2e && npm test`

## Commit and Branching

- Prefer Conventional Commit messages: `feat|fix|docs|style|refactor|perf|test|chore|ci|build(<scope>): <description>`.
- Useful scopes in this repo include `aggregator`, `frontend`, `nats`, `mqtt`, `dashboard`, `e2e`, `client`, and `infra`.
- Prefer feature branches and PRs for normal work. If a task explicitly requires direct work on `main`, keep the diff narrow and verification explicit.

## Quality Gates

- No secrets, tokens, passwords, or local-only machine config in committed files.
- Keep architecture, setup, and API docs aligned with behavior changes.
- New messaging subjects or payload contracts should be reflected in `docs/05-messaging.md`.
- Config changes should update `.env.example` and the relevant setup docs when applicable.
- API or schema changes should update the relevant docs and touched models in the same pass.

## Parallel Work

- Prefer isolated branches or worktrees for concurrent tasks.
- Do not rely on `git stash` as a shared coordination mechanism across concurrent sessions.
- When rebasing or force-pushing a task branch, prefer `--force-with-lease`.

## Validation

- Run the narrowest relevant verification after changes.
- If you change repository structure, shared agent instructions, committed config, Docker wiring, or other cross-project workflow files, restart the stack and run at least one smoke check that exercises the affected path.
- Backend-only Python edits: prefer a syntax check such as `python3 -m py_compile aggregator/*.py`.
- Frontend changes: run `cd frontend && npm run build` and an actual Playwright E2E verification, not just static checks.
- Backend API or messaging changes: run the backend syntax check and a runtime smoke path if the stack is available.
- OpenClaw client changes: run the relevant listener command if the broker environment is available.
- E2E changes: run the impacted Playwright tests when the test environment is available.
- Shared workflow/config/doc-only changes with runtime impact: use `docker compose down && docker compose up --build -d`, then verify at least one relevant endpoint or workflow and run Playwright if the change affects UI delivery, browser flows, or agent/operator workflow.
- Smoke checks can include one or more of: `curl http://localhost:8222/healthz`, `curl http://localhost/api/system/status`, or a targeted Playwright spec from `e2e/`.
- Prefer actual Playwright CLI verification via `cd e2e && npm test -- <specs>` for targeted runs or `cd e2e && npm test` for broader coverage. Do not treat curl-only checks as sufficient for UI or workflow changes.
- If verification cannot run, state that clearly.
