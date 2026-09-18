# Working in EdgeCitadel

## Make the change
- Read the relevant source, tests and nested instructions. Follow local patterns
  and make the smallest complete change; avoid speculative abstractions.
- Preserve unrelated user changes. Before deleting code/tests, check callers,
  entrypoints and maintained contracts; retain coverage for active behavior.
- Resolve routine implementation choices independently. Ask when an unresolved
  choice materially changes scope, behavior or risk.
- Use focused Conventional Commits on a feature branch. Routine fixes need no
  separate plan or verification report; save useful working notes in `local-docs/`.

## Verify proportionally
- Start with tests for the changed behavior and its callers. Broaden for shared
  contracts, cross-component effects, or failures, not simply file count.
- For prose-only changes, review content and links. Check runnable examples or
  configuration when those change; do not launch the application by default.
- Reuse passing results while source, dependencies, relevant configuration and
  environment remain unchanged. Commit boundaries alone do not require reruns.
- Commands: `CONTRIBUTING.md`. Check selection: `.agents/skills/commit-check/SKILL.md`.
  Consult the relevant `verify-*` recipe only when additional detail is needed.
- Report checks and meaningful gaps; skips are not passing evidence. Keep CI,
  release checks and hooks intact; do not assume they cover opt-in integration.

## Find the code
- `aggregator/`: backend/NATS/SQLite; `frontend/`: dashboard; `e2e/`: owned test stacks.
- `agent-runtime/`: agentd, runtime, SDK and tests; `agent-packages/`: installable Agents.
- `plugins/`: native host integrations; `edgecitadel/`: Python distribution.
- `scripts/`, `deploy/`: CLI/deployment; `docs/`: maintained guides.
- Setup: `docs/onboarding.md`; package workflow: `agent-runtime/README.md`;
  tracing: `docs/architecture/execution-trace-contract.md`.

## Keep changes reviewable
- Keep credentials, `.env`, local settings and runtime data out of commits.
- Update `.env.example` for environment configuration changes and the relevant
  guide for user-facing workflow changes. Keep instructions here short; put
  detailed procedures beside the subsystem that owns them.
- Follow the user's authorization for publishing and destructive actions.
