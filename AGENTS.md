# Working in EdgeCitadel

## Approach
- Make the smallest complete change that solves the request. Reuse existing code;
  avoid speculative abstractions, compatibility layers, and configuration.
- Inspect relevant code and nested instructions before editing. Ask only when a
  material ambiguity cannot be resolved from the request or repository.
- Preserve unrelated user changes. Remove code/tests made obsolete by the change;
  broader cleanup should have evidence that the behavior is unused or superseded.
- Use a feature branch and focused Conventional Commits. Keep PR descriptions
  about the problem, resulting behavior, and relevant verification.
- Plans and design documents are useful for substantial uncertainty, not required
  artifacts for routine fixes. Keep temporary work in `local-docs/`.

## Verification
- Choose checks for the changed behavior and its callers. Start with focused
  tests; broaden for shared contracts, cross-component changes, or failures.
- Documentation-only changes need content/link review, not builds or a stack.
- Reuse passing results when the tested code and dependencies have not changed.
  Do not repeat suites solely because another commit is being created.
- Use `.agents/skills/commit-check/SKILL.md` to select checks. The `verify-*`
  recipes explain subsystem checks when needed; they are not cumulative gates.
- Report what ran and any relevant limitations. Skips are not passing evidence.
- CI and release workflows retain their broader checks. Never bypass hooks.

## Repository map
- `aggregator/`: FastAPI backend, NATS subscriptions, SQLite persistence.
- `frontend/`: React/Vite dashboard; `e2e/`: Playwright and owned test stacks.
- `agent-runtime/`: agentd, package runtime, SDK, validation and tests.
- `agent-packages/`: installable Agents; `plugins/`: native host integrations.
- `edgecitadel/`: Python distribution; `scripts/` and `deploy/`: CLI/deployment.
- `docs/`: maintained guides; `local-docs/`: ignored plans and local evidence.

## References
- Development and commit conventions: `CONTRIBUTING.md`.
- Setup/enrollment: `docs/onboarding.md`; runtime/packages: `agent-runtime/README.md`.
- Experimental tracing: `docs/architecture/execution-trace-contract.md`.
- Build/release checks: `.github/workflows/`; host dependencies: `deploy/manifest.toml`.

## Boundaries
- Keep credentials, `.env`, local settings and runtime data out of commits.
- Update `.env.example` when changing environment configuration, and update the
  relevant guide when changing a user-facing workflow.
- Preserve authorization for publishing and destructive operations; task scope
  and user instructions determine approval, not an extra repository checklist.
