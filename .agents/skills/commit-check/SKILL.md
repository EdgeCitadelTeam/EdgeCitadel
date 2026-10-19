---
name: commit-check
description: Select relevant pre-commit checks and reuse valid results; do not run every subsystem for every commit.
---

# Check the change before committing

Inspect the staged diff, including deletions, and run `git diff --cached --check`.
Check for accidental credentials/local files and use a clear description of the
change. Select verification by behavior, not extension alone. A deleted file may
require checking callers instead of testing that file.

| Change | Local verification |
|---|---|
| Prose or agent instructions | Review content, links and consistency; validate changed runnable examples/configuration without defaulting to an application suite |
| Python behavior | Pinned Ruff lint/format on changed files, plus focused pytest tests and affected callers |
| Typed SDK or shared validators | Relevant maintained mypy command below, plus contract tests |
| Frontend behavior | Frontend lint, affected unit tests and build; browser/E2E coverage for changed user interactions |
| E2E helper or fixture | Its tests and the affected Playwright spec; frontend build only if frontend/build inputs changed |
| Deployment, NATS or shared configuration | Validate rendered config and affected integration path; see `verify-infra` |
| Agent Package contents | Adapter regression tests; regenerate and validate the affected package lock as documented in `agent-runtime/README.md` |
| Packaging or dependencies | Package/build or installed-artifact checks for the affected distribution |

Reuse the existing project environment when dependencies are adequate. Python
lint/format uses the Ruff version in `scripts/requirements-test.txt` and target
`py312`; do not upgrade tooling as part of an unrelated task. Pytest locations:
`aggregator/tests`, `agent-runtime/tests`, `scripts/tests`, `tests`, `deploy/tests`,
and `schemas/tests`. Select applicable files or test names first. Running the
root suite already includes `scripts/tests`; do not run it twice.

Maintained typing commands, from `agent-runtime/`:

```bash
python -m mypy --strict src/edgecitadel_plugin_sdk tests/typecheck_sdk_consumer.py
python -m mypy --strict src/edgecitadel_plugin_runtime/validator.py src/edgecitadel_plugin_runtime/jetstream.py ../aggregator/validator.py ../aggregator/jetstream_bootstrap.py
```

Run the command for the changed typed surface. Aggregator has no passing global
strict-type baseline; do not add broad suppressions or claim otherwise.

Broaden testing when the dependency surface or a failure warrants it. Reuse
passing checks if their source, dependencies and relevant configuration are
unchanged, including the relevant execution environment; commit boundaries alone do not invalidate results. Do not create
validation worktrees or rebuild environments for routine commits.

Before committing, resolve failures relevant to the change. Report the checks
run, their results, and relevant checks unavailable or deferred. No fixed
checklist or separate verification document is required. CI/release workflows
remain the authority for their own full gates; do not skip hooks. CI currently
does not run the Aggregator suite or opt-in broker/E2E gates, so check those
locally when affected rather than assuming CI covers them.
