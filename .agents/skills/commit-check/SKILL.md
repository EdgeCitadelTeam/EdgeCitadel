---
name: commit-check
description: Pre-commit quality gate. Run before every commit to verify lint, types, tests, and commit message format. Use proactively before git commit.
---

# Pre-Commit Quality Check

Before committing, verify ALL of the following:

## 1. Identify Changed Files
```bash
git diff --name-only --cached  # staged files
git diff --name-only           # unstaged changes
```

## 2. Python Quality (if .py files changed)
```bash
uv run --isolated --with-requirements scripts/requirements-test.txt ruff check --target-version py312 aggregator/ scripts/ plugin-toolkit/ plugins/ tests/ deploy/tests/
uv run --isolated --with-requirements scripts/requirements-test.txt ruff format --target-version py312 aggregator/ scripts/ plugin-toolkit/ plugins/ tests/ deploy/tests/ --check
cd aggregator && uv run --isolated --with-requirements requirements-dev.txt python -m compileall -q .
cd aggregator && uv run --isolated --with-requirements requirements-dev.txt python -m pytest -q
uv run --isolated --with-requirements scripts/requirements-test.txt python -m pytest -q scripts/tests
./scripts/research/run-python -m pytest tests/ -x --tb=short
```
All must pass with zero errors.

The Aggregator predates strict typing and does not currently have a passing
repository-wide mypy baseline. Do not claim that it does. Changes to the typed
Plugin Toolkit must run its maintained strict type gate:

```bash
cd plugin-toolkit
uv run --isolated --with-editable '.[type]' python -m mypy --strict src/edgecitadel_plugin_sdk tests/typecheck_sdk_consumer.py
uv run --isolated --with-editable '.[type]' python -m mypy --strict src/edgecitadel_plugin_runtime/validator.py src/edgecitadel_plugin_runtime/jetstream.py ../aggregator/validator.py ../aggregator/jetstream_bootstrap.py
```

Do not add broad suppressions to make a changed module pass. If a change begins
typing an Aggregator module, run strict mypy on that module and its typed
dependencies and document the narrowed scope.

## 3. Frontend Quality (if .js/.jsx files changed)
```bash
cd frontend && npm run lint
cd frontend && npm run build
```
Lint and build must succeed with zero errors.

## 4. Commit Message Validation
Verify the commit message follows Conventional Commits:
```
<type>(<scope>): <description>
```
- type: feat|fix|docs|style|refactor|perf|test|chore|ci|build
- scope: aggregator|frontend|nats|mqtt|dashboard|e2e|client|infra
- description: imperative mood, lowercase, no period at end

## 5. Security Check
```bash
grep -rn "password\|secret\|token\|api.key" --include="*.py" --include="*.js" --include="*.jsx" $(git diff --name-only --cached) 2>/dev/null
```
Flag any matches for manual review.

## 6. Maintainer Check
If the change introduces new features, API endpoints, or NATS subjects:
- Are user-facing instructions updated where the repository currently maintains them?

## Report
Output a pass/fail checklist:
- [ ] Lint clean
- [ ] Applicable maintained types check
- [ ] Tests pass
- [ ] Commit message valid
- [ ] No secrets detected
- [ ] User-facing instructions updated (if applicable)

Block the commit if any required check fails.
