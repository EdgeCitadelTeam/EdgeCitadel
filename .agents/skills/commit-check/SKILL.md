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
ruff check aggregator/ --fix
ruff format aggregator/ --check
mypy aggregator/ --strict
pytest tests/ -x --tb=short
```
All must pass with zero errors.

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

## 6. Documentation Check
If the change introduces new features, API endpoints, or NATS subjects:
- Is `docs/` updated?
- Is `docs/CHANGELOG.md` updated?
- If architectural: is there an ADR in `docs/adr/`?

## Report
Output a pass/fail checklist:
- [ ] Lint clean
- [ ] Types check
- [ ] Tests pass
- [ ] Commit message valid
- [ ] No secrets detected
- [ ] Docs updated (if applicable)

Block the commit if any required check fails.
