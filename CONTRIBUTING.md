# Contributing to EdgeCitadel

Engineering guidance lives in [AGENTS.md](AGENTS.md). This guide covers setup,
where code lives, and how to verify a change.

## Setup

```bash
git clone https://github.com/EdgeCitadelTeam/EdgeCitadel.git
cd EdgeCitadel
```

Follow [onboarding](docs/onboarding.md) for a working Core/Edge environment.
Start only the services needed for the task. Runtime and Agent Package setup is
in [agent-runtime/README.md](agent-runtime/README.md); it includes the editable
Python environment and package lock/validation commands.

For root Python tooling:

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r scripts/requirements-test.txt
```

For backend tests, install `aggregator/requirements-dev.txt` in the backend Python
environment. For frontend development, run `npm ci` in `frontend/`, then
`npm run dev`. Browser tests use the dependencies in `e2e/` (`npm ci` there).

## Find the implementation

| Directory | Responsibility |
|---|---|
| `aggregator/` | FastAPI backend, NATS subscriptions, SQLite persistence |
| `frontend/` | React/Vite dashboard |
| `e2e/` | Browser tests and disposable test stacks |
| `agent-runtime/` | agentd, Agent Package runtime, SDK and validation |
| `agent-packages/`, `plugins/` | Installable Agents and native host integrations |
| `edgecitadel/` | Python distribution entrypoint |
| `scripts/`, `deploy/`, `nats/`, `nginx/` | CLI, deployment and service configuration |
| `docs/`, `local-docs/` | Maintained guides and ignored local working notes |

## Verify the change

Choose checks for the behavior and callers you changed. These are alternatives,
not a checklist to run in sequence. Python commands use the configured environment
for that subsystem; see the runtime README for its source/editable setup.

| Area | Focused command | Working directory |
|---|---|---|
| Backend API | `python -m pytest -q tests/test_api.py` | `aggregator/` |
| Runtime | `python -m pytest -q tests/agentd/test_store.py` | `agent-runtime/` |
| Frontend unit test | `npm test -- src/components/StatusBadge.test.jsx` | `frontend/` |
| E2E helper | `node --test helpers/stack-config.spec.js` | `e2e/` |
| Browser workflow | `npm run test:playwright -- tests/operator-journey.spec.js` | `e2e/` |

Use the pinned Ruff in `scripts/requirements-test.txt` for changed Python files.
Frontend lint/build commands are `npm run lint` and `npm run build`. The
[verification recipe](.agents/skills/commit-check/SKILL.md) covers typing, package
locks and when broader checks help.

Run real end-to-end checks against the existing server on `jim-eq` over
`root@jim-eq`; updating that deployment to the latest changes is authorized.
Preserve its state, verify the deployed revision, and record the behavior tested.
The disposable local browser runner does not replace this live acceptance path.
`e2e`'s `npm test` builds its own stack, so select a remote-targeted invocation for
live checks. External Agent suites use `npm run test:external-plugins` against a
prepared environment.

CI runs root Python, runtime and frontend checks plus the Python build. It does
not currently run Aggregator tests or opt-in broker/E2E suites; run those when
the affected behavior calls for them. Reuse valid results for unchanged code.

## Explain the result

Use clear commit and PR descriptions: what problem was solved, why this approach,
and how it was verified. There is no required branch-name or commit-message format.
Call out changed interfaces or behavior so reviewers can assess the impact;
backward-compatible implementations are not required.

Review correctness, ownership/data flow, failure behavior, readability and useful
test coverage. Prefer a simpler complete solution over speculative generalization;
style preferences alone should not block a sound change.
