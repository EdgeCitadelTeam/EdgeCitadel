# E2E Guide

## Scope

- This directory owns Playwright coverage and the disposable integration test stack.
- The main config is `playwright.config.js`; tests live in `tests/`.

## Local Rules

- Prefer updating the narrowest test that covers the changed behavior.
- Keep helpers reusable, but avoid moving assertions into helpers unless multiple specs need the same logic.
- Treat generated artifacts under `playwright-report/` and `test-results/` as outputs, not source.
- When product behavior changes without matching e2e coverage, add or adjust tests unless the user explicitly scopes testing out.

## Commands

- Install deps: `npm install`
- Run deterministic owned-stack tests: `npm test`
- Run external Managed Agent tests against a prepared stack: `APP_URL=... AGG_URL=... npm run test:external-plugins`
- Start test stack: `docker compose -f docker-compose.test.yml up --build -d`
- Stop test stack: `docker compose -f docker-compose.test.yml down -v`

## Validation

- Run the smallest relevant Playwright scope you can support.
- For shared workflow, deployment, or agent-config changes, prefer at least one smoke-oriented spec after the stack is restarted.
- For UI, browser flow, or operator workflow changes, treat Playwright execution as required verification, not optional extra confidence.
- If the test stack is not started, state that the suite was not exercised end-to-end.
