---
paths:
  - "e2e/**"
---

# E2E Testing Rules (Playwright)

> Authoritative: `e2e/playwright.config.js`, `e2e/playwright.smoke.config.js`,
> `e2e/tests/phase1-smoke.spec.js`, `e2e/tests/phase2-gemma-smoke.spec.js`.

## Structure

- Test files: `e2e/tests/*.spec.js`. Phase smoke specs use either
  `phase{N}-...-smoke.spec.js` (Phase 1–2) or `phase{N}-<feature>.spec.js`
  (Phase 3+). The smoke config picks both up via a widened `testMatch`.
- Helpers: `e2e/helpers/fixtures.js` exports
  `buildCanonicalEnvelope({type, sender_id, recipient_id, task_id, body,
  payload})`. The legacy `helpers/mqtt-client.js`,
  `helpers/test-data.js`, `helpers/cleanup.js` were retired in Phase 1
  Task 17.
- Configs:
  - `playwright.config.js` — full test stack via `globalSetup` →
    `e2e/docker-compose.test.yml` on port 13000. Currently blocked by
    the same NATS auth bug fixed for the dev stack (Phase 1 follow-up).
  - `playwright.smoke.config.js` — minimal config that bypasses
    `globalSetup` and points at the already-running dev stack via
    `AGG_URL=http://localhost`. Use for ad-hoc verification today.

## Conventions

- CommonJS `require('@playwright/test')` — `e2e/package.json` has no
  `"type": "module"`. Don't introduce ESM imports without converting
  the package.
- Use the `request` fixture for HTTP-only tests (no browser needed for
  envelope round-trip checks).
- Polling pattern for async results: bounded `for` loop with
  `setTimeout`, ~60s budget for LLM workloads, ~15s for shell.
- Each test must be independent (no reliance on prior-test state).
- Use Playwright locators (`getByRole`, `getByTestId`); never CSS
  selectors.
- Wait with `expect(locator).toBeVisible()`; never
  `page.waitForTimeout()`.

## Test data isolation

- Tests that drive PRODUCTION agents (e.g. `gemma-1`, `shell-1` via
  `POST /api/command/{id}`) currently pollute the messages table with
  `deployment="default"` — not filterable by the `showTestAgents`
  toggle. Phase 1 follow-up tracked in `docs/roadmap.md`.
- The intended convention: a test runner registers its OWN agent with
  `runtime.deployment: test` in its A2A card and routes commands
  through that agent. `MessageRouter._deployment_for` then propagates
  `deployment="test"` to all related rows.

## What to test

- Agent registration and discovery (canonical `register` envelope +
  `/api/agents` listing).
- Round-trip: `POST /api/command/{id}` → `task_id` → polled `result`
  row in `/api/messages`.
- Card metadata (A2A v1.0 fields, NATS extension URI, `runtime.kind`,
  `runtime.roles`, skill identity).
- Queue endpoint (`/api/agents/{id}/queue`) returns integer pending
  and ack_pending.
- `/api/system/status` shape: `nats_connected`, `jetstream_stream_ok`,
  `version`. Must NOT contain `mqtt_connected`.
- Subject inventory coverage: DB persists `command` and `result` rows
  (via outbox mirror, ADR-0006); `register`/`heartbeat` update the
  `agents` table only, not `messages`.

## What NOT to test (yet)

- WebSocket real-time updates — `/ws/stream`, `/ws/agent/<id>` are
  Phase 1 follow-ups; frontend polls today.
- Test stack on `:13000` via `globalSetup` — blocked until
  `e2e/test-nats.conf` receives the same auth fix the dev `nats.conf`
  got in commit `abcce8e`.
