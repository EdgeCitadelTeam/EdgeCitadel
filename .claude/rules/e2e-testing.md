---
paths:
  - "e2e/**"
---

# E2E Testing Rules (Playwright)

## Structure
- Test files: `e2e/tests/*.spec.js`
- Helpers: `e2e/helpers/` (fixtures, cleanup, API client, MQTT client)
- Config: `e2e/playwright.config.js` (3 workers, 1 retry, baseURL: localhost:13000)

## Conventions
- Use fixtures from `e2e/helpers/fixtures.js` for test setup
- Clean up test data with `e2e/helpers/cleanup.js` after each test
- Prefix all test data with `test-` to avoid polluting real data
- Use proper Playwright locators (`getByRole`, `getByTestId`) — not CSS selectors
- Wait for elements with `expect(locator).toBeVisible()` — never `page.waitForTimeout()`
- Each test must be independent — no reliance on other test state

## Test Infrastructure
- `docker-compose.test.yml` for isolated test environment
- `test-nats.conf` for test NATS configuration
- `global-setup.js` starts services, `global-teardown.js` stops them
- Port 13000 for test nginx (avoids conflicts with dev)

## What to Test
- Agent registration and discovery flow
- Message send/receive through NATS→WebSocket pipeline
- Task lifecycle (create→assign→progress→complete/fail)
- Dashboard real-time updates via WebSocket
- Error states (agent offline, NATS disconnected)
