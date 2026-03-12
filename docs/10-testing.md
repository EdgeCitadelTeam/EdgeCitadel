# Testing

## Test Infrastructure

EdgeCitadel has two test suites:

1. **Playwright suite**: 112 tests across 21 spec files
2. **Full E2E smoke test**: 38 checks in `full-e2e.js`

Both use a dedicated Docker Compose stack with isolated ports.

## Running Playwright Tests

```bash
cd e2e
npx playwright test
```

This automatically:
1. Builds and starts the test stack (`docker-compose.test.yml`)
2. Waits for services to be healthy
3. Runs all 112 tests in parallel (3 workers)
4. Tears down the stack

### Test Ports

| Port | Service |
|---|---|
| 14222 | NATS (client) |
| 11883 | NATS MQTT adapter |
| 18222 | NATS monitoring |
| 18000 | Aggregator API |
| 13000 | Frontend |

### Test Configuration

- **Config**: `e2e/playwright.config.js`
- **Global setup**: `e2e/global-setup.js` (starts Docker stack)
- **Global teardown**: `e2e/global-teardown.js` (stops Docker stack)
- **Storage state**: `e2e/test-storage-state.json` (sets `showTestAgents: true`)
- **NATS config**: `e2e/test-nats.conf` (no auth, small JetStream limits)

### Test Files

| File | Tests | Description |
|---|---|---|
| `agent-crud.spec.js` | 7 | Agent CRUD API operations |
| `agent-detail.spec.js` | 7 | Agent detail view (profile, stats, commands) |
| `agent-heartbeat.spec.js` | 5 | Heartbeat updates and auto-creation |
| `agent-offline.spec.js` | 3 | Offline detection and UI updates |
| `agent-registration.spec.js` | 5 | MQTT registration flow |
| `agent-sidebar.spec.js` | 5 | Sidebar selection and filtering |
| `commands.spec.js` | 5 | Dashboard command sending |
| `dashboard-command-pipeline.spec.js` | 8 | Full command → reply → task flow |
| `dark-mode.spec.js` | 2 | Theme toggle |
| `flow-graph.spec.js` | 4 | Communication topology graph |
| `health.spec.js` | 3 | Health endpoints |
| `keyboard-shortcuts.spec.js` | 3 | Tab switching shortcuts |
| `logs.spec.js` | 8 | Log viewer filters and display |
| `messages-chat.spec.js` | 6 | Chat history and filtering |
| `messages-correlation.spec.js` | 4 | Correlation ID grouping |
| `messages-realtime.spec.js` | 4 | WebSocket live updates |
| `onboarding.spec.js` | 8 | Agent join protocol |
| `system-status.spec.js` | 4 | System status API and UI |
| `tasks-board.spec.js` | 7 | Task board CRUD and display |
| `tasks-lifecycle.spec.js` | 4 | Task lifecycle state machine |
| `tasks-trace.spec.js` | 3 | Task message trace |

### Test Helpers

- **mqtt-client.js**: MQTT client with convenience methods (`registerAgent`, `sendHeartbeat`, `assignTask`, `completeTask`, etc.)
- **api-client.js**: REST API client
- **ws-client.js**: WebSocket client for testing events
- **test-data.js**: Test agent/ID generators with timestamp suffixes
- **wait-utils.js**: Polling utilities (`pollUntil`, `sleep`)

## Full E2E Smoke Test

Run against the **production** stack (localhost:80):

```bash
node e2e/full-e2e.js
```

This simulates a complete user workflow:
1. Register 3 agents (Rupert, Jeeves, Percy)
2. Load dashboard, verify UI elements
3. Send inter-agent messages
4. Navigate all tabs (Chat, Flow, Logs, Tasks)
5. Send commands from dashboard
6. Verify API persistence
7. Check agent detail view
8. 38 assertions total

Screenshots saved to `test-artifacts/`.

## Demo Recording

Record a video of the dashboard in action:

```bash
node e2e/record-demo.js
```

Outputs:
- `docs/demo-raw.webm` (original)
- `docs/demo.mp4` (H.264, requires ffmpeg)
- `docs/demo.gif` (animated, requires ffmpeg)

## Test Data Isolation

Test agents use IDs with timestamp suffixes (e.g., `agent-1710000000000-1-abc123`). The frontend filters these out by default via the `exclude_test` parameter. The test storage state sets `showTestAgents: true` so tests can see their own data.

Test ID patterns recognized:
- Timestamp suffix: `-\d{13,}-\d{1,3}-[a-z0-9]{3,6}$`
- Test prefix/suffix: `^test[-_]`, `[-_]test$`, `[-_]test[-_]`
