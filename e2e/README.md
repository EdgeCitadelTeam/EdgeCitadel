# End-to-end checks

## Existing jim-eq trace UI checks

Real end-to-end work uses the existing jim-eq deployment. The `test:playwright`
npm script creates an isolated local stack and must not be used for this target.
Invoke Playwright directly after deploying the current Core/dashboard:

```bash
EDGECITADEL_TRACE_UI_E2E=1 APP_URL=http://jim-eq AGG_URL=http://jim-eq \
WS_BASE_URL=ws://jim-eq/ws \
/Users/yefanzhang/workplace/edge-research/e2e/node_modules/.bin/playwright test \
  --config /Users/yefanzhang/workplace/edge-research/e2e/playwright.config.js \
  /Users/yefanzhang/workplace/edge-research/e2e/tests/trace-execution-map.spec.js \
  /Users/yefanzhang/workplace/edge-research/e2e/tests/keyboard-shortcuts.spec.js
```

The opt-in trace spec checks the jim-eq hostname, obtains the separate read
credential over authorized SSH into process memory, discovers a retained
completed Hermes task, and exercises the deployed browser UI. It also uploads
the tracked `helpers/trace-live-task.py` into a unique private server directory
and drives one fresh real Hermes acknowledgment task. The helper owns/revokes
its temporary connector and closes its session; it checks exact source/Core
event tuples and export settlement. It requires the configured Core and Leaf
agentd runtimes and `jim-eq-hermes` to be running. It never starts a local stack.

The history check discovers an earlier server snapshot in a fresh browser,
refreshes metadata without moving the graph, reloads the frozen URL, and asserts
that browsing sent no execution requests. Set
`EDGECITADEL_TRACE_UI_EVIDENCE_PREFIX` to a distinct lowercase filename prefix
when recording a new milestone, preserving earlier evidence artifacts.

Screenshots and sanitized results go under the local architecture execution
evidence directory; browser trace/video capture remain off. Credentials are not
stored in screenshots, browser storage or URLs. Server diagnostics stay private
on jim-eq. These focused checks do not replace the full S1/S4/S6, fault, security,
performance and deterministic regression gates.
