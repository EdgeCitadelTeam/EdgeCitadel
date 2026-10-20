# Trace read client

Execution uses a DOM/SVG host topology above a single interaction-order list.
Current registry membership is labeled with its read time and is not historical
routing evidence. Each host contains its configured leaf and Agent identities;
configuration does not imply a live leaf connection. The canvas initially previews
the first drawable message. Selecting a numbered card draws that aggregate message,
with explicit sender/recipient direction, dashed results, and two finite motion
passes. Replay is explicit; reduced-motion mode hides the moving dot.

Core, transport and Agent cards show configured host/IP and port values from
`edgecitadel.core_address` and `edgecitadel.nats_address` registry metadata.
The producer strips credentials, paths and query strings. Agent endpoints are
labeled "via NATS": Agents share their host transport rather than exposing a
separate listener. Missing fields say "IP / port not recorded"; an offline Agent
can use its same-node registry peer's current transport address. Core addresses
are configured client endpoints, not a claim about the leaf listener or historical routing.

All retained runs and optional snapshot history live in the left navigation rail,
replacing the unrelated agent picker on Agent Flow. On small screens, Trace history
opens the rail as a left drawer. There is no search or task filtering.
Load more runs appends older pages without removing the earlier runs.
The interaction order list paginates 50 aggregate messages.
Publication, receipt, broker observations and acceptance share a message ID;
redelivery remains in details. Independent tasks do not acquire causal order
from numbering. Unknown identities and conflicts remain unassociated evidence.

Desktop uses the full canvas until selection opens a 360px detail column,
expandable up to 620px. At 1150px and below details become a 440px overlay,
and at 700px and below they fill the viewport. The topology scrolls horizontally
when necessary to keep its cards readable. The same
complete order drives card keys, Previous/Next and detail ArrowUp/Down/Home/End.
Closing restores the selected card, including its page, without scrolling.

Overview contains snapshot bodies/acceptance, Agent identity/session state or
transport responsibilities. Execution groups evidence by source, epoch and
attempt. Evidence retains complete original events/IDs and deep links. Agent
session state stays separate from protocol results. Runtime `agentd_sqlite`
observations establish local committed communication without broker transport;
NATS observations identify their own provenance. Current topology is a reference.

## Snapshot ownership

Create one `createTraceApi()` per mounted explorer and one
`createTraceSession(api)` per visible run/history selection. The API uses the same
trusted-network dashboard read boundary, canonical Ajv schemas and read-only
HTTP/WebSocket endpoints. No persistence schema or service interface is added.
A session owns one abortable graph/update loop, including graph expansion and
ordered catch-up. Only applied changes advance replay cursors; heartbeats never
acknowledge unseen graph commits. StrictMode recreates disposed resources.

`useCommunicationSnapshot` binds event pages to the graph's exact `at` and
projection generation. A new graph and its first event batch are published
together while the previous consistent view remains visible during refresh.
Each request loads at most 200 events; at most five pages load automatically.
Continue loading evidence reads another bounded batch. Partial range text never
asserts that an unseen return is lost. Snapshot/run changes abort stale requests;
authorization loss clears and unmounts the view. Conflicting snapshot replies and
repeated cursors are rejected.

Historical rendering uses only the chosen snapshot. `TaskCommunication` is mounted
only when a user requests **Latest messages** in a live task/message overview;
its response never enriches the communication projection or historical evidence.
Collection freshness and instrumentation coverage remain independent of execution
outcomes. Expired/unavailable content and unrecorded acceptance remain explicit.

## Navigation and verification

Selection retains `#execution?run=...&task=...&step=...&at=...&event=...` and adds
optional `message=...`. Original attempt/tool/transport IDs resolve to their owning
Agent, task or communication and open Evidence. Exact event links open their raw
observation; sparse event pages are followed within the same bounded loading
budget. More evidence remains user-loadable after that budget. Invalid saved
parameters do not silently fall back to live. References are rendered as inert
text, never fetched as arbitrary URLs.

Run `npm test -- src/traces`, `npm run lint` and `npm run build` in `frontend/`.
The remote browser acceptance uses the existing jim-eq deployment:

```sh
APP_URL=http://jim-eq AGG_URL=http://jim-eq EDGECITADEL_COMMUNICATION_E2E=1 \
  ./node_modules/.bin/playwright test --config playwright.config.js tests/trace-communication.spec.js
```

Run that command in `e2e/`. Its existing `npm run test:playwright` launcher provisions
a disposable local stack, so use the direct Playwright invocation for jim-eq.
The remote spec checks the retained two-Agent/two-task/four-message trace rendered with one focused message arrow, failed
tools, original event links, frozen reload, themes, keyboard selection and drawer
focus/scroll restoration. Existing low-level graph/session/protocol suites remain
relevant; old execution-node canvas E2E selectors describe the superseded UI.
This bounded acceptance is not a fleet-scale memory or latency qualification.
