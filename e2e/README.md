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
on jim-eq. These focused checks do not replace the remaining S1/S6, fault, security,
performance and deterministic regression gates.

The S4 denial case uses `helpers/trace-denied-dispatch.py` with a temporary native
connector that has tracing permission but no delegation grant. It verifies the
real durable rejection, retry without additional observations, absence of child
tasks on both sources, and exact Core event/export settlement, then checks the
deployed map and observation inspector. It does not require a running Hermes
worker: authorization rejects the request before delivery. Run only this case
with `--grep 'S4 denied'` added to the direct Playwright command above. The helper
always closes its session and revokes its own connector; private server evidence
stays under `/root/edgecitadel-s4-20260919/`.

The S6 case (`--grep 'S6 collector'`) uses the live task helper's explicit
`--collector-outage` mode. After the browser sees the native root, the helper uses
the existing authenticated collector control to stop collection, checks that
broker/task transport stays available, and waits for the browser's stale warning.
A real Hermes acknowledgment completes while its execution observations remain
uncollected. The browser must keep the old graph without fabricating a child.
Collection then resumes; the helper checks exact source/Core settlement and the
browser must show the completed child and recovered collection status. All
handshakes are bounded; the helper restores collection in `finally`, and the test
releases handshakes and waits for helper cleanup even after an assertion failure.
Only run this case when a temporary fleet-wide collector pause is appropriate.

## Three-worker S1 qualification

The opt-in S1 case provisions three uniquely named, temporary Hermes Agent
Packages on the existing jim-eq Leaf. Each has its own execution-bound wrapper,
loopback port, HTTP token and private Hermes profile. The helper copies only the
existing provider configuration and credentials into those private profiles;
MCP servers are disabled there, and the API toolset is limited to terminal and
scoped delegation. It leaves the existing Hermes gateway/adapter running.

Prepare the private Python dependency overlay on jim-eq once, using the matching
installed release (the Hermes interpreter must be Python 3.12 or newer):

```bash
ssh root@jim-eq 'install -d -m 700 /root/edgecitadel-s1-20260919; /root/.local/bin/uv pip install --python /opt/hermes-agent/venv/bin/python --target /root/edgecitadel-s1-20260919/python /root/.local/share/uv/tools/edgecitadel/share/edgecitadel/agent-runtime'
```

Add `EDGECITADEL_TRACE_S1_E2E=1` to the existing direct Playwright environment and
`--grep 'S1 three'` to its command. This case needs the configured production
provider and can make model requests. It uses a native connector root to dispatch
three distinct child tasks; each model executes `sleep 8` and prints its own
marker. Local Hermes tool records must prove successful output independently of
the model's final reply. Actual tool start/end source positions must prove that
all three actions overlapped. Each child needs model/tool observations, exact
source/Core event and export settlement, and the root emits an explicit completed
outcome. The browser checks concurrent child activity, resolved root/child links,
model/tool steps, completed root, owner inspection and text view.

`helpers/trace-multi-worker.py` owns the temporary packages, connectors and wrapper
processes; its cleanup attempts every owned resource even if another cleanup
fails. Private profiles/logs remain under `/root/edgecitadel-s1-20260919/` for
operator diagnosis and must not be exported. No production approval setting is
changed: the workload uses basic shell commands accepted by the unattended API,
not a Python `-c` command that Hermes blocks. S1 uses three agents on one Leaf;
it does not qualify the full multi-host topology or a human Codex session.

The retained branch-browser check (`--grep 'retained branches'`) discovers a
retained S1 run with three tasks and repeated model operations. It expands a task
and operation group with keyboard controls, selects an exact step despite an
active map filter, checks the inspector/selection, and verifies 320/1440 px layout
and zero non-GET API requests. Run S1 first if its retained history has expired.

The metadata case (`--grep 'hostile metadata'`) uses
`helpers/trace-hostile-metadata.py` on jim-eq. Its trace-only connector rejects five
invalid inputs without journal writes, then reports explicitly synthetic tool
metadata to exercise inert labels and canonical local-reference display. Exact
source/Core payloads settle and the connector is revoked. These observations do
not claim real tool execution or a retrievable local record. The browser separately
tampers with its graph read to verify strict rejection, checks no reference fetch
or execution API writes, and saves a sanitized report and screenshot.

The large-run case (`--grep 'large retained'`) additionally requires
`EDGECITADEL_TRACE_LARGE_E2E=1`. `helpers/trace-large-run.py` emits 600 explicitly
synthetic operations through a temporary trace-only connector on jim-eq, checks
1,202 exact source/Core events and settlement, and revokes the connector. It is
not tool-execution evidence. The browser follows real graph expansions beyond
500 initial nodes, visits every map page and selects a final-page step/text view.
It asserts complete node/relationship coverage and no execution API writes.
For read-only repetition, `EDGECITADEL_TRACE_LARGE_FIXTURE` may identify the existing
private `/root/edgecitadel-large-20260919/run-<timestamp>` directory; expired data
fails rather than silently creating a replacement. The single cold-load/heap
samples include cloned-response observer overhead and are diagnostic, not the
1,000-observation commit-to-render or retained-memory acceptance measurements.

The live focus case (`--grep 'keyboard focus survives'`, with the same large-run
opt-in) uses `trace-large-run.py --focus-update`. After its initial 600 operations,
the helper waits for the browser's focused canonical step ID, emits a new span
whose ID sorts before it, and verifies all 1,204 events settle exactly. The browser
uses End/Enter to check focus and selection across the resulting page boundary
with reduced motion enabled. The helper handshake is bounded, and the test releases
it in cleanup even after browser failures. This remains synthetic observation
qualification, not actual tool execution or a full accessibility audit.

`node helpers/trace-replay-profile.js <retained-trace-id> <absolute-report-path>`
performs read-only diagnostics on jim-eq. It samples the oldest snapshot from the
first retained-history page and reads its remaining changes, recording HTTP times
and patch/snapshot/clear counts. Tokens stay in memory and reports contain no
credentials or signed cursors. Compare the same trace and cursor interval across
releases; these single samples do not establish browser latency or p95 targets.

The replay profiler accepts an optional third argument: an absolute private file
containing a previously read history response. This mode requires a contiguous
window of at most 63 commits and checks that replay ends at its exact final graph
token. It enables repeatable before/after comparison while newer commits arrive.
Keep that input private; output reports omit its signed cursors. This mode fails
if any position is missing, expired or no longer reconstructible.

The live burst case (`--grep 'live burst above'`, with the large-run opt-in) uses
`trace-large-run.py --burst-update`. It waits for 500 initial operations to project
and for the real browser socket to connect, then emits 100 more operations and
finishes the root. It checks at least 201 distinct patch commits, zero graph
refetches during the burst, all final nodes reachable/terminal and 1,202 exact
settled events. HTTP replay and socket observations are deduplicated in browser
memory; reports omit signed cursors. The single release-to-final-display timing
includes emission, transport and instrumentation and is not commit-to-render p95.

## Controlled display latency

[The measurement plan](trace-latency-plan.md) identifies the actual commit and
React display boundaries, conservative latency bounds, required failure tests
and the 1,000-observation sampling gate. The clock prerequisite is executable:

```bash
ssh -o BatchMode=yes root@jim-eq /usr/bin/python3 - < /Users/yefanzhang/workplace/edge-research/e2e/helpers/trace-clock-probe.py
```

This read-only probe requires the existing running Core container. Its twenty
clock comparisons do not measure display latency. The bounded commit observer
component is implemented and tested in `helpers/trace_commit_observer.py`; the
service launcher and rendered acknowledgments now have a ten-sample jim-eq pilot.
A 1,000-terminal-state fixed-rate diagnostic is also verified; full-duration
baseline, stress, soak and observer-overhead qualification remain open.

The pilot consists of `trace-latency-pilot.py`, `trace-latency-core.py`,
`trace-latency-fixture.py`, `trace-latency-browser.js`, `trace_render_receiver.py`,
`trace_commit_observer.py`, `trace_latency_workload.py` and `trace-clock-probe.py`
in `helpers/`. Copy this set
into `/root/edgecitadel-latency-20260919/helpers` on jim-eq. The browser helper
requires the pinned local `playwright` and `playwright-core` packages in the
adjacent `node_modules` directory and the existing `/snap/bin/chromium`; use the
repository E2E lock/dependencies, without upgrading them.

Create a new, empty private output directory for each run, then invoke the
controller on jim-eq with its absolute path. For example, after preparing the
helpers/dependencies and an unused directory:

```bash
ssh -o BatchMode=yes root@jim-eq '/usr/bin/python3 /root/edgecitadel-latency-20260919/helpers/trace-latency-pilot.py /root/edgecitadel-latency-20260919/pilot-new'
```

This temporarily restarts Core with a scoped observer and restores the normal
command/image afterwards; it does not build an image or change source Agent
services. It rejects an unexpected existing Compose override. Browser failures
and controller exceptions unwind owned processes before restoration. SIGKILL or
host failure cannot guarantee cleanup: inspect the active Compose command and
restore the normal managed configuration before another run.

Private `scope.json` contains a receiver token and must not be exported. Keep
raw commit/ack reports and process logs on the server. `result.json`, `clock.json`
and `pilot.png` are the sanitized qualification artifacts. `restoration.json`
records exact image/command/config restoration and collector readiness, including
on a failed pilot. Ten samples must never be labeled p95 qualification.

For a predeclared fixed-rate terminal-state diagnostic, add
`--open-loop-samples 1000` (default rate `25/3` exported tool events/s). The
source emits one start/finish pair per operation without waiting for the browser,
renews its session and reports schedule lateness. `--event-rate` explicitly
changes the requested rate. Sample counts are capped at 1,500 to remain within
the commit observer's bound; this is not the 30-minute baseline runner.

Only terminal observations are eligible in this mode; the plan is written before
emission. The browser reveals each canonical step through the existing filter,
so conservative bounds include selection/filtering and acknowledgment overhead.
All declared samples must correlate before a nearest-rank p95 is reported, and
fewer than 1,000 samples yield no p95. Missing events or a timed-out browser fail
the run; they are not discarded. This does not qualify transient start-state
display, the adopted full-duration baseline/stress/soak schedule, or actual tools.
