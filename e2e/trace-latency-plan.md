# Controlled Core commit-to-display qualification

Status: clock prerequisite, bounded observers, pilot and a 1,000-terminal-state
fixed-rate jim-eq diagnostic are verified. Its conservative p95 is 1.706 s.
Observer-overhead comparison and the full baseline/stress/soak workload remain
open. This does not close M6 or M7.

## Observed boundaries

- `aggregator/trace_store.py:ingest` commits the raw event, export position and
  collector cursor inside one `with connection` transaction. It returns only
  after that context exits. Replayed positions can return the original outcome
  and cursor; an `accepted` result alone does not prove a new commit.
- `aggregator/trace_ingest.py:ingest_delivery` calls `on_commit` after persistence
  returns and before broker ACK. Its observation errors cannot change ACK
  semantics. The existing collector callback measures wall-clock ages, not
  commit-to-display latency.
- `aggregator/trace_collector.py:_run` owns a dedicated SQLite connection.
  Instrument this connection only, without changing other writers or durable
  event/schema contents.
- `frontend/src/traces/TraceExplorer.jsx:RunView` subscribes through
  `useSyncExternalStore`. Socket receipt and session state updates precede React
  commit. A received frame is not evidence that its step state was displayed.
- The current map exposes canonical `data-node-id` values and state classes.
  Large graphs paginate at 100 nodes; a graph cursor alone cannot prove that an
  off-page event was displayed.

## Clock prerequisite

Run `helpers/trace-clock-probe.py` on the existing jim-eq host. It makes twenty
read-only `docker exec` calls to the running Core, brackets each container
`time.monotonic_ns()` sample with host monotonic timestamps, and checks the boot
identity, time namespace and clock implementation. It fails closed on mismatch.
No broker, database, service configuration or process is changed.

The 2026-09-19 sample passed all twenty checks. Host and container both report
`clock_gettime(CLOCK_MONOTONIC)` with 1 ns advertised resolution. Docker execution
round trips were 76.163–83.207 ms (median 80.535 ms). Resolution is not timing
accuracy; these execution times are not observer overhead. Repeat the probe for
new containers/hosts or namespace changes. Do not use SSH/docker execution for
per-event commit timestamp acquisition.

## Implement the observers

1. Use an explicit, temporary qualification launcher around the deployed Core
   entrypoint. Supply a SQLite connection subclass only to the collector. Bracket
   the underlying successful transactional `__exit__` with monotonic nanoseconds;
   clear the marker before each ingestion and ignore rollback/no-transaction
   exits. Capture the last successful transaction for that delivery. Validate
   against a second connection that its event and cursor are committed before
   emitting a record. Keep this verification in tests, not the timed hot path.
2. At the existing commit callback, correlate the bracket with the validated
   `(node_id, source_epoch, event_id)`, collector epoch and ingest position.
   Record only the owned fixture source/run. Keep the first observation per
   identity; a redelivery must never reset the latency start. Bound the record
   count and queue, record overflow and observer failures, and invalidate the
   measurement on either. Never write credentials, payloads or signed cursors
   to exported results. Measure callback/queue/write overhead separately.
3. Run Chromium and its controller on jim-eq against the existing dashboard.
   Use a private host-loopback acknowledgment receiver whose timestamps come
   from Python `time.monotonic_ns()`, on the clock verified above. Keep an opaque
   fixture identity allowlist, cap request size/count, and close the receiver
   and owned browser in `finally`. Do not expose a public diagnostic endpoint.
4. Associate each fixture event with its canonical visible step and expected
   observable state. Observe the React-committed DOM, then use a rendering-frame
   acknowledgment with visibility and state rechecks. Record the exact policy:
   double animation frames allow a paint opportunity but do not prove physical
   pixel scanout. A socket frame, hidden tab, off-page node or a state overwritten
   before the acknowledgment is not a successful displayed sample. Preserve
   missed/coalesced samples rather than silently excluding them.
5. Record host acknowledgment receipt after the browser's rendering
   acknowledgment. With commit bracket `[c_before, c_after]` and host receipt
   `a`, `a - c_before` conservatively includes the commit interval and browser
   acknowledgment transport. This is an upper bound, not exact display latency.
   Do not call `a - c_after` a lower bound: acknowledgment transport is positive.
   Proving p95 of upper bounds <= 2 s is sufficient for the latency target;
   exceeding it is inconclusive about true display latency until observer
   overhead is characterized or the measurement is tightened. Never subtract
   estimated overhead to manufacture a pass.

## Tests and qualification sequence

First test the connection instrumentation with real SQLite transactions:
successful commit visible from another connection; rollback; failed COMMIT;
multiple transactions; idle context; replay returning the original ingest cursor;
and observation failure leaving durable ingestion and broker ACK behavior intact.
Test acknowledgment identity validation, overflow, timeout and cleanup. These are
local component checks; deployed transport/browser qualification stays on jim-eq.

Then run a small complete pilot on jim-eq and inspect every correlation before
scaling. Record the exact executable hashes, container images, clock prerequisite,
Chromium version, viewport/visibility, event schedule, graph sizes, frame policy,
observer overhead and all missing samples. Restore the normal Core launcher and
verify normal collector readiness after every instrumented run, including failure.

The >=1,000-observation run must predeclare its eligible visible observations and
keep the denominator when observations are missing or coalesced. Choose workload
and navigation explicitly so pagination cannot silently remove samples. Report
nearest-rank p95 over the full eligible cohort only when its completion rules
are met; report incomplete sampling otherwise. Do not pad the count with repeated
ACKs, duplicates, heartbeats or hidden graph updates. Start with small graphs to
validate the measurement, then exercise large graphs and the adopted baseline,
stress and soak schedule. A small-graph or same-host-network pass does not cover
large-run catch-up, multi-host topology, retained-volume reads or the full M7 gate.

## Commit observer implementation checkpoint

`helpers/trace_commit_observer.py` now supplies a collector-only SQLite facade,
transaction-exit brackets and a scoped in-memory observer (default 4,096 records,
explicit cap 100,000). It never writes a per-event diagnostic file in the ingest
path. Report serialization is outside the callback; maximum callback duration is
recorded but does not include every instrumentation cost or qualify overhead.
Capacity exhaustion or observer errors invalidate the report without suppressing
the original collector callback or broker ACK. A replay with no transaction
writes cannot create a fresh marker; identities keep their first bracket.

The instrumentation context restores module wiring on exit, including exceptions
and rejected nested installation. It does not manage the service: the future
launcher must start and stop the collector inside the context and export its
report only after shutdown. Production does not import this helper. No global
SQLite monkeypatch, persistence schema or event payload change is introduced.

Local component tests exercise real commit visibility, rollback, deferred
constraint COMMIT failure, idle/multiple transactions, real ingest replay,
observer failure, ingestion rollback/no ACK, scope filtering, capacity overflow
and wiring restoration. Live deployment and browser measurement remain next.

## Live pilot implementation checkpoint

The `trace-latency-core.py` launcher installs the observer before app startup,
lets the normal ASGI lifespan stop the collector, then saves its private report.
It accounts for Uvicorn re-raising SIGTERM after shutdown. The pilot controller
uses a temporary Compose command/mount override, no image rebuild, and restores
the original image/command/configuration after success or failure. Missing report
or incomplete correlations fail qualification. Forced termination cannot produce
a successful sample report.

`trace_render_receiver.py` binds host loopback, authenticates a private token,
caps bodies and expected identities, and preserves the first timestamp. Invalid
requests, overflow and missing acknowledgments invalidate measurement; duplicate
ACKs cannot reset timing. The browser controller alone holds the token. It checks
the canonical node/state, scrolls it into view and verifies a stable visible
center point across two animation frames before acknowledging. This is a paint
opportunity policy, not physical scanout measurement.

The pilot generates five synthetic tool spans, starting and finishing each only
after the previous display ACK. Its ten samples are deliberately closed-loop,
not the adopted open-loop baseline. Exact reconciliation covers twelve source/Core
events including root boundaries. The first launch failed before sampling because
Chromium could not reach the host through its hostname; the normal Core and owned
connector were restored. The browser now accesses the existing server through
`http://127.0.0.1`, an already-authorized origin. Both clock observations and browser
controller run on jim-eq.

Next generalize fixture scheduling and sample eligibility without dropping
missing/coalesced observations, characterize full observer overhead, and run the
>=1,000-observation workload. Preserve separate small/large graph, retained-volume,
network/topology and sustained-load qualifications. A fast pilot cannot waive them.

## Fixed-rate terminal-state diagnostic

The runner now accepts an explicit open-loop cohort (up to 1,500 operations).
It schedules starts and finishes at absolute monotonic offsets and records
scheduler wakeup lateness before lease renewal/RPC preparation; this is not the
complete RPC start delay. Acknowledgments never pace emission. The eligible cohort is
all declared terminal observations, with span identities recorded before the
first append. RPC receipts supply canonical event identities without repeatedly
scanning the source journal in the timing path. Exact source/Core reconciliation
is still required after the render cohort drains.

The browser reveals each expected node with the normal exact-ID filter. This
keeps off-page nodes in the denominator as the graph grows, and includes that
reveal cost in the upper bound. It measures terminal-step discovery/display, not
a guarantee that short-lived running states were painted. A missing correlation
invalidates the cohort. Nearest-rank p95 requires >=1,000 complete observations;
it does not by itself qualify the separate five-minute warmup, thirty-minute
baseline, stress, soak, retained volume or observer-overhead requirements.

The first complete 1,000-sample diagnostic emitted 2,000 tool observations in
239.889 seconds at a declared 8.333/s, without waiting for rendered acknowledgments.
All 2,002 source/Core events settled exactly. Nearest-rank p95 of conservative
display upper bounds was 1,706.434 ms; median 1,258.421 ms and maximum 2,120.200 ms
(three samples above 2,000 ms). This establishes the numerical target for that
terminal/reveal cohort only. Normal Core startup was restored and all eight
helper hashes matched the tested source.

Next implement the adopted five-minute warmup and thirty-minute baseline with
explicit source/run/operation distribution and bounded observer storage over the
whole schedule. Keep the 1,000-sample diagnostic separate: one growing synthetic
run on host loopback does not establish the full baseline population, long-lived
resource bounds, distributed topology or execution overhead.

## Full baseline distribution

The declared full profile has 350 distinct runs from ten synthetic agents on the
existing source, fifty observations per run, in 17,500 uniform 120 ms emission
slots. Agents receive one slot each every 1.2 seconds; a new run starts each
minute. The first fifty runs are warmup, and the next three hundred runs are the
thirty-minute measured window. Starts/finishes remain source scheduled, independent
of acknowledgments. This matches the exported metadata load model, not actual
task execution, adapter coverage or multi-host topology.

The predeclared visible cohort is every terminal observation for agents 0 and 1,
in two separate Chromium contexts. Both warmup and measured acknowledgments are
required (240 + 1,440). A run switch uses the ordinary saved run address and
canonical-step filter, so navigation/reveal cost remains inside the upper bound.
The source/agent allowlist prevents unrelated fleet events from filling observer
storage. Record capacities derive from the finite schedule: 17,500 commits and
1,680 render observations. Stress and soak need their own bounded storage design;
do not silently increase this run's duration or rates.

Before the full run, a two-minute preflight uses the same ten agents, slot order,
run rollover, filtering and cleanup with twenty runs and 1,000 exact events. Its
48 measured observations are insufficient for p95. Long-run evidence must come
from a verified live process through completion/restoration; a progress artifact
is only diagnostic.

## Full-profile failure follow-up

The first full jim-eq baseline on frozen harness `1cbdbbe` failed on a 20-second
terminal-node visibility wait after 17,122 events. All emitted source/Core tuples
match and are settled; normal Core startup and owned connector/session cleanup
are verified. Only 1,617 of the declared 1,680 acknowledgments were obtained,
so this run provides no complete-cohort latency acceptance. The affected node
renders in a fresh retained-run browser session; the live failure is unresolved.

Before another full run, capture bounded failure context in the browser harness:
current sample/lane/stage, session state, and request/socket failure counts,
without exporting credentials or raw payloads. Reproduce and correct the owning
layer on jim-eq. Keep all predeclared samples and the original failure evidence;
do not increase timeouts or trim the cohort to turn this run into a pass. Then
repeat the full profile and run the completion auditor, followed by the remaining
overhead, stress, soak and broader acceptance gates.

### Reproduced historical coverage read timeout

Bounded browser failure diagnostics are now implemented and exercised on jim-eq.
The updated two-minute preflight failed with both lanes reconnecting, HTTP 503s,
no page errors and no execution writes. Normal Core startup and cleanup were
verified after cancellation (560 settled events; ten revoked connectors and no
active owned sessions). A retained replay of the failed changes URL also returns
503 after 5.026 seconds, independently of the temporary observer.

Read-only profiling on the existing Core image places 4.798 seconds across 55
queries to historical `trace_projection_scope_progress` by exact source key.
This dominates the five-second read budget before the 64-commit page finishes.
The earlier exact-key query change does not bound work across many versions of
the same key. Next inspect the historical-view query plan and implement bounded
latest-version lookup while preserving the exact requested cursor, deletion and
generation semantics. Verify identical retained snapshots and replay before
redeploying, then rerun preflight/full baseline without widening timeouts.

### Point lookup fixed; repeat full qualification

Revision `28c03be` replaces historical source-progress version scanning with an
indexed latest-row seek inside the exact generation/cursor transaction. It
preserves tombstones and context cleanup. 246 focused backend checks pass; the
retained changes response matches exactly (5.911 seconds before / 0.218 after
in one read-only diagnostic). Deployed Core returns that request successfully.

The repeated two-minute preflight and independent completion auditor pass with
1,000 exact settled observations and all 96 ACKs; normal Core restoration and
zero backlog/lag are verified. The full 35-minute profile must now finish with
all predeclared observations/ACKs before claiming baseline latency acceptance.
Frozen harness `baseline-code-28c03be` and output `baseline-full-2` on jim-eq
identify this attempt; revalidate its process before acting. Observer overhead,
stress/soak, retained-volume and broader M4–M7 acceptance remain separate gates.
