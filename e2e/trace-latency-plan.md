# Controlled Core commit-to-display qualification

Status: measurement design and clock prerequisite verified; the latency harness
and acceptance run are not implemented. This does not close M6 or M7.

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
