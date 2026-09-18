# Trace read client

This directory supplies the read-only client for the planned execution map. It is
not yet mounted in the dashboard. The canonical response and event schemas live
in the repository's `schemas/` directory; Ajv validates those same files rather
than a separately maintained frontend schema. Dashboard Docker builds therefore
use the repository root as context (`frontend/Dockerfile`).

Create one `createTraceApi(credential)` per entered fleet read credential, then
one `createTraceSession(api)` per visible run. Both default to the current Core
origin. The API uses GET and an authenticated read WebSocket only. Credentials
stay in memory, travel in the HTTP Authorization header or first WS frame, and
never enter trace URLs, browser storage, cookies or response diagnostics.

React can use the session's `subscribe` and `getSnapshot` with
`useSyncExternalStore`. Call `open(traceId)` for live data or
`open(traceId, { at })` for a retained snapshot. `pause()` selects the currently
displayed graph's exact historical cursor; `resume()` requests a fresh snapshot.
Dispose the session when its owner unmounts and dispose the API when replacing
or clearing its credential. The UI must also clear its separately owned run
list/inspector data on credential denial or replacement. Do not mutate published
snapshots.

A session owns one abortable run/view and one ordered update loop. Switching
runs or history mode invalidates earlier work. Initial graphs and snapshot-mode
changes are assembled completely offscreen, including edge-only continuations,
before publication. Patch removals/upserts, coverage, inspector `at` and replay
cursor publish together. Clear-mode commits remove the complete graph. A later
run incarnation starts with an empty graph. Canonical node/edge IDs define
identity; array order can differ between incremental updates and full reads,
so the renderer must preserve keyed layout/selection rather than infer identity
or causality from array positions.

Only fully applied changes advance the reconnect cursor. HTTP catch-up may then
advance through its fully scanned prefix; a heartbeat never acknowledges a
change. Socket failure or backlog overflow closes the connection and resumes
through persisted HTTP catch-up before opening another socket. HTTP bodies are
limited to 2 MiB; WS messages to 2 MiB, queued frames to 16 and queued bytes to
4 MiB. Requests have a ten-second deadline and silent sockets a 25-second
watchdog. These bounds do not qualify the total memory or render cost of an
expanded run; retained-volume and large-run measurements remain required.

Historical mode polls only to indicate newer evidence and leaves graph and
inspector cursor unchanged. Inaccessible data is cleared; generation/history
errors require an explicit resnapshot action instead of silently jumping to
live. Coverage flags remain independent of node execution outcomes. Inspector
requests must use the displayed graph's `at`, replace their page cache when it
changes, and cancel stale requests. They must not reconstruct history by hiding
nodes based on timestamps.

Current verification covers canonical fixtures, response and queue bounds,
replacement races, reconnect ownership, generation/retention denial, frozen
history and stale-run cancellation. The bundled client has also exercised a
fresh Hermes task through the existing jim-eq Core. This is client protocol
verification, not browser UI, accessibility, layout or M6 acceptance. The next
step is the approved complete map, run navigation and inspector, followed by
browser scenarios and measurements on jim-eq.
