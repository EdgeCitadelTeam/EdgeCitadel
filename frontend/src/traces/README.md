# Trace read client

This directory supplies the read-only execution explorer mounted in the dashboard
Execution tab, with entry points from Flow and Tasks. The canonical response and event schemas live
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

The map retains positions by canonical ID, renders at most 100 steps per page,
and exposes all loaded steps through owner grouping, search, page navigation and
an equivalent text view. Arrow direction defines causality; late parents can
appear above or beside already positioned steps. Map labels retain a readable
size on narrow screens with internal panning and details below. Status is text
as well as color. Permission decisions are not execution outcomes.

Selection uses `#execution?run=...&task=...&step=...&at=...&event=...`.
The event key is its exact source/epoch/event tuple. Invalid saved parameters
stop the run view instead of silently switching an invalid historical link to
live. The task filter finds runs containing a task, including child tasks.
Inspector requests bind `node_id` to the displayed graph's `at`; membership uses
the backend projector's identity rules and sparse continuation pages are followed
when restoring an exact event link. An event cursor cannot move between node
filters. Arbitrary references are displayed as text and never auto-opened.

React effect ownership recreates API/session resources under StrictMode replay.
The credential belongs to Layout so tab changes preserve it in memory; page
reload, disconnect and authorization denial remove access. A view change aborts
its inspector and clears page state. The graph remains frozen while historical
mode indicates newer evidence. Browse retained history discovers server snapshots, including versions never
visited in this browser. The paginated index freezes its newest boundary and
retention floor; new arrivals require Refresh history and floor changes return
an explicit expiry error. Relevant observation/coverage changes and the retained
base remain reachable, including past versions of a currently retired run.
Selecting a row opens its exact graph cursor and preserves step selection;
retired/absent boundaries are labeled with no available graph. Times use the
Core clock rather than asserting cross-host execution ordering. The index owns
only its current metadata page; navigating or refreshing it does not change the
graph until the user selects a snapshot.

Verification includes component/protocol fixtures and real jim-eq browser
checks; see the milestone evidence for exact revision and scope. Full multi-branch
S1/S4/S6, retained-volume/history read-cost qualification, large-run render/memory measurements and
full deterministic regression remain open. A 501-step component fixture proves
reachability, not production stress performance or a completed M6 gate.

Collection availability is independent of socket connectivity and execution
outcomes. Required `freshness.collector_state` arrives on graph/list reads and WS
heartbeats. The mounted map uses the session's latest observed freshness, shows a
stale-view warning for `unavailable`, and preserves unknown availability rather
than inferring health from matching ingest/projection positions. A connected
collector does not establish complete source coverage. Historical views display
collection status at their last check, not retrospective health.

The branch browser groups canonical steps by task identity, then by operation
kind/name and owner. Steps without a task remain a separate group. Counts and
per-state totals describe steps, never invented execution outcomes or causal
relationships. One task and one operation group expand at a time; each level
paginates at 50 entries. Collapsed groups do not mount their step buttons.
Selecting an exact step clears map filters and opens its map page, while the
existing inspector and reloadable selection continue to use its canonical ID.
Live state changes preserve expanded groups and selected step identity. The map
keeps its recorded relationships; the browser does not generate aggregate edges.

The observation inspector uses canonical `attributes.local_content_ref` to label
local-only records. It never links or fetches these references. Component coverage
checks inert HTML-shaped text; jim-eq coverage checks rejected source metadata,
inert allowed labels and client rejection of a malformed graph response. This is
bounded metadata qualification, not full security acceptance.

During live graph updates, map and text-view pages follow the currently focused
step if its sorted position crosses a page boundary. The page changes before DOM
commit so that the keyed step stays mounted and keyboard focus is retained.
Moving focus to a different control ends that behavior; ordinary pagination and
filter controls retain ownership of the requested page.
