# Execution trace contract and experimental implementation

This document describes the trace contracts and the experimental source/Core
implementation developed against them. M4 end-to-end acceptance remains open;
source synchronization and Core collection are disabled by default. The contract
foundation can be reviewed before the later persistence and integration commits. The event, selected
export and settlement schemas live in `schemas/trace-*.v1.json`; their bounded
validator is `agent-runtime/src/edgecitadel_agentd/trace_contract.py`. Golden
records and expected hashes live in `agent-runtime/tests/fixtures/traces/`.

## Identity and serialization

Every event contains its version, immutable UUIDv4 event ID, node ID, UUIDv4
source epoch, positive source sequence, agent/run/task/attempt/span identities,
occurrence time, provenance, causal event references and family-specific
attributes. Inapplicable identity fields are explicit nulls. Trace IDs are 32
lowercase hexadecimal characters; other correlation IDs are UUIDv4. Conversation
context is distinct from execution identity. Native observational roots have a
trace and null task ID until actual task evidence exists.

The hash input is the entire event serialized as UTF-8 JSON with sorted object
keys, no insignificant whitespace, literal Unicode and no normalization. Arrays
retain their order. Numbers are integers within JavaScript's exact integer
range; floats, NaN, arbitrary Python objects and surrogate strings are rejected.
Timestamps use UTC `YYYY-MM-DDTHH:MM:SS.mmmZ` and must be valid calendar dates.
This is the explicitly restricted v1 serialization, not a claim of general
RFC 8785 support. `canonical_bytes` is the reference implementation.

The selected export envelope carries its own version, node/epoch, export
generation, export sequence and event SHA-256. Export generation and sequence
never enter the event hash. The same event can replay into a new generation
without changing identity. Event start and finish have distinct event IDs;
operation boundaries share a span ID. Identity conflict detection and durable
export-position ledgers belong to the later journal/collector implementation.

## Event families

| Kind | Evidence and allowed attributes |
|---|---|
| run | Explicit run outcome; bounded reason code |
| task | Original lifecycle state, sender/recipient/daemon role, reason code |
| dispatch | Dispatch identity, recipient, optional skill/grant version, reason |
| permission | Dispatch identity, policy/version, grant version, reason |
| model | Operation name, reported input/output tokens, unavailable-usage reason |
| tool | Operation name, reason, content availability and opaque local reference |
| coverage | Export generation/checkpoint, bounded loss ranges, unsupported families |
| source | Source/export-generation start, restore or selection-policy change |
| link | Explicit join between tasks, separate from ancestry |

The schemas define the exact phase and attribute sets. All unknown fields are
rejected. Raw prompts, arguments, outputs, arbitrary exception messages and
free-text summaries have no export field in this candidate. Operation names
must be bounded identifiers. Producers must still enforce approved source
names/redaction; a syntactically valid string is not evidence it is nonsensitive.
Model usage must be present as numbers or null with a reason; unknown usage
cannot silently become zero. Compatibility-synthesized events cannot assert
measured duration. The validator rejects self-parent references and simultaneous
parent-task/parent-run references; multi-record ancestry and authority checks
remain store/projection responsibilities.

## Bounds and rejection

Events are at most 16 KiB encoded UTF-8, with a maximum nesting depth of 16.
Export envelopes and settlement responses are at most 18 KiB. Each event has at
most 16 causal references; each range family has at most 128 entries. Sequence
positions and measured values are bounded integers. Errors expose stable codes
only, never caller payloads or schema-validation excerpts.

`validate_export_header` validates the known envelope/source position and raw
payload hash before payload semantics. It can succeed for an unsupported or
oversized event inside a bounded envelope. `validate_export` additionally
validates the event and origin match. A collector may durably reject a payload
whose envelope validates, but must not advance settlement for a malformed or
unsupported envelope. Neither validation call writes a receipt or authorizes
settlement by itself.

Principal error codes include `invalid_event`, `unsupported_event_version`,
`invalid_export_header`, `unsupported_export_header_version`, `oversize_record`,
`excessive_depth`, `invalid_json`, `invalid_integer`, `origin_mismatch`,
`hash_mismatch`, `self_parent`, `ambiguous_parent`, `synthetic_duration`,
`missing_loss_ranges`, `inconsistent_usage_coverage`, `invalid_settlement`,
`unsupported_settlement_version`, and `invalid_ranges`.

## Settlement

A checkpoint names source, export generation, collector epoch and contiguous
settled export position, plus rejected and lost ranges. Ranges are positive,
ordered, nonoverlapping, disjoint across rejection/loss and no higher than the
settled position. Position zero represents no settlement. Schema consistency
does not prove that Core committed every intervening record: M4 must establish
that fact transactionally before replying. Broker acknowledgement alone never
permits retirement of the only replayable source copy.

## Multi-level task correlation

`trace_correlation.TaskTraceContext` represents the entire task correlation
context. Persist it when accepting a task; do not rebuild depth from retained
parents or reset it in a managed handler. Child context preserves conversation
and trace, uses the executing task as parent and increments depth. Positive-depth
requests use the existing `delegation` envelope, while results and cancellation
preserve that same task context. Reserved fields are stamped after handler output
so output cannot replace ancestry. The helper does not authorize delegation:
agentd must validate the binding, session, parent ownership and recipient grant
before calling it.

`payload.execution_context` follows `schemas/execution-context.v1.json`. It is
optional versioned payload metadata, never a new top-level v1 envelope field.
A native observational-root link uses `parent_run_id` with no forged parent task
and remains a root wire command at depth zero. Descendants thereafter use task
parents. A legacy missing context defaults to task ID with explicit
`context_origin=legacy_default`; missing trace evidence stays unknown. The helper
must not manufacture a cross-task run just to make a graph connected. Existing
receivers accept the extra payload and still execute the task; that does not
certify their forwarding or telemetry coverage.

`delegation.v1.json` contains two same-conversation roots and their expected
three-level parent edges/depths. `lifecycle.v1.json` freezes the existing legal
state table, display mapping, operation and export transitions. Tests compare
its task transitions against the current execution owner. Export transitions
are scoped to a collector epoch: a restored Core invalidates prior settlement;
retained settled data may need replay, and expired data needs loss/unknown
coverage. Disabling telemetry never changes existing task-state ownership.

## Read interfaces and cursors

`schemas/trace-read.v1.json` defines list, graph, event-page, change-page,
WebSocket change/heartbeat and error response bodies. `read.v1.json` supplies
input observations, expected graph and examples of every response. Nodes retain
original task state and provenance, conflict/count indicators, and separate
coverage. A join can point back toward a root without creating ancestry.
Resolved parent cycles are invalid; attributed invalid edges may remain visible
but are excluded from ancestry. Completed children alone leave the root running.
These fixtures specify expected behavior; a production reducer is still M5 work.

| Interface | Request and response contract |
|---|---|
| `GET /api/traces` | Optional `cursor`, `agent_id`, `outcome`, `limit` (default/max 100); `trace_list`. Order by immutable creation projection position descending, then trace ID; freeze membership/filter evaluation at snapshot. |
| `GET /api/traces/{trace_id}` | Optional `at` graph cursor or `expand` expansion cursor; `trace_graph`. Expansion must use the token's historical snapshot and generation. |
| `GET /api/traces/{trace_id}/events` | `as_of` graph cursor, optional `after` event cursor and `limit` (default 200/max 500); `trace_events`. Page only observations through that graph snapshot's ingestion watermark. |
| `GET /api/traces/{trace_id}/changes` | `after` change cursor and optional `limit` (default 200/max 500); `trace_changes`. Return retained atomic upserts/tombstones and coverage. |
| `/ws/traces/{trace_id}` | `after` change cursor; the same durable changes as HTTP catch-up, plus heartbeat/error. Heartbeats advertise availability and do not acknowledge unapplied changes. |
| NATS settlement | Source/epoch/export-generation request and persisted checkpoint response; exact request/error schema remains an open M2 deliverable. |

Graph responses contain both `at` (graph snapshot cursor) and `resume_cursor`
(change cursor through the same committed projection position). They are not
interchangeable. After a snapshot, resume HTTP/WS with `resume_cursor`. Event and
expansion tokens remain tied to that snapshot. A `next_cursor` signals additional
pages; no interface is permitted to silently drop unreachable operations.

All encoded read responses are bounded at 2 MiB. Graphs expose at most 500 nodes
and 1,000 edges; omitted nodes require expansion tokens. The 500-event page limit
is an upper count bound, not permission to exceed the byte cap: M5 must return a
smaller page plus a continuation token when necessary. This extends the original
graph byte budget to every read response for bounded memory; it does not remove
access to events. Unknown totals/coverage must stay explicit.

`trace_cursor.py` implements the token contract: canonical claims, URL-safe
unpadded base64, HMAC-SHA256 with a domain prefix, a persistent Core signing key of
at least 32 bytes, and a maximum token length of 4,096 characters. A signature
provides integrity, not API authentication. Every token binds kind, trace,
normalized filter/access-policy hash and projection generation. The server
selects the access policy; client filters cannot grant access. Normalize omitted
filters to explicit null values before hashing. Key and policy version rotation
invalidate old tokens; projection rebuild changes generation.

| Cursor kind | Position meaning |
|---|---|
| list | `snapshot=upper` is projection snapshot; `position` plus trace `key` is the last emitted creation-order tuple. |
| graph | `position=snapshot` is the retained graph projection position; `upper` is its covered ingestion watermark. |
| events | `snapshot` is graph projection position; `position` is last ingestion row, bounded by ingestion `upper`. |
| changes | `position` is last delivered/scanned projection position; `snapshot=upper` is the known high watermark. New requests can catch up beyond the old high watermark. |
| expansion | `snapshot` is historical graph position; `position` is branch offset; `key` identifies branch and `upper` is covered ingestion watermark. |

A token from another kind/trace/filter/access scope returns HTTP 400
`cursor_scope_mismatch`; bad signature/encoding returns 400 `invalid_cursor`.
A generation change returns 409 `generation_changed`; history no longer
reconstructable returns 410 `history_expired`; both require a new snapshot.
Missing run returns 404 `not_found`; invalid query returns 400 `invalid_request`;
temporary service failure returns 503 `unavailable`. Error bodies contain no raw
request or exception text. The graph/changes store must supply the true retained
boundary to the codec; signing a cursor cannot prove the history still exists.

## Event ownership

| Family | Authoritative emitter and journal | Retry identity and consumer obligation |
|---|---|---|
| task | Existing agentd task transition, local journal in same transaction | Daemon event identity; preserve sender/recipient/daemon role and all conflicting terminal candidates. |
| dispatch/permission | Agentd evaluates authenticated binding and installed package grant before task creation | Stable dispatch/request identity; deny can exist without a child task, with historical policy/grant snapshot. |
| run | Root owner through its authorized observational binding | Stable observation request maps to daemon event ID; no implicit outcome from child completion. |
| model/tool | Selected integration's actual callback through authorized binding | Stable observation ID and span ID; retries keep event identity, changed-content retries fail. |
| link | Root/join owner through authorized binding | Explicit typed relation; never reinterpret as nesting. |
| source/coverage | Agentd recorder/exporter | Stable generation/checkpoint or loss-marker identity; ranges reflect retained evidence, not global completeness. |

Agentd assigns origin and sequence. Adapters cannot choose another actor, task,
trace or source position in append requests. Authentication failures are bounded
daemon diagnostics without a claimed actor/run. Imported history needs a
dedicated import authority and provenance rather than a live session claim.
Exporter retries committed selected records; Core commits acceptance/rejection
before settlement; projector is sole owner of derived graph/change positions.
Current registry state is not a historical grant snapshot. Outbound grant
revocation stops new dispatch but does not cancel already accepted work; session
or connector revocation blocks further live observation appends.

## Contract freeze and implementation boundary

M2 candidate interfaces are frozen for M3 implementation after the recorded gate
audit and native-session/remote-wire spikes. Freeze establishes tested contracts
and feasible integration seams; it does not assert durable recording, deployed
collectors/reducers, native application deep hooks or production rollout. Every
M3–M7 gate remains mandatory. Changes to these contracts require updating fixtures
and reopening affected producer, transport, projection and UI checks.

## Settlement control request and reply

The candidate NATS control subject is `edgecitadel.telemetry.settlement.v1`;
export records use `edgecitadel.telemetry.v1.<node_id>`. These are separate from
command delivery. Requests obey `trace-settlement-request.v1.json` (1 KiB maximum)
and identify one `(node_id, source_epoch, export_generation)` plus a fresh UUID
`request_id`. Replies obey `trace-settlement-reply.v1.json` (18 KiB maximum).
The reply echoes the request ID; successful replies contain the existing complete
settlement checkpoint. The validator requires its source tuple to match the
request and rejects overlapping rejected/lost ranges. Request IDs correlate
exchanges; they are not authentication credentials. Configured trusted-fleet
Core transport supplies the trust boundary.

Poll every 30 seconds, with at most one outstanding request per source/export
tuple and a 5-second request timeout. After timeout, unavailable reply or transport
failure, retry with exponential delay starting at 500 ms and capped at 30 seconds;
reset after a valid successful exchange. Honor `retry_after_ms` as a minimum delay
within the same 500–30,000 ms bound. Coalesce concurrent poll triggers. These rates
are implementation requirements for M4, not behavior provided by the codec.

Errors carry only a fixed code and bounded retry delay, never a checkpoint or
caller text. `unknown_source` means no durable ledger exists for that tuple:
retain/replay records and retry, never substitute successful position zero.
`temporarily_unavailable` and `rate_limited` retain records and back off.
`unsupported_version` and `invalid_request` retain records and surface a
compatibility fault; do not hot-loop the unchanged invalid request. Malformed or
uncorrelated replies and timeout are unavailable evidence, never settlement.
An undecodable request or one without a valid request ID is dropped because it
cannot receive a correlated error. Other errors may echo only a validated ID.

A successful zero checkpoint is valid for a known tuple. Settlement is scoped to
`collector_epoch`: a changed epoch invalidates prior retirement assumptions and
requires reconciliation of retained records and explicit loss/unknown evidence
for unavailable records. Within the same epoch, a regressing checkpoint cannot
undo settled state and must surface a collector consistency fault. The exporter
must compare epoch/position and commit checkpoint application atomically before
retiring replayable records; this validator supplies no durable state or deletion
authority. Broker ACKs never authorize retirement.

Range arrays are complete, ordered and bounded to 128 per class; truncation is
forbidden. If a checkpoint cannot fit the count/byte bounds, the collector must
return `temporarily_unavailable` and retain the unreported receipt evidence.
Operational handling of sustained fragmentation remains an M4 capacity gate; a
compact codec alone does not establish recovery progress under that condition.

## Corrections and anonymous rejection evidence

Every event now includes nullable `supersedes_event_id`. A correction has a new
immutable event ID and source position; it may reference the previous ID in its
own `(node_id, source_epoch)` namespace. Self-supersession is invalid. It never
mutates the original receipt or reverses its durable rejection. The recorder
must enforce namespace ownership and reject a reference to a known foreign
record; a reference alone does not establish authority or erase prior evidence.

The `security/authentication_rejected` family represents daemon-observed anonymous
RPC authentication failure. It requires null agent, trace, task, conversation,
parent, attempt and span identity, null duration/supersession and empty causes.
Its only attributes are fixed `reason=authentication_failed` and a positive count.
No caller-provided IDs, credentials, exception text, transport address or raw
request content may be exported. It is source evidence visible to authorized
fleet diagnostics, not a fabricated run. Authenticated dispatch denial continues
to use the scoped permission/dispatch families.

M3 must coalesce anonymous failures into at most one event per source per minute
with a bounded counter, using reserved metadata capacity. Loss of that counter
on crash cannot support an exact lifetime rejection total. Only daemon code may
produce this family; connector append requests must reject it even though the
serialized event validates. This schema does not install the recorder, rate
limiter or authentication enforcement.

## Binding and adapter append authority

The candidate `trace.bind` params use `trace-binding-request.v1.json`: version,
request_id, session_id, nullable task_id and nullable conversation context_id.
Authentication stays in the existing connector RPC envelope. The daemon resolves
the connector itself, verifies its telemetry capability and unexpired open
session, and requires the session to belong to that connector. A supplied task
must be accepted/running, name the authenticated agent as recipient and have
that exact claimed_session_id. Merely being its sender/participant is insufficient.
For task-bound requests the stored task context is authoritative; a conflicting
supplied context fails. Null task requests create observational roots, never
executable tasks; conversation context groups runs but does not identify a run.
The daemon issues binding, trace and fresh execution-attempt identity. Repeated
(connector_id, request_id) with identical params returns the same binding;
changed params fail `idempotency_conflict`. Retry cannot reopen revoked bindings.

The candidate `trace.append` params use `trace-append-request.v1.json`: version,
binding_id, observation_id and bounded model/tool observation. Observations
contain operation span identity, parent span, phase, time, duration and allowlisted
attributes. The connector cannot supply actor/task/trace/source identity or
provenance. Parent spans must belong to this binding; client span IDs must not
collide with a different binding. The daemon stamps authenticated identity,
immutable binding correlation, source position and integration_reported evidence,
then validates the complete 16 KiB event before committing. The 16 KiB append
request limit is not a promise that every request fits after daemon stamping.

The recorder owns run start/finish, task, dispatch, permission, link, security,
coverage and source events; adapters cannot submit those through this operation.
Native application run hooks still require qualification; a protocol session
alone does not prove model/tool hook coverage or successful run completion.
Binding closure emits only known outcome evidence, otherwise interrupted/unknown.
Exact finish/import RPC reply shapes remain pending M2 work.

Append idempotency key is (connector_id, binding_id, observation_id), with a
canonical hash of observation params. Identical retries return the original event
identity without consuming another source position; changed content fails
`idempotency_conflict`. Authorization is rechecked on retry, so revoked sessions
cannot use the idempotency table to retrieve history. Durable append must commit
identity mapping, local event and selected spool entry atomically before replying.
A storage failure is retryable with the same observation ID; invalid metadata,
identity mismatch, closed binding and permission failures are permanent for that
request. The policy helper does not implement this transaction.

`trace_authority.py` expresses decisions over trusted store snapshots. The store
must read those snapshots and perform the write in the same locked transaction;
constructing a dataclass from request input is forbidden. Closed/revoked/expired
sessions prevent every new binding write. Revoked outbound delegation only stops
new dispatch: an accepted task can still submit authorized completion evidence,
even after the task terminal transition commits. Terminal tasks cannot dispatch
or acquire a new binding. A closed binding allows no new observations. Historical
reads follow their separate access policy and are not reauthorized as new writes.

Historical import instead requires authenticated administrator authority plus an
enabled import source explicitly scoped to allowed agents. It never reuses a live
binding, claims a live session, issues a dispatch grant or creates executable work.
Imported evidence is historical_import; missing attempts/timing/parentage remain
unknown, and claimed IDs cannot confer task ownership. Source record identity and
import dedupe/persistence remain explicit requirements for the import RPC slice.

## Optional capability negotiation and compatibility

Advertise `https://edgecitadel.dev/extensions/execution-trace` in Agent Card
`capabilities.extensions`, using `trace-capability.v1.json`. `required` is false
(or omitted): telemetry cannot become a prerequisite for command/result delivery.
The advertisement is bounded to 4 KiB; the parser inspects at most 64 extensions.
Its params contain up to eight distinct positive `schema_versions` and a per-family
mode map. Current supported version is 1. Unrecognized advertised versions do not
prevent selecting a common version. With no common version, telemetry remains
unnegotiated and compatibility coverage is unknown.

Modes are `live`, `historical`, and `unsupported`. Omitted families in a valid,
negotiated advertisement are unsupported. A missing, malformed, duplicated, or
incompatible advertisement instead means unknown coverage, not proof that all
families are unsupported. Historical mode is never converted into live support.
Advertisements state producer ability, not completeness of any particular run;
actual source/coverage reconciliation remains necessary. They confer no append,
import, delegation, or read authority. Multiple components' families must not be
unioned into a run's coverage without evidence identifying which component
observed that run.

Example optional extension:

```json
{"uri":"https://edgecitadel.dev/extensions/execution-trace","required":false,"params":{"schema_versions":[1],"families":{"task":"live","tool":"historical","model":"unsupported"}}}
```

| Producer/consumer combination | Task execution | Telemetry interpretation |
|---|---|---|
| Old sender, new receiver | Existing envelope accepted | Missing trace remains unknown; legacy context fallback is labeled |
| New sender, old receiver | Optional payload metadata accepted by old validator | Old receiver provides no advertised live coverage |
| New card, old card reader | Optional extension accepted by existing card validator | Reader need not understand extension |
| Old card, new reader | Existing card remains usable | `not_advertised`, no invented live/unsupported declaration |
| Both support schema 1 | Existing task routing unchanged | Negotiate declared family modes only |
| Disjoint versions | Existing task routing unchanged | `unsupported_version`; known wrappers may still quarantine individual unsupported payloads |
| Historical-only family | No executable work created by import | Separate administrator import authority; historical provenance, no live timing claim |
| Duplicate/malformed extension | No task-delivery decision in telemetry parser | `invalid_advertisement`, no trusted family claim |

Tests execute existing card validation and the negotiation parser. Earlier
multi-level correlation tests execute the old envelope validator and receiver
with new optional payload metadata. Production card emitters are intentionally
not advertising hooks before their M3/M7 implementation and qualification.

## RPC replies, closure and historical record identity

`trace-rpc-reply.v1.json` defines the local versioned result payload inside the
existing RPC transport response (2 KiB maximum). Every reply identifies operation
and request_id. For append, request_id echoes observation_id; bind, finish and
import echo request_id. Clients reject operation/ID mismatches. Bind success
returns binding_id, trace_id, nullable task/context IDs and execution_attempt_id.
Append/finish/import success returns committed event_id, source_epoch and
source_seq; it never means broker ACK, Core settlement, or projection completion.

Error codes are fixed: invalid_metadata, unsupported_version, not_authorized,
session_unavailable, binding_closed, identity_mismatch, idempotency_conflict,
storage_unavailable and quota_exceeded. Only storage_unavailable is retryable
with the identical request identity; quota_exceeded requires a policy/capacity
change, not an automatic tight retry loop. Errors carry neither result nor caller
text. Authentication failure remains in the existing authentication response and
cannot expose binding state. Request parsing with no valid correlation ID cannot
produce a correlated trace reply. Internal detailed policy codes map to these
public categories without disclosing foreign task/binding existence.

`trace.finish` uses trace-finish-request.v1.json: version, request_id, binding_id,
outcome and a bounded reason. A native observational root may explicitly report
completed/failed/canceled or unknown; this remains adapter-reported evidence.
For a task binding, the store's terminal task state is authoritative: a caller
outcome that contradicts it fails identity_mismatch; a still-running task cannot
be declared completed by closing telemetry. Unknown closure is permitted without
claiming task outcome. Daemon crash/lease reconciliation may emit interrupted or
unknown, never inferred successful finish. Run-event phases now represent both.
Closure and its event commit atomically. An identical finish retry returns its
receipt only after authentication/session ownership revalidation; it cannot write
to the closed binding. This receipt lookup is distinct from authorization to
append new observations. Other requests against a closed binding fail.

`trace.import` uses trace-import-request.v1.json (16 KiB maximum), under existing
administrator authentication plus the separate source/agent import grant. It
carries request_id, record_id, historical_run_id, import_source_id, agent_id and a
bounded run/task/model/tool observation. IDs in that observation refer only to
archive identities; they are never looked up as executable live tasks. Import
cannot provide event ID, source epoch/position, trace ID or evidence provenance.
The daemon stamps a new local event, historical_import provenance and journal
position after mapping archival identities. It performs full event validation
after stamping; an input that cannot fit the final event is rejected. Import
creates journal evidence only, never a tasks row, outbox command or live binding.

Each import grant owns a persisted namespace_id, selected by the daemon, preserved
with journal backup and inaccessible as a caller-selected grant override.
`trace_import.historical_identity` freezes the deterministic mapping: domain
prefix `edgecitadel-historical-identity-v1` followed by NUL, then canonical JSON
array [namespace_id, import_source_id, agent_id, historical_run_id, kind,
original_id], SHA-256, first 16 bytes with v4/variant bits set. The UUID-shaped
result is a deterministic evidence ID, not a random credential. For the run's
trace, use kind=run and original_id=historical_run_id and remove hyphens. Task,
context, attempt, and span IDs use their respective kind; parent references use
the same kind mapping as their targets. Null stays null. Cross-run archive links
without an explicitly resolved target import scope remain unresolved; matching
an ID to a live task is forbidden. The domain and tuple prevent separate grants,
archives, agents, runs and identity kinds from accidentally sharing identity.

Record dedupe key is (namespace_id, import_source_id, agent_id,
historical_run_id, record_id). The full validated request content excluding only
request_id is hashed: identical content returns the original receipt even under a
new request_id; changed content fails idempotency_conflict. The import namespace,
record mapping, hash and local event commit in one store transaction. A revoked
grant rejects retries before receipt lookup. No import algorithm can infer missing
ancestry, attempt IDs, timings, or live completeness from absent archive evidence.
These contracts replace the earlier pending finish/import-shape notes; durable
transactions remain M3 implementation and recovery gates.

## Recovery acceptance corpus

`agent-runtime/tests/fixtures/traces/recovery.v1.json` supplies concrete source
records, ordered actions and expected settlements, graph tasks/ancestry edges,
original task states and independent coverage dimensions for 17 cases. The
reference model in test_trace_recovery_contract.py executes these logical rules:

- Unsettled hole; out-of-order hole fill; durable payload rejection; explicit loss.
- Broker ACK without settlement; broker expiry followed by retained local replay.
- Identical replay; changed-content identity conflict; replay into a new generation.
- Collector restore with retained replay or expired local copies; lost loss evidence.
- Projection lag; late unresolved parent then resolved parent; local-only records.
- Source restore with a fresh epoch for new records and original identity on replay.

Each fixture generation label is shorthand for one source/export-generation tuple;
positions from different tuples never fill each other's holes. Expected unique
counts refer to accepted input observations; durable_loss is an abstract committed
control action. M4 must implement that action by committing the actual loss marker
and its receipt ranges together, not by accepting an unauthenticated range hint.
The small reference model has no SQLite, broker, quota policy or fault injection:
passing it proves consistent examples, not crash safety or operational recovery.
M4 must execute this corpus against real components and add all retention/capacity
and process-failure gates. Source writer fencing and complete marker persistence
remain explicit runtime proof obligations.

The corpus separately checks collector-epoch change on raw-journal restore versus
unchanged collector epoch on projection-only rebuild. Derived projection generation
still changes on rebuild and needs the M5 cursor invalidation tests. Restored raw
history is not assumed complete merely because all currently retained records were
replayed. Known-source reconciliation is scoped; task success never implies
complete telemetry. Coverage lists now allow all ten declared event families,
including security, fixing the earlier nine-entry bound.

## Migration and rollback matrix

The current production local schema is v6. New trace event/schema versions are
independent of SQLite user_version. M2 adds contract modules, not an enabled DB
migration. The upgrade fixture builds owned completed work and a pending command,
then produces the actual v5 table shape by removing context_id. It is a structural
fixture made by the current binary, not a claim to have launched a historical
released v5 executable.

| Binary / stored state | Required behavior | Current evidence |
|---|---|---|
| Current runtime / fresh DB | Create v6 with private key and WAL | Existing store tests |
| Current runtime / populated v5 | Atomic v6 upgrade; preserve all prior columns, ciphertext, events, attempts and pending command | test_trace_migration_contract.py compares all tables and decoded tasks/results |
| Migration fails after SQL changes | Roll back schema and user_version together; retry can recover | Injected post-statement failure leaves exact v5 data and no context_id column |
| Current runtime / higher user_version | Refuse, never lower version to bypass fence | Simulated v7 fence; existing v6 binary raises newer-schema error |
| Restored matched DB/key | Decode exact saved results and preserve pending transport intent | SQLite backup plus matching key restored into fresh owned directory |
| Missing key | Refuse opening encrypted schema | D7 probe and existing store guard |
| Valid but wrong key | Never treat undecipherable content as empty/valid | Test opens owned mismatched pair, task read fails explicitly |
| Future M3 migration / populated v6 | Add tracing tables/columns atomically; mandatory event + task + spool writes share a transaction | Required M3 gate, not implemented or passed |
| Older binary / future tracing DB | Refuse unless explicitly proven compatible; disabling telemetry is insufficient | Existing version fence establishes default refusal policy |
| Downgrade after future writes | Fence all writers; preserve upgraded journal; restore matched pre-upgrade backup into fresh directory; reconcile commands and declare lost observation interval | Strategy only; actual future schema rollout/rollback remains M3/M7 proof |
| Core raw DB restore | New collector epoch, retained replay, explicit unretained loss/unknown | M2 reference corpus; durable M4 test pending |
| Projection-only rebuild | Same collector epoch, new projection generation | M2 contract; M5 cursor/rebuild proof pending |

Pre-upgrade backup must use SQLite backup/snapshot semantics that incorporate WAL,
with the matching payload.key and any persisted tracing identity/namespace state.
Protect backup files as private application data. A backup is not a writer fence:
stop agentd/adapters/exporter before switching a service directory. Do not resume
restored accepted/running tasks until their external effects are reconciled; a
restored command may already have executed remotely. The owned fixture has one
completed task and one never-dispatched pending command, so it does not establish
safe replay of arbitrary live work. Stable remote task dedupe remains necessary.

Keep the upgraded database for forward recovery. Never overwrite it with a
rollback snapshot or force PRAGMA user_version backward. Restoring a source from
backup allocates a fresh source epoch for new observations after fencing the old
writer; retained records keep their original IDs. A restore can recover only its
retained interval. All of these requirements must be rerun against the actual M3
migration; this matrix closes the rollback design choice, not implementation
qualification or permission to restore any user's active service.

## Bound dispatch and native session lifetime

`trace.dispatch` uses trace-dispatch-request.v1.json: version, request_id,
binding_id, recipient_id, request text, nullable skill_id and deadline_at_ms. Its
68 KiB UTF-8 bound accommodates the existing 16,384-character request limit.
Raw request content stays in the existing encrypted task path; it must never be
copied into exported dispatch/permission attributes. The daemon derives sender,
trace, context and parent identity solely from the binding. Success in the shared
RPC reply contains the queued task identity and exactly one typed parent: task or
observational run. Dispatch success is queued work, never recipient acceptance or
execution completion. Request retry must not enqueue a second child; changed
content under the same binding/request key fails idempotency_conflict. Grant and
binding authorization, child/task outbox creation and mandatory dispatch evidence
must share the store transaction in M3.

The native M2 spike uses real NativeMcpServer-issued sessions and an owned agentd
Unix socket. Two sessions on the same connector retain separate server-held roots
and attempts even with the same conversation. Root issuance creates no tasks;
MCP delegation callbacks create only actual children with typed parent_run links.
A model argument cannot override actor/trace/parent. Closing or replacing a session
invalidates its old binding. This is a candidate protocol seam; production must
move the prototype's separate authority read and dispatch into one transaction.

The observed native root is an MCP-session observation boundary. It does not
assert that one MCP process equals one model turn, or that all application model
and tool operations are observed. Only Edge MCP activity is currently proven at
that boundary. Native deep hooks remain unavailable/unqualified until implemented
and exercised in the selected application; M3 must advertise the actual partial
coverage and M7 must exercise a user-owned native session. This keeps the required
native observational root and complete-map design while explicitly preserving its
unknown interior. Session loss cannot imply a successful run outcome.

Execution attempts identify execution starts, not RPC calls. For an already-bound
(task_id, claimed_session_id) execution, a second bind request must return the
existing active execution identity rather than mint another attempt. It may add
an idempotency mapping to that binding. Only a newly authorized execution start
can allocate another attempt; a lease reopen alone does not reauthorize the old
claim. Native roots allocate an attempt for the observed session root, and never
claim it covers unobserved model turns. Task_attempts transition-row IDs remain
unrelated to execution_attempt_id.

## M3 implementation: journal foundation

The source runtime now migrates local SQLite v6→v7 in the existing migration
transaction. It adds trace_sources, trace_export_generations, trace_journal and
trace_spool with source/export uniqueness and foreign-key protection for pending
payload references. Existing command outbox and legacy telemetry tables remain
independent. The earlier migration matrix describes the v6 baseline; this additive
v7 migration now has populated upgrade and post-statement rollback tests.

TraceJournal uses the task store's connection and requires an active caller-owned
transaction. It never commits. It stamps local source identity/position, validates
bounded canonical events, hashes immutable content, and writes selected export
intent with its separate sequence. Identical event retries reuse identity and
position; changed content raises idempotency_conflict. Local-only records consume
no export position. Source/export identities and counters survive reopening.
An injected spool failure rolls back an accompanying task update, journal, source
creation and both counters; pending spool references prevent unmarked journal
deletion.

This foundation does not yet route production task transitions or adapter RPCs
through the journal. Binding/attempt persistence, scoped operations, mandatory
producer integration, quotas/retention, source restore fencing and exporter
handoff remain M3 work. The transaction test is an in-process injected SQLite
failure, not the required process-kill/restart or full runtime overhead proof.
Opening this source runtime upgrades the database to v7; an older v6 binary must
use the fenced matched-backup rollback strategy, not an in-place version override.

## M3 implementation: durable binding identity

The source runtime now includes v7→v8, adding trace_bindings and trace_requests.
AgentdStore.bind_trace owns BEGIN IMMEDIATE under the store lock. It validates
params, authenticates the connector, reads current session/task authority and
checks lease time after acquiring the transaction. Task ownership is checked
before comparing context so foreign context is not exposed. Node identity is an
internal daemon argument, not a request field.

Binding, immutable run-start event, selected spool intent and hashed retry receipt
commit together. Native roots create no executable task. Same request/content
returns the persisted receipt after reauthorization; changed content fails.
Separate bind request IDs for the same claimed task/session reuse the existing
binding/attempt. A terminal task can retrieve a prior authorized bind receipt,
but cannot mint a new binding. Closed/expired/revoked sessions, revoked connectors
and removed trace capability prevent retries. The request table stores a hash
and bounded reply, not credentials or raw prompts.

Tests cover concurrent duplicate calls, independent connection reopen, task claim
ownership, capability/session revocation, and a spool failure rolling back every
new binding/journal/spool/request row. The populated v7→v8 migration preserves
existing journal/spool data and rolls back on injected failure. RPC routing,
scoped append/finish, persistent span ownership, task-boundary instrumentation,
actual adapter integration, process-kill recovery and quota/retention remain
unimplemented M3 gates. No production service was restarted for these tests.

## M3 implementation: scoped operation append and private RPC

Schema v9 adds trace_operations for binding-owned model/tool span identity and
separate start/terminal event references. AgentdStore.append_trace authenticates
and rechecks the binding's session, capability and claimed execution in a locked
transaction before reading retry receipts. It stamps actor, trace, task, attempt
and original parent correlation; caller metadata cannot supply those fields.
A parent span must already belong to the same binding. Kind/name/parent remain
immutable; a new observation ID cannot create a second start or terminal boundary.
A terminal observation without a start remains missing-start evidence. A later
start may fill that gap without reopening the terminal operation.

Journal event, selected spool, operation state and retry receipt commit together.
A failure at receipt insertion rolls all of them back. Identical retry returns the
original event receipt across connection reopen. Appends retain integration_reported
provenance; declared duration is not independently verified daemon timing. Active
binding run-start evidence is currently used to recover immutable parent fields;
retention must preserve it until the binding no longer accepts observations.

Private service dispatch now exposes trace.bind/trace.append to managed connectors
and native connectors with edgecitadel_trace capability. Node identity comes from
the enrolled node.json agent_id, never request params. Missing enrollment identity
fails explicitly. Contract failures map to bounded versioned replies; SQLite
failures return storage_unavailable without database details. Invalid correlation
IDs and authentication failures remain in the existing RPC error envelope.

Real Unix-socket tests exercise bind/append and repeated receipts without creating
executable work. Other tests cover cross-binding spans/parents, secret-bearing
metadata rejection, session closure, late starts, full transaction rollback and
v8→v9 migration rollback. Task lifecycle emitters, scoped finish/dispatch/import,
actual adapter producers, retention/quota and process-kill recovery remain M3 work.

## M3 implementation: atomic closure

AgentdStore.finish_trace and private trace.finish now close a binding in the same
transaction as its terminal run observation, selected exports and retry receipt.
Append and finish share a store-owned authorization routine. A finish retry may
read the matching closed binding's receipt only after connector, capability,
session and task-claim reauthorization; it cannot append or reopen the binding.
Changed content under the same request ID is rejected. A new finish request on a
closed binding fails binding_closed.

A task-bound closure cannot claim completed/failed/canceled unless that outcome
matches the authoritative task state. Unknown closure makes no task-outcome claim;
no closure changes the tasks row. Native root outcome remains integration_reported.
Claimed tasks ending rejected may still record authorized unknown closure or
completion evidence; this corrects an omitted terminal state in the initial
binding policy without enabling further dispatch.

Open model/tool observations receive daemon-observed interrupted boundaries with
null duration and explicit unavailable usage. This means terminal observation is
missing at binding closure, not that the external tool was killed or should run
again. Already-terminal operations are not rewritten. These boundaries, span
state, binding closure and receipt roll back together if persistence fails.
Automatic session-expiry/crash reconciliation is still pending M3 work; explicit
closure does not qualify process-kill recovery or adapter shutdown behavior.

### M3 bound dispatch and durable task context

Private trace.dispatch now reauthorizes the active binding and evaluates the
current native delegation capability or installed managed-package recipient
grant while holding the store transaction. Permission and dispatch decisions,
child creation, command outbox, task context, queued observation and request
receipt commit together. Denied requests persist bounded decision evidence but
create no child. Raw request bodies stay in the encrypted task/outbox content.

A matching retry returns the original receipt after active binding/session
reauthorization; it does not reevaluate a changed outbound grant or create work.
New request IDs evaluate the current grant. Decision evidence retains a hash of
the grant snapshot. Closed bindings or terminal parent tasks cannot dispatch,
including receipt retrieval through this operation.

Schema v10 adds trace_task_contexts. Received commands retain their entire
correlation capsule, and outgoing command/result/cancel envelopes use that stored
capsule. A bound child increments the known parent hop count. Native roots use a
run parent and a stable binding-derived fallback context when none was supplied.
Legacy roots can establish a local trace; legacy delegated tasks without a saved
capsule fail rather than inventing depth. Duplicate received commands compare
saved correlation as well as task content.

This does not yet instrument all task lifecycle paths. Actual adapter producers, automatic closure,
retention/quota, scoped inspection and crash/performance qualification remain M3
work. Native app model hooks remain unqualified.

### Managed handler correlation and atomic claim

Authorized task claims include the saved trace_context capsule when one exists.
The managed runtime applies that capsule to its handler envelope, retaining the
delegation type, trace, parent, context origin and hop count. Tasks without a
stored capsule retain the legacy runtime path. No new caller-supplied authority
is introduced: only the store supplies this field after the recipient claim.

Task creation, transition and claim share an explicit transaction scope; nested
transitions join their caller's transaction. A queued task's offered and accepted
changes commit together. Injected acceptance persistence failure rolls back the
offer, claim ownership, attempts and events instead of leaving a partial claim.
This fixes an existing nested-commit boundary before mandatory lifecycle
instrumentation; it is not yet proof of that instrumentation or crash recovery.

### Mandatory task lifecycle journal

The store's existing task event boundary now writes a sanitized task observation
and selected export intent in the same transaction. This covers creation, normal
transitions, deadline expiry, and accepted/running session recovery. Requeue is
represented as queued with a bounded session-closed reason. Existing event IDs
identify the journal boundary too. Same-state retries do not emit another event.
Bound dispatch uses this shared creation path rather than emitting a second queued
observation. Spool failure aborts the corresponding task state and legacy event.

The source is the enrolled agent_id in node.json; bound dispatch can supply its
already-authorized node identity, which must match configured identity when
present. Standalone stores without node.json do not invent a host identity or
claim lifecycle coverage. Malformed enrolled identity fails lifecycle persistence.
This compatibility case needs explicit availability reporting in local inspection;
it is not evidence of complete history. Restore/writer fencing remains pending.

Payload/result bodies and free-form reasons never enter lifecycle metadata.
Reasons map to fixed categories, with unknown as the fallback. Saved correlation
and the binding for the current claimed session supply parent/attempt identity;
pre-binding boundaries honestly have no execution attempt. Sender/recipient and
daemon perspectives are explicit. Intermediate states synthesized from a remote
result use compatibility_synthesized evidence; the result itself is
integration_reported. Incoming offers and local execution/recovery are observed
locally. Broker acknowledgment remains separate and is not recipient acceptance.

This qualifies transaction rollback and restart retention tests, not process-kill,
quota/retention, automatic run closure, or real model/tool adapter coverage. Those
M3 gates remain open.

Generic event.append does not feed the mandatory lifecycle journal, even when a
caller chooses a task-like event name. Synthesized provenance and sender-side
result attribution are internal transport parameters, not caller evidence keys.
Before M3 exit, extend attribution tests across every terminal state and local
sender/recipient co-location, alongside the remaining adapter/recovery gates.

### Session-loss closure and co-located attribution

Explicit session close, connector revocation and lease expiry now close that
session's open trace bindings in the same transaction as task recovery. The
shared closure routine interrupts missing model/tool endings with unknown
duration and emits an interrupted run boundary. Native roots create no task;
accepted work follows existing requeue policy, while running work follows existing
failed/non-retry-safe policy. Closing an observation never repeats an external
effect. Reconciliation after reopening the store closes an expired live session;
repeat reconciliation does not duplicate boundaries.

Public trace.finish retains its existing authenticated receipt semantics and
uses the same internal closure primitive. Automatic closure creates no caller
receipt. Closure persistence failure rolls back session, task recovery and trace
changes together. No connector authorization is bypassed through a new RPC.

For two local participants, an action by the local sender is now attributed to
the sender before recipient-locality fallback. The full terminal-state matrix
checks sender/recipient cancellation, recipient completion/failure/rejection and
daemon expiry/undeliverability, including same-state retries.

This supersedes the earlier statement that automatic session-loss closure is
unimplemented. It does not qualify process-kill/writer-restore fencing, late
adapter shutdown delivery, or real model/tool producers; those remain open.

### Managed runtime run producer

Each claimed managed task now gets a task-scoped RuntimeTrace. It binds before
the running transition and finishes after the authoritative terminal transition.
The handler receives the producer through its runtime context, not model-supplied
arguments. Missing/invalid/error trace replies leave execution intact and increment
a saturating task-local dropped-observation counter with a fixed diagnostic;
exception text and payloads are not logged. Failed finish is not retried by
rerunning a handler. Session reconciliation remains the fallback for an unclosed
binding. Durable coverage reporting for producer loss is still pending.

Binding creation now takes parent/context identity from the stored correlation
capsule when available, avoiding loss when the task payload omits those fields.
The real private-socket managed runtime test covers legacy and delegated tasks
with tracing available and unavailable. Available tracing records started and
completed with one attempt identity and preserved delegation parent.

Hermes, Gemma, Home Assistant and echo handlers accept both command and delegation
envelopes; other message types remain outside executable handling. Their package
locks were regenerated and validated. Handler tests use owned/mocked downstream
services; this change does not qualify real Hermes model/tool observations.

### Paired operation producer

RuntimeTrace.operation wraps one actual model/tool operation with a shared span
ID and distinct start/end observation IDs. Nested callers pass a parent span ID.
It measures elapsed execution using a monotonic clock, records wall-clock boundary
timestamps separately, and exports only allowlisted identifiers, duration, fixed
failure reason and explicitly supplied usage. Missing model usage remains marked
not_reported. Operation errors propagate unchanged; telemetry failure neither
reinvokes the operation nor logs its exception text. RuntimeTrace.observe accepts
a caller-owned boundary ID for callbacks that already own stable identities.

The real managed-runtime/private-socket fixture verifies nested boundary ordering,
parent identity, nonzero measured duration and explicit token usage. Its operations
are owned fixture work, not a live provider qualification. Installed Hermes tool
callbacks still need the request-scoped HTTP bridge to this producer, and model
HTTP-call counting in the M2 probe does not itself implement a production model
hook. These remain M3 acceptance work.

### Hermes tool callback bridge

The packaged HermesToolObserver attaches to one AIAgent's tool_start_callback and
tool_complete_callback. It owns a per-run call-ID map, sends observations through
that run's RuntimeTrace on the owning event loop, and bounds callback waits and
pending entries. It ignores callback argument/result values entirely. Duplicate
completion cannot produce another boundary; transport failures increment a bounded
callback-loss counter and do not escape into the tool. Calling from the event-loop
thread drops the observation instead of deadlocking.

An owned probe ran the installed AIAgent under its own interpreter, with an owned
streaming model endpoint and real agentd socket/journal. Two overlapping runs used
the same conversation and tool-call ID: four model requests caused two tool effects
and four isolated durable tool boundaries, with no dropped observations or private
result sentinel in metadata. This proves the callback module, not deployment into
the existing Hermes HTTP server. The request-scoped HTTP authority bridge and
model hooks remain pending; installed Hermes was not modified.

### Hermes HTTP request binding

The managed Hermes adapter/client forwards successful runtime binding, task and
session IDs in dedicated request headers, separately from the conversation ID.
The opt-in bound_api_adapter wrapper authenticates the existing Hermes bearer
credential first, then resolves the task/session through agentd using the server's
connector credential. Binding and conversation must match before upstream execution.
Partial metadata is rejected; requests with no execution metadata retain upstream
behavior. Supplied metadata never substitutes for either credential. Bound requests
fail closed if authority cannot be established; optional tool observations after
authorized execution still cannot change the tool result.

A ContextVar scopes each HTTP request, and callback closures explicitly cross the
upstream executor boundary while preserving upstream callbacks. The installed
APIServerAdapter/AIAgent proof rejects a mismatched conversation before any model
request, then executes two overlapping authenticated requests with isolated durable
tool observations. The production adapter sends headers, but an ordinary upstream
server does not acquire hooks merely by receiving them: launcher/deployment setup
must opt into the wrapper. That setup, model hooks and scoped model delegation
remain pending. aiohttp is a runtime test extra so bridge tests run in the normal
contributor suite; installed Hermes already supplies its server dependency.

### Hermes model request observations

The bound HTTP adapter now attaches HermesModelObserver to each newly created
AIAgent. It wraps that instance's streaming and non-streaming request methods,
without changing the Hermes installation or globally patching the class. Nested
method calls share one logical observation, so an internal streaming fallback is
not double-counted. SDK-internal retries are not separate transport-attempt events.

Each logical request has distinct start/end IDs, one span and monotonic duration.
Usage comes only from explicit nonnegative integer response fields; missing or
partial usage stays unknown rather than becoming zero. The original response or
exception propagates, and request/response bodies and exception text are omitted.
The observer is qualified against the inspected installed methods; launcher and
version compatibility still require a maintained deployment check.

An installed HTTP/AIAgent proof produced eight durable model boundaries for four
owned provider calls across two overlapping requests, with explicit 10/5 token
counts and four isolated tool boundaries. Unit checks also cover missing/sparse
usage, nested fallback, repeat attachment and provider error propagation. This
supersedes the earlier pending-model-producer statement for the tested Hermes
integration; live provider, deployment, scoped delegation and broader recovery
qualification remain open.

### Execution-scoped MCP delegation

The native/managed MCP server accepts reserved `tools/call` transport metadata
`_meta.edgecitadel_execution` with exactly `schema_version: 1`, `binding_id` and
`request_id`. This metadata is only valid for `edgecitadel_delegate`; model-visible
arguments remain recipient_id, request, optional skill_id and deadline_at_ms.
The server routes bound delegation through private `trace.dispatch`, which checks
the live binding and current recipient grant and atomically persists the child,
outbox, journal and retry receipt. Transport retries retain request_id. Malformed
metadata and model-supplied ancestry fail instead of silently creating a root.
Managed calls without reserved metadata retain the existing unbound MCP behavior.
Native calls without metadata now use the session-root path described below.

The Hermes bound adapter wraps each agent's run_conversation in a ContextVar scope
and passes the actual execution task ID separately from the conversation context.
An operator-configured scoped_delegate_handler supplies the reserved metadata to
its MCP transport; Hermes propagates the scope into concurrent tool workers.
The handler rejects calls outside a bound execution. It does not register itself
into an existing Hermes installation or provide a production transport launcher.

An installed Hermes HTTP/AIAgent proof with a real MCP stdio subprocess produced
two correctly parented children from overlapping executions sharing a conversation.
Repeating each MCP request reused its child; revoking the recipient grant denied
a fresh request without adding a child. Model and tool observations remained
isolated. Launcher/interpreter compatibility, production registration, native
root integration and broader M3 recovery/availability gates remain open.

### Operator-run Hermes launcher

`python -m edgecitadel_hermes_plugin.server` now composes the upstream API adapter,
bound HTTP wrapper and `edgecitadel-scoped` delegation toolset in one process.
It uses the managed connector's existing credential and the MCP server's tool-call
handler; it does not create another execution session. Registration rejects a
pre-existing delegation tool. The operator selects the toolset in the Hermes API
profile. The listener is loopback-only and requires a nonempty bearer-token file.

The deployment proof uses a dedicated Python 3.12.11 environment with installed
Hermes 0.15.1 and runtime wheels, the Agent Package module and explicitly supplied
matching schemas. It starts the actual CLI subprocess, exercises two overlapping
HTTP executions through the upstream model factory and registered delegation
handler, checks two distinct child parents/eight model/four tool boundaries, and
verifies clean SIGTERM shutdown. Python 3.11 is rejected before service startup.
The standalone runtime wheel's schema-directory requirement is documented in
the package README. This supersedes the pending-launcher statement for this
qualified configuration; the shared Hermes service was not changed. Other
versions, providers and API entrypoints remain unqualified.

### Native MCP observational roots

The production native MCP server lazily binds an observational root on the first
delegation in its session, then uses atomic trace.dispatch for actual children.
The root creates no executable task. One session owns one root; a renewed lease
keeps it, while a replaced session receives a different binding and root. Model
arguments cannot supply ancestry. Invalid or unavailable binding does not silently
fall back to unparented task creation.

Native delegation requires a string/integer JSON-RPC request ID. A deterministic
receipt ID derived from session and request identity makes a lost-response retry
idempotent without a growing memory cache; reusing an ID with different arguments
is a conflict. Graceful MCP close finishes the observational run with unknown
outcome before closing the session. Abrupt session loss uses existing interrupted
reconciliation. Neither outcome claims knowledge of the native application's turn.

The owned stdio-entrypoint proof created two roots and two children across two
connections, replayed each request without another child, and verified durable
root closure on EOF. Private-socket tests additionally cover forged ancestry,
closed-session rejection and replacement authority. This qualifies EdgeCitadel
MCP observations only; actual native application model/tool hooks and the M7
user-session acceptance remain unqualified.

### Scoped local journal inspection

Private `trace.history` and native `edgecitadel_trace` expose the durable journal.
This is a local inspection response, not the Core projection/read contract.
Parameters are optional trace_id, source_epoch, after_source_seq (default 0), and
limit (1–32, default 32). A nonzero position requires an explicit source epoch.
Absent an epoch, the newest retained source containing matching agent/trace
records is selected. The response carries schema_version 1, kind
local_trace_history, visible sources, selected source_epoch, events and nullable
next_source_seq. Continue with the same trace filter and returned source epoch.
An unavailable epoch/trace returns no events without revealing another actor's
records. More than 100 visible source epochs produces explicit unavailability.

Authentication and current read capability are rechecked under the store lock.
Native connectors require edgecitadel_trace; managed connectors may inspect their
own Agent history. Visibility follows the existing agent_id scope and does not
expand to all participants merely because they share a trace. Reading historical
events requires no active execution session. Revoked connectors cannot read.
Raw payloads, results and legacy attributes are not joined into these responses.

Pages contain at most 32 events and 512 KiB of events under the socket's ASCII
JSON encoding, leaving room below its 1 MiB response limit. Pagination uses local
source positions, not Core projection cursors, and is a live retained-history
scan rather than a historical snapshot. Coverage explicitly reports partial
local_agent_observations. producer_loss is reported when matching retained coverage
contains a producer report, otherwise unknown; absence never means zero loss.
Retention availability remains an M3 follow-up.

### Durable producer uncertainty reports

Private trace.loss accepts schema_version 1, request_id, binding_id, producer_id
(canonical UUIDv4 identities), and dropped_observations (1–2147483647). The binding
must permit append. Caller-supplied trace, actor, source or export-range claims are
not accepted. Its reply uses the trace RPC event-position receipt shape. Agentd
commits the receipt, a coverage event and selected export intent atomically.
Same-ID retries reuse the event; changed content conflicts. Authorization and
schema failures remain bounded, without input content in diagnostics.

The daemon emits phase unknown with integration_reported evidence, a cumulative
producer counter and current export-generation position. It does not invent lost
export ranges: a failed acknowledgement may describe an already committed event,
and an observation rejected before commit has no export position. Consumers use
the highest reported count per producer/attempt, not the sum of snapshots. A
report is evidence of uncertain observation delivery, not an exact missing-event
count or proof of complete coverage.

RuntimeTrace retains one pending report with stable identity; retries do not
increase the observation counter. It flushes after successful appends and before
finish. The Hermes HTTP bridge also flushes its separate request-local producer
before returning control to the managed adapter. A pending older snapshot can be
followed by one current snapshot per flush, keeping recovery work bounded.
Reporting never retries tool/model execution. Lost-response and rollback fault
tests verify one effect, one durable report and one spool entry after recovery.

Counters remain volatile until their report commits. A producer killed before
reporting, persistent storage failure, and reserved-capacity enforcement remain
M3 work. The local reader continues to show partial coverage even when a report
exists. New report fields require collectors supporting this contract revision;
older strict validators may reject them and must not advertise full coverage.

### Journal capacity admission

SQLite schema 11 adds transactional trace_storage_usage counters seeded from
existing event payloads. Insert/delete/size-update triggers maintain exact encoded
journal bytes and record count, including rollback; receipt retries do not charge
again. Admission reads the counter under the journal's existing write transaction.
This avoids a full journal sum on every observation.

Model/tool records stop at 256 MiB of retained encoded journal payload. Mandatory
lifecycle and control records may use an additional 1 MiB reserve so optional
observation pressure can still be reported and runs closed. Exhaustion returns
quota_exceeded without advancing source/export positions or partially writing
task state. The existing RPC contract marks quota errors non-retryable until
capacity is restored; it does not invite a busy retry loop. Mandatory operation
failures report the bounded operation error rather than malformed JSON.

Owned tests fill the optional budget, execute one tool effect, persist the two
failed-boundary count and close its run using reserved capacity. Exhausting the
reserve rejects another bind atomically. Migration and spool-failure tests verify
counter seeding, unchanged history, rollback and same-receipt accounting.

These are logical payload admission limits, not a physical SQLite file quota or
preallocated disk reservation. Indexes, WAL, receipts, spool rows and other task
data require separate measurement/enforcement. Pending events are not deleted by
this mechanism. Loss-aware pruning, physical disk-pressure behavior and reserve
reclamation remain required M3 work; schema 11 alone does not close retention.

### Active-generation loss-aware reclamation

Agentd reconciliation now attempts one bounded pressure-reclamation batch once
encoded journal usage reaches 90% of its normal allowance. It considers at most
64 records across at most eight active node sources. The batch commits only if
it reduces encoded payload bytes. Open bindings' run-start records and existing
coverage/source/security records are protected. Records referenced by a different
export generation are preserved; old-generation replay is not silently discarded.

For each affected actor, the same transaction records a source-observed coverage
marker and its export intent, coalescing unresolved export positions into exact
lost_ranges. Pending and broker_acked rows become lost_with_marker; core_settled
positions are excluded from the loss ranges. Event IDs, export positions and
hashes remain in the spool while journal references are detached. Only then is
the payload removed. Local-only or fully settled deletion records unknown local
coverage without inventing export losses. Markers have no fabricated task/run.

A marker-write or deletion failure rolls back the complete reclamation savepoint.
If the marker cannot fit reserved capacity, reconciliation preserves payloads and
existing admission continues to report quota exhaustion. The pruning path does
not recursively discard the only coverage evidence. Local history returns an
actor-wide local_history_pruned flag, including when a requested trace now has no
retained events; the flag does not assert that every trace lost records.

An owned fixture reclaimed 2700 encoded payload bytes to a 663-byte marker while
preserving original export identities. Tests verify exact pending-loss ranges,
settled exclusion, deletion-fault rollback, open-root/old-generation protection,
automatic pressure maintenance and reserve exhaustion. Time-based expiry,
old-generation reclamation, marker/spool compaction and physical file reclamation
remain open M3 gates. This implementation does not replace the separate legacy
30-day span/event policy or claim the complete storage budget is qualified.

### Arrival-based journal expiry

Schema 12 adds local received_at_ms metadata outside immutable event JSON and its
hash. New journal inserts stamp daemon wall-clock arrival time; receipt retries
do not refresh it. Migrated rows receive migration time because their original
arrival time is unknown. The migration preserves event/export identities, hashes
and sequence positions. An index covers eligible records by source and arrival.

Reconciliation now also expires active-generation metadata older than 30 days,
matching the existing local metadata horizon. Expiry uses the same bounded batch,
open-root protection and atomic loss-marker mechanism as pressure reclamation,
with retention_expired attribution. A record exactly at the cutoff remains until
it is strictly older. Producer occurred_at values do not control retention.
Pressure takes precedence when both conditions apply; the legacy span/event
policy and raw-content policies are separate.

Controlled-time tests prove fresh arrivals survive old producer timestamps,
strict cutoff behavior, pinned open roots becoming eligible after closure, and
conservative migration without hash changes. This is not a 30-day live soak.
Coverage/control records and other export generations remain protected pending
their compaction/reclamation protocol; physical storage limits remain unqualified.

### Local writer crash qualification

Maintained tests launch actual child writer processes using production store APIs,
wait for an acknowledged fault point, send SIGKILL, and reopen the owned database.
Dispatch, append and pruning each run at three points: inside the write sequence,
immediately before COMMIT, and after COMMIT before any result is delivered back
to the parent. In-transaction kills must recover the exact previous task/outbox/
journal/spool/receipt/operation/counter snapshot. Post-commit kills must retain
the whole update; same-request retries must reuse identities without new records.
Pruning retries preserve exactly one marker and the original missing positions.
Each case checks SQLite integrity and foreign keys after recovery.

A separate case durably writes one owned external-effect file after recording a
tool start, then kills the producer before terminal observation. Session recovery
marks the tool interrupted and the running managed task failed; opening a new
session cannot reclaim that failed task. The effect count stays one. This proves
the local recovery semantics without claiming that an interrupted tool's external
effect failed or was undone.

These ten cases qualify local writer SIGKILL boundaries, not power failure, full
host restart, source restore fencing or the unimplemented exporter handoff. M3-B
and M3-E remain open for those broader gates and physical disk-pressure evidence.


### Daemon state-directory ownership prerequisite

`serve` holds a nonblocking OS `flock` on the persistent `writer.lock` inode for
its lifetime, before opening SQLite or changing the socket/process record.
A second daemon exits with an active-writer error and preserves the live socket
inode and process record. The shared maintenance lock also excludes daemon
startup before any SQLite/process/socket artifacts are created. Graceful exit
clears the process record and socket under the lock; SIGKILL releases the lock
through the OS, allowing a replacement to reclaim the stale socket.

Owned subprocess tests qualify both exclusion directions and SIGKILL replacement.
The lock inode must never be removed/replaced while a writer is running. This is
cooperative ownership of one local state directory, not a database restore API,
a source-epoch allocator, a cross-directory/host fence, or a network-filesystem
qualification. Direct store users must acquire ownership separately. Restoring a
copied source with a fresh epoch remains required M3 work.


### Restored-source epoch transition primitive

The internal `rotate_restored_source` operation requires an active transaction,
store lock and externally held writer ownership. It compares the expected active
source epoch, retires that source/export generation, allocates fresh identities,
and inserts a selected `source.restored` marker at source/export position 1.
The marker links the previous source epoch and export generation without claiming
that the interval missing from the backup is complete. Subsequent new observations
use the new epoch; retained journal JSON, hashes, IDs, source/export positions and
spool settlement state are unchanged for replay.

A retry naming the previous epoch returns the current epoch's original restore
marker, even after reopen or later observations. A stale request from an earlier
transition fails rather than rotating again. A marker/spool write failure rolls
back the identity switch with the caller transaction. Source markers remain
protected by retention. No schema migration is required.

Tests use SQLite backup plus its matching payload key in owned directories,
compare retained journal/spool rows exactly, check fresh sequence allocation,
retry/reopen and stale-epoch behavior, and inject a spool failure. The helper is
not exposed as an operator restore command: directory copying/switching, fencing
all old producers, saved-session invalidation, task/outbox reconciliation and
coverage of the unknown backup interval still require an integrated M3 workflow.


### Fenced restore staging and startup barrier

The internal `stage_restore` operation acquires previous/snapshot directory writer
locks in sorted order and requires a separate, new destination directory. It
holds destination ownership while creating a mode-0600 startup barrier, copying
via SQLite backup, copying the payload key, checking integrity/foreign keys,
decrypting every saved task/result/outbox payload with that key, and committing
the fresh source epoch. It never resumes, requeues, deletes or marks published
any saved execution work. Once staging succeeds it writes a durable retirement
barrier in the previous directory. Files and directory barrier entries are
fsynced; this is not yet a power-failure qualification.

Daemon startup checks barrier presence under ownership before touching SQLite,
socket or process records. Even empty/partial barrier content blocks startup.
A live owner prevents staging before the destination exists. Copy/key failure
leaves the old directory unretired and the failed destination barred for diagnosis;
never reuse an existing destination. A failure after retirement remains closed.

Maintained tests use owned temporary directories and actual daemon subprocesses.
Accepted task, live-session and pending command rows remain exactly equal in the
original and staged stores, and neither can start automatically after successful
staging. An unrelated valid key fails decryption without retiring the previous
store. This is staging only: no operator CLI or activation API is exposed. Saved
session invalidation, task/outbox reconciliation, unknown-interval coverage,
interrupted-workflow qualification and cross-host writer handling remain open.
A barrier must not be manually deleted to bypass these obligations.


### Durable holds for restored execution state

Schema 13 adds `restore_holds` keyed by object kind/identity. Ordinary migration
creates an empty ledger and preserves existing tasks and command outbox contents.
Restore staging fills it with every nonterminal saved task and unpublished outbox
message, then invalidates saved sessions in the same transaction. Binding closure
records interrupted observations without ordinary accepted-task requeue or a
claim that the external task failed. If closure fails, both holds and session
changes roll back; the destination stays barred and the previous store is not
retired.

Held task reads include `restore_status: reconciliation_required` alongside the
unchanged last observed task state. Claim, transition, deadline expiry and session
recovery paths exclude held tasks. Transport polling and publish acknowledgments
exclude held messages; their original payload and publication state are retained.
Holds survive reopen. Fresh task identities remain claimable and fresh commands
remain publishable. These facts are store-level qualification, not an activated
restore workflow: the startup barrier still remains until explicit reconciliation
and coverage reporting are implemented and verified. No hold-release API exists
yet, and deleting hold rows manually is not a supported recovery procedure.


### Reviewed activation with unresolved work held

Schema 14 adds activation receipts keyed by node/source epoch. The internal
activation API locks old/new directories, verifies that the old directory's
retirement points to this destination, checks destination lineage and the active
restored epoch, and compares an exact reviewed hold-inventory SHA-256. The bounded
review summary exposes counts and digest; saved row contents are streamed into
the digest without being returned. Every nonterminal task/unpublished message
must be held, and every saved session closed, before activation can commit.

The transaction writes a selected unknown-coverage event causally linked to the
source.restored marker and its activation receipt. It does not invent a lost
sequence range or assert complete recovery of the backup interval. Only after
COMMIT does it unlink/fsync the destination startup barrier. The old directory
remains retired. A post-COMMIT failure retries from the same receipt/event;
while still barred it rechecks the inventory and execution guards. After successful
activation, a matching retry returns the receipt without reclassifying fresh work.

Owned subprocess qualification starts the restored daemon, creates a fresh task,
reads the held task's reconciliation status and rejects its transition. Old daemon
startup remains rejected. Tests cover stale review, missing retirement, new unheld
task/session, failed event persistence, post-COMMIT barrier-removal failure, and
populated-v13 migration preserving holds. These are internal APIs for trusted
operators. Individual held effects remain unresolved; hold release, operator CLI,
enrollment switching, cross-host fencing and actual restore-process kill/power
qualification remain open. No shared service was restored or activated.


### Actual restore/activation process-kill qualification

Seven owned child-process cases acknowledge their fault boundary before SIGKILL;
the parent checks the exit signal before recovery. Staging boundaries are after
destination barrier creation, inside the holds/session transaction, before old
retirement, and after retirement. Old execution rows remain unchanged. An
incomplete destination stays barred; before retirement, recovery stages into a
fresh destination. After retirement, the complete staged copy can be reviewed
and activated even when the original call never returned.

Activation boundaries are pre-COMMIT, post-COMMIT/pre-unlink, and post-unlink.
Before commit, no activation event/receipt survives. After commit, exactly one
survives. Retry yields one coverage event, spool entry and receipt, then an owned
daemon starts with held messages excluded from publication. SQLite integrity and
foreign-key checks pass. These tests exercise process death and lock release,
not full-host power failure, remote writer fencing or reconciliation of held
external effects. Fault interception is confined to the owned child test process;
production restore code has no injected fault hooks.


### Retired epoch and export-generation reclamation

Coverage event v1 gains optional UUID affected_source_epoch. Its own source_epoch,
event identity and export wrapper always identify the current writer. The affected
export scope is (node_id, affected_source_epoch when present else source_epoch,
attributes.export_generation); coverage_scope validates and returns that tuple.
The node remains daemon-owned. This optional field does not restamp historical
observations. Existing records without it retain their original interpretation.
Older validators may reject the field; they must never silently reinterpret a
cross-epoch loss as a loss in the emitting epoch. M4 must qualify that routing.

The bounded maintenance batch now considers retired sources and payloads shared
with retired export generations before active-only history. For each epoch/actor
group it emits a marker for every referenced export generation, with exact ranges
for pending/broker-acked rows. Settled/local-only content produces no fabricated
export loss. All markers/export intents precede payload deletion in the caller
transaction; old spool IDs/hashes/positions and retired sequence counters remain
unchanged. Current writer counters advance for new loss evidence only. Timed
expiry uses local arrival time; control records and open roots stay protected.

At most 64 candidates and eight referenced generations per group are processed;
larger groups stay replayable. Unreferenced generations do not block reclamation.
Pressure maintenance still commits only net payload-byte reduction. This is not
physical SQLite/WAL/index/spool/receipt compaction. Tests cover old epoch identity,
shared-generation ranges, rollback, timed expiry, unrelated generations and the
bounded-fanout fallback. M3 storage/performance and M4 settlement gates remain open.


### Closed-session retry metadata compaction

Schema 15 adds indexes for closed bindings, binding-scoped receipts and operation
parents. Reconciliation removes at most 256 non-bind receipts and 256 terminal
operation leaves per call when both binding and session closure strictly predate
the existing 30-day local retention cutoff. Expired leases without permanent
session closure do not qualify: renewal can still make a finish receipt usable.
Parents remain until no operation row references them. Deletions share the caller
transaction, so an operation-delete failure also restores deleted receipts.

Bind receipts and binding tombstones remain: binding request IDs have connector-
wide scope, and reusing one from a fresh session must still conflict. Old append,
dispatch and finish requests remain rejected by session authority after their
receipts are compacted; no task/tool operation is recreated. Journal, spool,
source/export counters and task contents are unaffected. Tests cover retry denial,
renewable sessions, identity reuse, age/batch bounds, rollback, scheduled reconcile
and populated-v14 index migration. This reclaims logical metadata rows/result
bytes, not measured SQLite/WAL disk space. Bind identity accounting, control/spool
compaction and physical budget qualification remain required work.


### Real SQLite FULL boundary and reconciliation recovery

Owned tests cap SQLite max_page_count at the current allocated page count and
append until the engine raises SQLITE_FULL (code 13). This is a real SQLite write
failure, not a mocked exception or the logical event-byte admission limit. Failed
journal/dispatch writes leave task/outbox/journal/spool/counters unchanged; retry
after raising the page ceiling creates one child and one command. Failed retention
marker persistence preserves payloads and the original FULL error. Optional tool
observations still surround one external effect, and a durable uncertainty report
is emitted after capacity recovery.

This probe exposed a daemon bug: an unhandled SQLite error terminated the
reconciliation thread while health remained ready. The loop now catches SQLite
errors, reports reconciliation=storage_unavailable and status=degraded, logs one
fixed diagnostic on entry, and retries at the existing one-second interval. A
successful pass clears that condition; failed integrity still reports failed.
Shutdown waits for the reconciler before closing the store. Owned Unix-socket
health tests verify degraded-to-ready recovery without shared-service changes.

Limits: max_page_count does not fill a filesystem, limit WAL bytes or qualify
ENOSPC/power loss. No physical total-size guarantee or exact producer-loss count
after producer death is inferred from these tests. Those gates remain open.


### Physical storage sizing and reference lookup

Schema 16 adds `trace_spool_journal` over node, source epoch, journal event ID,
export generation, state and export sequence. Retention can resolve payload
references by event instead of repeatedly scanning an entire source epoch. The
additive migration preserves spool contents and identities.

The owned sizing fixture measured 6.22 MiB journal JSON against 28.50 MiB live
trace pages. A pinned reader let WAL reach 51.18 MiB; after logical pruning and
checkpointing the database retained 5,582 free pages. Only an offline VACUUM of
the disposable fixture reduced its file size. These observations confirm that
the encoded-payload quota and row deletion do not bound physical storage. Physical
accounting/admission, WAL pressure handling and safe reclamation remain required;
no automatic production VACUUM or total-size guarantee is implemented here.


### Physical-pressure admission and WAL maintenance

Optional model/tool journal records now check conservative shared-store physical
pressure before insertion: max(database file bytes, page_count × page_size) plus
WAL and SHM file sizes. At 256 MiB (including the candidate encoded event), they
return the existing quota_exceeded outcome. File-stat failures return the fixed
storage_unavailable outcome. Existing committed IDs/receipts bypass admission;
mandatory lifecycle/coverage records retain their logical quota and actual SQLite
failure semantics. Task/content pages, indexes and reusable free pages count too:
this deliberately stops optional tracing early when other shared-store data grows.

Health adds physical_storage with file, allocated-page and reusable-page bytes,
pressure_bytes and optional_pressure_limit_bytes. When sampled usage reaches the
threshold, status is degraded and trace_storage is physical_pressure. Integrity
failure still takes precedence. No paths, payloads or per-agent data are exposed.

After reconciliation commits, under the connection lock, high physical pressure
with a nonempty WAL triggers a TRUNCATE checkpoint with busy_timeout temporarily
zero. A pinned reader remains valid; a busy result leaves optional admission
closed, and later maintenance retries. The original timeout is restored even on
error. This avoids waiting for reader locks; it does not bound checkpoint disk I/O time.
No VACUUM, reader cancellation, task-page cap or shared-service restart is added.

This is a pressure guard, not a physical hard cap or reserved physical capacity.
One transaction can cross the threshold; mandatory records and task state can
still grow. Measurement does not reserve index/receipt/commit bytes, and free
pages stay counted until actual file reclamation. Control/spool/identity bounds,
safe DB-file compaction and the original performance/failure gates remain open.


### Explicit offline database reclamation

The internal `storage_maintenance.compact_database(state_dir)` operation reclaims
free SQLite pages after logical retention. It requires an existing database and
startable directory, holds the same persistent writer lock as agentd, and obtains
SQLite exclusive locking with zero busy timeout. Active daemon ownership, active
readers and restore barriers cause failure; the operation stops no process.
It checks integrity and foreign keys before and after transactional VACUUM,
truncates WAL, and reports physical sizes. It performs no schema migration,
identity rotation, record deletion, key replacement or task reconciliation.

Owned tests preserve every application table's values and the payload key, verify
file shrinkage, reject competing ownership, and reopen/retry after SQLite statement
interruption. This is an internal offline primitive, not automatic compaction or
a completed operator runbook. VACUUM needs temporary disk headroom and can fail
when space is exhausted; filesystem ENOSPC and process/power-loss qualification
remain separate gates. M7 must integrate usable maintenance/recovery procedures.


### Producer death and mandatory interruption capacity

Owned SIGKILL tests cover both native observational roots and managed attempts
when an external effect is fsynced but two failed observations remain counted
only in producer RAM. After session expiry, history stays partial with
producer_loss=unknown; daemon closure records run interruption, never a fabricated
exact loss count or missing tool completion. Managed work becomes failed and is
not claimable again. The harness knows two observations failed; durable history
correctly does not claim that knowledge after producer death.

Daemon-generated interruption of an already persisted operation now uses the
logical control reserve even though its event kind is model/tool. The internal
journal reserve_capacity argument is set only by binding closure, not supplied
by producer append requests. Ordinary model/tool observations remain subject to
both physical pressure and normal logical quota. Exhausting even the reserve
rolls closure back; reconciliation catches the known quota_exceeded and
storage_unavailable contract outcomes, reports storage-unavailable degradation,
and retries. Other contract errors still surface rather than being suppressed.
Actual SQLite storage errors retain the existing recovery behavior.


### Real filesystem exhaustion qualification

The opt-in macOS gate uses separate owned 64 MiB HFS+ disk-image volumes, verifies
the mount has a distinct device and bounded capacity, then fills only that volume
until writes return ENOSPC (errno 28). Unlike max_page_count tests, this exhausts
actual filesystem capacity with SQLite's normal page limit unchanged.

Failed dispatch rolls back task/journal/spool/receipt state exactly; after filler
removal, same-request retry creates one child and one outbox row. Expiration
cannot erase retained payloads when its mandatory marker fails to persist.
Optional tool observation failures still execute one effect, accumulate two
reported drops, and publish one durable loss report after recovery. Integrity and
foreign keys remain valid. Test-owned mounts are detached; pytest image files
remain only in its temporary test directories until normal cleanup.
This qualifies these boundaries on HFS+, not every filesystem, a physical
telemetry hard cap, or power-loss durability.


### Loss-tombstone compaction and sparse export ledgers

Schema 17 adds an indexed lookup for coverage/lost markers by node, affected
source epoch (falling back to origin epoch) and export generation. Reconciliation
inspects at most 256 payload-free lost_with_marker spool candidates per transaction.
A candidate retires only if a retained coverage/lost event includes its exact
sequence in a lost range and has a retained selected export row in pending,
broker_acked or core_settled state. Local-only markers, other generations and
uncovered positions do not qualify. Markers and their export rows are not changed;
source/export sequence counters and all non-spool application rows stay unchanged.
A failure rolls the caller's whole maintenance transaction back.

Spool rows can therefore be sparse after explicit loss. M4 must derive assigned
positions from generation counters and reconcile absent positions only through
validated retained loss markers, including affected_source_epoch. Absence alone
is neither settlement nor proof of loss. Collector ingestion of the marker still
must commit the marker and its ranges before advancing settlement. Compaction
never fabricates Core receipt, changes settled rows, or discards replayable payloads.
Individual expired event IDs/hashes no longer have spool tombstones; durable range
evidence remains the supported record of those lost positions.

This bounds each candidate batch and removes per-event loss rows, not total
control metadata or marker-lookup runtime. Markers, identity tombstones and
settled rows still need their retention policies. Missing evidence can leave
candidates in place; the algorithm does not delete them to make a size claim.


### Atomic coalescing of active loss markers

Reconciliation inspects up to 64 selected retention/quota loss markers and groups
only the same active source epoch, active export generation and actor. Markers
covering another epoch or referenced by another generation remain unchanged.
A group of at least two becomes one new immutable event with the union of prior
lost ranges and all replaced pending/broker-acked marker export positions. The
replacement has a fresh event ID/source/export position. Settled marker positions
are not labeled lost: their per-event spool receipts remain core_settled with
payload references cleared, for collector-epoch recovery handling in M4.

The replacement uses a savepoint inside the caller's transaction. Old marker
payloads and unsettled marker export rows are removed inside that uncommitted
transaction before inserting the replacement, so their logical quota space can
be reused even at the limit. Insertion failure restores old evidence; concurrent
readers never see committed deletion without replacement. The group commits only
if encoded journal bytes strictly decrease. More than 128 disjoint merged ranges
leaves the group intact. Unknown/producer-loss, source/security and unrelated
actor markers are not folded into this policy.

This explicitly coalesces control history while preserving export-loss evidence;
it does not mutate old events in place or infer loss from sequence holes. A
100-cycle owned single-scope probe retained one journal marker and two spool rows
per cycle, preserving a settled hole. It is not a total physical-bound proof:
retired scopes, fragmented ranges, identity records and settled receipts remain
subject to their still-open retention policies.


### Export validation work and performance evidence

Export validation hashes the canonical bytes returned by its event-validation
pass instead of validating the same event again through event_sha256. Header
validation, event semantic checks, origin equality and hash comparisons remain.
An owned 1,000-sample local tracing canary still measured about 9 ms added p95
latency around a cheap hash action; it does not establish the ≤10% deterministic
reference-workload target. Approved workload, disabled baseline and original
baseline/stress/soak durations remain required performance qualification.


The internal journal handoff performs full event validation once, then validates
its constructed export header and hash. It does not call full export validation
again for the same stamped event. Writer origin fields are populated from the
same initialized identity used in that event, and all validation precedes journal
insertion. Public validate_export/ingestion validation remains unchanged. A frozen
four-task local comparison still exceeds the overhead target after this reduction;
its execution pattern is not a substitute for distributed qualification.


At startup, event validators partially evaluate only exact, else-free kind/const
conditions for declared event families. They retain the common schema and all
unrecognized conditions; missing, malformed or unknown kinds use the original
full validator. Canonical size/depth/type checks and event semantics remain
unchanged. Differential fixtures/mutations compare the specialized path with the
unchanged full-schema oracle. This reduces schema dispatch work, not the required
validation rules or the unresolved performance acceptance criteria.


An owned real-Unix-socket workload now verifies two overlapping native-style
observational roots and three managed recipients per root. The synthetic native
connector uses actual authenticated bind/dispatch RPCs; managed sessions claim,
bind, trace the actual echo handler and complete tasks. Six effects, stable
retries, twelve tool boundaries and per-task root/actor attribution are checked
against persisted events. This is same-host local routing and a synthetic native
connector; it neither impersonates a user session nor proves distributed delivery
or performance acceptance.


The overlapping-root workload also crosses actual NATS between four owned
agentd stores on one machine. An independent broker subscription observes six
commands and six completed results, whose parsed task/trace/parent identity agrees
with each recipient's persisted tool events and the initiating store's results.
Remote result payloads retain reserved execution correlation alongside the exact
application body. The proof includes both sender and receiver perspectives and
must not be mistaken for telemetry ingestion, four physical hosts or deployed
Core/Leaf acceptance. Same-host socket and broker-crossing variants remain distinct.


### Current-writer markers about retired scopes

Marker coalescing now groups by actor, origin stream and exact affected epoch/
generation. A replacement preserves retired-scope loss intervals; a separate
current-stream marker records discarded pending/broker-acked marker positions.
Settled marker positions remain settled receipts with no payload. Both writes
and deletion commit atomically, and the batch rolls back unless encoded journal
bytes decrease. Other-generation references remain ineligible; unions exceeding
128 ranges remain intact. The 64-candidate inspection bound is unchanged.

Focused retirement/compaction tests: 23 passed, including second-marker insertion
failure rollback and separate retired/current scopes; changed code Ruff/mypy and
whitespace checks passed. Evidence: execution/m3-retired-marker-compaction-evidence.json.
Full suite is pending until the owned timing run ends. Old-origin markers,
fragmented scopes, identity/settled-row retention and physical bounds remain open.


### Binding identity admission under physical pressure

Every new bind request now checks the existing conservative shared DB/WAL/SHM
pressure threshold before creating a binding or durable receipt. This includes
new request IDs that reuse an existing task execution binding and previously
could grow receipts without another journal event. Authorized exact retries
return before admission; identity conflicts, session/task ownership and closed
binding checks remain authoritative. Existing identities are retained. Storage
measurement failures return fixed storage_unavailable without exception text.

Focused binding/capacity/physical-pressure/producer tests: 21 passed; changed
module/test Ruff, module mypy and whitespace checks passed. Evidence:
execution/m3-binding-pressure-evidence.json. This admission guard is not a hard
SQLite allocation reservation, old identity compaction, or a bound on mandatory
task/control growth. Full suite remains pending after the frozen timing run.


### Task transport retains storage-failed messages

A reproduced result-ingestion bug terminated a valid NATS result when mandatory
trace persistence failed: lifecycle errors became StoreError and the transport
classified every StoreError as poison. It now propagates errors whose cause is
trace quota_exceeded/storage_unavailable without ACK or TERM, preserving the
broker's existing redelivery policy. Other invalid messages still terminate.
Focused real-store/instrumented-message tests verify exact rollback, recovery,
duplicate stability and malformed termination: 42 passed. Evidence:
execution/m3-transport-pressure-evidence.json. This does not prove the cause of the
long workload timeout, change ACK wait/max-delivery policy, or qualify actual
broker recovery timing. Full regression after this fix remains pending.


### Real broker pressure recovery and long-run follow-up

Owned NATS proof now verifies that failed quota persistence leaves one pending
ACK/message, then redelivers the identical result at the same stream sequence.
Successful recovery commits one lifecycle sequence and only then retires the
message. The fixture uses a one-second ACK wait; production remains 300 seconds.
Full current runtime/schema suite with owned NATS and real ENOSPC: 986 passed,
2 NATS_URL_TEST-dependent plugin checks skipped, 92.07 seconds. Artifacts:
execution/m3-transport-pressure-nats.xml and m3-transport-pressure-full.xml.

The separate enabled-only timeout reproduction completed 1,000 measured roots,
3,060 effects/commands/results, 6,120 tool boundaries and 51,000 journal records;
p95 2,009.748 ms. It did not reproduce the original timeout and used sources
frozen before the transport fix. There is no matched retained baseline and no
overhead acceptance claim. Evidence: execution/m3-nats-timeout-reproduction.json.
Remaining M3 retention/resource and performance requirements are unchanged.


### Fragmented loss-marker compaction

Coalescing now splits an exact merged union into selected markers of at most 128
ranges instead of abandoning every union above that limit. The existing
64-candidate bound remains; a replacement may not increase marker count and only
commits if encoded journal bytes decrease. All fragments and any separate
current-stream loss marker commit atomically with old-payload removal. Failure
on the second fragment restores the entire previous state, including counters.
Settled holes remain absent from loss ranges; no-reduction passes preserve state.

Owned synthetic sparse-history probe: five markers/13,059 bytes became two
markers/6,271 bytes (128 and 75 ranges), conserving exactly 207 lost positions and
excluding settled position 3. Evidence: execution/m3-fragmented-marker-evidence.json.
Focused tests 27 passed; full runtime/schema with NATS and real ENOSPC 988 passed,
2 NATS_URL_TEST-dependent skips in 79.22 seconds. Changed code Ruff/mypy and
whitespace passed. This improves fragmented storage but is not a hard physical
bound or a policy for indefinitely growing independent/unmergeable scopes.


### Retired-origin marker replacement

Coalescing now includes selected markers whose original writer or export generation
is retired. New records always use the active writer. Their affected epoch and
generation refer to the original coverage; discarded-marker positions are merged
only when they share that scope, otherwise a separate exact marker covers their
origin stream. No retired source/generation counter is advanced. Multi-generation
references remain excluded; transaction, count and byte-reduction bounds remain.

One-restore and twice-restored cases verify separate scopes and settled holes;
a second-scope write failure restores all previous rows/counters. The owned
three-epoch probe reduced payload bytes 3,586 -> 2,881 while preserving retired
source rows and the settled receipt. Evidence:
execution/m3-old-origin-marker-evidence.json. Focused 30 passed; full runtime/schema
with NATS/ENOSPC opt-ins 991 passed, 2 NATS_URL_TEST skips in 80.30 seconds. Changed
code Ruff/mypy and whitespace passed. Independent unmergeable scopes, singleton
markers, identity/settled metadata and physical/performance gates remain open.


### Closed bind reply compaction

After both binding and session are permanently closed beyond the existing 30-day
cutoff, maintenance replaces bind result_json with an empty object. Connector,
request ID, digest, scope and binding reference remain unchanged, preserving
connector-wide request-identity conflict detection. Original-session retries fail
authority before reply access; a new session reusing the ID still conflicts.
Renewable/merely expired sessions retain full replies. No journal/export/task
record changes. Each maintenance category is bounded to 256 mutations per call.

Owned probe: reply payload 309 -> 2 bytes with identical identity fields and
execution evidence. Cutoff, bounded batch, cross-session conflict and transaction
rollback tests pass. Evidence: execution/m3-bind-tombstone-evidence.json. Focused
20 passed; full NATS/ENOSPC runtime/schema 994 passed, 2 NATS_URL_TEST skips in
81.20 seconds; changed Ruff/mypy/whitespace pass. Retained identity count, lookup
work, independent coverage/settled metadata and physical/performance bounds are
not established by this payload reduction and remain open M3 requirements.


### Current matched NATS comparison

The verified current worktree completed a frozen original/current comparison:
1,000 measured roots per arm, baseline p95 2,003.787ms versus enabled 2,013.334ms,
observed +0.47645% (within <=10% for this scoped workload). Each arm reconciled
3,060 effects/commands/results; enabled retained 51,000 journal events and 6,120
tool boundaries. Both source immutability and final current-worktree hash match
passed, with owned cleanup and database checks. Evidence:
execution/m3-nats-matched-current.json and execution/m3-nats-matched-current.md.

This resolves the missing matched same-machine measurement; it does not qualify
prescribed duration/rate/stress/soak, physical multi-host/native-user topology or
future exporter/Core/UI costs. Existing transport polling dominates second-scale
latencies; earlier local overhead diagnostics remain valid in their own scope.
M3's independent metadata/resource bounds remain open. No code changed during
measurement, so the preceding 994-pass full regression still covers current code.


## Historical import persistence (schema 18)

The private agentd service now implements administrator-only `trace.import.configure`
(`import_source_id`, `agent_id`, boolean `enabled`) and `trace.import` using the
frozen v1 request. Each source/agent grant retains a daemon-generated namespace
through revocation/regrant. A live connector/session is unnecessary. Imports
create historical journal/export evidence and a durable deduplication receipt;
they create no executable task, outbox command or live binding. Revocation is
checked before retries. Archive-local content references are not live content
claims; unresolved cross-run parent references stay null. Import receipts survive
journal pruning by design; their resource policy remains part of the open M3
bounded-metadata work. Maintained tests now qualify actual Unix-socket import across service restart,
populated fenced restore preserving namespace/receipts, pruning followed by retry,
concurrent dedupe, forged metadata rejection and final stamped-size validation.
The full runtime/schema gate passes 1,015 tests with two NATS_URL_TEST-dependent
skips. Operator activation and global metadata bounds remain separate work.


## Anonymous authentication rejection recorder

Agentd coalesces rejected connector/administrator authentication in one saturating
volatile counter (maximum 9,007,199,254,740,991). Missing and malformed credentials
are rejected and counted; authenticated permission denial is separate. Incrementing
this counter performs no database write and accepts no caller metadata.

The service reconciler flushes using daemon-owned node identity, reserved control
capacity and an atomic journal/export transaction. It writes at most one immutable
security/authentication_rejected event per (node, source epoch, UTC calendar minute).
Its deterministic event ID hashes the domain edgecitadel-authentication-rejection-minute-v1,
NUL, and canonical [node_id, source_epoch, minute] using SHA-256, first16 bytes with
UUIDv4 version/variant bits. The existing protected journal row is the durable
minute receipt. Repeated flush, restart and clock rollback cannot replace that
minute's event; further pending counts wait for another unrecorded minute.

Actor/run/task/context/attempt/span identities remain null, causes empty, and
attributes contain only authentication_failed plus count. No credential, supplied
ID, address, exception or request content enters the event. Failed flush retains
the pending count and leaves authentication denied. Counter saturation and counts
lost at shutdown/crash mean records are lower-bound observations, not exact lifetime
totals. Missing daemon identity/storage cannot be repaired with caller identity.

No schema change was required. Security records are currently protected from
pruning; any future retention policy must preserve the minute receipt before
removing one. Rate limiting and reserved capacity do not alone qualify global
metadata/storage bounds. That remains open M3 resource-policy work.


## M4 telemetry stream provisioning primitive

The shared runtime module `edgecitadel_plugin_runtime.telemetry_stream` defines
Core stream `EDGECITADEL_TELEMETRY_V1` on `edgecitadel.telemetry.v1.*`, separate from
`AGENT_INBOX` and `edgecitadel.telemetry.settlement.v1`. Initial configuration is
128MiB/100000 messages/1-hour age,18KiB message bound,120-second duplicate window,
file storage,one replica,one consumer,LIMITS retention and discard-new. Durable
`core_trace_ingest_v1` is pull/explicit-ACK with30-second ACK wait,128 pending ACKs,
four waiting pulls and redelivery until ACK or stream expiry. Provisioning verifies
existing required settings and rejects drift without silently rewriting policy or
consumer cursor. Broker ACK is still not Core settlement.

This primitive is not yet wired into live startup. Its owned-broker tests prove
idempotence, ACK-floor preservation, configuration drift refusal and reduced-quota
isolation from command publishing. Production Core/Leaf placement, independent
broker reservation, sustained saturation, measured capacity and exporter/collector
wiring remain M4 implementation and exit gates. The current1GB broker storage
limit cannot be assumed to reserve both the1GiB command stream and telemetry.


## V2 settlement interval pages (M4 amendment)

The cumulative v1 reply cannot encode arbitrarily many disjoint rejection/loss
ranges while keeping its128-range bound. V1 schemas, subject and interpretation
remain unchanged; its reader returns unavailable when the complete range set
cannot fit. New page semantics use separate schemas
`trace-settlement-page-{request,reply}.v2.json` and the control subject
`edgecitadel.telemetry.settlement.v2`. There is no silent fallback that treats a
page as a v1 cumulative checkpoint. The trusted-fleet boundary is unchanged.

A request includes source/export scope, request UUID, `after_export_seq` and a
nullable `collector_epoch`. The initial request uses base0 and null epoch. Any
positive continuation base requires the known collector epoch. A successful reply
contains `page`, never `checkpoint`, and echoes scope, epoch and base. It describes
only `(after_export_seq, settled_export_seq]`: omitted positions within that
interval are accepted, and explicit rejected/lost ranges classify the remainder.
It says nothing about positions at or below its base. Request size remains1KiB;
reply size remains18KiB and each range class remains limited to128 entries.

Core stops a page before the first range that would exceed a class limit and sets
`more=true`. The next page starts at that page's terminal position. Core stops at
the first uncovered position; a higher received sequence or marker watermark does
not fill a gap. `more=false` means no additional contiguous page is presently
available, not that the source is complete. Known scopes can return an empty page
with terminal position equal to the base and `more=false`. Unknown scopes return
`unknown_source` with no page. A supplied collector epoch different from the
current Core returns `collector_changed` with no retirement evidence.

The source must bind requests to its durable per-scope applied cursor. It must
atomically commit a page's classification/retirement with advancing that cursor,
using a compare-and-swap against the page base and collector epoch. A successful
initial page starts at0. A stale, reordered, skipped, differently scoped or
uncorrelated page cannot advance local retirement. Server acceptance of a base is
not evidence that the source applied previous pages. Duplicate application must
be idempotent; restart must resume from the durable cursor, not the last received
reply or broker ACK. Collector change invalidates old retirement assumptions:
retain/replay available payloads and report expired/unknown evidence honestly.

Polling retains the30-second idle interval, five-second timeout and one outstanding
request per scope. While `more=true`, continuation requests must remain sequential,
with at least500ms between requests, honoring bounded backoff/retry hints. Unknown
or unsupported page protocol pauses with a compatibility fault and retains payloads;
it must not reinterpret v1 as paginated v2. Source-side application, scheduling,
Core rate/work limits and live responder are still implementation requirements.

The codec `edgecitadel_agentd.trace_settlement_pages` and Core reader
`aggregator.trace_settlement.settlement_page_reply` are implemented and tested.
They do not enable collection or retirement. The reader has bounded working memory
and response size, but scan work still grows with retained evidence; query budgets
and incremental checkpointing remain M4 qualification work.


## Source page application implementation (schema19)

`trace_settlement_apply.page_request` reads a durable per-generation cursor;
`apply_page` validates correlated v2 replies, checks the existing cursor/collector
epoch and locally assigned high-watermark, and commits spool classifications,
`core_settled` marks and cursor advancement in one transaction. Schema19 adds
`trace_source_settlements` (one cursor/latest page per export generation) and
nullable `trace_spool.core_outcome` (`accepted`, `rejected`, `lost`). Existing rows
are not inferred settled by migration. No journal payload is deleted by application.

Identical latest-page reapplication is idempotent; stale/reordered bases, future
unassigned positions and collector changes cannot advance retirement. Error replies
make no storage change. Sparse retained rows use assigned export positions rather
than row count; retired source generations remain addressable. A collector change
is refused pending explicit recovery; no automatic reset or missing-payload replay
claim is implemented here. Latest-page storage is bounded per scope, but total scope,
index/WAL and spool growth still require the M4 physical-budget policy. Live polling
and source recovery remain unwired.


## Settlement polling implementation boundary

`trace_settlement_poll.SettlementPoller` is an event-loop-owned worker for one
source/export tuple. Concurrent triggers share one outstanding request and page
application. It enforces five-second request timeout, 30-second idle polling,
500ms page continuation, and transient backoff from500ms to30s with bounded server
retry hints. Requests always read the durable source cursor. Reply decoding is
bounded to18KiB and rejects duplicate keys; malformed/uncorrelated replies are
unavailable evidence. Only a validated correlated page reaches atomic application.

Unknown-source replies and unsettled tails produce a replay_required signal;
the worker does not itself schedule exporter replay. Unsupported/invalid request
faults and collector_changed pause with fixed diagnostics. Collector change still
requires explicit recovery, not an automatic cursor reset. Stop cancels network
and scheduled waits without undoing a committed application. The lifecycle owner
must ensure one worker per scope and isolate synchronous persistence from command
handling; no daemon startup, global rate admission or operator API is wired yet.


## Collector-change recovery implementation (schema20)

`trace_collector_recovery.begin_recovery` requires the expected durable collector
epoch and fences that export scope before replay work. The caller must first stop
its scope worker and confirm collector change; failed recovery admission must leave
the worker paused. Schema20 adds one recovery row per export generation with scan
progress, a captured assigned-position boundary, phase and retired-collector epochs.
At most64 prior collector epochs are retained per scope; capacity exhaustion is an
explicit failure, never permission to forget an epoch or continue unsafe retirement.

`recover_batch` processes at most64 existing spool rows per transaction. Retained
payloads return to pending with old settlement classification cleared. Unavailable
assigned positions, including compacted sparse holes, receive exact selected loss
markers before replay state/progress commits. Markers use the current writer for
that node and explicitly name the affected epoch/generation. The captured boundary
prevents newly appended events/markers from making recovery scan forever. Marker
failure rolls back the entire batch; restart resumes durable scan progress.

Settlement requests and application are fenced while scanning. Once ready, requests
start at0 with no claimed collector epoch. Pages from every retained retired epoch
remain refused. Adopting a genuinely new epoch atomically returns recovery to live
and installs its page cursor. Normal journal retention still owns payload removal.
The scoped sync worker now invokes recovery after a collector-change reply and
resumes a durable scan at worker startup. Daemon startup remains unwired. Physical
metadata budgets, admission policy at the epoch-history cap, real network reset
faults and restore operations remain gates.


## Source synchronization worker

`trace_sync.TraceSyncWorker` coordinates one explicitly supplied source/export
scope. Its publisher remains independent of settlement network/backoff waits.
Unknown-source and unsettled-tail responses coalesce retained replay requests;
replay uses the same event payload and broker message identity in 32-row batches.
A collector-change response cancels and joins publishing before recording the
recovery fence. Recovery processes bounded transactions, yields between batches,
and retries transient storage failures while publishing remains stopped. Startup
resumes an existing scan before either network path. After recovery, polling starts
from the durable ready state and publishing sends retained pending rows.

Permanent control/publish faults pause the scope. Stop cancels the shielded request
and publisher before releasing the worker; committed journal or settlement state
is not undone. Concurrent runs of the same worker are rejected. The future lifecycle
manager must additionally guarantee uniqueness across worker instances, discover
retired and current writer generations fairly, cap admission, and isolate SQLite
work from the command loop. This helper is not wired into production startup.


## Bounded source generation scheduling (schema21)

`TraceSyncManager` admits at most eight scopes per 30-second window and joins all
workers before advancing its keyset cursor. Discovery reads at most eight generation
rows using their primary-key ordering, including empty, retired and paused rows;
only nonempty, unpaused scopes start workers. A per-pass upper boundary and cursor
wrap discover newly created generations, including current-writer loss markers
created while recovering a retired scope. No list of all generations is retained.

A separate `telemetry-sync/writer.lock` under the state directory admits one manager
across instances/processes. Schema21 adds nullable `sync_fault` (at most64 characters)
to each existing export-generation row. Permanent worker faults persist there and
remain excluded across rotation/restart until explicitly cleared with
`clear_scope_fault`. Fault-write failure pauses the manager after worker cleanup.
The scheduler stores fixed fault codes, never exception text or payloads.

Scheduling bounds worker count and discovery result size; it does not prove a total
physical metadata budget or a catch-up deadline for arbitrary generation counts.
Window rotation revisits durable spool/cursor/recovery state, while ephemeral retry
state is recreated on readmission. A scope's readmission is separated by at least
the normal30-second polling interval. Lifecycle wiring must run this manager with
its own telemetry connection/event loop and expose paused state and explicit retry.
Production daemon and Core startup are still unwired; measured network, crash,
backlog, outage and physical budget gates remain required.


## Source daemon lifecycle

Agentd now constructs a default-disabled `TraceSyncService`; only process environment
`EDGECITADEL_TRACE_SYNC=1` enables startup. It owns a distinct thread/event loop,
SQLite handle and NATS connection, reads the same validated endpoint state as command
transport, and publishes on Core-routed subjects in direct and Leaf modes.
Only Core provisions the stream/consumer. Direct source startup verifies read-only
and retries missing resources; Leaf startup skips management API lookup because
those requests resolve locally. Core verifies central policy; a direct source also
pauses on observed policy drift. Initial transient connection failures retry with
capped backoff. Shutdown cancels and joins manager/worker requests before closing
its connection and store. No Core consumer is created by source startup.

Health exposes enabled/state/connected/active_scopes/fault without credentials or
exception text. Configuration or lifecycle faults pause telemetry rather than the
command transport. Scope faults remain durable across service restart. Shared SQLite
write locks/disk still require pressure/overhead qualification. Operator controls,
complete metrics and original M4 acceptance gates remain unfinished.


## Core collector lifecycle and control admission

The Aggregator now starts `TraceCollectorService` only when its process environment
sets `EDGECITADEL_TRACE_COLLECTOR=1` (default off; raw Compose forwards the setting).
It owns a separate thread/event loop, NATS connection and SQLite handle on the
Aggregator database. A state-directory lock admits one collector. Startup verifies
the stream and durable policy, then pulls at most32 records and subscribes to v2
settlement requests. Commit precedes broker ACK. Failed durable disposition NAKs
that record with delay while proceeding through the fetched batch; no receipt is
invented. If many unacknowledged poison records fill max_ack_pending, broker
backpressure still applies; this is not a general invalid-flood availability claim.

`/api/system/status` exposes collector enabled/state/connected/fault/epoch. Transient
startup database locks retry. Shutdown joins the collector before draining command
transport. The Aggregator's command inbox task now has an explicit stop flag and
owned task handle: NATS fetch cancellation can become a timeout, so cancellation
alone was insufficient to terminate its former unconditional loop.

The v2 control subscription bounds pending work to32 messages/32KiB. Requests over
1KiB or without a trusted UUID nonce are dropped; valid nonces receive correlated
version/shape/rate errors. A global token bucket admits20 SQL requests/second with
burst20. SQL work is interrupted at100000 progress steps or a20ms execution deadline,
checked every1000 VM steps. SQLite busy waits have a separate50ms bound; the deadline
is not a hard end-to-end wall-clock guarantee. Failure returns temporarily_unavailable
and never an incomplete successful checkpoint. Callbacks yield between requests.

V2 snapshot reads cap dense evidence to512 positions by finding the next indexed
position boundary, while retaining efficient jumps across huge explicit loss
intervals. Pages indicate continuation at that boundary. V1 helper behavior is
unchanged; this network lifecycle serves v2. Legacy deployed-client compatibility
still needs qualification before release.

This is a development opt-in, not M4 acceptance. Physical raw/index/WAL/receipt
budgets, safe metadata compaction, managed deployment setting persistence, operator
restore epoch rotation and controls/metrics, full Leaf/crash/network faults, ten-minute
outage/five-minute catch-up and saturation/overhead remain required.


## Core evidence capacity and physical pressure

Core evidence transactions maintain six fixed accounting rows through SQLite triggers.
Admission limits encoded raw payload to 8 GiB, raw rows to 10 million, positions to
20 million, conflicts and loss ranges to one million each, and rejected positions
and identities to 4096 each. Exceeding a limit rolls back evidence, accounting and
cursor together; known durable dispositions remain ACK eligible. No eviction is
implied. Initial accounting backfill scans legacy evidence under a write transaction;
subsequent starts do not rescan. Large migration latency remains unqualified.

Before evidence writes, including fixed poison-counter updates, physical admission
checks shared main/allocation/WAL/SHM pressure under the write lock. Thresholds are
16 GiB total pressure and 64 MiB WAL with configured 8 MiB headroom. A nonblocking
checkpoint is attempted before the transaction when pressured; pinned readers are
not canceled. Collector cache spill is disabled. A pressure refusal NAKs without
creating a settlement receipt. Status exposes logical usage/limits/capacity and
physical pressure even after a duplicate is acknowledged.

These are evidence admission guards, not a hard bound on the shared database.
Command writers and schema bootstrap/backfill are outside these guards; worst-case
transaction headroom, safe compaction and global physical-storage qualification
remain open. Logical byte/row accounting alone does not bound WAL growth.


## Preparing a Core journal restore

From the repository root, run `aggregator/.venv/bin/python -m aggregator.trace_restore
<snapshot.db> <new-output.db>` with the runtime dependency installed. The destination
must be a new offline path in an existing private directory. The command uses SQLite
backup to include committed WAL state, checks integrity and collector identity,
upgrades accounting in the private copy, and rotates the collector epoch. Raw events,
positions, rejection identities, loss ranges, ingest sequence and unrelated command
tables are preserved. Repeated preparations generate distinct epochs. Startup then
preserves the prepared epoch; requests naming the previous epoch receive
`collector_changed`.

Preparation publishes a closed, synchronized database with mode 0600 using an atomic
no-overwrite link. Existing files and symlinks are refused. Failures before publication
leave no destination and do not modify snapshot evidence. A failure after publication
(such as directory fsync failure) may leave a complete destination; inspect it instead
of assuming no file exists. An abrupt process death may leave private staging files.

This command does not activate the database. Cutover requires stopping every user of
the target Core database, preserving the prior database with its sidecars, and using
the prepared database with no stale sidecars before restart. Automated cutover, process
crash/durability qualification, managed deployment integration and cross-host source replay
and the full restore fault matrix remain open. A backup cannot recover rejection identities or raw
evidence absent from that backup; source reconciliation and explicit loss remain
required. Do not restore a raw snapshot directly under its old collector epoch.


## Broker deduplication during collector recovery

Immutable event IDs, canonical payloads and export positions remain unchanged during
collector restore. Broker delivery IDs are stable within a recovery cycle, but must
change when recovering from a newly retired collector epoch: the same broker may
still remember an original publication that its durable consumer already ACKed.
Reusing that broker ID can suppress delivery to the restored Core even though the
source receives a successful duplicate broker ACK.

The exporter preserves its original message-ID hash before any recovery. Afterwards
it adds the literal `collector-recovery` and the last durable blocked collector epoch
to the hashed identity. This discriminator survives restart and remains unchanged
when recovery transitions to live; a subsequent collector change advances it. No
event/wrapper schema, source identity or export generation changes. The existing
bounded recovery history supplies the discriminator without another metadata table.

Owned broker tests now exercise actual older-snapshot preparation followed by Core
service replacement on the same broker/durable within its deduplication window.
Both fully retained replay and an injected unavailable local position converge; exact
retained event IDs and the sole missing range are checked, and a delayed old page
cannot mutate source settlement. Test polling is accelerated to 50 ms; these cases
do not prove normal polling latency, the ten-minute outage target, full Leaf restore,
or arbitrary process-crash/cutover safety.


## Leaf broker recovery qualification

An owned real Core/two-Leaf test runs production source/Core telemetry services,
terminates the Core broker, queues five more source events, verifies no settlement
advance during six seconds of outage, and restarts the broker with retained state.
All six event IDs, hashes and canonical payloads reconcile with the original collector
epoch. The Leaf-local command stream accepts a record while Core is unavailable,
and no telemetry stream is created on the Leaf. This proves local command capture,
not inter-Edge command availability during Core loss. Normal timers are retained.

This test exposed source provisioning through the Leaf's local management API.
Source startup now never creates a stream: direct mode verifies read-only and Leaf
mode relies on Core provisioning and routed publish/settlement. Existing unexpected
Leaf telemetry streams are not automatically deleted by this change. Broker-age
expiry, link-only faults, full cross-host recovery and storage/operations gates
remain separate qualification obligations.


## Broker expiry before collection

Owned direct-Core tests now verify actual age eviction between broker ACK and Core
commit. Both an unknown source and a known generation with a missing tail retain
payloads and unchanged settlement until polling triggers replay and Core commits.
Exact IDs, hashes and canonical payloads reconcile with no loss markers or tasks.
Only test broker max_age/deduplication windows are shortened (2s/1s); production
policy remains 3600s/120s and source scheduling is unchanged. This does not qualify
one-hour buffer sizing, Leaf expiry or total retention exhaustion.


## Link-only recovery qualification

An owned bidirectional TCP relay test severs the Core/Leaf connection for six
seconds while both brokers remain alive. Source payloads and checkpoint remain
honest, Core receives no outage records, and Leaf-local command capture remains
available. Reopening the same link reconciles exact event IDs/hashes/payloads with
an unchanged collector epoch. The fixture routes advertised reconnect addresses
through the relay as well. This is loopback hard-disconnect evidence, not asymmetric
packet-loss, cross-host availability, saturation or total-retention qualification.


## Retention exhaustion during a collector outage

Owned direct-Core tests expire broker-acknowledged records, then run production
source maintenance against artificially aged receipt timestamps. Both partial loss
and removal of every original payload preserve exact durable coverage markers.
When test limits deny marker reserve, maintenance leaves the entire logical store
unchanged. After source-store reopen and collector recovery, raw IDs/hashes/payloads
match the retained journal exactly, and Core records the precise missing interval
before settlement advances. No tasks are created. Broker age/dedupe windows alone
are shortened to 2s/1s; source scheduling and deployed retention policy are unchanged.
This does not qualify destruction of control evidence, physical filesystem exhaustion,
Leaf retention loss or full storage/operations acceptance.

## Read-only Core evidence inspection

Run `python -m aggregator.trace_inspect /absolute/path/to/core.db` from the repository
root with the backend Python environment, or with the repository root on PYTHONPATH.
The command opens an existing Core database read-only without initializing or
migrating it. It requires local filesystem access and creates no HTTP/RPC surface.
Source-side inspection is documented in `agent-runtime/README.md`.

`--view raw` (the default) returns immutable raw event IDs, hashes, canonical event
content, receipt time and commit cursor. Coverage events retain explicit loss ranges
and affected-source epoch/generation. `--view positions` shows accepted, duplicate
and conflict dispositions; `--view conflicts` shows retained conflict hashes/reasons;
`--view rejected` shows hash-only rejection receipts, never rejected payload text.
All views include collector identity/cursor, fixed capacity-accounting rows and
fixed poison counters. Usage is encoded payload/row accounting, not physical disk
measurement. None of these views asserts complete source history.

Pages contain at most 32 records (`--limit 1..32`) in indexed ingest order. Continue
the same view with `--after N --collector-epoch E`, using `next_ingest_seq` and
`collector.collector_epoch` from the previous response. A changed epoch is refused;
restart inspection at zero after a restore. The command also refuses a cursor ahead
of the current collector. Missing ingest numbers within one view are normal because
the global cursor covers several kinds of commits. A null next cursor means only
that no further records existed in that view at the snapshot, not that collection
is complete; live ingestion can append more records later.

One read snapshot has a cooperative 100,000-step/50ms SQLite query budget, checked
every 1,000 VM steps, and a separate 50ms busy timeout. These limits are not a hard
wall-clock SLA. Errors return fixed JSON diagnostics and nonzero exit status.
Inspection does not replay events, rotate collector identity, activate a restore,
change settlement, clear rejection evidence, or execute tasks. Operator lifecycle
controls and full live operational metrics remain separate M4 work.

## Administrator Core collector control

`POST /api/system/telemetry/control` accepts JSON `{"action":"stop"}`,
`{"action":"start"}`, or `{"action":"retry"}` with the existing
`X-EdgeCitadel-Admin-Token` header matching `EDGECITADEL_ADMIN_TOKEN`. Missing,
incorrect or non-ASCII credentials are rejected; an unset configured token leaves
control unavailable. Unknown actions/extra fields are rejected. The endpoint cannot
enable a collector omitted at startup: `EDGECITADEL_TRACE_COLLECTOR=1` must already
be set, otherwise it returns 409. It returns 503 for an unavailable lifecycle.

Stop cancels/joins the dedicated consumer and settlement responder without stopping
the Aggregator's command connection. Source payloads remain governed by their
normal retention/replay rules while settlement is unavailable. Start is idempotent
and asynchronous. Retry stops and restarts the configured collector after diagnosis;
it does not repair invalid broker policy or rotate collector identity, delete raw
records, clear rejection evidence, or change the durable consumer configuration.
A restart revalidates stream policy and can pause again on the original fault.

Lifecycle operations are serialized; terminal application shutdown prevents late
control requests from restarting workers. Stop lasts for the current process only;
a new process follows its startup environment. Inspect `/api/system/status` for the
resulting running/paused/stopped state and the read-only Core CLI for evidence.
These controls do not complete the live metrics, physical storage or performance
gates. No public unauthenticated lifecycle operation is provided.

## Core live collection metrics

The existing system-status telemetry object now contains `metrics` with fixed keys
and `lifetime: service_instance`. Counters survive stop/start/retry on that object
but reset with a new service/process. They saturate at 2^53−1, have no per-source
labels or retained samples, and are best-effort operational observations, not a
replacement for durable raw/position/rejection accounting.

`commit_observations` separates accepted, duplicate, conflict, rejected and
quarantined dispositions observed after the persistence helper succeeds and before
broker ACK. A redelivery may return the original accepted disposition, so these
are delivery observations rather than counts of newly inserted unique events.
`ack_successes` counts confirmed ACK returns; `ack_failures` counts NATS/OS ACK
errors after a commit; `persistence_failures` counts database/contract failures that
prevent disposition and lead to delayed NAK. A commit can therefore be visible in
metrics even when ACK fails. The post-commit observer is isolated: its exception
cannot change acknowledgment behavior. Missing observations remain possible during
crash or observer failure; durable inspection is authoritative.

`last_commit_observed_at_ms` is the collector wall-clock observation time.
`last_delivery_age_ms` measures that time minus the broker's delivery metadata
publication timestamp. It includes broker queue/retry time, uses two clocks, and
is not source occurrence-to-ingest latency or source export lag. Missing metadata
reports null; a future broker timestamp reports null with `delivery_clock_skew`
true. Positive clock skew cannot be diagnosed from this value alone. There is no
histogram, total latency, or complete live-metrics claim. Source publication failure
metrics and complete transport/collection lag reporting remain M4 work.

## Configured telemetry saturation qualification

An owned loopback broker using the production 128 MiB telemetry quota now has
measured byte-saturation evidence: 134,217,061 logical bytes retained, overflow
rejected, and 18 task effects/command/result pairs completed on four real agentd
nodes during pressure. Sampled total broker allocation peaked at 134,356,992 bytes;
logical quota is not a physical-filesystem bound. The source exporter retained its
pending canonical event through rejection and published it after owned capacity
release without claiming Core settlement. Pressure uses padded repeated valid
observations with distinct transport IDs and Core collection absent; this does not
qualify representative workload overhead, active Core SQLite pressure, long-run
metadata growth or Leaf/cross-host/global filesystem saturation.


## Core broker-backlog observations

The system-status telemetry object includes `broker_backlog`. While the collector
is connected, an independent asynchronous sampler reads its existing durable
consumer's metadata: `pending_delivery` is messages not yet delivered, and
`awaiting_ack` is delivered messages without a confirmed broker ACK. A successful
sample includes `state: available` and `sampled_at_ms`; it is an observation at
that time, not a live transactional view of Core SQLite. In particular, a message
awaiting ACK may already have committed to Core, and zero backlog does not prove
complete source history or source settlement. No counts are inferred from Core
rows or source assignment watermarks.

There is at most one metadata request in flight, with a two-second timeout and
a five-second delay after each attempt. These are cooperative event-loop limits,
not a hard deadline during a process/OS pause. Timeout, invalid counts or metadata
errors replace old counts with `state: unavailable`; they do not stop ingestion,
ACK, publish, provision resources or call task APIs. Stop/retry/shutdown cancels
the sampler and clears its sample. No histories or per-source labels accumulate.
Use source per-state retained bytes/age, broker backlog, committed Core metrics
and exact persisted positions as distinct observations. Complete lag semantics
and remaining storage/topology qualification are still required for M4 exit.


## Read-only Core source progress

`python -m aggregator.trace_inspect /absolute/path/to/core.db --scope NODE EPOCH GENERATION`
reads a committed v2 settlement interval for that exact source/export generation.
The new mode needs the installed Agent runtime contract package; in a checkout,
include both the repository root and `agent-runtime/src` on `PYTHONPATH`. Existing
raw/position/conflict/rejection views retain their independent read-only operation.
Missing runtime support produces `progress_runtime_unavailable` in source mode.

The response has `coverage: committed_interval_only` and a `page` containing
`after_export_seq`, `settled_export_seq`, `collector_epoch`, exact rejected/lost
ranges and `more`. It uses the same bounded interval reader as the live settlement
protocol: dense pages read at most 512 positions, and explicit sparse loss can
advance without expanding every lost position. A hole stops progress even if a
higher position is persisted. `more: false` means this page reader stopped; it
does not prove complete source history. No claim is made before the page base.

For continuation, pass `--after-export-seq` with the previous page's end and
`--collector-epoch` with its epoch. Start at zero after a collector restore; stale
epochs are refused. These export cursors cannot be mixed with global `--after`,
`--view` or a custom `--limit`. Unknown scopes and exhausted query budgets return
explicit errors, never fabricated empty checkpoints. The command opens the
existing database with read-only/query-only flags, performs no migration or
network call, and shares the inspection SQL budget and busy timeout.


## Apparent collection age and clock uncertainty

Core live metrics expose `last_event_collection_age_ms` with
`event_collection_age_state` (`observed`, `clock_skew`, or `unavailable`). After
a validated accepted/duplicate event has committed, the collector compares its
local commit-observation time with the immutable event's `occurred_at`. Future
event time produces null age and clock_skew. Missing/unparseable timestamps or
rejected, conflicting and quarantined dispositions produce null/unavailable and
do not retain a prior delivery's age. Valid payloads alone supply event time.
The observation remains independent of ACK success and follows the existing
service-instance metric lifetime without retaining histories or source labels.

This is apparent event age, including source buffering, import/replay delay and
clock offset. It is distinct from `last_delivery_age_ms`, which uses the broker's
publication timestamp, and from source retained queue age, which uses the local
journal receipt timestamp. No source receipt timestamp is exported by the current
wrapper, so these fields do not assert exact source-append-to-Core latency or
network-only latency. A nonnegative age does not establish synchronized clocks,
and this last-delivery sample is not a worst-case backlog age or a percentile.
The corresponding `last_commit_observed_at_ms` identifies observation freshness.

Source inspection now pairs each oldest retained age with
`oldest_retained_age_state`. Future local receipt time gives null/clock_skew,
no retained receipt gives null/unavailable, and otherwise age is observed. This
avoids silently converting a backwards local clock adjustment into zero queue
age. Existing bounded per-scope summary availability limits remain explicit.


## Resumable Core capacity accounting

When upgrading a database without capacity counters, Core scans at most 256 rows
per transaction using a persisted rowid cursor. Counts, encoded payload totals
and that cursor commit together. Evidence-table mutations are refused by SQLite
triggers until all six tracked tables finish; partial totals are never reported
as complete capacity accounting. Existing complete counters are retained.

The collector yields between batches and checks stop before subscribing to the
broker. Restart resumes the committed cursor without changing the collector epoch
or evidence identities. The synchronous initialization helper completes the same
batches for offline callers. This bounds rows and lock ownership per batch, not
filesystem latency, total upgrade time, or physical disk allocation. This is not
seven-day retention or permission to delete settled identity evidence.

Read-only Core inspection checks migration completeness in the same snapshot as
its evidence and counters. All four views return `core_accounting_unavailable`
while backfill is incomplete or its accounting keys are inconsistent. Older
complete-counter databases without a backfill table remain readable without
migration. Inspection never creates the missing migration table.


## Core payload expiry and durable identity evidence

Active collectors scan up to 256 raw-event identities per maintenance transaction,
at most once per second between fetch batches, including idle fetch periods.
Payloads become eligible after seven days from Core receipt time. Expiry clears
`event_json` and sets `payload_expired_at_ms` atomically with capacity counters and
the persisted scan cursor. Scans wrap so a previously recent row is reconsidered;
seven days is the eligibility threshold, not a precise deletion deadline.

Inspection returns `payload_state: expired`, `event: null`, the expiry timestamp
and the original hash/identity. Retained payloads report `payload_state: retained`.
Exact retries keep their original disposition, and a matching event in a new
export generation remains a duplicate without restoring expired content. Event
identity and source-position conflicts remain detectable. Settlement dispositions
and explicit loss ranges survive expiry, including when their coverage-event
payload expires. Settlement means durable acceptance/disposition, not a promise
of indefinite payload retention.

Collector status exposes the latest bounded `retention` observation and updated
storage counters. Maintenance failure reports retention unavailable and leaves
failed-transaction payloads/cursor unchanged; physical pressure can defer expiry.
Stopping collection also stops this maintenance. Expiry neither invokes task APIs
nor changes the broker protocol.

Identity, position, conflict and loss records remain subject to existing row caps;
expiry does not authorize their deletion or reset those counts. Freed SQLite pages
may be reused but are not proof of filesystem reclamation. Scanning retained
identities, permanent identity growth and pinned-reader journal pressure still
require M4 sizing/compaction qualification. Older binaries are not a supported
rollback path after this schema upgrade; use the matched restore procedure.

A controlled telemetry-only dataset of 5.04 million events and positions, with
1 KiB payloads, used approximately 10.17 GiB of observed Core file pressure.
Expiring all payloads and checkpointing left the database file unchanged at
10,888,593,408 bytes, with zero freelist pages. The current in-row expiry format
therefore does not establish physical reclamation. Its 19,688 scan batches also
imply roughly 5h28m of one-second scheduling slots. Shared task data, maximum
payloads, simultaneous readers and migration copies remain outside that fixture;
these measurements do not qualify the full physical-capacity or overhead gate.


## Checked separate-payload operations and collector adoption

`aggregator.trace_payloads` provides explicit preparation, checked writer opening,
bounded migration, mixed-layout point reads and indexed expiry. Opt-in collector
startup finishes accounting backfill, prepares this layout, and migrates in batches
before opening its NATS subscription. Stop requests yield between batches; restart
resumes the durable cursor. Preparation fences legacy trace writers.

Each migration batch retains row identities and expiry markers, verifies retained
payload hashes, compacts at most 256 rows, and commits content/counters/cursor
together. Existing expired rows remain expired. Accounting backfill includes both
inline and separated payload bytes. Writer opening verifies the layout version,
expiry index and exact guard and payload accounting trigger definitions before granting its
connection capability. A failed reopen revokes the prior capability.

The completed layout rejects inline insertion and attempts to expire raw rows
without first removing their separated payload. Mixed-layout reads report missing
or inconsistent content explicitly. Persistent guards are compatibility controls,
not protection from a privileged local schema editor.

Accepted ingestion commits the compact identity, separate payload, disposition and
capacity counters in one transaction. Exact retry after expiry does not recreate
the payload. Expiry deletes eligible payload rows through the receipt-time index
and retains identity and expiry evidence. Reopened writers validate the layout
before mutation, including connections created before adoption and restored files.
Standalone inspection resolves legacy, mixed and separated bodies using a
standard-library-only reader without migrating or loading writer runtime code.
Inspection materializes its bounded page and summary in one snapshot, then closes
the connection before decoding payload JSON, so application decoding cannot pin
the shared WAL. This does not impose a lease on external database readers.
Unsupported layouts pause collection with a fixed diagnostic.

Final-source throughput, complete writer ownership and physical worst-case
qualification remain pending; collector adoption does not establish a hard bound
on the shared database or permanent identity growth.
