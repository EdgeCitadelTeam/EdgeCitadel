# EdgeCitadel Agent Runtime

`agent-runtime/` contains agentd, the Managed Agent runtime, and repository-side
infrastructure for validating installable EdgeCitadel packages. The
`edgecitadel_supervisor` package owns safe
loading, strict schemas, compatibility checks, canonical locks, and deterministic
inventory. The `edgecitadel_plugin_runtime` package owns Agent Card, heartbeat,
durable inbox, result, and JetStream primitives. The separate
`edgecitadel_plugin_sdk` package defines typed,
framework-neutral extension seams and immutable values for future runtimes.
Installable Agent Packages live in [`../agent-packages/`](../agent-packages/), not in this
directory.

## End-user lifecycle

Newcomers do not create this environment or install the Supervisor separately.
From the repository root, the unified CLI prepares a private environment on the
first Agent command and composes validation with the host-local lifecycle:

```bash
./scripts/edgecitadel agent install ./agent-packages/examples/echo
./scripts/edgecitadel agent list
./scripts/edgecitadel agent logs edgecitadel.echo
./scripts/edgecitadel agent stop edgecitadel.echo
./scripts/edgecitadel agent start edgecitadel.echo
```

Before a managed runtime starts, agentd owns its connector, durable inbox, task
state, and process lifecycle. Managed processes receive a private local API
credential, never NATS or Leaf credentials. The lower-level commands below are
the contributor interface for package authoring and CI.

## Contributor setup

Create the environment from this directory. The editable install is required by
the current source-layout schema lookup model.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test,type]'
```

## Package commands

`lock` validates package structure and writes or regenerates
`plugin.lock.json`; it mutates the package. `validate` verifies the existing lock
without writing anything and prints a deterministic JSON inventory. Neither
command imports handlers or executes plugin runtime code.

Both console-script and module forms are supported:

```bash
edgecitadel-supervisor lock ../agent-packages/examples/echo
edgecitadel-supervisor validate ../agent-packages/examples/echo

python -m edgecitadel_supervisor lock ../agent-packages/examples/echo
python -m edgecitadel_supervisor validate ../agent-packages/examples/echo
```

Finalize every package file before running `lock`; any subsequent package byte
change requires regenerating the lock. `validate` requires the lock's exact
canonical bytes: two-space indentation, recursively sorted object keys, and one
final newline. After semantic integrity checks, it also requires those bytes to
equal the current lock generator output exactly.

## Maintained contributor gate

Run the focused checks from this directory:

```bash
python -m pytest -q
python -m compileall -q src tests
python -m edgecitadel_supervisor validate ../agent-packages/examples/echo
mypy --strict src/edgecitadel_plugin_sdk tests/typecheck_sdk_consumer.py
```

The combined extras install both pytest and the constrained mypy version used by
the typing gate.

Real filesystem-exhaustion qualification on macOS is opt-in:

```bash
RUN_AGENTD_FILESYSTEM_FULL=1 python -m pytest -q tests/agentd/test_trace_filesystem_full.py
```

It creates owned 64 MiB HFS+ disk images with system `hdiutil`, checks mount/device
and capacity before filling them to ENOSPC, and detaches them on completion. It
never fills the host filesystem. This gate must pass explicitly; its default skip
is not exhaustion evidence. It checks dispatch and retention rollback on actual
`SQLITE_FULL`, stable retry, protected active evidence, and closed optional
evidence whose loss marker/export intent must commit before deletion. Reopening
preserves the committed cleanup. A separate case verifies one optional effect
and durable loss reporting after capacity returns.

The opt-in `tests/agentd/test_trace_linux_quota.py` qualification runs only as
root on jim-eq (`RUN_AGENTD_USER_QUOTA=1` for pytest, or execute the file
directly with Python). It creates an owned ext4 user-quota fixture, drops the
writer to UID 65534 with no effective capabilities, and verifies allocation
refusal, charging of unlinked open files across writers, atomic task/slot
rollback, reserved-slot completion, pinned-reader refusal and SIGKILL recovery.
The fixture unmounts and detaches its own loop device before deleting scratch.

This establishes a Linux enforcement mechanism for UID-charged allocations,
including file/directory blocks and SQLite journals. Shared filesystem metadata
and the test backing image are separate infrastructure, explicitly outside that
user quota. Its synthetic slots complete at the fixture's database admission
ceiling; this does not prove completion at arbitrary filesystem exhaustion or
implement production reservations, migration, restore, or cross-platform support.
The existing deployed stores remain unchanged by this qualification.
The approved production contract requires an administrator-provisioned Linux
user-quota filesystem and a dedicated unprivileged service UID without quota
bypass privileges, with shared infrastructure accounted separately. Production
admission must fail closed if enforcement cannot be verified; that enforcement
integration remains outstanding.


Agentd now opens its source store in verified `journal_mode=DELETE` with
`synchronous=EXTRA`. This is the transaction-mode prerequisite for an atomic
attached task/trace split: [SQLite does not provide cross-file crash atomicity
with WAL](https://www.sqlite.org/lang_attach.html).
[EXTRA](https://www.sqlite.org/pragma.html#pragma_synchronous) also syncs the
containing directory after rollback-journal deletion. Startup converts an existing WAL store
through SQLite; an outstanding WAL reader refuses startup rather than leaving the
service in WAL mode. Initialization failures close the connection. Operator and
inspection readers must release snapshots promptly: pinned rollback readers can
block a writer commit, whose entire task/trace/export transaction then rolls back.
No sidecar is deleted manually. This does not yet split storage or implement
Linux user-quota admission checks, physical completion reservations or paired
migration/restore. Prior WAL workload measurements do not qualify the new mode.

The runtime suite includes owned child-process SIGKILL tests at journal commit
and side-effect boundaries, plus restore staging/activation boundaries. They use temporary stores and do not stop a running
agentd or any installed Agent service.

The daemon holds an OS advisory lock on `agentd/writer.lock` before opening the
store or replacing its socket/process record. A competing daemon fails without
changing the running daemon's endpoints; process exit (including SIGKILL)
releases ownership. Do not delete or replace this lock file while a writer is
running. Maintenance code must acquire the same lock before touching the store.
This guards one state directory; fresh source epochs after restore and fencing
copies in other directories remain unqualified M3 work.

A `restore-barrier.json` file blocks daemon startup before SQLite, socket or PID
changes, even if its contents are incomplete. Internal restore staging creates
this barrier in the destination and retires the previous directory behind the
same barrier after validating the copy and rotating its source epoch. Do not
remove the file to resume work. Schema 13 staging invalidates saved sessions and
holds nonterminal tasks/unpublished commands; task reads flag these as
`restore_status: reconciliation_required`. Fresh work is unaffected by these
holds. The internal activation API accepts an exact reviewed hold inventory,
records unknown coverage durably and removes only the destination barrier; it
keeps all uncertain work held. Operator commands, enrollment switching and
individual hold reconciliation are not yet implemented.

If reconciliation encounters a SQLite storage error, agentd stays available and
reports `status: degraded` with `reconciliation: storage_unavailable` in health.
It retries on the normal one-second maintenance interval and returns to `ready`
after a successful pass (provided integrity is still OK). One fixed diagnostic
is emitted per transition into this degraded state; no request content is logged.
This does not mean failed writes were retained or that the disk has spare space.

## Static guarantees and trust boundary

The scaffold rejects duplicate YAML and JSON mapping keys. Untrusted structured
documents are limited to 1 MiB, `SKILL.md` to 2 MiB, and its frontmatter to
64 KiB; parsed trees are limited to depth 64 and 100,000 traversed values. YAML
anchors or aliases that reuse a container are rejected. Validation applies
strict schemas, accepts only local-fragment (`#...`) `$ref` and `$dynamicRef`
values in skill input/output schemas, checks compatibility and agent-to-skill
mappings, and resolves declared paths within the package. Portable paths exclude
absolute paths, empty or dot components, traversal, backslashes, and every
Unicode `Cc` control character; ordinary Unicode names remain allowed.
Validation also rejects symbolic links and special filesystem nodes and uses
canonical SHA-256 hashes and ordering.

Recognized optional Agent Skills frontmatter fields are `license` (string),
`compatibility` (string, at most 500 characters), `metadata` (string-to-string
mapping), and experimental `allowed-tools` (space-separated string). Unknown
frontmatter fields remain accepted for forward compatibility. If
`metadata.version` is present, it must equal `binding.yaml` `version`; otherwise
the binding version is authoritative.

Diagnostics may include identifiers and escaped package-relative paths; an
invalid or missing root argument may report the escaped caller-supplied or
resolved root path. They do not dump procedure bodies, secret values, or complete
file contents. Validation never imports package handlers or launches the
declared runtime.

These guarantees assume the package root is owned by the supervisor and remains
immutable throughout `lock` or `validate`. They do not make validation safe
against concurrent mutation of an externally writable tree.

## SDK boundary

SDK fields that carry flexible data use JSON-shaped `Mapping[str, object]`
values. Mapping/list/tuple trees are deeply snapshotted so caller mutation cannot
change an SDK value. `TransportMessage.to_mapping()` returns an independent,
canonical envelope-shaped wire mapping; it intentionally does not validate the
envelope. Schema validation belongs to the supervisor or future host boundary.

The SDK ships a PEP 561 `py.typed` marker. Its `runtime_checkable` Protocols only
support presence checks at runtime; static type checking owns method signatures
and return types.

## Managed Agent delegation over MCP

Native-host MCP delegation automatically creates a durable observational root
on first use in each MCP session. It creates only real delegated tasks, retaining
the root as their parent run. Retry a lost response with the same JSON-RPC request
ID and unchanged arguments; IDs must otherwise be unique within the connection.
Session replacement starts a new root. This observes EdgeCitadel MCP activity,
not the host's model turns or unrelated tools.

The native `edgecitadel_trace` tool reads the durable journal through private
`trace.history`. Optional `trace_id` filters the run; `limit` is 1–32. To continue,
pass the returned `source_epoch` and `next_source_seq` as `after_source_seq`.
The response lists visible source epochs and reports partial local coverage with
reported or unknown producer loss. It contains only the authenticated Agent's metadata, not
all participants in a shared trace. Legacy `trace.get`/`trace.list` remain available
for their original span/event store.

When journal pressure causes loss-aware pruning, `coverage.local_history_pruned`
warns that this Agent has retired local history. Pending export identities remain
with explicit loss markers. The warning is actor-wide, not proof that every
requested trace lost records; coverage remains partial.

For execution-bound Hermes HTTP requests, use the packaged server launcher and
`edgecitadel-scoped` toolset described in
[`agent-packages/hermes/README.md`](../agent-packages/hermes/README.md).
It supplies per-execution MCP metadata outside model arguments. The generic
stdio configuration below alone does not attach that execution metadata.

A Managed Agent can expose the existing `edgecitadel_delegate` and
`edgecitadel_task_status` tools to its upstream model using a local stdio MCP
server. Use the installed supervisor Python and the existing Managed Agent
connector; this mode opens no execution session and exposes no inbox or task
transition tools. The adapter remains the sole executor.

```bash
/absolute/state/supervisor/bin/python -m edgecitadel_agentd.mcp \
  --state-dir /absolute/state --host-type managed-agent \
  --connector-id managed-your-agent --agent-id your-agent
```

Before use, add each recipient to the package manifest's
`permissions.messaging.outboundAgents`, regenerate its lock, and reinstall
the package through the normal permission approval flow. agentd checks the
administrator-reconciled installed package grant on each delegation; missing
grants, stopped packages, and unlisted recipients are denied. Task status is
limited to work involving the authenticated Agent. Older installed summaries
without a recipient grant fail closed until reinstalled. Hermes supports this
stdio command under `mcp_servers` in its local `config.yaml`; restart its gateway
after adding it. No NATS credentials belong in the Hermes MCP configuration.

## Non-goals

agentd owns Managed Agent process lifecycle, broker connectivity, local identity,
and task/trace persistence. The toolkit does not provide a learned-memory store,
sandbox enforcement, permission granting, package signing, or publisher
verification. It also does not support normal wheel deployment of the
validator's schema resources; schema lookup is supported only from the
source/editable layout for now.


Physical trace pressure is visible through health's `physical_storage` byte
counters. At 256 MiB of conservative shared DB/page allocation plus WAL/SHM,
optional model/tool records report `quota_exceeded`; health reports degraded with
`trace_storage: physical_pressure` when sampled usage reaches that threshold.
Task/content and free pages count, so optional tracing can stop before its JSON
quota is exhausted. Mandatory records retain existing persistence semantics.
Legacy WAL pressure diagnostics retain nonblocking checkpoint support for owned
fixtures. The production source writer now uses rollback journals; a pinned
reader can prevent its whole transaction from committing.
This guard is not a total-size cap or a physical reserve. Database VACUUM and
control/spool compaction are not performed automatically.


For explicit offline reclamation, the internal
`edgecitadel_agentd.storage_maintenance.compact_database(state_dir)` API runs
transactional VACUUM under the daemon writer lock and SQLite exclusive ownership.
It refuses active writers/readers and restore-fenced state, preserves records and
the payload key, and returns before/after byte counts. It stops no service, needs
temporary free disk space, and propagates errors. Operator command/runbook
integration remains part of the M7 recovery work.


Daemon-generated missing-terminal records use the control reserve even when the
interrupted operation is a model/tool call. If closure cannot fit the reserve,
reconciliation remains alive, exposes storage-unavailable degradation and retries
when capacity returns. Producer counters lost to process death remain unknown;
expiry closure does not invent an exact count or repeat the external effect.


Reconciliation compacts up to 256 payload-free loss spool candidates only behind
retained selected coverage ranges. Schema 17 indexes affected epoch/generation
lookup. Export ledgers may be sparse after explicit loss: assigned positions come
from generation counters, and absent rows require retained range evidence before
reconciliation. This does not retire pending payloads, change Core settlement or
bound all control/identity storage.


Active retention-loss markers can coalesce within one source/generation/actor.
The fresh replacement preserves earlier loss and covers its replaced unsettled
marker positions; settled positions retain payload-free receipts. Replacement is
atomic and must reduce encoded bytes. Other-generation references and unions over
128 ranges are retained. This does not yet bound every marker/identity scope.


With `RUN_AGENTD_NATS_INTEGRATION=1`, the suite also checks overlapping traced
roots across four owned agentd instances and an isolated loopback broker. It
observes actual commands/results and verifies scoped parentage through managed
claim/bind/tool/finish execution. This is separate from physical-host/Core/Leaf
and telemetry-export acceptance; all fixture services and the broker are owned
and stopped by the test.


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


### Dispatch admission under physical pressure

Fresh trace dispatch requests also check the shared DB/WAL/SHM pressure threshold
before creating permission/dispatch evidence, retry receipts or child tasks.
This includes denied decisions. Authorized exact retries return their retained
receipt before this check; conflicting retries still fail. A fresh request gets
`quota_exceeded` at the watermark or fixed `storage_unavailable` if measurement
fails. Binding closure remains available under its existing control quota.
This closes new dispatch admission under pressure; it does not reserve physical
bytes for an admitted transaction or its future lifecycle.

### Settled spool retirement (schema 22)

Maintenance inspects at most 256 payload-free settled positions using a partial
index. A volatile keyset cursor advances after each committed batch, including
fenced rows, and resets at the end of the scan or on daemon restart. This lets
later candidates pass an unchanged fenced prefix; each position is rechecked
when visited. A position is removed only when its collector epoch matches the durable
source settlement checkpoint, its sequence is covered, and collector recovery
is absent or live. Retained payload mappings stay available for replay. During
Core replacement recovery, retired positions become explicit loss ranges; the
old checkpoint does not establish settlement in the new collector epoch.

The migration adds the partial index without deleting evidence. Building it may
scan existing spool history, so large-database upgrade latency remains a
qualification requirement. Retirement frees reusable SQLite pages; it does not
shrink the file, bound WAL growth or establish the full physical storage cap.

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

Fresh producer-loss reports and new anonymous-audit minute records now check
the shared DB/WAL/SHM physical-pressure watermark before adding persistent
control evidence. Existing authorized loss-report retries and existing minute
receipts resolve first. Rejection preserves the producer's pending report or
the recorder's bounded volatile count; it does not claim durable coverage.
Filesystem measurement failures return fixed `storage_unavailable`. Existing
binding closure remains governed by its control quota. These admission guards
do not reserve all physical bytes required by a future lifecycle or checkpoint.

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


## Telemetry provisioning primitive

`edgecitadel_plugin_runtime.telemetry_stream` provides Core-only stream and pull
consumer provisioning with explicit drift refusal, wired into the opt-in Core collector.
Versioned initial limits and remaining deployment qualifications are recorded
in `docs/architecture/execution-trace-contract.md` under M4 telemetry provisioning.
Owned broker tests in `tests/runtime/test_telemetry_stream.py` use the existing
`RUN_AGENTD_NATS_INTEGRATION=1` opt-in and do not touch shared NATS services.

The collector-outage fixture uses two owned Leaves and ten local task agents,
with 50 exported events per task at 600 tasks/hour. Its six-second rehearsal runs
with the ordinary NATS opt-in; the ten-minute wall-clock case needs an additional
explicit opt-in (allow roughly twelve minutes with normal settlement timers):

```bash
RUN_AGENTD_NATS_INTEGRATION=1 RUN_TRACE_OUTAGE_QUALIFICATION=1 \
  python -m pytest -q tests/runtime/test_telemetry_collector_outage.py -k 600
```

The test checks continuing task completion, catch-up within five minutes, and
exact source/Core event identities, hashes and payloads. Tool metadata is synthetic;
it does not qualify live adapters, cross-Edge task routing or hard storage bounds.

`tests/runtime/test_telemetry_leaf_poison.py` uses the ordinary NATS opt-in to
exercise failed poison persistence, valid-event continuation, unsupported payload
rejection and collector restart through real Leaves. It checks exact rejection
identity/hash, correlated settlement, and absence of the injected private sentinel
from Core SQL state. The injected wire payload remains broker data; this is not a
whole-pipeline secret-disclosure or hostile-fleet isolation test.

`tests/runtime/test_telemetry_leaf_saturation.py` uses the same NATS opt-in and the
unmodified 128 MiB telemetry quota. It fills an owned stream, completes ten
cross-Leaf command/result tasks while source telemetry is refused, then removes
only synthetic filler and verifies exact retained-event/export replay. It samples
combined Core/Leaf broker file allocation; source/Core SQLite hard bounds and
sustained workload performance remain separate acceptance gates.

`tests/runtime/test_telemetry_leaf_restore.py` restores an older prepared Core
snapshot while retaining the same broker and two Leaf links. Both retained and
missing-payload cases use normal timers, compare exact payloads/loss scope, and
reject stale settlement pages without mutation. Replay must finish inside the
broker duplicate window. These graceful restore cases take roughly two minutes;
crash cutover, source restore and external effects remain separate gates.

`tests/runtime/test_telemetry_leaf_crash.py` exercises five real SIGKILL boundaries
through a Leaf using the current Core WAL/separated-payload layout. It reuses the
backend crash harness and checks immutable replay, consumer redelivery and durable
settlement. These are helper-level child-process kills, not a full daemon or
remote-settlement-responder crash qualification. The backend regression uses
RUN_JETSTREAM_INTEGRATION=1 and its owned Docker broker.


## Experimental source telemetry synchronization

Source synchronization is disabled by default. Set `EDGECITADEL_TRACE_SYNC=1` in
an agentd process environment to start it; agentd does not read the repository
`.env` file. This is a development opt-in. Enable the Core separately with `EDGECITADEL_TRACE_COLLECTOR=1` in the Aggregator
environment; raw Compose forwards it. The M4 network, outage, saturation and
physical-storage acceptance gates are unfinished.

When enabled, agentd creates a separate telemetry thread/event loop, SQLite handle
on its existing database, and authenticated NATS connection using validated
`node.json` endpoint state. Publishing and settlement use subjects routed to Core;
command transport retains its own local-domain selection. Only Core provisions the
telemetry stream and consumer. Direct source startup verifies the stream read-only
and retries if it is absent. Leaf-mode startup skips JetStream management lookup:
domainless management requests resolve at the Leaf and cannot verify Core placement.
Core still verifies its stream policy before collection. Sources run bounded scope
scheduling and retain payloads until Core settlement.

Agentd `health` includes `telemetry` with enabled/state/connected/active_scopes/fault.
Faults are fixed codes, without endpoint credentials or exception text. Disabled
startup creates no telemetry thread, connection or database handle. Service shutdown
joins telemetry work and closes only its own resources. Internally the service can
stop/restart independently. Administrator source controls are described below;
full live queue/lag/failure metrics remain incomplete. Persisted per-scope faults
are not cleared by an ordinary restart.

Separate handles avoid sharing the command event loop or Python store lock. SQLite
write locks and physical disk capacity are still shared, so this is not proof of the
execution-overhead or storage-pressure gates. Successful broker ACKs continue to
retain source payloads until actual Core settlement; absent Core service causes
retry/replay, never task execution or invented settlement.

## Read-only source telemetry inspection

With the runtime installed in the selected Python environment, run
`python -m edgecitadel_agentd.trace_inspect /absolute/path/to/agentd.sqlite3`.
For a source-tree checkout, set `PYTHONPATH` to the absolute `agent-runtime/src`
directory. The command opens an existing database read-only; it does not initialize,
migrate, replay, settle, or contact a broker. Local filesystem permissions govern
access to the sanitized raw records; this is a host-operator command, not a
connector-authorized API.

The first response lists at most 32 export scopes. Continue with
`--after-scope NODE EPOCH GENERATION` using `next_scope`, or inspect a scope with
`--scope NODE EPOCH GENERATION`. Scope inspection returns up to 32 spool records
and their retained raw events, including coverage/loss markers. Continue with
`--after N` using `next_export_seq`; `--limit` accepts 1–32. Positions compacted
from the spool are absent: neither a missing position nor the end of a page proves
complete history. Retained markers report explicit loss; otherwise coverage stays
`partial_local_evidence`. The page and summary are materialized from one snapshot;
the read transaction ends before event JSON decoding, so decoding does not pin
database pages or block a rollback-journal writer.

The response separates generation allocation, durable source settlement, collector
recovery phase, and per-record pending/broker_acked/core_settled/lost_with_marker
states. A settlement retained during collector recovery must be interpreted together
with the recovery phase; it does not prove acceptance by the replacement Core.
The full-scope summary reports stored positions, retained canonical payload bytes,
and oldest retained receipt age per state. Each age has an
`oldest_retained_age_state`: `observed`, `clock_skew` (the oldest receipt is in
the future), or `unavailable` (no retained receipt). A future receipt returns
null age rather than a misleading zero. These are not wire bytes, physical disk
usage, publish latency, or collector ingest latency. Missing states have no rows.

Reads use one short SQLite snapshot, a 50ms busy timeout, and progress checks every
1,000 VM steps with a 100,000-step/50ms budget. Record reads and full-scope statistics
have separate budgets; these are cooperative query limits, not a hard wall-clock
SLA. If statistics exceed their budget, the record page remains available and the
summary reports unavailable, with no partial aggregate. Top-level failures return
fixed JSON error codes and exit nonzero. This read-only command does not clear faults or control export. Source lifecycle
controls are described below; Core inspection is documented in the tracked trace
contract. Live publish-failure counters and full collection-lag metrics remain open.

## Administrator source telemetry controls

Run `python -m edgecitadel_agentd.trace_control --state-dir /absolute/path/to/agentd stop`
with the runtime installed, or the checkout `agent-runtime/src` on PYTHONPATH.
The directory must be the service directory containing `admin.token`, not the
parent node directory. The command reads that private token and uses the existing
Unix socket; no token is printed or placed in command arguments. CLI failures return
a fixed JSON diagnostic. Detailed fixed operation errors are available through the
administrator AgentdClient call `trace.sync.control`.

`stop` cancels and joins source telemetry work without stopping task transport or
changing retained journal/spool/settlement data. `start` starts the configured
telemetry service asynchronously; it does not clear persisted scope faults. Both
are idempotent. Start cannot override the daemon's disabled-by-default process
configuration: `EDGECITADEL_TRACE_SYNC=1` must already be set at daemon startup.

After diagnosing a paused scope, run `retry --scope NODE EPOCH GENERATION`. This
validates the scope, stops and joins all source telemetry workers, clears only that
scope's durable fault, then starts telemetry again. Other scope faults remain.
The temporary restart affects other telemetry workers on the same daemon, but
never command transport; persisted evidence and ordinary replay identity remain.
A retry does not repair invalid data or configuration and can pause again.

These controls are serialized with daemon lifecycle operations. Terminal shutdown
refuses later starts. Stop is a process-lifetime choice: a new daemon process
follows its environment setting again. No deployment configuration is changed.
Read-only source inspection now reports `sync_fault` per listed/selected generation.
The private RPC requires the administrator token; connector credentials and
unauthenticated health access do not authorize control. Parameters are `action`
(stop/start/retry), plus a three-string `scope` list only for retry. Core collector controls are documented in the tracked trace contract; full live
operational metrics remain separate work.

## Source publication and settlement observations

Agentd health's telemetry object now includes `metrics` with
`lifetime: service_instance`, fixed `counts`, and fixed `last_observed_at_ms` maps.
One object is shared across rotating scopes, replacement publishers/pollers and
administrator stop/start/retry. A new daemon/service object resets observations.
Counters saturate at 2^53−1. No per-scope labels, sample history, payloads, credentials
or extra SQLite writes are retained. Snapshot callers cannot mutate the counters.

`publish_attempts` counts actual broker publish calls; `publish_failures` counts
NATS/API/OS/timeout errors from those calls. Selection or validation failures before
a call are represented by existing fault status, not invented network attempts.
`broker_acknowledgments` counts ACKs naming the expected telemetry stream; a wrong
stream increments `invalid_broker_acknowledgments`. A broker ACK followed by failed
local checkpointing also increments `broker_ack_checkpoint_failures`. Neither ACK
counter means Core acceptance, and replay may count the same record again.
Cancellation or a process crash between boundaries can leave attempts without a
recorded success/failure; these counters are not an accounting identity.

`settlement_requests` and `settlement_request_failures` count control requests and
transport failures, respectively. A valid error reply is not a transport failure.
`settlement_page_observations` counts successful returns from durable page application,
including duplicate/stale/no-progress outcomes; it is not a count of newly settled
records. Read-only source inspection remains authoritative for applied positions,
collector recovery, queued bytes/receipt age, retained loss markers and scope faults.

Timestamps are local wall-clock observation times, not durations or latency. These
volatile service observations do not replace durable evidence, and do not complete
fleet-wide queue/lag metrics or execution-overhead/storage acceptance.


### Broad local read budgets

`task.list`, legacy `trace.list`, and legacy `trace.get` bound SQL work to
100,000 VM steps or a 50-ms elapsed progress check, sampled every 1,000 steps,
with a 50-ms SQLite busy timeout during the read. Budget exhaustion returns
`operation_failed` with `read_query_budget_exceeded`; lock contention returns
`read_query_unavailable`. Neither returns a truncated successful result. The
progress handler is removed and the previous busy timeout restored under the
store lock before other operations use the connection. These are SQL-work
limits, not an end-to-end deadline: mutex waits, scheduling, filesystem stalls
and result decoding are outside that guarantee. Large legacy reads may now
fail explicitly rather than monopolize the shared connection.


### Named-file physical accounting

Physical pressure sums all attached database schemas, pending page allocations,
and their named WAL, SHM and rollback-journal files. File contributions use the
larger of logical length and allocated filesystem blocks. Health adds
`rollback_journal_file_bytes` and `filesystem_allocated_bytes`; existing byte
fields now aggregate attached stores. Reusable pages and legacy task content
remain conservatively included. This measurement does not include super-journals,
unlinked temporary files or future transaction growth and is not a hard quota
or an attribution of only telemetry-owned bytes.

For legacy WAL fixtures, reconciliation attempts a nonblocking checkpoint at
physical pressure before cache maintenance. If a reader or another checkpoint
blocks reclamation, health
reports `cache_maintenance: checkpoint_blocked` and defers cache writes until a
later pass can checkpoint. Task/session recovery still commits before this check.
The daemon neither evicts the reader nor spends more WAL space deleting cache
records while reclamation is blocked.

### Disposable development traces

For an isolated development/test source, set `EDGECITADEL_TRACE_TEST_RUN_ID` to a
UUIDv4 before starting agentd. The source must not have emitted events yet; a
restart may reuse the same ID. Use a fresh isolated state directory for another
run. The daemon persists this provenance and stamps it into authenticated trace
exports; RPC callers cannot set or change it. Unclassified sources retain normal
retention, even on development machines.

Eligible test payloads take cleanup priority over ordinary payloads. Source test
payloads become age-eligible after 24 hours; ordinary local history retains its
30-day policy. Active tasks/open executions and unsettled mandatory evidence stay
protected. Settled-first cleanup requires the current durable Core checkpoint;
a broker ACK alone does not qualify. Core prioritizes eligible test payloads
within its existing seven-day retention and preserves identity/settlement rows.
Cleanup remains bounded and may leave protected test data in place.

This improves reclamation, but does not establish the strict physical-storage
limit or implement completion-space reservations. It does not delete abandoned
state directories automatically. Owned test fixtures must stop services and clean
up their own directories; never manually remove a live SQLite journal.

Real Core/Leaf cleanup qualification:

```bash
RUN_AGENTD_NATS_INTEGRATION=1 PYTHONPATH=.:agent-runtime/src \
  agent-runtime/.venv/bin/python -m pytest -q \
  agent-runtime/tests/runtime/test_telemetry_test_retention.py
```

### Experimental Core trace reads

Set `EDGECITADEL_TRACE_READS=1` alongside `EDGECITADEL_TRACE_COLLECTOR=1`
in the Core Compose environment to start the projector and mount `/api/traces`
and `/ws/traces/{trace_id}`. Set a separate `EDGECITADEL_TRACE_READ_TOKEN` to
32–256 URL-safe characters (generate with `secrets.token_urlsafe(32)`); do not
reuse the NATS or administrator credential. Set `EDGECITADEL_TRACE_ORIGINS` to a
JSON array of exact dashboard origins, such as `["https://core.example.com"]`.
The empty array allows native clients without Origin, but no browser origin.
Invalid enabled configuration fails startup before connecting services.

HTTP clients send `Authorization: Bearer TOKEN`. WebSocket clients send
`{"type":"authenticate","token":"TOKEN"}` as their first text frame within
five seconds; put the durable replay cursor in `after`, never the credential.
Use the trusted private/encrypted deployment perimeter: this credential grants
fleet-wide trace access, and does not isolate individual users. The dashboard's
Execution tab accepts this credential in memory, and Flow/Tasks link into the
explorer. Reloading the page requires entering it again. Saved run/step/event
and historical snapshot links contain no credential. Browse retained history
loads server versions, including snapshots never visited in the browser. This
supports exact retained playback; full M6 scenario/performance acceptance remains
incomplete.

`GET /api/traces` accepts an optional UUIDv4 `task_id` filter matching any
observed task in the run, not only its root task. The normalized filter is part
of the list cursor scope. `GET /api/traces/{id}/events` accepts an optional
canonical graph `node_id`; the server derives matching observation identities
with the projector's rules. Supply the displayed graph's `as_of` on every page.
Filtered scans are bounded and can return an empty page with a continuation;
continue until `next_cursor` is null. Event cursors are bound to the node filter
and cannot be reused for another step or an unfiltered request.

`GET /api/traces/{id}/history?cursor=...&limit=...` discovers snapshots newest
first. A request scans at most 64 retained Core clocks and returns at most 100
short entries (default 20); a sparse page may have no entries and a continuation.
Each page includes the current upper boundary, relevant run observation/coverage
changes and, if present, its retained base. Retired/absent boundaries carry no
`at`; present entries carry the exact graph cursor. The `received_at_ms` clock is
Core receipt/maintenance time, not source execution time. History cursors freeze
the browse ceiling and retention floor; compaction invalidates an old range with
`history_expired` rather than silently skipping it. Rebuild and credential scope
changes use the existing explicit error contract. Refresh starts a new range.
These response/scan bounds are not a fleet-scale SQL latency qualification.

The Core creates `trace-cursor.key` beside its database, owned by the process
with mode 0600. Preserve it in private backups to retain cursor validity across
restart; invalid existing keys fail closed. Rotating the read credential changes
cursor scope and requires a fresh snapshot. `/api/system/status` reports
`trace_reads.enabled` and sanitized `trace_projection` worker health. A worker
waiting for collector initialization or requiring a rebuild does not imply
readiness; read requests return explicit unavailable errors until ready.

The packaged server uses the websockets transport with a 64 KiB incoming-message
limit, four queued incoming messages per connection and compression disabled.
This transport bound applies to all WebSocket routes (including terminal input);
clients must split larger input. Trace authentication has a stricter 512-character
application bound, eight admitted sockets and four concurrent read workers.
The nginx trace path preserves the URI and disables buffering. Shutdown drains
readers before stopping projector, collector, memory and command services.
These opt-in interfaces do not establish full M4–M7 acceptance or hard storage
limits; retained-volume, network-perimeter and full scenario qualification remain.
