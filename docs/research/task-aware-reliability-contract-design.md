# EdgeCitadel Task-Aware Reliability Artifact Design

**Status:** Design direction approved; written specification v0.1 pending user review

**Date:** 2026-07-25

**Target:** EuroSys-style systems paper and reproducible artifact

## 1. Decision

EdgeCitadel will be developed and evaluated as a task-aware reliability contract for
edge agents, not as a general broker benchmark or a claim that one transport is
universally superior.

The working paper framing is:

> EdgeCitadel separates durable task-bearing traffic from ephemeral progress and
> liveness traffic, then adds stable task identity, explicit acknowledgement,
> bounded duplicate suppression, terminal-state invariants, and explicit failure
> outcomes at the agent boundary.

The implementation is divided into four ordered, independently testable slices:

1. A hermetic experiment spine with executable controls.
2. A deterministic end-to-end operator journey with frontend/backend repairs and
   captured evidence.
3. A reproducible controller and node setup for a small multi-agent IoT testbed.
4. A concise documentation set that distinguishes current behavior, experimental
   evidence, proposed work, and history.

Each slice gets its own implementation plan and verification gate. A later slice
may consume artifacts from an earlier slice, but it must not silently change the
earlier slice's measurement contract.

### 1.1 Requirement Status

This document is authoritative for the proposed research mechanism and evaluation
contract. It does not make proposed behavior current merely by describing it.

| Status | Meaning |
| --- | --- |
| Current | Present in the repository and covered by an existing test |
| Proposed | Required by this design but not yet implemented |
| Verified | Implemented and passed the named verification gate |
| Measured | Verified and supported by a valid publication campaign |

Current behavior includes canonical envelopes, schema validation, durable
per-agent inboxes, explicit consumer acknowledgement, durable terminal-result
publication to the sender's inbox, ephemeral progress/liveness, and best-effort
outbox audit mirrors.

Proposed behavior includes the transport-neutral experiment interface, durable
outcome ledger, idempotent audit persistence, deterministic task-state reducer,
four executable modes, failure-injection controls, artifact manifest/checker,
resource sampler, isolated operator journey, and lab launchers.

Implementation plans must assign a requirement ID to every proposed behavior and
advance its status only with a linked test or evidence path.

The initial traceability anchors are:

| ID | Proposed requirement | Initial evidence owner |
| --- | --- | --- |
| R-01 | Transport-neutral `TaskExecutor` and durable `OutcomeStore` | Common adapter unit/integration tests |
| R-02 | Legal state reducer and idempotent audit persistence | Backend and frontend reducer tests |
| R-03 | Four executable `TaskTransport` modes | Mode contract and broker-backed tests |
| R-04 | W6 wire retry, semantic retry, collision, and ablations | Duplicate workload raw records |
| R-05 | W5 crash-boundary recovery and W8 side-effect accounting | Crash workload raw records |
| R-06 | Manifest, raw evidence, hashes, checker, and deterministic analysis | Artifact checker fixtures |
| R-07 | Comparable resource measurement and statistical protocol | Metrics calibration and analysis tests |
| R-08 | Isolated deterministic operator journey and evidence bundle | Playwright evidence manifest |
| R-09 | Isolated controller and multi-node lab launchers | Lab lifecycle integration tests |
| R-10 | Current/experimental/proposed/historical documentation split | Documentation checks |

## 2. Research Scope

### 2.1 Hypothesis

For edge-agent workloads, applying durable delivery selectively to task-bearing
messages can improve task-level correctness during disconnection, duplication,
worker failure, and broker restart while avoiding the cost of making progress,
liveness, and other transient traffic durable.

### 2.2 Research Questions

**RQ1: Correctness.** Under offline delivery, duplicate publication,
crash-before-acknowledgement, broker restart, and non-idempotent side effects, how
often does each design produce one logical-task execution and one valid terminal
result?

**RQ2: Cost.** What latency, tail latency, CPU, memory, broker storage, wire-byte,
and transient-progress-loss costs accompany the additional correctness?

**RQ3: Agent specificity.** Under the same faults, how often are task identity,
context/parent linkage, and terminal-state invariants preserved, and which
application failures remain invisible to broker-level delivery metrics?

### 2.3 Reliability Model

Transport acceptance is not task completion. The artifact distinguishes:

1. **API accepted:** the controller accepted the request.
2. **Durably stored:** JetStream returned a publication acknowledgement.
3. **Delivered:** a named durable consumer received the envelope.
4. **Processing:** the worker started the handler and owns an acknowledgement
   lease.
5. **Terminal stored:** the sender's durable inbox accepted a terminal result.
6. **Acknowledged:** the worker acknowledged the inbound task only after terminal
   storage.
7. **Observed:** the sender consumed and validated the terminal result.
8. **Audited:** best-effort mirrors reached the aggregator database and UI.

Only `completed`, `failed`, `canceled`, and `rejected` are terminal. The legal
sender-observed transitions are:

| Current state | Legal next states |
| --- | --- |
| none | `submitted` |
| `submitted` | `working`, `input-required`, `auth-required`, or any terminal |
| `working` | `working`, `input-required`, `auth-required`, or any terminal |
| `input-required` | `working` or any terminal |
| `auth-required` | `working` or any terminal |
| any terminal | The same logical terminal outcome only |

The sender assigns a per-task observation index when it receives an event.
Untrusted envelope timestamps never order state. For the dashboard, the database
insertion sequence is the observation index. For a direct durable observer, the
local receive sequence is the observation index and broker stream sequence is
recorded when available. A terminal state always dominates a later-arriving
nonterminal event. Among legal nonterminal events, the greatest observation index
wins.

One logical terminal outcome is identified by `(sender_id, recipient_id, task_id,
request_fingerprint, terminal_state, canonical_terminal_payload_hash)`. It may
have multiple envelope IDs, publication attempts, or wire deliveries. A repeated
terminal with the same logical identity and content is idempotent. A second
terminal with different state or payload hash is a contract violation and remains
visible in raw evidence. RQ1 counts logical terminal outcomes and reports distinct
terminal envelope IDs, publication attempts, and wire deliveries separately.

Cancellation is accepted only while the sender has not observed a terminal
outcome. The first valid terminal outcome observed after a cancellation race wins;
a later conflicting terminal is recorded as a violation. The initial artifact does
not synthesize terminal failures from heartbeat staleness or delivery exhaustion,
because doing so cannot safely fence a slow worker. A timeout or exhausted delivery
is an explicit trial failure, not a forged worker result. Existing watchdog
synthesis is outside the publication mechanism and receives a separate functional
test only.

Proposed aggregator audit storage must be idempotent on envelope `id`. Replayed
mirrors increment an observable duplicate counter but do not create another
operator-visible message or regress the task state.

The proposed `TaskExecutor` protocol is transport-neutral:

1. Validate the envelope and compute a canonical request fingerprint.
2. Check a bounded durable task-outcome ledger.
3. If an identical terminal outcome is cached, republish it without executing the
   handler.
4. If the task key exists with a different sender, recipient, type, context, or
   payload fingerprint, publish `rejected` with `task_id_collision`.
5. Otherwise execute the handler while the mode-specific receiver maintains any
   available acknowledgement lease.
6. Persist the prepared terminal outcome in the ledger.
7. Publish the terminal result through the mode-specific transport and mark its
   publication acknowledgement when that mode provides one.
8. Commit or acknowledge the inbound delivery when that mode provides one.

The outcome key is `(worker_agent_id, task_id)`, and its stored record owns one
`sender_id`. A second sender or fingerprint for that key is a collision rather than
a separate task. The request fingerprint is SHA-256 over canonical JSON containing
envelope type, sender, recipient,
`task_id`, `context_id`, `hop_count`, and payload. The ledger records the inbound
envelope `id`, fingerprint, prepared terminal envelope, publish state, and
completion time.

`OutcomeStore` will be defined in `adapters/_common/outcome_store.py`. The deterministic
fixture uses a run-owned SQLite implementation. It performs no eviction during an
experiment. Outside a run, its configured retention must be at least:

```text
max(stream max_age, maximum retry horizon, maximum task deadline)
+ duplicate_window + one hour
```

The manifest records all terms. A late semantic retry after broker deduplication
expires but before ledger retention expires is a required test.

This protocol limits duplicate handler execution across redelivery, but it does not
provide general exactly-once side effects. An external non-idempotent actuator can
commit immediately before the worker crashes and before the outcome ledger
commits. The artifact measures this boundary and requires application-level
idempotency keys where exactly-once effects are needed.

Crash injection covers these boundaries:

| Point | Expected recovery |
| --- | --- |
| After receive, before handler | Durable modes redeliver; no side effect occurred |
| After external side effect, before ledger prepare | A duplicate side effect is possible and must be counted |
| After ledger prepare, before result publish | Redelivery republishes the cached terminal without handler execution |
| After terminal publish returns, before publish-state mark | Durable modes have a PubAck; Core/relay record send/HTTP return. A retry reuses the same terminal ID |
| After publish-state mark, before inbound commit | Durable modes delay ack; relay delays its response; Core records this boundary as not applicable |
| During handler-exception conversion | The same prepare, publish, and acknowledgement rules apply to the `failed` outcome |

Malformed JSON or a schema-invalid envelope is terminated and recorded as poison;
it cannot reliably produce a result because sender/task identity may be invalid.
Only a well-formed request rejected by capability, sender, hop, cancellation, or
collision policy produces `rejected`.

Correlation semantics are:

- `id` identifies one wire envelope and is the `Nats-Msg-Id` deduplication key.
- `task_id` identifies one logical task at a worker for the ledger-retention
  interval; UUIDv4 generation makes accidental reuse unlikely, while W6c verifies
  explicit collision rejection.
- `context_id` groups related tasks.
- A delegated child receives a fresh UUIDv4 `task_id`, repeats the parent's
  `context_id`, sets `hop_count = parent.hop_count + 1`, and carries the parent's
  UUIDv4 in `payload.parent_task_id`. Its terminal result repeats
  `payload.parent_task_id`.
- Run and trial IDs live in harness observations and manifests, not in production
  envelope fields.

`schemas/task-correlation.v1.json` defines and validates the direct/delegated
correlation payload rules. The common validator applies it to delegation envelopes
and delegated results; the unconstrained base `payload` object is not sufficient
evidence by itself.

Failure outcomes are explicit and attributable. Well-formed policy failures
produce `rejected`, handler failures produce `failed`, and accepted cancellation
produces `canceled`. Poison, timeout, delivery exhaustion, and infrastructure
failure remain distinct artifact outcomes rather than being rewritten as agent
results.

### 2.4 Evaluated Modes

The artifact must execute, rather than merely describe, these modes:

| Mode | Description | Purpose |
| --- | --- | --- |
| Central relay | HTTP relay with a durable central SQLite lease queue, but no durable agent-local inbox | Strong executable centralized control |
| Core-only | Plain NATS subjects for task and transient traffic | Lowest-overhead transport control |
| EdgeCitadel | Durable task inbox/result path plus ephemeral progress/liveness | Proposed split-plane design |
| All-durable | JetStream-backed task, result, progress, and liveness traffic | Reliability-at-all-costs control |

`scripts/research/modes/base.py` will define the transport-neutral `TaskTransport`
interface: start receiver, submit task, publish progress, publish terminal, observe
terminal, commit inbound delivery, inspect transport state, and close. Mode
implementations live in:

- `scripts/research/modes/central_relay.py`
- `scripts/research/modes/core_nats.py`
- `scripts/research/modes/edgecitadel.py`
- `scripts/research/modes/all_durable.py`

Central relay and Core-only must not use `PullConsumer`. They adapt their HTTP and
plain-NATS receivers to the same `TaskExecutor`. EdgeCitadel and all-durable must
use the production `PullConsumer` adapter. `PullConsumer` receives injected
terminal/progress publisher hooks so all-durable does not inherit the current
plain-NATS progress path. The sender observer likewise uses the mode-specific
result path.

All primary modes use the same `TaskExecutor`, `OutcomeStore`, logical workload,
task identifiers, payload bytes, timeout policy, worker process, and measurement
hooks. This holds application-level deduplication and terminal semantics constant
while transport durability changes. Transport-specific fields such as PubAck,
consumer sequence, or delivery count are nullable by design.

Mode acceptance and delivery semantics are fixed:

- **Central relay:** acceptance follows a committed SQLite task row. An offline
  worker later leases the row by HTTP long poll; terminal result storage and lease
  completion are transactional. Relay process restart reopens the same run-owned
  database.
- **Core-only:** acceptance follows a plain-NATS publish plus connection flush.
  There is no server-side queue or replay for a disconnected subscriber.
- **EdgeCitadel:** acceptance follows a JetStream PubAck for command/result. Progress
  and liveness remain plain NATS.
- **All-durable:** acceptance follows a JetStream PubAck for command, result,
  progress, and liveness streams.

The runner initiates every logical request exactly once except in W6. It does not
add a cross-mode client retry loop. Each mode may use only the queue, lease,
redelivery, and acknowledgement behavior declared above. The primary correctness
denominator is all initiated logical requests, not only transport-accepted or
completed requests; acceptance, completion, and timeout rates are reported
separately.

Task-bearing JetStream configuration is fixed for EdgeCitadel and all-durable:

- Stream `AGENT_INBOX`, subject `agents.*.inbox`, WorkQueue retention, file
  storage, 24-hour max age, 1 GiB max bytes, 1 MiB max message, discard-new, and a
  five-minute duplicate window.
- One run-unique consumer per agent, exact inbox filter, explicit acknowledgement,
  30-second ack wait, three maximum deliveries, and one maximum ack-pending task.

EdgeCitadel publishes `agents.<id>.task_progress.<task_id>`,
`agents.<id>.heartbeat`, and `agents.<id>.status` only through Core NATS. Preflight
must prove no JetStream stream captures those subjects.

All-durable adds stream `TRANSIENT_EVENTS` with subjects
`agents.*.task_progress.>`, `agents.*.heartbeat`, and `agents.*.status`; Limits
retention; file storage; one-hour max age; 1 GiB max bytes; 1 MiB max message;
discard-old; and a five-minute duplicate window. Its run-unique observer consumer
uses explicit acknowledgement, 30-second ack wait, three maximum deliveries, and
256 maximum ack-pending events. Publishers await PubAck for every transient event.
Preflight records and compares the complete stream and consumer configurations.

All modes generate one heartbeat per second from idle-baseline start through the
active-window end. W3 generates exactly 20 progress envelopes at 50 ms intervals
with a 256-byte application payload. A dedicated progress observer consumes five
frames, disconnects for 500 ms while the worker emits ten frames, then reconnects
for the final five; the terminal-result observer remains connected throughout.
Generated, live-delivered, replay-delivered, and missing progress counts are
recorded. This schedule is the required transient-loss/backpressure treatment; a
different schedule is a different cell.

Duplicate attribution uses three EdgeCitadel ablations for the duplicate and crash
workloads:

| Ablation | Broker `Nats-Msg-Id` | Outcome ledger |
| --- | --- | --- |
| none | Disabled | Disabled |
| broker-only | Enabled | Disabled |
| full-contract | Enabled | Enabled |

The four-mode comparison answers the selective-durability question. The ablation
answers which duplicate outcomes come from broker deduplication versus the task
contract. A required cell that fails preflight or execution invalidates its entire
predeclared comparison; it is never silently omitted from a figure.

### 2.5 Workloads

The initial paper artifact covers this predeclared matrix:

| ID | Workload | Central relay | Core-only | EdgeCitadel | All-durable |
| --- | --- | --- | --- | --- | --- |
| W1 | Deterministic command and terminal result | Required | Required | Required | Required |
| W2 | One-hop delegation with a fresh child task ID | Required | Required | Required | Required |
| W3 | Fixed-count progress followed by terminal result | Required | Required | Required | Required |
| W4 | Submit while worker disconnected, then reconnect | Required | Required | Required | Required |
| W5 | Crash at each acknowledgement/ledger boundary | Required | Required | Required | Required |
| W6a | Wire retry: byte-identical envelope, same `id` and `task_id` | Required | Required | Required plus ablations | Required |
| W6b | Semantic retry: new `id`, same `task_id` and request | Required | Required | Required plus ablations | Required |
| W6c | Collision: new `id`, same `task_id`, changed sender or payload | Required | Required | Required | Required |
| W7 | Restart the mode's task-transport coordinator with work in flight | Required | Required | Required | Required |
| W8 | Fake non-idempotent actuator with crash after side effect | Required | Required | Required plus ablations | Required |

In JetStream modes, W6b waits 301 seconds after observing the first terminal
before submitting its new-envelope retry: this exceeds the configured five-minute
broker duplicate window while remaining inside the one-hour outcome-ledger
retention. Its predeclared timeout is therefore 330 seconds. W7 has a 35-second
budget: JetStream retains an in-flight explicit-ack delivery for its 30-second
acknowledgement window across broker replacement, and the remaining margin covers
the restarted worker's redelivery and terminal publication. All other initial
workloads retain the 30-second timeout unless a later campaign revision changes
the contract.

For W4, the benchmark submits through `TaskTransport`, not the production operator
HTTP endpoint. The operator API's 409 response for an already-declared-offline
agent remains current product behavior and receives a separate API test.

For W7, central relay restarts its relay process; the three NATS modes restart the
broker. The report names these faults separately and compares recovery outcomes
rather than pretending they are the same implementation event. The restart occurs
after transport acceptance while the worker is disconnected. Process/container
state is replaced, but the central SQLite file and JetStream volume are retained;
no volume is deleted during the fault. The host-owned actuator waits for a
runner request in the per-run control mount, force-recreates only that declared
service, and acknowledges completion before the runner reconnects.

Model inference latency is not part of the transport mechanism evaluation.
Deterministic workers isolate messaging behavior. A model-backed demonstration may
be captured separately and must be labeled as a demonstration.

### 2.6 Non-Goals

This design does not claim or implement:

- A universal NATS-versus-MQTT-versus-HTTP performance ranking.
- A general exactly-once delivery or side-effect guarantee.
- Multi-hop delegation, L3 autonomous planning, or semantic agent quality.
- Production-complete MQTT firmware or constrained-device benchmarking.
- A2A or MCP protocol conformance.
- Dynamic authorization, zero-trust fleet identity, or production secret rotation.
- A production fleet installer, upgrade system, or Internet-facing deployment.
- Wide-area leaf-node topologies.
- Temporal world-model security evaluation.
- Model quality, energy efficiency, or inference benchmarking.
- A broad dashboard redesign.

These are follow-on directions. They must not appear as measured contributions in
the initial paper.

## 3. Claim and Evidence Contract

Every paper claim must map to an executable workload, recorded raw observations,
an analysis function, and a named artifact output.

| Claim | Required evidence | Invalid substitute |
| --- | --- | --- |
| A task survives an offline worker | Transport acceptance, later direct worker receipt, observed execution count, one logical terminal outcome, wire-delivery count | REST row eventually appears |
| Broker identity suppresses wire retry | W6a with equal `id`/`task_id`, publication acknowledgements, stream sequences, execution/result counts, configured broker window | No client-visible error |
| Task identity suppresses semantic retry | W6b with new `id`, equal sender/worker/task/fingerprint, ledger lookup, execution/result counts, configured ledger retention | Broker duplicate flag alone |
| Reused task identity is rejected safely | W6c with changed sender or fingerprint, `task_id_collision`, no cached-result leak, no handler execution | An exception or timeout |
| Crash recovery is correct | Crash point, missing acknowledgement, redelivery evidence, final execution/result/side-effect counts | Worker restart log alone |
| Split-plane lowers cost | Same workload in EdgeCitadel and all-durable modes, resource samples, bytes, storage, latency distribution | Comparing different payloads or agents |
| Agent context is preserved | Task ID, context ID, parent task ID, delegation hop, and terminal envelope assertions | Free-form text mentioning a parent |
| Operator state is correct | Deterministic browser journey, API audit, screenshot, trace, and video from the same run | A manually staged screenshot |

Publication figures may be generated only from runs whose manifests pass the
artifact checker. Existing July 2026 outputs remain functional probes and are not
publication-grade benchmark results.

The initial paper must not claim:

- Exactly-once delivery or side effects. Duplicate suppression is bounded by the
  broker window and outcome-ledger retention.
- Relative superiority over MQTT, HTTP/SSE, RabbitMQ, or another broker without a
  paired implementation of the same workload.
- p95, p99, throughput, scalability, energy, constrained-link, Raspberry Pi,
  multi-site, or general edge performance from a quick or single-host run.
- Durability for the plain-NATS outbox mirror or `task.progress`.
- Authorization or semantic-security results from the existing synthetic
  temporal evaluator.
- Distributed correctness or performance from screenshots, videos, or UI traces.

## 4. Slice 1: Hermetic Experiment Spine

### 4.1 Boundary

The experiment spine owns environment isolation, deterministic workers, direct
measurement, raw evidence, analysis, and validity checks. It does not depend on a
developer's running Compose stack or host ports.

### 4.2 Runtime Topology

A dedicated `scripts/research/docker-compose.artifact.yml` starts only the
components required by the selected mode:

- A mode-specific controller/transport service.
- A deterministic worker that invokes the common `TaskExecutor`.
- A run-unique sender/result observer for the mode-specific result path.
- A one-shot benchmark runner, evidence writer, and resource sampler.
- NATS with fresh storage and run-specific credentials for the three NATS modes.
- JetStream only for EdgeCitadel and all-durable.

The EdgeCitadel and all-durable workers receive tasks through the production
`PullConsumer`. Core-only uses a plain-NATS receiver. Central relay uses an HTTP
worker endpoint and callback/result endpoint. All receivers call the same
`TaskExecutor`; none reimplements handler, ledger, fingerprint, or terminal logic.

The production aggregator does not run in publication benchmark repetitions.
Starting it later cannot recover Core-NATS mirrors, while running it during only
NATS modes would perturb those treatments. Aggregator audit correctness is tested
in the separate Slice 2 functional journey and never contributes benchmark
latency, correctness, or resource data.

The launcher creates a unique Compose project name, refuses ambiguous inputs,
records the resolved configuration, and always tears down containers, networks,
and volumes that it owns. It exposes no development host ports and cannot connect
to the normal development stack.

Stable component ownership is:

| Path | Responsibility |
| --- | --- |
| `scripts/research/run_artifact.py` | Profile parsing, seeded schedule, run lifecycle, exit status |
| `scripts/research/artifact_env.py` | Run-owned Compose project, credentials, ports, and cleanup |
| `scripts/research/preflight.py` | Mode-specific readiness and configuration validation |
| `scripts/research/modes/*.py` | `TaskTransport` implementations |
| `adapters/_common/task_executor.py` | Common validation, fingerprint, ledger, handler, and terminal protocol |
| `adapters/_common/task_publisher.py` | Injectable terminal and progress publisher protocols |
| `scripts/research/fixtures/native_control.py` | Deterministic handler, heartbeat/card, failure injection |
| `adapters/_common/outcome_store.py` | OutcomeStore protocol and SQLite implementation |
| `scripts/research/metrics.py` | Monotonic events and resource/byte/storage samples |
| `scripts/research/evidence.py` | Raw JSONL, manifest, checksums, and final run status |
| `scripts/research/analyze_artifact.py` | Deterministic summaries, tables, and figure data |
| `scripts/research/check_artifact.py` | Schema, hash, invariant, secret, and cleanup verification |
| `schemas/research-manifest.v1.json` | Benchmark and operator evidence manifest |
| `schemas/task-correlation.v1.json` | Direct/delegated task identity and parent linkage |

### 4.3 Measurement Flow

For each trial:

1. The direct observer starts fetching before the trial clock starts.
2. The runner creates a stable logical task ID and a unique trial nonce.
3. The mode-specific publisher submits the task.
4. The runner records API acceptance or publication acknowledgement using a
   monotonic clock.
5. The deterministic worker records direct receipt before execution.
6. The fake actuator records every attempted execution and side effect.
7. The worker emits progress and a terminal result according to the workload.
8. The direct observer records and validates the terminal result.
9. The evidence writer checks the direct task, execution, result, and side-effect
   invariants.
10. The trial ends after all expected counts and invariants are checked.

REST polling is never used in a publication benchmark clock or correctness
assertion. All intervals use
`perf_counter_ns` or the equivalent monotonic source within a single host. Epoch
timestamps are retained for correlation, not latency subtraction. Slice 2 may
query REST after browser actions as functional operator evidence.

### 4.4 Required Raw Measurements

The direct-command record contains:

- Start-to-API-accept or start-to-PubAck latency.
- Start-to-direct-terminal-result latency.
- Post-accept terminal latency.
- Terminal state and exact nonce check.
- Worker execution, logical terminal, distinct terminal-ID, publication-attempt,
  and wire-delivery counts.

The W6a wire-retry record contains:

- Both publication acknowledgement latencies.
- Duplicate flags and both stream sequence values.
- Direct-terminal-result latency.
- Worker execution and durable-result counts.
- Stream and consumer deltas before and after the trial.
- Pending and acknowledgement-pending counts at completion.

The W6b semantic-retry record contains both envelope IDs, equal task/fingerprint
evidence, the elapsed retry interval, ledger hit/miss decision, handler execution
count, terminal logical/wire counts, and broker duplicate flags. The W6c collision record contains both
fingerprints and senders, the collision decision, rejection envelope, cached-result
exposure count, and handler execution count.

Every W5 record names its crash point and records ledger state, inbound delivery
count, handler execution count, side-effect count, terminal publication attempts,
terminal logical outcomes, wire deliveries, and final consumer state.

Each trial also links to resource samples for controller, broker, worker, and
observer CPU/RSS; broker storage and message counts; application and broker wire
bytes; and generated, delivered, and dropped progress counts.

Every failure and timeout remains a raw trial record with missing values represented
explicitly and a machine-readable reason.

Cost measurement uses the same window and rules in every mode:

- A two-second idle baseline precedes the trial.
- The active window starts at T0 and ends at terminal observation or the
  predeclared workload timeout.
- Samples occur every 100 ms using container cgroup counters.
- Reported CPU is delta CPU-seconds; memory is peak RSS and integrated RSS-seconds.
- Network cost includes per-container interface RX/TX deltas and application
  payload bytes. NATS modes additionally record server connection byte deltas;
  central relay records HTTP request/response bytes.
- Storage cost is the change in run-owned database and JetStream bytes after a
  flush and before cleanup.
- System totals include only the mode controller/relay, transport, worker, and
  sender observer. The sampler is itemized separately.
- A no-op calibration records sampler CPU and wakeup cost. A run is invalid for
  cost claims if sampler CPU exceeds two percent of one core during the active
  window.
- Completion-latency distributions contain completed observations only and always
  appear beside failure/timeout counts. Failed trials are not imputed or silently
  dropped; their full-window resource cost remains included.

The manifest pins each workload timeout and resource component set. A comparison
is invalid if component membership, sampler interval, or measurement window differs
between its modes.

### 4.5 Trial Protocol

The quick profile is an installation check:

- One mode-owned stack, one warmup, and three W1 trials for each of the four modes.
- One W6a and one W6b trial for each EdgeCitadel duplicate ablation.
- Expected wall time below 30 minutes on the supported controller.
- Output labeled `quick`; no inferential statistics.
- No p99 output.

The `matrix-smoke` profile runs one unmeasured repetition of every required W1-W8
mode/variant cell and every EdgeCitadel ablation. It validates implementation
coverage and failure injection without producing statistics.

The statistical units are:

- **Trial:** one logical workload execution with one terminal or failure outcome.
- **Repetition:** one fresh Compose project and empty mode-owned state containing
  exactly one measured trial. Warmup repetitions are equally fresh and never
  contribute measurements.
- **Cell:** one mode, workload/variant, hardware profile, and network profile.
- **Block:** one seeded, randomized pass containing one repetition of every
  predeclared cell.
- **Campaign:** the complete set of blocks for named hardware and network
  conditions.

The preliminary paper campaign schedules exactly five warmup blocks and exactly 30
measured blocks. Each block contains one repetition of every required cell. The
seeded scheduler randomizes mode order within each block and records every task
failure or timeout. Each repetition uses a fresh Compose project, empty
JetStream/database/ledger state, and run-unique identities. No outcome-based
replacement, optional stopping, or extra block is permitted; an infrastructure-
invalid block makes the campaign incomplete and requires a new campaign ID after
the harness is fixed.

Correctness proportions report `n`, failures, Wilson 95% confidence intervals, and
pairwise risk differences with Newcombe 95% intervals. Continuous completed
observations report median and p95, paired median difference against EdgeCitadel
within each block, relative change, and a 95% percentile-bootstrap interval using
10,000 resamples of paired measured blocks. The manifest records the bootstrap
seed and estimator.
p99 is emitted only with at least 1,000 measured observations in the cell.

The preliminary campaign is a single x86_64 controller on a declared LAN profile.
A Paper Evidence Ready campaign additionally repeats W1, W3, W4, W5, W6a, W6b,
W6c, and W8 on a declared ARM64 Linux gateway class and on declared `lan`,
`50ms-rtt`, and `1pct-loss` profiles, with exactly five warmup and 30 measured
blocks per declared profile. Claims remain limited to valid measured conditions; a
missing platform or condition is reported as missing evidence, not inferred
portability.

The `lan` profile applies no shaping and records observed RTT/loss. `50ms-rtt`
applies 25 ms fixed egress delay at each endpoint with no injected loss.
`1pct-loss` applies one-percent independent egress loss at each endpoint with no
injected delay. Preflight verifies the active `tc netem` state and records a
five-second probe before the campaign; a mismatch invalidates the profile.

The command-line contract is:

```bash
python3 scripts/research/run_artifact.py run --profile quick
python3 scripts/research/run_artifact.py run --profile matrix-smoke
python3 scripts/research/run_artifact.py run \
  --profile paper \
  --campaign-config scripts/research/configs/campaigns/preliminary-x86-lan.yaml
python3 scripts/research/analyze_artifact.py \
  --campaign preliminary-x86-lan \
  --confidence 0.95 \
  --bootstrap-samples 10000
python3 scripts/research/check_artifact.py --campaign preliminary-x86-lan
```

The campaign file contains the fixed seed, required matrix cells, trial/warmup
counts, hardware declaration, network declaration, timeouts, and output root.
Hardware and network schemas live under `scripts/research/configs/schema/`.
Automatic cleanup runs after every repetition; the idempotent recovery command is:

```bash
python3 scripts/research/run_artifact.py cleanup --run-id ec-20260725-example
```

### 4.6 Artifact Layout

Raw evidence is immutable:

```text
docs/research/results/raw/<campaign>/
  campaign.json
  schedule.jsonl
  <run-id>/
    manifest.json
    preflight.json
    events.jsonl
    trials.jsonl
    logs/
```

Derived evidence is reproducible from raw inputs:

```text
docs/research/results/derived/<campaign>/
  summary.json
  report.md
  tables/
  figures/
```

`campaign.json` and `schedule.jsonl` are written and hashed before the first
measured block. Every run manifest references that hash and scheduled cell. They
are append-forbidden after execution begins; a changed schedule invalidates the
campaign.

The manifest records:

- Commit, dirty state, and a source hash that includes relevant untracked files.
- Benchmark arguments, mode/workload configuration, schema hashes, and dependency
  versions.
- Container image digests and resolved Compose configuration hash.
- OS, architecture, CPU, memory, cgroup limits, and clock source.
- NATS server, stream, and consumer configuration.
- Start/end times, cleanup outcome, and hashes of raw artifacts.

Secrets and bearer tokens are never serialized.

Publication campaigns require committed tracked source, `git_dirty=false`, and
immutable image identifiers. Dirty or untracked-source runs are development
evidence and are excluded from paper figures.

### 4.7 Validity and Failure Handling

Preflight rejects:

- Placeholder or missing credentials.
- Failed authentication for every NATS mode; central relay validates its
  run-scoped authorization secret independently.
- For EdgeCitadel and all-durable, any mismatch in `AGENT_INBOX`, WorkQueue
  retention, `agents.*.inbox`, duplicate window, or the run-specific durable
  consumer filters and acknowledgement settings.
- For EdgeCitadel, any stream capture of declared progress/liveness subjects.
- For all-durable, any mismatch in `TRANSIENT_EVENTS`, its subjects/retention/
  storage/limits, publisher PubAck behavior, or observer consumer settings.
- Non-fresh storage where freshness is required.
- Unsupported mode/workload combinations.
- Missing direct observer or deterministic worker readiness.
- Dirty output directories or duplicate run IDs.
- Any required matrix cell without an executable mode implementation.

A **valid task failure** is an injected or naturally observed workload outcome such
as transport non-acceptance, no terminal before the fixed deadline, duplicate
execution, conflicting terminal, or side-effect duplication. It remains in the
scheduled block, contributes to correctness/failure counts, and does not make the
harness invalid.

A **harness-invalid repetition** is caused by failed preflight, an uninjected
process crash, observer not ready before T0, configuration drift, evidence-write or
schema failure, nonmonotonic clock observation, excessive sampler overhead, or
owned-resource cleanup failure. It preserves logs/raw events, records the reason,
and causes a nonzero campaign/checker result. It is never replaced after outcomes
are inspected.

If any required cell or measured block is harness-invalid or absent, the checker
marks the campaign incomplete and figure generation exits nonzero. Development
analysis may show invalid records diagnostically, but publication analysis accepts
only a complete predeclared campaign.

### 4.8 Slice 1 Acceptance

Slice 1 is complete when:

- A single documented command runs the quick profile from a clean checkout with
  the declared Docker, Compose, Python, and Git prerequisites.
- The matrix-smoke profile executes every required mode/workload/ablation cell;
  missing or harness-invalid coverage fails the gate.
- W5 injects and verifies every named boundary applicable to the mode and records
  transport-inapplicable boundaries explicitly.
- W6a, W6b, and W6c distinguish broker retry, semantic retry, and task-ID
  collision with the three declared ablations.
- Offline, restart, delegation, progress, duplicate, and non-idempotent workloads
  assert logical outcomes, wire deliveries, executions, and side-effect counts.
- Resource totals use identical component sets/windows and pass sampler-overhead
  calibration.
- Re-running analysis from raw files reproduces byte-identical tables and stable
  figure data.
- The artifact checker detects a modified raw file, leaked credential pattern,
  missing provenance field, and incomplete cleanup.
- Unit tests cover benchmark statistics, manifest creation, mode invariants,
  deterministic worker behavior, and cleanup ownership.
- Two concurrent quick runs use distinct projects, state, subjects, identities,
  and resources.

## 5. Slice 2: Deterministic Operator Journey

### 5.1 Boundary

The operator journey proves that the production frontend and backend expose the
same task lifecycle measured by the experiment spine. It uses one deterministic
production-style agent fixture and does not require Gemma, Hermes, Ollama, or an
external service.

### 5.2 Required Repairs

Paper-supporting correctness repairs are:

- Registry rows pass the actual status value to `StatusBadge`.
- Task derivation cannot let an older command or progress event overwrite a newer
  terminal state and implements the Section 2.3 observation-order reducer.
- Fleet-wide WebSocket updates remain active while an agent is selected.
- Registration and status events patch registry state consistently.
- Offline notifications read the canonical event field.
- Playwright uses the isolated test base URL rather than hardcoded development
  ports.

Two existing product defects are goal-critical but not paper evidence: the
light-mode control must either render the audited views readably or be removed, and
visible product naming in those views must use EdgeCitadel consistently. They do
not add benchmark cells or claims.

### 5.3 Journey

The evidence test:

1. Confirms backend, NATS, and JetStream health.
2. Observes one valid `shell-1` Agent Card online with L1 conformance.
3. Selects the agent through relative application URLs.
4. submits a deterministic delayed command with a unique nonce.
5. Captures the accepted task ID.
6. Observes the command and provenance in the conversation.
7. Observes a pending or working state before completion.
8. Observes the exact terminal output.
9. Confirms through the API that exactly one operator-visible command row and one
   logical terminal-result row exist.
10. Confirms the durable queue is drained.
11. Fails on browser console, page, or unexpected request errors.

### 5.4 Evidence Bundle

A dedicated evidence configuration uses one Playwright worker, no retries, trace
on, video on, and explicit desktop and mobile screenshots. Its wrapper owns the
Compose lifecycle and writes:

- Desktop/mobile screenshots, video, trace, and machine-readable test result.
- API snapshots for system status, registry row, messages, and queue state.
- Git commit/dirty state and relevant source diff hash.
- Compose configuration hash, image digests, tool versions, OS, architecture,
  command line, timestamps, and artifact hashes.

The screenshot and video must originate from the same passing run. Existing March
2026 media is archived as historical and never presented as current evidence.

### 5.5 Slice 2 Acceptance

Slice 2 is complete when:

- The deterministic operator journey passes from a clean isolated stack.
- Frontend production build and targeted component tests pass.
- Backend unit tests and the deterministic agent round trip pass.
- The screenshot visibly contains the selected online agent, command, task state,
  and exact terminal result without overlap at desktop and mobile widths.
- The trace and video are playable and match the recorded task ID.
- Legacy model-backed specs are classified as optional live tests or converted to
  deterministic fixtures; they cannot silently contaminate the default gate.

## 6. Slice 3: Reproducible Multi-Agent IoT Lab Setup

### 6.1 Supported Baseline

This pass supports one honest research deployment baseline:

- Ubuntu 24.04 LTS.
- x86_64.
- Docker Engine with Compose v2.
- Python 3.12 and Git as preinstalled prerequisites.
- One controller and one or more remote Linux gateway nodes on a trusted
  experiment LAN or Tailnet.
- Native NATS deterministic agents.

macOS, ESP32 firmware, production MQTT devices, fleet-wide TLS identity, arbitrary
upgrades, and Internet-facing deployment are explicitly unsupported in this pass.
ARM64 Linux is a Paper Evidence Ready qualification target, not part of the
initial x86_64 launcher verification.

Paper-critical scope is a clean, isolated controller plus two deterministic node
identities, including disconnect/reconnect behavior and precise teardown. A remote
second-host run, doctor diagnostics, and ARM64 qualification are separate operator
or paper-portability gates; their absence cannot be hidden behind the core
same-host result.

### 6.2 Controller Contract

The lab controller launcher:

- Creates an isolated run-scoped Compose project on collision-free or
  caller-selected ports.
- Generates a strong run-scoped NATS token and writes it only to a mode-0600
  credential file.
- Renders and validates NATS configuration before startup.
- Creates only run-owned storage and starts core services in dependency order.
- Checks NATS authentication, JetStream configuration, aggregator health, and
  registry readiness.
- Provides an idempotent teardown that removes only resources owned by the run.

A clean start never requires a prior `.env`, database, broker, backup, model,
EdgeCitadel service, or developer stack. Docker Engine, Compose v2, Python 3.12,
and Git are explicit prerequisites. The general `deploy-host.sh` migration, backup,
package-management, upgrade, and reboot-persistence paths are deferred.

### 6.3 Node Contract

The deterministic node launcher accepts a controller address, validated agent ID,
run ID, behavior/failure mode, and credential file. It:

- Uses the production `PullConsumer`.
- Publishes a schema-valid Agent Card and heartbeat.
- Creates a durable consumer identity unique to the run and agent.
- Supports delay, crash point, duplicate, and fake-actuator behavior without
  source edits.
- Provides a doctor command that checks time, DNS/routing, broker reachability,
  authentication, card validity, process state, and heartbeat freshness.

Two deterministic agents must run concurrently on one or multiple nodes without
editing tracked source, sharing durable identities, or stopping each other. The
same launcher is used by the benchmark and Playwright environments.

The controller and node entrypoints are:

```bash
python3 scripts/research/lab_controller.py start --run-id ec-lab-01
python3 scripts/research/lab_controller.py status --run-id ec-lab-01
python3 scripts/research/lab_node.py start \
  --controller-config tmp/research/ec-lab-01/controller.json \
  --credential-file tmp/research/ec-lab-01/nats.creds \
  --agent-id fixture-1
python3 scripts/research/lab_controller.py stop --run-id ec-lab-01
```

The controller inventory rejects a duplicate agent ID within a run before process
start. With a shared lab credential it cannot authenticate global ownership or
prevent an external process from claiming the same durable consumer. The runbook
states this limitation; broker-level duplicate identity security is deferred to
per-agent credentials.

For this research artifact, all native agents may use an experiment-scoped broker
credential distributed through a mode-0600 file. The design does not present this
as production per-agent authorization. Credentials never appear in command-line
arguments, logs, screenshots, or generated manifests.

### 6.4 OpenClaw and MQTT Boundary

`openclaw-client` is documented as an operator/session client unless and until it
has scoped broker credentials, a valid card, inbox consumption, and command
execution. It is not part of node onboarding in this slice.

MQTT remains an explicit optional ingress capability. A bound port is not treated
as a protocol health check, and no production constrained-device claim is made
without a maintained reference client and protocol-level test.

Legacy `join.sh`, `add-agent.sh`, and the general production deployment scripts
are not used by the paper artifact. Their known limitations are documented rather
than partially repaired in this slice.

### 6.5 Slice 3 Acceptance

Slice 3 is complete when an automated clean-checkout run demonstrates:

- An isolated controller start and semantic health without a pre-existing
  environment.
- Two node launches without a token in shell history or process arguments.
- Two simultaneous deterministic agents.
- Command and terminal result round trips to both agents.
- Queued delivery after one agent disconnects and reconnects.
- Duplicate agent-ID rejection by the run-owned launcher inventory.
- A passing deterministic Playwright registry-to-result journey.
- Two sequential runs and two concurrent runs without cross-run subjects, state,
  ports, consumers, results, or cleanup.
- Idempotent teardown with no unrelated Docker or JetStream resource deletion.

The runbook records unsupported platforms and security limitations next to the
commands, not in a detached caveat.

Remote Lab Qualified additionally requires the same workflow on a declared second
Ubuntu host with the network path recorded. Paper Evidence Ready additionally
requires the ARM64 and controlled-network repetitions in Section 4.5. Until those
gates pass, documentation says `remote-capable` or `preliminary`, not `remote
verified` or `edge-platform measured`.

## 7. Slice 4: Documentation and Artifact Organization

### 7.1 Source-of-Truth Rules

| Concern | Authority |
| --- | --- |
| Envelopes and Agent Cards | `schemas/*.json` |
| Subjects, streams, and delivery | `aggregator/jetstream_bootstrap.py` and rendered NATS config |
| Proposed reliability semantics, mode matrix, and evidence gates | This design specification |
| HTTP and WebSocket API | FastAPI routes and generated OpenAPI |
| Runtime topology and ports | Compose and deployment files |
| Dashboard behavior | `frontend/` |
| Test gates | Executable test configuration and specs |
| Host and node setup | Executable lab launchers and their tests |
| Research results | Validated immutable raw artifacts and deterministic analysis |
| Decisions | Implemented runtime; ADRs record history only |

Documentation explains these authorities but does not redefine them.

### 7.2 Maintained Shape

The maintained documentation set is:

- `docs/README.md`: index with Current, Experimental, Proposed, and Historical
  status.
- `docs/01-architecture.md`: current runtime and task-aware reliability path.
- `docs/agent-contract.md`: schema-valid envelope and card examples.
- `docs/05-messaging.md`: current subjects, streams, delivery, and failure rules.
- `docs/08-api-reference.md`: generated or checked route/event reference.
- `docs/04-dashboard.md`: current operator workflow.
- `docs/10-testing.md`: deterministic default gates and optional live tests.
- `docs/setup-development.md`: local development stack.
- `docs/setup-lab-controller.md`: supported artifact-controller lifecycle.
- `docs/setup-lab-node.md`: deterministic local/remote node lifecycle.
- `docs/research/artifact.md`: quick/full experiment entrypoint, outputs,
  provenance, analysis, cleanup, and limitations.
- `docs/adr/README.md`: unique ADR identifiers and accurate implementation status.

These historical moves are exact:

- `docs/NATS_ARCHITECTURE.md` to
  `docs/archive/pre-v0.1/NATS_ARCHITECTURE.md`.
- `docs/06-p2p-delegation.md` to
  `docs/archive/pre-v0.1/06-p2p-delegation.md`.
- `docs/07-task-management.md` to
  `docs/archive/pre-v0.1/07-task-management.md`.
- `docs/11-future-potential.md` to
  `docs/archive/pre-v0.1/11-future-potential.md`.
- `docs/research/future-directions-roadmap.html` to
  `docs/archive/research/future-directions-roadmap.html`.
- `docs/demo.gif` and `docs/demo.mp4` to `docs/archive/media/2026-03/`.

Historical results stay available but are labeled as functional probes.

### 7.3 Documentation Checks

Automated checks cover:

- Internal links.
- Duplicate ADR identifiers.
- Route-table drift against OpenAPI.
- JSON examples against schemas.
- Commands referenced by maintained runbooks.
- Personal absolute paths and placeholder credentials.
- Research-result labels and required provenance fields.
- Active references to removed `/api/tasks`, `tasks.*`, `CONVERSATIONS`,
  `AGENT_STATE`, and `mqtt-listener.js` outside `docs/archive/`.

### 7.4 Slice 4 Acceptance

Slice 4 is complete when a new reader can:

1. Identify current, proposed, and historical behavior from `docs/README.md`.
2. Start the supported lab controller and nodes using only maintained runbooks.
3. Run the quick artifact with one command.
4. Locate raw evidence, reproduce derived outputs, and understand exclusions.
5. Run the default frontend, backend, and deterministic end-to-end gates.
6. Find explicit limitations for unsupported platforms, MQTT, authorization, and
   model-backed tests.

No maintained document may claim a feature, endpoint, stream, security property,
or benchmark result that lacks executable evidence.

## 8. Cross-Slice Interfaces

The four slices share only these contracts:

- Canonical envelope and Agent Card schemas.
- The correlation and state rules in Section 2.3.
- `scripts/research/modes/base.py::TaskTransport`.
- `adapters/_common/task_executor.py::TaskExecutor`.
- `adapters/_common/task_publisher.py::{TerminalPublisher, ProgressPublisher}`.
- `adapters/_common/outcome_store.py::OutcomeStore`.
- `scripts/research/fixtures/native_control.py`, runnable in benchmark, E2E, and
  lab environments.
- `schemas/research-manifest.v1.json` for benchmark and operator evidence.
- `scripts/research/preflight.py::PreflightReport` for NATS, JetStream,
  aggregator, and agent readiness.
- One classification vocabulary: `current`, `experimental`, `proposed`, and
  `historical`.

The benchmark runner does not import frontend code. Playwright does not calculate
paper statistics. Deployment scripts do not mutate research results. Documentation
checks consume public schemas and generated metadata rather than duplicating
constants.

## 9. Verification Strategy

Every implementation plan follows test-driven development and ends with the
relevant repository gate:

- Benchmark: research unit tests, hermetic quick run, artifact checker, analysis
  reproducibility.
- Backend: syntax, focused unit tests, complete aggregator suite, and runtime
  smoke.
- Frontend: component tests, production build, deterministic Playwright journey,
  desktop/mobile screenshot inspection.
- Infrastructure: launcher unit tests, Compose validation, clean-checkout
  lifecycle, concurrent-run isolation, idempotent teardown, and deterministic
  Playwright.
- Documentation: link/schema/OpenAPI/command checks and a clean quick-start dry
  run.

A final completion audit maps every requirement in this specification to a passing
command and artifact path. Work is not complete while a required command is
skipped, an evidence bundle is missing, or a limitation is represented as an
implemented feature.

## 10. Readiness Gates and Research Deliverables

Readiness is cumulative:

| Gate | Required evidence |
| --- | --- |
| Design Ready | User-approved written specification with no unresolved placeholder, contradiction, or ambiguous required cell |
| Artifact Verified | Unit/integration gates, clean quick and matrix-smoke profiles, deterministic analyzer output, passing artifact checker, and owned-resource cleanup |
| Preliminary Measured | Complete x86_64/LAN campaign with exactly 30 scheduled measured blocks and no missing or replacement block |
| Operator Evidence Ready | Passing isolated browser journey with same-run API snapshots, desktop/mobile screenshots, trace, video, and manifest |
| Remote Lab Qualified | Controller and nodes exercised on a declared second Ubuntu host with the observed network path recorded |
| Paper Evidence Ready | Preliminary gate plus the ARM64 and controlled-network matrix declared in Section 4.5, valid statistics, and claim-to-artifact audit |

The preliminary milestone produces:

- A precise mechanism description and task lifecycle model.
- Four executable comparison modes.
- The complete W1-W8 failure-oriented matrix and duplicate ablations.
- Raw and derived artifact formats with validity checking.
- Preliminary single-controller x86_64/LAN results with appropriate statistics.
- A deterministic operator evidence bundle.
- A reproducible small multi-agent deployment runbook.
- A limitations section that separates current evidence from future IoT,
  authorization, and model-quality work.

Paper submission claims require Paper Evidence Ready. A preliminary campaign may
support an artifact-development report, but it cannot be labeled paper-ready,
cross-platform, or representative of constrained edge networks.

The intended ambitious venue is EuroSys. OSDI is considered only if the measured
mechanism and evaluation become substantially broader and more novel than the
initial task-aware contract.

## 11. Implementation Order

1. Implement and validate the hermetic experiment spine.
2. Reuse its deterministic fixture in the operator journey and repair blocking
   frontend/backend defects.
3. Package the same fixture and health contracts for controller/node deployment.
4. Rewrite and check documentation against the now-executable system.
5. Run the preliminary campaign and the requirement-by-requirement completion
   audit.

This order ensures that later screenshots, runbooks, and claims describe one
tested task lifecycle rather than separate demonstrations.
