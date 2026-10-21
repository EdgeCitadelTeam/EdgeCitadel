# Integration notes: `m2/agentd` and `m2/monitor` into `m3/integration`

## Inputs

| Side | Branch | Head | Commits since `containment/sprint` (`df67efd`) |
|---|---|---|---|
| agentd | `m2/agentd` | `07d22b4` | 11: pause/resume/cancel, Pi abort/resume, no unbound `task.create`, effect scope and leases, effect gateway, Pi effect gate, control channel, held-result withholding, monitor actuator vocabulary, scheduled effects, authority on `run/started` (schema 32) |
| monitor | `m2/monitor` | `c6361d8` | 3: grouped min-cut solver, monitor engine, aggregator service and HTTP API |

Both branches start at `df67efd`, so `m2/agentd` was merged first as an
ordinary merge commit without conflicts, then `m2/monitor` on top.

## Textual merge

Git reported no conflicts. Three files were changed by both sides and merged
line-wise; each was checked by hand:

| File | agentd side | monitor side | Result |
|---|---|---|---|
| `.env.example` | `EDGECITADEL_CONTROLLER_LEASE_MS` (agentd process) | `EDGECITADEL_CONTAINMENT_MONITOR`, `EDGECITADEL_CONTAINMENT_CONFIG` (Core) | both kept; different processes, no overlap |
| `docs/architecture/execution-trace-contract.md` | new sections for delegated authority, effect gateway, control protocol, pause/resume | new "Containment monitor (experimental)" section at the end | both kept; the monitor's "Control" paragraph named the removed RPCs and was corrected (see below) |
| `aggregator/main.py`, `docker-compose.yml` | untouched | monitor lifespan and environment | monitor side |

agentd's "Core ingest accepts the effect and containment families" is not an
aggregator change: Core validates events with the shared
`edgecitadel_agentd.trace_contract`, which `f330da6` updated, and the commit's
proof is `aggregator/tests/test_trace_projection_completeness.py`. The monitor
already followed `effect` events with the phases agentd now emits
(`requested`/`committed`/`denied`), so the two sides agree on the event shape.

## Semantic conflicts resolved in the merge commit

The monitor was written against `containment/sprint`, where agentd exposed the
admin RPCs `containment.hold`/`containment.release` and `AgentdStore.list_holds`.
`m2/agentd` (`91f33fe`) removed all three in favour of the control protocol. After
the textual merge the monitor's real-agentd tests failed with `control_failed`
and `AttributeError`. Resolution, kept minimal so the merge commit is consistent:

- `LocalAgentdControlClient` sends `containment.control {action, target: {kind,
  object_id}, owner, policy_version}`; a `rejected` confirmation raises
  `ControlError(code)`, and `applied_at_ms` comes from the confirmation.
- The two monitor tests read holds through `list_containment()["holds"]`.
- The monitor section of the trace contract names `containment.control`.

The full adaptation to the control protocol (describe-based capabilities, the
signed NATS client, the extended action vocabulary and fan-out, unconfirmed
actions) follows in separate commits on this branch; see below.

## Defect found on `m2/agentd`

`agent-runtime/tests/agentd/test_nats_integration.py` (opt-in,
`RUN_AGENTD_NATS_INTEGRATION=1`) still built its signed command with
`target: {kind, id}`, the shape before `f34f7fa` renamed it to `object_id`. The
test had never run because no `nats-server` was installed. With nats-server
2.14.6 it failed with `rejected`; it is fixed in its own commit and now passes
(4 passed) against a real broker.

## Adapting the monitor to the agentd control protocol (after the merge)

| Change | Where |
|---|---|
| One `send(node, action, target, policy_version)` per transport; three outcomes: `Confirmation` (with `applied_at_ms` and the node's `enforcement` report), `ControlError(code)` (rejected, or not delivered), `ControlUnconfirmed` (sent, unanswered) | `aggregator/containment_monitor/control.py` |
| `LocalAgentdControlClient` on `containment.control`; `NatsControlClient` signing with agentd's `command_mac` on `edgecitadel.control.v1.<node>`, retrying one `command_id` until the confirmation timeout and never past expiry | same |
| Capabilities from each node's `describe`, cached per node (`refresh`), instead of a fixed set | same, `service.py` |
| Vocabulary: `hold:egress`, `revoke:authority`, `cancel:task` added; revocations with node-local state fan out to every node that may enforce them | `model.py`, `projection.ScopeMirror.sites` |
| The projection offers an action only where every site describes it | `projection.py` |
| An action is in force only when every site confirmed; unconfirmed actions are neither counted nor released and keep phase-1 holds | `enforcement.py` |
| Scopes from `run/started.attributes.authority`; configured grants only where no export exists | `projection.ScopeMirror`, `provenance.py` |
| Effects from the gateway's `requested`/`committed`/`denied`; only scheduled effects stay pending, cut by an effect hold or by holding the issuer | `provenance.py`, `projection.py` |

Mapping of the monitor's actions onto agentd commands:

| Monitor action | agentd command | Sent to |
|---|---|---|
| `hold:task` | `hold {kind: task, object_id: <task>}`; released by `release` | the node of the task (pause) or of its parent (result park) |
| `hold:egress` | `hold {kind: egress, object_id: <task or native binding>}` | the invocation's node |
| `hold:transport` | `hold {kind: transport, object_id: <message>}` | the publishing node |
| `hold:effect` | `hold {kind: effect, object_id: <effect_id>}` (scheduled effects only) | the issuing invocation's node |
| `revoke:egress` | `revoke {kind: egress, object_id: <task>}` | the invocation's node |
| `revoke:authority` | `revoke {kind: authority, object_id: <task>}` | every node that may hold the delegation's descendants |
| `revoke:credential` | `revoke {kind: credential, object_id: <agent>}` | the agent's node |
| `revoke:grant` | `revoke {kind: grant, object_id: "a>b" or "a>fx:<class>"}` (an ingress filter) | a's node, and b's node for an agent target |
| `revoke:admission` | `revoke {kind: admission, object_id: <agent>}` (filter `any` into the agent) | the agent's node and the nodes of agents that may dispatch to it |
| `cancel:task` | `cancel {task_id}` | the task's node |

`revoke:authority` and `cancel:task` are mapped and executable but never
offered by the projection (M4 item 1: the vocabulary is vertex-local by
design). `hold:egress` is offered only in phase 1 (M4 item 4).

Immediate effects are decided at request time and cannot be held individually:
the projection covers them only through the issuing unit's task hold, egress
revocation, credential or grant revocation. `stop_team_level` (new monitor
setting) selects how many org levels above the root's agent "Stop team" starts.

## Team decisions applied (2026-10-20)

| Decision | Implementation |
|---|---|
| Best-effort guard on by default | `MonitorConfig.best_effort_guard = True`. The cut-property test disables it explicitly, because guard actions target sinks the decision judged unavoidable. |
| Fail-closed controller leases on for the lab, configurable, renewed by the monitor | agentd: `EDGECITADEL_CONTROLLER_LEASE_MS` (unchanged, off unless set); lab value 30000 ms in `DEPLOY_RUNBOOK.md` with its justification. Monitor: `controller_lease_renew_ms` (default 10000, 0 disables) sends `renew` to every known node that describes `renew:controller`, on its own thread pool so an unreachable node never delays a decision, with the owner's current policy version (not advanced). Per-node lease state is in status `control.controller_leases`. The local end-to-end runs with leases enforced on all three nodes. |
| Horizon h = 1, boundary as sink | Already the defaults (`horizon: 1`, `boundary: "sink"`); recorded in the runbook. |

## Local end-to-end test (`e2e/containment/`)

Real processes on one host: nats-server 2.14.6 (JetStream), Core
(`uvicorn aggregator.main:app`, collector and monitor, signed NATS control),
three agentd nodes `jim-eq`, `cluster`, `endurance` (trace sync on, controller
lease 30 s), one Managed Agent process per agent running agentd's production
`managed_runtime.run` with a scripted handler, and a 25 ms one-way delay relay
in front of the broker for cluster and endurance (`tc` is not installed here, so
the delay is a TCP relay rather than netem; it delays the nodes' task,
telemetry and control traffic alike). agentd uses the ordinary-store test
entrypoint (`agent-runtime/tests/agentd/service_test_support.py`): the
provisioned production storage layout is not exercised.

nats-server provenance: GitHub releases and the Go module proxy are blocked by
this sandbox's egress policy (403), and no Docker daemon is available, so the
binary came from the PyPI wheel `nats-server-bin==2.14.6` (a third-party
repackaging, Bing-su/pip-binary-factory; `nats-server --version` reports
v2.14.6, the version Core's compose file pins). It was used only for local
tests.

Workflow (C2): `kipp` (jim-eq) → `tars` (cluster) and `case` (endurance). `tars`
starts the healthy `a3`, has `a1` produce a shared artifact, passes it to the
sibling `a2` and returns a2's analysis; `kipp` fans it out to `case`, whose `b1`
schedules an actuator command (`effect.authorize` with `scheduled_at_ms` 20 s
ahead, then `effect.commit`). `a3` and `b2` never see the artifact. A validator
failure on a1's result version is injected while a2 holds the artifact
(`early`) or after b1 scheduled its command (`late`), once Core and the monitor
hold every workflow event the nodes recorded (11 to 136 ms after the trigger).

| Strategy, variant | Actions (phase) | Released after confirmation | Effect | Healthy done | Status |
|---|---|---|---|---|---|
| Firebreak, early | hold a2, tars @cluster, kipp @jim-eq (1); cut = {hold tars} (2) | a2, kipp | none requested | 2/2 | contained, cut valid |
| Firebreak, late | hold effect, b1, case @endurance, kipp @jim-eq (1); cut = {b1, case, kipp} (2) | effect hold | scheduled, never committed | 2/2 | contained, cut valid |
| Trace & stop, early | hold tars, a2 @cluster, tars-result park and kipp @jim-eq | none | none requested | 2/2 | applied, cut valid |
| Trace & stop, late | hold effect, b1, case @endurance, kipp, case-result park @jim-eq | none | scheduled, never committed | 2/2 | applied, cut valid |
| Stop team, early | hold a2, tars, a3 @cluster | none | none requested | 1/2 (a3 paused) | applied |
| Stop team, late | hold a3 @cluster (team A had nothing else live) | none | **committed and performed** | 1/2 | applied, cut not valid |

Root cause named a1's invocation (the producer) in every run. Per-action times
after the alert's intake, over all six runs: decision 24 to 104 ms, sent 34 to
109 ms, applied (node clock, same host) 85 to 201 ms, confirmed 124 to 267 ms;
Firebreak sent its releases 261 to 272 ms after the alert and only after every
action of the chosen cut was confirmed. No action was unconfirmed. Controller
leases were enforced and renewed on all nodes throughout. Each run's JSON holds
the full decision (ranking, contamination, G_C statistics, per-site
enforcement reports) and the nodes' own `containment.list`.

Reproduce: `PATH=<nats-server dir>:$PATH python -m e2e.containment.c2
--strategy firebreak --variant late --out result.json`; the opt-in pytest
`aggregator/tests/test_containment_e2e.py` (`RUN_CONTAINMENT_E2E=1`) checks the
Firebreak properties for both variants.

## Defects found by the end-to-end run

| Defect | Status |
|---|---|
| Monitor: the thread could start between the collector creating `trace_collector` and inserting its singleton row, then fail with `monitor_lifecycle_fault` (1 of 4 cold starts in a dedicated check, 2 of 6 scenario runs before the fix) | fixed: readiness requires the row |
| agentd (pre-existing): `AGENT_INBOX` subjects are updated by read-modify-write; single-client nodes starting together on one broker drop each other's subjects, and deliveries to the lost agents fail with `NoStreamResponseError` | not fixed; the harness starts nodes one at a time and verifies the subjects; runbook step 4.4 |
| agentd (pre-existing): admin writes such as `connector.register` use a deferred transaction that reads before writing, so a concurrent write by the telemetry connection makes them fail with "database is locked" | not fixed; the harness retries; runbook known gaps |
| `m2/agentd`: real-NATS control test used the old target shape | fixed (see above) |

## Hooks a recovery milestone will need

Re-running the affected work once a corrected artifact exists needs:

- an incident close/release operation in the monitor: today phase-2 actions are
  never released and the monitor's `in_force` is only changed by its own
  decisions (the runbook's manual release leaves it stale);
- the affected set per incident: the decision's `contamination` summary (live
  and silent invocations, versions, effects) and the held tasks, persisted in
  `containment_incidents`/`containment_actions` and the decision ledger;
- an alert or API that names the corrected version for a revoked one, so the
  monitor can stop treating consumers of the new version as tainted;
- a rerun path in agentd and the handlers: a held task can be cancelled
  (`cancel` is mapped) and its parent resumed (`context.resumed`), but a resumed
  handler that derives child IDs from its task (as Pi and the scripted agents
  do) would reattach to the cancelled child, so a rerun needs a new child
  identity or an explicit redo marker;
- held scheduled effects need a decision per effect (release to commit, or
  deny); committed effects are not reversible;
- unconfirmed actions must be reconciled with each node's `containment.list`
  before releasing anything, and the owner's fence must continue (a fresh
  monitor database needs a new owner).

## M4 design changes (`m4/design`, from `m3/integration` at `1b24f02`)

Four changes the team approved after an external review of the theory, plus two
baselines. Each item is its own commit; this section records what changed and
why.

### 1. Fast-path conditions are checked, not assumed

The single max-flow is exact only if every grouped action is gated (its removal
set is equivalent, for source-sink connectivity, to edges at one port) and the
gate sets at every port are laminar.

- Vocabulary: the projection never offers `revoke:authority` (a delegation
  subtree across invocations; with per-site deadlines it has no gate) or
  `cancel:task`; `_Builder.allowed` refuses them. Descendants are cut through
  their own per-invocation actions and taint-aware admission.
- Solver (`solver.solve`): classifies every action on the actual instance as
  private (one edge), gated at a port (members share a tail or a head, G1; or a
  dominating member, G3, whose head must not be a source, which the previous
  check missed), or an exception; then keeps, per port, gate sets largest first
  while they nest with or avoid every kept set. With no exception it runs one
  max-flow on the laminar chain gadget of theory 4.3 (an edge leaves through its
  tail's out-chain, crosses its private actions in series and enters through its
  head's in-chain). The previous series split of multiply covered edges is gone:
  it was exact but turned nested vertex-local families (theory example 6.1) into
  ungated groups. With exceptions it enumerates them (`enum_limit`, 12) or runs
  the ILP over the timely masks (HiGHS, now in `aggregator/requirements.txt`);
  the greedy fallback without HiGHS is reported `approximate`.
- Deadlines: `ControlGraph.masks()` gives, per action, R(a) and its timely mask
  R_tau(a) = {e in R(a) : now + tau + L(site(a)) < d(e)}; the cut instance is
  built from the masks. An action in force keeps its whole R(a). No action
  removes another's edges (no tree semantics). G_C stats count
  `partial_actions` and `untimely_actions`.
- Reporting: every solve records `solver`, `fast_path`, `fast_path_reason`
  (`ungated`, `non_laminar`, both, or `no_cut_solver`/`heuristic` for rules and
  the greedy), `exceptions` and `exception_actions` in the decision's `solve`
  (last round) and `solves` (every round), hence in the ledger and in status
  `last_solve`.

Tests: exhaustive comparisons on random gated, ungated and multiply covered
instances and on 200 random nested-scope instances (all on the fast path); a
Set-Cover-at-one-vertex instance that must leave the fast path
(`non_laminar`); the source-headed dominating member; the ILP route above the
limit; per-action masks and in-force masks on random workloads.

### 2. Effects executed by the gateway, fenced at commit

- `effect.execute` (`schemas/trace-effect-execute-request.v1.json`): the
  gateway performs a registered effect through an adapter
  (`agent-runtime/src/edgecitadel_agentd/effect_adapters.py`): `homeassistant`
  (REST service call, `<domain>.<service>/<entity_id>` targets, long-lived token
  from `token_file`, default `http://127.0.0.1:8123`) and `http` (POST of the
  committed effect's record to a configured ledger URL). Configuration:
  `EDGECITADEL_EFFECT_ADAPTERS` (JSON file, no secret in it). The worker never
  sees a credential; no token is logged or returned.
- Commit fence: a new store-owned `control_lock` replaces the transport's
  publication lock. Containment commands, the outbox's last check before
  publishing, and the gateway's re-check + commit transaction all take it, so a
  command confirmed before an effect's commit wins and an effect committed
  before a command is in the journal before the command's position (item 3).
  Effects committed but still in their adapter call appear in confirmations as
  `effects_in_flight`.
- Scheduled effects: `effect.execute` with `scheduled_at_ms` keeps the effect
  in `scheduled_effects` (data encrypted); the gateway's scheduler thread
  re-checks, commits and performs it when due, also after the issuer ended. An
  effect hold in place when it is due cancels it (`effect/denied`, reason
  `effect_held`). The monitor now models pending effects whose issuer ended
  (effect hold only).
- `effect.authorize` stays for effects the harness performs (bash): immediate
  only, not fenced at commit; documented as a limitation, with bypass measured
  at the destination ledger. Its scheduled form and `effect.commit` are removed.
- Events: `effect_version` 2 adds `adapter` and `committed_at_ms`; the denial
  reasons gain `effect_held`. Reply results gain `adapter` and `outcome`.
- agentd schema 33 (columns only); the migration denies effects a harness had
  scheduled for the removed `effect.commit`.
- e2e C2: b1 schedules its actuator command with `effect.execute`; endurance's
  gateway posts it to the harness's actuator ledger, so the ledger is written
  by the destination, never by an agent.

### 3. Confirmations carry the journal position

- agentd: every confirmation has `journal: {source_epoch, source_seq}` (null for
  rejections): the node's active source epoch and the latest journaled sequence,
  read under the control lock after the decision committed. Events that passed
  the decision's gates before it took effect are at or below it (they are
  decided under the same lock and journaled in the same transaction).
- Monitor: the follower records, per node and source epoch, a contiguous
  watermark of applied sequences (`fence.SourceProgress`, table
  `containment_sources`, in the batch transaction). `ContainmentMonitor.fence`
  follows Core until every confirmed site's position is covered or
  `ingest_timeout_ms` (default 5000) passes. The protocol calls it in place of
  the former one-shot reconcile after phase 1 and before the P3 check; a
  phase-2 fence timeout ends the decision `ingest_timeout` with every phase-1
  hold kept. Records carry `t_fenced`, sites `journal` and `t_ingested`;
  status shows the watermarks under `ingest`.
- A gap that never closes (an event lost before Core collected it) leaves later
  decisions on that node unfenced: their phase-1 holds stay (safe, reported).
  The watermark starts at the first sequence seen per epoch.

### 4. Admission invariant between the phases

Invariant: any new dispatch, effect or spawn requested by an invocation on the
source side of the cut in force is held or denied before it can propagate or
act, until a cut covering it is confirmed.

How it is realized (checked, then completed):

- agentd denies, at its node, every dispatch, effect (`effect.authorize` and
  `effect.execute`), result and claim of a held task (`execution_held`), and
  every dispatch, effect and result of an egress-held one (`egress_held`).
  Effects are re-checked under the control lock at commit (item 2).
- Phase 1 already pauses every source-side unit whose input lies in the
  uncuttable closure U: the early-window split moves its emissions before the
  hold lands to private copies, so its task hold is always feasible for what
  follows and is in the frontier block. Its emissions before the landing are
  crossings found by reconciliation (fence logs, item 3). Pending tasks in U
  are held before they can be claimed (no spawn).
- Gap found: a unit whose node does not offer `hold:task` stayed unheld in
  phase 1 (its spokes carry only revocations), so its new dispatches and
  effects passed until phase 2 landed. Fix: the phase-1 projection (`freeze`)
  puts the reversible `hold:egress` on egress edges, and `admission_freeze`
  adds, for every unit whose output lies in U, the hold G_C offers on its
  egress (running) or start (pending), regardless of deadlines.
- After phase 2: phase-1 holds are released only once a confirmed and fenced
  B_2 separates the sources (closed world: B_2 covers the unit's modelled
  dispatches and effects). `taint_aware_admission` (egress revocations of every
  unit B_2 leaves on the source side) covers the open world.

Test: `test_a_tainted_invocation_is_admitted_nothing_between_the_phases`
(real agentd nodes, admin RPC): right after the phase-1 confirmations the
tainted invocation asks a new dispatch and a new `effect.execute`; both are
denied (paused, or egress-held where the node cannot hold tasks), the ledger
receives nothing, no request is queued, and phase 2 then contains with every
action fenced.

Theorem 5 obligations (theory appendix v2, A.6), checked against the monitor
and agentd:

- **(a) Phase 2 treats edges removed by confirmed in-force actions as
  d = infinity: fixed.** The projection propagated arrival times through
  edges whose action was in force, so the immediate emissions of a
  phase-1-held unit gave every replacing action behind it a deadline of now
  (untimely), and phase 2 could only keep the hold. `arrival_times` now stops
  at edges whose action is in force; deadlines behind them are infinite and
  the replacing actions timely. Test:
  `test_a_replacing_action_behind_a_confirmed_hold_is_timely` (fails without
  the fix). The fix exposed a weak convergence definition in
  `test_releases_only_after_every_phase_two_confirmation` (it counted only
  phase-1 actions still in force at the end, not those released later), now
  corrected.
- **(b) Q covers the source side S, not only the confirmed taint:
  satisfied.** Labels are computed by reachability, not from the taint set: in
  phase 1 the frontier block and `admission_freeze` hold every unit of the
  closure U reachable from the taint through edges no action removes (which
  includes units not yet tainted), and taint-aware admission in phase 2 uses
  the G_C closure with the actions of B_2 removed, which contains S. Test (the
  counter-scenario of grouped_cut_v2 section 8.3 step 2):
  `test_phase_one_labels_the_source_side_not_only_the_taint` (real agentd
  nodes): a parent whose tainted child result has reached its node but is
  unread is not tainted, yet phase 1 holds it and its new dispatch between the
  phases is denied (`execution_held`).
- **(c) A label push that enlarges S is confirmed before the release that
  enlarges it: satisfied only with `taint_aware_admission: true`.** The
  labels of phase 2 are ordinary phase-2 actions (egress revocations of the
  units B_2 leaves on the source side), so they are confirmed and fenced
  before any phase-1 release. With the default (`false`), releasing the
  phase-1 holds enlarges S with no label pushed; that is safe only under the
  closed-world assumption that B_2 covers every dispatch and effect a released
  unit can make (the G_C model). Runs that claim Theorem 5 must set
  `taint_aware_admission: true`. A native root with no agentd task cannot be
  labelled (agentd's labels are task holds and egress revocations).

### 5. Shared mechanisms and two new baselines

(Superseded in M5 by per-strategy mechanism profiles; the `shared` profile
keeps the behaviour described here, with taint-aware admission on.)

A strategy now chooses only its phase-2 actions. Everything around that choice
is applied identically to every strategy: the engine wraps each strategy's
result in `strategies.shared_mechanisms` (best-effort guard on the sinks
unavoidable in the deadline-aware G_C, then taint-aware admission on the units
the actions leave on the source side), and the two-phase protocol runs for
every strategy when `phase1_freeze` is on (new setting, default true; false
applies every strategy's actions directly). Before, only the three cut
strategies ran phase 1, the guard and the admission. Consequence for the
experiments: a rule whose actions are not a cut now ends
`phase2_check_failed` with the shared phase-1 freeze kept (the same freeze for
every strategy), so strategies differ in what their rule holds and costs, not in
whether a freeze was available; set `phase1_freeze: false` to measure the rules
alone (for all strategies at once).

New baselines (same vocabulary, `--strategy` in the C2 harness):

- **Trace & quarantine** (`trace_and_quarantine`): Trace & stop plus a
  persistent quarantine of the tainted region: every live unit in the declared
  taint closure is paused (`hold:task`) and its egress revoked
  (`revoke:egress`: all new dispatches and effects denied). No optimization.
- **Sink guard** (`sink_guard`): on alert, without tracing or optimization:
  hold every pending scheduled effect at its gateway (`hold:effect`), revoke
  every agent's protected effect classes at its gateway
  (`revoke:grant a>fx:<class>`), and park every result delivered into a
  delegation root (`hold:task` of the child at the root's node), i.e. the
  protected outputs the roots return to their requesters.

Strategy-mechanism matrix (Y: applied, identical for all; config: same setting
for all; rule: what the strategy itself decides in phase 2):

| Strategy | Phase-2 rule | Uses taint | Deadline-aware | Optimizes | Action vocabulary | Phase-1 freeze + admission freeze | Confirm, fence (ingest), check, then release | Best-effort guard | Taint-aware admission | agentd gates (holds, revocations, filters, leases, commit fence) |
|---|---|---|---|---|---|---|---|---|---|---|
| Reject source | park the roots' outputs and effects | roots only | no | no | Y | config | Y | config | config | Y |
| Stop team | hold the root agent's team | roots only | no | no | Y | config | Y | config | config | Y |
| Stop everything downstream | every action in the full G_C closure | yes | no | no | Y | config | Y | config | config | Y |
| Trace & stop | every action in the declared closure | yes | no | no | Y | config | Y | config | config | Y |
| Trace & quarantine | Trace & stop + pause and egress-revoke the tainted units | yes | no | no | Y | config | Y | config | config | Y |
| Sink guard | hold pending effects, revoke protected classes, park root inputs | no | no | no | Y | config | Y | config | config | Y |
| Greedy cut | greedy cover | yes | yes | heuristic | Y | config | Y | config | config | Y |
| Cut without deadlines | exact cut, every element feasible | yes | no | exact | Y | config | Y | config | config | Y |
| Firebreak | exact deadline-aware cut (checked fast path, else enumeration/ILP) | yes | yes | exact | Y | config | Y | config | config | Y |

Defaults: `phase1_freeze: true`, `best_effort_guard: true`,
`taint_aware_admission: false`. Test:
`test_every_strategy_shares_the_vocabulary_and_mechanisms` (the phase-1 set and
the first round's guard are identical across all nine strategies on one
workload), plus one test per new baseline.

### Findings from the experiment harness (`m3/harness`)

- **Lineage through agent memory.** In the harness's C2 realization a producer
  pushes its item to the consumer as a delivery task; the consumer's delivery
  invocation stores it in agent memory and a later invocation of that agent
  uses it. Nothing recorded `used` on that later invocation, so the backward
  slice from the symptom stopped at the consumer's session and the producer
  was never a root-cause candidate. Fixed two ways, both explicit: the trace
  RPC `trace.use` (`schemas/trace-use-request.v1.json`) lets the consuming
  invocation declare the delivered task it used, and agentd records `used` of
  that task's request version on it (only for deliveries to the same agent,
  idempotent); and the monitor setting `shared_memory_agents` links all
  invocations of an agent as one session across conversations, for agents
  whose memory is shared. Test: `test_containment_rootcause.py`
  (`pushed_artifact` cases: without either, the producer is not a candidate;
  with either, it is). Harness follow-up: the det-worker's consuming step must
  call `trace.use` with the delivery's task ID, or the run's config must list
  the agent in `shared_memory_agents`.
- **Ingest lag.** The trace exporter looked for new journal events once per
  second when idle, so Core saw events up to a second late, and decisions and
  fences (item 3) waited on it. The idle interval is now
  `EDGECITADEL_TRACE_EXPORT_IDLE_MS` (default 100, 10 to 60000). Measured with
  nats-server 2.14.6, journal commit to delivery at Core's subscriber: mean
  493 ms, p95 958 ms at 1000 ms; mean 23 ms, p95 48 ms at 100 ms.
- **Leases in every run.** With fail-closed leases on all nodes, a monitor
  that renewed only once the collector was ready, or a run that disabled
  control to avoid enforcing, left the nodes denying every effect. The monitor
  now renews from its start (before the collector is ready, to the
  `agent_nodes` nodes) whether or not an alert ever arrives, and
  `enforce: false` computes and logs decisions without sending any command
  while describes and renewals still go out. Harness follow-up: keep
  `EDGECITADEL_CONTROLLER_LEASE_MS` on and configure control with
  `enforce: false` for non-enforcing runs. Tests in
  `test_containment_service.py`.

## M5 (`m5/integration`, from `m4/design` at `77f479f`)

### Per-strategy mechanism profiles (supersedes item 5's sharing)

Sharing the phase-1 freeze, the guard and taint-aware admission with every
strategy conflicts with the evaluation plan, in which each strategy family adds
one ingredient and the no-lineage baselines get nothing lineage-based. The three
mechanisms are now set per strategy (`config.Mechanisms`). `profile` sets them
for every strategy; `mechanisms` overrides them per strategy (or `"*"`):

- `deployed` (default, main runs): every rule-based baseline applies its own
  rule directly; the three cut strategies run the two-phase protocol with the
  guard and taint-aware admission (needed for Theorem 5, theory A.6 (c)).
- `shared`: all three on for every strategy (taint-aware admission included,
  unlike the M4 default), an ablation in which strategies differ only in the
  phase-2 rule.

Theory A.6 obligation (c) therefore holds by default for the three cut
strategies (the only ones Theorem 5 is claimed for). Confirmation, fencing
(ingest-then-release) and agentd's gates stay on for every strategy in both.
Each decision records `profile`, `mechanisms`, `phase1_mode`,
`phase1_labels` and `receding_during_waits` (decision log, status `last_decision`; status also
lists every strategy's mechanisms), and the C2 run output and the harness's
`run.json` carry them.

| Strategy | Family | Phase-2 rule | `deployed`: freeze / guard / taint-aware admission | `shared` |
|---|---|---|---|---|
| Reject source | no lineage | park the roots' outputs and effects | off / off / off | on / on / on |
| Stop team | no lineage | hold the root agent's team | off / off / off | on / on / on |
| Stop everything downstream | no lineage | every action in the full G_C closure | off / off / off | on / on / on |
| Sink guard | no lineage | hold pending effects, revoke protected classes, park root inputs | off / off / off | on / on / on |
| Trace & stop | past lineage | every action in the declared closure | off / off / off | on / on / on |
| Trace & quarantine | past lineage | Trace & stop + pause and egress-revoke the tainted units | off / off / off | on / on / on |
| Greedy cut | past lineage + future authority | greedy cover, deadline-aware | on / on / on | on / on / on |
| Cut without deadlines | past lineage + future authority | exact cut, every element feasible | on / on / on | on / on / on |
| Firebreak | past lineage + future authority | exact deadline-aware cut | on / on / on | on / on / on |

For every strategy, in both profiles: the same G_P, X_t, roots, G_C and action
vocabulary; actions confirmed, fenced and enforced by agentd's gates (holds,
revocations, filters, leases, commit-time effect fence). A rule applied directly
ends `applied` (with `cut_valid` reported); a two-phase decision ends
`contained`, `phase2_check_failed`, `phase2_unconfirmed` or `ingest_timeout`.
Tests: `test_the_deployed_profile_adds_mechanisms_only_to_the_cut_strategies`
and `test_the_shared_profile_isolates_the_phase_two_rule`.

### Phase-1 modes (`phase1_mode`) and receding additions during the waits

Options only; the team decides the default after the simulator and theory
checks. They apply to the strategies that run the two-phase protocol
(`phase1_freeze`); a rule applied directly has no phase 1.

- `freeze` (default, current): the frontier freeze B_1 (frontier holds and
  admission freeze).
- `freeze_plus_cut` (V2): B_1 together with the at-alert cut B_a, the
  strategy's own solve on the phase-2 G_C at the alert (Firebreak: its
  deadline-aware cut), both in the first round. More phase-1 actions only
  shrink the source side, so Theorem 5 is unaffected.
- `adaptive` (V3): at the alert only B_a and labels covering its source side
  S(B_a) = every vertex reachable from the taint through edges B_a does not
  remove in time (its timely masks), not only U: admission holds (hold:egress
  on running units, hold:task on pending ones, released like the rest of phase
  1) pushed with B_a. The frontier freeze is added when the at-alert solve
  leaves a protected sink (a pending effect) untimely or doomed, or when the
  earliest tainted-effect deadline lies within `phase1_margin_rounds`
  (default 2) control rounds of B_a's landing (a round is the largest control
  latency of B_a's sites), or when the convergence check fails.

  Convergence at tau_1 is checked literally (theory grouped_cut_v2 8.6,
  appendix (J)): (C1) every relied-on action is confirmed and its nodes'
  journals are ingested through the confirmed positions; (C2) labels are
  confirmed at every site hosting a vertex of S(A(tau_1)) on the reconciled
  graph (missing labels are pushed and the check repeated); (C3) taint is
  reconciled under (J'): before every solve and check the projection marks
  as tainted the head of every known commit whose tail is tainted or on the
  source side, whatever order the per-site journals arrived in (a version
  used while its producer's journal is missing is attributed to its producer:
  the dispatcher for a request, the child for a result; and commits of
  source-side invocations at or after their earliest possible taint); (C4)
  the actions in force are a cut on the reconciled graph, every admitted edge
  included. On failure the freeze is added and the check repeated. Each
  round's check is recorded under `phase1_trigger.checks`; the trigger's
  `freeze`, `untimely_effects`, `earliest_effect_deadline_ms`, `landing_ms`,
  `margin_ms` and `freeze_after_check` say why the freeze was (not) added.
  Phase 2 keeps the literal masks: d is infinite only behind edges held by
  confirmed in-force actions (M4 fix (a)); elsewhere the deadlines and timely
  masks bind as at the alert. (J') over-taints (cost) and is applied only in
  this mode; under the freeze every edge that can move taint is covered and
  the fence makes the reconciliation exact. Limitation: vertices with no
  agentd identity (a tainted conversation's future tasks, invocations beyond
  the horizon) cannot carry a label; their creating edges come from labelled
  units, or they are unavoidable sinks.

`phase1_labels` (every mode; default `frontier`): `frontier` labels U (freeze
modes) or S(B_a) (adaptive); `forward_closure` labels every unit of the
forward closure of the sources, which removes the benign refused releases of
the post-release label race (theory 8.6 (2c); about 0.1 % of simulated runs)
at the price of more phase-1 holds on normal work. Decisions record
`phase1_mode` and `phase1_labels`.

In every mode every at-alert solve is the `phase: 1` entry of `solves`, phase
2 is unchanged, and the guarded release releases the phase-1 holds the
refined cut does not keep, only after it is confirmed, fenced and checked; a
revocation (in B_a or among the labels) cannot be released and stays.

`receding_during_waits` (default false): a taint advance seen while the
protocol waits for ingest (during either phase) re-solves at once and applies
whatever the strategy adds, never a release. Those additions (listed under
`receding`) are fenced before any release, and an addition made during phase
1 is a phase-1 action like the freeze's.

Tests (adaptive safety): `test_phase_one_labels_the_source_side_not_only_the_taint`
(real agentd, freeze and adaptive with both label settings: a reachable,
untainted parent is labelled in phase 1 and its dispatch between the phases is
denied), `test_a_failed_convergence_check_adds_the_frontier_freeze` ((C4): a
task and its scheduled effect raced B_a; (C1): a fence timeout),
`test_conservative_reconciliation_keeps_a_stale_journal_from_releasing`
(T11 with injected ingest delays on a slow and two fast nodes: fails without
(J')), `test_phase_two_keeps_literal_masks_beside_the_held_edges` and
`test_forward_closure_labels_cover_every_unit_the_taint_can_reach`.

Tests: `test_every_phase_one_mode_blocks_the_late_effect_and_releases_after_phase_two`
(in-process, late alert, each mode: the effect hold is in the first round,
the effect's sink is separated at every instant the nodes applied from then
on, releases are holds sent after the last phase-2 confirmation),
`test_the_cut_modes_add_the_at_alert_cut_to_the_frontier_freeze`,
`test_adaptive_phase_one_freezes_only_when_the_cut_is_not_safely_timely`
(no freeze for a timely cut; the freeze for an untimely effect and for one
within the margin), `test_receding_additions_during_the_waits_are_fenced_before_any_release`
and, opt-in on the multi-process C2 (late variant, `freeze_plus_cut` and
`adaptive`),
`test_phase1_with_the_at_alert_cut_blocks_the_late_effect_from_the_first_round`.

### Convergence and release checks cover every protected sink (safety fix)

The simulator team found that the check used for phase-1 convergence and for
the guarded release (`cut_holds`) left out the sinks the deadline-aware graph
judged unavoidable, so it could declare tau_1 while a doomed protected effect
was still reachable (with the monitor's semantics, 12 tainted effects after a
"converged" tau_1 in V1; 0 with the theory's check). Now:

- The tau_1 convergence check (every phase-1 mode; (C4) of the adaptive one)
  and the release re-check test separation from every avoidable sink and from
  every protected effect, doomed or not. Only unavoidable boundary sinks (new
  invocations at the horizon) stay out, as in the solve.
- A sink still reached means not converged (or no release): the holds stay,
  the last hop into each blocking sink is guarded (every action on an edge
  into it, timely or not, since deadlines are lower bounds), the check runs
  again, and otherwise the freeze is (re)computed for the next round; in phase
  2 the guard is added and the cut solved again. A sink no action can reach
  keeps every hold (`phase2_check_failed`).
- The decision records every check under `convergence` (`phase`, `round`,
  `converged`, `blocking_sinks`, and the `guard` it added); the adaptive
  checks also carry `blocked_by` and `guard`.
- Phase 2 sees an admission label in force as held: an egress stopped by an
  action in force keeps that action in the phase-2 graph (a phase-1
  `hold:egress` label as much as a `revoke:egress`), so flows behind it have
  d = infinity and the label counts in the checks.
- Every action added during the waits (receding additions, label pushes,
  guards) must be in force before the guarded release; otherwise nothing is
  released (`receding_unconfirmed`, or `phase2_unconfirmed` when phase 2 asks
  for it again).

Tests: `test_a_doomed_sink_reachable_through_an_unheld_edge_blocks_convergence`
(an effect due before any control lands, judged unavoidable at the alert and
reachable only through its unheld last hop: round 1 reports it blocking,
the guard holds it, then phase 1 converges; fails with the old check),
`test_a_sink_no_action_can_reach_keeps_every_hold` (fails with the old check),
`test_phase_two_sees_an_admission_label_in_force_as_held`,
`test_receding_additions_must_be_confirmed_before_any_release`, and the
adaptive (C4) cases in `test_a_failed_convergence_check_adds_the_frontier_freeze`.

### The experiment harness on M4 (merged from `m3/harness`)

`m3/harness` (`a21fc29`) is merged; its harness and det-worker now run on
this branch's runtime and monitor (`HARNESS_RUNTIME_ROOT` is this checkout):

- Registered effects go through agentd's gateway (`effect.execute`): the
  stack's `effect_sink` is the run's ledger (the gateway's `http` adapter
  posts each committed effect to the services' `/sink/<token>`, which records
  it as `committed`) or, for the physical case, Home Assistant
  (`configs/lab-homeassistant.json`: the gateway's `homeassistant` adapter
  calls the services' Home Assistant front, which forwards the service call to
  `HARNESS_HA_URL` without the worker's `ec_*` attribution keys and records
  what Home Assistant accepted; the token stays in `HARNESS_HA_TOKEN_FILE` on
  each edge host; a dry run answers for a per-run token). The worker records
  `attempted`, `scheduled`, `denied` and `sink_failed`; only the sink records
  `committed`. Each run also collects the gateway's decisions from Core
  (`gateway_effects.jsonl`).
- Consuming steps report inputs taken from agent memory with `trace.use`
  (the delivery's task), so lineage links them to their producers.
- Leases: every node enforces `EDGECITADEL_CONTROLLER_LEASE_MS=30000`
  (`controller_lease_ms`); the monitor is configured with control in every
  run and `enforce: false` for runs without containment (`none`,
  `reference`); work starts once every node's lease is renewed.
- Runs carry `profile`, `phase1_mode`, `phase1_labels` and
  `receding_during_waits` (grid dimensions; `parts` add sub-grids); the
  monitor's mechanisms per strategy are recorded in `run.json`.
- Grids: `lab-smoke` (every strategy x scenario x early/late alert once
  under the deployed profile, plus C2 under the shared profile: 165 runs),
  `lab-pilot` (3 repetitions: 495 runs), `lab-main` a draft (size after the
  pilot).

Local dry run (`configs/dryrun-nats.json`: every process on one 2-CPU
container, NATS control path; nats-server 2.14.6; shared with simulator jobs
of other agents, load average about 4): `lab-smoke` ran 113 of its 165 runs
(every scenario: c2, c3, n2, n3 complete, rand-n7-s0 early alert complete,
rand-n7-s1/s2 sampled; C2 under the shared profile complete) and `lab-pilot`
its first 4 runs. One attempt takes 67-85 s for c2, c3, n2, n3 and 93-120 s
for rand-n7 at `time_scale` 2 (stack start, workload, quiet period, collection).
On this machine 37 of 113 runs needed the harness's testbed retry: worker slots
lost agentd (RPC timeouts of 5 s under CPU starvation; 91 % of rand-n7 runs,
whose many slots each poll `task.claim` every 0.5 s), and two rand-n7-s1 runs
never reached their alert. At one attempt per run, `lab-smoke` takes about
4.1 h and `lab-pilot` 12.4 h; the lab spreads the nodes over three hosts.
Scores of the dry run: Firebreak clean and established in 13 of 13 runs, the
three cut strategies and Trace & stop/quarantine clean in every run, Reject
source clean in 2 of 9 under the deployed profile (its rule alone) and 2 of 2
under the shared one; `none` leaked in 12 of 13.

## M6 (`m6/service`, from `m5/integration` at `e445457`)

### Defaults: the adaptive phase 1 for the deployed cut strategies

`phase1_mode` is unset by default (`MonitorConfig.phase1_mode_for`): the
profile decides (`PROFILE_PHASE1_MODES`). Under `deployed` the three cut
strategies (Greedy cut, Cut without deadlines, Firebreak) run the adaptive
phase 1 (V3) with the theory's conditions; the rule-based baselines apply
their rule directly as before. Under `shared` (the ablation in which
strategies differ only in the phase-2 rule) every strategy keeps the frontier
freeze, so they still share one phase 1. `freeze` and `freeze_plus_cut` stay
settable as ablations. The (J') reconciliation is used only inside an
adaptive decision. Harness: `lab-smoke` (169 runs) and `lab-pilot` (507) run
the deployed profile with the default and gain an ablation part (C2,
Firebreak, `freeze` and `freeze_plus_cut`); run ids carry a mode suffix only
for a non-default mode; `run.json` records the mode each run used.

### Per-event admission for labelled units ((AI) with the guarded release (R))

Before M6 an admission label did not admit anything event by event. Under
`hold:egress` agentd denied each new dispatch and each effect outright
(`egress_held`), refused the unit's result (its terminal transition) until the
label was released, and withheld its outbox messages until then; under the
`revoke:egress` that taint-aware admission placed, dispatches and effects were
denied for good and results flowed. A labelled unit's egress was therefore
blocked until the guarded release, whatever the event.

Now (schema 34):

- agentd holds each new event of a unit under an egress label, once the
  event passes every other gate: an effect (`effect.execute` replies
  `state: held`; the label is checked right after the task hold, so a
  labelled effect is held whatever else would deny it, and it is checked
  again without the label once admitted), a dispatch (the child task is
  created but its request is not published and a local recipient cannot
  claim it; the label is checked last) and a result (the terminal transition
  is refused once and held; the managed runtime retries it). Each held event
  gets its own hold (`kind: event`, object the held event's id, owned at the
  label's owner and version), a `held_events` row and a `containment/held`
  journal event in the same transaction, so the event enters G already
  removed. Releasing the event hold admits that event alone; agentd then
  publishes the dispatch, lets the result through, or performs the effect
  after its other checks.
- The monitor (`admission.py`, `ContainmentMonitor.admit_held`, every step
  while enforcing) decides each held event not yet decided under the current
  policy version: an effect of an unprotected class is admitted; one of a
  protected class is kept while its invocation is on the source side of the
  actions in force; a dispatch or a result (head: the new task, or the live
  parent that would consume it) is admitted only after every unit the head
  reaches past the actions in force carries a label, confirmed and fenced
  (a unit that cannot carry one keeps the event held), and the head reaches
  no avoidable or protected sink. An event whose invocation is not on the
  source side is admitted. A kept event is decided again when the policy
  version changes. Each decision is an `admission` record in the decision log
  (decision, reason, blocking sinks, labels pushed for it, node-clock hold
  time, decision time, release sent and applied, latency); the harness
  reports `n_events_admitted`, `n_events_kept` and the admission latency
  (median, p95).
- Taint-aware admission now places labels (`hold:egress`) instead of
  `revoke:egress`, so a tainted unit keeps serving unprotected work; the
  adaptive phase-1 labels no longer pause pending effects (their effects are
  held per event).
- The projection removes a pending scheduled effect at its commit when its
  class is revoked for the agent, its credential is revoked or its issuer is
  labelled (commit gate), so such an effect no longer forces the freeze.
- The guarded release also releases phase-1 holds that never confirmed
  (`hold_not_found` counts as released), which leaked worker slots.
- Cost: `future_requests` (expected future requests per agent pattern) times
  `future_request_value` is added to revoking that agent's credential or
  admission; pausing one of its invocations or revoking its egress costs one
  request; labels and grant revocations cost none.

The managed runtime offers a labelled result again every watch interval; a
labelled task that a frontier pause caught after its handler finished now
offers its result again once the pause ends (it waited for the label too,
which stays in force, and kept its worker slot: the first svc dry run of
Greedy cut stalled the assistant with six such slots;
`test_a_labelled_result_paused_at_commit_is_admitted_after_the_pause`).
Limitation: held events are decided in the monitor's step, so they wait while
a decision's two-phase protocol runs (its waits for confirmations and
ingest); with slow control rounds this adds to their admission latency.

Tests: `test_a_labelled_units_reply_is_admitted_and_its_device_command_stays_held`
(real agentd: the reply is admitted after one round, the device command whose
class the cut blocks stays held),
`test_a_labelled_dispatch_is_admitted_once_its_new_task_is_labelled`,
`test_a_released_decision_also_releases_its_unconfirmed_holds`,
`test_a_pending_effect_whose_class_is_revoked_is_refused_at_its_commit`,
`test_stopping_a_service_costs_its_requests_and_a_label_does_not`; agentd:
`test_an_egress_label_holds_each_new_event_until_it_is_admitted`,
`test_a_held_local_dispatch_cannot_be_claimed_until_admitted`,
`test_a_labelled_effect_is_held_then_performed_once_admitted`,
`test_an_admitted_effect_is_checked_again_without_its_label` and
`test_a_label_placed_before_a_scheduled_effect_is_due_holds_it`. The safety
tests (every phase-1 mode, convergence and release checks) still pass.

### Service-continuity scenario `svc` (pre-registered)

`harness/svc.py` generates `workloads/svc.json` deterministically from
`PARAMS` (seed 20261021): an assistant (site K, one det-worker whose memory is
shared across invocations, `shared_memory_agents`) serves Poisson user
requests (0.5/s in [5, 85] s: 54 requests, each 1 s of compute and a reply;
30 % also command the desk lamp); an automation agent (site A) commands two
other devices every 5 s; a planner (site P, the root) hands the assistant a
lamp task at 10, 30, 50 and 70 s and the device schedule at 20 s. Fault: the
schedule artifact is wrong (`sched@v1`); the assistant reads it, and every
later invocation of it is tainted. Alerts: `early` 0.3 s after the schedule is
produced, `late` 15 s after the assistant read it. Protected sinks: device
commands (`device.command`); replies (`network.http`) are unprotected and go to
the ledger as class `reply`. Requests are external: each node's `nodectl
schedule` submits its agent's root tasks at their release times and marks
each submission. The monitor runs with `boundary: defer` (a future request is
handled when it arrives, not as a horizon sink) and `future_requests` = the
request rate times the time left after the alert's nominal time, at
`request_value` 1.0 per request (the same for every strategy).

Pre-registration record: `PARAMS`, the metrics, the cost (`request_value`
1.0) and the grids are as committed in `ee76f03`. One functional dry run
(Firebreak, late alert) came before that commit; it exposed mechanism defects
(unconfirmed phase-1 pauses leaking worker slots, tie-chosen pauses of
tainted requests, pending effects already blocked by a revoked class forcing
the freeze) that were fixed in the monitor, and the svc monitor setting
`boundary: defer` was chosen then: under the default horizon the solver
revoked the assistant's self-dispatch grant, which the planner's hand-offs
need. After the commit, the svc-smoke dry runs exposed two runtime defects
(a paused labelled result never offered again; the ordinary store's write
deadlock, below), both fixed in agentd. No scenario parameter, metric or
cost setting was changed after any result.

Metrics from the alert to the end of the run: `svc_tainted_device_commands`,
`svc_answered` (of `svc_requests`), `svc_automation_committed` (of
`svc_automation_due`), `svc_tainted_replies`, per-request latency and
`svc_added_latency_s` against the uncontained run of the same cell.
Grids: `svc-smoke` (every strategy and `none` once per alert, plus the
reference: 21 runs) and `svc-pilot` (5 repetitions: 105). The physical
configuration `configs/lab-svc-homeassistant.json` sends only the lamp
(`dev_lamp`) through Home Assistant and only on site K's node (`core`); the
automation's commands and the replies go to the ledger on every node.

Local dry run of `svc-smoke` (`configs/dryrun-nats.json`, one 2-CPU machine
shared with two simulator processes, at this branch's final revision): 21
runs in 42.7 min, **114–129 s per run** (stack start, the 85 s workload at
`time_scale` 1, quiet period, collection), no testbed retry, no slot lost,
the scorer checks (`commits_unlabelled`, `commits_unresolved`,
`commits_label_mismatch`) all 0. Per run, from the alert on (requests
answered of those submitted after the alert; added latency per answered
request against `none`):

| Strategy | Early: tainted cmds / answered / automation / added s | Late: tainted cmds / answered / automation / added s |
|---|---|---|
| none | 19 / 45 of 45 / 13 of 13 / 0 | 13 / 33 of 33 / 10 of 10 / 0 |
| Reject source | 0 / 45 / 13 / -0.1 (taint never reaches the assistant) | 13 / 33 / 10 / 0.0 |
| Sink guard | 0 / 45 / **0 of 13** / -0.5 | 0 / 33 / **0 of 9** / -0.2 |
| Stop team, Stop everything downstream, Trace & stop, Trace & quarantine | 0 / **0** / 13 / – | 0 / **0** / 10 / – |
| Greedy cut | 0 / 45 / 13 / 2.6 | 0 / 33 / 10 / 2.5 |
| Cut without deadlines | 0 / 45 / 13 / 3.0 | 0 / 32 / 10 / 2.5 |
| Firebreak | 0 / 44 / 13 / 3.3 | 0 / 33 / 10 / 2.4 |

The cut strategies revoke the assistant's device class and label each of
its tainted invocations; its replies (tainted, unprotected) are admitted one
by one (Firebreak: 78 events admitted and 12 device commands kept early, 60
and 9 late; admission latency p50 1.8–2.3 s, p95 5.4–6.6 s on this loaded
machine), which is the added latency. One Firebreak late run with
`configs/dryrun-svc-homeassistant.json` sent the lamp's 12 commands through
the Home Assistant front on `core` and everything else to the ledger (0
tainted commands, 32 of 33 answered). The first two local svc-smoke runs,
before the runtime fixes below, lost slots in most runs and stalled the
assistant in Greedy cut and Cut without deadlines; they are not results.
`svc-pilot` (105 runs) would take about 3.6 h locally at this rate.

### Fixes from the G1 lab smoke

- **Scorer: leaks are classified by lineage.** The sink copied a committed
  effect's fault labels from the worker's earlier `attempted` record, so a
  commit without one carried no labels and could never count as a leak (its
  ledger entry also lacked the class the scorer read). Every committed sink
  entry is now classified by `groundtruth.commit_lineage`: the runtime task
  the gateway committed for (the gateway's post to the sink, or Core's
  gateway record of the same commit in `gateway_effects.jsonl`), mapped to
  its planned task by the run's worker events and request marks, is tainted
  when the fault reaches it (the fault's seeds, every task the worker events
  show computing on fault-labelled content or polluted, and their declared
  dependents; spawned invocations always). An issuer that cannot be resolved
  counts as tainted. Runs report `commits_unlabelled` (no `attempted`
  record), `commits_unresolved` and `commits_label_mismatch`, each 0 in a
  sound run; the svc metrics use the same classification. Rescoring the
  first local svc-smoke gave the same leak counts with all three at 0.
  Tests: `test_a_tainted_commit_without_an_attempted_record_is_a_violation`,
  `test_the_commit_lineage_joins_cores_gateway_record`.
- **Bind addresses.** In lab mode the run's NATS and services listen on the
  controller's tailnet address only (host field `listen`,
  `HARNESS_CONTROLLER_LISTEN` in the lab configs; the URLs handed to nodes
  use it too), NATS monitoring on the loopback, and Core's API, used by the
  runner alone, on the loopback when the runner runs on the controller (a
  runner elsewhere reaches it at the controller's address). Dry runs bind
  everything to the loopback.
- **A runner on jim-eq without SSH.** A host listed with `helper` is driven
  by `harness.labhelper` running on it, over request/reply on the lab's
  token-protected experiment broker (`configs/lab-helper.json`: Endurance and
  Cluster through helpers, control over the run's NATS). The helper's fixed
  operations: start a harness process (`python -m harness.nodectl` or
  `det_worker`) as a transient `firebreak-exp-h-<run>-<process>` unit (user
  or system units; plain process groups for a dry run) with its environment
  in a mode-600 file of the run directory, check and stop those units, run a
  short `nodectl` command (`configure`, `install`, `operator`, `socket`), and
  write, read (in chunks), list and remove files and run directories, all
  under the host's work root; other programs and paths are refused. SSH mode
  is unchanged; netem profiles are not installed through a helper. The
  existing `deploy/fbx_lab` helper is not changed: `harness.labhelper` is the
  harness's own per-host helper with the same pattern and can run beside it
  on the same broker, as a unit of its own (for example
  `firebreak-exp-harness-helper`).
- **SQLite write contention on agentd (slot losses).** Instrumenting the
  store lock and every SQLite call of the experiment nodes on a local
  rand-n7-s1 run (lab-smoke settings) found the losses' cause: the harness's
  nodes (and `e2e/containment`'s) run an ordinary store, which has no
  completion workspace, and the workspace's lock is what keeps agentd's own
  connection and its telemetry (trace export) connection from writing at the
  same time on a provisioned store. Without it, a session renewal or close
  (writes the task member, after reading the trace member) and the
  exporter's broker-ack checkpoint (writes the trace member, after reading
  the task member) each waited at commit for the lock the other held; both
  failed with "database is locked" after the 5 s busy timeout, with agentd's
  store lock held the whole time, so every RPC queued behind them and worker
  slots timed out. The journal mode (rollback, required for the attached
  pair's atomic commits) and busy timeout (5 s) were already as the store
  contract wants them. Fix (`storage_workspace.ordinary_writer`): every
  connection a process opens to an ordinary store takes one per-file writer
  lock where the workspace lock is taken, so their write transactions are
  serialized as on a provisioned store; persistence semantics and the store
  and control lock order are unchanged. Measured on 6 local rand-n7-s1 runs
  (none, stop_team, firebreak; late alert; 2 repetitions; no retries) on the
  shared 2-CPU machine: before, 102 slot restarts (17 per run, every run
  lost slots), 26 locked commits, RPC waits on the store lock up to 6.5 s,
  and only 1 of 6 runs reached its alert; after, 0 slot restarts, 0 locked
  commits, the longest store-lock wait 1.9 s, all 6 runs reached their
  alert. Test: `test_writes_of_two_connections_to_one_store_do_not_deadlock`
  (fails with both "database is locked" without the lock). Batching the
  exporter's checkpoints was not needed after this.
- **Run end and teardown.** The services track effects the gateway
  scheduled (or held) until the sink commits them or a worker record refuses
  them (`GET /effects/pending`, from the due time the worker now records); a
  run ends only once each is resolved or past its due time plus 5 s. The
  teardown stops workers and nodes first, then Core, then NATS and the
  services, so Core records commits up to the nodes' stop; decisions the
  monitor makes after the run's end (`t_end_ms`; lease alerts for the
  sessions the teardown interrupts) are not scored.
- **Lab runner.** From the G1 jim-eq launcher: SSH hosts with `"units":
  "user"` (or `"system"`) run each process as a transient
  `firebreak-exp-h-<run>-<process>` unit; before every attempt the runner
  stops harness units an interrupted run left on such hosts (never the
  runner's), removes the run's stale directories and waits for the stack's
  fixed ports (busy after 30 s: a testbed failure; it kills nothing);
  `python -m harness unit --log <file> -- run ...` starts the runner as the
  transient unit `firebreak-exp-h-runner`; copies off a Mac use
  `COPYFILE_DISABLE=1 tar`. With the approved SSH route from jim-eq the
  helper mode above is optional.

## M7 (`m7/eventdriven`, from `m6/service` at `0e33433`)

### Hand-offs wake on new work (agentd)

A local dry run measured each EdgeCitadel hand-off at 0.3-1.3 s against
23-56 ms for Google A2A. Two polls caused it: agentd's outbound transport loop
slept 1 s after each batch, and an idle Managed Agent slot asked for a task
every 0.5 s. Besides the latency, this slowed taint propagation and gave every
strategy slack. Both paths now wake on new work:

- `work_signal.WorkSignal` (one per store): a generation counter with waiting
  threads and asyncio wakers. The store notifies it where a task becomes
  claimable or an outbox message publishable: a task created (local, or
  ingested from NATS), session recovery requeuing tasks, an outbox message
  enqueued (`_queue_transport_locked`), a containment hold or label released
  or a held event admitted (`release_hold`), an ingress filter removed, a task
  cancelled. A notification only wakes waiters; they re-read the store under
  its lock, so a notification sent inside a transaction is seen after the
  commit, and one for a rolled-back transaction costs one re-check.
- The outbound loop publishes the outbox when woken. Consumers, presence and
  heartbeats keep their 1 s cadence (`MAINTENANCE_SECONDS`). Each message keeps
  its commit-time re-check under the control lock (`publication_allowed`,
  `_in_flight`), its order (`created_at_ms`) and its at-least-once publication
  (marked published after the broker's ack; a failed publish ends the
  connected loop and reconnects, as before).
- `task.claim` takes `wait_ms` (0 to 2000; 0, the default, claims at once as
  before). A waiting claim sleeps on the signal, checks with a read
  (`has_claimable_task`, the claim's own `CLAIMABLE_SQL`) before it tries
  again, and each attempt is the ordinary claim with its session check. The
  Managed Agent runtime waits up to `EDGECITADEL_CLAIM_WAIT_MS` (1000; the
  client's socket timeout is 5 s); a claim that returns early and empty is
  retried after 50 ms, so it cannot spin.
- Fallback re-checks: `EDGECITADEL_TRANSPORT_POLL_MS` (outbound loop) and
  `EDGECITADEL_CLAIM_POLL_MS` (waiting claim), 50 ms by default (1 to 60000).
  Every write that agentd's own store makes notifies, so the fallback only
  bounds a change written by another connection or process (a restore tool, a
  test's second connection). Why 50 ms: a change that came without a
  notification still meets the 100 ms hand-off budget, and 50 is the cheaper
  end of the 20-50 ms range. Its idle cost is measured below.

Invariants checked before the change (deliberate-changes): the control-lock
order and the commit-time re-check are unchanged, and nothing touches fencing
or the journal positions in confirmations. At-least-once delivery and outbox
order are unchanged. Leases: each claim attempt is the ordinary claim, with
its session check. The M6 writer lock: a waiting claim re-checks with a read,
not a write transaction, and the signal's condition is a leaf lock taken
inside the store lock. Semantics are unchanged.

Compatibility: either side may be older. An older agentd ignores `wait_ms`,
and the runtime then asks every 50 ms; an older runtime claims without
waiting.

Tests:

- `tests/agentd/test_handoff_latency.py`:
  - An idle slot starts a created task with a median below 100 ms (state on
    tmpfs; the fallback is set to 5 s, so only the wake-up can meet it).
  - A task written by another connection is still found by the fallback.
  - `wait_ms` is validated.
  - Opt-in with NATS: an enqueued message reaches the broker with a median
    below 100 ms (the fallback set to 5 s).
- The same test with notifications disabled takes about 745 ms.
- A probe of a local hand-off counts 5-8 SQLite commits up to the handler.
  Most of them are read-only. The hand-off takes 23-44 ms on tmpfs and
  41-75 ms on this VM's disk.

### Measurements (local dry run, before and after)

Setup:

- Stack: `configs/dryrun-nats.json`, with three agentd nodes (core, edge-1,
  edge-2), NATS, Core and the services on one shared 2-vCPU VM. Other
  workloads kept its load average at 1.5-5 throughout. Each before/after
  pair ran within half an hour under similar load.
- Instances: `workloads/chain6.json` (new: a0..a6 on alternating nodes,
  T0..T6 of 50 ms each, six created-task hand-offs, nominal makespan 1.35 s)
  and the C2 no-fault reference (nominal 25.9 s).
- Each instance had 3 repetitions at time scale 1, scored by the M7 scorer.

The scorer's hand-off metrics (`metrics.handoff_latencies`,
`task_handoff_latencies`) are timed on each worker's own clock, shifted onto
the services' clock by the worker's smallest report delay. This keeps the up
to 1 s that a report spends on its way to the services under load out of the
metric.

- Per hop: each message through agentd, from its dispatch (RPC returned) to
  the start of the invocation it delivered (a pending creation, a created
  task, a pushed item's delivery).
- Per created task: both hops from the creator's dispatch to the task's
  start, without the time the pending creation waits for the task's pushed
  inputs.

| run | chain6 hop median (p95) | C2 hop median (p95) | chain6 task median | C2 task median | chain6 makespan | C2 makespan |
|---|---|---|---|---|---|---|
| before, disk | 774 ms (1421) | 980 ms (2179) | 1684 ms | 2051 ms | 13.52 s | 41.96 s |
| after, disk | 431 ms (1288) | 661 ms (1894) | 952 ms | 1776 ms | 10.97 s | 38.65 s |
| before, tmpfs | 238 ms (1187) | 220 ms (734) | 834 ms | 514 ms | 7.18 s | 27.63 s |
| after, tmpfs | **84 ms (185)** | **103 ms (407)** | 187 ms | 260 ms | 3.58 s | 26.62 s |

Sample sizes: chain6 has 36 hops and 18 created tasks; C2 has 135 hops and
48 created tasks. Makespans are the mean of the 3 runs. "tmpfs" puts every
node's state (the harness work root) on `/dev/shm`. Every run completed with
0 slot restarts after the change; before it, the tmpfs C2 runs lost 1 and 3
slots and one chain6 run lost 1.

What remains after the change is storage and CPU. On this VM a durable
commit, as agentd makes it (rollback journal, `synchronous=EXTRA`), has a
median of 32-36 ms to one database and 52-60 ms to the attached pair. On
tmpfs the same commit takes 0.1 ms. A hop is several durable commits across
the sender, the transport and the recipient (create, publication, ingest,
claim, bind, start). So:

- On this disk a hop costs about half a second.
- On tmpfs the median is about 100 ms with the CPU oversubscribed about
  twofold.
- On an idle machine with tmpfs, the unit test's local hop takes 25-45 ms.

The lab gate's 100 ms median therefore depends on the lab's storage.
`python -m harness commit-cost <work root>` measures that commit on each
host beforehand, and the pipeline records the controller's in `gate.json`.

Idle CPU per agentd: chain6 with T0 released at 45 s, sampled over 30 s while
nothing runs. Each node held about 7 idle worker slots. Mean of the three
agentd processes, in % of one CPU, with the det-worker processes beside it:

| code | agentd (core, edge-1, edge-2) | det-worker |
|---|---|---|
| M6 (polls) | 4.94 (5.99, 4.28, 4.56) | 0.61 |
| M7, 50 ms fallbacks (default) | 7.63 (9.01, 6.91, 6.98) | 0.24 |
| M7, 1 s fallbacks | 3.57 (4.28, 3.28, 3.14) | 0.25 |

An earlier pair under heavier load gave 4.90 (M6), 6.52 (M7) and 3.73 (M7,
1 s). The 50 ms fallbacks cost about 3-4 points of a CPU per idle agentd (the
waiting claims' re-checks, about 20 per second per slot). The workers no
longer poll. With `EDGECITADEL_CLAIM_POLL_MS` and
`EDGECITADEL_TRANSPORT_POLL_MS` at 1000, idle agentd is below M6 and every
in-process change still wakes at once. The defaults stay in the requested
range.

Consequence for the svc scenario: its early alert is 0.3 s after the
schedule is produced. With the M6 polls the assistant's `A.sched` was
probably still queued then, so input blocks (Reject source, Trace & stop) caught it. With
fast hand-offs it may already have started. The early alert's meaning moves
with hand-off speed; compare svc runs only within one code version.

### Version ids are per task (side-experiment gap, D)

`trace_lineage.version_id` hashes the entity, the task id and the content. The
same stale content re-served in a new task therefore gets a new version id,
and a detector that matches revoked version ids (`revoked_version_consumed`)
misses it.

This does not affect C3, any grid scenario or RQ1:

- Every alert in the grid, including C3's `stale_version` fault on `V@v1`
  produced by P, is an operator alert with a designated root (root mode
  `given`).
- Containment follows lineage from that root through dispatch, delivery and
  `trace.use` edges, not through version-id matches.
- The detector's own decisions are reported only as `n_detector_decisions`
  and are not scored.

A fix needs a task-independent content digest in the lineage events and
matching on it in the detectors. That changes the trace schema on every node
and in Core, which is not small, so it stays a documented limitation: a
detector-driven stale-content alert (as opposed to an operator alert) cannot
recognize content re-served under a new task.

### Pilot sizes (E) and the lab pipeline (F)

- `lab-pilot`: lab-smoke with 2 repetitions per cell, 338 runs.
- `svc-pilot`: svc-smoke with 3 repetitions, 63 runs.
- `experiments/regression.json`: the regression cells of
  `deploy/g2/regression-m6.json` (C2, N2 and rand-n7-s1 lab-smoke cells, 63
  runs) with paths relative to the repository.
- `python -m harness pipeline --stack <lab stack> --out <root>`
  (`harness/pipeline.py`) runs regression, scores it, runs svc-smoke, scores
  it, then applies the gate. Each part runs in its own directory. The gate
  requires:
  - every run complete or quiescent;
  - `commits_unlabelled`, `commits_unresolved` and `commits_label_mismatch`
    all 0;
  - Firebreak clean after the alert in every faulted run;
  - at most 3 worker-slot restarts in total;
  - a median per-hop hand-off over all runs below 100 ms.
- The verdict, with the controller's commit cost, goes to `gate.json`. On a
  failed gate the pipeline writes the reasons to `GATE-FAILED`, stops and
  exits 1. Otherwise it runs and scores lab-pilot and svc-pilot.
- A rerun resumes: finished runs are skipped. To run it as the runner unit:
  `python -m harness unit --log <root>/pipeline.log -- pipeline --stack
  <stack> --out <root>`.

### Phase-1 convergence and idle tainted contexts (simulator v5 check 1)

The simulator's v5 found that an idle context with whole-context taint (no
live invocation at the alert) had no control-graph node. Its label was not
counted in (C2), so tau_1 was declared before the label landed and tainted
commands committed after a "converged" tau_1.

The monitor had the same gap in a different form. There is no context label
in agentd's vocabulary: a tainted context's future invocation was an
arrival with h = 1 and no next events. With `boundary: sink` it is a horizon
sink that is unavoidable at the alert and therefore left out; with `defer` it
is a dead end. Either way the convergence check covered nothing for it.

Reproduced with real agentd: a shared-memory assistant reads the corrupted
input and finishes, then the alert. The decision was `contained`, with phase
1 converged and no action. A request arriving afterwards committed its
device command, until a receding re-solve would label it.

Fix:

- The projection models one level of next events (its protected classes and
  dispatches) for the future invocations of a whole-context tainted context
  (a shared-memory agent's `ctx:<agent>:*`), whatever the horizon.
- `enforcement.context_sinks`: the context's protected-class sinks count in
  every convergence and release check (`cut_holds`), doomed or not.
- `enforcement.context_covers`: the adaptive phase 1 pushes, with its labels,
  the revocation of those classes for the agent (C2). It is confirmed and
  fenced before tau_1, one control round after the alert.
- A request seen before convergence is also labelled (its command is held);
  later ones are refused at the gateway (`ingress_filtered`); replies go out.
- Per-context sessions (every grid instance) are unchanged. Covering them
  agent-wide would stop the agent's clean conversations, and a per-context
  label would be a new agentd actuator. They stay with the receding re-solve
  (a limitation).
- The freeze modes (holds only) cannot cover such a context in phase 1, so
  they converge only in phase 2. They do not run in svc.

Effect on earlier results:

- No grid instance has a shared-memory agent, so G1 and the lab-smoke cells
  are unaffected.
- In svc, any tainted device command committed after a converged tau_1
  would show in the svc metric "tainted device commands committed" after the
  alert. The lab's svc smoke data are not available here to check.
- Svc baselines may now report `cut_valid` false when their rule leaves the
  idle context's classes open. This is reporting only.

Test: `test_an_idle_tainted_context_is_covered_before_phase_1_converges`
(both boundaries; a request arrives between the alert and the cover's
confirmation, another after convergence). It fails without the fix.

### Pause pricing (simulator v5 check 2)

The simulator priced a pause of a held or queued invocation as free when its
planned end preceded the pause's landing. The monitor's cost model has no
planned ends. A task hold at the unit's own node always costs the unit's
value plus, for a service agent, its request, whether the unit is running,
held or queued. So this pattern is absent, and no earlier lab result was
affected by it. Test (pinning it):
`test_pausing_queued_or_held_work_costs_it_whatever_its_planned_end`.

### Cost option `tainted_output_value` (post-hoc ablation, off by default)

Context: in the m6 svc smoke with an early alert, the input blockers
answered all 45 requests with 0 tainted replies. Firebreak let the schedule
in, labelled the context and cancelled device commands: 45 answered, 43
tainted, because the pre-registered model values a tainted reply as fully
answered.

`MonitorConfig.tainted_output_value` (0 to 1, default 1.0, the
pre-registered model) is the value of an unprotected output produced from
tainted input, relative to a clean one.

- Below 1, letting taint reach a shared-memory service agent's context costs
  (1 - value) × `future_request_value` per expected future request
  (`future_requests`, the svc cost basis).
- This loss is not an action: G_C gets a priced sink `T:<agent>`, entered
  from each of the agent's units when it starts with tainted input and from
  its tainted context. Only the strategies' solve sees it
  (`ControlGraph.instance(penalties=True)`); the cut either pays it (a
  `model.TaintedOutputs` token, never sent) or stops the taint first.
- The way to stop it first is the hold of a queued unit, if that hold lands
  before the unit can start.
- Checks never see the priced sink.
- Decisions record `tainted_output_value`, and each solve reports
  `tainted_outputs_accepted`. At 1.0 nothing is added: no edges, the same
  solves.

Harness: the `tainted_output_values` grid dimension (default [1.0]). Another
value runs as a `tov<value>` variant, is recorded in `run.json` and the scored
row, and is passed to the monitor. The 1.0 cells keep their ids and configs.

Tests: `test_a_tainted_output_value_below_one_prefers_blocking_the_input`.

- Early (hand-off in transit): the sink cut at 1.0, the input block at 0.5.
- Hand-off already queued, so the block cannot land in time: the sink cut at
  both values.
- Late: the sink cut at both values, with the accepted loss of 15 reported
  at 0.5.
- `test_the_tainted_output_value_ablation_is_named_and_recorded` covers the
  harness side, and the config tests cover validation.

Caveat for running it in the lab: the svc monitor settings keep the delivery
and start lower bounds at 0. Under those bounds a hold on the queued
`A.sched` is never judged able to land before it starts, so Firebreak's
choice would not change. The ablation needs measured minimum delivery or
start bounds in the svc monitor settings (or the in-transit window) to
compare anything.

## Evaluation branch (`eval/2026-10-22`, from `m7/eventdriven` at `896719e`)

### G-Safeguard baseline (`gsafeguard`)

G-Safeguard's remediation rule (Wang et al., ACL 2025, section 3.3, Eq. 11),
reproduced under the shared alert. Their code (`TA/agents.py`
`re_generate`/`are_generate`, `TA/main_defense_for_different_topology.py`
`defense_communication`) behaves as follows:

- once an agent is flagged, its replies are dropped from every receiver's
  input from the next round on;
- the flag is sticky;
- the flagged agent keeps running and receiving;
- nothing already received is withdrawn.

Ours (`strategies.gsafeguard`):

- **Flag:** the agent of each root invocation (a version root names its
  producer).
- **Actions, for each unit of that agent, live or queued:**
  - its egress is revoked: agentd denies its new dispatches and effect
    requests;
  - its result is parked at its live parent's node, when that parent runs
    on another node. Where they share a node, a park would pause the unit,
    so that result flows.
- **What it does not touch:**
  - the flagged agent's tasks are neither held nor cancelled;
  - consumers;
  - outputs already published or delivered;
  - other agents' leases and grants.
- **Effect requests** count as outgoing edges because the actuator is a node
  of our graph. This can only help the baseline: the original has no effects
  to drop.
- **Stickiness**, through the persistent quarantine's mechanism:
  - an active `gsafeguard` incident flags its agents in the provenance
    graph (`flagged_agents`, rebuilt from the incident on restart);
  - each later unit of a flagged agent is then a frontier advance, even a
    clean one;
  - the receding re-solve applies the same rule to it. A later unit can emit
    before its own revocation lands, the deployed counterpart of the
    original's "next round".
- **Profiles:** a rule baseline under both, like the others.
- **Display:** "G-Safeguard", no-lineage family.
- **Grids:** regression 69 runs, svc-smoke 23, svc-pilot 69, lab-smoke 185
  (including its shared-profile part), lab-pilot 370, plus lab-main.

Tests:
- `test_gsafeguard_flags_the_root_agent_and_drops_only_its_outputs`;
- `test_gsafeguard_covers_a_later_unit_of_the_flagged_agent` (a later clean
  task of the flagged agent is covered after a monitor restart; another
  agent's is not);
- the opt-in end-to-end
  `test_gsafeguard_lets_output_published_before_the_alert_reach_its_consumers`
  (C2 early: a1 has already finished, so nothing is flagged live; its
  artifact travels on to b1, whose actuator command is accepted).

**Pre-registered expectation (recorded 2026-10-21, before any run):**
- In every paired cell, G-Safeguard commits at least as many tainted
  protected effects after the alert as Stop team. It stops neither the
  flagged agent's sub-agents nor anything that already received its outputs,
  while Stop team pauses the root agent's whole team.
- It has collateral on the flagged agent's later clean outputs: they are
  dropped whatever their lineage.

In the local svc early run the flagged agent was the planner, whose
schedule hand-off had already left. The assistant then:
- served all 45 requests, with 45 tainted replies;
- committed 17 tainted device commands (No containment: 20).

The planner's later hand-offs were denied, so the run ended quiescent rather
than complete.

### A2A and bare-runtime baselines (merged)

**Source.** `a2a-baseline.bundle`, sha256 `2e5a9912d1501b2e…` (checked). Its
head is `d77f9ec`, five commits on `m5/integration` `e445457`; the
coordinator's note named `0950578`, which `d77f9ec` contains. The merge
(`4dc12a2`) had five conflicts, each resolved keeping both sides:
- `harness/stack.py`:
  - the M6 operator connector for every node's external recipients;
  - no monitor or leases on `edgecitadel_bare`;
  - the monitor options with the profile's phase-1 default and the M7
    settings.
- `harness/runner.py`:
  - `make_stack` with the `tainted_output_value` setting;
  - the request scheduler helpers beside the A2A transport codes.
- `harness/__init__.py`, `tests/test_runner.py`, `tests/test_stack.py`: both
  sides' text and tests.

Two semantic conflicts surfaced in a local A2A run and are fixed:
- **External requests.** M6 moved them to per-node schedules (`nodectl
  schedule` through agentd), which an A2A stack lacks, so every A2A run lost
  its `requests-<node>` process. `A2ARunStack.schedule` now submits from the
  controller at the same times, with the same marks.
- **Hop timing.** A2A's `message/send` blocks until the recipient finished,
  so the dispatch event (reported when the call returns) came after the
  child started: hops were negative (median -51 ms). The det-worker's
  dispatch event now carries `sent_local_ms`, and hops count from the send.
  On EdgeCitadel a hop therefore now includes the sender's `task.create`.

**Local measurement.** Six-hop chain `bench/chain-k2e`, 2 repetitions each,
on this VM's disk:

| stack | per-hop median | per-task median | ratio |
|---|---|---|---|
| A2A-SQLite | 27 ms | 54.5 ms | – |
| EdgeCitadel | 644.5 ms | 1364 ms | about 24× A2A-SQLite |

Here the fallback criterion below would fail. EdgeCitadel pays several
durable commits of an attached pair per hop (60 ms each on this disk), so
the lab's storage decides.

### Gate criterion for hand-offs (approved 2026-10-21)

The per-hop median of regression + svc-smoke passes at **100 ms or less**.

Otherwise, and only if every other condition passed, the fallback runs:
- the six-hop chain between jim-eq and Endurance
  (`experiments/handoff-chain.json`, `bench/chain-k2e`, 10 repetitions,
  no fault) runs on the pipeline's EdgeCitadel stack (`handoff-ec`);
- the same chain runs on the durability-matched A2A stack, A2A with its
  SQLite task store (`--a2a-stack`, `configs/lab-a2a-sqlite-gate.json`,
  `handoff-a2a-sqlite`);
- the gate passes if EdgeCitadel's per-hop median on the chain is **at most
  3 times** A2A-SQLite's (H11's criterion).

`gate.json` records:
- `handoff_criterion` (`absolute` or `a2a_sqlite_ratio`);
- the pooled `handoff_ms_median`;
- `chain_handoff_ms_median` for both systems and their ratio.

A missing A2A stack, one that is not A2A with SQLite, a failing A2A run or a
chain without hand-offs fails the gate, giving that reason.

The gate also checks the control latency L; see the next section.

### Control latency L = 150 ms (decided 2026-10-21)

**Decision** (the user, 2026-10-21 21:40 London). The monitor's planning
control latency is `"*": 150` ms in the native regime of every experiment
file:
- regression, svc-smoke, lab-pilot, svc-pilot, lab-main, compare-ec;
- lab-stress's native regime;
- the other native settings too, so nothing plans with the old value:
  lab-smoke, lab-rootcause, overhead, compare-a2a, handoff-chain and the
  dry runs.

The stress regimes (L = injected delay = 1000, 3000 or 4000 ms) are
unchanged. The monitor's code default (50 ms) is unchanged.

**Why.** The monitor plans an action to land at now + tau + L, and an edge's
action counts as feasible only if that is before the edge's deadline
(`projection.t_ap`, `edge.feasible`). The theory therefore needs L to be an
upper bound on decide→enforced. Below the real latency, a cut that lands
late looks timely.
- Pre-M7 lab sent→confirmed: p50 18 ms, p95 82 ms, max 124 ms.
- The 4 ms used so far (the LAN round trip) was below all of them.

`MonitorConfig`'s comment, `deploy.monitor_config` and the runbook called
L a lower bound. They now say upper bound. No code path treats it as
one: a larger L only moves more emissions into early windows and more
arrivals to doomed hubs.

**Pre-registration amendment.**
- L = 150 ms is a measured upper bound: above the pre-M7 maximum, and
  re-checked by the gate on this branch's regression stage.
- The G1/m6 lab runs with L = 4 ms are pre-calibration evidence, not pooled
  with the evaluation runs. Their run ids are the same (the setting is still
  named `native`), so the evaluation pipeline needs a fresh output root.
- **Expected effect:** more conservative timeliness.
  - More pending effects are untimely or doomed at the alert, and the
    adaptive phase 1's margin is now 2 rounds × 150 ms = 300 ms (8 ms
    before), so the freeze is added more often.
  - Possibly a little less kept work.
  - The safety predictions are unchanged.

**Regime label.** The runner's stress check used to compare every value
with the 4 ms round trip, which would have made native L = 150 a stress
setting. It now compares:
- the planned L with the measured control bound (`measured_control_ms`,
  default `runner.MEASURED_CONTROL_MS` = 150);
- injected delay and netem with the round trip (4 ms), as before.

Old runs keep their recorded regime, and the report's fallback for runs
that predate it uses the same split. A latency setting that leaves a site
unset now plans with 150 ms.

**Simulator backend.** It uses the planning L as the simulated latency, so
its rows of these grids now simulate 150 ms.

**Gate check.**
- From the regression stage's decision logs (scored runs only, without the
  monitor's teardown answers), `gate.json` records each site's
  sent→confirmed n, p50, p95, p99 and maximum
  (`control_sent_to_confirmed_ms`; nearest rank; monitor clock at both
  ends; a command repeated by later records counts once) and the bound
  (`control_latency_ms`).
- The gate fails if any site's p99 is above 150 ms (L is then no longer an
  upper bound), or if no command was confirmed (the bound is unchecked).
- Such a failure is not a hand-off failure, so the A2A fallback does not
  run.

Tests:
- `test_control_latency_is_read_per_site_from_the_regression_decision_logs`;
- `test_the_gate_fails_when_a_sites_p99_is_above_L`;
- `test_a_control_latency_failure_skips_the_handoff_fallback`;
- `test_every_native_lab_setting_plans_with_the_gated_L` (all 16 files);
- `test_planned_L_is_measured_up_to_the_control_bound_injection_up_to_the_rtt`.

`gate.json` also reports `svc_early_consumed_at_alert` (information, not a
condition). `configs/lab-a2a-sqlite-gate.json`:
- is `lab-a2a-sqlite` with the A2A virtual environments in
  `HARNESS_A2A_PYTHON` (jim-eq) and `HARNESS_A2A_REMOTE_PYTHON` (Endurance),
  so one pipeline process runs both systems;
- runs Endurance's processes as user units.

### Lab defaults for agentd's fallback re-checks

A stack file's `runtime_env` sets `EDGECITADEL_*` values for agentd and the
Managed Agent runtime on every node.
- Never the harness's own per-node settings.
- Recorded in `run.json`.
- The repository's EdgeCitadel lab stacks set
  `EDGECITADEL_TRANSPORT_POLL_MS` and `EDGECITADEL_CLAIM_POLL_MS` to 1000:
  full (`lab`, `lab-helper`, both Home Assistant variants,
  `lab-ec-overhead`) and bare (`lab-bare`) alike, so H11 compares like with
  like.
- The code default stays 50 ms. Dry runs and A2A stacks are unchanged.
- A lab copy of a stack (such as the lab agent's `lab-m6.json`) needs the same
  `runtime_env` field.

### svc early alert (local check)

Run on `dryrun-nats`, early cells only. In every run the schedule was not yet
consumed at the early alert, so the early/late distinction holds here. All
times are relative to the alert.

| strategy | consumed_at_alert | dispatch carrying the schedule | `A.sched` starts | `A.sched` reads it |
|---|---|---|---|---|
| Firebreak | 0.0 | -0.53 s | +0.90 s | +1.06 s |
| No containment | 0.0 | -0.21 s | +2.41 s | +2.53 s |
| G-Safeguard | 0.0 | -0.53 s | +1.59 s | +1.74 s |

The margin is two hand-off hops of this slow-disk VM (about 0.5–1.2 s each).
The dispatch carrying the schedule already leaves before the alert, which
fires 0.4–0.7 s after the produce event: its 0.3 s delay plus the trigger
watch.

With lab hops at or below 100 ms, `A.sched` would read the schedule about
0.2 s after that dispatch, which is before the alert, and the distinction
would collapse. The workload is unchanged. The proposed fix goes to the
coordinator, because it touches the H8/H9 pre-registration.

Superseded: the early alert now fires on the produce event itself (see
"svc early alert on the produce event" below).

### tau_1 in the decision log and `tainted_after_tau1` (spec review A)

Before this change the scorer's "containment established" was the last
confirmation of the first decision's actions. It checks neither fencing nor
the convergence conditions, and tau_1 was logged nowhere.

**The record.** When phase 1's literal convergence check passes, the monitor
now writes a `converged` record to the decision log.
- **Adaptive phase 1:**
  - (C1): every relied-on action confirmed and fenced, its journal ingested
    through the confirmed positions;
  - (C2): the label set over the whole source side, idle whole-context
    tainted contexts included, confirmed;
  - (C3): taint reconciled under (J') in every projection;
  - (C4): the actions in force a cut against every checked sink on the
    reconciled graph.
- **Freeze modes:** every phase-1 action fenced (C1); the frontier freeze
  stands for C2 and C3; their C4.

The record holds:
- `tau1` (monitor clock) and `t_alert`;
- the checks;
- the fence position per site (epoch and sequence);
- the size of the label set and the number of actions in force;
- every sink the C4 check covered (`sinks_checked`).

The protocol only reports through a callback, so no decision changes. A rule
applied directly (the baselines under the deployed profile) has no phase 1
and no tau_1.

**The metric.** The scorer adds:
- `converged` (0 or 1);
- `tau1_s` (from the alert);
- `tainted_after_tau1`: violations committed after the operator incident's
  first tau_1, each ordered by its commit time at its own site. That time
  comes from the gateway, through its post to the sink or Core's record of
  the commit, and falls back to the ledger time. This assumes the hosts'
  clocks are synchronized, as on the tailnet. NaN when the run never
  converged.

`tainted_after_established` is unchanged.

**Results.**
- Opt-in C2 end to end: Firebreak early, late, freeze_plus_cut and adaptive
  log tau_1 121–159 ms after the alert, with `tainted_after_tau1` 0.
- Local harness C2 runs:
  - Firebreak converges 3.3–3.8 s after the alert, with 0 after tau_1 and
    clean runs;
  - G-Safeguard reports not converged, with 1 and 2 violations.

**Tests.**
- The idle-context test checks the record's checks, fence position, label
  set and the context's sink, and that tau_1 is not before the cover was
  fenced.
- The doomed-sink test checks that tau_1's check covered the sink judged
  unavoidable at the alert.
- G-Safeguard logs no tau_1.
- The scorer test orders commits at their site, ignores another incident's
  record, and reports a run without tau_1 as not converged.

### Theorem 5 coverage (spec review B), checked on this branch

**(i) Sinks doomed at the alert are outside Theorem 5 after tau_1: false here.**
- **The C4 check at tau_1.** It runs on the graph projected at tau_1, where
  every action in force counts as feasible. A sink judged unavoidable at the
  alert but blockable by an action now in force is therefore checked. On top
  of that:
  - every pending protected effect is checked, doomed or not (the M5 fix);
  - every protected class of a whole-context tainted context is checked
    (M7).
- **Sinks left out.** Only those that no action can separate at tau_1
  itself: horizon-boundary new invocations, by design, and a unit on a node
  without the label capability.
- **Phase 2 and the release.** Phase 2 releases a phase-1 hold only after
  `cut_holds(B_2)` passes on the same set of sinks, where an admission label
  in force counts as held in the phase-2 graph. A sink no action can reach
  keeps every hold (`phase2_check_failed`).

  Evidence:
  - `test_a_doomed_sink_reachable_through_an_unheld_edge_blocks_convergence`
    (the sink, doomed at the alert, is in tau_1's `sinks_checked`, and
    tau_1 follows its guard's confirmation);
  - `test_a_sink_no_action_can_reach_keeps_every_hold` (no tau_1 at all);
  - `test_phase_two_sees_an_admission_label_in_force_as_held`.

**(ii) svc with `boundary: defer`, later new tasks of a tainted session:
covered for svc.**

Corrected by review (H) below: what follows holds for the late alert, and
for the early alert only when A.sched's start is ingested before tau_1. Since
(H), a source-side invocation's write to shared memory is modelled before it
happens, which covers the early alert too.
- **Literally true:** such a task is neither labelled nor a sink until the
  receding re-solve sees it, so the admission invariant does not hold for it.
- **Theorem 5's conclusion still holds in svc.** The assistant is a
  shared-memory agent, so its tainted context is whole-context. The M7 fix:
  - revokes its protected classes (`dev_lamp`) at its node at the alert;
  - counts that in (C2), confirmed and fenced before tau_1;
  - checks those classes in every (C4) and release.

  A task created after tau_1 is refused at the gateway for any protected
  effect (`ingress_filtered` in
  `test_an_idle_tainted_context_is_covered_before_phase_1_converges[defer]`
  and `[sink]`), and its replies are unprotected.
- **Its other outputs.** Its only recipient is itself (a pending creation),
  so they stay within the revoked agent. The planner's sessions are per task,
  and no later planner task joins the tainted `sP.sched`.
- **A residual outside svc, reported rather than fixed (the fix changes the
  design).**
  - With per-conversation sessions (every grid instance), a new task
    created after tau_1 in a tainted conversation is covered only once the
    receding re-solve labels it.
  - Revoking the agent's classes would stop its clean conversations, and a
    per-conversation label would be a new agentd actuator.
  - `tainted_after_tau1` now measures whether this happens in the lab.

### Per-event admission reconciles under (J') (theory review D, safety fix)

**Finding** (the theory agent, from the code). `admit_held` decides guarded
releases of event holds through `_project(2, ...)` with `_phase1_mode` unset,
so it never applied the conservative reconciliation (J').

**When the view is stale.** (J') taint lives only in the provenance index.
An adaptive `_enforce` adds it, and `admit_held` usually still sees it there.
It is missing in two cases:
- **The index is rebuilt from the roots:** a monitor restart, or a failed
  ingest batch (`graph.transaction` reloads).
  - `reload` clears `frontier_advanced`, so `step()` reaches `admit_held`
    before any re-solve.
  - The labels in force come back from the database.
- **A commit whose tail is on a lagging journal:** it advances no frontier,
  so no re-solve reconciles it before the next admission.

**Test** (two sites, the slow journal arriving after the fast one):
`test_conservative_reconciliation_keeps_a_stale_view_from_admitting_an_event`.
- x runs on the slow node; its dispatch of y (fast node) is not yet
  ingested.
- (J') taints y at the alert, and Firebreak labels it: its node can neither
  pause it nor revoke its class.
- y's next protected effect is held as an event.

Before the fix:
- live: the event was kept;
- after a restart: it was admitted as `not_on_source_side`, a leak.

With the fix both stay held, also once the slow journal arrives, and no
release is sent.

**Chains without a restart, under agentd's semantics:**
- **A held dispatch:** agentd journals the request version's generation in
  the same transaction, before the held event. The monitor cannot see the
  held event without it.
- **A labelled child's held result:** its version is generated at
  completion, after the release, on the child's site. However, the egress
  label does not close the result edge in G_C, so the parent stays on the
  source side and its events stay held (checked: kept).
- **A paused child:** it produces no held result (agentd refuses the
  transition).

So the only leak without a restart found here is the failed-batch reload.

**Fix.** `admit_held` sets the newest active incident's phase-1 mode for its
projections, as `_enforce` does.
- `_project` therefore reconciles (J') under the adaptive phase 1.
- Rule strategies (no phase 1) and the freeze modes are unchanged, as on
  the enforce path.
- The reconciliation is conservative: it can only keep more events held,
  and the taint it adds can trigger a re-solve.
- No pre-registered run changes unless its view is stale.

**Not changed (FYI from the coordinator): write-ahead logging of releases,
assumption (D).** The controller persists action records only after
`TwoPhaseProtocol.run` returns, and the releases are sent inside it. This is
not trivial to add:
- writing the records before each release is a small persistence hook;
- but a restart then needs a recovery rule for a release logged but
  unconfirmed (re-send it, or keep the hold and decide again), with tests of
  both.

It stays a stated limitation of the prototype.

### svc early alert on the produce event (pre-registration amendment, H8/H9)

**Decided** by the user, 2026-10-21 about 23:00 London, before any lab run
of svc.

**Change.** The early alert fires on the schedule-produced event itself,
offset 0, and the run's services post it when they receive that event.
- Before: the controller saw the event on its next poll of the services'
  feed, waited 0.3 s, then resolved the faulty invocation through Core.
  The alert lagged the produce event by 0.4–0.7 s.
- The late alert is unchanged.

**Meaning.** The registered meaning ("the alert comes before the automation
consumes the schedule") is unchanged; only the trigger changes, so the
meaning no longer depends on hand-off speed. `consumed_at_alert` stays as
the manipulation check (reported in gate.json).

**Mechanism.**
- `AlertTrigger.posted_on_receipt`, allowed only on a produce event with no
  delay.
- The svc workload's early alert sets it, and `alert_early_delay_s` becomes
  0.0. The regenerated workloads/svc.json differs only there.
- `future_requests` follows the earlier nominal alert time: 0.5 × (85 − 20.5)
  instead of 0.5 × (85 − 20.8), i.e. 0.15 more expected requests.
- Before the run starts, the runner tells the services which item to watch
  (`POST /alert/arm`). It looks up the faulty invocation's attempt at once and
  then every 0.2 s (`ATTEMPT_LOOKUP_FAST_S`), and arms the services with the
  alert, the intake URL and the admin header once it has it.
- The services mark `alert` and post it from a thread as soon as they have
  both the event and the alert.
- `GET /alert` reports the trigger, arming and mark times and the reply.
  Neither the headers nor the token are ever written or returned.
- An uncontained run (`none`) is armed with `post: false`: only its alert
  time is marked, at receipt, as before.
- A stack without an HTTP alert intake (A2A, which never runs svc) keeps
  the controller's path, with no delay.
- If the attempt is still unknown 5 s after the trigger, the alert names the
  task, as before, and that is recorded as an error.
- run.json records `alert_posted_by` and `alert_armed_ms`.

**Local check** (`dryrun-nats`, svc early, L = 150 ms):

| strategy | `consumed_at_alert` | armed | alert marked | dispatch carrying the schedule | `A.sched` reads it |
|---|---|---|---|---|---|
| Firebreak | 0.0 | 2 ms before the produce event | 6 ms after it | +0.29 s | +4.87 s |
| No containment | 0.0 | – (`post: false`) | at receipt | after the alert | after the alert |

Times in the last two columns are from the alert.

Firebreak: 0 tainted device commands, τ₁ at +1.53 s, `tainted_after_tau1`
0. No containment: 20 tainted device commands. Both runs complete.

**Tests:**
- `test_an_alert_is_posted_once_on_receipt_of_its_produce_event[armed
  first/event first]`: the post happens once; the trigger is timed from the
  event; another item, a non-task invocation and a second produce do not
  trigger; the token is not in the marks.
- `test_an_uncontained_run_only_marks_its_alert_time`.
- The spec rule and its round trip.
- `test_the_early_alert_is_the_produce_event_posted_on_receipt` (the late
  alert unchanged) and the updated cost test.

### Hand-offs recorded, not gated (decided 2026-10-21)

Tonight's pipeline does not stop on hand-off speed.
- **Still measured and recorded in gate.json:** the per-hop median, the
  A2A-SQLite fallback (run when the absolute check fails and the gate
  otherwise passed), `commit_cost_ms` and `handoff_criterion`.
- **New fields:** `handoff: "recorded_not_blocking"`, and `handoff_verdict`
  (`passed` and `reasons`), the verdict the approved criterion would have
  given. An A2A measurement that cannot run is recorded there with its
  reason.
- **Still blocking:**
  - the control-latency check (every site's p99 ≤ 150 ms);
  - run status;
  - scorer integrity;
  - Firebreak clean after the alert;
  - at most 3 lost worker slots;
  - testbed failures. This check is new and explicit: a run whose
    `testbed_failure` is anything but lost worker slots (which keep their
    budget of 3) blocks. Before, such runs were caught only through their
    status.
- The pass message prints whether the hand-off check would have passed.

**Tests:**
- `test_hand_offs_are_recorded_with_their_verdict_but_do_not_block`;
- `test_a_failed_fallback_is_recorded_and_the_pilots_still_run`;
- a testbed-failure case in `test_each_gate_condition_fails_on_its_own`;
- `test_a_control_latency_failure_skips_the_handoff_fallback` (still
  blocking, with the hand-off verdict recorded).

### Why an EdgeCitadel hop costs about 18 durable commits (analysis, no change)

The local six-hop chain measured 644.5 ms per hop, about 18 times this VM's
36 ms durable commit to one database.

**Storage.** Each agentd keeps two SQLite files: the trace database (main:
journal, bindings, lineage) and `-tasks.sqlite3`, attached as `task_state`
(tasks, outbox, holds, held events). Both use a rollback journal
(`journal_mode=DELETE`) with `synchronous=EXTRA`.
- **One file:** a commit fsyncs the journal and the database, deletes the
  journal, and (EXTRA) fsyncs the directory. Here that is 32–36 ms.
- **Both files ("pair"):** SQLite adds a super-journal (more file writes,
  fsyncs and directory syncs), so the change is atomic across both files.
  Here that is 52–60 ms.

**Commits from a sender's dispatch to the recipient's start**, sender agentd
S and recipient agentd R, by code reading:

| # | where | what | files |
|---|---|---|---|
| 1 | S `trace.dispatch` | permission, dispatch and lineage events; child task; outbox message; idempotency row | pair |
| 2 | S outbound loop | `publish_started` observation | main |
| – | NATS JetStream | publish; file store, no per-message fsync by default | broker |
| 3 | S | `publish_accepted` observation | main |
| 4 | S | `mark_transport_published` | task_state |
| 5 | R consumer | `received` observation | main |
| 6 | R `ingest_transport_envelope` | task created, lifecycle events | pair |
| 7 | R | `durably_accepted` observation | main |
| 8 | R, after the JetStream ack | `ack_sent` observation | main |
| 9 | R `task.claim` | claim, attempt, lifecycle event | pair |
| 10 | R `trace.bind` | binding, run started, lineage used | main |
| 11 | R `task.transition running` | task state and lifecycle event | pair |

That is 4 pairs plus 7 single-file commits, about 13 single-commit
equivalents (4 × 1.6 + 7). The remainder of the 18 is CPU contention (three
agentd and their workers on 2 vCPUs) and concurrent writers taking agentd's
writer lock:
- the trace-sync exporter's cursor commits (`EDGECITADEL_TRACE_SYNC=1`);
- the worker's `trace.use`/`trace.append` commits.

An instrumented count would pin this down.

**The result path back** repeats the pattern:
- R completes: task state, result in the outbox, lifecycle and lineage
  events (pair), then the binding finish (main);
- R publishes (commits 2–4 again);
- S ingests the result (pair) with its three observations.

**What could be merged without weakening the journal or fencing (F).**
- **The five transport observations** are "optional communication evidence,
  isolated from task and broker transactions": each is its own commit so
  that a failure cannot affect delivery. Fencing relies on the journal
  positions of control actions and on lineage, not on these. They could be
  written in a savepoint inside the neighbouring task transaction (commits
  5–7 into 6; 3 with 4), or buffered and group-committed. That saves up to
  5 single commits per hop.
- **Claim, bind and transition to running** (9–11) are three RPCs for one
  step. A combined "claim and start" could commit them as one pair, saving
  one pair and one single commit.
- **WAL.** Only after the pair becomes one file (task_state merged into
  main). In WAL mode, a transaction over attached databases is atomic per
  file but not across the set. agentd relies on cross-file atomicity:
  - a task state change and its journal event;
  - an outbox message and its dispatch event;
  - a held event and its hold.

  That is why `attach_tasks` refuses anything but rollback journaling.
  - With one file in WAL and `synchronous=FULL`, a commit is one WAL append
    and one fsync, with no directory sync.
  - Readers then no longer block the writer.
  - Journal positions (`source_seq`) are row values and are unaffected.
- **A cheaper middle step.** `journal_mode=PERSIST` (or TRUNCATE) with
  `synchronous=FULL` keeps rollback-journal atomicity across both files.
  EXTRA's directory sync exists only because DELETE unlinks the journal, so
  this drops it per single-file commit. Pairs keep their super-journal cost.
- **Not to weaken:**
  - durability before acknowledging (the JetStream ack after commit 6, and
    the outbox marked published after the broker's ack);
  - the journal and the action committed together (the effect gateway and
    control commands);
  - a confirmation's journal position covering what it confirms.

  None of the merges above touches these.
- **Broker durability.** JetStream's file store does not fsync each message
  by default (`sync_interval`). A broker crash could lose a message whose
  outbox entry is already marked published. This is not on the latency
  path, but it bears on the at-least-once claim.

### Shared memory written after tau_1 (theory review H, safety fix)

**Finding** (the theory agent, read-only, on 4aed792). G_C modelled a
shared-memory agent's continuation (the `ctx:` source and its context sinks)
only once `graph.tainted_sessions` already held that memory.
- The adaptive phase 1 labels a pending source-side invocation without
  pausing it.
- In svc with the early alert, `A.sched` has not started at the alert, so
  Firebreak labelled it and let it run.
- Once it ran, the memory was tainted. A user request starting after it
  could command the lamp before the class revocation, which waited for the
  next projection.

**Test:**
`test_a_source_side_invocation_that_taints_shared_memory_after_tau_1_is_covered`.
- Real agentd; the planner (root) has handed the shared-memory assistant
  its schedule task, still queued at the alert.
- After tau_1, with no further monitor step, the schedule task runs, and a
  user request starts after it.
- Before the fix the request's device command was **committed**, and
  tau_1's C4 had checked no sink at all: the context's lamp class was doomed,
  and was not yet a context sink.
- With the fix the command is refused, the reply commits, and tau_1's
  `sinks_checked` includes `K:assistant:device.command@new`.

**Fix** (as the theory models it). Each unit of a shared-memory agent has an
undeclared `memory` edge from its output into the agent's context
continuation `ctx:<agent>:*`, a pending edge of G(tau).
- Holding the invocation (its split edge) stops the write; an egress label
  does not.
- The context node and its arrival into the agent's new invocations exist
  whenever the agent has units. The node is a source only once the memory is
  tainted.
- `context_sinks` takes every whole context, so the context's protected
  classes count, doomed or not, wherever a source-side invocation's write
  reaches them: in the at-alert solve, in (C2) through `context_covers`, in
  C4 at tau_1 and in every release check.
- **Conservative only:**
  - agents without shared memory get no edge;
  - in the grids only svc has one;
  - the rule baselines are unchanged: `stop_downstream`'s closure skips
    `MODEL_ONLY_KINDS`, and the other rules do not walk undeclared edges;
  - the convergence and release checks of the shared-profile ablation see
    the context sinks too, which is more conservative.

**svc early alert, behaviour change.** The revocation of the assistant's
`dev_lamp` class moves from phase 2 (after tau_1) into phase 1: (C2) covers
the context that `A.sched`'s memory write reaches, and confirms it before
tau_1, whose (C4) now checks the class.
- **Before:** Firebreak also revoked the class at the alert, but in its
  phase-2 cut, after tau_1. The leak window was any `A.sched` start before
  that revocation landed: wide with lab-speed hops, and also open whenever
  phase 2 chose another cut.

Local svc early runs on `dryrun-nats` (times from the alert):

| code | class revocation confirmed | tau_1 | `A.sched` starts | tainted device commands | clean lamp commands after the alert |
|---|---|---|---|---|---|
| before (H) | +1.51 s (phase 2) | +1.53 s | +4.58 s | 0 | 1, held (Q011) |
| with (H) | +1.06 s (phase 1, C2) | +1.07 s | +3.20 s | 0 | 1, held (Q011) |

- **Collateral:** unchanged here. It is the lamp commands of clean requests
  between the revocation and `A.sched`'s start, which were refused before
  (H) too, once phase 2 landed.
- Replies are unaffected: 45 of 45 requests answered.

### Known non-guarantees (theory appendix N10)

Not fixed; stated here as limits of Theorem 5's coverage in the prototype.
Neither occurs in svc as configured, nor in any grid instance: only svc has
a shared-memory agent, and no instance creates a later task into a tainted
conversation from outside the source side.
- **(a) Per-conversation (non-shared) tainted contexts.** A later task of
  that conversation, created after tau_1 by a sender outside the source side,
  starts tainted and unlabelled, at h = 1, under either boundary setting. The
  receding re-solve covers it only once it is seen (`tainted_after_tau1`
  measures it).
- **(b) Shared-memory agents' onward flows.** The class revocation covers
  the context's protected classes only. Later invocations' dispatches,
  results or store read-backs are not covered by it.
