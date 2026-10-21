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
(ingest-then-release) and agentd's gates stay on for every strategy in both. Each decision records `profile`, `mechanisms` and
`phase1_include_cut` (decision log, status `last_decision`; status also lists
every strategy's mechanisms), and the C2 run output and the harness's
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

### `phase1_include_cut` (default false)

Phase 1 issues the frontier freeze B_1 together with the at-alert cut B_a: the
strategy's own solve on the phase-2 G_C at the alert (for Firebreak, its
deadline-aware cut), applied at once in the first round and recorded as the
`phase: 1` entry of `solves`. Phase 2 is unchanged: it solves with B_1 and B_a
in force and, once its cut is confirmed, fenced and checked, releases the holds
of B_1 ∪ B_a it does not keep (a revocation in B_a cannot be released and stays
in force). More phase-1 actions only shrink the source side, so Theorem 5 is
unaffected. Only strategies that run the two-phase protocol use it. Tests:
`test_phase_one_with_the_at_alert_cut_blocks_the_late_effect_at_once`
(in-process, late alert: the effect hold is in the first round, the effect's
sink is separated at every instant the nodes applied from then on, releases
follow the last phase-2 confirmation) and, opt-in on the multi-process C2,
`test_phase1_with_the_at_alert_cut_blocks_the_late_effect_from_the_first_round`.
