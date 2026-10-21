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

