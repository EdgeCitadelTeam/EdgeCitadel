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
