# Deployment runbook: containment milestone (m4/design) on the 3-node testbed

This runbook upgrades the real testbed to the containment milestone: agentd
schema 34 (**M6**: events an egress label holds, admitted one by one) with the
control channel and the effect gateway (which performs
registered effects itself) on every node, and Core with trace projection v9 and
the containment monitor (checked solver with HiGHS, ingest-then-release).
A testbed already on `m3/integration` (schema 32) needs only the changes marked
**M4** below. It contains no
secrets: every credential below is generated or copied on the hosts and never
printed, pasted into chat, committed or logged.

| Host | Role in this milestone |
|---|---|
| jim-eq | Core (NATS, aggregator with collector and containment monitor, dashboard), agentd with the root (`jim-eq-pi`, Pi root package) and Hermes |
| NetSys-Endurance | agentd with leaves (`endurance-pi`, Pi worker package, local Qwen) |
| NetSys-Cluster | agentd with leaves; **shared host: processes of other users must not be touched** |
| Raspberry Pis (later) | additional Edges; see the last section |

Facts about the current deployment come from `docs/pi-harness.md` (verified
2026-09-27) and `docs/research-context.zh.md` (2026-10-12). They are dated:
confirm every path, unit name and messaging mode in step 1 before changing
anything, and record what you find.

## 0. What changes, and why everything moves together

| Component | Before (last recorded) | After | Compatibility |
|---|---|---|---|
| agentd SQLite | schema 29 (main `4d464fe`), 31, 32 or 33 | **34** | migrated in place at first start; an older agentd refuses a newer database. **M4**: scheduled effects recorded by schema 32 (no adapter) are marked denied by the migration. **M6**: `held_events` and the `event` hold kind |
| Delegation envelopes | no `effect_scope`/`lease`/`authority_chain` | carried in `payload.execution_context` | a schema 32 node rejects delegations without an authority scope; mixed versions break cross-node delegation |
| Control RPC | `containment.hold`/`containment.release` | `containment.control` (+ NATS `edgecitadel.control.v1.<node>`) | old RPCs removed. **M4**: confirmations carry `journal` (the monitor waits until it has ingested the node's events through it) |
| Effects (**M4**) | `effect.authorize` + harness-performed, `effect.commit` for scheduled ones | `effect.execute`: agentd performs Home Assistant and HTTP-ledger effects through adapters in `EDGECITADEL_EFFECT_ADAPTERS`, re-checked at commit; `effect.authorize` stays for effects only the harness can perform (not fenced) | `effect.commit` removed; harnesses that perform registered effects themselves must switch to `effect.execute` |
| Trace RPC (**M4**) | — | `trace.use`: a harness reports an input it consumed from agent memory or a pushed delivery | harnesses that pass delivered results through memory should call it, or the lineage misses the edge |
| Native connector grants | recipients only (or the old implicit grant) | recipients **and effects**, both empty by default | re-grant after upgrade (step 5) |
| Core trace projection | v8 | **v9** | an existing v8 projection pauses the projector (`projector_rebuild_required`); see step 3 |
| Containment monitor | absent | `EDGECITADEL_CONTAINMENT_MONITOR=1` | off until step 7. **M4**: Core image needs `highspy` (in `aggregator/requirements.txt`); rebuild it |
| Controller lease | none | **on in the lab**: `EDGECITADEL_CONTROLLER_LEASE_MS=30000` on every agentd, renewed by the monitor from its start, alert or not | effects are denied (fail closed) whenever no renewal arrived within 30 s |
| Trace export (**M4**) | new events polled every 1000 ms | every 100 ms (`EDGECITADEL_TRACE_EXPORT_IDLE_MS`, 10 to 60000) | measured export lag p95 958 ms before, 48 ms after |
| Hand-offs (**M7**) | outbox published every 1 s; an idle Managed Agent slot asked for a task every 0.5 s | both woken when work arrives; fallback re-checks every 50 ms (`EDGECITADEL_TRANSPORT_POLL_MS`, `EDGECITADEL_CLAIM_POLL_MS`, 1 to 60000); `task.claim` takes `wait_ms` (0 to 2000) | either side may be older: an older agentd ignores `wait_ms` (the runtime then asks every 50 ms), an older runtime claims without waiting |
| Pi packages | 0.1.1 | rebuilt from this revision (abort/resume, effect gate) | reinstall; state preserved |

Upgrade all agentd nodes and Core in one maintenance window. Record the
deployed revision (`git rev-parse HEAD` of the source you install from) on every
host.

## 1. Inventory (read-only)

On **each host**, as the user that owns the EdgeCitadel services there:

```bash
git -C <source checkout> rev-parse HEAD            # what is deployed now
systemctl --user list-units 'edgecitadel*'         # or the system units on jim-eq
edgecitadel status && edgecitadel doctor
edgecitadel connector list                         # native connectors and sessions
edgecitadel agent list                             # Managed Agent packages
python3 - <<'EOF'                                  # node identity and messaging mode
import json, pathlib
node = json.loads((pathlib.Path("<state dir>") / "node.json").read_text())
print(node["agent_id"], node.get("mode"), node.get("messaging_mode"), node.get("jetstream_domain"))
EOF
timedatectl show -p NTPSynchronized -p TimeUSec    # clocks must be synchronized
```

Record per host: the agentd state directory (`<state>/agentd`), the service unit
and its user, the node ID (`node.json` `agent_id`; it is also the control subject
suffix), the messaging mode (`single-client` or `nats_leaf`), and the Managed
Agents and native connectors with their current grants. Last recorded values:
jim-eq agentd `edgecitadel-agentd-b5184c2bc72c.service` as user `edgecitadel-leaf`
with state under `/var/lib/edgecitadel-leaf/state`; Endurance agentd
`edgecitadel-agentd-ee441de5009e.service` and leaf broker
`edgecitadel-pi-leaf.service` as user `zhonghao`, trace volume mounted at
`~/.edgecitadel/agentd/trace`. Cluster has not been recorded; establish it now.

**Cluster is shared.** Before any change there:

```bash
ps -eo user,pid,cmd --sort=user | grep -i edgecitadel   # look, do not act yet
```

Only act on units and processes owned by the EdgeCitadel service user you
identified. Never use `pkill`/`killall` or name patterns, never `sudo systemctl`
a unit you did not identify as ours, and do not touch other users' processes,
GPU jobs, ports or directories. If an EdgeCitadel-looking process belongs to
someone else (the 2026-10-12 notes mention an old, locally modified bridge),
leave it running and note it.

Clock check: commands expire by the receiving node's clock (default lifetime 30
s, at most 300 s) and the monitor compares node-reported `applied_at_ms` with
its own clock. Do not continue with a host that is not NTP-synchronized.

## 2. Freeze and back up

1. Stop submitting experiment and chat traffic. Let running tasks finish, or
   note their task IDs; a task interrupted by the upgrade fails as
   `executor_session_lost` when its session cannot be recovered.
2. On each Edge, stop the Managed Agents and agentd (as their user), then back up
   the complete agentd state directory **as one unit** (database, task database,
   `payload.key`, `admin.token`, connector credentials and the trace volume
   contents). Restoring only the database makes encrypted task content
   unrecoverable.

   ```bash
   edgecitadel service stop                         # or the unit recorded in step 1
   tar -C <state parent> -czf <backup dir>/agentd-$(date +%Y%m%dT%H%M).tgz <state basename>
   ```

   On Endurance include the mounted trace directory; stop the leaf broker only
   if it must be upgraded.
3. On jim-eq, back up Core's data directory with the aggregator stopped (or use
   `deploy/backup/edgecitadel-backup.sh` if that is how Core was installed).
   Back up the Pi root's private state (`plugin-state/edgecitadel.pi-root`) and
   credentials directory as files; do not read their contents.

## 3. Upgrade Core on jim-eq

1. Install the revision into the Core deployment the way it was installed
   (compose project or `deploy/deploy-host.sh --update`). **Rebuild** the
   aggregator image rather than reusing the previous one: the monitor's ILP
   solver needs `highspy` from `aggregator/requirements.txt` (without it,
   instances off the fast path fall back to the greedy cut and the solve is
   reported `approximate`). Keep
   `EDGECITADEL_TRACE_COLLECTOR=1` and leave `EDGECITADEL_CONTAINMENT_MONITOR=0`
   for now.
2. Start the aggregator and check `GET /api/system/status` and the collector
   (`/api/trace/...` reads only if `EDGECITADEL_TRACE_READS=1`).
3. Projection v9: if trace reads are enabled and the projector reports
   `projector_rebuild_required`, the existing Core database holds a v8
   projection. This revision has no operator command to rebuild it (the shadow
   rebuild exists only as library code). Either run the experiments with
   `EDGECITADEL_TRACE_READS=0` (the collector and the monitor read raw events and
   are unaffected), or start Core on a fresh data directory and keep the old one
   as the archive. Do not delete the projection tables by hand.

## 4. Upgrade agentd and the packages on every node

Do the nodes one at a time and verify each before the next.

1. Install the revision's runtime on the host (the same distribution method as
   before, e.g. `uv tool install --force <revision>` or the pip/systemd setup on
   Endurance with `EDGECITADEL_DISTRIBUTION=pip`).
2. In the agentd unit's environment (a systemd drop-in; Endurance already has
   one for trace sync) set:
   - `EDGECITADEL_TRACE_SYNC=1`, so the node's evidence reaches Core and the
     monitor;
   - `EDGECITADEL_CONTROLLER_LEASE_MS=30000`, the fail-closed controller lease
     (team decision 2026-10-20: on for the lab). With it the node denies every
     effect (`controller_lease_expired`, plus a `containment/alert` event)
     unless a controller renewal was applied in the last 30 s. Effects stay
     denied from now until the monitor runs (step 7); that is intended while
     traffic is frozen. Dispatch and task execution are not gated.
   - **M4**, on nodes whose agents perform registered effects:
     `EDGECITADEL_EFFECT_ADAPTERS=<agentd state>/effect-adapters.json`, a JSON
     file with one entry per adapter (no secrets in it):

     ```json
     {
       "homeassistant": {"base_url": "http://127.0.0.1:8123",
                         "token_file": "<agentd state>/credentials/homeassistant.token",
                         "timeout_ms": 5000},
       "http": {"ledger_url": "http://127.0.0.1:9100/effects", "timeout_ms": 5000}
     }
     ```

     `base_url` defaults to `http://127.0.0.1:8123`. The token file holds a Home
     Assistant long-lived access token: create it on the node (never paste it
     into a shell history or chat), as a regular file (not a symbolic link)
     owned by the agentd user, mode 0600, directory 0700. agentd reads it at
     start, never logs or returns it, and refuses to start on an unreadable or
     malformed file. An effect targets `<domain>.<service>/<entity_id>` (class
     `device.command`), so the package or connector grant names those targets
     (step 5). Omit the variable on nodes without such effects; `effect.execute`
     then answers `effect_adapter_unavailable`.
   - **M4**, optional: `EDGECITADEL_TRACE_EXPORT_IDLE_MS` (default 100) is how
     often trace sync looks for new events when idle. Leave the default.
   - **M7**, optional: `EDGECITADEL_TRANSPORT_POLL_MS` and
     `EDGECITADEL_CLAIM_POLL_MS` (default 50) are the fallback re-checks of the
     outbound loop and of a waiting task claim; both are woken when work
     arrives, so these only bound a change that came without a wake-up (another
     process writing the store). On the Managed Agent runtime,
     `EDGECITADEL_CLAIM_WAIT_MS` (default 1000) is how long an idle slot's claim
     waits in agentd. Leave the defaults. (The experiment harness's lab stacks
     set both fallbacks to 1000 through their `runtime_env`, on the full and
     the bare runtime alike, for the system comparison.)

   Why 30 s: the monitor renews every 10 s (`controller_lease_renew_ms`), so 30 s
   tolerates two lost renewals in a row or a monitor/aggregator restart; a
   renewal never extends past its own command's expiry (`command_lifetime_ms`,
   30 s), so a longer lease buys nothing; and if Core, the monitor or the link
   to it fails, effects stop within 30 s, which is shorter than the scheduled
   effects and tool loops of the experiments. A restarted agentd keeps the lease
   only in memory and denies effects until the next renewal (at most 10 s). The
   value is configurable per node; keep it at least twice the renew interval and
   no longer than the command lifetime.
3. Start agentd. It migrates the database to schema 34 on start. Check:

   ```bash
   edgecitadel doctor                              # service, database, transport, Leaf
   ```

   and that `health` reports the transport connected and telemetry `running`.
4. Start one node after another. In `single-client` mode all nodes share Core's
   `AGENT_INBOX` stream and each agentd adds its agents' subjects to it by
   read-modify-write; agentd processes that start at the same moment can drop
   each other's subjects, after which deliveries to the lost agents fail with
   `NoStreamResponseError`. After the last node is up, list the stream's
   subjects on Core NATS and confirm every agent's `agents.<id>.inbox` is
   present (`nats stream info AGENT_INBOX`). `nats_leaf` nodes own their stream
   in their own domain and are not affected.
5. Pi packages: build and validate from this revision
   (`tools/pi/build-package.py`, `edgecitadel_supervisor lock` and `validate`),
   then `edgecitadel agent install <package>` for the root on jim-eq and the
   worker on Endurance. Installation is transactional and keeps the prior
   package for rollback. Both manifests declare `permissions.effects`
   (`fs.write workspace/*`, `process.exec bash`).
6. The Pi worker's effect gate depends on Pi SDK 0.87.1 honouring
   `extensionFactories` with `noExtensions` and `{block: true}`; it was only
   tested against a stand-in. Verify it on Endurance (step 8) before relying on
   it.

## 5. Grants

A child's recipients and effects are its own grant intersected with what its
delegator could do, so **a root's grant must cover every agent and effect class
below it**, not only its direct children.

- Managed Agent packages take their grant from the manifest
  (`permissions.messaging.outboundAgents`, `permissions.effects`). Change the
  manifest and reinstall to change it. The Pi root currently grants
  `aggregator`, `endurance-pi`, `jim-eq-hermes`; add Cluster's leaves there
  before delegating to them.
- Native connectors (host types `pi`, `claude-code`, `codex`) have **no
  recipients and no effects** until granted. For each native connector found in
  step 1:

  ```bash
  edgecitadel connector grant <connector-id> \
      --outbound-agent <agent> [--outbound-agent <agent> ...] \
      --effect fs.write=workspace/* [--effect process.exec=bash ...]
  edgecitadel connector status <connector-id>      # shows "Delegates to"
  ```

  The grant replaces the previous one. Grant only what the experiment needs;
  `--effect` classes are `fs.write`, `process.exec`, `network.http`,
  `device.command`, targets exact or a `*`-suffixed prefix.
- Hermes is a Managed Agent package (`agent-packages/hermes`); its manifest
  declares no effects, which is correct while Hermes does not call
  `effect.authorize` or `effect.execute`.

## 6. Control keys, one per owner

The monitor's commands are authenticated per **owner** (default `core-monitor`)
with an HMAC key; the broker has no per-subject permissions, so the key is what
lets a command spend that owner's fencing token. Generate it once on jim-eq and
copy the same bytes to every node:

```bash
umask 077
python3 -c 'import secrets; print(secrets.token_hex(32))' > core-monitor.key
sha256sum core-monitor.key                          # compare digests, never the key
```

- Core: place it in the aggregator's data volume, e.g.
  `<compose project>/data/control-keys/core-monitor.key` (container path
  `/data/control-keys/core-monitor.key`), mode 0600.
- Each node: `<agentd state>/control-keys/core-monitor.key`, a regular file (not
  a symbolic link), owned by that node's agentd user, mode 0600, directory 0700.
  agentd strips surrounding whitespace and requires at least 32 bytes.
- Copy with `scp` over the existing SSH access and compare `sha256sum` on both
  ends. Remove the transfer copy afterwards.
- For manual tests use a **separate owner** (for example `operator-drill` with
  its own key on the nodes). Each owner has its own fence; sending a decision as
  `core-monitor` with a high version would make the monitor's later decisions
  `stale_policy_version`.
- Rotation: install the new key on all nodes, then Core, then restart the
  aggregator. Commands signed with the old key fail `not_authenticated`.
- The monitor's policy version lives in `containment.sqlite3`. Deleting that file
  restarts its versions at 1, which every node then rejects as stale for that
  owner. To start a clean monitor between runs, also switch to a new owner name
  and key (fences are per owner), or keep the file.

## 7. Enable the monitor

Write the monitor configuration into Core's data volume, e.g.
`<compose project>/data/containment.json` (no secrets in it; the key is
referenced by path):

```json
{
  "monitor": {
    "owner": "core-monitor",
    "protected_effect_classes": ["device.command", "fs.write"],
    "agent_nodes": {"jim-eq-pi": "jim-eq", "endurance-pi": "endurance"},
    "control_latency_ms": {"jim-eq": 150, "endurance": 150, "cluster": 150},
    "strategy": "firebreak"
  },
  "control": {
    "nats": {
      "key_file": "/data/control-keys/core-monitor.key",
      "confirmation_timeout_ms": 5000,
      "command_lifetime_ms": 30000
    }
  }
}
```

- Node names are the `node.json` `agent_id` values from step 1 (the example
  names are placeholders).
- `control_latency_ms` is an **upper bound** on a control action's
  decide-to-enforced time: the monitor plans an action to land at
  now + tau + L, so a smaller L makes a late cut look feasible. Use at least
  the largest sent-to-confirmed time you measured (the lab uses 150 ms at
  every node; pre-M7 p50 18 ms, p95 82 ms, max 124 ms). The other timing
  values (delivery, start, emission) are **lower bounds**: use the smallest
  you measured, never a typical one, or the projection becomes unsound.
- Grants come from the authority agentd exports on `run/started`; `grants` and
  `effect_scopes` are only a fallback and can stay unset.
- In the Core environment (`/etc/edgecitadel/env` or the compose `.env`):
  `EDGECITADEL_TRACE_COLLECTOR=1`, `EDGECITADEL_CONTAINMENT_MONITOR=1`,
  `EDGECITADEL_CONTAINMENT_CONFIG=/data/containment.json`. The monitor uses
  Core's own `NATS_URL`/`NATS_TOKEN`.
- Defaults that encode team decisions, so the file above does not repeat
  them: `horizon: 1` with `boundary: "sink"`, `controller_lease_renew_ms:
  10000`, and **M5** `profile: "deployed"`: the rule-based baselines apply their
  rule directly, the three cut strategies run the two-phase protocol with the
  best-effort guard and taint-aware admission (which Theorem 5 needs; **M6**:
  its labels are egress holds whose events the monitor admits one by one) and,
  **M6**, the adaptive phase 1. Set them
  explicitly only to deviate (`controller_lease_renew_ms: 0` stops renewing,
  which makes lease-enforcing nodes deny all effects; `profile: "shared"` gives
  every strategy every mechanism, an ablation; `mechanisms: {"<strategy>" |
  "*": {"phase1_freeze" | "best_effort_guard" | "taint_aware_admission":
  bool}}` overrides one strategy's mechanisms).
- Other settings (defaults shown; set only to deviate): **M6**
  `phase1_mode` unset (the profile's: `adaptive` for the deployed cut
  strategies, `freeze` in the shared ablation; `freeze` and `freeze_plus_cut`
  are ablations; with `phase1_margin_rounds: 2`), **M5**
  `phase1_labels: "frontier"` (or `forward_closure`),
  `receding_during_waits: false`, **M6** `future_requests: {}` and
  `future_request_value: 1.0` (a service agent's expected future requests,
  which revoking its credential or admission forfeits),
  **M4** `ingest_timeout_ms: 5000` (how long a
  release waits for the confirmed journal positions to be ingested; on timeout
  the decision ends `ingest_timeout` and keeps its phase-1 holds),
  `enforce: true` (`false`: compute and log decisions, send no command, but
  still describe nodes and renew leases, for runs that must not contain),
  and `shared_memory_agents: []` (agents whose
  memory persists across contexts, so their invocations share one session).
- With leases on, the monitor renews them from the moment it starts, before
  Core's collector is ready and whether or not an alert ever arrives; a
  non-enforcing run uses `enforce: false`, not a stopped monitor.
- `agent_nodes` must name at least one agent on every node: the monitor renews
  leases (and describes capabilities) on the nodes it knows, and a node it has
  not seen in any event is otherwise unknown until its first event.

Restart the aggregator and check, with the administrator token from the Core
environment (do not paste it into shared shells or tickets):

```bash
curl -s -H "X-EdgeCitadel-Admin-Token: $EDGECITADEL_ADMIN_TOKEN" \
  http://127.0.0.1/api/containment/status | python3 -m json.tool
```

Expect `state: running`, `cursor.lag` falling to 0, under `control.capabilities`
every node with its `describe` answer (13 pairs from `hold:task` to
`renew:controller`), under `ingest` a watermark per node with `gap_from: 0`,
and under `control.controller_leases` every node with `enforced: true`,
`error: null` and `until_ms` 20 to 30 s ahead. A node with an
empty capability set has not answered `describe`; a lease `error` of
`not_authenticated` is a key mismatch, `stale_policy_version` a monitor database
or owner change (step 6), `node_unreachable` a transport problem. While any node
shows an error, that node denies effects.

## 8. Smoke checks

1. **Transport and telemetry.** A chat task through the Pi root to Endurance
   completes; the monitor's `g_p` counts grow; the root's `run/started` event
   carries `attributes.authority` (view it with `edgecitadel trace show`).
2. **Effect gate (Endurance).** Ask the Pi worker to write a file in its
   workspace (allowed: `effect/requested` then `effect/committed`) and outside it
   (denied `outside_effect_scope`, and the model reports the block). If tools run
   without these events, the SDK contract from step 4.6 does not hold: stop and
   report.
   **M4**, on a node with adapters: ask `effect.execute` for a harmless Home
   Assistant service (for example `light.turn_on/light.test_lamp` on a test
   entity) and check `effect/committed` with `adapter: homeassistant` and
   `committed_at_ms`, then the result's `outcome.performed: true`. With the
   task held the same request is `effect/denied` and Home Assistant receives
   nothing.
3. **Control channel.** Under the drill owner, hold and release an unused task
   ID on each node with a signed command (or `containment.control` on the
   node's admin socket) and check the confirmation's `status: applied` and
   `applied_at_ms`. `containment.list` on the node shows the hold in between.
4. **Containment drill** (optional, harmless): run a scripted task chain and post
   an operator alert for a test task (`POST /api/containment/alerts` with
   `{"kind": "operator", "task_id": ...}`). Status shows the decision, each
   action's `t_alert`, `t_decision`, `t_sent`, `t_applied`, `t_confirmed`, and no
   `unconfirmed` entries; `containment.list` on the nodes shows exactly the
   actions in force. The same scenario as the local end-to-end test can be
   reproduced off the testbed with `python -m e2e.containment.c2`.

Lifting a decision: the monitor never releases the actions it chose in phase 2,
and it has no incident-close API in this revision. To lift them, release each
hold on its node with owner `core-monitor` and `policy_version` equal to the
monitor's current `status.policy_version` (equal is accepted and does not
advance the fence). Revocations are irreversible; grant and admission
revocations are stored as ingress filters and are lifted with `unfilter`.
Afterwards the monitor still lists those actions as in force; restart it with a
new owner (step 6) before the next run.

## 9. Rollback

- **Monitor only:** release what it holds (step 8). With controller leases on,
  disabling the monitor stops all effects within 30 s, so first remove
  `EDGECITADEL_CONTROLLER_LEASE_MS` from the agentd units and restart them one
  at a time. Then set `EDGECITADEL_CONTAINMENT_MONITOR=0` and restart the
  aggregator. `containment.sqlite3` and `containment-decisions.jsonl` can stay;
  they are the experiment record.
- **Everything:** stop Managed Agents and agentd on every node, restore each
  node's state backup from step 2 as a unit, reinstall the previous runtime, and
  start the nodes one at a time. A schema 34 database cannot be opened by the
  previous agentd ("newer than supported"), so restoring the backup is required,
  and tasks and traces recorded after the upgrade are lost with it: export what
  you need first. Reinstall the previous Pi packages with
  `edgecitadel agent install <prior package>`. On jim-eq restore Core's data
  directory if the projection or collector state must return to v8, and restart
  the previous images (the prior dashboard image tag is the rollback image).
- Remove the control keys from nodes that return to the previous version.

## 10. Adding Raspberry Pis later

Enroll each Pi as an Edge (`edgecitadel install --join ... --messaging-mode
nats_leaf` keeps local messaging when the Core link drops), install the schema 34
runtime, set `EDGECITADEL_TRACE_SYNC=1` and `EDGECITADEL_CONTROLLER_LEASE_MS`,
copy the owner key (step 6), add one of its agents to `agent_nodes`, grant its
connectors (step 5), extend the root's grant to its agents, and add its node to
`control_latency_ms` with a measured upper bound on its sent-to-confirmed
time. Check that the monitor lists
its capabilities. Effects on real devices must be reversible and
interlocked locally: holding an effect stops a commit through agentd, not a
device that already received a command.

## Known gaps before relying on the testbed

- agentd admin writes such as `connector.register` can fail once with "agentd
  operation failed" (SQLite "database is locked") while the telemetry thread
  writes right after start: they read before writing in a deferred transaction.
  The write rolled back; repeat the command. **M6**: on an ordinary store (the
  experiment nodes of the harness and of `e2e/containment`, which have no
  provisioned completion workspace) the cause was two write transactions of
  agentd's own and its telemetry connection each waiting at commit for the
  other (a session renewal writing the task member and reading the trace
  member, the exporter's broker-ack checkpoint the reverse), both failing
  after the 5 s busy timeout with agentd's store lock held. Such stores now
  serialize their connections' writes with one lock per file, as the
  workspace lock does on a provisioned store (journal mode and lock order
  unchanged); deploy schema 34 agentd to the lab nodes to get it.

- No operator command rebuilds a v8 trace projection (step 3).
- No incident close or release API in the monitor (step 8).
- Resetting the monitor database requires a new owner (step 6).
- The `AGENT_INBOX` subject list is updated by read-modify-write; start
  `single-client` nodes one at a time (step 4.4).
- The Pi worker effect gate is unverified against the real SDK (step 4.6).
- Unconfirmed actions are reported, not reconciled automatically: compare
  status `unconfirmed` with `containment.list` on the node.
- Effects decided by `effect.authorize` and performed by the harness (bash,
  sandbox files) are not fenced at commit; measure their bypass at the
  destination. Only `effect.execute` effects are.
- A gap in a node's journal as Core collected it (status `ingest.gap_from`)
  keeps every later command on that node unfenced, so its phase-1 holds stay.
- A delegation root without an agentd task (a native session's own work)
  cannot be held or labelled; only its delegated tasks can.
