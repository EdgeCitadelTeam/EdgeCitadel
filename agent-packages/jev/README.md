# JEV

JEV (`jev`) is a persistent managed Agent. It chooses one text execution or
execution followed by review using TypeSafe `jev-latest` at
`https://api.typesafe.ai/v1/systemone`. The model returns a choice from enumerated
workflows and authorized executors, never an executable arbitrary plan.

The first adapter is `reasoning.chat`: input and output are objects containing a
string `body`. The initial grant is `jim-eq-hermes`; it must have an active online
presence and advertise that capability. Both steps may use that same executor.
The initial deployment shares the leaf agentd with Hermes so local capabilities
and session presence can be checked. Remote-only identities without advertised
capabilities are deliberately ineligible. Adding an executor requires an explicit
contract adapter/allowlist update and the manifest messaging grant, followed by a
new package version and lock.

## API

Use the existing `POST /api/command/jev` endpoint:

```json
{"body":"Explain this design, then review the answer.","skill_id":"jev.run","args":{"request_id":"eb365a98-4286-45fa-91cb-59f314179357"}}
```

Retain the request UUID for transport retries. Reusing it with another goal is
rejected. A duplicate submission returns the saved run without advancing it.
Use a new request ID when supplying clarification or changing the goal.

```json
{"body":"Continue","skill_id":"jev.resume","args":{"run_id":"<returned run_id>"}}
```

Resume creates a new parent invocation, reads the existing child tasks, and only
advances steps that have never been attempted. Run ownership follows the sender
identity supplied by the existing command API; this does not add user identity or
an authentication boundary to the dashboard. Output includes `body`, `run_id`,
`outcome`, `resumable`, and `steps` with actual executors and stable task IDs.
The dashboard shows those child tasks in the existing Agent Flow explorer.

## Persistence and failure behavior

`EDGECITADEL_PLUGIN_STATE_DIR/jev.sqlite3` stores request deduplication, decisions,
step IDs, dispatch-attempt markers and observed states. Agentd remains authority
for results and task lifecycle. The managed runtime serializes goals and renews
its session while waiting; additional goals remain in the durable agentd inbox.

Each child UUID is committed before `trace.dispatch(child_task_id=...)`. The
attempt marker is committed before sending. Agentd commits the child and its
trace/permission evidence atomically, and rejects reuse of the child ID. If the
process dies between the attempt marker and dispatch, the state is ambiguous:
JEV does not resubmit it. An explicit resume queries that same ID and reports
unknown if absent. This conservative boundary sacrifices automatic liveness to
avoid repeating remote effects. An operator must investigate unknown work.

On startup JEV scans unfinished runs and records current child observations,
without calling the model or dispatching further steps. Explicit resume never
redispatches a completed step. Failed, expired, cancelled, denied or undeliverable
work is terminal and is not retried. Missing results, unknown tasks, offline
selected executors and wait timeouts interrupt the run. A completed first step
with an oversized review input is reported without dispatching the review.

Child waits are bounded to five minutes each; a coordination invocation to ten
minutes; model HTTP requests to thirty seconds. Stopping waiting does **not**
cancel a remote task. The dashboard's Continue control queries the saved run;
while an invocation is active it queues behind it. Historical progress controls
can therefore safely reference runs that have since completed.

## Install and operate on jim-eq

Use the same matching runtime/schema release for agentd and the CLI. The runtime
must support caller-reserved `child_task_id` in trace dispatch and the managed
startup hook. Do not point an old daemon at a new schema without upgrading its
code. Preserve the running Hermes launch configuration and existing task/trace
storage when upgrading the leaf service; never restart the main NATS for this.

The production state is `/var/lib/edgecitadel-leaf/state`. JEV uses a distinct
`managed-jev` credential and `plugin-state/edgecitadel.jev/` directory; it does not
rename or reuse `demo-router`, the demo viewer or either remote demo host.

With a matching checkout at `/opt/edgecitadel/jev-20260926`, run as the
`edgecitadel-leaf` service account with `NoNewPrivileges` and its user manager
environment. First validate and install disabled:

```sh
/absolute/python -m edgecitadel_supervisor validate /opt/edgecitadel/jev-20260926/agent-packages/jev
/absolute/python /opt/edgecitadel/jev-20260926/scripts/edgecitadel agent install /opt/edgecitadel/jev-20260926/agent-packages/jev --state-dir /var/lib/edgecitadel-leaf/state --yes --keep-disabled
```

Supply `TYPESAFE_API_KEY` in a private environment file, owned/readable by that
service account, using `.env.example` as the shape. Load it into the CLI's process
environment before `agent start edgecitadel.jev --state-dir ...`. The existing
manifest-declared secret injection copies it into the private managed launch
configuration for restarts. Do not extract a key from the old demo process.
The entrypoint refuses startup with a clear error when the key is absent.
Validate credentials, the package lock and the exact messaging grant before
starting. A missing key means live model acceptance remains blocked, not passed.

Stop or roll back with the existing `agent stop edgecitadel.jev` lifecycle and
restore the matching release if needed. Retain `jev.sqlite3`, agentd storage and
credentials. Stop/start resumes observations only; users explicitly continue runs.

## Verification

`agent-runtime/tests/agentd/test_jev.py` covers choices, filtering, contracts,
timeouts, durable duplicate requests, dispatch crash boundaries, recovery and
real agentd SQLite/trace integration. `test_trace_dispatch_store.py` also covers
caller IDs, transactional duplicate rejection and permission receipts.
`frontend/src/components/JevRun.test.jsx` covers submission retries, direct
commands, offline state, child links and resume. `e2e/tests/jev-ui.spec.js` covers
the browser workflow with API fixtures; it does not claim live model acceptance.

Live acceptance additionally needs a real TypeSafe key and Hermes: one-step and
review runs, parent/child trace inspection, idle lease renewal, JEV restart and
explicit resume, old Agent health, and a direct Hermes call during model failure.
Record the base revision plus working-tree file hashes for an uncommitted release.
