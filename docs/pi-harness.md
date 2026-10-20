# Persistent Pi harness on EdgeCitadel

Pi is a resident Managed Agent, not a browser script or a demo worker. The root
runs on jim-eq and delegates through agentd's actual `trace.dispatch` API. A second
resident Pi session on Endurance executes with local Qwen and Pi's read, bash,
edit and write tools. The existing JEV and Hermes packages remain independent.

## Collaborator entry point

On jim-eq, run **`pi`** in a terminal. The installed command is a small HTTP
client of the existing `jim-eq-pi` service, not another local model process or
the upstream Pi coding-agent TUI. No API key or additional harness is required
on the client. It displays the roster, delegation/tool progress, result and
parent/child task IDs. For a single task:

```bash
pi --status
pi "Ask Endurance to report its hostname and Python version."
```

The interactive commands are `/agents`, `/new`, `/history`, `/wait`, `/retry`,
`/help` and `/quit`. The default conversation survives exit/reconnect in
`~/.local/state/edgecitadel-pi/chat.json` (private to the Linux user). Use
`pi --new` for a new conversation or `pi --state-file PATH` for a separate
terminal/conversation, including separate people sharing a Linux account.
This separation is not a server-side identity or access-control boundary.

Ctrl-C detaches; it does not cancel remote work. A saved accepted task is polled
on reconnect without resubmission. If acceptance is unknown, `/retry` explicitly
resends the saved request ID and identical payload, using the root's existing
deduplication. New tasks cannot replace an unresolved local request.

The jim-eq installation uses `/usr/local/bin/pi` pointing to
`/opt/edgecitadel/pi-20260927/client/chat.py`, installed from `tools/pi/chat.py`.
It requires only system Python 3.12 and the existing localhost Core API.

Open **http://100.97.29.74/pi.html** from a machine with access to the existing
Tailscale fleet. Send a natural-language task; the page preserves a conversation
ID across reloads. **New conversation** creates a separate session. Task and child
links open the actual EdgeCitadel execution view. Tool progress and results come
from the production message API; the page does not execute remote work itself.

Example: “On Endurance, inspect the Python environment and write a short report
in your workspace.” Follow up: “Read that report and add the GPU information.”

The shared Core currently identifies dashboard requests as `aggregator`.
Conversation IDs separate context; they are not authentication or per-user ACLs.
Use the existing protected fleet access. Do not expose this endpoint as an
unauthenticated public multi-user execution service.

The same interface is available programmatically:

```bash
curl http://100.97.29.74/api/command/jim-eq-pi \
  -H 'Content-Type: application/json' \
  --data '{"body":"On Endurance, inspect the workspace and report what you find.","skill_id":"pi.coordinate","args":{"session_id":"my-conversation","request_id":"a-unique-request-id"}}'
```

The response is an acceptance receipt, not completion. Query
`GET /api/messages?task_id=<returned-id>&limit=200` for progress and the terminal
`result`. Keep `session_id` for follow-ups; use a fresh `request_id` for new work.
Retry an uncertain submission with the **same** request ID, body and session ID.
The root returns the original result/child if already processed. It never silently
repeats a previously attempted remote dispatch.

## Responsibilities and limits

### Where the Jev integration lives

The installed root adapter is
`/var/lib/edgecitadel-leaf/state/plugins/edgecitadel.pi-root/0.1.1/pi_runtime/__main__.py`.
Its `select()` function calls `POST https://api.typesafe.ai/v1/systemone` with
`model: jev-latest`, the request/recent context and a typed `choice` question.
The `Authorization: Bearer ...` value comes from `TYPESAFE_API_KEY` in the
Python adapter environment. The protected source credential file is
`/var/lib/edgecitadel-leaf/state/credentials/jev.env` (0600, service-user owned).
`tools/pi/activate-root.py` loads it at installation; the managed launcher
retains the declared secret in private service state for subsequent restarts.
The Node Pi subprocess explicitly excludes that key from its environment.
Neither the terminal client nor the web page calls TypeSafe directly.

### Runtime responsibilities

- **Pi:** the official `@earendil-works/pi-coding-agent` SDK, pinned to 0.87.1.
  Real `AgentSession` objects retain conversation and tool results; `SessionManager`
  writes private JSONL transcripts and reopens them after process restarts.
- **Jev:** selects one explicitly granted, online executor. It supplies a typed
  choice, not generated prose. The Pi provider maps this choice to its delegation
  tool and relays the executor's actual text result. One remote dispatch per turn.
- **EdgeCitadel:** connector identity, session leases, recipient grants, NATS leaf
  transport, tasks, results and trace correlation. No NATS credential is handed to
  the Pi Node process. Jev's key stays in the jim-eq Python adapter, loaded through
  the existing declared-secret mechanism; it is absent from the browser and worker.
- **Endurance:** a real local Qwen `qwen3.8:27b` inference/tool loop, with workspace
  `/home/zhonghao/edgecitadel-workspace`. Tools run with the service user's OS
  permissions. The workspace instruction and manifest `sandbox: restricted` are
  not an OS sandbox or a filesystem confinement guarantee.

Authorized root destinations are `endurance-pi` (`pi.execute`) and
`jim-eq-hermes` (`reasoning.chat`). Agentd currently omits capabilities from remote
presence rows, so the adapter intersects that presence with the Core's online
Agent Cards and the explicit grant map. A model choice cannot add a destination.
This is a configured fleet router; autonomous multi-step planning, general
capability discovery and arbitrary new destinations are not implemented here.

Full provider errors and interrupted work are not converted into success. Root
waiting is bounded to 20 minutes and the Pi turn to 25 minutes. A wait timeout
leaves remote completion unconfirmed; it neither cancels nor redispatches the
remote task. Inspect the saved child ID. Explicit observe-only resume within this
Pi package is not yet implemented. Reusing an interrupted request never reruns it.
The runtime serializes tasks per managed identity; concurrent users are queued.
Jev receives the current request and recent conversation context, which can include
text returned by a worker. This routing path does not promise that all result text
stays local to Endurance.

## Containment hooks

The Pi adapter cooperates with agentd's containment of running work
([pause, resume and cancel](architecture/execution-trace-contract.md#pause-resume-and-cancel-of-running-managed-work),
[effect gateway](architecture/execution-trace-contract.md#effect-gateway)):

- **Pause and cancel.** When the managed runtime cancels the handler, the adapter
  sends `abort` to `session.mjs`, rejects any adapter call the Pi turn is waiting
  on, and waits up to 5 s for the turn to end before stopping the process. The
  request journal records `paused` or `cancelled`. On resume after a pause, the
  worker re-prompts the same Pi session with a resume instruction; the root
  re-routes the original request and waits for a child it already dispatched
  instead of dispatching again. Cancelled or interrupted turns are not repeated.
- **Effect gate (worker).** `session.mjs` installs an inline extension whose
  `tool_call` handler asks the adapter (`effect` IPC call, which calls
  `effect.authorize` with the task's binding) before every tool except `read`,
  `grep`, `find` and `ls`. `write` and `edit` are `fs.write` with target
  `workspace/<path>` inside the workspace or the absolute path outside it
  (lexical resolution, not an OS sandbox); every other tool, including `bash`, is
  `process.exec` with the tool name as target. A denial or an unreachable agentd
  blocks the tool, and the model sees the reason. If the extension cannot be
  installed, the worker refuses to run tools at all.
- **Grants.** Both manifests declare `permissions.effects`
  `fs.write workspace/*` and `process.exec bash`. The root holds them only to
  delegate them, because a child's effects never exceed its delegator's.

The gate relies on SDK 0.87.1 accepting `DefaultResourceLoader({extensionFactories})`
with `noExtensions: true` and honouring `{block: true}` from a `tool_call`
handler. This was tested only against a scripted stand-in for the SDK
(`tests/pi/test_pi_effect_gate.py`); verify it on Endurance before relying on it.

## Installation artifacts

Source: `agent-packages/pi-root/`; shared runtime supports both roles.
`tools/pi/build-package.py worker <directory>` builds a standalone worker package
with its own identity, manifest and schemas. Run `edgecitadel_supervisor lock` and
`validate` on generated packages before installation. No runtime symlink back to
the repository or old demo is used.

`tools/pi/install-node.sh <absolute-directory>` installs checksum-verified Node
24.21.0 and the pinned Pi SDK without replacing system Node. Declare `PI_ROLE`,
`PI_NODE`, `PI_SDK_DIR` and `PI_WORKSPACE` through the package environment.

The current root depends on the deployed September 26 runtime extension supporting
caller-reserved `child_task_id` in `trace.dispatch` and managed `agent.list`. These
changes are not yet in upstream `db542b7`; installing the root against unmodified
upstream is not sufficient. Endurance uses upstream db542b7's runtime. This work
preserves the existing runtime extension rather than overwriting it.

## Current service layout

| Component | Location |
|---|---|
| Root managed package | `edgecitadel.pi-root` 0.1.1, identity `jim-eq-pi` |
| Root state | `/var/lib/edgecitadel-leaf/state/plugin-state/edgecitadel.pi-root` |
| Root SDK release | `/opt/edgecitadel/pi-20260927/runtime` |
| Root service | existing `edgecitadel-agentd-b5184c2bc72c.service`, user `edgecitadel-leaf` |
| Worker package | `edgecitadel.pi-worker` 0.1.1, identity `endurance-pi` |
| Worker state | `/home/zhonghao/.edgecitadel/plugin-state/edgecitadel.pi-worker` |
| Worker source/SDK | `/home/zhonghao/edgecitadel-pi` |
| Worker agentd | `edgecitadel-agentd-ee441de5009e.service`, user `zhonghao` |
| Worker broker | `edgecitadel-pi-leaf.service`, user `zhonghao` |
| Worker trace volume | `/var/lib/edgecitadel-endurance/trace.ext4` mounted at `~/.edgecitadel/agentd/trace` |

Endurance user lingering and both user units are enabled. The leaf uses its
existing PID file/configuration and automatic restart. Trace sync is enabled in
the agentd drop-in; inherited capabilities are cleared with `setpriv` to satisfy
the production trace writer. Its dedicated ext4 volume enforces the existing
256 MiB/128-inode user quota. The one-time provisioning script creates a 512 MiB
image, not a physical disk partition. The user ran it with sudo and the real
storage verifier passed. A reboot was not performed during acceptance.

Use the deployed CLI with the matching `EDGECITADEL_SUPERVISOR_PYTHON` for managed
Agent stop/start/status. Endurance additionally uses `EDGECITADEL_DISTRIBUTION=pip`
to select the systemd backend. Do not stop the shared jim-eq Core to manage Pi.
Preserve package state, credentials, transcripts and databases during upgrades.
The dashboard image `edgecitadel-dashboard:pi-live-20260927` adds only `/pi.html`
to `edgecitadel-dashboard:jev-live-20260926`; the base remains a rollback image.

## Verified on 2026-09-27

1. A browser request traversed jim-eq Pi → live Jev → actual EdgeCitadel dispatch
   → Endurance Pi → local Qwen/tools → real EdgeCitadel result → browser.
   Parent: `cb8fadd4-70a0-4525-ac24-56db4530d2d0`;
   child: `024dcf6e-6cb0-42d6-b4d6-f3dac50f026f`.
2. Endurance created `pi-e2e-20260927.txt` using actual hostname, UTC time and Python
   version. SSH verification matched the reported content and SHA-256.
3. Root adapter process was killed while idle; agentd restarted it (264709 →
   267157). Endurance agentd was restarted, restoring its managed Pi worker.
   A follow-up gave no filename and successfully reused the earlier file and
   session, appended one line and returned the matching new SHA-256.
   Parent: `c61badf8-2e84-45ae-9ad4-7e78eeaa49db`;
   child: `088bbda3-7938-4357-809c-6b63c9069dac`.
4. Repeating that exact request ID returned the original child; the line still
   occurred once and the file checksum stayed unchanged.
5. A separate live request routed to existing Hermes and returned actual text.
6. Core trace `cea09d032e8b4f08a4baa2916cc7f36c` contains both agents, the parent-child
   edge, Jev model span, Pi delegation span, Endurance model/tool spans and transport
   evidence. Core still reports `partial=true` / `unknown_sources=true`; this is
   not a claim of complete fleet-wide PROV coverage. Missing token usage is marked
   unavailable rather than reporting a measured zero.
7. Four focused tests cover remote capability validation, explicit grants,
   persistence before uncertain dispatch, and refusal to replay interrupted work.
   Both package locks validate; the actual browser submit/result path passed.

Private transcripts and credential files are intentionally not included in the
repository. Sanitized local evidence is under `local-docs/pi-20260927/` (ignored).
