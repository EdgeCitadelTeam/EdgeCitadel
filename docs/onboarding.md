# EdgeCitadel newcomer onboarding

There is one product command: `edgecitadel`. Homebrew or pip installs it for both
Core and Edge nodes; users choose whether this host creates a network or joins
one. They do not install the Supervisor separately. In a source checkout, the
same command is available as `./scripts/edgecitadel`.

The public tap and PyPI distribution are not yet published. The repository
Formula is HEAD-only and the wheel is source-build-only until a verified release
is explicitly published; see `deploy/homebrew/README.md` and
`deploy/pip/README.md`.

## Node role and messaging mode

`core` and `edge` are node roles. Every Edge independently selects one of two
messaging modes during its first `join`:

| Mode | Plugin broker | Core disconnected | Local NATS |
|---|---|---|---|
| `single-client` (default) | Core NATS | Agent messaging pauses | Not started |
| `nats_leaf` | `127.0.0.1:4223` | Same-host messaging continues; cross-node pauses | User-level service |

The names are exact public API values. A host already joined in one mode rejects
a conflicting `join`; topology conversion is intentionally not implicit.

### Create the first node

```bash
brew tap zhonghaozhan/edgecitadel
brew install edgecitadel
edgecitadel create
```

Alternatively, install the wheel in an isolated Python environment:

```bash
python3 -m venv ~/.edgecitadel/cli-venv
~/.edgecitadel/cli-venv/bin/python -m pip install edgecitadel
~/.edgecitadel/cli-venv/bin/edgecitadel create
```

This one command creates `.env` with generated local secrets, creates data
directories, renders NATS configuration, starts Docker Compose, records the
node as `core`, and waits for NATS, JetStream, and the API. Rerunning it is safe
and preserves existing secrets. Use `--host <reachable-name-or-ip>` when other
machines will connect.

### Join an additional host

On the core:

```bash
edgecitadel invite --node-id studio-macmini --host 100.64.0.10
```

On the Mac mini or Linux edge host:

```bash
brew tap zhonghaozhan/edgecitadel
brew install edgecitadel
edgecitadel join 'ecjoin://...' --messaging-mode single-client
```

The same `pip install` flow above can be used on an Edge. A pip-installed
`nats_leaf` Edge must install the native `nats-server` executable separately;
`single-client` does not require it.

The invitation is short-lived and can be redeemed once. The API stores only
its SHA-256 digest. Redeeming it writes the node configuration with mode `0600`.
Omitting `--messaging-mode` is equivalent to `single-client`, preserving old
commands and version-1 Edge state.

For an Edge that must retain local Agent-to-Agent messaging when Core is down:

```bash
edgecitadel join 'ecjoin://...' --messaging-mode nats_leaf
```

This mode validates `nats-server` and loopback ports before redeeming the
invitation, creates a private Edge JetStream domain under
`~/.edgecitadel/nats_leaf`, and starts an authenticated outbound Leaf link to
Core port 7422. Plugins receive only the loopback client endpoint and local
token; they never receive Leaf credentials. The current implementation uses a
separately scoped fleet Leaf credential, so per-node NKey/JWT identities and
NATS accounts/subject ACLs remain required before internet exposure.

## Make an agent visible

A joined host is a place where agents may run; it is not itself an agent. To
connect Codex, Claude, OpenClaw, or another runtime, install that runtime's
Plugin on the same host:

```bash
edgecitadel plugin install ./path/to/plugin
```

On the first plugin command, the CLI creates its private Supervisor Python
environment and installs the repository toolkit automatically. Installation is:

1. Validate manifest, schemas, compatibility, paths, and package lock without
   executing plugin code.
2. Display requested knowledge, messaging, network, device, sandbox, and secret
   permissions. Require approval (`--yes` for automation).
3. Reject Agent IDs already claimed by another local Plugin or Core registry
   owner.
4. Copy the verified package to the Supervisor-owned read-only store under
   `~/.edgecitadel/plugins/`, preserving declared executable entrypoints.
5. Reconcile exact destination inbox ownership, then start the runtime through
   an owned process-group runner with the declared restart policy and the
   mode-selected NATS URL/token and JetStream domain injected.
6. Wait until every declared Agent ID is online in the core registry.

The Supervisor records a process-instance identity as well as the PID. Stop and
remove operations signal the verified process group, including Plugin children;
they refuse to signal a live PID whose identity no longer matches stored state.

The runtime then joins the messaging plane in this exact order:

```text
plugin runtime -> NATS connect
plugin runtime -> agents.<id>.register  (Agent Card)
aggregator     -> SQLite registry       (agent becomes visible)
plugin runtime -> agents.<id>.heartbeat (agent remains online)
JetStream      -> agents.<id>.inbox     (durable commands/delegations)
plugin runtime -> agents.<sender>.inbox (correlated result)
```

The working `plugins/examples/echo` package proves this path without requiring
an external model account. Host enrollment and Agent Card registration are
deliberately separate: one host can run zero, one, or several agent plugins.

## Operations

```bash
edgecitadel status
edgecitadel doctor
edgecitadel plugin list
edgecitadel plugin start edgecitadel.echo
edgecitadel plugin stop edgecitadel.echo
edgecitadel plugin logs edgecitadel.echo
edgecitadel plugin remove edgecitadel.echo
edgecitadel supervisor start
edgecitadel supervisor stop
edgecitadel supervisor status
edgecitadel messaging status       # nats_leaf Edge only
edgecitadel messaging restart      # nats_leaf Edge only
edgecitadel down
```

`doctor --json` exposes stable checks for the process, local client, JetStream,
Leaf connection, Core, and cross-node path. A healthy local broker with a
disconnected Leaf is `degraded`: local messages remain available and cross-node
messages are paused. `down` preserves core data. Plugin removal preserves the plugin log. Local node
and plugin state defaults to `~/.edgecitadel`; tests and advanced operators may
override it with `EDGECITADEL_STATE_DIR`.

## What is automatic and what is not

The Homebrew and pip layouts remove all manual `.env`, directory, broker
rendering, Compose, and Supervisor-install commands. Neither installs Docker:
only Core creation checks for a running Docker Desktop/Engine. The Formula
installs the `nats-server` binary needed by `nats_leaf`; pip users install that
native executable separately. `single-client` never starts it. Public installs
still require separate, explicitly authorized release/tap/PyPI publication.

The Supervisor owns package validation, permission approval, immutable install,
process start/stop, broker credential injection, readiness confirmation, status,
and logs. In v0.1 the declared sandbox and permission set is reviewed but is not
yet an operating-system enforcement boundary. The plugin owns runtime-specific
authentication, its Agent Card, heartbeats, durable inbox handling, and
correlated results. The core owns enrollment, messaging infrastructure,
registry persistence, API, and dashboard.

## Troubleshooting

- `create` says Docker is missing: install Docker Desktop or Docker Engine and
  rerun the same command.
- An invitation is expired or used: create a new invitation on the core.
- `join` cannot reach the core: use a reachable LAN/Tailscale hostname and make
  ports 80 and 4222 reachable; `nats_leaf` also needs outbound access to Core
  port 7422.
- `doctor` reports `degraded`: local agents can still communicate; restore Core
  reachability or the Leaf credential, then run `edgecitadel messaging restart`.
- A cross-node command fails while disconnected: it was not reported as durably
  accepted. Retry with the same logical message/task ID after the Leaf returns.
- A plugin starts but never becomes visible: inspect `plugin logs <package-id>`,
  then run `doctor` on the edge host.
- To inspect technical errors, place `--verbose` before the command.

## Recovery and uninstall

Use `plugin stop` before inspecting or backing up a plugin. `plugin remove`
removes only its immutable installed copy and preserves its log. `down` stops a
core stack without deleting SQLite, JetStream, node state, or `.env`.

Before uninstalling either package on a `nats_leaf` Edge, run
`edgecitadel supervisor stop` and `edgecitadel messaging stop`. Homebrew removes
the installed binaries and `python -m pip uninstall edgecitadel` removes the
wheel; both deliberately preserve `~/.edgecitadel`, including local JetStream
data. Package upgrades also preserve this state, and `messaging restart` reloads
launchd with the current `nats-server` binary path.

There is intentionally no broad `reset` command in v0.1. A full uninstall must
be a deliberate manual operation after backup: stop the Supervisor and core,
then remove only this checkout's `data/`, `nats/data/`, and `.env`, plus the
specific node state directory (normally `~/.edgecitadel`). Never point a
recursive deletion at a home directory or an unverified path.
