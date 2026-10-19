# EdgeCitadel onboarding and troubleshooting

The `edgecitadel` command installs on both Core and Edge hosts. A Core provides
enrollment, shared NATS/JetStream, the API, and dashboard. An Edge runs the
host-local EdgeCitadel service, Agents installed from Agent Packages, and
Connector sessions opened by Plugins.

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) if it is
not already available, then install EdgeCitadel as an isolated CLI tool:

```bash
uv tool install edgecitadel
```

On macOS, Homebrew is also supported through the EdgeCitadel tap:

```bash
brew tap EdgeCitadelTeam/edgecitadel
brew trust --tap EdgeCitadelTeam/edgecitadel
brew install edgecitadel
```

Homebrew 6 requires the trust step before loading formulas from a non-official
tap.

Verify the installation, then run the guided installer:

```bash
edgecitadel --version
edgecitadel install
```

`uv tool install` avoids modifying an operating-system-managed Python
environment. The [Python distribution guide](../deploy/pip/README.md) documents
a manual virtual-environment fallback.

The guided installer enrolls the host, starts agentd, detects supported native
hosts, installs only the Plugins the user selects, and reports Plugin package
state separately from Connector activity. Use explicit `--create` or `--join`,
one or more `--plugin`, `--scope`, and `--yes` flags for non-interactive use;
when joining, select `--messaging-mode single-client|nats_leaf`. Use the same
package manager for upgrades and uninstall.

In an interactive terminal, the guide proceeds in this order:

1. Choose `join` or `create`.
2. Enter the one-time invitation, or choose local-only, Tailscale, or custom Core access.
3. On an Edge, choose `single-client` or `nats_leaf` after reading the tradeoff.
4. Select from the native agent hosts detected on the machine.
5. Review the exact Plugin installation plan and confirm it.

## Create a Core

Run `edgecitadel install` and choose `create` to configure the server, agentd and
selected Plugins. Run `edgecitadel create` for the same deployment guide with
server setup only. All three choices run the full Core on this computer; they
do not provision a cloud instance or deploy to a different computer.

1. **Local only:** publish the API, NATS clients, Leaf listener and monitoring on
   `127.0.0.1`. Other computers cannot use this managed deployment directly.
2. **Accessible over Tailscale:** detect this computer's connected Tailscale IPv4
   and publish API/client/Leaf access there as well as on loopback. Monitoring
   stays local. Install and sign in to Tailscale externally before setup; the
   guide offers detection retry. Every connecting computer must join the same
   tailnet and have appropriate access.
3. **Accessible at your own IP or hostname:** advertise an address reachable by
   your agents on an operator-protected network. The listening address must be
   assigned to this computer. If DNS/public/NAT addresses do not identify one
   assigned IP, supply `--bind-address`. This does not configure public-internet
   TLS, API authorization, firewall rules, or certificate renewal.

MQTT is optional: set `EC_ENABLE_MQTT=1` for the first `create` to include port
1883 under the same access policy. The NATS file-storage budget is 2 GiB; the
agent inbox reserves 1 GiB, leaving room for MQTT session and message streams.

For unattended setup, start Docker and provide every choice as flags:

```bash
edgecitadel install --create --network tailscale --plugin codex --scope user --yes
# Server only, loopback-only access:
edgecitadel create --network local --yes
# Protected custom network; replace the IP with one assigned to this host:
edgecitadel create --network custom --host core.example.internal --bind-address 192.168.1.10 --yes
edgecitadel doctor
```

The unified command generates private local credentials, renders configuration,
starts the Core services and agentd, waits for NATS, JetStream, and the API, and
installs the selected Plugin. It is safe to rerun and preserves existing
credentials and data.

Managed activation requires a local Docker Engine/Desktop context, Engine 28+
and Compose 2.24.4+. The generated override replaces the complete publication
lists, including removing MQTT publication when MQTT is disabled. Container
networking must use the qualified default NAT bridge. Administrator-installed
forwarding, routing and raw `docker compose` are outside this policy: raw Compose
does not read the CLI's saved access choice.

Fresh non-interactive setup without a host/network choice uses local access.
Interactive setup has no preselected network. `--yes` suppresses questions but
does not bypass validation. `install --dry-run` resolves choices without applying
them and reports runtime checks as unverified. `create --no-start` saves a
configuration without running Docker; it cannot change an already applied or
legacy deployment's access policy.

Reruns reuse saved access settings. Explicit changes require acknowledgment or
`--yes`, and preserve data and credentials. Existing invitations retain their
encoded addresses; create new invitations for a changed endpoint. Failed apply
keeps a candidate for retry and stops affected exposed services rather than
restoring a broader old port map. Reuse the same state directory, Docker context
and runtime when retrying; another `--state-dir` cannot claim the same Core.
If first startup failed before writing the node record, `down` can still stop
the owned stack using its runtime descriptor. A later `create` resumes the
saved candidate, including its original access choice.
Retries after failed or interrupted applies recreate the Core containers to
restore their declared network configuration while retaining persistent data.

When converting a source checkout, setup discovers the existing Compose project
from its container labels, preserving a name originally supplied with `-p`.
Multiple projects referencing the checkout, a conflicting
`COMPOSE_PROJECT_NAME`, or mismatched ownership/data mounts stop conversion
before apply. Resolve the conflicting deployment identity before retrying.

An older deployment stays visibly `legacy/unknown` on a no-option rerun until
you explicitly select a network mode. Earlier unattended remote `--host`
commands can now require `--bind-address`; localhost/loopback hosts select local
access. Advanced API addresses such as `https://core.example:8443/` are accepted
with `--host`, but they do not create an HTTPS listener or change NATS port 4222
or Leaf port 7422. Paths, credentials, queries, fragments and unusable addresses
are rejected. A hostname resolving to loopback on the Core is unsuitable for
remote advertisement, even if another computer resolves that name differently.

Core readiness reports local API, internal NATS/JetStream and host client/Leaf
ports independently of remote acceptance. A healthy local stack does not prove
that every Edge can reach it. In the tested Linux Engine and macOS Docker
Desktop configurations, an already-running broker retained authenticated
loopback messaging after the Tailscale interface disappeared. Restarting its
containers while that address was absent failed to bind the address, including
the broker's loopback access. Restore Tailscale before starting or restarting
the Core; no wildcard fallback is used.

Before downgrading, stop the managed stack with the current CLI and back up its
complete state and data. Old CLIs do not enforce this access policy; running an
old `create` can widen exposure. A rollback needs explicitly reviewed bindings,
not an assumption that the older CLI understands the managed descriptor.

## Join an Edge

On the Core:

```bash
edgecitadel invite --node-id studio-macmini
```

Copy the returned invitation URI to the Edge. The invitation is expiring,
single-use, and stored as a digest on the Core.
The command reuses the saved advertised endpoints and sends administration
credentials only to the verified local Core API, bypassing proxies and
redirects. Standard output contains one invitation line for shell capture.
Local-only deployments reject remote invitations; select a remote access mode
first. `invite --host` remains an advanced override for remote/legacy deployments.

```bash
# Default: enroll, start services, install the Plugin, and connect agentd
# directly to Core NATS.
edgecitadel install --join 'ecjoin://...' --plugin codex --scope user --yes

# Enroll, start services, and install the Codex Plugin in one command while
# preserving same-host Agent messaging if the Core link goes down.
edgecitadel install --join 'ecjoin://...' --messaging-mode nats_leaf --plugin codex --scope user --yes
```

Both unified commands enroll the Edge, start its services, install the selected
Plugin, and report Plugin and Connector state. `edgecitadel join` remains
available for enrollment without installing Plugins. When replacing an existing
enrollment, it also restarts agentd so the new connection takes effect.

An explicit new invitation replaces the previous enrollment, including a local
Core enrollment. This applies to both `join` and `install --join`, even when the
Plugin is already installed. The CLI validates the redemption response before
stopping the old local service, then saves the previous enrollment, agentd task
history, connector credentials, and local Leaf data in a private directory under
`~/.edgecitadel/enrollment-backups/` (or the selected state directory). The new
connection gets fresh task and connector state; previously revoked native
connectors can register when their host sessions restart. Installed Plugins and
Managed Agent packages are retained, and enabled Managed Agents are restarted.
Existing Core containers and their data are retained; replacing enrollment does
not shut down the old Core deployment.

Restart Codex, Claude Code, or Pi sessions after replacement to activate their
Plugins against the new enrollment. Reusing the exact invitation that created
the current enrollment checks connectivity without redeeming it again; changing
Core, credentials, or messaging mode requires a new invitation. An invalid
redemption leaves the old configuration untouched. If local setup fails after
redemption, the CLI restores the previous local enrollment, but the invitation
may already be consumed and must not be blindly retried. If activation fails
after saving the new enrollment, use the reported service/Managed Agent recovery
commands instead of redeeming again. `install --join --dry-run` only reports the
planned replacement.

`single-client` does not use a local NATS process. `nats_leaf` runs one local
NATS server and connects it outbound to the Core through an authenticated Leaf
Node. If no `nats-server` is already on `PATH`, EdgeCitadel downloads the pinned
NATS release for macOS or Linux on arm64 or amd64, verifies its SHA-256 and
version, and installs it under `~/.edgecitadel/runtime/nats-server`. Set
`EDGECITADEL_NATS_SERVER` to use another executable. A Leaf Node still needs a
real NATS server process; the simplified flow now provisions that process
without a separate package-manager command.

The selected mode is durable. Repeating `join` with the same mode is safe;
requesting a different mode is rejected rather than silently changing message
ownership.

Before redeeming an invitation, `join` checks TCP reachability to the Core API
and the selected transport: port 4222 for direct clients or 7422 for Leaf Nodes.
Remote monitoring access on 8222 is unnecessary. A failed preflight does not
consume the invitation. Successful enrollment saves credentials; the subsequent
TCP check does not prove authenticated agent messaging or a remote reply. Use
`doctor` to inspect transport health and exchange a task with a remote Agent to
verify the complete path.

## Agent Packages

An Agent Package contains a complete long-running runtime operated by
EdgeCitadel and declares one or more Agent identities. Gemma owns its
model-backed Agent harness. The Home Assistant Agent Package contains an
adapter process, while the user's Home Assistant installation and data remain
external.

```bash
edgecitadel agent install gemma
edgecitadel agent list
edgecitadel agent status edgecitadel.gemma
edgecitadel agent logs edgecitadel.gemma
edgecitadel agent stop edgecitadel.gemma
edgecitadel agent start edgecitadel.gemma
edgecitadel agent remove edgecitadel.gemma
```

Installation validates the package and lock before execution, shows requested
permissions, creates a private dependency runtime, records immutable package
state, and waits for a fresh Agent registration. Agents call the local
agentd socket and do not receive NATS or Leaf credentials.

Legacy package records remain inspectable and stoppable during local-state
migration; the `agent` command remains their lifecycle surface.

## Plugins and Connectors

Pi, Claude Code, and Codex keep ownership of their model, tools, permissions,
session, and execution loop. Their EdgeCitadel plugins add host-native skills
and MCP tools backed by agentd:

```bash
edgecitadel plugin install pi --scope user
edgecitadel plugin install claude-code --scope project
edgecitadel plugin install codex --scope user
edgecitadel plugin list
```

These commands delegate installation to each native host package manager.
Re-running an install is a no-op when source, scope, and version match; use
`edgecitadel install` to reconcile selected stale sources, or
`edgecitadel plugin repair <host>` to do so explicitly when a distribution
upgrade moves the packaged source. Codex supports user scope; Claude Code and Pi
support user and project scope.

A Connector is available only while its host session is active. Closing the
session closes its renewable lease and agentd publishes an unavailable state.
An inbox entry is not automatic consent to execute: the host plugin records an
explicit acceptance, running state, and one terminal result.

Agent discovery combines local connectors with the most recent validated NATS
presence observed by agentd. During a transport outage, remote entries are a
cached observation rather than proof that the remote Agent is currently online.

```bash
edgecitadel connector list
edgecitadel connector status codex-local
edgecitadel connector revoke codex-local
```

## Local state and diagnostics

```bash
edgecitadel status
edgecitadel doctor
edgecitadel service status
edgecitadel task list --connector-id codex-local
edgecitadel task show <task-id> --connector-id codex-local
edgecitadel task cancel <task-id> --connector-id codex-local
edgecitadel trace list --connector-id codex-local
edgecitadel trace show <trace-id> --connector-id codex-local
edgecitadel trace purge --connector-id codex-local
```

agentd is the only writer of its private SQLite database. It records task
orchestration, attempts, bounded diagnostic events, metadata-only traces, and
presence history. Native plugins use the scoped local API rather than opening
the database or connecting to NATS. Private file modes isolate other OS users,
not arbitrary code running as the same user; install Plugins only into trusted
Agent hosts. The Agent Package effect/outcome ledger remains separate
so external side effects keep their idempotency boundary.

Trace metadata is retained for at most 30 days, with record caps enforced in
bounded cleanup batches. `edgecitadel service status --json` reports database
bytes and telemetry counts; `edgecitadel trace purge` removes trace metadata
without deleting identity or pending tasks.

`doctor` distinguishes the service process, database, task transport, Core API,
Core NATS, local NATS, JetStream, and Leaf connection. A healthy local broker
with a disconnected Leaf is degraded: same-host work is available and
cross-node work is paused.

For a managed Core, `doctor` verifies runtime ownership, the applied generation,
and network bindings before probing the literal local API and NATS endpoints.
A server-only Core does not need a running agentd. Component health still does
not prove a remote Agent can complete a request and reply.

Back up and restore the complete `~/.edgecitadel/agentd` directory as one unit.
Its SQLite database, `payload.key`, and `admin.token` are private, related state;
restoring only the database makes encrypted task content unrecoverable. Keep the
directory and files restricted to the account that runs EdgeCitadel.

Service setup builds from a private runtime and schema copy inside its
`supervisor` environment. Bundled installation assets can remain read-only;
the generated copy is rebuilt when the service runtime changes.

## Troubleshooting

- Docker unavailable during `create`: start Docker Desktop or Docker Engine and
  rerun the same command.
- An invitation's local expiry warning is advisory; the Core decides whether it
  can be redeemed. A Core rejection may mean invalid, expired, or already used:
  create a new invitation.
- Enrollment response lost or unreadable: consumption is uncertain. Request a
  new invitation; do not blindly retry the one submitted.
- Enrollment saved but transport unavailable: preserve the saved state, restore
  connectivity, then run `edgecitadel doctor` and `edgecitadel service start`.
  Repeating `join` checks the saved transport without redeeming again.
- `nats_leaf` download fails before enrollment: restore internet access, install
  `nats-server` on `PATH`, or set `EDGECITADEL_NATS_SERVER`, then rerun with the
  same invitation. If setup fails after redemption, create a new invitation; a
  redeemed invitation is never silently reused after partial setup.
- `doctor` reports the EdgeCitadel service stopped: run `edgecitadel service
  start`.
- `doctor` reports disconnected task transport: restore Core connectivity; in
  `nats_leaf`, confirm local messaging first with `edgecitadel messaging status`.
- Agent Package does not register its Agent: inspect `edgecitadel agent logs <package-id>`
  and then run `edgecitadel doctor`.
- Plugin tools are absent: start a new host session and check `edgecitadel
  connector status <connector-id>`.
- On Linux without a systemd user manager, `agentd` uses a current-login
  background process; run `edgecitadel service start` after a host reboot.
- Add global `--verbose` before a command only when technical detail is needed.

## Upgrade, rollback, and uninstall

```bash
uv tool upgrade edgecitadel
# or
brew upgrade edgecitadel
```

Package upgrades preserve `~/.edgecitadel`, including node identity, connector
credentials, SQLite state, Agent Package data, logs, and local JetStream data.
Legacy `plugins.json` is retained as a rollback record after its atomic migration
to `managed-agents.json`. Stale Watchdog records are ignored because task and
presence reconciliation are now system-owned.

Installing a newer Agent Package is transactional. If its fresh process
does not become ready, EdgeCitadel restores the prior install record and restarts
the prior version when it was previously running. After a successful upgrade,
the prior immutable package remains available for an explicit rollback:

```bash
edgecitadel agent install /path/to/the/prior/package
```

Back up `~/.edgecitadel` before manual rollback. A code downgrade must not be
used with a newer SQLite schema unless that older release documents support for
the schema.

Before uninstalling an Edge:

```bash
edgecitadel service stop
edgecitadel messaging stop  # nats_leaf only
```

Then run `uv tool uninstall edgecitadel` or `brew uninstall edgecitadel`. Both
preserve `~/.edgecitadel`; deleting that state is a separate, explicit operator
decision after backup. Removing the Home Assistant adapter never removes or
changes Home Assistant itself.
