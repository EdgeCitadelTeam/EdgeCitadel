# EdgeCitadel onboarding and troubleshooting

The `edgecitadel` command installs on both Core and Edge hosts. A Core provides
enrollment, shared NATS/JetStream, the API, and dashboard. An Edge runs the
host-local EdgeCitadel service and any Managed Agents or Native Agent Plugins.

## Install

```bash
pip install edgecitadel
# or
brew install edgecitadel

edgecitadel --version
```

Use the same package manager for upgrades and uninstall.

## Create a Core

Start Docker, then provide a hostname or address that Edge hosts can reach:

```bash
edgecitadel create --host core.example.internal
edgecitadel doctor
```

`create` generates private local credentials, renders configuration, starts the
Core services, and waits for NATS, JetStream, and the API. It is safe to rerun
and preserves existing credentials and data.

## Join an Edge

On the Core:

```bash
edgecitadel invite --node-id studio-macmini --host core.example.internal
```

Run the returned command on the Edge. The invitation is expiring, single-use,
and stored as a digest on the Core.

```bash
# Default: the EdgeCitadel service connects directly to Core NATS.
edgecitadel join 'ecjoin://...'

# Same-host Agent messaging remains available if the Core link is down.
edgecitadel join 'ecjoin://...' --messaging-mode nats_leaf
```

`single-client` does not use a local NATS process. `nats_leaf` runs one local
NATS server and connects it outbound to the Core through an authenticated Leaf
Node. Users who select `nats_leaf` install `nats-server` with their operating-
system package manager (`brew install nats-server` on macOS). It is needed
because a Leaf Node is a NATS server topology, not a client feature.

The selected mode is durable. Repeating `join` with the same mode is safe;
requesting a different mode is rejected rather than silently changing message
ownership.

## Managed Agents

A Managed Agent is a complete long-running runtime operated by EdgeCitadel.
Gemma owns its model-backed Agent harness. The Home Assistant package is a
Managed Adapter: EdgeCitadel owns the adapter process, while the user's Home
Assistant installation and data remain external.

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
state, and waits for a fresh Agent registration. Managed Agents call the local
agentd socket and do not receive NATS or Leaf credentials.

The old `edgecitadel plugin` spelling remains a deprecation alias for migrated
installations. New documentation and automation should use `edgecitadel agent`.

## Native Agent Plugins

Pi, Claude Code, and Codex keep ownership of their model, tools, permissions,
session, and execution loop. Their EdgeCitadel plugins add host-native skills
and MCP tools backed by agentd:

```bash
pi install "$(edgecitadel connector path pi)"

claude plugin marketplace add "$(edgecitadel connector path claude-code)"
claude plugin install edgecitadel@edgecitadel

codex plugin marketplace add "$(edgecitadel connector path codex)"
codex plugin add edgecitadel@edgecitadel
```

The connector is available only while its host session is active. Closing the
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
not arbitrary code running as the same user; install native plugins only into
trusted Agent hosts. The Managed Agent effect/outcome ledger remains separate
so external side effects keep their idempotency boundary.

Trace metadata is retained for at most 30 days, with record caps enforced in
bounded cleanup batches. `edgecitadel service status --json` reports database
bytes and telemetry counts; `edgecitadel trace purge` removes trace metadata
without deleting identity or pending tasks.

`doctor` distinguishes the service process, database, task transport, Core API,
Core NATS, local NATS, JetStream, and Leaf connection. A healthy local broker
with a disconnected Leaf is degraded: same-host work is available and
cross-node work is paused.

Back up and restore the complete `~/.edgecitadel/agentd` directory as one unit.
Its SQLite database, `payload.key`, and `admin.token` are private, related state;
restoring only the database makes encrypted task content unrecoverable. Keep the
directory and files restricted to the account that runs EdgeCitadel.

## Troubleshooting

- Docker unavailable during `create`: start Docker Desktop or Docker Engine and
  rerun the same command.
- Invitation expired or already used: create a new invitation on the Core.
- `nats_leaf` setup fails: install `nats-server`, then use a new invitation; a
  redeemed invitation is never silently reused after partial setup.
- `doctor` reports the EdgeCitadel service stopped: run `edgecitadel service
  start`.
- `doctor` reports disconnected task transport: restore Core connectivity; in
  `nats_leaf`, confirm local messaging first with `edgecitadel messaging status`.
- Managed Agent does not register: inspect `edgecitadel agent logs <package-id>`
  and then run `edgecitadel doctor`.
- Native tools are absent: start a new host session and check `edgecitadel
  connector status <connector-id>`.
- On Linux without a systemd user manager, `agentd` uses a current-login
  background process; run `edgecitadel service start` after a host reboot.
- Add global `--verbose` before a command only when technical detail is needed.

## Upgrade, rollback, and uninstall

```bash
pip install --upgrade edgecitadel
# or
brew upgrade edgecitadel
```

Package upgrades preserve `~/.edgecitadel`, including node identity, connector
credentials, SQLite state, Managed Agent data, logs, and local JetStream data.
Legacy `plugins.json` is retained as a rollback record after its atomic migration
to `managed-agents.json`. Stale Watchdog records are ignored because task and
presence reconciliation are now system-owned.

Installing a newer Managed Agent package is transactional. If its fresh process
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

Then run `pip uninstall edgecitadel` or `brew uninstall edgecitadel`. Both
preserve `~/.edgecitadel`; deleting that state is a separate, explicit operator
decision after backup. Removing the Home Assistant adapter never removes or
changes Home Assistant itself.
