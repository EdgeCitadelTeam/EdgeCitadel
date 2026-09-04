# EdgeCitadel

EdgeCitadel connects AI Agents across a Core and enrolled Edge hosts. It runs
complete Agents from packages such as Gemma and Home Assistant adapters, and connects
active Pi, Claude Code, and Codex sessions through their native plugin systems.

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) if it is
not already available, then install EdgeCitadel as an isolated CLI tool:

```bash
uv tool install edgecitadel
```

On macOS, Homebrew is also supported:

```bash
brew install edgecitadel
```

`uv tool install` avoids modifying an operating-system-managed Python
environment. See the [Python distribution guide](deploy/pip/README.md) for a
manual virtual-environment fallback.

Then run the guided, idempotent installer from the project where host-local
Plugins should be configured:

```bash
edgecitadel install
```

For automation, make every choice explicit, for example
`edgecitadel install --create --plugin codex --scope user --yes`.

## Create a Core

Start Docker, then choose a hostname or IP that Edge hosts can reach:

```bash
edgecitadel create --host core.example.internal
edgecitadel doctor
```

The command checks local requirements and prints the dashboard URL.

## Join an Edge

Create a one-time invitation on the Core:

```bash
edgecitadel invite --node-id studio-macmini --host core.example.internal
```

Run the returned `edgecitadel join` command on the Edge. The default
`single-client` mode connects the EdgeCitadel service directly to Core NATS:

```bash
edgecitadel join 'ecjoin://...'
```

Use `nats_leaf` when Agents on this host must keep communicating while the Core
connection is unavailable:

```bash
brew install nats-server  # macOS; use your system package manager elsewhere
edgecitadel join 'ecjoin://...' --messaging-mode nats_leaf
```

In both modes, Agent integrations talk to the host-local EdgeCitadel service.
Only `nats_leaf` needs a local NATS server; it is the durable local message bus
and maintains the outbound Leaf connection to Core.

## Install an Agent Package

Agent Packages contain complete runtimes operated by EdgeCitadel. An installed
runtime may declare one or more Agent identities:

```bash
edgecitadel agent install gemma
edgecitadel agent list
edgecitadel agent status edgecitadel.gemma
```

Home Assistant is installed the same way after its URL, token, and allowlist are
configured. EdgeCitadel operates the adapter and never installs or removes Home
Assistant itself. See the [Gemma guide](agent-packages/gemma/README.md) and
[Home Assistant guide](agent-packages/homeassistant/README.md).

## Connect an existing Agent

Plugins add Agent discovery, delegation, inbox, task-state, trace, and
diagnostic tools to an active host session. They do not start the host in the
background, and EdgeCitadel does not pass NATS credentials through the plugin
protocol.

```bash
edgecitadel plugin install codex
edgecitadel plugin install claude-code --scope project
edgecitadel plugin install pi --scope user
edgecitadel plugin list
```

Each command delegates to the host's native package manager and reports package
installation separately from activation. Start a new host session, then use its
`edgecitadel_*` tools. Inspect active sessions with `edgecitadel connector list`
and `edgecitadel connector status <connector-id>`; use
`edgecitadel plugin repair <host>` after a distribution path changes.

## Operate

```bash
edgecitadel status
edgecitadel doctor
edgecitadel service status
edgecitadel task list --connector-id <connector-id>
edgecitadel trace list --connector-id <connector-id>
```

Stop a Core without deleting its data with `edgecitadel down`.

## Upgrade or uninstall

Use the package manager that installed EdgeCitadel:

```bash
uv tool upgrade edgecitadel
uv tool uninstall edgecitadel

brew upgrade edgecitadel
brew uninstall edgecitadel
```

Before uninstalling an Edge, run `edgecitadel service stop`; in `nats_leaf`
mode also run `edgecitadel messaging stop`. Uninstall preserves local state in
`~/.edgecitadel`.

More detail: [onboarding and troubleshooting](docs/onboarding.md) and
[contributing](CONTRIBUTING.md).

## License

MIT
