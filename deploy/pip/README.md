# Python distribution

The Python distribution provides the same `edgecitadel` command and both
messaging modes as the Homebrew Formula. It installs immutable Core Compose
sources, schemas, agentd sources, Agent Packages, and host Plugins under the Python
environment's `share/edgecitadel` directory while keeping mutable state under
`~/.edgecitadel`.

## Recommended install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) if it is
not already available, then install the public release from PyPI as an isolated
CLI tool:

```bash
uv tool install edgecitadel
edgecitadel --version
```

Use `uv tool upgrade edgecitadel` for upgrades. Tool mode owns its virtual
environment and avoids modifying an operating-system-managed Python
installation.

After obtaining an invitation, a non-interactive NATS-leaf Edge setup is:

```bash
edgecitadel install --join 'ecjoin://...' --messaging-mode nats_leaf --plugin codex --scope user --yes
```

This command performs enrollment, starts the local NATS and EdgeCitadel
services, and installs the selected native-host Plugin.

To test the checked-out source in an isolated environment:

```bash
python3 -m venv /tmp/edgecitadel-pip-test
/tmp/edgecitadel-pip-test/bin/python -m pip install .
/tmp/edgecitadel-pip-test/bin/edgecitadel --version
```

As a manual fallback, install the public release in a dedicated virtual
environment:

```bash
python3 -m venv ~/.edgecitadel/cli-venv
~/.edgecitadel/cli-venv/bin/python -m pip install edgecitadel
```

Do not install into the system Python. Upgrades replace package assets but
preserve `~/.edgecitadel`. Before uninstalling a `nats_leaf` Edge, stop managed
processes. For an installation managed by uv:

```bash
edgecitadel service stop
edgecitadel messaging stop
uv tool uninstall edgecitadel
```

For a manual virtual-environment installation:

```bash
~/.edgecitadel/cli-venv/bin/edgecitadel service stop
~/.edgecitadel/cli-venv/bin/edgecitadel messaging stop
~/.edgecitadel/cli-venv/bin/python -m pip uninstall edgecitadel
```

The `nats-server` executable is intentionally not a Python dependency. It is a
native service required only for `nats_leaf`; install it through the operating
system package manager or an official NATS release and confirm that
`nats-server` is on `PATH` before joining. `single-client` does not need it.

## Build verification

Build in an isolated environment, inspect the wheel, then install it into a
second clean environment. A valid wheel must contain at least:

- the wheel data root's `share/edgecitadel/docker-compose.yml`;
- `share/edgecitadel/scripts/edgecitadel_cli.py`;
- `share/edgecitadel/agent-runtime/pyproject.toml` and `src/`;
- `share/edgecitadel/agent-packages/`, `plugins/`, and `schemas/`;
- the Core `aggregator`, `frontend`, `nats`, and `nginx` assets.

It must not contain `.env`, runtime data, virtual environments, `node_modules`,
test caches, bytecode, or local credentials.
