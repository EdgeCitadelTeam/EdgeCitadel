# pip distribution

The Python distribution provides the same `edgecitadel` command and both
messaging modes as the Homebrew Formula. It installs immutable Core Compose
sources, schemas, agentd sources, Managed Agents, and Native Agent Plugins under the Python
environment's `share/edgecitadel` directory while keeping mutable state under
`~/.edgecitadel`.

To test the checked-out source in an isolated environment:

```bash
python3 -m venv /tmp/edgecitadel-pip-test
/tmp/edgecitadel-pip-test/bin/python -m pip install .
/tmp/edgecitadel-pip-test/bin/edgecitadel --version
```

Install the public release from PyPI in an isolated environment:

```bash
python3 -m venv ~/.edgecitadel/cli-venv
~/.edgecitadel/cli-venv/bin/python -m pip install edgecitadel
```

Use a virtual environment or `pipx`; do not modify the system Python. Upgrades
replace package assets but preserve `~/.edgecitadel`. Before uninstalling a
`nats_leaf` Edge, stop managed processes:

```bash
~/.edgecitadel/cli-venv/bin/edgecitadel service stop
~/.edgecitadel/cli-venv/bin/edgecitadel messaging stop
~/.edgecitadel/cli-venv/bin/python -m pip uninstall edgecitadel
```

For an installation managed by `pipx`, use the command exposed by `pipx` to
stop the same processes, then let `pipx` remove its environment:

```bash
edgecitadel service stop
edgecitadel messaging stop
pipx uninstall edgecitadel
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
- `share/edgecitadel/plugin-toolkit/pyproject.toml` and `src/`;
- `share/edgecitadel/plugins/`, `native-plugins/`, and `schemas/`;
- the Core `aggregator`, `frontend`, `nats`, and `nginx` assets.

It must not contain `.env`, runtime data, virtual environments, `node_modules`,
test caches, bytecode, or local credentials.
