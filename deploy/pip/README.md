# pip distribution

The Python distribution provides the same `edgecitadel` command and both
messaging modes as the Homebrew Formula. It installs immutable Core Compose
sources, schemas, Supervisor sources, and bundled plugins under the Python
environment's `share/edgecitadel` directory while keeping mutable state under
`~/.edgecitadel`.

There is no public PyPI release yet.

## Test before publication

Test the checked-out source in an isolated environment:

```bash
python3 -m venv /tmp/edgecitadel-pip-test
/tmp/edgecitadel-pip-test/bin/python -m pip install .
/tmp/edgecitadel-pip-test/bin/edgecitadel --version
```

## First public release

The release workflow uses PyPI Trusted Publishing, so it does not require a
long-lived API token. A PyPI project owner must complete the one-time setup:

1. Confirm that the `edgecitadel` project name is available on PyPI and that
   the project metadata, license, and README are ready to be public.
2. Create a pending Trusted Publisher on PyPI for repository
   `zhonghaozhan/EdgeCitadel`, workflow `publish-pypi.yml`, and environment
   `pypi`.
3. Create the `pypi` GitHub environment. Add required reviewers if publication
   should need a second approval.
4. Update the version in `pyproject.toml`, `edgecitadel/__init__.py`, and
   `scripts/edgecitadel_cli.py`. The version embedded in the wheel data paths in
   `pyproject.toml` must match it too.
5. Run the package tests below, merge the release commit, and create a GitHub
   release whose tag is exactly `v<version>` (for example, `v0.1.0`).

Publishing the GitHub release starts `.github/workflows/publish-pypi.yml`. The
workflow rejects a tag that does not match the package version, builds the
source distribution and wheel, checks their metadata, uploads them as a GitHub
Actions artifact, and then publishes them through the protected `pypi`
environment. PyPI versions are immutable; a failed or incorrect upload must be
fixed with a new version rather than overwriting the release.

After an explicitly authorized PyPI release, the intended user command is:

```bash
python3 -m venv ~/.edgecitadel/cli-venv
~/.edgecitadel/cli-venv/bin/python -m pip install edgecitadel
```

Use a virtual environment or `pipx`; do not modify the system Python. Upgrades
replace package assets but preserve `~/.edgecitadel`. Before uninstalling a
`nats_leaf` Edge, stop managed processes:

```bash
~/.edgecitadel/cli-venv/bin/edgecitadel supervisor stop
~/.edgecitadel/cli-venv/bin/edgecitadel messaging stop
~/.edgecitadel/cli-venv/bin/python -m pip uninstall edgecitadel
```

For an installation managed by `pipx`, use the command exposed by `pipx` to
stop the same processes, then let `pipx` remove its environment:

```bash
edgecitadel supervisor stop
edgecitadel messaging stop
pipx uninstall edgecitadel
```

The `nats-server` executable is intentionally not a Python dependency. It is a
native service required only for `nats_leaf`; install it through the operating
system package manager or an official NATS release and confirm that
`nats-server` is on `PATH` before joining. Homebrew installs this dependency
automatically, while `single-client` does not need it.

## Build verification

Run the repository's package gate:

```bash
uv run --isolated --with-requirements tests/requirements.txt \
  python -m pytest -q tests/test_pip_distribution.py
python -m build
python -m twine check dist/*
```

Then inspect the wheel and install it into a second clean environment. A valid
wheel must contain at least:

- the wheel data root's `share/edgecitadel/docker-compose.yml`;
- `share/edgecitadel/scripts/edgecitadel_cli.py` and `plugin_runner.py`;
- `share/edgecitadel/plugin-toolkit/pyproject.toml` and `src/`;
- `share/edgecitadel/plugins/` and `schemas/`;
- the Core `aggregator`, `frontend`, `nats`, and `nginx` assets.

It must not contain `.env`, runtime data, virtual environments, `node_modules`,
test caches, bytecode, or local credentials.
