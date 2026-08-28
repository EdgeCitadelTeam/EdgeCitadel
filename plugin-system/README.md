# EdgeCitadel plugin system

`plugin-system/` contains host-side infrastructure for validating installable
EdgeCitadel plugin packages. The `edgecitadel_supervisor` package owns safe
loading, strict schemas, compatibility checks, canonical locks, and deterministic
inventory. The separate `edgecitadel_plugin_sdk` package defines typed,
framework-neutral extension seams and immutable values for future runtimes.
Installable plugin packages live in [`../plugins/`](../plugins/), not in this
directory.

## Contributor setup

Create the environment from this directory. The editable install is required by
the current source-layout schema lookup model.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

## Package commands

`lock` validates package structure and writes or regenerates
`plugin.lock.json`; it mutates the package. `validate` verifies the existing lock
without writing anything and prints a deterministic JSON inventory. Neither
command imports handlers or executes plugin runtime code.

Both console-script and module forms are supported:

```bash
edgecitadel-supervisor lock ../plugins/examples/placeholder
edgecitadel-supervisor validate ../plugins/examples/placeholder

python -m edgecitadel_supervisor lock ../plugins/examples/placeholder
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
```

Finalize every package file before running `lock`; any subsequent package byte
change requires regenerating the lock.

## Maintained contributor gate

Run the focused checks from this directory:

```bash
python -m pytest -q
python -m compileall -q src tests
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
mypy --strict src/edgecitadel_plugin_sdk tests/typecheck_sdk_consumer.py
```

The test extra installs pytest but not mypy. Contributors running the typing gate
must make `mypy` available in their environment.

## Static guarantees and trust boundary

The scaffold safely parses YAML and JSON, applies strict schemas, checks package
compatibility and agent-to-skill mappings, and resolves declared paths within
the package. Validation rejects symbolic links and special filesystem nodes,
uses canonical SHA-256 hashes and ordering, and emits package-relative,
content-redacted diagnostics. It never imports package handlers or launches the
declared runtime.

These guarantees assume the package root is owned by the supervisor and remains
immutable throughout `lock` or `validate`. They do not make validation safe
against concurrent mutation of an externally writable tree.

## SDK boundary

SDK fields that carry flexible data use JSON-shaped `Mapping[str, object]`
values. Mapping/list/tuple trees are deeply snapshotted so caller mutation cannot
change an SDK value. `TransportMessage.to_mapping()` returns an independent,
canonical envelope-shaped wire mapping; it intentionally does not validate the
envelope. Schema validation belongs to the supervisor or future host boundary.

The SDK ships a PEP 561 `py.typed` marker. Its `runtime_checkable` Protocols only
support presence checks at runtime; static type checking owns method signatures
and return types.

## Non-goals

This milestone does not provide runtime launch or process lifecycle management,
NATS or another transport implementation, identity provisioning, persistence or
a learned-memory store, sandbox enforcement, permission granting, package
signing, or publisher verification. It also does not support normal wheel
deployment of the supervisor's schema resources; schema lookup is supported only
from the source/editable layout for now.
