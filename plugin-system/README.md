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
python -m pip install -e '.[test,type]'
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
change requires regenerating the lock. `validate` requires the lock's exact
canonical bytes: two-space indentation, recursively sorted object keys, and one
final newline. After semantic integrity checks, it also requires those bytes to
equal the current lock generator output exactly.

## Maintained contributor gate

Run the focused checks from this directory:

```bash
python -m pytest -q
python -m compileall -q src tests
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
mypy --strict src/edgecitadel_plugin_sdk tests/typecheck_sdk_consumer.py
```

The combined extras install both pytest and the constrained mypy version used by
the typing gate.

## Static guarantees and trust boundary

The scaffold rejects duplicate YAML and JSON mapping keys. Untrusted structured
documents are limited to 1 MiB, `SKILL.md` to 2 MiB, and its frontmatter to
64 KiB; parsed trees are limited to depth 64 and 100,000 traversed values. YAML
anchors or aliases that reuse a container are rejected. Validation applies
strict schemas, accepts only local-fragment (`#...`) `$ref` and `$dynamicRef`
values in skill input/output schemas, checks compatibility and agent-to-skill
mappings, and resolves declared paths within the package. Portable paths exclude
absolute paths, empty or dot components, traversal, backslashes, and every
Unicode `Cc` control character; ordinary Unicode names remain allowed.
Validation also rejects symbolic links and special filesystem nodes and uses
canonical SHA-256 hashes and ordering.

Recognized optional Agent Skills frontmatter fields are `license` (string),
`compatibility` (string, at most 500 characters), `metadata` (string-to-string
mapping), and experimental `allowed-tools` (space-separated string). Unknown
frontmatter fields remain accepted for forward compatibility. If
`metadata.version` is present, it must equal `binding.yaml` `version`; otherwise
the binding version is authoritative.

Diagnostics may include identifiers and escaped package-relative paths; an
invalid or missing root argument may report the escaped caller-supplied or
resolved root path. They do not dump procedure bodies, secret values, or complete
file contents. Validation never imports package handlers or launches the
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
