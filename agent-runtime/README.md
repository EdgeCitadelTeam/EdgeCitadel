# EdgeCitadel Agent Runtime

`agent-runtime/` contains agentd, the Managed Agent runtime, and repository-side
infrastructure for validating installable EdgeCitadel packages. The
`edgecitadel_supervisor` package owns safe
loading, strict schemas, compatibility checks, canonical locks, and deterministic
inventory. The `edgecitadel_plugin_runtime` package owns Agent Card, heartbeat,
durable inbox, result, and JetStream primitives. The separate
`edgecitadel_plugin_sdk` package defines typed,
framework-neutral extension seams and immutable values for future runtimes.
Installable Agent Packages live in [`../agent-packages/`](../agent-packages/), not in this
directory.

## End-user lifecycle

Newcomers do not create this environment or install the Supervisor separately.
From the repository root, the unified CLI prepares a private environment on the
first Agent command and composes validation with the host-local lifecycle:

```bash
./scripts/edgecitadel agent install ./agent-packages/examples/echo
./scripts/edgecitadel agent list
./scripts/edgecitadel agent logs edgecitadel.echo
./scripts/edgecitadel agent stop edgecitadel.echo
./scripts/edgecitadel agent start edgecitadel.echo
```

Before a managed runtime starts, agentd owns its connector, durable inbox, task
state, and process lifecycle. Managed processes receive a private local API
credential, never NATS or Leaf credentials. The lower-level commands below are
the contributor interface for package authoring and CI.

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
edgecitadel-supervisor lock ../agent-packages/examples/echo
edgecitadel-supervisor validate ../agent-packages/examples/echo

python -m edgecitadel_supervisor lock ../agent-packages/examples/echo
python -m edgecitadel_supervisor validate ../agent-packages/examples/echo
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
python -m edgecitadel_supervisor validate ../agent-packages/examples/echo
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

agentd owns Managed Agent process lifecycle, broker connectivity, local identity,
and task/trace persistence. The toolkit does not provide a learned-memory store,
sandbox enforcement, permission granting, package signing, or publisher
verification. It also does not support normal wheel deployment of the
validator's schema resources; schema lookup is supported only from the
source/editable layout for now.
