# EdgeCitadel Plugin Infrastructure Design

**Status:** Approved for implementation on 2026-08-26

**Source:** `/Users/yefanzhang/Downloads/deep-research-report.md`

## Goal

Create the first EdgeCitadel plugin infrastructure scaffold: a framework-neutral,
out-of-process package contract plus a runnable no-op supervisor that discovers
and validates plugins without executing them.

This milestone establishes durable extension boundaries. It does not migrate the
existing adapters or implement process management, NATS connectivity, identity,
authorization, sandboxing, or knowledge persistence.

## Design principles

1. A plugin is an installable package and process boundary, not an in-process
   Python import convention.
2. The package contract is language-neutral even though the reference SDK and
   supervisor are initially Python.
3. Existing `envelope.v1` and A2A Agent Card semantics remain unchanged.
4. Public skill discovery, packaged procedures, and learned procedural memory
   are separate concepts with separate trust and persistence rules.
5. Core schemas are strict. Future additions use explicitly namespaced extension
   maps instead of accepting arbitrary top-level fields.
6. A manifest requests capabilities; it never grants them.

## Repository shape

```text
plugins/
  README.md
  pyproject.toml
  schemas/
    agent-plugin.v1alpha1.schema.json
    skill.v1alpha1.schema.json
  edgecitadel_plugin_sdk/
    __init__.py
    lifecycle.py
    runtime.py
    skills.py
    knowledge.py
    transport.py
  edgecitadel_supervisor/
    __init__.py
    __main__.py
    cli.py
    loader.py
    validator.py
  examples/
    placeholder/
      plugin.yaml
      runtime/
        __init__.py
        __main__.py
      skills/
        placeholder/
          skill.yaml
          procedure.md
          input.schema.json
          output.schema.json
      assets/
        .gitkeep
  tests/
    test_loader.py
    test_validator.py
    test_cli.py
```

The top-level `plugins/` directory is self-contained so the initial scaffold does
not modify aggregator, adapter, NATS, or deployment behavior.

## Package manifest

Each plugin root contains `plugin.yaml`, validated against
`agent-plugin.v1alpha1.schema.json`. The core shape is:

```yaml
apiVersion: edgecitadel.io/v1alpha1
kind: AgentPlugin

metadata:
  id: placeholder-agent
  version: 0.1.0
  publisher: local

runtime:
  command: ["python", "-m", "runtime"]
  healthTimeoutSeconds: 10
  restartPolicy: on-failure

agent:
  skillsDirectory: skills
  listensBroadcast: false

permissions:
  knowledge: []
  messaging:
    outboundAgents: []
  network:
    outbound: []
  devices: []

security:
  sandbox: restricted
  secrets: []

extensions: {}
```

The schema requires a normalized plugin ID, semantic version, non-empty runtime
command, relative skills directory, complete permission categories, and a known
sandbox/restart policy. Paths may not be absolute or escape the plugin root.

`extensions` is an object whose keys must be reverse-domain or URI-like
namespaces. Core loaders preserve extension values without interpreting them.
Unsupported core fields are rejected.

## Skills and procedural memory

### Public skill discovery

Every immediate child directory under `agent.skillsDirectory` represents one
skill and contains `skill.yaml`. The descriptor includes:

```yaml
apiVersion: edgecitadel.io/v1alpha1
kind: AgentSkill

metadata:
  id: example.placeholder
  version: 0.1.0
  name: placeholder
  description: Demonstrates the plugin package contract.
  tags: [example]

procedure:
  path: procedure.md
  format: markdown

handler:
  name: placeholder

schemas:
  input: input.schema.json
  output: output.schema.json

capabilities:
  knowledge: []
  network: []
  devices: []

extensions: {}
```

The supervisor converts the public subset (`id`, `name`, `description`, `tags`)
into the future Agent Card skill catalog. Execution-specific fields remain
private to the package and are not exposed automatically.

`handler.name` is an opaque identifier interpreted only by the plugin runtime.
It is deliberately not a Python import path, allowing other languages and agent
frameworks to use the same package contract.

### Packaged procedures

`procedure.md` is immutable package content containing a prompt, checklist,
workflow, or operating procedure. It is versioned with both the plugin and skill.
Input and output schemas make the procedure boundary inspectable without
requiring a particular agent framework.

The scaffold validates that each referenced procedure and schema exists and is
inside its skill directory. It does not interpret procedure content or import the
handler.

### Learned procedural memory

Runtime-learned procedures must not rewrite the installed package. The SDK
defines a placeholder `KnowledgeStore` protocol for future persistence using a
record keyed by plugin identity, skill ID, skill version, namespace, revision,
content hash, and provenance.

The plugin manifest requests the knowledge namespaces it may use. A later policy
layer resolves those requests into grants. This keeps reviewed package procedures
separate from mutable learned memory and allows the latter to be audited,
rejected, rolled back, or shared without changing executable code.

## SDK extension boundaries

The Python SDK defines protocols and value types only:

- `AgentRuntime`: initialization, message handling, drain, and shutdown surface.
- `SkillProvider`: enumerate and resolve packaged skills.
- `KnowledgeStore`: read and propose versioned knowledge records.
- `Transport`: register, receive, publish, and drain transport messages.
- `LifecycleHooks`: optional hooks around supervisor lifecycle states.

The scaffold contains no concrete transport, knowledge, identity, or sandbox
implementation. Methods whose use would cross those boundaries raise a clear
`NotImplementedError` describing the later milestone.

The interfaces avoid NATS-specific and framework-specific parameter types so a
future non-Python plugin can implement the same process protocol.

## No-op supervisor

The runnable command is:

```bash
python -m edgecitadel_supervisor validate plugins/examples/placeholder
```

It performs these steps:

1. Resolve and verify the plugin root.
2. Load `plugin.yaml` with safe YAML parsing.
3. Validate it against the plugin schema.
4. Resolve `skillsDirectory` without following paths outside the package.
5. Discover skill directories in deterministic name order.
6. Validate each `skill.yaml` against the skill schema.
7. Reject duplicate skill IDs and unsafe/missing referenced files.
8. Emit a deterministic JSON inventory containing plugin identity, runtime
   metadata, requested permissions, and public skill metadata.

The command exits zero only for a fully valid package. It does not run the
runtime command, import handlers, inspect procedure contents, connect to NATS,
or grant requested permissions.

Future supervisor subcommands (`install`, `start`, `stop`, `status`, `upgrade`)
are documented as reserved lifecycle operations but are not registered as usable
commands in this milestone.

## Error model

The loader and validator expose stable domain errors rather than raw library
exceptions:

- `PluginNotFoundError`
- `ManifestLoadError`
- `ManifestValidationError`
- `UnsafePackagePathError`
- `SkillDiscoveryError`
- `DuplicateSkillError`

CLI failures write one concise diagnostic to stderr and return a non-zero status.
Diagnostics include the package-relative failing path and schema location but do
not dump procedure contents or secret values.

## Security boundaries

This scaffold enforces only static package safety:

- safe YAML loading;
- no absolute or parent-traversing package references;
- no handler imports during validation;
- no subprocess execution;
- no environment or secret reads;
- strict schema fields plus a namespaced extension escape hatch.

It does not claim runtime isolation. Process sandboxing, signature verification,
credential provisioning, broker ACL compilation, egress enforcement, and secret
injection remain later supervisor capabilities.

## Testing

Unit tests cover:

- successful loading of the placeholder package;
- deterministic skill discovery and inventory output;
- missing or malformed plugin manifests;
- malformed skill descriptors;
- duplicate skill IDs;
- missing procedure/input/output files;
- absolute paths and `..` traversal attempts;
- unknown top-level core fields;
- preservation of namespaced extension values;
- CLI exit codes and stdout/stderr separation;
- confirmation that validation never imports or starts plugin runtime code.

The test suite uses temporary plugin packages and does not require NATS, Docker,
the aggregator, or network access.

## Non-goals

- Extracting or changing `adapters/_common`.
- Converting an existing adapter into a plugin.
- Starting, monitoring, or restarting plugin processes.
- Defining the final supervisor control protocol.
- Implementing NATS transport, discovery, identity, ACLs, or audit capture.
- Persisting or retrieving learned procedural memory.
- Implementing knowledge-daemon, MCP, A2A gateway, or transport plugins.
- Adding Docker Compose services or host dependencies.

## Acceptance criteria

The milestone is complete when:

1. The placeholder plugin contains a manifest, runtime stub, skill descriptor,
   procedure, and input/output schemas.
2. The no-op supervisor validates it and emits a deterministic inventory.
3. Invalid package structures fail with stable, actionable errors.
4. SDK protocols preserve the future runtime, skill, knowledge, transport, and
   lifecycle seams without implementing them.
5. Tests pass without external services.
6. No existing adapter, message subject, database schema, or deployment behavior
   changes.
