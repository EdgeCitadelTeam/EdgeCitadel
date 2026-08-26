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

## Research basis

The package design borrows specific, compatible lessons from established plugin
systems without adopting any one system as EdgeCitadel's runtime ABI:

- OpenAI plugins and the Agent Skills specification use `SKILL.md` plus optional
  scripts, references, and assets for portable, progressively disclosed
  procedures.
- OpenAI public plugin examples record vendored skill provenance and SHA-256
  integrity in `plugin.lock.json`.
- HashiCorp's out-of-process plugin system treats protocol-version negotiation as
  separate from ordinary package versioning.
- VS Code extensions declare host-engine compatibility independently of extension
  version and identity.
- Grafana signs a manifest of packaged file hashes; EdgeCitadel reserves signing
  for a later milestone but establishes deterministic hashes now.
- Backstage keeps host APIs and extension points separate from installable plugin
  packages and discourages direct cross-plugin code dependencies.

References:

- <https://developers.openai.com/plugins/concepts/plugins>
- <https://developers.openai.com/plugins/concepts/skills>
- <https://developers.openai.com/plugins/build/plugins>
- <https://agentskills.io/specification>
- <https://github.com/openai/plugins>
- <https://github.com/hashicorp/go-plugin>
- <https://github.com/microsoft/vscode-docs/blob/main/api/references/extension-manifest.md>
- <https://github.com/grafana/plugin-tools/blob/main/docusaurus/docs/publish-a-plugin/sign-a-plugin.md>
- <https://github.com/backstage/backstage/blob/master/docs/backend-system/architecture/04-plugins.md>

## Repository shape

```text
plugin-system/
  pyproject.toml
  schemas/
    agent-plugin.v1alpha1.schema.json
    agent-skill-binding.v1alpha1.schema.json
    plugin-lock.v1.schema.json
  src/
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
      inventory.py
      loader.py
      validator.py
  tests/
    test_cli.py
    test_inventory.py
    test_loader.py
    test_validator.py

plugins/
  examples/
    placeholder/
      plugin.yaml
      plugin.lock.json
      README.md
      runtime/
        __init__.py
        __main__.py
      skills/
        placeholder/
          SKILL.md
          binding.yaml
          schemas/
            input.json
            output.json
          references/
            README.md
          scripts/
            README.md
          assets/
            README.md
```

`plugin-system/` owns the host-side SDK, schemas, supervisor, and tests.
`plugins/` contains only installable plugin packages. This prevents runtime
framework code from becoming part of a plugin's distributable contents and lets
the two trees evolve independently.

Adding both top-level directories requires updating the repository map in
`AGENTS.md`. The initial scaffold does not modify aggregator, adapter, NATS, or
deployment behavior.

## Package manifest

Each plugin root contains `plugin.yaml`, validated against
`agent-plugin.v1alpha1.schema.json`. The core shape is:

```yaml
apiVersion: edgecitadel.io/v1alpha1
kind: AgentPlugin

metadata:
  name: placeholder
  displayName: Placeholder Plugin
  description: Demonstrates the EdgeCitadel plugin package contract.
  version: 0.1.0
  publisher: local

compatibility:
  supervisorApi: ">=0.1.0,<0.2.0"
  protocols:
    - edgecitadel.plugin.v1

runtime:
  command: ["python", "-m", "runtime"]
  healthTimeoutSeconds: 10
  restartPolicy: on-failure

skills:
  directory: skills

agents:
  - id: placeholder-agent
    skillNames: [placeholder]
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

The schema requires a normalized package name, separate publisher, semantic
version, display metadata, supervisor compatibility range, supported process
protocols, non-empty runtime command, relative skills directory, at least one
agent identity, complete permission categories, and known sandbox/restart
policies. Paths may not be absolute or escape the plugin root.

The stable package identity is `<publisher>.<name>`; it is distinct from every
entry in `agents[]`. The first package contains one agent, but the schema does not
require a future breaking change to represent a plugin process that owns multiple
agent identities.

`extensions` is an object whose keys must be reverse-domain or URI-like
namespaces. Core loaders preserve extension values without interpreting them.
Unsupported core fields are rejected.

## Skills and procedural memory

### Portable procedure

Every immediate child directory under `skills.directory` represents one skill
and contains a standard `SKILL.md`. Its YAML frontmatter supplies the portable
skill name, activation description, compatibility statement, and version
metadata; its Markdown body contains instructions, examples, edge cases, and
success criteria.

```markdown
---
name: placeholder
description: Demonstrates a validated EdgeCitadel runtime skill.
compatibility: Requires the EdgeCitadel plugin runtime v1 protocol.
metadata:
  version: "0.1.0"
---

# Placeholder

Follow the packaged procedure and return output matching the declared schema.
```

The skill directory name must equal `SKILL.md.name`. Names follow the Agent
Skills constraints: 1-64 lowercase alphanumeric or hyphen characters, no leading,
trailing, or consecutive hyphens. `SKILL.md` is the single source of truth for
the portable name, description, and procedure.

Optional `references/`, `scripts/`, and `assets/` directories support progressive
disclosure. The supervisor validates their package containment but does not load,
execute, or interpret them.

### EdgeCitadel skill binding

Each skill also contains `binding.yaml`, which keeps EdgeCitadel execution and
A2A metadata separate from portable procedure content:

```yaml
apiVersion: edgecitadel.io/v1alpha1
kind: AgentSkillBinding

skillId: example.placeholder
version: 0.1.0

execution:
  kind: runtime-handler
  name: placeholder

schemas:
  input: schemas/input.json
  output: schemas/output.json

requires:
  knowledge: []
  network: []
  devices: []

extensions: {}
```

The supervisor combines `binding.skillId` with the name and description from
`SKILL.md` to form the future Agent Card skill catalog. Execution-specific fields
remain private to the package and are not exposed automatically.

`execution.name` is an opaque identifier interpreted only by the plugin runtime.
It is deliberately not a Python import path, allowing other languages and agent
frameworks to use the same package contract.

### Packaged procedures

`SKILL.md` is immutable package content containing a prompt, checklist, workflow,
or operating procedure. It is versioned with both the plugin and binding. Input
and output schemas make the procedure boundary inspectable without requiring a
particular agent framework.

The scaffold validates `SKILL.md` frontmatter, `binding.yaml`, and referenced
schemas. It does not interpret procedure content or import the handler.

### Learned procedural memory

Runtime-learned procedures must not rewrite the installed package. The SDK
defines a placeholder `KnowledgeStore` protocol for future persistence using a
record keyed by plugin identity, skill ID, skill version, namespace, revision,
content hash, and provenance.

The plugin manifest requests the knowledge namespaces it may use. A later policy
layer resolves those requests into grants. This keeps reviewed package procedures
separate from mutable learned memory and allows the latter to be audited,
rejected, rolled back, or shared without changing executable code.

### Package lock and integrity

Every distributable package contains `plugin.lock.json`, validated against
`plugin-lock.v1.schema.json`. The lock records its format version, package
identity/version, process protocol, and a deterministic sorted list of packaged
files with SHA-256 hashes. Skill entries also record the portable name, A2A skill
ID, version, and relevant content hashes.

The no-op supervisor recomputes hashes and rejects missing, modified, duplicated,
or unlisted regular files. Symbolic links are rejected in this milestone. The
lock establishes reproducible procedural content and future signature input;
cryptographic signing and publisher verification remain later work.

The lockfile does not list or hash itself and contains no generation timestamp or
other volatile value. The `lock` command structurally validates the package and
writes canonical JSON with lexicographically sorted paths; the `validate` command
then verifies that canonical record without modifying the package.

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

Package semantic version, supervisor API compatibility, and process protocol
version are independent. A package update may change procedures without changing
the process protocol; an incompatible protocol is rejected before process launch.

The lifecycle vocabulary is explicit even though process execution is absent:

```text
discovered -> validated -> installed -> starting -> ready -> draining -> stopped
       \----------- any active state may transition to failed -----------/
```

Only `discovered` and `validated` occur in this milestone. The remaining states
reserve stable names for later supervisor behavior.

## No-op supervisor

The runnable command is:

```bash
cd plugin-system
python -m edgecitadel_supervisor lock ../plugins/examples/placeholder
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
```

It performs these steps:

1. Resolve and verify the plugin root.
2. Load `plugin.yaml` with safe YAML parsing.
3. Validate it against the plugin schema.
4. Check declared supervisor and process-protocol compatibility.
5. Resolve `skills.directory` without following paths outside the package.
6. Reject every symbolic link in the package.
7. Discover skill directories in deterministic name order.
8. Validate each `SKILL.md` frontmatter and directory-name match.
9. Validate each `binding.yaml` against the binding schema.
10. Reject duplicate portable names and A2A skill IDs.
11. Verify every referenced schema stays inside its skill directory.
12. Verify `agents[].skillNames` refers only to packaged skills.
13. Recompute and verify `plugin.lock.json` file hashes.
14. Emit a deterministic JSON inventory containing package identity,
    compatibility, runtime metadata, agent-to-skill mappings, requested
    permissions, skill metadata, and content hashes.

The command exits zero only for a fully valid package. It does not run the
runtime command, import handlers, inspect procedure contents, connect to NATS,
or grant requested permissions.

Future supervisor subcommands (`install`, `start`, `stop`, `status`, `upgrade`)
are documented as reserved lifecycle operations but are not registered as usable
commands in this milestone.

`lock` is a packaging operation, not a lifecycle operation. It never imports or
executes plugin code.

## Error model

The loader and validator expose stable domain errors rather than raw library
exceptions:

- `PluginNotFoundError`
- `ManifestLoadError`
- `ManifestValidationError`
- `CompatibilityError`
- `UnsafePackagePathError`
- `SkillDiscoveryError`
- `DuplicateSkillError`
- `LockIntegrityError`

CLI failures write one concise diagnostic to stderr and return a non-zero status.
Diagnostics include the package-relative failing path and schema location but do
not dump procedure contents or secret values.

## Security boundaries

This scaffold enforces only static package safety:

- safe YAML loading;
- no absolute, parent-traversing, or symlinked package references;
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
- malformed `SKILL.md` frontmatter and binding descriptors;
- mismatched skill directory and portable name;
- duplicate portable names and A2A skill IDs;
- unknown agent-to-skill references;
- missing input/output schemas;
- absolute paths, `..` traversal attempts, and symbolic links;
- unsupported supervisor API or process protocol;
- missing, modified, duplicated, and unlisted lockfile content;
- canonical lockfile generation with no volatile fields or self-hash;
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
- Signing packages or verifying publisher identity.
- Resolving or installing third-party language dependencies.
- Implementing knowledge-daemon, MCP, A2A gateway, or transport plugins.
- Adding Docker Compose services or host dependencies.

## Acceptance criteria

The milestone is complete when:

1. The placeholder plugin contains a manifest, lockfile, runtime stub, standard
   `SKILL.md`, EdgeCitadel binding, references/scripts/assets directories, and
   input/output schemas.
2. The no-op supervisor validates it and emits a deterministic inventory.
3. The no-op supervisor can regenerate its canonical lockfile without executing
   plugin code.
4. The supervisor verifies package compatibility, containment, and SHA-256
   integrity without loading runtime code.
5. Invalid package structures fail with stable, actionable errors.
6. SDK protocols preserve the future runtime, skill, knowledge, transport, and
   lifecycle seams without implementing them.
7. Tests pass without external services.
8. `AGENTS.md` documents the two new top-level directories.
9. No existing adapter, message subject, database schema, or deployment behavior
   changes.
