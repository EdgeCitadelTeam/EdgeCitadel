# EdgeCitadel plugin packages

`plugins/` contains installable, framework-neutral EdgeCitadel plugin packages.
Host schemas, SDK protocols, validation logic, and tests live in
[`../plugin-toolkit/`](../plugin-toolkit/). See
[`examples/echo`](examples/echo/README.md) for a working lifecycle example and
[`examples/placeholder`](examples/placeholder/README.md) for a validation-only
example.

## Authoring layout

```text
my-plugin/
  plugin.yaml
  plugin.lock.json
  unique_python_package/
  skills/
    my-skill/
      SKILL.md
      binding.yaml
      schemas/
        input.json
        output.json
      references/       # optional
      scripts/          # optional
      assets/           # optional
```

- `plugin.yaml` declares package identity and version, supervisor/protocol
  compatibility, runtime metadata, agent identities, requested permissions, and
  security intent.
- `plugin.lock.json` is the generated canonical inventory and SHA-256 integrity
  record for every other regular package file.
- The runtime package holds the out-of-process implementation named by the manifest;
  static validation never imports or executes it.
- Every immediate child directory of `skills.directory` is one packaged skill
  and must contain both `SKILL.md` and `binding.yaml`.

The stable package identity is `<publisher>.<name>` from `plugin.yaml`. It is
separate from every `agents[].id`, the portable `SKILL.md` name, the public A2A
`binding.yaml` `skillId`, and the opaque execution handler name.

## Procedure and binding boundaries

`SKILL.md` is portable, immutable procedural memory. Its frontmatter requires the
portable name and activation description. It may also declare a string `license`,
a string `compatibility` of at most 500 characters, string-to-string `metadata`,
and the experimental space-separated `allowed-tools` string; unknown fields are
accepted for forward compatibility. Its body owns the instructions, workflow,
examples, and success criteria. Optional `references/`, `scripts/`, and `assets/`
support progressive disclosure and remain inert during static validation.

`binding.yaml` supplies the EdgeCitadel-specific execution binding, A2A skill
ID, input/output schema references, and capability requirements. Its `version`
is authoritative when `SKILL.md` omits `metadata.version`; when present, the two
versions must agree. The referenced JSON Schemas are the typed skill boundary
and may use only fragment-local `$ref` or `$dynamicRef` values beginning with
`#`. Keeping these concerns outside `SKILL.md` preserves procedure portability
across runtimes and prevents validation from retrieving another schema.

Learned procedural memory must never rewrite an installed package. A future
external `KnowledgeStore` record contains `plugin_id`, `skill_id`,
`skill_version`, `namespace`, `revision`, `content_hash`, and `provenance`.
`KnowledgeStore.read()` looks up records by the first four fields; revision,
content hash, and provenance are record and audit metadata.

## Author workflow

From an activated editable environment in `plugin-toolkit/`:

1. Finalize every file in the package, including optional resources.
2. Generate or regenerate the canonical lock:
   `python -m edgecitadel_supervisor lock ../plugins/path/to/package`.
3. Run the read-only validation:
   `python -m edgecitadel_supervisor validate ../plugins/path/to/package`.
4. Run `python -m pytest -q`.

Regenerate the lock after any package byte changes. `validate` never repairs or
rewrites the package and rejects a lock unless its bytes exactly match the
two-space-indented, sorted-key JSON representation with one final newline and,
after semantic checks, the current generated lock record.

## Trust and non-goals

End users install through the unified lifecycle after a host has created or
joined a fleet:

```bash
./scripts/edgecitadel plugin install ./plugins/examples/echo
./scripts/edgecitadel plugin install gemma
./scripts/edgecitadel plugin install watchdog
```

Host enrollment and agent registration are separate. `edgecitadel join` gives
the host broker configuration according to `single-client` or `nats_leaf`;
starting a plugin reconciles its exact destination inbox, then publishes the
Agent Card and heartbeat that make each declared agent visible in the registry.
Plugins do not select the mode. In `nats_leaf`, they receive only the Edge-local
endpoint, client token, and Edge JetStream domain—not the upstream Leaf identity.

Treat package contents as untrusted input until the supervisor has validated a
supervisor-owned immutable package root. YAML and JSON reject duplicate keys;
structured files are limited to 1 MiB, `SKILL.md` to 2 MiB, frontmatter to
64 KiB, trees to depth 64 and 100,000 traversed values, and YAML container aliases
are rejected. Static checks also use strict schemas, local-fragment-only schema
references, control-free contained relative paths, canonical hashes, and
rejection of symbolic links and special filesystem nodes. A manifest only
requests capabilities; it does not grant them.

Portable package paths reject absolute and drive paths, backslashes, empty or dot
components, traversal, and all Unicode `Cc` control characters. Ordinary Unicode
filenames are allowed.

Static toolkit validation alone does not launch runtimes, implement transport or
identity, provision secrets, enforce a sandbox, grant permissions, persist
learned memory, sign packages, or verify publishers. The root CLI currently adds
local process control and enrolled broker injection, but v0.1 permission and
sandbox declarations remain reviewable intent rather than an OS enforcement
boundary. Package authors must not infer runtime guarantees from validation.

Python Plugins may declare a lock-covered `runtime.pythonRequirements` file.
The Supervisor installs it with the shared Plugin runtime into a private,
versioned environment. Parent-process environment values are not inherited by
default: a Plugin receives only a small runtime baseline, names listed in
`runtime.environmentVariables`, names listed in `security.secrets`, and its
mode-selected NATS client settings. Plugins never receive Leaf credentials.
