# EdgeCitadel plugin packages

`plugins/` contains installable, framework-neutral EdgeCitadel plugin packages.
Host schemas, SDK protocols, validation logic, and tests live in
[`../plugin-system/`](../plugin-system/). See the validation-only
[`examples/placeholder`](examples/placeholder/README.md) package for a complete
example.

## Authoring layout

```text
my-plugin/
  plugin.yaml
  plugin.lock.json
  runtime/
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
- `runtime/` holds the future out-of-process implementation named by the
  manifest; static validation never imports or executes it.
- Every immediate child directory of `skills.directory` is one packaged skill
  and must contain both `SKILL.md` and `binding.yaml`.

The stable package identity is `<publisher>.<name>` from `plugin.yaml`. It is
separate from every `agents[].id`, the portable `SKILL.md` name, the public A2A
`binding.yaml` `skillId`, and the opaque execution handler name.

## Procedure and binding boundaries

`SKILL.md` is portable, immutable procedural memory. Its frontmatter owns the
portable name and activation description; its body owns the instructions,
workflow, examples, and success criteria. Optional `references/`, `scripts/`,
and `assets/` support progressive disclosure and remain inert during static
validation.

`binding.yaml` supplies the EdgeCitadel-specific execution binding, A2A skill
ID, input/output schema references, and capability requirements. The referenced
JSON Schemas are the typed skill boundary. Keeping these concerns outside
`SKILL.md` preserves procedure portability across runtimes.

Learned procedural memory must never rewrite an installed package. A future
external `KnowledgeStore` will hold records keyed by plugin identity, skill ID,
skill version, namespace, revision, content hash, and provenance.

## Author workflow

From an activated editable environment in `plugin-system/`:

1. Finalize every file in the package, including optional resources.
2. Generate or regenerate the canonical lock:
   `python -m edgecitadel_supervisor lock ../plugins/path/to/package`.
3. Run the read-only validation:
   `python -m edgecitadel_supervisor validate ../plugins/path/to/package`.
4. Run `python -m pytest -q`.

Regenerate the lock after any package byte changes. `validate` never repairs or
rewrites the package.

## Trust and non-goals

Treat package contents as untrusted input until the supervisor has validated a
supervisor-owned immutable package root. Static checks use safe YAML/JSON
loading, strict schemas, contained paths, canonical hashes, and rejection of
symbolic links and special filesystem nodes. A manifest only requests
capabilities; it does not grant them.

This scaffold does not launch runtimes, implement transport or identity,
provision secrets, enforce a sandbox, grant permissions, persist learned memory,
sign packages, or verify publishers. Package authors must not infer any of those
runtime guarantees from successful static validation.
