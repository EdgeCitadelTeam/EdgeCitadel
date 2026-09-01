# Hermes Plugin

Hermes bridges an operator-managed local Hermes Agent service into
EdgeCitadel. Hermes remains the owner of its model session memory.

```bash
edgecitadel plugin install hermes --keep-disabled
```

Start the local Hermes service separately, then provide `HERMES_TOKEN` and any
non-default `HERMES_BASE_URL`, `HERMES_MODEL`, or `HERMES_TIMEOUT_SEC` settings
when starting `edgecitadel.hermes`. The ignored `agent.env` convention is for
legacy source checkouts only; the Plugin package accepts configuration solely
through the Supervisor's manifest-declared environment allowlist. Tests live in
`plugin-toolkit/tests/hermes_runtime/`.
