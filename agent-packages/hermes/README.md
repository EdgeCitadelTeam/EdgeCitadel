# Hermes Managed Agent

Hermes bridges an operator-managed local Hermes Agent service into
EdgeCitadel. Hermes remains the owner of its model session memory.

```bash
edgecitadel agent install hermes --keep-disabled
```

Start the local Hermes service separately, then provide `HERMES_TOKEN_FILE` and any
non-default `HERMES_BASE_URL`, `HERMES_MODEL`, or `HERMES_TIMEOUT_SEC` settings
when starting `edgecitadel.hermes`. The ignored `agent.env` convention is for
legacy source checkouts only; the Managed Agent accepts configuration solely
through the manifest-declared environment allowlist. Tests live in
`agent-runtime/tests/hermes_runtime/`.
