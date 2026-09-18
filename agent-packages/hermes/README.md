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

## Execution-bound HTTP server

For live model/tool observations and correctly parented model delegation, run the
packaged server wrapper instead of the ordinary Hermes API server. Use a dedicated
Python 3.12+ environment with Hermes, `edgecitadel-agent-runtime` and `aiohttp`
installed. The existing Hermes Python 3.11 environment is not sufficient. This
launcher does not install dependencies or change a running Hermes service.

Set `HERMES_HOME` to the intended Hermes profile and configure its model/provider
normally. Include `edgecitadel-scoped` in that profile's
`platform_toolsets.api_server` list. The launcher registers the
`edgecitadel_delegate` tool; do not also register an unbound tool with that name.
Recipient permissions still come from the installed Agent Package manifest.

For a source checkout, use absolute paths as follows (replace the example paths
and IDs with the installed Managed Agent's values):

```bash
HERMES_HOME=/absolute/hermes-profile \
EDGECITADEL_SCHEMA_DIR=/absolute/edge-research/schemas \
PYTHONPATH=/absolute/edge-research/agent-packages/hermes \
/absolute/python312-env/bin/python -m edgecitadel_hermes_plugin.server \
  --state-dir /absolute/edgecitadel-state \
  --connector-id managed-hermes --agent-id us-mac-hermes \
  --token-file /absolute/hermes-http.token --port 8642
```

`EDGECITADEL_SCHEMA_DIR` points to the matching release's root protocol schemas;
the standalone runtime wheel does not bundle these files. `PYTHONPATH` points to
the Hermes Agent Package directory. The state directory must contain the existing
Managed Agent connector credential; the launcher neither enrolls a connector nor
opens an execution session. `--token-file` contains the HTTP bearer token and is
also supplied to the Managed Agent as `HERMES_TOKEN_FILE`. The listener is fixed
to loopback. SIGINT/SIGTERM stops the server cleanly.

The managed adapter supplies execution headers on `/v1/chat/completions`. Bound
requests verify session/task authority through agentd before model execution;
model and tool observations use that binding. Delegation outside a bound request
fails closed. Requests without execution headers retain upstream behavior and
do not gain execution tracing. Other upstream API routes are not qualified as
bound execution entrypoints.

Verification uses an owned provider endpoint and two concurrent executions with
shared conversation identity against installed Hermes/runtime wheels. Production
provider behavior and other Hermes versions still require integration checks.
