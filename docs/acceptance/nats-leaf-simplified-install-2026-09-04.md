# NATS leaf simplified-install acceptance — 2026-09-04

## Verdict

Pass after remediation against a locally built 0.2.0 wheel. The initial public
0.1.0 acceptance remains recorded below because it explains the release gap and
the defects found. After the fixes, one `edgecitadel install` command enrolled
this Mac as a `nats_leaf` Edge against `jim-eq`, started NATS and agentd, repaired
the prior Codex marketplace registration, and installed the Plugin. Both the
manifest's exact MCP entrypoint and a new Codex session successfully called
`edgecitadel_diagnose` and observed `mode=nats_leaf`.

PyPI itself still serves 0.1.0. The tested 0.2.0 artifact is local and has not
been published, tagged, committed, or pushed.

| Check | Result | Evidence |
|---|---|---|
| Install the public CLI with `uv` | PASS | `edgecitadel==0.1.0` installed and exposed `/Users/yefanzhang/.local/bin/edgecitadel`. |
| Run the documented unified installer | FAIL | Public 0.1.0 has no `edgecitadel install` command. |
| Join `jim-eq` in `nats_leaf` mode | PASS with fallback | `edgecitadel join ... --messaging-mode nats_leaf` succeeded after extracting the invitation URI from human-readable output. |
| Local NATS and JetStream | PASS | Local listeners on 4223/8223; `client_ready`, `jetstream_ready`, and `leaf_connected` were all true. |
| Local `agentd` | PASS with fallback | A separate `edgecitadel service start` was required; overall status then became healthy. |
| Install the Codex Plugin through EdgeCitadel | FAIL | Public `edgecitadel plugin install codex` treats `codex` as a Managed Agent directory. |
| Install the Codex Plugin manually | PASS | Codex marketplace add and plugin add both succeeded; Codex reports version 0.1.0 installed and enabled. |
| Use the Plugin in a new Codex session | FAIL | The skill loaded, but no EdgeCitadel MCP tools appeared and no connector/session was created. The Plugin's exact MCP entrypoint fails during connector registration. |
| Current `main` unified NATS-leaf flow | FAIL | The local-source `edgecitadel install` exists but rejects `--messaging-mode nats_leaf`. |
| Infrastructure regression gate | PASS | Full Compose rebuild, both smoke endpoints, 22 helper tests, and the operator-journey Playwright test passed. |

### Fixed-source retest

| Check | Result | Evidence |
|---|---|---|
| Build and install the next distribution | PASS | Built `edgecitadel-0.2.0-py3-none-any.whl`, installed it in a clean Python 3.12 environment, then replaced the uv tool from that wheel. |
| Run one Edge-side setup command | PASS | `edgecitadel install --join ... --messaging-mode nats_leaf --plugin codex --scope user --yes` completed enrollment, NATS, agentd, Plugin repair/install, and post-install reporting. |
| Upgrade from the 0.1.0 Plugin path | PASS | The driver classified the removed `native-plugins` marketplace as stale and the confirmed unified plan repaired it to `share/edgecitadel/plugins`. |
| Exact Plugin MCP entrypoint | PASS | MCP initialize, tools/list, and `edgecitadel_diagnose` succeeded; seven EdgeCitadel tools were exposed and the connector session closed. |
| New native Codex session | PASS with test-only config | Codex invoked `edgecitadel_diagnose` and returned `Ready: yes. Messaging mode: nats_leaf`; an MCP `env` override targeted the isolated acceptance state. |
| Python regression suites | PASS | Root: 154 passed, 3 skipped. Agent runtime: 542 passed, 3 skipped. Ruff and Homebrew style passed. |
| Infrastructure regression gate | PASS | Full Compose rebuild, NATS/API smoke checks, 22 Node helper tests, and one Chromium operator-journey test passed. |

## Resulting local state

- Local tool: `edgecitadel==0.2.0`, installed by `uv` from the locally built
  wheel. PyPI remains at 0.1.0.
- Passing acceptance Edge state:
  `/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904`.
- Passing Edge identity: `codex-local-leaf-fixed-20260904`.
- Core: `http://jim-eq`; upstream NATS: `nats://jim-eq:4222`.
- Local leaf client: `nats://127.0.0.1:4223`; monitor: `http://127.0.0.1:8223`.
- The passing NATS leaf and isolated `agentd` were left running. The old
  acceptance leaf and agentd were stopped before the retest to release ports
  4223 and 8223.
- The Codex Plugin was left installed and enabled globally from the current
  `share/edgecitadel/plugins` marketplace, and its MCP bridge is functional with
  the local 0.2.0 wheel.
- Two Codex policy/environment probes started `agentd` against the unrelated
  default state and registered `core-codex`. That connector was revoked, its
  token was removed, and the default-state service was stopped again.
- The pre-existing default `/Users/yefanzhang/.edgecitadel/node.json` was not
  overwritten. It describes a local Core, so the acceptance Edge used the hidden
  `--state-dir` escape hatch. Because a non-default state directory selects process
  mode for the NATS leaf, that leaf will not automatically return after reboot.

No invitation secret is included in this report. One invitation from the first
failed capture remained unredeemed and was allowed to expire after its 15-minute
lifetime; the fixed-run invitation was redeemed once and never printed.

## Problems encountered

### 1. PyPI is behind the documentation and current source

`uv tool install edgecitadel` resolved version 0.1.0. Its CLI has no `install`
subcommand and still labels `plugin` as the deprecated Managed Agent alias. The
release tag `v0.1.0` points at `8598b6a`; unified installation arrived later in
`ae6a60f`, and the `uv` documentation update is `8990e56`.

Impact: a newcomer following the current README cannot run the next documented
command after a successful public install.

Required fix: publish a new version containing the unified installer and ensure
the release gate installs from the public index before asserting the README flow.

Resolution status: source and package metadata now identify the next
distribution as 0.2.0, and its wheel passed clean-install testing. Publication
is still pending explicit authorization, so this remains unresolved on PyPI.

### 2. The unified installer still cannot create a NATS leaf

Current `main` exposes `edgecitadel install`, but its parser has no
`--messaging-mode` option. A direct current-source test rejected
`--messaging-mode nats_leaf`. Source inspection also showed that the join call
inside `command_install` passes `messaging_mode="single-client"` unconditionally.

Impact: even after the new installer is released, a one-command Edge install can
only create the non-leaf topology.

Required fix: add the messaging-mode choice to both interactive and
non-interactive install flows and forward it unchanged to `command_join`.

Resolution: implemented and covered by parser, interactive-choice, forwarding,
wheel-help, and live one-command acceptance tests.

### 3. `invite` output is not safe to capture as a value

The first enrollment attempt used command substitution around `edgecitadel
invite`. It failed with `invitation must start with ecjoin://` because `invite`
prints a header, the URI, and an expiry footer to stdout. Filtering for the URI
line made the next invitation work.

Impact: the natural operator pattern `invitation="$(edgecitadel invite ...)"`
does not compose.

Required fix: add structured output such as `invite --json`, or write human
guidance to stderr and the invitation URI alone to stdout. Until then, examples
that capture the value must explicitly select the `ecjoin://` line.

Resolution: the invitation URI is now the only stdout line; the header and
expiry guidance go to stderr. A focused test verifies capture-safe output.

### 4. The shipped `plugin install` is the wrong command surface

Public 0.1.0 interpreted `edgecitadel plugin install codex` as installation of a
Managed Agent package at `./codex`, then failed because that directory did not
exist. Before validating the source it started `agentd` against the default local
Core state, leaving an unrelated service running with transport unconfigured.

Impact: the documented Plugin command fails and has a surprising side effect.

Required fix: ship the new native-host Plugin driver and validate the requested
host/source before starting or modifying services.

Resolution status: the 0.2.0 wheel contains the native-host driver, but users of
PyPI 0.1.0 will not receive it until a new release is published.

### 5. Codex marketplace setup succeeds, but the Plugin MCP bridge is broken

The two native Codex commands succeed:

```bash
codex plugin marketplace add /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins --json
codex plugin add edgecitadel@edgecitadel --json
```

Codex reports the Plugin installed and enabled. A fresh Codex session loaded the
bundled skill, but exposed no `edgecitadel_*` tools, created no `native-mcp`
process, registered no connector, and opened no Agent session.

Running the Plugin manifest's exact entrypoint directly reproduced the failure:

```text
error: connector_id, host_type, and agent_id are required
```

The root cause is the RPC envelope. `_agentd_rpc` builds parameterized requests
as `{"operation": operation, **params}`, while `agentd` reads operation arguments
only from `request["params"]`. Connector registration therefore receives empty
identity fields. The same mismatch is present in current `main`.

Required fix: send operation arguments under `params`, retain authentication
fields at their protocol-defined level, and add an integration test that starts
the exact `.mcp.json` command, performs MCP initialize/tools-list/diagnose, and
asserts connector/session cleanup.

Resolution: the bridge now separates operation parameters from authentication,
both sides of the subprocess boundary have regression tests, and the exact MCP
entrypoint plus a new Codex session passed live acceptance.

### 6. Isolated-state Plugin verification needs inherited environment

The Plugin manifest launches `edgecitadel native-mcp --host-type codex` without a
state-directory argument. On a normal clean installation that correctly targets
`~/.edgecitadel`; in this acceptance run it would target the pre-existing local
Core unless the new Codex process inherited
`EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904`.

This is primarily a test-environment deviation, but it also means multiple local
EdgeCitadel installations cannot select a Plugin target declaratively.

Retest handling: the final native Codex run supplied a test-only
`mcp_servers.edgecitadel.env.EDGECITADEL_STATE_DIR` override. A normal clean host
using the default `~/.edgecitadel` state needs no override.

### 7. Diagnostic-only issues

- One parallel diagnostic batch was rejected before process creation because its
  proposed working directory did not exist; the batch was rerun from the repo.
- The first safe Core `/leafz` projection treated `leafnodes` as an array. This
  NATS response used `leafs` for the array, so the corrected query used that key.
- The ephemeral Codex test warned that installed skill descriptions exceeded its
  context budget. This was not the cause of the missing EdgeCitadel tools; the
  direct MCP entrypoint independently reproduced the registration failure.

### 8. A 0.1.0 Codex marketplace path becomes invalid after uv upgrade

Replacing the 0.1.0 uv tool removed its owned `native-plugins` files, but Codex
still referenced that now-empty directory. Consequently, both Codex Plugin JSON
commands exited before producing JSON. The driver classified this as `unknown`,
and the unified installer could not repair it.

Resolution: the Codex driver recognizes this bounded native error as a stale
EdgeCitadel marketplace source. A confirmed unified install now plans and runs
the existing repair operation automatically. Unrelated or malformed native
output remains `unknown` and blocks mutation.

### 9. Non-interactive Codex execution has two test-harness constraints

The first new Codex session discovered the EdgeCitadel tool but could not invoke
it because this user's Codex configuration sets `approval_policy="never"`.
`--approve-for-me` enabled automatic review, but Codex rejects that flag when it
is combined with an explicit `--sandbox` value. Omitting `--sandbox` used the
flag's documented workspace-write sandbox and allowed the read-only diagnostic.

Separately, the outer `EDGECITADEL_STATE_DIR` was not inherited by the Plugin's
MCP child. `shell_environment_policy.inherit="all"` did not change MCP transport
environment. Supplying the state directory in the MCP server's `env` field did.
These are acceptance-harness constraints caused by testing beside a pre-existing
default Core, not extra steps for a normal single-state installation.

### 10. Repository test environment was absent

The first focused pytest commands failed before collection because
`/Users/yefanzhang/workplace/edge-research/.venv/bin/python` did not exist, and
`python3.12` was not on `PATH`. uv already had CPython 3.12.11, so the documented
local `.venv` was created with uv. The root requirements did not include the
agent-runtime-only `respx` extra; installing `agent-runtime[test]` completed the
full runtime test environment.

### 11. Expected inactive Connector affects aggregate status

Immediately after the new Codex process exited, its renewable connector lease
was briefly active and then became inactive as designed. `edgecitadel status`
therefore reports the otherwise healthy Leaf as degraded whenever the registered
native Plugin has no live host session. The component checks still showed local
NATS, JetStream, Leaf, agentd, transport, and Plugin installation healthy. This
matches the current architecture contract and was not changed in this fix.

### 12. Edge setup documentation mixed unified and lower-level flows

The installation sections introduced `edgecitadel install`, but the default
Edge and Homebrew examples still led with enrollment-only `edgecitadel join`.
That made the advertised simplified path depend on which section a newcomer
read.

Resolution: the README, onboarding guide, Python distribution guide, Homebrew
guide, and Formula caveats now consistently lead with `edgecitadel install` for
both default and Leaf Edges. They retain `edgecitadel join` only as the explicit
lower-level operation for users who intentionally manage services and Plugins
separately.

## Successful simplified retest flow

The remote Core still ran the pre-fix invitation command, so `sed` was needed
only while capturing that remote output. On a Core running the fixed source,
plain command substitution receives only the invitation URI. The Edge itself was
configured by one command:

```bash
leaf_invitation="$(ssh root@jim-eq 'cd /root/snap/EdgeCitadel && ./scripts/edgecitadel invite --node-id codex-local-leaf-fixed-20260904 --host jim-eq' | sed -n '/^ecjoin:\/\//p')"
edgecitadel install --join "$leaf_invitation" --messaging-mode nats_leaf --plugin codex --scope user --yes --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
```

The command reported `ok: true` and performed distribution validation,
enrollment, local NATS Leaf startup, agentd startup, stale Codex marketplace
repair, Plugin installation, and the expected pending-session Connector check.
The hidden `--state-dir` argument is acceptance isolation only.

For a normal new Core, the intended newcomer path is the two public commands
`uv tool install edgecitadel` and `edgecitadel install`; the guided installer
then asks whether to create or join and which Plugin hosts to configure. A Leaf
Edge additionally needs an invitation from its Core and a native `nats-server`
binary because the Leaf is a server topology, not a Python package.

## Initial successful fallback flow

These are the minimum commands that actually established the leaf in this test.
The invitation remains a shell variable and is never printed or persisted:

```bash
uv tool install edgecitadel
leaf_invitation="$(ssh root@jim-eq 'cd /root/snap/EdgeCitadel && ./scripts/edgecitadel invite --node-id codex-local-leaf-20260904 --host jim-eq' | sed -n '/^ecjoin:\/\//p')"
edgecitadel join "$leaf_invitation" --messaging-mode nats_leaf --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel service start --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
```

Manual Plugin installation reached the installed/enabled state, but not a usable
MCP connection:

```bash
codex plugin marketplace add /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins --json
codex plugin add edgecitadel@edgecitadel --json
codex plugin list --marketplace edgecitadel --available --json
```

## Complete shell-command transcript

This section records every shell command executed directly for the acceptance,
investigation, verification, and required documentation sync. Commands inside a
single shell invocation are kept in the same block. Browser lookups and the
ephemeral Codex agent's internal MCP/JavaScript calls were not shell commands;
they are described after the transcript.

### Required workflow instructions

```bash
cat /Users/yefanzhang/workplace/edge-research/.agents/skills/deliberate-changes/SKILL.md
cat /Users/yefanzhang/.codex/skills/.system/openai-docs/SKILL.md
cat /Users/yefanzhang/workplace/edge-research/.agents/skills/verify-infra/SKILL.md
```

### Repository, host, dependency, and state preflight

```bash
git status --short --branch
find . -name AGENTS.md -print
rg -n 'add_parser\("install"|messaging-mode|plugin|scope|def cmd_install|def command_install|nats_leaf' scripts/edgecitadel_cli.py scripts/plugin_installation.py README.md docs/onboarding.md pyproject.toml
command -v uv
uv --version
uv tool list
command -v edgecitadel
edgecitadel --version
edgecitadel --help
command -v nats-server
nats-server --version
brew list --versions nats-server
brew services list | rg '(^|[[:space:]])nats-server([[:space:]]|$)'
command -v codex
codex --version
codex plugin --help
codex plugin marketplace --help
rg --files /Users/yefanzhang/.edgecitadel
jq -r 'keys[]' /Users/yefanzhang/.edgecitadel/node.json
rg -n 'STATE_DIR|state-dir|EDGECITADEL_.*STATE|default_state|Path.home\(\)' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py /Users/yefanzhang/workplace/edge-research/scripts/nats_leaf.py /Users/yefanzhang/workplace/edge-research/edgecitadel
sed -n '380,500p' /Users/yefanzhang/workplace/edge-research/scripts/plugin_installation.py
sed -n '2700,2905p' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py
sed -n '2970,3060p' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py
launchctl list | rg 'edgecitadel|nats'
lsof -nP -iTCP:4222 -iTCP:7422 -sTCP:LISTEN
ssh root@jim-eq 'cd /root/snap/EdgeCitadel && git rev-parse --short HEAD && ./scripts/edgecitadel service status && ./scripts/edgecitadel messaging status'
```

### Public package installation and surface inspection

```bash
uv tool install edgecitadel
edgecitadel --version
edgecitadel --help
edgecitadel install --help
edgecitadel join --help
edgecitadel plugin --help
command -v edgecitadel
ls -l /Users/yefanzhang/.local/bin/edgecitadel
sed -n '1,280p' /Users/yefanzhang/workplace/edge-research/scripts/nats_leaf.py
ssh root@jim-eq 'cd /root/snap/EdgeCitadel && docker compose ps'
ssh root@jim-eq 'curl -fsS http://127.0.0.1:8222/healthz'
ssh root@jim-eq 'curl -fsS http://127.0.0.1/api/system/status'
jq '{version, mode, agent_id, core_url, nats_url}' /Users/yefanzhang/.edgecitadel/node.json
codex plugin marketplace list --json
codex plugin list --marketplace edgecitadel --available --json
lsof -nP -iTCP:4223 -iTCP:8223 -sTCP:LISTEN
edgecitadel plugin install --help
edgecitadel connector --help
edgecitadel connector register --help
edgecitadel messaging --help
edgecitadel service --help
find . \( -name edgecitadel_cli.py -o -name plugin_installation.py \) -print
rg -n 'Version:|Name:' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/lib/python*/site-packages/edgecitadel-*.dist-info/METADATA
test ! -e /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
nc -vz jim-eq 7422
curl -fsS http://jim-eq:8222/healthz
curl -fsS http://jim-eq/api/system/status
edgecitadel connector path codex
find /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/plugins -maxdepth 4 -type f -print
rg -n 'connector register|native-mcp|marketplace|plugin add|edgecitadel' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/plugins /Users/yefanzhang/workplace/edge-research/plugins/codex
```

The `find .` command above ran with
`/Users/yefanzhang/.local/share/uv/tools/edgecitadel` as its working directory;
all other commands in that block ran from the repository.

### Enrollment and NATS-leaf verification

```bash
leaf_invitation="$(ssh root@jim-eq 'cd /root/snap/EdgeCitadel && ./scripts/edgecitadel invite --node-id codex-local-leaf-20260904 --host jim-eq')"
edgecitadel join "$leaf_invitation" --messaging-mode nats_leaf --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
rg -n 'def command_invite|ecjoin://|Invitation' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py
find /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins -maxdepth 5 -type f -print
find /Users/yefanzhang/workplace/edge-research/plugins -maxdepth 5 -type f -print
sed -n '520,585p' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py
sed -n '520,585p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
leaf_invitation="$(ssh root@jim-eq 'cd /root/snap/EdgeCitadel && ./scripts/edgecitadel invite --node-id codex-local-leaf-20260904 --host jim-eq' | sed -n '/^ecjoin:\/\//p')"
edgecitadel join "$leaf_invitation" --messaging-mode nats_leaf --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel messaging status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
lsof -nP -iTCP:4223 -iTCP:8223 -sTCP:LISTEN
curl -fsS 'http://127.0.0.1:8223/healthz?js-enabled-only=true'
curl -fsS http://jim-eq:8222/leafz | jq '{num_leafnodes, leafnodes: [.leafnodes[] | {name, account, ip, port}]}'
jq '{version, mode, messaging_mode, core_url, upstream_nats_url, agent_id, plugin_nats_url, jetstream_domain}' /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904/node.json
stat -f '%Sp %N' /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904 /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904/node.json /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904/nats_leaf/nats.conf /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904/nats_leaf/credentials.json
edgecitadel service start --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel service status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
curl -fsS http://jim-eq:8222/leafz | jq '{num_leafnodes, leafs: ((.leafs // []) | map({name, account, ip, port}))}'
jq . /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins/.agents/plugins/marketplace.json
jq . /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins/plugins/edgecitadel/.codex-plugin/plugin.json
sed -n '1,180p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins/plugins/edgecitadel/.mcp.json
sed -n '1,220p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins/plugins/edgecitadel/skills/edgecitadel/SKILL.md
```

### Plugin installation and runtime diagnosis

```bash
edgecitadel plugin install codex
edgecitadel service status --json
launchctl list | rg 'com\.edgecitadel\.(agentd|nats-leaf)'
rg -n 'def command_native_mcp|native-mcp|connector.*register|host_type' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py | head -n 120
rg -n 'def command_plugin_install|Preparing the local Agent service|_ensure_agentd' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
codex plugin marketplace add /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins --json
codex plugin add edgecitadel@edgecitadel --json
codex plugin marketplace list --json
codex plugin list --marketplace edgecitadel --available --json
sed -n '1495,1565p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
sed -n '1330,1395p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
find /Users/yefanzhang/.codex/plugins/cache/edgecitadel/edgecitadel/0.1.0 -maxdepth 4 -type f -print
jq . /Users/yefanzhang/.codex/plugins/cache/edgecitadel/edgecitadel/0.1.0/.codex-plugin/plugin.json
codex exec --help
EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904 codex exec --ephemeral --sandbox read-only --cd /Users/yefanzhang/workplace/edge-research --json 'Use the installed EdgeCitadel plugin to call edgecitadel_diagnose exactly once. Do not use shell commands. Return a concise summary of the tool result.'
edgecitadel connector list --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel service status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
ps -axo pid,ppid,command | rg 'edgecitadel_agentd\.mcp|edgecitadel native-mcp|codex exec'
rg -n 'edgecitadel|MCP|mcp' /Users/yefanzhang/.codex/log/codex-tui.log | tail -n 100
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"acceptance-test","version":"1"}}}' '{"jsonrpc":"2.0","method":"notifications/initialized"}' '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"edgecitadel_diagnose","arguments":{}}}' | EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904 edgecitadel native-mcp --host-type codex
edgecitadel native-mcp --help
rg -n 'connector_id, host_type, and agent_id are required|connector_id.*required|args\.connector_id|args\.agent_id' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
sed -n '2920,2955p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
sed -n '2990,3045p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
rg -n 'connector_id, host_type, and agent_id are required' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel /Users/yefanzhang/workplace/edge-research
edgecitadel connector list --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
rg --files /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904 | sort
tail -n 120 /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904/logs/agentd.log
sed -n '360,405p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/plugin-toolkit/src/edgecitadel_agentd/store.py
rg -n 'connector\.register|register_connector|connector_id.*host_type.*agent_id' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/plugin-toolkit/src/edgecitadel_agentd
tail -n 120 /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904/agentd/agentd.log
sed -n '240,280p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/plugin-toolkit/src/edgecitadel_agentd/service.py
rg -n 'def _agentd_rpc' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
sed -n '1140,1185p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
sed -n '1028,1065p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
rg -n 'def _agentd_request|def request|"params"' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py | head -n 80
sed -n '1038,1085p' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/scripts/edgecitadel_cli.py
sed -n '1130,1175p' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py
rg -n 'def _params' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/plugin-toolkit/src/edgecitadel_agentd/service.py /Users/yefanzhang/workplace/edge-research/agent-runtime/src/edgecitadel_agentd/service.py
rg -n '^def _agentd_rpc' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py
sed -n '1105,1150p' /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py
sed -n '165,190p' /Users/yefanzhang/workplace/edge-research/agent-runtime/src/edgecitadel_agentd/service.py
edgecitadel service stop
```

The ephemeral Codex process was polled until the missing-tool behavior was clear,
then interrupted with Ctrl-C. Polling and Ctrl-C were process-control operations,
not additional shell commands.

### Current-source comparison and infrastructure gate

```bash
uv tool run --from /Users/yefanzhang/workplace/edge-research edgecitadel --version
uv tool run --from /Users/yefanzhang/workplace/edge-research edgecitadel install --join ecjoin://redacted --messaging-mode nats_leaf --plugin codex --scope user --yes --state-dir /Users/yefanzhang/.edgecitadel-acceptance-unused
uv tool run --from /Users/yefanzhang/workplace/edge-research edgecitadel plugin status codex --scope user --json
docker compose down && docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8222/healthz
curl -fsS http://localhost/api/system/status
rg --files /Users/yefanzhang/workplace/edge-research/e2e | rg '\.(spec|test)\.(ts|js)$' | sort
edgecitadel status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
curl -fsS 'http://127.0.0.1:8223/healthz?js-enabled-only=true'
npm test -- tests/operator-journey.spec.js
rg --files /Users/yefanzhang/workplace/edge-research/docs | sort
git log --oneline --decorate -12
git log --oneline v0.1.0..main -- README.md docs/onboarding.md scripts/edgecitadel_cli.py scripts/plugin_installation.py plugins pyproject.toml
```

The `npm test` command ran with
`/Users/yefanzhang/workplace/edge-research/e2e` as its working directory; the
remaining commands ran from the repository.

### Vault-sync preparation and final checks

```bash
ls -la /Users/yefanzhang/Documents/Obsidian\ Vault
rg --files /Users/yefanzhang/Documents/Obsidian\ Vault | rg -i 'edgecitadel|edge-research|(^|/)\.manifest\.json$|(^|/)log\.md$' | head -n 200
cat /Users/yefanzhang/Documents/Obsidian\ Vault/.manifest.json
sed -n '1,240p' /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/overview.md
sed -n '1,220p' /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/docs.md
sed -n '1,220p' /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/plugins.md
sed -n '1,220p' /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/nats.md
tail -n 120 /Users/yefanzhang/Documents/Obsidian\ Vault/log.md
rg -n 'manifest|Obsidian Vault|codebases/edge-research' /Users/yefanzhang/workplace/edge-research /Users/yefanzhang/.codex /Users/yefanzhang/.agents --glob '*.py' --glob '*.sh' --glob '*.md' | head -n 200
tail -n 100 /Users/yefanzhang/Documents/Obsidian\ Vault/log.md
jq '.sources["/Users/yefanzhang/workplace/edge-research"], .sources["/Users/yefanzhang/workplace/edge-research/docs/onboarding.md"]' /Users/yefanzhang/Documents/Obsidian\ Vault/.manifest.json
sed -n '1,120p' /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/docs.md
sed -n '1,140p' /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/plugins.md
sed -n '1,140p' /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/nats.md
date -u '+%Y-%m-%dT%H:%M:%SZ'
date '+%Y-%m-%d %H:%M:%S %Z'
jq empty /Users/yefanzhang/Documents/Obsidian\ Vault/.manifest.json
git diff --check
git status --short --branch
edgecitadel status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel service status --json
codex plugin list --marketplace edgecitadel --available --json
launchctl list | rg 'com\.edgecitadel\.(agentd|nats-leaf)'
if rg -n '[[:blank:]]+$' /Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md; then
  exit 1
fi
jq -r '.sources["/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md"].pages[]' /Users/yefanzhang/Documents/Obsidian\ Vault/.manifest.json | while IFS= read -r page; do
  test -e "/Users/yefanzhang/Documents/Obsidian Vault/$page"
done
rg -n 'nats-leaf-simplified-install-acceptance|nats-leaf-simplified-install-2026-09-04' /Users/yefanzhang/Documents/Obsidian\ Vault/.manifest.json /Users/yefanzhang/Documents/Obsidian\ Vault/log.md /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/overview.md /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/plugins.md /Users/yefanzhang/Documents/Obsidian\ Vault/codebases/edge-research/modules/nats.md
```

The final ten commands were declared here before execution so their exact text
could remain part of the transcript. Their results are summarized below.

## Remediation and retest shell-command transcript

The commands below are every shell command run while investigating, fixing,
packaging, and retesting after the initial acceptance. Patch-tool edits and
process polling are not shell commands. Unless a different working directory is
called out, commands ran from `/Users/yefanzhang/workplace/edge-research`.

### Source, release, and test inspection

```bash
sed -n '2670,2795p' scripts/edgecitadel_cli.py
rg -n '^def test_.*(install|invite|native_mcp|agentd_rpc)|def _agentd_rpc|def command_invite|def command_install|messaging_mode' scripts/tests/test_edgecitadel_cli.py scripts/edgecitadel_cli.py agent-runtime/src/edgecitadel_agentd/rpc.py
sed -n '1,220p' scripts/tests/test_edgecitadel_cli.py
sed -n '1,180p' agent-runtime/src/edgecitadel_agentd/rpc.py && sed -n '940,1115p' scripts/tests/test_edgecitadel_cli.py
sed -n '500,625p' scripts/edgecitadel_cli.py && sed -n '1080,1165p' scripts/edgecitadel_cli.py
sed -n '2790,2925p' scripts/edgecitadel_cli.py && sed -n '3170,3370p' scripts/edgecitadel_cli.py
rg -n 'command_invite|invite_parser|invite.*json|Single-use invitation|Expires at|command_install\(' scripts/tests tests scripts -g '*.py'
rg -n 'edgecitadel install|uv tool install|messaging-mode|nats_leaf|PyPI|version' README.md docs pyproject.toml scripts -g '*.md' -g '*.toml' -g '*.py'
sed -n '2960,3075p' scripts/edgecitadel_cli.py
sed -n '220,390p' scripts/tests/test_edgecitadel_cli.py && sed -n '790,915p' scripts/tests/test_edgecitadel_cli.py && sed -n '1160,1315p' scripts/tests/test_edgecitadel_cli.py
sed -n '1,125p' README.md && sed -n '1,135p' docs/onboarding.md && sed -n '1,125p' pyproject.toml
git log --oneline --decorate -12 && git status --short --branch && rg -n '0\.1\.0|bump|release|publish|pypi' .github deploy scripts pyproject.toml Makefile -g '*'
sed -n '1,220p' agent-runtime/pyproject.toml && find agent-runtime/tests -maxdepth 3 -name 'conftest.py' -print && sed -n '1,180p' agent-runtime/tests/conftest.py 2>/dev/null
sed -n '1,140p' agent-runtime/src/edgecitadel_agentd/client.py && rg -n 'AgentdClient\(' agent-runtime/tests -g '*.py' | head -40
sed -n '410,470p' docs/acceptance/nats-leaf-simplified-install-2026-09-04.md && sed -n '135,185p' deploy/pip/README.md 2>/dev/null && sed -n '1,90p' deploy/pip/README.md
git show 8598b6a:pyproject.toml | sed -n '1,25p'; git show 8598b6a:scripts/edgecitadel_cli.py | rg -n 'def command_install|add_parser\("install"'; git show ae6a60f:scripts/edgecitadel_cli.py | rg -n 'def command_install|add_parser\("install"'
rg -n '_agentd_rpc\(' scripts/edgecitadel_cli.py
rg -n 'def _dispatch|connector\.register|connector\.configure|operation ==|params\[' agent-runtime/src/edgecitadel_agentd/service.py
sed -n '1020,1095p' scripts/edgecitadel_cli.py && sed -n '1510,1715p' scripts/edgecitadel_cli.py && sed -n '2380,2530p' scripts/edgecitadel_cli.py
sed -n '150,390p' agent-runtime/src/edgecitadel_agentd/service.py
rg -n '^def _toolkit_python|edgecitadel-toolkit|toolkit' scripts/edgecitadel_cli.py | head -80 && sed -n '920,1045p' scripts/edgecitadel_cli.py
rg -n 'rpc\.main|edgecitadel_agentd\.rpc|sys\.stdin|StringIO' agent-runtime/tests scripts/tests -g '*.py'
sed -n '1425,1625p' scripts/edgecitadel_cli.py
git diff -- docs/acceptance/nats-leaf-simplified-install-2026-09-04.md; git check-ignore -v dist 2>/dev/null || true; ls -ld .venv .venv/bin/python 2>/dev/null
sed -n '1,130p' .github/workflows/publish-pypi.yml && sed -n '1,110p' deploy/homebrew/Formula/edgecitadel.rb
rg -n 'VERSION|version = "0\.1\.0"|edgecitadel 0\.1\.0|edgecitadel-0\.1\.0\.data' --glob '!docs/acceptance/**' --glob '!docs/architecture/network-topology-and-request-routing.md' .
git tag --list --sort=-version:refname && gh release list --repo zhonghaozhan/EdgeCitadel --limit 10
git show --stat --oneline ae6a60f && git show --stat --oneline 8990e56 && git log --format='%h %ad %s' --date=iso-strict 8598b6a..HEAD
rg -n '^def _write_node|CompletedProcess|subprocess\.run' scripts/tests/test_edgecitadel_cli.py | head -80
sed -n '360,460p' scripts/tests/test_edgecitadel_cli.py && sed -n '1300,1395p' scripts/tests/test_edgecitadel_cli.py
rg -n 'monkeypatch\.setattr\(.*input|builtins, "input"|isatty|interactive_install' scripts/tests -g '*.py'
sed -n '1,70p' agent-runtime/src/edgecitadel_agentd/service.py && sed -n '1,90p' scripts/edgecitadel_cli.py
sed -n '1,470p' docs/acceptance/nats-leaf-simplified-install-2026-09-04.md
```

### Test-environment bootstrap and failing regression reproduction

```bash
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q scripts/tests/test_edgecitadel_cli.py -k 'unified_install_forwards_nats_leaf_mode or interactive_install_collects_nats_leaf_mode or agentd_rpc_keeps_operation_params_separate_from_auth or invite_stdout_is_only_the_capture_safe_invitation or parser_exposes_unified'
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q tests/agentd/test_rpc.py
command -v python3.12 && python3.12 --version
command -v python3 && python3 --version && python3 -m pytest --version
git diff --check && git diff --stat && git status --short --branch
uv python find 3.12
uv venv --python 3.12 /Users/yefanzhang/workplace/edge-research/.venv
uv pip install --python /Users/yefanzhang/workplace/edge-research/.venv/bin/python -r /Users/yefanzhang/workplace/edge-research/scripts/requirements-test.txt
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q scripts/tests/test_edgecitadel_cli.py -k 'unified_install_forwards_nats_leaf_mode or interactive_install_collects_nats_leaf_mode or agentd_rpc_keeps_operation_params_separate_from_auth or invite_stdout_is_only_the_capture_safe_invitation or parser_exposes_unified'
```

The following command ran from
`/Users/yefanzhang/workplace/edge-research/agent-runtime`:

```bash
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q tests/agentd/test_rpc.py
```

### Focused fixes, full Python gates, and first wheel build

```bash
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q scripts/tests/test_edgecitadel_cli.py -k 'unified_install_forwards_nats_leaf_mode or interactive_install_collects_nats_leaf_mode or agentd_rpc_keeps_operation_params_separate_from_auth or invite_stdout_is_only_the_capture_safe_invitation or parser_exposes_unified'
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q tests/test_pip_distribution.py
sed -n '1,80p' edgecitadel/__init__.py && sed -n '1,55p' tests/test_pip_distribution.py && rg -n '__version__' edgecitadel
rg -n 'edgecitadel-0\.1\.0\.data|edgecitadel 0\.1\.0|^version = "0\.1\.0"|__version__ = "0\.1\.0"|VERSION = "0\.1\.0"' pyproject.toml edgecitadel scripts deploy tests
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q tests/test_pip_distribution.py
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff check scripts/edgecitadel_cli.py scripts/tests/test_edgecitadel_cli.py agent-runtime/src/edgecitadel_agentd/rpc.py agent-runtime/tests/agentd/test_rpc.py
git diff --check && git diff --stat && git status --short --branch
sed -n '20,85p' README.md
sed -n '20,75p' docs/onboarding.md
sed -n '8,42p' deploy/pip/README.md && sed -n '245,265p' docs/architecture/agent-packages-plugins-installation-migration.md
sed -n '1,120p' scripts/requirements-test.txt && /Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pip show edgecitadel-agent-runtime 2>/dev/null || true && /Users/yefanzhang/workplace/edge-research/.venv/bin/python -c 'import edgecitadel_agentd.rpc; print(edgecitadel_agentd.rpc.__file__)'
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q tests scripts/tests deploy/tests schemas/tests
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff format --check scripts/edgecitadel_cli.py scripts/tests/test_edgecitadel_cli.py agent-runtime/src/edgecitadel_agentd/rpc.py agent-runtime/tests/agentd/test_rpc.py
brew style deploy/homebrew/Formula/edgecitadel.rb
sed -n '180,370p' scripts/plugin_installation.py && sed -n '810,875p' scripts/edgecitadel_cli.py
sed -n '1420,1475p' scripts/tests/test_edgecitadel_cli.py && git diff -- scripts/edgecitadel_cli.py | sed -n '1,240p'
sed -n '1,150p' agent-runtime/tests/hermes_runtime/conftest.py && sed -n '1,80p' agent-runtime/tests/homeassistant_runtime/conftest.py
uv pip install --python /Users/yefanzhang/workplace/edge-research/.venv/bin/python --editable '/Users/yefanzhang/workplace/edge-research/agent-runtime[test]'
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff format scripts/edgecitadel_cli.py scripts/tests/test_edgecitadel_cli.py agent-runtime/src/edgecitadel_agentd/rpc.py agent-runtime/tests/agentd/test_rpc.py
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q tests scripts/tests deploy/tests schemas/tests
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff check scripts/edgecitadel_cli.py scripts/tests/test_edgecitadel_cli.py agent-runtime/src/edgecitadel_agentd/rpc.py agent-runtime/tests/agentd/test_rpc.py && /Users/yefanzhang/workplace/edge-research/.venv/bin/ruff format --check scripts/edgecitadel_cli.py scripts/tests/test_edgecitadel_cli.py agent-runtime/src/edgecitadel_agentd/rpc.py agent-runtime/tests/agentd/test_rpc.py
git diff --check && git diff --stat
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m build
test ! -e /tmp/edgecitadel-wheel-test-20260904
uv venv --python 3.12 /tmp/edgecitadel-wheel-test-20260904
uv pip install --python /tmp/edgecitadel-wheel-test-20260904/bin/python /Users/yefanzhang/workplace/edge-research/dist/edgecitadel-0.2.0-py3-none-any.whl
/tmp/edgecitadel-wheel-test-20260904/bin/edgecitadel --version
/tmp/edgecitadel-wheel-test-20260904/bin/edgecitadel install --help
find /tmp/edgecitadel-wheel-test-20260904 -path '*share/edgecitadel/agent-runtime/src/edgecitadel_agentd/rpc.py' -o -path '*share/edgecitadel/native-plugins/plugins/edgecitadel/.mcp.json' -o -path '*share/edgecitadel/scripts/edgecitadel_cli.py'
find /tmp/edgecitadel-wheel-test-20260904/share/edgecitadel/plugins -maxdepth 5 -type f -print | sort | head -80
find plugins -maxdepth 5 -type f -print | sort | head -80 && sed -n '1,120p' scripts/installation_assets.py
unzip -l dist/edgecitadel-0.2.0-py3-none-any.whl | rg 'native-plugins|plugins/edgecitadel/.mcp|edgecitadel_agentd/rpc.py|scripts/edgecitadel_cli.py'
```

The two full agent-runtime pytest commands in this phase ran from
`/Users/yefanzhang/workplace/edge-research/agent-runtime`:

```bash
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q
```

### Old-state shutdown and first live 0.2.0 installation

```bash
edgecitadel --version
edgecitadel service status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel messaging status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
test ! -e /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
edgecitadel service stop --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
edgecitadel messaging stop --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-20260904
lsof -nP -iTCP:4223 -iTCP:8223 -sTCP:LISTEN
launchctl list | rg 'com\.edgecitadel\.(agentd|nats-leaf)'
uv tool install --force /Users/yefanzhang/workplace/edge-research/dist/edgecitadel-0.2.0-py3-none-any.whl
edgecitadel --version
edgecitadel install --help
edgecitadel plugin status codex --scope user --json
nc -vz jim-eq 7422
codex plugin marketplace list --json
codex plugin list --marketplace edgecitadel --available --json
rg -n '^class Codex|CodexDriver|marketplace list|--available|invalid Plugin JSON' scripts/plugin_installation.py scripts/tests/test_plugin_installation.py
sed -n '580,755p' scripts/plugin_installation.py && sed -n '240,360p' scripts/tests/test_plugin_installation.py
sed -n '370,475p' scripts/plugin_installation.py
sed -n '1,225p' scripts/tests/test_plugin_installation.py
ls -la /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel && find /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/plugins -maxdepth 3 -type f -print | sort
jq . /Users/yefanzhang/.codex/plugins/marketplaces.json 2>/dev/null || true; rg -n 'native-plugins|edgecitadel' /Users/yefanzhang/.codex -g '*.json' -g '*.toml' --glob '!plugins/cache/**' | head -80
find /Users/yefanzhang/.local/share/uv/tools/edgecitadel/share/edgecitadel/native-plugins -maxdepth 4 -type f -print | sort
find /Users/yefanzhang/.local/share/uv/tools/edgecitadel/lib/python3.12/site-packages -maxdepth 1 -name 'edgecitadel-*.dist-info' -print && rg -n 'native-plugins|plugin-toolkit' /Users/yefanzhang/.local/share/uv/tools/edgecitadel/lib/python3.12/site-packages/edgecitadel-0.2.0.dist-info/RECORD
codex plugin marketplace add --help && codex plugin marketplace remove --help
sed -n '455,510p' scripts/plugin_installation.py && jq . plugins/.agents/plugins/marketplace.json && jq . plugins/plugins/edgecitadel/.codex-plugin/plugin.json
sed -n '1,185p' scripts/plugin_installation.py
rg -n '^def command_native_plugin|_confirm_native_plans' scripts/edgecitadel_cli.py && sed -n '2570,2675p' scripts/edgecitadel_cli.py
rg -n 'serverInfo|"0\.1\.0"|version' agent-runtime/src/edgecitadel_agentd/mcp.py plugins -g '*.json' -g '*.py' -g '*.toml'
```

### Upgrade-path regression, final package build, and uv replacement

```bash
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q scripts/tests/test_plugin_installation.py -k missing_configured_marketplace_is_stale
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q scripts/tests/test_edgecitadel_cli.py -k unified_install_repairs_stale_plugin_source
sed -n '2825,2925p' scripts/edgecitadel_cli.py
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q scripts/tests/test_plugin_installation.py -k 'codex_missing_configured_marketplace_is_stale or codex_install_is_status_first or codex_unknown_json or codex_repair'
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q scripts/tests/test_edgecitadel_cli.py -k 'unified_install_repairs_stale_plugin_source or unified_install_skips_missing_host or unified_install_forwards_nats_leaf'
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff check scripts/plugin_installation.py scripts/edgecitadel_cli.py scripts/tests/test_plugin_installation.py scripts/tests/test_edgecitadel_cli.py
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff format scripts/plugin_installation.py scripts/edgecitadel_cli.py scripts/tests/test_plugin_installation.py scripts/tests/test_edgecitadel_cli.py
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q tests scripts/tests deploy/tests schemas/tests
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff check scripts/edgecitadel_cli.py scripts/plugin_installation.py scripts/tests/test_edgecitadel_cli.py scripts/tests/test_plugin_installation.py agent-runtime/src/edgecitadel_agentd/rpc.py agent-runtime/tests/agentd/test_rpc.py && /Users/yefanzhang/workplace/edge-research/.venv/bin/ruff format --check scripts/edgecitadel_cli.py scripts/plugin_installation.py scripts/tests/test_edgecitadel_cli.py scripts/tests/test_plugin_installation.py agent-runtime/src/edgecitadel_agentd/rpc.py agent-runtime/tests/agentd/test_rpc.py
brew style deploy/homebrew/Formula/edgecitadel.rb && git diff --check
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m build
test ! -e /tmp/edgecitadel-wheel-test-final-20260904
uv venv --python 3.12 /tmp/edgecitadel-wheel-test-final-20260904
uv pip install --python /tmp/edgecitadel-wheel-test-final-20260904/bin/python /Users/yefanzhang/workplace/edge-research/dist/edgecitadel-0.2.0-py3-none-any.whl
/tmp/edgecitadel-wheel-test-final-20260904/bin/edgecitadel --version
/tmp/edgecitadel-wheel-test-final-20260904/bin/edgecitadel install --help
unzip -p /Users/yefanzhang/workplace/edge-research/dist/edgecitadel-0.2.0-py3-none-any.whl edgecitadel-0.2.0.data/data/share/edgecitadel/scripts/plugin_installation.py | rg -n 'failed_codex_marketplace_source|configured Codex marketplace path is stale'
uv tool install --force /Users/yefanzhang/workplace/edge-research/dist/edgecitadel-0.2.0-py3-none-any.whl
edgecitadel plugin status codex --scope user --json
```

The full agent-runtime test in this phase ran from
`/Users/yefanzhang/workplace/edge-research/agent-runtime`:

```bash
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m pytest -q
```

### One-command Leaf and exact Plugin MCP verification

```bash
leaf_invitation="$(ssh root@jim-eq 'cd /root/snap/EdgeCitadel && ./scripts/edgecitadel invite --node-id codex-local-leaf-fixed-20260904 --host jim-eq' | sed -n '/^ecjoin:\/\//p')"
edgecitadel install --join "$leaf_invitation" --messaging-mode nats_leaf --plugin codex --scope user --yes --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
edgecitadel status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
edgecitadel messaging status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
edgecitadel service status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
codex plugin list --marketplace edgecitadel --available --json
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"acceptance-test","version":"1"}}}' '{"jsonrpc":"2.0","method":"notifications/initialized"}' '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"edgecitadel_diagnose","arguments":{}}}' | EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904 edgecitadel native-mcp --host-type codex
edgecitadel connector list --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
edgecitadel service status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
curl -fsS 'http://127.0.0.1:8223/healthz?js-enabled-only=true'
curl -fsS http://jim-eq:8222/leafz | jq '{num_leafnodes, leafs: ((.leafs // []) | map(select(.name == "codex-local-leaf-fixed-20260904") | {name, account, ip, port}))}'
```

### New Codex session policy and isolated-state verification

```bash
EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904 codex exec --ephemeral --sandbox read-only --cd /Users/yefanzhang/workplace/edge-research --json 'Use the installed EdgeCitadel plugin to call edgecitadel_diagnose exactly once. Do not use shell commands. Return only whether the service is ready and which messaging mode it reports.'
codex exec --help
codex --help
jq . plugins/plugins/edgecitadel/.mcp.json && jq . /Users/yefanzhang/.codex/plugins/cache/edgecitadel/edgecitadel/0.1.0/.mcp.json
rg -n 'approval|requires approval|mcp.*approval|tool.*approval' /Users/yefanzhang/.codex/config.toml /Users/yefanzhang/.codex/plugins/cache/edgecitadel/edgecitadel/0.1.0 plugins -g '*.toml' -g '*.json' -g '*.md'
EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904 codex exec --ephemeral --sandbox read-only --approve-for-me --cd /Users/yefanzhang/workplace/edge-research --json 'Use the installed EdgeCitadel plugin to call edgecitadel_diagnose exactly once. Do not use shell commands. Return only whether the service is ready and which messaging mode it reports.'
EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904 codex exec --ephemeral --approve-for-me --cd /Users/yefanzhang/workplace/edge-research --json 'Use the installed EdgeCitadel plugin to call edgecitadel_diagnose exactly once. Do not use shell commands. Return only whether the service is ready and which messaging mode it reports.'
EDGECITADEL_STATE_DIR=/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904 codex exec --ephemeral --approve-for-me -c 'shell_environment_policy.inherit="all"' --cd /Users/yefanzhang/workplace/edge-research --json 'Use the installed EdgeCitadel plugin to call edgecitadel_diagnose exactly once. Do not use shell commands. Return only whether the service is ready and which messaging mode it reports.'
codex mcp add --help
codex mcp get edgecitadel --json
codex debug --help && codex doctor --help
sed -n '1,105p' /Users/yefanzhang/.codex/config.toml
codex mcp get edgecitadel --json -c 'mcp_servers.edgecitadel.env.EDGECITADEL_STATE_DIR="/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904"'
codex mcp get edgecitadel --json -c 'mcp_servers.edgecitadel={command="edgecitadel",args=["native-mcp","--host-type","codex"],env={EDGECITADEL_STATE_DIR="/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904"}}'
codex exec --ephemeral --approve-for-me -c 'mcp_servers.edgecitadel={command="edgecitadel",args=["native-mcp","--host-type","codex"],env={EDGECITADEL_STATE_DIR="/Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904"}}' --cd /Users/yefanzhang/workplace/edge-research --json 'Use the installed EdgeCitadel plugin to call edgecitadel_diagnose exactly once. Do not use shell commands. Return only whether the service is ready and which messaging mode it reports.'
```

The long-running `codex exec` commands were polled without additional shell
commands. One session first tried an incorrect cache path, recovered to the
actual path, and then reached the expected approval failure; those were model
tool calls, not shell commands issued by this acceptance.

### Probe cleanup and infrastructure verification

```bash
edgecitadel connector list --json
edgecitadel service status --json
edgecitadel connector list --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
curl -fsS http://jim-eq:8222/leafz | jq '{keys: keys, num_leafnodes, leafs: ((.leafs // []) | map({name, account, ip, port}))}'
edgecitadel connector revoke codex-local
edgecitadel service stop
edgecitadel service status --json
edgecitadel connector list --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
ps -axo pid,ppid,command | rg 'edgecitadel_agentd\.mcp|edgecitadel native-mcp|codex exec'
test ! -e /Users/yefanzhang/.edgecitadel/connectors/codex-local.token
docker compose down && docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8222/healthz
curl -fsS http://localhost/api/system/status
edgecitadel status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
rg -n 'connector.*inactive|inactive.*connector|session_active|Connector activity|doctor' scripts/tests docs/architecture docs/onboarding.md README.md -g '*.py' -g '*.md'
sed -n '630,675p' docs/architecture/agent-packages-plugins-installation-migration.md && sed -n '635,670p' docs/architecture/managed-agents-and-native-plugins.md
rg -n 'connector_' scripts/tests/test_edgecitadel_cli.py | tail -40
sed -n '345,405p' docs/architecture/agent-packages-plugins-installation-migration.md && sed -n '548,568p' docs/architecture/agent-packages-plugins-installation-migration.md && sed -n '168,182p' docs/onboarding.md
rg -n 'plugin repair|distribution (path|upgrade)|stale' README.md docs/onboarding.md deploy/pip/README.md docs/architecture/agent-packages-plugins-installation-migration.md | head -80
git diff -- scripts/edgecitadel_cli.py scripts/plugin_installation.py agent-runtime/src/edgecitadel_agentd/rpc.py edgecitadel/__init__.py pyproject.toml deploy/homebrew/Formula/edgecitadel.rb
git diff -- scripts/tests/test_edgecitadel_cli.py scripts/tests/test_plugin_installation.py agent-runtime/tests/agentd/test_rpc.py
git diff -- README.md docs/onboarding.md deploy/pip/README.md docs/architecture/agent-packages-plugins-installation-migration.md
git status --short --branch && git diff --check
```

The following command ran from
`/Users/yefanzhang/workplace/edge-research/e2e`:

```bash
npm test -- tests/operator-journey.spec.js
```

### Documentation reconciliation and release-artifact checks

```bash
git status --short
rg -n '08:55:27|Simplified Leaf|PyPI|0\.2\.0' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/overview.md'
sed -n '1,180p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/overview.md'
sed -n '1,220p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/plugins.md'
sed -n '1,200p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/nats.md'
sed -n '1,220p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/scripts.md'
sed -n '1,240p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/agent-packages-plugins-installation-migration.md'
jq '.sources | to_entries[] | select(.key | contains("edge-research")) | select(.key | test("README.md$|docs/onboarding.md$|agent-packages-plugins-installation-migration.md$|nats-leaf-simplified-install"))' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
rg --files '/Users/yefanzhang/workplace/edge-research' | rg '/(plugins|agent-packages|agent-runtime)/README\.md$'
rg -n '^## |^### |messaging-mode|stale|repair|connector.register|_agentd_rpc|0\.2\.0' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/agent-packages-plugins-installation-migration.md'
tail -n 180 '/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md'
rg -n '"/Users/yefanzhang/workplace/edge-research(|/README.md|/deploy/pip/README.md|/docs/onboarding.md|/docs/architecture/agent-packages-plugins-installation-migration.md|/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md)"' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
sed -n '1,50p' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
tail -n 30 '/Users/yefanzhang/Documents/Obsidian Vault/log.md'
sed -n '245,410p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/agent-packages-plugins-installation-migration.md'
tail -n 70 '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/agent-packages-plugins-installation-migration.md'
sed -n '1,35p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/plugins.md'
rg -n 'Formula is HEAD|HEAD Formula|Homebrew wraps' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/scripts.md'
sed -n '1,28p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/scripts.md'
sed -n '138,160p' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/scripts.md'
sed -n '1,180p' '/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md'
rg -n '8598b6a|ae6a60f|8990e56|0\.1\.0|0\.2\.0|uv' '/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md' | head -n 80
jq empty '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
rg -n '08:55:27|0\.2\.0|nested|stale marketplace|nats_leaf' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/nats-leaf-simplified-install-acceptance.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/overview.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/agent-packages-plugins-installation-migration.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/plugin-infrastructure.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/plugins.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/nats.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/scripts.md' '/Users/yefanzhang/Documents/Obsidian Vault/log.md'
git diff --stat
git diff --check
git diff -- pyproject.toml edgecitadel/__init__.py deploy/homebrew/Formula/edgecitadel.rb scripts/edgecitadel_cli.py scripts/plugin_installation.py agent-runtime/src/edgecitadel_agentd/rpc.py scripts/tests/test_edgecitadel_cli.py scripts/tests/test_plugin_installation.py
sed -n '1,240p' '/Users/yefanzhang/workplace/edge-research/agent-runtime/tests/agentd/test_rpc.py'
sed -n '2670,2925p' '/Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py'
sed -n '130,210p' '/Users/yefanzhang/workplace/edge-research/scripts/plugin_installation.py'
sed -n '390,525p' '/Users/yefanzhang/workplace/edge-research/scripts/plugin_installation.py'
sed -n '1,90p' '/Users/yefanzhang/workplace/edge-research/deploy/homebrew/Formula/edgecitadel.rb'
rg -n '0\.1\.0|edgecitadel-0\.1\.0|v0\.1\.0' '/Users/yefanzhang/workplace/edge-research' -g '!dist/**' -g '!docs/acceptance/**' -g '!frontend/node_modules/**' -g '!e2e/node_modules/**' -g '!*.lock' -g '!*.json' | head -n 160
rg -n 'edgecitadel (join|install)|uv tool install|pip install' '/Users/yefanzhang/workplace/edge-research/deploy/homebrew/README.md' '/Users/yefanzhang/workplace/edge-research/README.md' '/Users/yefanzhang/workplace/edge-research/docs/onboarding.md' '/Users/yefanzhang/workplace/edge-research/deploy/pip/README.md'
sed -n '1,95p' '/Users/yefanzhang/workplace/edge-research/README.md'
sed -n '1,90p' '/Users/yefanzhang/workplace/edge-research/deploy/homebrew/README.md'
sed -n '1,95p' '/Users/yefanzhang/workplace/edge-research/docs/onboarding.md'
rg -n 'deploy/homebrew/(README.md|Formula/edgecitadel.rb)' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
rg -n 'deploy/homebrew/(README.md|Formula/edgecitadel.rb)' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/overview.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/plugin-infrastructure.md' '/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/scripts.md' '/Users/yefanzhang/Documents/Obsidian Vault/_entities/EdgeCitadel.md'
sed -n '208,240p' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
sed -n '1,80p' '/Users/yefanzhang/Documents/Obsidian Vault/_entities/EdgeCitadel.md'
rg -n 'Homebrew|install --join|edgecitadel join' '/Users/yefanzhang/Documents/Obsidian Vault/_entities/EdgeCitadel.md'
sed -n '42,125p' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
sed -n '150,210p' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
sed -n '210,240p' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
nl -ba '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json' | sed -n '185,225p'
jq empty '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff check /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py /Users/yefanzhang/workplace/edge-research/scripts/plugin_installation.py /Users/yefanzhang/workplace/edge-research/scripts/tests/test_edgecitadel_cli.py /Users/yefanzhang/workplace/edge-research/scripts/tests/test_plugin_installation.py /Users/yefanzhang/workplace/edge-research/agent-runtime/src/edgecitadel_agentd/rpc.py /Users/yefanzhang/workplace/edge-research/agent-runtime/tests/agentd/test_rpc.py
/Users/yefanzhang/workplace/edge-research/.venv/bin/ruff format --check /Users/yefanzhang/workplace/edge-research/scripts/edgecitadel_cli.py /Users/yefanzhang/workplace/edge-research/scripts/plugin_installation.py /Users/yefanzhang/workplace/edge-research/scripts/tests/test_edgecitadel_cli.py /Users/yefanzhang/workplace/edge-research/scripts/tests/test_plugin_installation.py /Users/yefanzhang/workplace/edge-research/agent-runtime/src/edgecitadel_agentd/rpc.py /Users/yefanzhang/workplace/edge-research/agent-runtime/tests/agentd/test_rpc.py
brew style /Users/yefanzhang/workplace/edge-research/deploy/homebrew/Formula/edgecitadel.rb
git diff --check
/Users/yefanzhang/workplace/edge-research/.venv/bin/python -m build
uvx --from 'twine>=6' twine check /Users/yefanzhang/workplace/edge-research/dist/edgecitadel-0.2.0-py3-none-any.whl /Users/yefanzhang/workplace/edge-research/dist/edgecitadel-0.2.0.tar.gz
ls -ld /tmp/edgecitadel-wheel-test-20260904 /tmp/edgecitadel-wheel-test-final-20260904
rm -r /tmp/edgecitadel-wheel-test-20260904 /tmp/edgecitadel-wheel-test-final-20260904
rg -n '^## Complete|^### ' '/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md' | tail -n 35
sed -n '455,525p' '/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md'
sed -n '180,305p' '/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md'
```

The documentation edits themselves used the patch tool, not shell commands. The
following closing checks were declared here before execution:

```bash
jq empty '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json'
jq -r '.sources | to_entries[] | select(.value.ingested_at == "2026-09-04T08:55:27Z") | .value.pages[]' '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json' | sort -u | while IFS= read -r page; do test -e "/Users/yefanzhang/Documents/Obsidian Vault/$page"; done
if rg -n '[[:blank:]]+$' '/Users/yefanzhang/workplace/edge-research/docs/acceptance/nats-leaf-simplified-install-2026-09-04.md'; then exit 1; fi
brew style /Users/yefanzhang/workplace/edge-research/deploy/homebrew/Formula/edgecitadel.rb
edgecitadel --version
edgecitadel messaging status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
edgecitadel service status --json --state-dir /Users/yefanzhang/.edgecitadel-acceptance-nats-leaf-fixed-20260904
git diff --check
git status --short --branch
```

## External documentation checked

The official OpenAI Plugin documentation was checked before testing Codex. It
states that Codex CLI installs Plugins from configured marketplaces, exposes the
`codex plugin` and `codex plugin marketplace` commands, and requires a new
session before newly installed skills or tools become available:

- [Plugins](https://developers.openai.com/codex/plugins/)
- [Codex CLI reference](https://developers.openai.com/codex/cli/reference/)

## Final verification

- Root Python suite: 154 passed, 3 skipped.
- Agent-runtime suite: 542 passed, 3 skipped.
- Ruff lint/format and Homebrew Formula style: passed.
- `python -m build`: produced the 0.2.0 sdist and wheel; a clean Python 3.12
  environment installed the wheel and exposed the Leaf option.
- Twine metadata checks passed for both the final 0.2.0 sdist and wheel.
- One-command Edge setup: passed, including automatic stale Codex marketplace
  repair.
- Exact `.mcp.json` entrypoint: initialized, listed seven tools, diagnosed the
  ready `nats_leaf` service, exited zero, and left its session inactive.
- New Codex session: invoked `edgecitadel_diagnose` and returned `Ready: yes.
  Messaging mode: nats_leaf` with the isolated-state MCP override.
- `docker compose down && docker compose up --build -d`: passed.
- `curl http://localhost:8222/healthz`: passed.
- `curl http://localhost/api/system/status`: passed with NATS and JetStream true.
- `npm test -- tests/operator-journey.spec.js`: 22 helper tests and one Chromium
  Playwright test passed.
- Final isolated Edge component checks: local NATS, JetStream, Leaf connection,
  cross-node messaging, service, agentd transport, and Plugin installation were
  healthy. Aggregate status was degraded only because the tested Codex Connector
  correctly became inactive after its host session exited.
- Final default-state service status: stopped, as intended after cleanup.
- Final Codex state: EdgeCitadel Plugin 0.1.0 installed and enabled from the
  current 0.2.0 distribution's marketplace assets.
- README, onboarding, pip, and Homebrew examples consistently use unified
  installation for both single-client and Leaf Edges.
- Final launchd state: only the isolated acceptance `agentd` label remained;
  the NATS leaf remained a live process because custom state selects process
  mode.
- PyPI was not changed and still serves EdgeCitadel 0.1.0.

## Simplification check

The fixed Core newcomer path is two commands: `uv tool install edgecitadel`, then
`edgecitadel install`. A Leaf Edge necessarily starts with an invitation created
on its Core, but after installing the CLI it now needs only one Edge-side setup
command: `edgecitadel install --join ... --messaging-mode nats_leaf --plugin
codex --scope user --yes`. There is no separate `join`, `service start`, Codex
marketplace, or Codex Plugin command.

`nats-server` remains an external prerequisite only for `nats_leaf`, just as
Docker remains an external prerequisite for a Core. The remaining action needed
to make the documented public flow work is publishing the tested 0.2.0 package;
uv cannot install commits that have not been released to PyPI.
