# Agent Packages, Plugins, and Unified Installation Migration

Status: Implemented through Phase 3; local verification complete
Owner: EdgeCitadel maintainers
Reviewers: Unassigned
Date: 2026-09-03

## 1. Executive mental model

EdgeCitadel has two different installable artifacts whose former directory names
made them look reversed. An **Agent Package** contains a complete runtime such as
Gemma, Hermes, or the Home Assistant adapter; EdgeCitadel installs it and agentd
owns its process lifecycle. A **Plugin** extends an existing host such as Codex,
Claude Code, or Pi; that host owns its model, session, tools, permissions, and
execution loop. A **Connector** is the live authenticated session created by an
installed Plugin, not the Plugin package itself. The CLI now provides one
idempotent `edgecitadel install` onboarding flow while retaining explicit
`agent`, `plugin`, and `connector` commands for automation and diagnosis.

## 2. Decision and status

### Implemented decisions

1. Use **Agent Package** for the distributable artifact that EdgeCitadel runs.
2. Use **Agent** for an installed/running identity declared by an Agent Package.
3. Use **Plugin** for a Codex, Claude Code, or Pi host extension.
4. Keep **Connector** for the runtime registration/session opened by a Plugin.
5. Rename repository directories:
   - `plugins/` -> `agent-packages/`;
   - `native-plugins/` -> `plugins/`;
   - `plugin-toolkit/` -> `agent-runtime/`.
6. Add `edgecitadel plugin install|list|status|remove|repair` while preserving
   `edgecitadel connector list|status|revoke` for runtime state.
7. Add `edgecitadel install` as an interactive orchestration command that reuses
   existing create/join, service, Agent Package, and Plugin operations.
8. Prefer native host package managers for Codex, Claude Code, and Pi. Do not
   directly edit `AGENTS.md`, `CLAUDE.md`, hooks, or host settings when a supported
   native plugin mechanism exists.

### Explicitly deferred decisions

- Keep `kind: ManagedAgent`, the `v1alpha2` schema ID, package protocol strings,
  installed-state filenames, SQLite schema, environment variables, and Python
  import package names unchanged in the first release.
- Support only scopes exposed by each native host: Codex user scope; Claude Code
  user, project, and local scopes; Pi user and project scopes. The common first
  release exposes `user|project`; Claude Code's `local` scope remains available
  through its native CLI until EdgeCitadel defines a non-ambiguous common name.
- Do not rename NATS subjects, envelope types, connector IDs, or Agent IDs.

## 3. Problem and evidence

### Implemented baseline

- `plugins/` contains `ManagedAgent` manifests for Gemma, Hermes, Home Assistant,
  Echo, and Placeholder. Each package may contain a Python runtime, skills,
  bindings, schemas, and a canonical lock.
- `native-plugins/` contains actual Codex, Claude Code, and Pi plugin packages.
- `plugin-toolkit/` contains agentd, the Managed Agent runtime, SDK contracts,
  package validation, schemas, and tests.
- `edgecitadel agent install` validates and installs Managed Agents into the
  agentd-owned store.
- Existing-host integration requires the operator to obtain a bundled path and
  run one or more host-specific commands.
- Durable state already uses `managed-agents.json`, but installed package roots
  remain under the private `state_dir/plugins/` path for compatibility.

### Naming failure

The ordinary meaning of Plugin is an extension loaded by another product. The
repository instead assigns the shortest `plugins/` name to complete
EdgeCitadel-owned runtimes and qualifies the actual host plugins as
`native-plugins/`. `plugin-toolkit/` then uses the same overloaded word for code
that mostly belongs to Agent execution and agentd.

### Installation friction

After installing the Python distribution, a new operator must understand
create-versus-join, start agentd, locate bundled native assets, and translate
those paths into host-specific marketplace commands. The explicit commands are
useful for diagnosis but are too much vocabulary for the first successful run.

Graphify provides a useful precedent: it installs one Python CLI and exposes a
single installer that selects a host adapter, supports user or project scope,
copies the appropriate skill assets, writes a version marker, and verifies the
result. Its implementation also uses staged writes and backs up differing local
skill content. EdgeCitadel should adopt the single orchestration entry point and
platform-driver pattern, but retain native host plugin managers because
EdgeCitadel Plugins include MCP and hook integration rather than only procedure
files. See [Graphify installation](https://github.com/Graphify-Labs/graphify#install)
and [its installer implementation](https://github.com/Graphify-Labs/graphify/blob/v8/graphify/install.py).

## 4. Goal, assumptions, verification, and constraints

### Goal

Make artifact names match lifecycle ownership and reduce first-run setup to one
safe, idempotent command after distribution installation.

### Verified assumptions

| Assumption | Evidence | Result |
|---|---|---|
| Agent Packages already have a distinct public lifecycle | `scripts/edgecitadel_cli.py` exposes `edgecitadel agent install|list|status|start|stop|logs|remove` | Confirmed |
| Plugin assets are bundled with the Python distribution | Root [`pyproject.toml`](../../pyproject.toml) includes and maps `native-plugins/` into the wheel | Confirmed |
| Plugin installation currently delegates to host CLIs | README and onboarding call `pi install`, `claude plugin ...`, and `codex plugin ...` with `connector path` | Confirmed |
| Plugin and Connector are different state | Host manifests are static packages; agentd stores connector credentials, configuration, leases, and sessions | Confirmed |
| Renaming private installed roots is unnecessary | [`scripts/edgecitadel_cli.py`](../../scripts/edgecitadel_cli.py) keeps Managed Agent copies under `state_dir/plugins/`, while durable inventory is already named `managed-agents.json` | Confirmed; preserve the private path |
| A repository rename affects distribution and service startup | [`pyproject.toml`](../../pyproject.toml), [`edgecitadel/cli.py`](../../edgecitadel/cli.py), CLI service commands, requirements, Dockerfiles, and distribution tests reference current paths | Confirmed |
| Manifest terminology can remain compatible | Current validator requires `kind: ManagedAgent`; directory names do not participate in the wire protocol | Confirmed |
| No NATS or database migration is required | Repository paths and CLI orchestration do not change subjects or stored record shapes | Confirmed |

### Host evidence snapshot and remaining proofs

The following evidence was collected on 2026-09-03. It establishes the proposed
driver shape, not a timeless host contract; the external acceptance gate must be
rerun against every minimum supported version before Phase 1 merges.

| Host | Observed or documented contract | Result |
|---|---|---|
| Codex CLI 0.151.0 | Local `--help` exposes `plugin marketplace add|list|upgrade|remove`, `plugin add|list|remove`, and JSON for every status/mutation command. No scope option is exposed. Official OpenAI documentation did not document this surface when reviewed. | User scope only; version 0.151.0 is the provisional minimum pending an official stability statement or compatibility smoke against the chosen release floor. |
| Claude Code 2.1.150 | The installed CLI exposes non-interactive marketplace and plugin commands, JSON list output, and `user|project|local` scope. Current [Claude Code marketplace documentation](https://code.claude.com/docs/en/plugin-marketplaces) and [plugin reference](https://code.claude.com/docs/en/plugins-reference) document the same operations and scopes. | User and project scope are admissible after isolated idempotency tests; local scope is intentionally not mapped in release 1. |
| Pi package manager | Current [Pi package documentation](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/packages.md) documents `install`, `list`, `remove`, local-path packages, global settings, and project settings via `-l`. The repository package currently peers on `@earendil-works/pi-coding-agent ^0.84.4`. | User and project scope are admissible after a real-host smoke at 0.84.4; Pi was not installed on the evidence host. |

| Remaining assumption | Validation owner | Proof required before Phase 1 merge |
|---|---|---|
| Adding the same local marketplace and Plugin is idempotent for every supported host | Plugin-driver implementer | Run install twice in isolated user profiles at each minimum version; the second run must report `changed: false` and preserve unrelated packages. |
| Host status distinguishes installed, stale, and unknown without brittle text parsing | Plugin-driver implementer | Codex and Claude Code: contract-test their JSON fields. Pi: compare its documented settings entry's resolved local path with the packaged path and use `unknown`, never `installed`, when the settings format cannot be parsed safely. |
| Codex 0.151.0 is an acceptable support floor | Release owner | Pin or revise the floor after testing the oldest distributed Codex version EdgeCitadel will support. No implementation may infer support from executable presence alone. |

### Constraints

- Existing Agents, task history, connector tokens, and local SQLite files must
  survive upgrade and rollback.
- An old executable must continue to operate its old distribution; a new
  executable must understand the new packaged layout.
- `edgecitadel install` must never silently install an Agent Package, grant a
  capability, overwrite user-authored instruction files, or delete an existing
  host plugin.
- Interactive prompts require a TTY. Automation must use explicit flags.
- Installation success must distinguish **installed** from **active**: a Plugin
  can be installed correctly while its native host has no active session.

## 5. Success criteria and non-goals

### 5.1 Implementation quality rubric

The implementation is accepted only after strict review reaches at least
95/100 using these weighted criteria:

| Criterion | Weight | Excellent evidence |
|---|---:|---|
| Functional completeness | 30% | Codex, Claude Code, and Pi drivers; explicit Plugin CLI; unified installation; and separate Plugin/Connector diagnostics match this design. |
| Safety and idempotency | 25% | Status-first native operations, safe scope and path checks, no shell, bounded/redacted evidence, reconciliation after timeout or interruption, and preservation of pre-existing installations. |
| Compatibility and migration | 20% | New-first/legacy-second resolution, unchanged private state/protocols, complete repository rename, and identical source/wheel behavior. |
| Tests and verification | 15% | Unit, contract, distribution, runtime, E2E, and available real-host acceptance evidence cover the changed workflows. |
| Documentation and maintainability | 10% | Consistent Agent Package/Plugin/Connector vocabulary, actionable help and recovery, centralized paths, and isolated host drivers. |

### Success criteria

1. A new user can run:

   ```bash
   uv tool install edgecitadel
   cd my-project
   edgecitadel install
   ```

   and receive guided create/join setup, agentd readiness, host detection,
   selected Plugin installation, and a precise verification summary.
2. Re-running the command performs no unnecessary mutation and reports current
   state.
3. `edgecitadel plugin install codex` installs through Codex's native plugin
   mechanism; Claude Code and Pi have equivalent drivers.
4. `edgecitadel plugin status` reports package installation separately from
   `edgecitadel connector status`, which reports registration/session state.
5. Existing `edgecitadel agent ...` behavior and stored Agent state remain
   unchanged across the directory rename.
6. A wheel installed into a clean environment contains the renamed assets and
   can create/join, start agentd, locate Agent Packages, and locate Plugins.
7. Documentation and errors use Agent Package, Plugin, and Connector consistently.

### Non-goals

- Installing or launching Codex, Claude Code, or Pi themselves.
- Making native Agent hosts run headlessly.
- Changing Agent Package manifest or NATS protocol versions.
- Renaming private state directories, credentials, database tables, or Python
  modules in the first release.
- Supporting every Agent host through one generic file-copy implementation.
- Automatically installing Gemma, Hermes, or Home Assistant during onboarding.

## 6. Proposed repository and package layout

```text
edge-research/
├── agent-packages/
│   ├── gemma/
│   ├── hermes/
│   ├── homeassistant/
│   └── examples/
│       ├── echo/
│       └── placeholder/
├── plugins/
│   ├── codex/
│   │   ├── .codex-plugin/plugin.json
│   │   ├── .mcp.json
│   │   └── skills/
│   ├── claude-code/
│   │   ├── .claude-plugin/plugin.json
│   │   ├── .mcp.json
│   │   ├── hooks/
│   │   └── skills/
│   ├── pi/
│   │   ├── package.json
│   │   ├── extensions/
│   │   └── skills/
│   └── marketplaces/
│       ├── codex.json
│       └── claude-code.json
└── agent-runtime/
    ├── pyproject.toml
    ├── schemas/
    ├── src/
    │   ├── edgecitadel_agentd/
    │   ├── edgecitadel_plugin_runtime/   # compatibility name in release 1
    │   ├── edgecitadel_plugin_sdk/       # compatibility name in release 1
    │   └── edgecitadel_supervisor/       # compatibility name in release 1
    └── tests/
```

The current nested Codex marketplace layout exists because its marketplace
source points at `./plugins/edgecitadel`. During the move, retain the host-required
package shape inside `plugins/codex/` even if the exact marketplace wrapper must
remain one level above it. The final physical layout must follow verified host
requirements rather than the illustrative tree above.

## 7. Responsibility contracts

| Actor | Owns | Requests | Reports | Must not do |
|---|---|---|---|---|
| Package manager (`uv`, `pipx`, `pip`, Homebrew) | EdgeCitadel distribution installation | Filesystem/package changes | Installed CLI version | Configure Agent hosts or enroll a fleet |
| `edgecitadel install` | Orchestration and user-visible progress | Existing create/join, service, and Plugin driver operations | Per-step outcome and recovery command | Hide prompts, install Agent Packages, or claim an inactive session is active |
| Plugin driver | One host's detect/install/status/remove contract | Native host CLI operations | Installed version, source path, scope, and next action | Edit unrelated host config or own connector state |
| Native host | Plugin package registration and active execution session | MCP process and hooks | Installed/active plugin state | Receive NATS credentials or write agentd state |
| Plugin | Host-native skills, tools, hooks, and MCP bridge | Authenticated agentd RPC | Session, task, result, and trace events | Supervise its host or access NATS directly |
| agentd | Connector identity/session and Agent lifecycle | Local RPC, NATS, process operations | Health, connector state, task/trace evidence | Modify native host plugin installation |
| Agent Package validator | Package structure, schema, compatibility, and lock integrity | Read-only package inspection | Deterministic inventory or errors | Execute untrusted package code during validation |

## 8. CLI and driver design

### 8.1 Command surface

```text
edgecitadel install [--create | --join <invitation>]
                    [--messaging-mode <single-client|nats_leaf>]
                    [--plugin <codex|claude-code|pi>]...
                    [--scope <user|project>]
                    [--yes] [--dry-run] [--json]

edgecitadel plugin install <codex|claude-code|pi>
                           [--scope <user|project>] [--dry-run] [--json]
edgecitadel plugin list [--json]
edgecitadel plugin status <host> [--json]
edgecitadel plugin repair <host> [--json]
edgecitadel plugin remove <host> [--json]

edgecitadel connector list|status|revoke ...
edgecitadel agent install|list|status|start|stop|logs|remove ...
```

`edgecitadel install` may be run from a project directory, but the current
directory does not silently imply project scope. The prompt and final summary
must state the selected scope. A driver rejects any scope outside its verified
host-specific set with an actionable explanation; Codex therefore rejects
`--scope project` in release 1.

### 8.2 Platform-driver contract

Introduce a small internal driver interface rather than branching throughout
the CLI:

```text
detect() -> absent | available(version)
plan(scope, packaged_source) -> ordered native operations
install(plan) -> installation evidence
status() -> absent | installed | stale | unknown
repair() -> installation evidence
remove() -> removal evidence
```

Each operation returns structured evidence before human formatting. The CLI owns
prompting, JSON output, dry-run behavior, and cross-driver reconciliation;
drivers own only host-specific commands and parsing.

### 8.3 Native-first policy

| Host | Supported scope | Preferred installation operation | Authoritative status |
|---|---|---|---|
| Codex | `user` | `codex plugin marketplace add <root> --json`, then `codex plugin add edgecitadel@edgecitadel --json` | `codex plugin marketplace list --json` plus `codex plugin list --available --json` |
| Claude Code | `user`, `project` | `claude plugin marketplace add <root> --scope <scope>`, then `claude plugin install edgecitadel@edgecitadel --scope <scope>` | `claude plugin marketplace list --json` plus `claude plugin list --json` |
| Pi | `user`, `project` | `pi install <package-dir>`; add `-l` for project scope | The matching package entry in global or project `settings.json`, cross-checked with `pi list` |

The driver passes absolute packaged paths. For Codex, `project` is rejected
because the observed CLI has no native scope flag. For Claude Code and Pi,
project scope is explicit and mutates `.claude/settings.json` or
`.pi/settings.json`, respectively; the confirmation plan must name that file.
The packaged sources are the bundled Codex marketplace root, Claude Code
marketplace root, and Pi package directory, never a caller-supplied path.

Direct skill copying is a future fallback for hosts without a native plugin
system. It is not the Codex/Claude/Pi default.

### 8.4 Execution, idempotency, and errors

- **Status first:** every mutation begins with `detect` and `status`. If the
  expected Plugin ID, scope, resolved source, and packaged version already match,
  return `unchanged` without invoking the native install command.
- **No shell:** resolve the executable once, retain that absolute path in the
  plan, and execute an argument array with `shell=False`.
- **One owner of retries:** EdgeCitadel does not retry native mutations. A native
  package manager may perform its own bounded network retries. An interrupted,
  timed-out, or nonzero operation is re-observed once and reported from the
  resulting authoritative state.
- **Timeout:** the first implementation uses a 120-second deadline per native
  command, matching the existing create-readiness default. Timeout terminates the
  child, records bounded stdout/stderr, and prints `plugin repair <host>`.
- **Cancellation:** SIGINT stops the current child, does not begin another host,
  and runs status reconciliation before returning exit code 130. It never removes
  an installation that predated the invocation.
- **Output bound:** retain at most the final 64 KiB from each stdout/stderr stream
  after redacting known credential-shaped values. Human mode prints a concise
  error; JSON embeds the bounded evidence.
- **Stable parsing:** parse JSON only when the host offers it. Missing or unknown
  fields produce `unknown`, not optimistic success. Pi status may read its
  documented settings JSON but must never edit that file directly.
- **Rollback journal:** record pre-state and successful native operations only in
  memory. Automatic rollback is off in release 1; the summary may offer a removal
  command only for a Plugin proven absent before this invocation and installed
  successfully by it.

## 9. Unified installation lifecycle

### 9.1 States

| State | Meaning | Durable evidence | Owner |
|---|---|---|---|
| `distribution_ready` | Executable and runtime assets resolve | Installed package metadata and asset paths | Package manager |
| `host_unconfigured` | No Core/Edge node record exists | Missing node configuration | CLI |
| `host_ready` | Create or join completed and agentd is ready | Node config, service health, admin credential | CLI/agentd |
| `plugin_available` | Native host executable is detected | Executable/version observation | Plugin driver |
| `plugin_installing` | Native operations are in progress | In-memory operation journal only | CLI |
| `plugin_installed` | Native host reports the package installed | Host-native package evidence | Native host |
| `plugin_stale` | Installed source/version no longer matches current assets | Host status plus packaged version/path | Plugin driver |
| `connector_inactive` | Plugin is installed but no live host session exists | Connector record or absence of lease | agentd |
| `connector_active` | Native host opened a renewable agentd session | Connector session lease | agentd |
| `degraded` | Local installation succeeded but Core/transport is unavailable | Health output and diagnostic event | agentd/CLI |

Legal Plugin transitions are deliberately derived from native state rather than
persisted as an EdgeCitadel state machine:

| From | Trigger and guard | Action | To | Restart/recovery evidence |
|---|---|---|---|---|
| `plugin_available` | Confirmed install; host, scope, and source are explicit | Execute the planned native operations once | `plugin_installed` or `unknown` | Re-run native status; never infer success from the subprocess exit code alone |
| `plugin_installed` | Status source, scope, path, or version differs | No mutation during status | `plugin_stale` | `plugin repair` re-registers the packaged source and re-observes status |
| `plugin_stale` | Confirmed explicit repair or unified-install reconciliation | Invoke the host's update/reinstall sequence once | `plugin_installed` or `unknown` | Preserve the prior installation on failure when the host supports atomic replacement |
| `plugin_installed` or `plugin_stale` | Explicit remove for the same host and scope | Invoke native removal once | `plugin_available` or `unknown` | Re-observe native status; leave Connector revocation as a separate explicit action |
| any Plugin state | Process restart or interrupted mutation | Read native status only | observed state | The ephemeral journal is lost safely because the native host is authoritative |

`unknown` is an observation result, not a durable lifecycle state. It blocks
automatic install, repair, and remove until the operator retries status or uses
the printed native recovery command.

### 9.2 Happy path

1. Verify distribution assets and supported Python/host prerequisites.
2. Inspect local node state. If absent, prompt to create a Core or join an Edge;
   non-interactive use requires `--create` or `--join`.
3. Start/verify agentd before changing host plugin state.
4. Detect supported native host executables and versions.
5. Present an explicit install plan: host, scope, source, commands, and whether
   the operation changes existing state.
6. After confirmation, invoke each selected native host driver.
7. Verify package registration using the host's own status/list operation.
8. Report each Plugin as installed, stale, skipped, or failed. Report Connector
   activity separately and explain that a new host session may be required.

### 9.3 Failure and recovery

| Failure | Required behavior | Recovery |
|---|---|---|
| No TTY and missing decisions | Make no changes and return a usage error | Re-run with explicit create/join, plugin, scope, and confirmation flags |
| agentd cannot become ready | Stop before native Plugin installation | Run `edgecitadel doctor` or `service status`, fix, and retry |
| One host CLI is missing | Skip only that host; do not install the host application | Install the host or select another Plugin |
| Native install command fails | Preserve captured stdout/stderr, mark that host failed, and stop by default | Run the printed native command or `plugin repair` |
| Multi-host installation partly succeeds | Stop before the next host, retain successes, and never remove a pre-existing Plugin | Optionally run the printed remove command only for a package proven newly installed by this invocation |
| Plugin installed but Connector inactive | Treat installation as successful with a pending activation step | Start a new native host session and check `connector status` |
| Core/NATS unavailable after local setup | Report `degraded`, preserving local configuration and installed Plugins | Restore transport and let agentd reconnect |
| Distribution upgrade moves asset path | Report `stale`; a confirmed unified install or `plugin repair` re-registers the current packaged source | Reconcile after every detected path/version mismatch |

The operation journal is ephemeral because the native host remains authoritative
for installed Plugin state. Do not add an EdgeCitadel database table merely to
duplicate another package manager's state.

## 10. Compatibility and migration strategy

### 10.1 Compatibility rules

- Preserve all existing private state paths, including `state_dir/plugins/`,
  `managed-agents.json`, connector token paths, and package IDs.
- Preserve `edgecitadel connector path <host>` for at least one minor release as
  an advanced/deprecated escape hatch.
- Resolve packaged assets through centralized helpers. During the compatibility
  release, search the new directory first and the old directory second:

  ```text
  Agent Packages: agent-packages/ -> plugins/
  Plugins:        plugins/        -> native-plugins/
  Agent runtime:  agent-runtime/  -> plugin-toolkit/
  ```

  The fallback is for mixed source checkouts and downstream packagers. It must
  emit a deprecation warning when the legacy layout is used.
- Do not expose the dual-path lookup inside individual drivers, validators, or
  service commands.
- Keep `ManagedAgent`, protocol identifiers, environment variables, and Python
  package imports stable in this release.

### 10.2 Rollout phases

#### Phase 0: Lock the behavioral baseline

- Add focused tests for current Agent installation, connector registration,
  native asset path resolution, service startup, and wheel contents.
- Capture CLI help/output snapshots that identify intentional wording changes.
- Verify a clean wheel in an isolated environment before any rename.

Gate: Existing tests pass and the baseline wheel can locate all three current
directory families.

#### Phase 1: Add vocabulary and commands without moving files

- Add the Plugin driver abstraction over the existing `native-plugins/` paths.
- Add `edgecitadel plugin ...` and `edgecitadel install`.
- Keep current host-specific documentation as a troubleshooting escape hatch.
- Make `plugin install` idempotent and add `--dry-run` and JSON results.
- Deprecate `connector path` in help without removing it.

Gate: Repeated mocked installs are no-ops; partial failure never removes
pre-existing host packages; real-host smoke tests pass for each supported host.

#### Phase 2: Perform the physical repository rename

- Move `plugins/` to `agent-packages/`.
- Move `native-plugins/` to `plugins/` and normalize host subdirectory names.
- Move `plugin-toolkit/` to `agent-runtime/` without renaming Python imports.
- Update root packaging, editable requirements, CI, Dockerfiles, test fixtures,
  docs, contributor commands, distribution entrypoint paths, and AGENTS.md in the
  same change.
- Keep centralized legacy path fallbacks.

Gate: Source checkout and clean-wheel workflows produce identical observable
behavior; no maintained file except compatibility tests/docs references the old
repository paths directly.

#### Phase 3: Make unified installation the primary onboarding path

- Change README and onboarding quick starts to `edgecitadel install`.
- Keep explicit `create`, `join`, `service`, `plugin`, `agent`, and `connector`
  commands documented for automation and recovery.
- Add `doctor` checks for Plugin installation, stale asset paths, native host
  version, agentd readiness, and Connector activity.

Gate: A clean-machine acceptance test completes installation using only the
package manager and `edgecitadel install`, then reports a newly opened native
host session as active.

#### Phase 4: Remove repository-path compatibility

- After at least one released compatibility window, remove old repository path
  fallbacks and `connector path` from primary help.
- Retain the command as a hidden compatibility alias only if downstream users
  still depend on it.
- Consider Python package and manifest-kind renames only as a separate versioned
  design with import/schema compatibility.

Gate: Published telemetry or explicit downstream audit finds no maintained use
of old repository paths; rollback documentation remains available.

### 10.3 Rollback

- Phase 1 rollback removes only the additive command/driver surface; existing
  manual host installation remains valid.
- Phase 2 rollback restores old source directory names and packaging mappings.
  It does not touch private state or uninstall Plugins/Agents.
- Recovery after a failed Plugin install may offer removal only for a package
  that the same invocation proved absent and then installed. Release 1 never
  performs that removal automatically.
- A failed unified onboarding run must not undo a successful Core creation or
  Edge join automatically. It prints the durable state reached and the exact
  next command.

## 11. Security and trust boundaries

- `edgecitadel install` executes external host CLIs. Show the resolved executable
  and planned arguments before confirmation; never construct commands through a
  shell string.
- Resolve host executables through explicit discovery and execute argument
  arrays without `shell=True`.
- Validate bundled Plugin roots remain within the installed distribution and
  contain the expected host manifest before invoking a native installer.
- Treat host CLI output as untrusted bounded text. Do not parse secrets or echo
  connector tokens.
- Plugin installation grants host-native capabilities; interactive output must
  name MCP, skills, and hooks before confirmation.
- User scope writes only the current host user's native package configuration.
  Project scope additionally requires a trusted project root and confirmation of
  the exact `.claude/settings.json` or `.pi/settings.json` target. Symlinked
  project roots and targets outside the resolved project root are rejected.
- The invoking operating-system user is the authorization principal. Managed or
  administrator-enforced host configuration is read-only to EdgeCitadel; a
  native permission/policy denial is reported without fallback mutation.
- Connector registration remains authenticated by agentd and does not pass NATS
  credentials to Plugins.
- `--yes` suppresses confirmation only when all choices are explicit; it must
  not imply auto-detection plus installation of every available host.

## 12. Observability and operations

Human and JSON output must use the same per-step model:

```text
step: distribution | enrollment | service | plugin | connector | transport
target: edgecitadel | core | agentd | codex | claude-code | pi | <connector-id>
state: absent | unchanged | planned | running | installed | stale | unknown |
       succeeded | skipped | degraded | failed
changed: true | false
evidence: bounded structured details
recovery_command: optional string
```

For `--json`, stdout contains one document and no progress text. The top-level
contract is `{schema_version: 1, command, ok, changed, steps: [...]}`; each step
contains exactly the fields above, with `evidence` as an object and
`recovery_command` as a string or `null`. Human progress and native child output
go to stderr. Unknown additional evidence fields are permitted, but changing or
removing a top-level or per-step field requires a schema-version change.

Exit codes are `0` when all requested steps are `unchanged`, `installed`,
`succeeded`, or explicitly `skipped`; `2` for usage, missing non-interactive
choices, unsupported scope, or rejected confirmation; `1` for operational
failure, `unknown`, or `degraded`; and `130` for SIGINT. Dry-run returns `0`
after a valid plan and `2` when the requested plan itself is invalid.

`edgecitadel doctor` should answer separately:

1. Is the distribution complete?
2. Is the host enrolled?
3. Is agentd ready?
4. Is each selected Plugin installed and current?
5. Does each Plugin have an active Connector session?
6. Is transport to the Core healthy?

### 12.1 Capacity, performance, and cost

This migration adds no server, queue, database, or background polling loop.
Installations are serialized and bounded to the explicitly selected hosts, so
traffic, concurrency, storage-growth, and cloud-cost models are not architecture
drivers. The relevant local bounds are the 120-second per-command deadline and
64 KiB captured-output limit in section 8.4; external host package downloads and
cache storage remain owned by the native package managers.

## 13. Test and validation plan

### Unit and contract tests

- CLI parser, TTY/non-TTY decision handling, JSON output, dry-run, and prompt
  confirmation.
- Driver detect/plan/install/status/repair/remove with mocked executables.
- Version-floor rejection and feature probing for every host CLI.
- Argument-array execution and rejection of missing/out-of-root assets.
- Same-version repeated installation is a no-op.
- Stale asset path becomes `plugin_stale` and is repaired.
- Malformed, oversized, and schema-changed native JSON yields `unknown` without
  mutation; captured output is bounded and secrets are redacted.
- Timeout and SIGINT reconcile native status and do not continue to another host.
- Project scope rejects symlink escapes and names the exact host settings file.
- Partial multi-host failure preserves all pre-existing packages.
- New-first/legacy-second asset resolution and deprecation warnings.
- Existing Managed Agent state and connector-token compatibility.

### Distribution and integration tests

- Build sdist and wheel; install the wheel in a clean virtual environment.
- Assert `agent-packages/`, `plugins/`, and `agent-runtime/` assets are present at
  the expected installed paths.
- Run `edgecitadel --version`, `doctor`, service startup smoke, Agent Package
  validation, Plugin dry-run, and uninstall/repair paths from the installed wheel.
- Run the root pytest suites, agent-runtime tests, frontend build/unit tests,
  deterministic Playwright suite, and the infrastructure verification gate.

### External acceptance tests

- Codex: native marketplace add/install/status, MCP start, Connector registration,
  uninstall, repeated install, and distribution-path repair.
- Claude Code: marketplace add/install/status, hooks/MCP discovery, Connector
  registration, uninstall, and repeated install.
- Pi: package install/status, extension/skill discovery, Connector registration,
  uninstall, and repeated install.
- Run each on a clean user profile and an existing profile containing unrelated
  plugins. Project scope is admitted only after a separate supported-scope test.

## 14. Risks and mitigations

| Risk | Impact | Mitigation | Release gate |
|---|---|---|---|
| Path inversion breaks wheel assets or service startup | EdgeCitadel cannot operate after upgrade | Central path resolver, clean-wheel test, service smoke, compatibility fallback | Blocking |
| Host CLI changes output or command syntax | Plugin driver misreports state | Minimum versions, machine-readable output where available, external acceptance suite | Blocking per host |
| Existing marketplace stores an obsolete distribution path | Plugin stops working after upgrade | Detect source mismatch and reconcile it in confirmed unified install or idempotent `plugin repair` | Blocking |
| Unified installer claims success before a session exists | User cannot tell whether integration works | Separate installed Plugin from active Connector in state and output | Blocking |
| Rollback removes an existing user Plugin | Destructive data/config loss | Pre-operation evidence and rollback only for newly installed package | Blocking |
| Renaming schemas/imports multiplies compatibility work | Large risky refactor | Explicitly defer protocol and Python import renames | Accepted/deferred |
| Project-scope semantics differ by host | Surprising global mutation | Explicit scope, exact target-file preview, and rejection for unsupported host/scope combinations | Blocking |

## 15. Alternatives considered

### Keep current directory names and change documentation only

Lowest implementation risk, but it preserves the central vocabulary error and
forces every contributor and user to learn that `plugins/` does not contain the
actual host plugins. Rejected.

### Copy Graphify's direct skill/config installation model for every host

This gives one highly portable installer and easy project scope, but it would
bypass native marketplace/plugin behavior, write host-owned instruction files,
and underrepresent EdgeCitadel's MCP/hooks/package requirements. Retain this only
as a future fallback for hosts without native plugin packaging. Rejected as the
default.

### Rename only `plugins/` to `managed-agents/`

This fixes the most visible directory but leaves package-versus-instance
ambiguity and retains `native-plugins/` plus `plugin-toolkit/`. It is a viable
conservative fallback if the full physical rename proves too disruptive, but it
does not produce the clean public taxonomy. Not recommended.

## 16. Implementation work breakdown

1. **Baseline tests:** lock current CLI, service, Agent installation, native path,
   and wheel behavior.
2. **Path resolver:** centralize source and installed distribution asset paths;
   add new/legacy lookup tests.
3. **Plugin drivers:** implement Codex, Claude Code, and Pi detection and plans,
   then install/status/repair/remove.
4. **Explicit Plugin CLI:** add `edgecitadel plugin ...`, structured results,
   dry-run, idempotency, and rollback journal.
5. **Unified installer:** compose distribution, enrollment, service, Plugin, and
   Connector checks behind `edgecitadel install`.
6. **Repository rename:** move the three directory families and update every
   packaging, CI, Docker, test, contributor, and documentation reference.
7. **Doctor and UX:** report Plugin installation and Connector activity as
   separate checks; make unified installation the primary quick start.
8. **Full verification:** run all repository gates plus real-host acceptance.
9. **Compatibility release:** publish with legacy path lookup and manual-command
   escape hatches.
10. **Deprecation audit:** collect downstream feedback and remove old path
    compatibility only in a later release.

## 17. Open questions

| Question | Owner | Decision deadline | Current default |
|---|---|---|---|
| Which exact Codex and Claude Code versions form the support floor? | Release owner | Before Phase 1 implementation merges | Codex 0.151.0 and Claude Code 2.1.150 are provisional evidence versions; Pi starts at the package's declared 0.84.4 peer floor. |
| Should interactive `edgecitadel install` detect hosts and ask, or require an explicit selection? | Product owner | Before unified-installer parser implementation | Detect and present available hosts, but install none until the user selects and confirms. Non-interactive mode always requires `--plugin`. |
| Should a Plugin failure stop subsequent selected Plugin installs? | Product owner | Before Phase 1 external acceptance | Stop on first failure; no `--continue-on-error` in release 1. |
| How long is repository-path compatibility retained? | Release owner | Before Phase 2 release | At least one minor release and until a downstream-use audit is clean. |

Resolved during design review: Claude Code and Pi support project-local native
package configuration; Codex 0.151.0 does not expose a project-scope plugin flag.
Release 1 therefore rejects only the unsupported host/scope combination rather
than reducing every host to user scope.

## 18. Recommended implementation boundary

The smallest safe first release is Phase 0 plus Phase 1: add the correct public
Plugin CLI and unified installer over the existing layout, without moving files
or changing protocols. Once real-host behavior is proven, perform the physical
directory rename as a separately reviewable Phase 2 change. This ordering tests
the product model before paying the migration cost and leaves the current manual
commands available at every rollback point.

## 19. Implementation record

Implemented on `feat/agent-package-plugin-installation` on 2026-09-03. The
compatibility release includes Phases 0 through 3 because the requested change
also required the physical repository migration. Phase 4 remains intentionally
deferred until at least one minor release and a downstream-use audit prove the
legacy layout can be removed.

- `scripts/installation_assets.py` centralizes new-first, legacy-second asset
  discovery and validates every bundled root.
- `scripts/plugin_installation.py` implements Codex, Claude Code, and Pi native
  package-manager drivers with status-first plans and structured evidence.
- `edgecitadel plugin ...`, `edgecitadel install`, and expanded `doctor` checks
  expose package installation separately from live Connector sessions.
- Repository roots are now `agent-packages/`, `plugins/`, and
  `agent-runtime/`; Python imports, manifest kinds, protocols, connector IDs,
  NATS subjects, SQLite state, and private installed-package paths are unchanged.
- The Python distribution, Homebrew formula, Docker contexts, CI, contributor
  commands, package locks, and onboarding documentation consume the new roots.

Verification evidence and strict-review scores are recorded in the merge
request so that runtime and CI status remain tied to the reviewed commit.
