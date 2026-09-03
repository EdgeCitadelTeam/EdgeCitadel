# Repository Cleanup and Migration Plan

Status: Implemented
Date: 2026-09-02
Scope: Entire `edge-research` repository, including tracked source, ignored local artifacts, documentation, developer-agent configuration, packaging, tests, and deployment helpers
Decision owner: EdgeCitadel maintainer
Required reviews: code, security, tests, and documentation

## Implementation record

Executed on 2026-09-02 on `feat/managed-agents-native-plugins`. The review
reconfirmed that the repository has no Git tags or GitHub releases establishing
a compatibility promise for the removed CLI and runtime surfaces.

The implementation differs from the proposal in two evidence-backed ways:

1. Echo and placeholder were also legacy packages, so both were migrated to
   `ManagedAgent` fixtures before schema/runtime retirement.
2. The duplicated `.claude/agents`, `.claude/rules`, and `.codex/agents`
   definitions had no active repository consumers. They were deleted instead of
   recreated as thin copies; Claude commands and skill links now defer to
   `AGENTS.md` and `.agents/skills/`.

Verification completed:

- root Python: 832 passed, 37 skipped;
- plugin toolkit: 572 passed, 3 skipped; its explicitly configured live-NATS
  cases: 2 passed; migrated package/schema/validator focus: 249 passed;
- frontend: lint passed, 63 tests passed, production build passed;
- E2E: 22 harness tests and 13 Playwright tests passed;
- backend compile, rebuilt Compose stack, NATS health, API status, Python
  formatting/lint, package build/install/content smoke, shell syntax, YAML parse,
  and Homebrew style passed.

The live pull-consumer fixture now skips immediately when `NATS_URL_TEST` is not
provided and provisions both recipient and sender inbox subjects when it is run
against a real broker.

## Executive decision

Remove artifacts that are demonstrably unreachable now, then migrate the two remaining legacy product integrations before removing their shared runtime. Keep `.claude/` as a supported tool integration, but reduce it to active platform-specific settings, commands, and shared-skill links; keep repository facts and workflows in `AGENTS.md` and `.agents/skills/`.

The migration is intentionally split into reversible changes. OpenClaw removal is independent of the legacy AgentPlugin removal. Hermes must become a Managed Agent and the installable Shell plugin must be separated from the deterministic `shell-1` test fixture before the legacy runtime can be retired. Existing SQLite and NATS state is never deleted by this plan.

## Outcome and success criteria

The cleanup is complete when all of the following are true:

1. Every tracked file belongs to a current product surface, test fixture, build/package input, deployment path, or maintained documentation/tool integration.
2. `join.sh`, `add-agent.sh`, `tmp/google-doc-export/`, the obsolete OpenClaw client path, and the installable unrestricted Shell plugin are absent.
3. Hermes is installed and run through the Managed Agent contract.
4. No user-facing `AgentPlugin`, `plugin runner`, `plugin` CLI alias, or `supervisor` CLI alias remains; a narrowly scoped legacy state reader may remain temporarily and must be named and tested as such.
5. `.claude/`, `.codex/`, `AGENTS.md`, contributor docs, and verification skills describe the same architecture and commands.
6. The wheel, Docker configuration, Homebrew formula, developer setup, and CI use the same supported entry points.
7. Repository-wide denylist searches return no stale references except documented compatibility allowlists.
8. Python tests, plugin-toolkit tests, frontend unit/build checks, deterministic Playwright, packaging smoke tests, and the relevant infrastructure verification gate pass.
9. Runtime data under `data/`, the legacy-compatible `openclaw.db` filename, agent history, and installed-agent state remain recoverable.

## Scope boundaries

### In scope

- Dead files, orphaned tests, unused dependencies, dead functions, and stale imports.
- Superseded wrappers and developer scripts.
- OpenClaw browser/client ingress and its unused authentication surface.
- Migration of Hermes from `AgentPlugin` to `ManagedAgent`.
- Removal of the installable Shell plugin after preserving deterministic test behavior.
- Removal of the legacy plugin runtime and user-facing compatibility aliases.
- Consolidation of Claude/Codex guidance and repair of stale documentation.
- Test discovery, local quality gates, CI coverage, package-content checks, and vault synchronization.

### Out of scope

- Renaming `data/openclaw.db`; the filename is persisted state and is independent of the removed OpenClaw transport.
- Removing MQTT support or changing the `agents.*`, `tasks.*`, or `system.*` subject contracts.
- Redesigning the envelope schema, Agent Card schema, agentd, or native-plugin protocol.
- Deleting any runtime data, secrets, local settings, or ignored user-authored design documents automatically.
- Renaming the internal `edgecitadel_supervisor` Python package solely for aesthetics.
- Publishing a release, pushing a branch, or opening a pull request.

## Evidence-backed baseline

| Finding | Status | Evidence and implication |
|---|---|---|
| The supported setup and enrollment entry point is `scripts/edgecitadel` | Implemented | Root instructions and current onboarding use `create`, `invite`, and `join`; root `join.sh` and `add-agent.sh` are unreferenced compatibility wrappers and are not packaged. |
| No public compatibility window is established | Implemented | The audit found no Git tags and no GitHub releases. Re-check immediately before executing destructive migration steps. |
| Native plugins and Managed Agents are the current integration direction | Implemented | Current architecture and root instructions identify these as the two public integration paths. |
| OpenClaw is not a coherent supported path | Implemented | The client is not launched or packaged; its required token is not provisioned by NATS; the login token store is written but never enforced; advertised HTTP dispatch is absent. |
| Hermes and Shell still depend on the legacy AgentPlugin runtime | Implemented | Their manifests use the legacy runtime/contract; therefore shared runtime deletion cannot precede their migration/removal. |
| `shell-1` is also a deterministic test identity | Implemented | E2E depends on that identity and behavior, independently of whether an installable Shell product package exists. |
| `.claude/` contains both current integration and stale repository facts | Implemented | Settings and skill links are active, while rules/commands/agent prompts reference removed ADRs, adapters, services, directories, and old messaging behavior. |
| Current quality gates have discovery gaps | Implemented | A frontend tooling contract is outside normal discovery, frontend unit tests are omitted from the commit gate, helper E2E suites are separate, and GitHub workflows only cover Claude automation. |
| Existing local runtime state may matter even without releases | Assumed | Preserve all data and provide a compatibility reader for installed legacy state; do not infer that absence of releases makes local state disposable. |

## Target ownership model

| Concern | Canonical owner after cleanup | Consumers/adapters |
|---|---|---|
| Repository architecture, commands, quality policy | `AGENTS.md` | `CLAUDE.md`, contributor docs, PR template |
| Repeatable verification workflows | `.agents/skills/verify-*`, `.agents/skills/commit-check` | Claude skill links, Codex runtime |
| Tool-specific permissions and hooks | `.claude/settings.json`, `.claude/settings.local.json` | Claude Code only |
| Tool-specific workflows | `.claude/commands/`, `.claude/skills/` | Thin platform integration with no duplicated architecture facts |
| Agent package lifecycle | `edgecitadel agent`, agentd, Managed Agent schema/runtime | Gemma, Home Assistant, migrated Hermes |
| Existing-host integrations | `native-plugins/` | Pi, Claude Code, Codex |
| Deterministic test agent | E2E-owned fixture runtime | `shell-1` identity retained only where tests require it |
| Messaging contracts | `schemas/`, NATS config, aggregator router | All agents and dashboard |
| Runtime state | External state directory and SQLite | Compatibility readers; never cleanup scripts |

## Repository category map

This is the disposition for every top-level source category found in the scan. The Phase 0 inventory expands each category to one row per file before deletion begins.

| Category | Purpose/functionality | Direction | Primary phase |
|---|---|---|---|
| Root entry points and configuration | Product CLI shim, Compose orchestration, Python packaging, contributor policy, environment template | Keep current entry points; delete compatibility wrappers; reconcile docs/config | 1-3 |
| `aggregator/` | FastAPI API, NATS routing, registry, persistence, WebSocket updates | Keep; remove OpenClaw route/subscription/token code; tighten dependencies | 1, 4 |
| `frontend/` | Only React/Vite dashboard source | Keep; remove unused dependency/orphan meta-test; run unit and build gates | 1, 3 |
| `openclaw-client/` | Experimental Node NATS client/browser ingress | Delete as one subsystem with server/config references | 4 |
| `plugin-toolkit/` | agentd, managed runtime, validation, schemas, SDK protocols, tests | Keep as canonical package infrastructure; retire only legacy runtime/protocol acceptance | 5, 7 |
| `plugins/` | Installable Managed Agent packages, legacy runtimes, examples | Keep Gemma, Home Assistant, and examples; migrate Hermes; remove Shell | 5-7 |
| `native-plugins/` | Native integrations for Pi, Claude Code, and Codex | Keep; update only stale cross-references and verification | 2, 9 |
| `edgecitadel/` | Python distribution entrypoint and build-time runtime assets | Keep; remove deprecated CLI aliases/assets and verify wheel contents | 4, 7, 9 |
| `scripts/` | Product CLI implementation, deployment tooling, checks | Keep supported commands; delete dead fixtures/A2A/worktree/legacy runner paths | 1, 7-8 |
| `schemas/` | Shared messaging and Agent Card contracts | Keep product contracts; delete the retired experiment schemas | 1, 3 |
| `tests/` | Root CLI, packaging, schema, and workflow regression coverage | Keep; consolidate dependency setup and migrate assertions with removed surfaces | 1-9 |
| `e2e/` | Deterministic Playwright and stack/lifecycle integration tests | Keep; delete obsolete env; make suite discovery/ownership explicit | 1, 3-7 |
| `deploy/` | Host provisioning, manifests, systemd, Homebrew | Keep; remove legacy Shell/plugin runtime deployment and repair stale docs | 2, 6-9 |
| `nats/` | NATS/JetStream/MQTT configuration and authorization | Keep; remove OpenClaw-only permissions/comments; do not change core subjects | 4, 9 |
| `nginx/` | Core HTTP/WebSocket ingress | Keep; remove only OpenClaw-specific route/config if present | 4, 9 |
| `docs/` | Maintained onboarding, architecture, operations, and migration record | Keep relevant content; repair missing paths and stale architecture; remove obsolete planning artifacts | 2, 9 |
| `.agents/` | Shared executable skills and verification procedures | Keep as workflow source of truth; update gates when commands change | 2-3, 9 |
| `.claude/` | Claude-specific permissions, hooks, commands, skill links | Keep active integration; remove local junk and unused duplicated policy files | 2 |
| `.codex/` | Codex project task state | Remove unused duplicated role configuration | 2 |
| `.github/` | Templates and automation | Keep; fix stale checklist and add actual hermetic CI coverage | 2-3 |
| `tmp/` | Accidental export staging | Delete tracked export artifact and keep temporary output ignored | 1 |
| `data/`, `nats/data/` | Runtime databases and broker state | Preserve; never include in automatic repository cleanup | All |

## Dependency order

```text
baseline and inventory
    |
    +--> immediate leaf deletion
    |
    +--> guidance and test-gate repair
    |
    +--> OpenClaw removal ------------------------------+
    |                                                   |
    +--> Hermes -> Managed Agent ----+                  |
    |                                +--> legacy runtime removal
    +--> Shell package -> fixture ---+                  |
                                                        |
    +--> developer-tool cleanup ------------------------+
                                                        |
                                                        v
                                             final consistency gate
```

OpenClaw removal does not depend on the AgentPlugin work. Legacy runtime removal depends on both Hermes migration and Shell product/fixture separation.

## Phase 0: Freeze the baseline and make the work auditable

Status: Implemented

### Changes

1. Create a feature branch dedicated to cleanup; do not execute the migration directly on `main`.
2. Record the starting commit, dirty-worktree state, package versions, current Git tags, and GitHub releases.
3. Generate a machine-readable inventory with, at minimum: path, tracked/ignored status, subsystem, purpose, runtime/build/test/docs reachability, disposition, dependency, and evidence.
4. Save baseline output for all existing test commands. Record expected failures separately; do not normalize a red baseline as success.
5. Back up the external state directory before any installer/state migration test. Never copy secrets into the repository.
6. Create denylist searches for removed concepts: `join.sh`, `add-agent.sh`, `tmp/google-doc-export`, `OPENCLAW_TOKEN`, `openclaw.`, `AgentPlugin`, `edgecitadel.plugin.v1`, `plugin_runner`, deleted ADR paths, deleted adapter paths, and `thoughts/`.

### Exit gate

- Inventory accounts for every tracked file.
- Baseline failures are explained and assigned to a phase.
- Runtime state backup location and restoration command are verified outside the repository.
- Tags/releases have been re-checked; any newly published compatibility contract pauses phases 4-7 for review.

### Rollback

No product changes occur in this phase. Delete only generated audit output if it contains no user data.

## Phase 1: Remove proven dead leaves and repair local hygiene

Status: Implemented

### Safe tracked deletions

| Path | Current purpose | Why safe now | Verification |
|---|---|---|---|
| `join.sh` | Deprecated enrollment wrapper | Unreferenced, unpackaged, superseded by `scripts/edgecitadel join` | CLI join tests and repository search |
| `add-agent.sh` | Deprecated setup wrapper | Unreferenced, unpackaged, superseded by `edgecitadel agent install` and invite/join flow | CLI tests and repository search |
| `tmp/google-doc-export/nats-agent-communication-sources.sanitized.docx` | Export staging artifact | Tracked temporary binary with no runtime or documentation references | Package-content test and repository search |
| `e2e/.env.test` | Old E2E environment | Encodes fixed ports and obsolete MQTT assumptions; current isolated runner owns config | Deterministic E2E |
| `tests/requirements.txt` | Old test dependency list | Superseded by `scripts/requirements-test.txt` and current setup instructions | Clean virtualenv setup and root tests |
| `scripts/research/fixtures/fake_actuator_agent.py` | Old research fixture | Unreferenced and superseded by the native deterministic control fixture | Research tests and experiment dry-run |
| `scripts/research/configs/schema/hardware.schema.json` | Orphan schema | Not referenced by campaign schemas, loaders, or tests | Schema-reference closure test |
| `scripts/research/configs/schema/network.schema.json` | Orphan schema | Not referenced by campaign schemas, loaders, or tests | Schema-reference closure test |
| `frontend/tests/tooling-contract.test.cjs` | Tooling meta-test | Not discovered or run; replace useful assertions in actual config/build checks before deletion | Frontend unit tests and production build |

### Safe code/dependency cleanup

- Remove unused `json` and `dataclasses.field` imports from `deploy/lib/run-checks.py`.
- Remove dead `_require_system_health` and `_publish_status` functions after confirming no dynamic imports.
- Remove the unused `fixture_args` parameter and its callers.
- Remove dead `_receipt_body` and `raises_control_flow` helpers if coverage and repository searches still show no callers.
- Remove `recharts` from the frontend manifest and regenerate the lockfile.
- Remove `pydantic-settings` from aggregator runtime requirements if import analysis remains empty.
- Move `httpx` to test-only requirements if only tests import it.
- Stop declaring `jsonschema` and `pyyaml` twice if the aggregator can consume the plugin-toolkit dependency boundary without creating an undeclared transitive dependency. If aggregator imports either directly, keep them explicit.

### Ignored local cleanup

The execution request supplied confirmation. `.DS_Store` files and the nested
`.claude/.claude/` directory were moved to
`/tmp/edgecitadel-local-junk-20260902`. `.claude/settings.local.json`,
Ralph-loop state, `data/openclaw.db`, `nats/data/`, and existing build caches
were preserved.

### Exit gate

- `ruff`/syntax checks are clean for changed Python.
- Root and schema tests pass.
- Frontend unit tests and production build pass after lock regeneration.
- A built wheel and source distribution contain none of the deleted files.
- Inventory rows for each deletion include direct search evidence.

### Rollback

Revert this phase as one commit. It contains no data migration.

## Phase 2: Consolidate instructions and documentation

Status: Implemented

### `.claude/` disposition

Keep the directory. It is an active Claude Code integration, not disposable project debris.

1. Keep `.claude/settings.json`, `.claude/settings.local.json` as an ignored local override, current hook scripts, and skill links that point to `.agents/skills/`.
2. Rewrite or delete every `.claude/rules/*` file that repeats repository architecture. Retain only path-scoped behavior that cannot live cleanly in root or nested `AGENTS.md`.
3. Rewrite `.claude/commands/plan.md` and review commands so maintained plans live directly under `docs/`, and so commands use current AGENTS instructions and verification skills. Remove references to obsolete planning frameworks, `thoughts/`, old gotcha files, and deleted ADRs.
4. Delete unused `.claude/agents/*`, `.claude/rules/*`, and `.codex/agents/*`
   duplicates. Shared architecture facts remain in `AGENTS.md`.
5. Keep the remaining commands and skill links small enough that repository-wide
   stale-reference scans provide the drift gate without a new generator.

### Repository documentation updates

- Correct the backend dev command to run from repository root as `uvicorn aggregator.main:app ...`; the documented `cd aggregator && uvicorn main:app` path fails relative imports.
- Update `AGENTS.md`, `CLAUDE.md`, `CONTRIBUTING.md`, the PR template, `.env.example`, deploy docs, backup docs, and package READMEs to match current commands and directories.
- Replace blanket “strict mypy” checklist language with the actual type gate or add a real configured type gate before requiring it.
- Remove references to deleted adapters, watchdog behavior, OpenClaw, removed specs/ADRs, and old Gemma paths.
- Declare that `edgecitadel agent` is the only package lifecycle command and `edgecitadel service` is the only local service command after phase 7.
- Update the Obsidian codebase pages and source manifest in the same change that makes facts stale.

### Exit gate

- All documented commands execute far enough to prove imports/argument parsing in a clean checkout.
- A link/path check finds no missing repository targets.
- No architecture statement is duplicated across `AGENTS.md` and tool-specific role/rule files unless it is an intentional one-line pointer.
- Documentation and vault pages distinguish Implemented, Proposed, and compatibility-only behavior.

### Rollback

Revert documentation and prompt changes independently of runtime changes.

## Phase 3: Make verification reflect the supported product

Status: Implemented

### Changes

1. Add frontend unit tests to `.agents/skills/commit-check`; keep `npm run build` as a separate gate.
2. Give E2E scripts explicit names and ownership:
   - `test:unit` for hermetic Node tests.
   - `test:playwright` for the isolated deterministic stack and Playwright suite.
   - `test:stack-integration` for lifecycle checks requiring a prepared Docker stack.
   - `test:external-plugins` only for real external services and credentials.
3. Make root instructions state exactly which suites are mandatory locally and which are environment-dependent.
4. Add a real GitHub CI workflow for hermetic Python, plugin-toolkit, frontend unit/build, package build/install, and configuration validation. Add deterministic Docker/Playwright only after measuring runner reliability; it must remain mandatory somewhere before merge.
5. Add package-content assertions for deprecated roots and secret/config files.
6. Add schema-reference closure and documentation-path checks so orphaned schemas and deleted-path drift are caught automatically.

### Exit gate

- A clean checkout can run every hermetic gate using documented setup commands.
- Test scripts do not silently omit files by naming/location convention.
- External integration tests skip only with an explicit reason and never make a required local gate appear green.

### Rollback

CI changes can be reverted without affecting runtime. Do not weaken existing local gates to make CI pass.

## Phase 4: Remove the obsolete OpenClaw surface atomically

Status: Implemented

### Preconditions

- Reconfirm no published release or known external OpenClaw consumer.
- Search running deployment definitions and operator docs for the client and `openclaw.*` subjects.
- Confirm native plugins/Managed Agents cover supported use cases.
- Record the current `openclaw.db` state path and prove the cleanup will not unlink or recreate it.

### Removal set

1. Delete `openclaw-client/` in full.
2. Remove the aggregator login endpoint, `_OPENCLAW_TOKENS`, OpenClaw ingress callback, NATS subscription, and focused tests from `aggregator/{main,aggregator}.py` and `aggregator/tests/test_api.py`.
3. Remove `OPENCLAW_TOKEN` from `.env.example`, `docker-compose.yml`, `scripts/edgecitadel_cli.py`, generated environment logic, checks, and their tests.
4. Remove `openclaw.*` permissions/comments from `nats/nats.conf` and `nats/nats.conf.tpl`.
5. Remove stale references from `AGENTS.md`, `CONTRIBUTING.md`, `deploy/backup/**`, `deploy/lib/checks.yaml`, `docs/architecture/**`, E2E Compose, and frontend package metadata. Inspect Nginx for a route, but do not manufacture a change if none exists.
6. Rename the frontend npm package only if it is still named for OpenClaw and the lockfile update is isolated; this is source metadata, not runtime state.
7. Retain the `openclaw.db` filename with a compatibility comment and an explicit allowlist entry. A future state migration may rename it with backup, dual-read, and rollback support.

### Security and failure behavior

Removal closes an unused token-issuing endpoint and an unauthenticated/unenforced ingress concept. Failure should be fail-closed: no wildcard `openclaw.>` subscription or route may survive without authorization and tests.

### Exit gate

- Repository-wide `rg -i openclaw` returns only the legacy SQLite filename and its documented compatibility test.
- No service opens or subscribes to an OpenClaw subject.
- Core dashboard command flow, agent inbox/outbox flow, WebSocket updates, and native-plugin connection tests pass.
- Wheel and container inspection show no `openclaw-client` assets.

### Rollback

Revert the atomic removal commit. No schema or data conversion is involved; the existing DB remains untouched.

## Phase 5: Migrate Hermes to Managed Agent

Status: Implemented

### Target contract

- Keep the stable Hermes package ID and agent ID so dashboard history and installed-state identity remain continuous.
- Convert its manifest to the current `v1alpha2` `ManagedAgent` form.
- Use runtime kind `service_adapter` and protocol `managed-agent.v1` under agentd ownership.
- Replace direct `HERMES_TOKEN` environment injection with a `HERMES_TOKEN_FILE` secret-file contract, matching the Home Assistant boundary.
- Preserve only Hermes-specific process/protocol code; use toolkit lifecycle, health, logging, and identity primitives.

### State migration

1. Add fixture coverage for an installed legacy Hermes record.
2. On install/upgrade of the same package identity, translate the record to Managed Agent state while retaining enabled/disabled intent, agent ID, history references, and safe configuration.
3. Never serialize the token into manifests, lockfiles, logs, receipts, or SQLite.
4. Fail before switching the active record if validation, secret-file access, or managed runtime startup fails.
5. Keep the previous package directory until the new agent is healthy; then retire it through the normal managed-package cleanup path.

### Exit gate

- Manifest/lock validation and all plugin-toolkit tests pass.
- Upgrade, fresh install, enable/disable, restart, health, log redaction, and rollback fixtures pass.
- A live Hermes smoke test passes when credentials/service are available; otherwise the migration cannot be called production-verified and must remain behind an explicit compatibility flag.
- No Hermes code imports or invokes `edgecitadel.plugin.v1`.

### Rollback

Before activation, rollback is automatic because the old record remains authoritative. After activation, restore the backed-up installed-state record and package directory; secret files and agent identity do not change.

## Phase 6: Remove the Shell product package while preserving test behavior

Status: Implemented

### Decision

Delete the installable unrestricted Shell AgentPlugin. Preserve `shell-1` only as a deterministic, in-test native fixture identity where E2E assertions require it. An agent identifier is not evidence that a product package must remain.

### Changes

1. Move any unique deterministic behaviors required by tests into the E2E-owned native control fixture.
2. Prove the owned test stack launches that fixture directly and does not install `plugins/shell`.
3. Update test prose and fixtures to label `shell-1` as non-production. Generic toolkit tests may use a neutral `worker-1` identifier when identity is irrelevant.
4. Delete `plugins/shell/`, Shell runtime tests, deploy manifest entries, and systemd templates that install or launch the product package.
5. Add a packaging/security assertion that no installable package requests unrestricted host-shell execution.

### Exit gate

- Deterministic E2E passes with the product package absent.
- No install, deploy, or onboarding path presents Shell as a supported agent.
- Wheel/package inspection finds no Shell manifest/runtime or unrestricted sandbox declaration.
- Test-only fixture boundaries prevent real operator commands from reaching a host shell.

### Rollback

Revert the package removal commit. Test fixture changes should be a preceding commit and remain valid with or without the package.

## Phase 7: Retire the legacy AgentPlugin runtime

Status: Implemented

### Preconditions

- Hermes is healthy as a Managed Agent.
- The installable Shell package is gone and all tests use the fixture boundary.
- No package manifest in `plugins/` declares `AgentPlugin` or `edgecitadel.plugin.v1`.
- Current installed-state fixtures prove the compatibility reader can identify and report legacy records without launching them.

### Removal set

1. Remove `scripts/plugin_runner.py`, its tests, package inclusion, and documentation.
2. Remove the legacy protocol implementation and schema acceptance for new `AgentPlugin` packages.
3. Remove legacy deploy virtualenv/setup scripts, Shell service templates, manifest entries, and render logic. Renumber deploy phases only if phase discovery requires it; otherwise keep stable phase identifiers with a documented new purpose.
4. Remove public `edgecitadel plugin` and `edgecitadel supervisor` aliases, alias tests, and duplicated help text.
5. Rename internal command functions to the supported `agent` and `service` vocabulary where this reduces confusion without renaming stable state files.
6. Keep a small, read-only installed-state migration reader if existing local records require it. It may classify, export, or translate state but must never launch legacy code. Mark it with removal criteria and a target release/date.
7. Keep the `edgecitadel_supervisor` module namespace for now if renaming would add package churn without removing behavior.

### Exit gate

- `rg 'AgentPlugin|edgecitadel\.plugin\.v1|plugin_runner|edgecitadel plugin|edgecitadel supervisor'` is empty outside the named state-compatibility reader, migration fixtures, and historical migration note.
- New legacy manifests fail validation with a clear migration message.
- Existing-state fixtures can be inspected/exported without importing or executing legacy runtime code.
- Managed Agent, native plugin, deployment, package validation, and onboarding suites pass.
- Full infrastructure verification passes.

### Rollback

Revert runtime removal while leaving migrated packages on their current contracts. Because the phase does not rewrite runtime data destructively, rollback restores the old launcher without downgrading Hermes.

## Phase 8: Remove or replace stale developer tooling

Status: Implemented

### Worktree scripts

Delete `scripts/worktree-create.sh`, `scripts/worktree-resume.sh`, and `scripts/worktree-cleanup.sh` unless a current workflow owner demonstrates a required capability. They use `.claude/worktrees` while `.gitignore` names `.worktrees`, install ambient dependencies, and include forceful local/remote branch deletion. Document native `git worktree add`, `git worktree remove`, and explicit branch cleanup instead. Never automate remote deletion.

### A2A schema updater

Delete `scripts/update-a2a-schema.sh`. Its source URL is dead, it references a deleted ADR, and the [upstream A2A JSON guidance](https://github.com/a2aproject/A2A/blob/main/specification/json/README.md) documents the JSON artifact as generated from protobuf rather than a committed canonical file. Treat the EdgeCitadel envelope/Agent Card schemas as project-owned contracts with documented A2A alignment. If exact upstream conformance becomes a requirement, add a pinned protobuf/toolchain generation job as a separate design, not a curl-to-moving-URL script.

### Generated and cache directories

Keep ignores for build output, node modules, virtualenvs, caches, and test results. Provide an optional, target-specific cache cleanup command, but never include `data/`, a repository root, `$HOME`, or unresolved globs. Local cleanup requires an exact preview and confirmation.

### Exit gate

- No maintained workflow invokes the deleted scripts.
- Contributor docs show safe native replacements.
- `.gitignore`, tool settings, and actual generated paths agree.
- No cleanup helper can delete a branch remotely or erase runtime data implicitly.

## Phase 9: Final consistency, packaging, and documentation gate

Status: Implemented

### Required verification

Run from a clean checkout with documented tool versions:

1. Root Python syntax, lint, and test suite.
2. Aggregator verification skill, including runtime smoke when the stack is available.
3. Plugin-toolkit full contributor gate, manifest/lock validation, and migration fixtures.
4. Frontend unit tests and production build.
5. Deterministic isolated Playwright suite.
6. Infrastructure verification after Compose/Nginx/NATS/deploy changes.
7. `python -m build`, install the wheel in a fresh virtualenv, run CLI help/setup smoke, and inspect archive contents.
8. Homebrew formula style and source-install smoke.
9. Repository-wide stale-reference, broken-path, secret, and package-content scans.
10. Code, test, security, and documentation reviews of the final diff.

### Denylist with explicit allowlists

| Term/path | Expected final result |
|---|---|
| `join.sh`, `add-agent.sh`, `tmp/google-doc-export` | No matches |
| `OPENCLAW_TOKEN`, `openclaw.`, `openclaw-client` | No matches |
| `openclaw.db` | Only state-path definition, compatibility comment, and focused test |
| `AgentPlugin`, `edgecitadel.plugin.v1`, `plugin_runner` | Only state migration reader/fixture and historical migration note, until its removal criterion is met |
| `plugins/shell`, unrestricted host-shell package | No product/package matches |
| Deleted ADR/adapter/spec paths, `thoughts/` | No active documentation or agent-config matches |
| `HERMES_TOKEN` | No direct secret value contract; only `HERMES_TOKEN_FILE` and redaction tests |

## Superseding cleanup: retire the lab/research harness

Status: Implemented

The experiment harness was not part of the supported product direction and made
the generic Python CI job depend on a separately provisioned multi-container
environment. Remove `scripts/research/`, `tests/research/`, the lab-only backend
router, and the three research evidence schemas. Remove their packaging,
dependency, ignore, and contributor-command references.

Preserve only capabilities still required by maintained tests:

- move the deterministic `shell-1` fixture and its EdgeCitadel transport into
  `e2e/fixture_agent/`;
- move the reusable owned NATS server helper into `tests/nats_server.py`;
- keep the digest-pinned NATS image under the owning E2E/test configuration.

The result must have no production import or package dependency on the retired
harness. Root Python tests, package builds, and the isolated E2E suite are the
replacement gates.

### Completion record

Update `AGENTS.md` for changed commands/directories/gates, regenerate the inventory with final dispositions, sync the plan and implemented facts into the Obsidian vault, and record any compatibility allowlist with an owner and removal date.

## Pull request decomposition

| PR | Contents | Depends on | Independent rollback |
|---|---|---|---|
| 1. Dead leaves and dependency hygiene | Phase 1 tracked deletions, dead code, requirements/lock cleanup | Baseline | Yes |
| 2. Guidance and verification | Phases 2-3; agent config, docs, test discovery, CI | Baseline; may follow PR 1 paths | Yes |
| 3. OpenClaw removal | Phase 4 complete atomic cluster | PR 2 for accurate gates | Yes |
| 4. Hermes Managed Agent migration | Phase 5 with state/secret migration tests | PR 2 | Yes |
| 5. Shell fixture separation | First half of phase 6, proving fixture independence | PR 2 | Yes |
| 6. Shell product removal | Remaining phase 6 | PR 5 | Yes |
| 7. Legacy runtime retirement | Phase 7 | PRs 4 and 6 | Yes, without downgrading migrated packages |
| 8. Developer tooling and final consistency | Phases 8-9 | All applicable prior PRs | Yes |

Do not combine PRs 4-7 into one unreviewable deletion. Cross-subsystem consistency still belongs in the PR that changes the relevant public contract.

## Risk register

| Risk | Likelihood | Impact | Mitigation | Detection |
|---|---|---|---|---|
| Hidden external OpenClaw consumer | Low, based on no releases/references | High | Recheck releases/deployments; announce removal before execution if consumers exist | Subject monitoring and deployment search |
| Hermes upgrade loses state or secret | Medium | High | Same identity, file secret, staged activation, backed-up record, rollback fixture | Upgrade and log-redaction tests; live smoke |
| Shell package removal breaks E2E | Medium | High | Separate fixture first; keep `shell-1` test identity | Deterministic E2E |
| Legacy state becomes unreadable | Medium | High | Read-only migration reader; fixture from real record shape; no destructive rewrite | Clean-install and upgrade tests |
| Package omits a required asset or includes debris | Medium | Medium | Build/install/archive inspection in CI | Package-content assertions |
| Docs/tool prompts drift again | High | Medium | Single ownership model; thin adapters; stale-path check | Documentation/config test |
| CI becomes slow or flaky | Medium | Medium | Separate hermetic, deterministic stack, and external suites | Runtime/flake tracking; explicit suite status |
| Cleanup deletes ignored user work or runtime data | Low if plan followed | Critical | Never automate ignored substantive/data deletion; exact preview and confirmation | Dirty/ignored inventory and backup check |
| Subject/schema changes accidentally broaden scope | Low | High | No core subject/envelope changes; focused diff review | Contract tests and NATS config diff |

## File disposition summary

### Delete without migration after Phase 0 evidence is captured

- `join.sh`
- `add-agent.sh`
- `tmp/google-doc-export/nats-agent-communication-sources.sanitized.docx`
- `e2e/.env.test`
- `tests/requirements.txt`
- Retired lab/research harness files listed in the superseding cleanup section
- `frontend/tests/tooling-contract.test.cjs`

### Delete only as a coordinated subsystem migration

- `openclaw-client/**` plus all server/config/docs/token references: Phase 4.
- `plugins/hermes` legacy manifest/runtime: replace in place with Managed Agent form in Phase 5.
- `plugins/shell/**` and its deployment surface: Phase 6 after fixture separation.
- `scripts/plugin_runner.py`, legacy protocol/schema paths, deploy support, and CLI aliases: Phase 7.
- Worktree helpers and `scripts/update-a2a-schema.sh`: Phase 8 after replacement documentation.

### Keep, but rewrite or narrow

- `.claude/settings.json`, active hooks, and skill links.
- `.claude/agents/**`, `.claude/commands/**`, `.claude/rules/**` only where a current tool-specific responsibility exists.
- `.codex/agents/**` as thin role definitions.
- `AGENTS.md`, `CLAUDE.md`, contributor docs, PR template, `.env.example`, deploy docs, and vault pages.
- Legacy installed-state reader, temporarily and read-only, with removal criteria.
- `data/openclaw.db` filename as a compatibility-only state path.
- Deterministic `shell-1` fixture identity where tests require it.

### Never delete automatically

- `data/**`, `nats/data/**`, operator databases, logs, or installed-agent state.
- `.claude/settings.local.json` or any local secrets.
- User-created untracked files not explicitly classified and confirmed.

## Open decisions before execution

1. Confirm whether any operator still depends on Hermes in production and can provide a live migration smoke environment. Default: migrate, do not delete.
2. Confirm whether any private deployment uses OpenClaw despite the absence of repository/package integration. Default: remove after deployment search.
3. Choose the removal criterion for the read-only legacy state reader: one tagged release after migration, a dated deadline, or zero discovered legacy records across known hosts. Recommended: one tagged release plus zero known records.
4. Decide whether deterministic Docker/Playwright runs in GitHub CI or remains a required protected local/pre-merge gate until runner stability is proven. It must not become optional.

## Final simplification check

The target has two agent paths, not three: Managed Agents for EdgeCitadel-owned runtimes and native plugins for existing hosts. It has one onboarding CLI, one package lifecycle CLI, one local service CLI, one repository policy source, and explicit platform adapters. Compatibility code is limited to reading state that cannot yet be discarded; it does not preserve dead runtime behavior, endpoints, subjects, or installable packages.
