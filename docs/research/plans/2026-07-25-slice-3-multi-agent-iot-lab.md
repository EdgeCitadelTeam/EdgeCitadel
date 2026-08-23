# Slice 3 Multi-Agent IoT Lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement R-09 as a clean-checkout, isolated controller and two-node
Linux gateway lab with deterministic command, reconnect, Playwright, evidence,
and teardown gates.

**Architecture:** A run-scoped controller uses the Slice 1
`ArtifactEnvironment` and its raw mode-0600 token to own an exact Compose
project. A separate private service-env file adapts that token for NATS and the
aggregator without changing the Slice 1 fixture credential contract. The
controller prebuilds every local image, starts Compose only by immutable image
ID/repo digest, appends reservation and command observations, and persists an
atomic recovery journal so a new CLI process is the single evidence finalizer.
Each node reserves one logical agent ID and runs the Slice 1 `native_control`
fixture by immutable Docker image ID. Same-host automation proves R-09 but
always remains preliminary; remote qualification is a post-cleanup
classification of a valid lab manifest containing independent machine
fingerprints, a server-observed cross-host route, commands to both hosts, and an
ordered disconnect/queue/reconnect result.

**Tech Stack:** Python 3.12 in the hashed Slice 1 research lock, FastAPI, SQLite,
Docker Engine, Docker Compose v2, NATS 2.10 JetStream, pytest, Node 24.6.0,
npm 11.5.1, Playwright 1.58.2 from `e2e/package-lock.json`, JSON Schema Draft
2020-12.

---

## Global Constraints

- Implement only requirement **R-09** from
  `docs/research/task-aware-reliability-contract-design.md`.
- Slice 1 must be complete first. Consume its public fixture, preflight,
  ownership, evidence, and checker contracts. Do not create a second fixture,
  preflight report, artifact owner, JSON serializer, manifest finalizer, or
  artifact checker.
- Evidence code may import only `write_json` and `finalize_bundle` from
  `scripts.research.evidence`. Runtime qualification code imports only
  `check_bundle` from `scripts.research.check_artifact` and consumes its
  post-Slice-2 `CheckReport`. `check_artifact.py` dispatches to the shared,
  transport-free predicates in `scripts.research.lab_contract`; there is no
  second finalizer or checker CLI. The checker CLI always supplies `--bundle`,
  `--require-kind lab`, and the absolute `--source-root`.
- `credential_file` values are `Path` objects in Python interfaces. Pass a
  `Path`, not `str(path)`, to `PreflightRequest`.
- `ArtifactEnvironment.owned_resources()` is
  `tuple[OwnedResource, ...]`. Read `resource.kind` and `resource.name`; never
  index it as a dictionary or expect a `"resources"` key.
- The controller and node runtime is exactly Ubuntu 24.04 LTS, x86_64, Docker
  Engine with Compose v2, Python 3.12, Git, and a trusted experiment LAN or
  Tailnet. The browser evidence host additionally requires exact Node 24.6.0 and
  npm 11.5.1; the locked bootstrap below fails closed on other versions. Node
  and npm are not node-runtime dependencies.
- Slice 1 Task 0 is the only Python bootstrap. Before Task 1 RED, verify and use
  its hash-locked launcher for every repository Python command in this plan:

  ```bash
  cd "$TASK_ROOT"
  test "$(uv --version)" = "uv 0.8.13"
  scripts/research/run-python -c \
    'import sys; assert sys.version_info[:2] == (3, 12)'
  scripts/research/run-python -c \
    'import dotenv, fastapi, httpx, jsonschema, nats, pytest, uvicorn'
  test "$(node --version)" = "v24.6.0"
  test "$(npm --version)" = "11.5.1"
  npm --prefix e2e ci
  test "$(node -p "require('./e2e/node_modules/@playwright/test/package.json').version")" \
    = "1.58.2"
  npm --prefix e2e exec -- playwright install chromium
  ```

  `run-python` verifies `scripts/research/requirements.lock.txt` hashes, creates
  its managed Python 3.12 environment outside the checkout, and synchronizes it
  before execution. Do not create a Slice 3 venv or call a host `python3`.
  Node/npm checks are required only for Tasks 5-8. Local controller/node
  commands use `scripts/research/run-python`; remote commands use the identical
  checkout-local launcher after commit and source-snapshot equality checks.
- The fixture never runs through an arbitrary host `sys.executable`. Build the
  digest-pinned Slice 1 `scripts/research/Dockerfile`, resolve its immutable
  `sha256:<64-hex>` image ID, persist that ID, and pass the ID, not a mutable tag,
  to every `docker create`.
- Fixture configuration contains `"crash_point": null` when no crash is
  requested. The container environment contains exactly `NATS_URL` and
  `EC_CREDENTIAL_FILE`. The mounted fixture credential is exactly one raw token
  plus `\n`, matching Slice 2. A separate `service.env` contains exactly
  `NATS_TOKEN=<same-token>\n`; it is never mounted as `EC_CREDENTIAL_FILE`.
- Logical agent IDs remain the registry and Playwright targets. The
  run-qualified ownership identity is `<run-id>--<agent-id>` and must be at most
  64 characters. The Slice 1 fixture receives both the logical `agent_id` and
  `run_id`, so its durable identity remains run-specific.
- A controller `start` failure, failed preflight, node reservation failure,
  Docker create/start failure, or readiness failure rolls back every resource
  acquired by that attempt. Controller state is an atomic recovery journal;
  `stop` resumes `starting`, `active`, `stopping`, or `failed` state and a
  second `stop` of `stopped` state performs no Docker or finalizer mutation. An
  active reservation is always rejected, including a retry by the same
  reservation ID. Only an explicitly retained reservation may be resumed by its
  original reservation ID and declared host.
- Credential errors name the file and line number only. They never include a
  malformed line, token fragment, authorization header, or file contents.
- State JSON, config JSON, logs, process arguments, Compose evidence, and
  manifests never contain a credential value or authorization header.
- Finalized evidence is portable. Before any retained JSON/YAML write, replace
  the exact source root, run state root, credential/config paths, evidence root,
  and remote scratch root, longest first, with `$SOURCE_ROOT`, `<run-state>`,
  `<credential-file>`, `$EVIDENCE_DIR`, and `<remote-state>`. Live controller
  state remains outside the bundle. The lab checker rejects any retained
  absolute checkout, task-worktree, `/tmp`, `/var/folders`, or Windows path.
- A same-machine run can never produce `REMOTE QUALIFIED`, even if it uses two
  checkout paths, two declared host names, two interfaces, or two addresses.
- Node containers run only on the declared Ubuntu Linux hosts with
  `--network host`. On a same host, a loopback `ControllerConfig.nats_url`
  therefore reaches the controller's Docker-published host port from both node
  containers. A node on a different machine must receive a non-loopback
  advertised NATS URL; a loopback URL plus a different machine fingerprint is
  rejected before reservation or Docker create. Docker Desktop host-network
  behavior is outside this artifact and may not be used as qualifying evidence.
- Remote qualification requires all five items in one finalized, valid bundle:
  two explicit declared host IDs, distinct machine fingerprints, an observed
  non-loopback cross-host path, a successful command to one agent on each host,
  and a remote disconnect/accepted queued command/reconnect/terminal result.
- Use concrete examples throughout: run `ec-remote-01`, controller host ID
  `controller-lab-01` at `100.64.10.10`, remote host ID `gateway-lab-02` at
  `100.64.10.11`, and interface `tailscale0`.
- Reuse Slice 2 exactly. From `e2e/`, `playwright.config.js` requires
  `APP_URL` and `AGG_URL`, uses one `chromium` project, and targets
  `tests/operator-journey.spec.js` with registry agent `shell-1`. The evidence
  config is `playwright.evidence.config.js`, additionally requires
  `EVIDENCE_DIR`, and runs exactly two projects, `desktop` and `mobile`, against
  one shared stack.
- Default operator-spec execution is exactly one test. Evidence
  operator-spec execution is exactly two tests, one desktop and one mobile,
  with two distinct task IDs. The complete Slice 2 default gate is exactly 13
  tests.
- `EVIDENCE_DIR` always names the bundle root. After the two-project test,
  Slice 3 reuses Slice 2
  `passed_project_results()` and `copy_media()` from
  `scripts.research.capture_operator_journey`; it does not duplicate media
  discovery or leave trace/video files in `e2e/test-results`.
- Accepted source provenance is captured before any run directory or evidence
  file is created. It hashes the exact `LAB_SOURCE_PATHS` declared in Task 1 and
  requires those paths clean. Generated `docs/research/results/**` and
  `tmp/research/**` paths can never affect the source dirty flag or snapshot.
- Compose services use the exact Slice 1 ownership labels
  `ai.edgecitadel.owner=artifact` and
  `ai.edgecitadel.run-id=<run-id>`. Node containers use the separate
  `research-lab-node` label and are removed by the node owner before controller
  cleanup.
- Every task follows RED, GREEN, regression, then commit. Invoke
  `commit-check` immediately before every commit, stage only that task's files,
  and require `git diff --cached --check` to exit zero.
- No gate may wait for stdin. Remote automation requires pre-provisioned SSH
  keys and known-host entries and always passes `BatchMode=yes` and
  `StrictHostKeyChecking=yes`. Stage only each task's exact file map with
  noninteractive `git add --`; never use an interactive patch selector.
- Use `deliberate-changes` before Tasks 2 and 3. Invoke `verify-backend` after
  Task 2 and `verify-infra` as the final verification in Task 8.
- Continue the exact Slice 1/2 autonomous chain without creating another branch
  or worktree. Before Task 1, derive the persistent sibling handoff from the
  canonical checkout's unchanged base, source it, and fail closed unless the
  current process is in its clean task root at its recorded branch and final
  commit:

  ```bash
  set -euo pipefail
  test -n "${BASH_VERSION:-}"
  CANONICAL_ROOT=/Users/yefanzhang/workplace/edge-research
  EXPECTED_BASE="$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
  CHAIN_KEY="$(printf '%s' "$EXPECTED_BASE" | cut -c1-12)"
  CHAIN_ROOT="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY"
  HANDOFF="$CHAIN_ROOT/handoff.env"
  CANONICAL_SNAPSHOT="$CHAIN_ROOT/canonical"
  test -f "$HANDOFF"
  # shellcheck disable=SC1090
  source "$HANDOFF"
  test "$CANONICAL_ROOT" = /Users/yefanzhang/workplace/edge-research
  test "$CANONICAL_BASE" = "$EXPECTED_BASE"
  test "$(git rev-parse --show-toplevel)" = "$TASK_ROOT"
  test "$(git branch --show-current)" = "$BRANCH"
  test "$(git rev-parse HEAD)" = "$FINAL_COMMIT"
  test -z "$(git status --porcelain --untracked-files=all)"

  verify_canonical_snapshot() (
    set -euo pipefail
    CHECK_ROOT="$(mktemp -d "$CHAIN_ROOT/slice3-canonical.XXXXXX")"
    trap 'rm -rf "$CHECK_ROOT"' EXIT
    for suffix in index-tree unstaged.patch staged.patch status.z \
      untracked.z untracked.sha256; do
      test -f "$CANONICAL_SNAPSHOT.$suffix"
    done
    git -C "$CANONICAL_ROOT" write-tree >"$CHECK_ROOT/index-tree"
    git -C "$CANONICAL_ROOT" diff --binary >"$CHECK_ROOT/unstaged.patch"
    git -C "$CANONICAL_ROOT" diff --cached --binary \
      >"$CHECK_ROOT/staged.patch"
    git -C "$CANONICAL_ROOT" status \
      --porcelain=v2 -z --untracked-files=all >"$CHECK_ROOT/status.z"
    git -C "$CANONICAL_ROOT" ls-files \
      --others --exclude-standard -z >"$CHECK_ROOT/untracked.z"
    while IFS= read -r -d '' relative; do
      test -f "$CANONICAL_ROOT/$relative"
      digest="$(shasum -a 256 "$CANONICAL_ROOT/$relative" | awk '{print $1}')"
      printf '%s  %q\n' "$digest" "$relative"
    done <"$CHECK_ROOT/untracked.z" >"$CHECK_ROOT/untracked.sha256"
    for suffix in index-tree unstaged.patch staged.patch status.z \
      untracked.z untracked.sha256; do
      cmp -s "$CANONICAL_SNAPSHOT.$suffix" "$CHECK_ROOT/$suffix"
    done
  )
  verify_canonical_snapshot
  cd "$TASK_ROOT"
  ```

  Run every subsequent repository command from `TASK_ROOT`; each literal
  canonical checkout path below means that recorded root. Keep the same
  `BRANCH`, leave the canonical checkout byte-for-byte untouched, and never
  fast-forward, cherry-pick, merge, stash, reset, create a competing worktree,
  or ask for a destination. At the end, atomically advance only
  `FINAL_COMMIT` in this same `handoff.env`; Slice 4 consumes it.
- The clean task worktree must never contain canonical-checkout user edits.
  Verify its index and worktree clean before each task and stage only that
  task's exact paths. Run the exact `verify_canonical_snapshot` definition above
  after every task commit and again before the final handoff update. If commands
  run in a fresh Bash process, re-evaluate that definition first. HEAD equality
  alone is insufficient: all six Slice 1 snapshot files must continue to prove
  the canonical index tree, staged and unstaged binary diffs, NUL-delimited
  status/untracked inventory, and untracked file hashes byte-for-byte without
  reading any canonical change into the task index.
- Unsupported scope stays explicit: `join.sh`, `add-agent.sh`,
  `openclaw-client` onboarding, `deploy-host.sh`, package upgrades, reboot
  persistence, macOS, ESP32, production MQTT devices, fleet TLS, per-agent
  credentials, credential rotation, Internet exposure, ARM64 performance,
  Ollama, Gemma, Hermes, and model-backed measurements.

## Consumed Contracts

Slice 1 owns these exact interfaces:

```python
@dataclass(frozen=True)
class NativeControlConfig:
    run_id: str
    agent_id: str
    mode: str
    behavior: str
    delay_ms: int
    crash_point: str | None
    heartbeat_interval_ms: int
    outcome_db: str
    side_effect_db: str

def build_agent_card(config: NativeControlConfig) -> dict[str, object]: ...

def write_json(path: Path, value: object) -> None: ...

def finalize_bundle(
    bundle_dir: Path,
    manifest: Mapping[str, object],
    schema_path: Path,
) -> str: ...

@dataclass(frozen=True)
class PreflightRequest:
    run_id: str
    mode: str
    expected_agents: tuple[str, ...]
    resolved_config: Mapping[str, object]
    credential_file: Path

@dataclass(frozen=True)
class PreflightReport:
    valid: bool
    checked_at: str
    checks: tuple[Mapping[str, object], ...]
    errors: tuple[str, ...]
    config_snapshot: Mapping[str, object]

    def to_dict(self) -> dict[str, object]: ...
    def require_valid(self) -> None: ...

async def run_preflight(request: PreflightRequest) -> PreflightReport: ...

@classmethod
def ArtifactEnvironment.create(
    cls, run_id: str, mode: str, output_root: Path
) -> "ArtifactEnvironment": ...

def ArtifactEnvironment.start_topology(
    self, compose_file: Path, env_overrides: Mapping[str, str]
) -> None: ...

def ArtifactEnvironment.owned_resources(self) -> tuple[OwnedResource, ...]: ...
def ArtifactEnvironment.cleanup(self) -> CleanupReport: ...

# Settled Slice 1 fresh-process recovery CLI:
# scripts/research/run-python scripts/research/run_artifact.py cleanup
#   --run-id RUN_ID --scratch-root ABSOLUTE_SCRATCH_ROOT

@dataclass(frozen=True)
class ArtifactIssue:
    code: str
    path: str
    message: str

@dataclass(frozen=True)
class CheckReport:
    valid: bool
    issues: tuple[ArtifactIssue, ...]

    def require_valid(self) -> None: ...

def check_bundle(
    path: Path,
    *,
    expected_kind: Literal["benchmark", "operator", "lab"] | None = None,
    source_root: Path | None = None,
) -> CheckReport: ...
```

`ArtifactEnvironment.create()` owns `environment.credential_file: Path`. Its
bytes are the Slice 1/Slice 2 raw transport token followed by one newline. Slice
3 does not replace that file or generate a second transport token.

The Slice 1 fixture CLI remains:

```bash
python3 -m scripts.research.fixtures.native_control \
  --config /run/config/native-control.json
```

Slice 2 owns these exact files and external contract:

```text
e2e/run-isolated.js
e2e/playwright.config.js
e2e/playwright.evidence.config.js
e2e/tests/operator-journey.spec.js
scripts/research/capture_operator_journey.py
```

For a Slice 3 controller, run Playwright with `cwd=e2e`, set `APP_URL` and
`AGG_URL` to the controller's advertised nginx URL, select the stated config,
and pass `tests/operator-journey.spec.js`. Do not set a Slice 3-only target-agent
variable and do not create another journey spec.

The evidence-only media handoff remains:

```python
def passed_project_results(
    report: Mapping[str, object],
) -> dict[str, Mapping[str, object]]: ...

def copy_media(
    source_root: Path,
    bundle: Path,
    results: Mapping[str, Mapping[str, object]],
) -> dict[str, object]: ...
```

Slice 3 may call these two settled Slice 2 helpers after running the evidence
configuration against its already-running controller. It replaces the original
Playwright JSON report with the portable mapping returned by `copy_media` via
Slice 1 `write_json`. It does not call the Slice 2 capture wrapper because that
wrapper owns a different Compose stack.

## File Map

| Path | Responsibility |
| --- | --- |
| `scripts/research/lab_config.py` | Strict IDs, `Path`-typed config, raw credential/service-env split, private JSON mode |
| `scripts/research/lab_runtime.py` | Pre-output source provenance, prebuilt immutable image provenance, private NATS config validation |
| `scripts/research/lab_contract.py` | Pure lab manifest, source, observation, and semantic predicates reused by runtime and checker |
| `scripts/research/lab_observations.py` | Append-only, fsynced, secret-free controller observation journal |
| `scripts/research/nats-lab.conf.tpl` | Native NATS and JetStream config; MQTT absent |
| `scripts/research/docker-compose.lab.yml` | Isolated controller topology |
| `nginx/default.conf` | Overwrite the lab API peer header used by the server-observed route fact |
| `scripts/research/lab_preflight.py` | Add lab checks to the shared `PreflightReport` |
| `scripts/research/lab_controller.py` | Start, status, command, await, export-image, stop, and qualify |
| `scripts/research/lab_node.py` | Reservation, immutable fixture container, doctor, reconnect, and release |
| `scripts/research/lab_gate.py` | Full same-host, paired-run, Playwright, and clean-checkout orchestration |
| `scripts/research/lab_qualification.py` | Fail-closed preliminary versus remote-qualified classifier |
| `aggregator/lab_inventory.py` | Authenticated run-only reservation and node-report store |
| `aggregator/Dockerfile` | Copy the lightweight lab validator module into the lab image |
| `aggregator/main.py` | Mount lab routes only when `LAB_RUN_ID` is set |
| `schemas/research-manifest.v1.json` | Strict lab branch alongside settled benchmark/operator branches |
| `scripts/research/check_artifact.py` | Dispatch finalized lab bundles through shared lab predicates |
| `tests/research/test_lab_config.py` | Dependency, ID, credential, and fixture-image contract |
| `tests/research/test_lab_controller.py` | Persistent ownership, rollback, and finalization |
| `tests/research/test_lab_evidence.py` | Lab schema, checker, corruption, single-finalizer, and provenance contract |
| `tests/research/test_lab_node.py` | Reservation, container argv, rollback, and doctor facts |
| `tests/research/test_lab_lifecycle.py` | Same-host, sequential, concurrent, and cleanup integration gates |
| `tests/research/test_lab_qualification.py` | Remote classification and runbook contract |
| `aggregator/tests/test_lab_inventory.py` | Conditional routes and reservation state machine |
| `docs/setup-lab-node.md` | Exact local and remote commands with adjacent limitations |
| `docs/research/task-aware-reliability-contract-design.md` | R-09 status only |

---

### Task 1: Freeze Dependencies, IDs, Credentials, And Fixture Runtime

**Files:**
- Create: `tests/research/test_lab_config.py`
- Create: `scripts/research/lab_config.py`
- Create: `scripts/research/lab_runtime.py`

**Interfaces:**
- Consumes: every signature in **Consumed Contracts**, the Slice 1
  `scripts/research/Dockerfile`, and
  `scripts/research/requirements.lock.txt`.
- Produces:

  ```python
  def validate_run_id(value: str) -> str: ...
  def validate_agent_id(value: str) -> str: ...
  def validate_declared_host_id(value: str) -> str: ...
  def qualified_agent_id(run_id: str, agent_id: str) -> str: ...
  def credential_token(credential_file: Path) -> str: ...
  def credential_sha256(credential_file: Path) -> str: ...
  def write_credential_file(credential_file: Path, token: str) -> None: ...
  def write_service_env_file(
      service_env_file: Path, raw_credential_file: Path
  ) -> None: ...
  def write_private_json(path: Path, value: object) -> None: ...
  def sha256_file(path: Path) -> str: ...

  class CommandRunner(Protocol):
      def __call__(
          self, argv: Sequence[str], *, cwd: Path
      ) -> CompletedProcess[str]: ...

  @dataclass(frozen=True)
  class ControllerConfig:
      run_id: str
      lab_variant: Literal[
          "lifecycle", "operator-smoke", "operator-evidence"
      ]
      controller_host_id: str
      compose_project: str
      bind_host: str
      advertised_host: str
      advertised_ip: str
      app_url: str
      agg_url: str
      nats_url: str
      monitor_url: str
      inventory_url: str
      controller_machine_id_sha256: str
      credential_sha256: str
      credential_file: Path
      fixture_image_id: str
      state_dir: Path
      evidence_dir: Path

      def to_dict(self) -> dict[str, object]: ...

  @dataclass(frozen=True)
  class FixtureImage:
      image_id: str
      dockerfile_sha256: str
      requirements_lock_sha256: str
      built_at: str

  @dataclass(frozen=True)
  class SourceProvenance:
      commit: str
      dirty: bool
      source_snapshot_sha256: str
      source_diff_sha256: str

  LAB_NGINX_IMAGE = (
      "nginx@sha256:"
      "5616878291a2eed594aee8db4dade5878cf7edcb475e59193904b198d9b830de"
  )

  LAB_SOURCE_PATHS = (
      ".dockerignore",
      "aggregator",
      "frontend",
      "e2e",
      "nginx",
      "scripts/research",
      "schemas",
      "docs/setup-lab-node.md",
      "docs/research/task-aware-reliability-contract-design.md",
  )

  def capture_clean_source_provenance(
      repo_root: Path,
  ) -> SourceProvenance: ...

  def build_fixture_image(
      repo_root: Path, run_id: str, runner: CommandRunner
  ) -> FixtureImage: ...
  ```

- Invariants: run IDs and logical agent IDs are 3-31 characters using lowercase
  letters, digits, and hyphens; declared host IDs use the same rule;
  `qualified_agent_id` rejects a result longer than 64 characters.
  `ControllerConfig.credential_file` is the exact external scratch path returned
  by `ArtifactEnvironment`; it is not assumed to live below `state_dir`.
  `controller.json` may expose this path to maintained local launchers but never
  its contents. Finalized evidence replaces it with `<credential-file>`.

- [ ] **Step 1: Write the eight failing contract tests**

  The tests must be eight test functions with these assertions:

  ```python
  def test_slice_dependencies_are_exact_public_contracts():
      from scripts.research.check_artifact import check_bundle
      from scripts.research.evidence import finalize_bundle, write_json

      assert callable(check_bundle)
      assert callable(write_json)
      assert callable(finalize_bundle)
      assert os.access("scripts/research/run-python", os.X_OK)
      assert Path("scripts/research/requirements.lock.txt").is_file()
      assert Path("scripts/research/toolchain.json").is_file()
      assert LAB_NGINX_IMAGE == (
          "nginx@sha256:"
          "5616878291a2eed594aee8db4dade5878cf7edcb475e59193904b198d9b830de"
      )
      assert Path("e2e/playwright.config.js").is_file()
      assert Path("e2e/playwright.evidence.config.js").is_file()
      assert Path("e2e/tests/operator-journey.spec.js").is_file()


  def test_identifier_boundaries_are_exact():
      assert validate_run_id("ec-lab-01") == "ec-lab-01"
      assert validate_agent_id("shell-1") == "shell-1"
      assert validate_declared_host_id("controller-lab-01") == "controller-lab-01"
      for value in ("ab", "UPPER", "contains.dot", "../escape", "a" * 32):
          with pytest.raises(LabConfigError):
              validate_run_id(value)


  def test_qualified_agent_id_never_exceeds_wire_limit():
      value = qualified_agent_id("r" * 31, "a" * 31)
      assert len(value) == 64


  def test_credential_errors_never_echo_malformed_content(tmp_path):
      credential = tmp_path / "nats.creds"
      malformed = "private-material-that-must-not-be-echoed=bad"
      credential.write_text(malformed + "\n")
      credential.chmod(0o600)
      with pytest.raises(LabConfigError) as error:
          credential_token(credential)
      assert malformed not in str(error.value)
      assert "line 1" in str(error.value)


  def test_raw_credential_and_service_env_are_distinct_private_formats(tmp_path):
      raw = tmp_path / "nats.creds"
      service_env = tmp_path / "service.env"
      token = "4" * 64
      write_credential_file(raw, token)
      write_service_env_file(service_env, raw)
      assert raw.read_bytes() == (token + "\n").encode()
      assert service_env.read_bytes() == ("NATS_TOKEN=" + token + "\n").encode()
      assert credential_token(raw) == token
      with pytest.raises(LabConfigError):
          credential_token(service_env)
      assert raw.stat().st_mode & 0o777 == 0o600
      assert service_env.stat().st_mode & 0o777 == 0o600


  def test_private_config_serializes_no_crash_as_json_null(tmp_path):
      path = tmp_path / "native-control.json"
      config = NativeControlConfig(
          run_id="ec-lab-01",
          agent_id="shell-1",
          mode="edgecitadel",
          behavior="echo",
          delay_ms=125,
          crash_point=None,
          heartbeat_interval_ms=1000,
          outcome_db="/run/state/outcomes.sqlite",
          side_effect_db="/run/state/side-effects.sqlite",
      )
      write_private_json(path, asdict(config))
      assert json.loads(path.read_text())["crash_point"] is None
      assert path.stat().st_mode & 0o777 == 0o600


  def test_fixture_build_returns_and_uses_only_immutable_image_id(tmp_path):
      repo_root = make_slice1_fixture_repo(tmp_path)
      runner = recording_runner(inspect_stdout="sha256:" + "3" * 64)
      image = build_fixture_image(repo_root, "ec-lab-01", runner)
      assert re.fullmatch(r"sha256:[0-9a-f]{64}", image.image_id)
      assert image.image_id == "sha256:" + "3" * 64
      assert image.dockerfile_sha256 == sha256_file(
          repo_root / "scripts/research/Dockerfile"
      )
      assert image.requirements_lock_sha256 == sha256_file(
          repo_root / "scripts/research/requirements.lock.txt"
      )
      assert runner.calls[-1][-1] == "edgecitadel-lab-fixture:ec-lab-01"


  def test_source_provenance_is_captured_before_outputs_and_is_path_scoped(
      tmp_path,
  ):
      repo = make_clean_git_repo(tmp_path, LAB_SOURCE_PATHS)
      before = capture_clean_source_provenance(repo)
      assert before.dirty is False
      (repo / "docs/research/results/lab/run-1").mkdir(parents=True)
      (repo / "docs/research/results/lab/run-1/raw.json").write_text("{}\n")
      assert capture_clean_source_provenance(repo) == before
      runtime = repo / "scripts/research/lab_runtime.py"
      original_runtime = runtime.read_text()
      runtime.write_text("# changed\n")
      with pytest.raises(LabConfigError, match="source paths must be clean"):
          capture_clean_source_provenance(repo)
      runtime.write_text(original_runtime)
      (repo / ".dockerignore").write_text("changed-build-input\n")
      with pytest.raises(LabConfigError, match="source paths must be clean"):
          capture_clean_source_provenance(repo)
  ```

  The fixture runner records the build argv and a deterministic
  `docker image inspect --format={{.Id}}` response. It also asserts the build
  context contains both Slice 1 files.

- [ ] **Step 2: Run RED**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_config.py -q
  ```

  Expected: collection fails only because `scripts.research.lab_config` and
  `scripts.research.lab_runtime` do not exist.

- [ ] **Step 3: Implement the strict config and credential boundary**

  Use these validators:

  ```python
  ID_RE = re.compile(r"^[a-z][a-z0-9-]{2,30}$")
  IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
  TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")

  def qualified_agent_id(run_id: str, agent_id: str) -> str:
      qualified = f"{validate_run_id(run_id)}--{validate_agent_id(agent_id)}"
      if len(qualified) > 64:
          raise LabConfigError("qualified agent ID exceeds 64 characters")
      return qualified
  ```

  `credential_token` must:

  1. Require a `Path`, a regular file, and exact mode `0600`.
  2. Read UTF-8 text without logging it.
  3. Accept exactly one raw token line matching `TOKEN_RE` and reject `=`,
     whitespace, quotes, backslashes, dollar signs, comments, or a second line.
     This accepts the Slice 1 32-byte hex/URL-safe encoding without allowing
     NATS-config or env-file syntax.
  4. Raise `LabConfigError(f"malformed credential file at line {line_no}")`
     for extra lines or decoding failures.
  5. Reject the exact values `changeme`, `change-me`, and `test-token` even
     before the length/character check.
  6. Return the value only to the caller; do not place it in exception text.

  `write_credential_file` creates the parent at `0700`, opens the destination
  with `os.open(..., O_CREAT | O_EXCL | O_WRONLY, 0o600)`, writes exactly one
  raw-token line, fsyncs the file and parent, and closes it.
  `write_service_env_file` reads the raw token through `credential_token`, writes
  exactly `NATS_TOKEN=<token>\n` to a different `O_EXCL` mode-0600 file, and
  fsyncs it and its parent. `write_private_json` calls Slice 1 `write_json` under
  an `0o077` process umask and then enforces mode `0600`; it does not call
  `json.dump`.

- [ ] **Step 4: Implement immutable fixture-image preparation**

  Define `LAB_NGINX_IMAGE` exactly as above. Do not resolve a mutable nginx tag
  at run time. Slice 1 `toolchain.json` remains the only NATS digest source;
  Task 3 consumes these two immutable references directly.

  `build_fixture_image` computes both source hashes, then runs these argv lists
  through the injected runner:

  ```python
  [
      "docker", "build", "--pull",
      "--file", str(repo_root / "scripts/research/Dockerfile"),
      "--label", "ai.edgecitadel.owner=artifact",
      "--label", f"ai.edgecitadel.run-id={run_id}",
      "--tag", f"edgecitadel-lab-fixture:{run_id}",
      str(repo_root),
  ]
  [
      "docker", "image", "inspect",
      "--format={{.Id}}",
      f"edgecitadel-lab-fixture:{run_id}",
  ]
  ```

  Reject a non-immutable inspect result. Return the immutable ID and hashes.
  Runtime code in later tasks receives only `FixtureImage.image_id`. If inspect
  fails or returns a mutable/malformed value after build succeeds, remove the
  exact run tag before raising so rollback cannot leave an owned image.

  `capture_clean_source_provenance` runs Git commands only against
  `LAB_SOURCE_PATHS`, requires that scoped porcelain status is empty, hashes the
  same sorted file set used later by `lab_contract`, and runs before any
  `tmp/research` or `docs/research/results` directory is created. Generated
  output paths are outside the source set by construction, not by a broad
  `.gitignore`.

- [ ] **Step 5: Run GREEN and dependency scans**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_config.py -q
  scripts/research/run-python - <<'PY'
  import ast
  from pathlib import Path

  allowed = {
      "scripts.research.evidence": {"write_json", "finalize_bundle"},
      "scripts.research.check_artifact": {"check_bundle"},
  }
  for path in (
      Path("scripts/research/lab_config.py"),
      Path("scripts/research/lab_runtime.py"),
  ):
      tree = ast.parse(path.read_text())
      for node in ast.walk(tree):
          if isinstance(node, ast.ImportFrom) and node.module in allowed:
              assert {item.name for item in node.names} <= allowed[node.module]
  print("public imports: PASS")
  PY
  ```

  Expected: exactly 8 tests pass and the import scan prints
  `public imports: PASS`.

- [ ] **Step 6: Commit Task 1**

  Invoke `commit-check`, require `git diff --cached --check` to exit zero, then:

  ```bash
  git add -- scripts/research/lab_config.py scripts/research/lab_runtime.py \
    tests/research/test_lab_config.py
  git commit -m "feat(infra): define strict lab runtime contracts"
  verify_canonical_snapshot
  ```

---

### Task 2: Add A Fail-Closed Lab Inventory

**Files:**
- Create: `aggregator/lab_inventory.py`
- Create: `aggregator/tests/test_lab_inventory.py`
- Modify: `aggregator/Dockerfile`
- Modify: `aggregator/main.py`

**Interfaces:**
- Consumes: `validate_run_id`, `validate_agent_id`,
  `validate_declared_host_id`, and `qualified_agent_id` from Task 1.
- Produces:

  ```python
  def build_lab_router(
      *, run_id: str, token_sha256: str, inventory_path: Path
  ) -> APIRouter: ...
  ```

  Authenticated routes:

  ```text
  POST   /api/lab/reservations
  PATCH  /api/lab/reservations/{agent_id}/retain
  DELETE /api/lab/reservations/{agent_id}
  POST   /api/lab/node-reports
  GET    /api/lab/status
  ```

- Reservation states are exactly `active` and `retained`. A first reservation
  returns 201. Any existing active reservation returns 409. A retained
  reservation returns 200 only when resumed with its original reservation ID
  and declared host ID.
- Every successful reservation transition appends an immutable event with a
  controller-assigned sequence and UTC timestamp. `node-reports` requires the
  matching reservation ID and declared host, stores self-reported
  `network_path` unchanged, and adds a separate top-level
  `server_observed_peer_ip = request.client.host`; it never stores the
  `Authorization` header. Task 3 makes that socket peer meaningful across hosts:
  the only published HTTP ingress is nginx, nginx overwrites
  `X-Forwarded-For` with `$remote_addr`, and only the lab aggregator enables
  Uvicorn proxy-header trust.

- [ ] **Step 1: Use `deliberate-changes`, then write nine failing API and packaging tests**

  Cover exactly:

  1. Routes are 404 without `LAB_RUN_ID`.
  2. Missing, wrong, and malformed bearer credentials fail closed.
  3. A first reservation is 201 and a second active claim is 409, even with the
     same reservation ID.
  4. Only the matching owner can resume a retained reservation.
  5. Release is idempotent for the matching owner and rejects another owner.
  6. A node report with the matching active/retained reservation records the
     server-observed peer IP outside `network_path`; the topology test proves
     nginx overwrites rather than appends the forwarded peer and a mismatched
     reservation, host, or qualified ID returns 409.
  7. `reserved`, `retained`, `resumed`, and `released` events remain ordered and
     queryable after the current reservation row is deleted.
  8. Serialized status contains neither token nor authorization header and
     returns `reservations`, `reservation_events`, and `node_reports` as distinct
     collections.
  9. Building `aggregator/Dockerfile` and importing `aggregator.main` with
     `LAB_RUN_ID` set succeeds because the lightweight `lab_config` boundary is
     present in the image.

  Use this active-reservation assertion:

  ```python
  first = client.post("/api/lab/reservations", headers=AUTH, json=body)
  assert first.status_code == 201
  repeated = client.post("/api/lab/reservations", headers=AUTH, json=body)
  assert repeated.status_code == 409
  assert repeated.json()["detail"] == "agent_id has an active reservation"
  ```

- [ ] **Step 2: Run RED**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests/test_lab_inventory.py -q
  ```

  Expected: the disabled-route test passes and the other eight tests fail
  because the lab router, event history, and image packaging boundary are absent.

- [ ] **Step 3: Implement the transactional reservation state machine**

  Create these exact tables:

  ```sql
  CREATE TABLE reservations (
      run_id TEXT NOT NULL,
      agent_id TEXT NOT NULL,
      qualified_agent_id TEXT NOT NULL,
      reservation_id TEXT NOT NULL,
      declared_host_id TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('active', 'retained')),
      updated_at TEXT NOT NULL,
      PRIMARY KEY (run_id, agent_id)
  );

  CREATE TABLE node_reports (
      run_id TEXT NOT NULL,
      agent_id TEXT NOT NULL,
      reservation_id TEXT NOT NULL,
      declared_host_id TEXT NOT NULL,
      report_json TEXT NOT NULL,
      PRIMARY KEY (run_id, agent_id)
  );

  CREATE TABLE reservation_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id TEXT NOT NULL,
      agent_id TEXT NOT NULL,
      qualified_agent_id TEXT NOT NULL,
      reservation_id TEXT NOT NULL,
      declared_host_id TEXT NOT NULL,
      event TEXT NOT NULL CHECK (
          event IN ('reserved', 'retained', 'resumed', 'released')
      ),
      observed_at TEXT NOT NULL
  );
  ```

  Use `BEGIN IMMEDIATE` around lookup and mutation. Validate that
  `qualified_agent_id == qualified_agent_id(run_id, agent_id)`. Compare bearer
  token hashes with `hmac.compare_digest`.

  `PATCH .../retain` accepts only an active row with matching reservation and
  host and appends `retained`. `POST /reservations` appends `reserved` for a new
  row and `resumed` only for a retained matching row. `DELETE` appends
  `released` before deleting a matching row and returns 204 when the matching
  release event already exists. A mismatched extant owner remains 409. Events
  and reports are never deleted by reservation release; controller teardown
  snapshots them before removing run storage.

  A node report requires:

  ```text
  run_id, agent_id, qualified_agent_id, reservation_id, declared_host_id,
  machine_id_sha256, hostname, os_release, architecture,
  launcher_source_commit, source_snapshot_sha256,
  network_path, preflight_valid, lifecycle_state, cleanup, checked_at
  ```

  `network_path` requires `source_ip`, `destination_ip`, `interface`,
  `route_output_sha256`, and `controller_dns_name`. Store
  `server_observed_peer_ip` as a sibling of `network_path`. Before upsert, require
  the current reservation's ID, host, and qualified ID to match the report.
  `lifecycle_state` is `active`, `retained`, or `released`; `cleanup` is null
  until normal stop and then records exact local paths/image removal booleans.

- [ ] **Step 4: Make the lab import boundary explicit**

  Keep ID/host validation in the stdlib-only top level of
  `scripts/research/lab_config.py`; `write_private_json` performs its
  `scripts.research.evidence` import inside that function. Modify
  `aggregator/Dockerfile` to copy these exact files:

  ```dockerfile
  COPY scripts/__init__.py ./scripts/__init__.py
  COPY scripts/research/__init__.py ./scripts/research/__init__.py
  COPY scripts/research/lab_config.py ./scripts/research/lab_config.py
  ```

  The packaging test builds a run-tagged image with `docker build`, executes
  `python -c 'from aggregator.main import make_app; make_app()'` with a temporary
  absolute inventory path and valid token hash, then removes the exact image in
  `finally`. No production image imports the evidence module merely by mounting
  the disabled lab routes.

- [ ] **Step 5: Mount only in lab mode**

  In `make_app`, include the router only when `LAB_RUN_ID` is nonempty. Require a
  64-character lowercase hex `LAB_TOKEN_SHA256` and an absolute
  `LAB_INVENTORY_PATH`. Production startup without `LAB_RUN_ID` must not import
  or initialize the lab database.

- [ ] **Step 6: Run GREEN and backend regressions**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests/test_lab_inventory.py \
    aggregator/tests/test_api.py aggregator/tests/test_registry_endpoint.py -q
  ```

  Expected: the 9 lab inventory tests pass; the two existing test files have
  zero failures and zero new skips. Invoke `verify-backend`.

- [ ] **Step 7: Commit Task 2**

  Stage the exact Task 2 file map from the clean task worktree. Invoke
  `commit-check`, inspect every cached hunk, and require
  `git diff --cached --check` to exit zero:

  ```bash
  git add -- aggregator/lab_inventory.py aggregator/tests/test_lab_inventory.py \
    aggregator/main.py aggregator/Dockerfile
  git diff --cached -- aggregator/main.py aggregator/Dockerfile
  git commit -m "feat(aggregator): add fail-closed lab inventory"
  verify_canonical_snapshot
  ```

---

### Task 3: Start, Persist, Roll Back, And Finalize The Controller

**Files:**
- Create: `scripts/research/nats-lab.conf.tpl`
- Create: `scripts/research/docker-compose.lab.yml`
- Create: `scripts/research/lab_contract.py`
- Create: `scripts/research/lab_observations.py`
- Create: `scripts/research/lab_preflight.py`
- Create: `scripts/research/lab_controller.py`
- Create: `tests/research/test_lab_controller.py`
- Create: `tests/research/test_lab_evidence.py`
- Modify: `nginx/default.conf`
- Modify: `schemas/research-manifest.v1.json`
- Modify: `scripts/research/check_artifact.py`

**Interfaces:**
- Consumes: Task 1 config/runtime, Task 2 inventory, Slice 1
  `ArtifactEnvironment`, `OwnedResource`, `PreflightRequest`,
  `PreflightReport`, `run_preflight`, `write_json`, `finalize_bundle`, and the
  post-Slice-2 `check_bundle` dispatcher.
- Produces:

  ```python
  @dataclass(frozen=True)
  class ControllerOwnershipState:
      schema_version: str
      phase: Literal["starting", "active", "stopping", "stopped", "failed"]
      config: ControllerConfig
      compose_file: Path
      compose_environment: Mapping[str, str]
      artifact_scratch_root: Path
      raw_credential_file: Path
      service_env_file: Path
      owned_resources: tuple[OwnedResource, ...]
      completed_cleanup_steps: tuple[str, ...]
      exported_image_paths: tuple[Path, ...]
      controller_argv: tuple[str, ...]
      started_at: str

  def load_controller_state(state_file: Path) -> ControllerOwnershipState: ...
  def write_controller_state(
      state_file: Path, state: ControllerOwnershipState
  ) -> None: ...
  def start_controller(args: Namespace) -> ControllerConfig: ...
  def stop_controller(state_file: Path) -> Mapping[str, object]: ...
  async def run_controller_preflight(
      controller_config: ControllerConfig,
      credential_file: Path,
      expected_agents: tuple[str, ...] = (),
  ) -> PreflightReport: ...

  @dataclass(frozen=True)
  class LabContractIssue:
      code: str
      relative_path: str
      message: str

  def lab_semantic_issues(
      bundle: Path,
      manifest: Mapping[str, object],
      source_root: Path,
  ) -> tuple[LabContractIssue, ...]: ...

  def require_complete_lab_manifest(
      bundle: Path,
      manifest: Mapping[str, object],
      source_root: Path,
  ) -> None: ...

  def append_observation(
      path: Path, observation: Mapping[str, object]
  ) -> None: ...
  ```

- `controller-state.json` is the durable handoff between CLI processes. Its
  serializer converts paths to strings and resources to `{kind, name}`; its
  loader reconstructs `Path` and `OwnedResource` objects. Writes use a private
  temporary file, file fsync, `os.replace`, and parent-directory fsync.
- `lab-observations.jsonl` is append-only and each record contains
  `schema_version`, run-scoped monotonic `sequence`, `observed_at`, `event`,
  `agent_id`, `reservation_id`, `task_id`, and `data`; nullable identifiers are
  explicit. `append_observation` rejects secret-shaped keys/values before an
  `O_APPEND` write and fsync.
- The lab manifest uses the Slice 1 schema without dummy benchmark fields. Every
  accepted lab manifest contains exactly these required top-level fields:

  ```text
  schema_version, evidence_kind, lab_variant, status, run_id,
  source, command, timing, host, dependencies, images,
  compose_config_sha256, schemas, cleanup, artifacts,
  controller, nodes, observations
  ```

  `lab_variant` is exactly `lifecycle`, `operator-smoke`, or
  `operator-evidence`. The lab branch explicitly rejects `campaign_id`, `profile`,
  `transport_config`, `workload_config`, `metric_contract`, and `projects`.
  `controller` contains project, `bind_host`, `advertised_host`, the selected
  resolved `advertised_ip`, advertised/bound endpoints, declared host, and
  machine facts. `nodes` is a nonempty array of logical/run-qualified IDs,
  reservation IDs, declared hosts, machine facts, fixture image IDs, and
  network paths. `observations` names and hashes the reservation-event,
  node-report, controller-command, Playwright, and cleanup evidence used by the
  checker.

- [ ] **Step 1: Use `deliberate-changes`, then write sixteen failing controller tests**

  Write exactly these sixteen tests before controller implementation:

  1. `ControllerOwnershipState` round-trips atomically with
     `config.lab_variant`, the absolute Slice 1 artifact scratch root, both
     credential `Path` values, cleanup progress, exported paths, and
     `tuple[OwnedResource, ...]`.
  2. A simulated interruption during state replacement leaves either the old or
     new complete JSON document, never a partial document.
  3. `run_controller_preflight` passes the raw credential as a `Path` into
     `PreflightRequest`.
  4. Existing `phase="active"` is rejected before image build or Compose.
  5. Source provenance is captured and verified clean before any run/evidence
     directory is created.
  6. The environment's raw credential remains raw, `service.env` is separate,
     and neither token bytes nor service-env bytes enter state.
  7. NATS config validation runs against the pinned image and private service env
     before any Compose build/up call; invalid config leaves no runtime.
  8. Aggregator and dashboard are prebuilt under run tags, inspected to immutable
     IDs, and only those IDs plus NATS/nginx repo digests enter runtime Compose.
  9. A fixture/app-image inspect failure removes the exact run tag and both
     credential files in `finally`.
  10. A Compose start failure calls cleanup once, removes prebuilt images and
      secrets, and leaves recoverable `phase="failed"`.
  11. A failed preflight tears down all owned resources, removes secrets before
      finalization, and finalizes exactly one `INVALID` bundle.
  12. A fresh process stops an `active` state using the persisted project, files,
      environment, resource tuple, and exported image paths.
  13. Fresh-process stops resume each of `starting`, `stopping`, and `failed`
      without repeating completed cleanup steps.
  14. A second stop of `stopped` returns the cached cleanup result without Docker
      mutation or another `finalize_bundle` call.
  15. `status --json` is secret-free and `export-image` saves the exact immutable
      fixture ID, writes SHA-256 metadata, journals the output path, and never
      exports a mutable tag.
  16. A complete manifest includes source hash, secret-free argv, UTC timestamps,
      controller/node/network facts, exact dependency versions, every immutable
      image, Compose hash, observation paths, cleanup, and artifact hashes. The
      topology exposes no aggregator port, nginx overwrites
      `X-Forwarded-For $remote_addr`, and lab-only
      `FORWARDED_ALLOW_IPS="*"` makes `request.client.host` the ingress-observed
      peer.

  The fresh-process test must discard the original object before stopping:

  ```python
  write_controller_state(state_file, state)
  del state
  result = stop_controller(state_file)
  assert result["owned_resources_removed"] is True
  assert fake_docker.compose_project == "edgecitadel-artifact-ec-lab-01"
  ```

- [ ] **Step 2: Write ten failing lab schema and checker tests**

  In `tests/research/test_lab_evidence.py`, write one valid-variants function
  that loops over all three fixtures and nine isolated corruption functions:

  1. Complete `lifecycle`, `operator-smoke`, and `operator-evidence` fixtures
     each finalize `PASS`; `check_bundle(..., expected_kind="lab",
     source_root=clean_source).require_valid()` returns normally and returns a
     `CheckReport`. The same lab call without `source_root` returns invalid with
     stable `LAB_SOURCE_ROOT_REQUIRED`; optional parameters remain optional at
     the public Python boundary.
  2. Missing `nodes`, `controller`, or `observations` fails schema finalization.
  3. Any benchmark-only or operator-only top-level field fails the lab branch.
  4. A modified raw byte after finalization yields
     `CheckReport.valid is False` without mutating the immutable manifest.
  5. A different clean source snapshot yields stable
     `LAB_SOURCE_SNAPSHOT_MISMATCH`.
  6. Missing `reserved/retained/resumed/released` history yields stable
     `LAB_RESERVATION_HISTORY_INCOMPLETE`.
  7. Queue acceptance outside
     `disconnect < accepted < reconnect < terminal` yields stable
     `LAB_RECONNECT_ORDER_INVALID`.
  8. A node report whose reservation/host differs from its launch observation
     yields stable `LAB_NODE_BINDING_INVALID`.
  9. Cleanup residue, a missing raw observation path, or a path absent from
     `manifest["artifacts"]` yields stable lab issues. An absolute source,
     worktree, credential, `/tmp`, `/var/folders`, or Windows path yields stable
     `LAB_NONPORTABLE_PATH`.
  10. A controller finalization spy is called once; a gate/checker invocation
      calls `check_bundle` only and never calls `finalize_bundle`.

- [ ] **Step 3: Run RED**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_controller.py \
    tests/research/test_lab_evidence.py -q
  ```

  Expected: collection fails because `scripts.research.lab_controller`,
  `scripts.research.lab_contract`, `scripts.research.lab_observations`, and
  `scripts.research.lab_preflight` are absent.

- [ ] **Step 4: Create the complete controller topology**

  `nats-lab.conf.tpl` is:

  ```conf
  server_name: "edgecitadel-lab"
  listen: 0.0.0.0:4222
  http: 0.0.0.0:8222

  jetstream {
    store_dir: "/data/jetstream"
    max_mem: 256MB
    max_file: 1GB
  }

  authorization {
    token: "$NATS_TOKEN"
  }
  ```

  MQTT remains absent. `docker-compose.lab.yml` contains exactly `nats`,
  `aggregator`, `dashboard`, and `nginx`. It uses:

  ```yaml
  services:
    nats:
      image: ${LAB_NATS_IMAGE:?required}
      command: ["-c", "/etc/nats/nats.conf"]
      env_file: ["${LAB_SERVICE_ENV_FILE:?required}"]
      ports:
        - "${LAB_BIND_HOST:?required}:${LAB_NATS_PORT-}:4222"
        - "127.0.0.1:${LAB_MONITOR_PORT-}:8222"
      volumes:
        - "${LAB_NATS_CONFIG:?required}:/etc/nats/nats.conf:ro"
        - nats-data:/data
      labels: &lab-labels
        ai.edgecitadel.owner: artifact
        ai.edgecitadel.run-id: ${LAB_RUN_ID:?required}
      healthcheck:
        test: ["CMD", "wget", "--spider", "-q", "http://127.0.0.1:8222/healthz"]
        interval: 2s
        timeout: 2s
        retries: 30

    aggregator:
      image: ${LAB_AGGREGATOR_IMAGE:?required}
      build:
        context: ../..
        dockerfile: aggregator/Dockerfile
        labels: *lab-labels
      env_file: ["${LAB_SERVICE_ENV_FILE:?required}"]
      environment:
        DB_PATH: /data/openclaw.db
        NATS_URL: nats://nats:4222
        FORWARDED_ALLOW_IPS: "*"
        LAB_RUN_ID: ${LAB_RUN_ID:?required}
        LAB_TOKEN_SHA256: ${LAB_TOKEN_SHA256:?required}
        LAB_INVENTORY_PATH: /data/lab-inventory.db
      volumes:
        - "${LAB_DATA_DIR:?required}:/data"
      labels: *lab-labels
      depends_on:
        nats: {condition: service_healthy}

    dashboard:
      image: ${LAB_DASHBOARD_IMAGE:?required}
      build:
        context: ../../frontend
        labels: *lab-labels
      labels: *lab-labels

    nginx:
      image: ${LAB_NGINX_IMAGE:?required}
      ports:
        - "${LAB_BIND_HOST:?required}:${LAB_HTTP_PORT-}:80"
      volumes:
        - "../../nginx/default.conf:/etc/nginx/conf.d/default.conf:ro"
      labels: *lab-labels
      depends_on:
        aggregator: {condition: service_started}
        dashboard: {condition: service_started}

  volumes:
    nats-data:
      labels: *lab-labels
  ```

  Add `proxy_set_header X-Forwarded-For $remote_addr;` beside the existing
  `X-Real-IP` line in nginx's `/api/` location. It overwrites caller input
  rather than appending it. The aggregator remains unexposed, so wildcard proxy
  trust is scoped to the run-owned Compose network and is enabled only in this
  lab service environment. The topology test must inspect the rendered nginx
  config and a live remote-form request; a client-supplied forwarded value may
  never survive as `server_observed_peer_ip`.

  The blank published-port component is intentional. For automated loopback
  runs, pass an empty `LAB_HTTP_PORT`, `LAB_NATS_PORT`, and
  `LAB_MONITOR_PORT`; Docker assigns all three atomically, and the controller
  resolves them with `docker compose port` before writing final URLs. For a
  non-loopback bind, require three explicit, distinct caller-selected ports and
  reject an empty value.

  Use the exact Slice 1 ownership labels shown above. Read the digest-pinned NATS
  reference from Slice 1 `toolchain.json` and use Task 1's exact
  `LAB_NGINX_IMAGE`; no mutable service tag is resolved at run time. Prebuild
  aggregator/dashboard with run-owned tags and Compose `build`, inspect each tag
  to `sha256:<64-hex>`, then replace
  `LAB_AGGREGATOR_IMAGE` and `LAB_DASHBOARD_IMAGE` in the runtime environment
  with those immutable IDs. `ArtifactEnvironment.start_topology()` therefore
  succeeds with its settled `--no-build`. Record final local IDs/repo digests for
  all five images: NATS, aggregator, dashboard, nginx, and fixture. On every
  failure, remove only the recorded run tags/IDs.

  Before build/up, validate the private NATS template with an argv-only runner:

  ```python
  [
      "docker", "run", "--rm",
      "--env-file", str(service_env_file),
      "--mount", (
          f"type=bind,src={nats_config},"
          "dst=/etc/nats/nats.conf,readonly"
      ),
      nats_repo_digest,
      "-t", "-c", "/etc/nats/nats.conf",
  ]
  ```

  The runner captures output in memory, rejects output containing the raw token,
  and persists only exit status plus config SHA-256.

- [ ] **Step 5: Implement the lab schema, observations, and shared checker predicates**

  Extend only the `evidence_kind == "lab"` branch in
  `schemas/research-manifest.v1.json`; do not modify the settled benchmark or
  operator branches. Define the exact fields above with
  `additionalProperties: false` and branch on `lab_variant`:

  - `lifecycle` requires at least two nodes, three successful logical tasks (one
    per node plus queued reconnect), one duplicate-wire observation, ordered
    reservation history, and complete cleanup.
  - `operator-smoke` requires exactly one `shell-1` node, exactly one default
    `chromium` Playwright process with return code zero and exact `1 passed`
    output, one correlated successful API task recorded in portable
    `playwright-smoke.json`, and no `operator_evidence` or multi-node claim.
  - `operator-evidence` requires exactly one `shell-1` node and
    `operator_evidence` with exactly desktop/mobile media, portable report
    attachments, and distinct task IDs.

  Neither operator variant can satisfy the two-node lifecycle claim or remote
  qualification. A manifest mixing variant-only fields is invalid.

  `lab_contract.py` imports the one `LAB_SOURCE_PATHS` and source-hashing
  implementation from Task 1 `lab_runtime`; it owns only the pure
  `lab_semantic_issues` function and `require_complete_lab_manifest`.
  `lab_controller.py` calls `require_complete_lab_manifest` immediately before
  its sole successful finalizer call. `check_artifact.py` calls the same pure
  function only after base schema/hash/kind checks and maps each
  `LabContractIssue` to its existing `ArtifactIssue`; the public signature and
  `CheckReport` remain unchanged. For a lab manifest with no `source_root`, it
  returns `CheckReport(valid=False, ...)` with
  `LAB_SOURCE_ROOT_REQUIRED`; it never raises a Python signature error.
  Checker-detected post-finalization corruption makes `CheckReport.valid` false
  but never rewrites the finalized manifest.

  Implement `append_observation` with canonical JSON from Slice 1, a private
  lock file, a sequence read/append under that lock, `O_APPEND`, fsync, and no
  credential-shaped keys or values. Callers normalize any path-bearing data
  before append; the function rejects a surviving absolute transient path. It
  never calls `finalize_bundle`.

- [ ] **Step 6: Implement preflight with the shared types**

  Construct:

  ```python
  request = PreflightRequest(
      run_id=controller_config.run_id,
      mode="edgecitadel",
      expected_agents=tuple(expected_agents),
      resolved_config=controller_config.to_dict(),
      credential_file=credential_file,
  )
  ```

  Await `run_preflight`, then return a new shared `PreflightReport` containing
  the original checks plus:

  ```text
  system_status_semantic
  lab_inventory_authenticated
  registry_ready
  mqtt_not_listening
  fixture_image_immutable
  ```

  No check, error, or config snapshot may contain the token or an authorization
  header. The in-memory live snapshot may contain controller paths; the retained
  `preflight.json` must pass the shared path normalizer and uses
  `<credential-file>`, `<run-state>`, and `$SOURCE_ROOT`.

- [ ] **Step 7: Implement controller start with transactional rollback**

  The CLI is:

  ```text
  start --run-id ec-lab-01 --host-id controller-lab-01
        --lab-variant lifecycle
        --bind-host 127.0.0.1 --advertise-host 127.0.0.1
        [--http-port 18080 --nats-port 14222 --monitor-port 18222]
  status --run-id ec-lab-01 [--json]
  stop --run-id ec-lab-01
  export-image --run-id ec-lab-01 --output /tmp/ec-lab-01-fixture.tar
               --result-file /tmp/ec-lab-01-export.json
  ```

  `start` performs exactly:

  1. Validate Python/Docker/Compose/Git and the required `lab_variant`; when
     browser evidence is requested, validate exact Node/npm/Playwright versions.
     Validate run/host IDs and addresses. `bind_host` is a concrete,
     non-unspecified IPv4 address. Resolve `advertised_host` once, require the
     selected IPv4 address to be reachable, and persist both the original host
     string and selected `advertised_ip`.
     Loopback with omitted ports selects Docker-assigned ports. Non-loopback
     requires three distinct explicit ports and
     `--trusted-network-confirm`; binding `0.0.0.0` is rejected.
  2. Reject `active`; require recovery through `stop` for `starting`, `stopping`,
     or `failed`. Capture and require clean `SourceProvenance` before creating
     any run/evidence output.
  3. Resolve `EC_ARTIFACT_SCRATCH_ROOT` to an absolute path, defaulting to
     `/tmp/edgecitadel-artifact`, set that exact value for
     `ArtifactEnvironment.create(run_id, "edgecitadel",
     Path("tmp/research"))`, and persist it for Slice 1 fresh-process recovery.
     Treat `environment.credential_file` as the sole raw credential; verify its
     exact raw format and mode without replacing it.
  4. Atomically persist `phase="starting"` with the validated `lab_variant`,
     deterministic project, and raw credential path. Create a separate private
     `service.env`, persist only its path/hash, and register both paths for
     recovery.
  5. Create the evidence directory
     `docs/research/results/lab/<run-id>` and fail if it already exists.
  6. Copy the NATS template byte-for-byte to a generated mode-0600 file and
     validate it with the pinned NATS image before any Compose build/up.
  7. Resolve immutable NATS/nginx repo digests; prebuild and inspect immutable
     aggregator/dashboard IDs; build the Slice 1 fixture and retain its immutable
     ID. Persist each owned tag/ID after acquisition.
  8. Write initial `controller.json`, `lab-observations.jsonl`, and atomic
     `controller-state.json`; `controller.json` includes the exact raw
     credential path, its hash, and the controller machine-ID hash so maintained
     node launchers never guess either identity. None contains the token or
     service-env bytes.
  9. Call `ArtifactEnvironment.start_topology(lab_compose_file,
     immutable_compose_environment)`. The runtime environment contains image
     IDs/repo digests, secret-file paths, labels, and ports but no token.
  10. Resolve Docker-assigned loopback ports, update `ControllerConfig`, and
      require `owned_resources = environment.owned_resources()`. Persist the
      returned tuple plus every prebuilt/fixture image `OwnedResource`.
  11. Render Compose evidence with
      `docker compose config --no-env-resolution --resolve-image-digests`.
      Reject the rendered bytes if they contain the token, then write them and
      their SHA-256 to the bundle.
  12. Record Git commit/dirty flag/source hash; secret-free controller argv;
      start time; declared host ID; machine-ID hash; OS and architecture;
      Python, Docker, Compose, Git, Node, npm, and Playwright versions; image
      IDs/repo digests; bound and advertised addresses.
  13. Run preflight, write `preflight.json`, and call `require_valid()`.
  14. Persist `phase="active"` and print only the config path, credential path,
      dashboard URL, and `controller: READY`. Automation reads the same
      credential path from `controller.json`; no command assumes
      `state_dir/nats.creds`.

  Wrap Steps 3-13 in `try/except BaseException/finally`. On failure, persist
  `phase="failed"`, use the still-live `ArtifactEnvironment.cleanup()`, remove
  every journaled app/fixture image and export, verify the recorded tuple is
  gone, remove the raw credential, service env, and generated private config in
  `finally`, then write `cleanup.json`. Only after secret removal is recorded may
  the controller call `finalize_bundle(bundle, invalid_manifest, schema_path)`
  exactly once with `invalid_manifest["status"] == "INVALID"`.
  Finalizer failure never bypasses secret cleanup, and rollback failure is
  reported without replacing the original error.

- [ ] **Step 8: Implement fresh-process stop and manifest finalization**

  `stop_controller` loads `controller-state.json`; it does not rely on an
  in-memory `ArtifactEnvironment`. It accepts `starting`, `active`, `stopping`,
  or `failed`, changes the phase to `stopping`, and skips every cleanup step
  already named in `completed_cleanup_steps`:

  1. Snapshot authenticated current inventory, append-only reservation events,
     and bound node reports into the bundle while the raw credential exists.
     Snapshot the append-only controller observation journal.
  2. Run exact Compose down using the persisted project, file, and non-secret
     environment:

     ```text
     docker compose --project-name PROJECT --file FILE
       down --volumes --remove-orphans
     ```

  3. Invoke Slice 1's settled recovery CLI as an argv-only subprocess with the
     persisted non-secret Compose environment:

     ```text
     scripts/research/run-python scripts/research/run_artifact.py cleanup
       --run-id RUN_ID --scratch-root ABSOLUTE_ARTIFACT_SCRATCH_ROOT
     ```

     Require exit zero and independently require its run scratch, mutable state,
     raw credential, and owner record absent. Scan captured output for the token
     before retaining only exit status. This second idempotent ownership pass
     covers interruption inside Slice 1 after its owner-record write; it never
     removes controller evidence.
  4. Remove only persisted aggregator/dashboard/fixture tags and IDs plus every
     journaled export path with explicit image-removal argv. Never pass an
     implicit Compose image-removal flag: Slice 1 ownership forbids it,
     and every Slice 3 image is removed from the recovery journal by identity.
  5. Remove the service env and generated private NATS config; require the raw
     credential already absent from Slice 1 cleanup.
  6. Inspect every persisted `OwnedResource` and require none remains.
  7. Compare pre/post Docker inventories and require no pre-existing foreign
     resource disappeared.
  8. Write `cleanup.json` with `complete`, `attempted`,
     `remaining`, `owned_resources_removed`, `foreign_resources_touched`,
     `credential_removed`, `artifact_state_removed`,
     `artifact_scratch_removed`, `artifact_recovery_record_removed`, and
     `completed_at`.
  9. Build the complete lab manifest from recorded facts. Its `lab_variant`
     comes only from the persisted `state.config.lab_variant`; stop/finalization
     never infers a variant from node count or file presence. `command.argv` is
     a list of argv lists containing only the portable placeholders above and no
     secrets. Normalize Compose and every retained JSON string before writing,
     then reject any surviving absolute transient path. `controller`, `nodes`,
     and `observations` use the exact lab branch; no benchmark-only field is
     present.
  10. Call `require_complete_lab_manifest`. On a complete successful lifecycle,
      call `finalize_bundle(bundle_dir, manifest, schema_path)` exactly once and
      require `PASS`. On a failed/incomplete lifecycle, set `status="INVALID"`
      and call the finalizer exactly once. Persist finalizer completion as its
      own cleanup step.
  11. Persist `phase="stopped"` and return the cleanup object.

  If stopped state and a valid `cleanup.json` already exist, return it without a
  Docker call or second finalization. If a process dies after finalization but
  before the stopped phase write, recovery detects the existing immutable
  manifest, verifies it with
  `check_bundle(..., expected_kind="lab", source_root=repo_root.resolve())`,
  records the finalizer step, and transitions to stopped without finalizing
  again.

  `status --json` reads only the journal and sanitized cleanup. `export-image`
  runs `docker image save` on `fixture_image_id`, fsyncs the tar, writes a
  machine-readable result containing output path/image ID/SHA-256, journals the
  export for teardown, and refuses an active export path or mutable image tag.

- [ ] **Step 9: Run GREEN, shared regressions, and Compose validation**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_controller.py \
    tests/research/test_lab_evidence.py \
    tests/research/test_artifact_env.py tests/research/test_preflight.py \
    tests/research/test_evidence.py -q
  CHECK_DIR="$(mktemp -d /tmp/edgecitadel-compose.XXXXXX)"
  trap 'rm -rf "$CHECK_DIR"' EXIT
  umask 077
  scripts/research/run-python - <<'PY' > "$CHECK_DIR/nats.creds"
  import secrets
  print(secrets.token_hex(32))
  PY
  RAW_TOKEN="$(tr -d '\n' < "$CHECK_DIR/nats.creds")"
  printf 'NATS_TOKEN=%s\n' "$RAW_TOKEN" > "$CHECK_DIR/service.env"
  unset RAW_TOKEN
  cp scripts/research/nats-lab.conf.tpl "$CHECK_DIR/nats.conf"
  mkdir "$CHECK_DIR/data"
  NATS_IMAGE="$(scripts/research/run-python -c \
    'import json; print(json.load(open("scripts/research/toolchain.json"))["nats_image"])')"
  NGINX_IMAGE="$(scripts/research/run-python -c \
    'from scripts.research.lab_runtime import LAB_NGINX_IMAGE; print(LAB_NGINX_IMAGE)')"
  test "${NATS_IMAGE#*@sha256:}" != "$NATS_IMAGE"
  test "${NGINX_IMAGE#*@sha256:}" != "$NGINX_IMAGE"
  PLACEHOLDER_DIGEST="sha256:0000000000000000000000000000000000000000000000000000000000000000"
  LAB_RUN_ID=ec-lab-check \
  LAB_BIND_HOST=127.0.0.1 \
  LAB_HTTP_PORT= \
  LAB_NATS_PORT= \
  LAB_MONITOR_PORT= \
  LAB_SERVICE_ENV_FILE="$CHECK_DIR/service.env" \
  LAB_NATS_CONFIG="$CHECK_DIR/nats.conf" \
  LAB_DATA_DIR="$CHECK_DIR/data" \
  LAB_TOKEN_SHA256=0000000000000000000000000000000000000000000000000000000000000000 \
  LAB_NATS_IMAGE="$NATS_IMAGE" \
  LAB_AGGREGATOR_IMAGE="$PLACEHOLDER_DIGEST" \
  LAB_DASHBOARD_IMAGE="$PLACEHOLDER_DIGEST" \
  LAB_NGINX_IMAGE="$NGINX_IMAGE" \
    docker compose -f scripts/research/docker-compose.lab.yml config \
      --no-env-resolution --quiet
  ```

  Expected: exactly 16 lab controller and 10 lab evidence tests pass; Slice 1
  regression files have zero failures and zero new skips; Compose exits zero
  with no output.

- [ ] **Step 10: Commit Task 3**

  Invoke `commit-check`, require `git diff --cached --check` to exit zero, then:

  ```bash
  git add -- scripts/research/nats-lab.conf.tpl \
    scripts/research/docker-compose.lab.yml \
    scripts/research/lab_contract.py scripts/research/lab_observations.py \
    scripts/research/lab_preflight.py scripts/research/lab_controller.py \
    tests/research/test_lab_controller.py tests/research/test_lab_evidence.py \
    nginx/default.conf schemas/research-manifest.v1.json \
    scripts/research/check_artifact.py
  git diff --cached -- schemas/research-manifest.v1.json \
    scripts/research/check_artifact.py
  git commit -m "feat(infra): add recoverable lab controller"
  verify_canonical_snapshot
  ```

---

### Task 4: Reserve And Run Deterministic Nodes In The Pinned Image

**Files:**
- Create: `scripts/research/lab_node.py`
- Create: `tests/research/test_lab_node.py`

**Interfaces:**
- Consumes: Task 1 `ControllerConfig`, Task 2 reservation routes, the Slice 1
  `NativeControlConfig` and `build_agent_card`.
- Produces:

  ```python
  @dataclass(frozen=True)
  class NodeState:
      schema_version: str
      phase: Literal["starting", "active", "retained", "stopping", "released"]
      run_id: str
      agent_id: str
      qualified_agent_id: str
      reservation_id: str
      declared_host_id: str
      machine_id_sha256: str
      container_id: str
      container_name: str
      fixture_image_id: str
      config_path: Path
      state_dir: Path
      log_path: Path
      reservation_state: Literal["active", "retained", "released"]
      started_at: str

  def build_fixture_config(
      *,
      run_id: str,
      agent_id: str,
      behavior: str,
      delay_ms: int,
      crash_point: str | None,
  ) -> NativeControlConfig: ...

  def build_fixture_create_argv(
      *,
      controller: ControllerConfig,
      credential_file: Path,
      node_state_dir: Path,
      config_path: Path,
      container_name: str,
  ) -> tuple[str, ...]: ...
  ```

- The fixture config uses the logical `agent_id`, the run ID, and container
  paths `/run/state/outcomes.sqlite` and `/run/state/side-effects.sqlite`.
  `qualified_agent_id` is the reservation/container/state ownership key.

- [ ] **Step 1: Write eleven failing node tests**

  Write exactly:

  1. Default fixture config serializes `crash_point` as JSON null.
  2. Docker create argv uses the exact immutable image ID and never a tag.
  3. Docker create environment contains exactly `NATS_URL` and
     `EC_CREDENTIAL_FILE=/run/secrets/transport-token`, with no token or
     authorization header. It includes `--network host`, contains no
     `--add-host`, uses `ControllerConfig.nats_url` byte-for-byte, permits its
     loopback form only when the local machine hash equals
     `controller_machine_id_sha256`, and rejects remote-loopback before an
     inventory or Docker call.
  4. An existing active local state is rejected before an inventory request.
  5. Inventory 409 is reported before Docker create.
  6. Docker create/start/readiness failure removes a partial container and rolls
     a new reservation back to absent.
  7. Failed reconnect returns the original reservation to retained, not absent
     or active.
  8. Doctor publishes explicit host identity, machine fingerprint, observed
     network path, fixture image ID, matching reservation ID, and no secret.
  9. Node state writes are atomic and a fresh process recovers `starting`,
     `retained`, and `stopping` without orphaning a container or reservation.
  10. A contender using a distinct `--state-root` reaches inventory, receives
      409 for the active logical ID, and makes no Docker call.
  11. Final normal stop scans then removes the exact container, private config,
      SQLite state, log, state JSON, and immutable image when no other local
      run container uses it; retained stop preserves restart state/image. A
      remote no-state stop after an interrupted image load removes the exact
      unused imported image, while the same no-state stop on the controller
      machine preserves its journaled image.

  The environment assertion is:

  ```python
  rendered = "\0".join(argv)
  assert "NATS_URL=nats://127.0.0.1:14222" in rendered
  assert "EC_CREDENTIAL_FILE=/run/secrets/transport-token" in rendered
  assert fixture_environment_names(argv) == {"NATS_URL", "EC_CREDENTIAL_FILE"}
  assert credential_token(credential_file) not in rendered
  ```

- [ ] **Step 2: Run RED**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_node.py -q
  ```

  Expected: collection fails because `scripts.research.lab_node` is absent.

- [ ] **Step 3: Implement the node CLI and exact fixture config**

  The CLI is:

  ```text
  start --controller-config PATH --credential-file PATH
        --host-id controller-lab-01 --agent-id fixture-1
        --behavior echo --delay-ms 125 [--state-root PATH]
  status --controller-config PATH --agent-id fixture-1
         [--state-root PATH] --json
  doctor --controller-config PATH --credential-file PATH
         --host-id controller-lab-01 --agent-id fixture-1
         [--state-root PATH] --publish
  stop --controller-config PATH --credential-file PATH
       --agent-id fixture-1 [--state-root PATH] [--retain-reservation]
  ```

  `--crash-point` is optional. Omission passes `None`; explicit values are only:

  ```text
  after-receive-before-handler
  after-side-effect-before-ledger-prepare
  after-ledger-prepare-before-result-publish
  after-result-publish-before-publish-mark
  after-publish-mark-before-inbound-commit
  during-handler-exception-conversion
  ```

  Construct:

  ```python
  NativeControlConfig(
      run_id=run_id,
      agent_id=agent_id,
      mode="edgecitadel",
      behavior=behavior,
      delay_ms=delay_ms,
      crash_point=crash_point,
      heartbeat_interval_ms=1000,
      outcome_db="/run/state/outcomes.sqlite",
      side_effect_db="/run/state/side-effects.sqlite",
  )
  ```

  Write it through `write_private_json`. The JSON key is `null` when no crash is
  requested; do not encode a sentinel string.

- [ ] **Step 4: Run the immutable fixture container with rollback**

  Before reservation, require the raw credential hash to match
  `ControllerConfig.credential_sha256`, inspect the exact
  `ControllerConfig.fixture_image_id`, validate controller reachability, and
  reject an active local state. Compute the local machine-ID hash before those
  calls. If `ControllerConfig.nats_url` is loopback, require that hash to equal
  `controller_machine_id_sha256`; otherwise fail before reservation. This is the
  explicit Linux host-network contract: each same-host node container shares
  the Ubuntu host namespace and reaches the controller's published loopback
  port, while a remote host must use the non-loopback advertised address.

  Reserve, then run this exact shape as an argv tuple:

  ```text
  docker create
    --name edgecitadel-node-QUALIFIED_ID
    --network host
    --label ai.edgecitadel.owner=research-lab-node
    --label ai.edgecitadel.run-id=RUN_ID
    --label ai.edgecitadel.qualified-agent-id=QUALIFIED_ID
    --env NATS_URL=NATS_URL
    --env EC_CREDENTIAL_FILE=/run/secrets/transport-token
    --mount type=bind,src=CONFIG,dst=/run/config/native-control.json,readonly
    --mount type=bind,src=CREDENTIAL,dst=/run/secrets/transport-token,readonly
    --mount type=bind,src=STATE_DIR,dst=/run/state
    --read-only
    --tmpfs /tmp:rw,noexec,nosuid,size=16m
    FIXTURE_IMAGE_ID
    python3 -m scripts.research.fixtures.native_control
    --config /run/config/native-control.json
  ```

  Then call `docker start CONTAINER_ID`, wait for the exact logical agent ID to
  be online, and atomically persist `NodeState`. Capture logs with
  `docker logs CONTAINER_ID` into the node log only after scanning each chunk for
  the token.

  If create, start, or readiness fails, stop/remove the exact partial container.
  Delete a new reservation; change a resumed reservation back to retained.
  Remove the private config and state only after rollback completes. On a
  different machine from the controller, also remove the exact imported fixture
  image if no other labeled run container uses it; on the controller machine,
  leave the controller-owned image journal intact. Never leave the inventory
  active or an imported remote image behind when no matching container is
  running.

  `stop --retain-reservation` stops/removes the verified labeled container,
  patches the reservation to retained, atomically persists `phase="retained"`,
  and preserves config/state/image for reconnect. A normal stop first scans the
  in-memory log/token, updates its bound node report with a secret-free cleanup
  summary, deletes the matching reservation, removes private
  config/SQLite/log/state JSON, and removes the exact immutable image only when
  no other local container with the run label exists. A repeated normal stop
  with no state still compares the local and controller machine hashes; on a
  remote machine it removes the exact `ControllerConfig.fixture_image_id` when
  no labeled run container uses it, covering interruption immediately after
  `docker load`. On the controller machine it preserves the controller-owned
  image. It then prints `node: already stopped` and exits zero. Fresh-process
  recovery resumes each partial phase idempotently.

- [ ] **Step 5: Implement doctor and publish observed facts**

  Doctor checks exactly:

  ```text
  ubuntu_24_04
  x86_64
  docker_engine
  fixture_image_exact
  clock_synchronized
  controller_dns_resolves
  controller_route_present
  nats_tcp_reachable
  nats_authentication
  agent_card_valid
  fixture_container_matches_state
  heartbeat_fresh
  ```

  The declared host ID comes only from required `--host-id`. Compute
  `machine_id_sha256` from `/etc/machine-id`; never hash the checkout path.
  Require the launcher's `LAB_SOURCE_PATHS` clean, record its Git commit and
  source snapshot, and require both equal the values in `controller.json`.
  Parse `ip route get CONTROLLER_IP` into:

  ```json
  {
    "source_ip": "100.64.10.11",
    "destination_ip": "100.64.10.10",
    "interface": "tailscale0",
    "route_output_sha256": "64-lowercase-hex",
    "controller_dns_name": "controller-lab.internal"
  }
  ```

  Resolve `CONTROLLER_IP` from the advertised host in `ControllerConfig`.
  `controller_dns_name` records that exact advertised host string; an IP literal
  is allowed and satisfies the resolution check without DNS. When it is a DNS
  name, require one selected route destination and record its resolved IP in the
  controller facts. Remote qualification compares `destination_ip` with that
  resolved advertised IP, never with an unresolved name.

  Include the current `reservation_id`, publish this secret-free report through
  Task 2, and require the response to repeat the same reservation binding. The
  server adds top-level `server_observed_peer_ip`; the self-reported
  `network_path` never contains that field.

- [ ] **Step 6: Run GREEN and fixture regressions**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_node.py \
    tests/research/test_native_control.py -q
  ```

  Expected: exactly 11 node tests pass; Slice 1 fixture tests have zero failures
  and zero new skips.

- [ ] **Step 7: Commit Task 4**

  Invoke `commit-check`, require `git diff --cached --check` to exit zero, then:

  ```bash
  git add -- scripts/research/lab_node.py tests/research/test_lab_node.py
  git commit -m "feat(infra): run pinned deterministic lab nodes"
  verify_canonical_snapshot
  ```

---

### Task 5: Prove The Two-Node Lifecycle And Exact Slice 2 Journey

**Files:**
- Create: `scripts/research/lab_gate.py`
- Create: `tests/research/test_lab_lifecycle.py`
- Create: `tests/research/test_lab_commands.py`
- Modify: `scripts/research/lab_controller.py`

**Interfaces:**
- Consumes: the maintained controller and node CLIs from Tasks 3 and 4, relative
  aggregator APIs beneath `ControllerConfig.agg_url`, and the exact Slice 2
  contract.
- Produces:

  ```python
  @dataclass(frozen=True)
  class LifecycleResult:
      run_id: str
      project: str
      ports: tuple[int, int, int]
      subject_scope: frozenset[tuple[str, str]]
      consumer_names: frozenset[str]
      state_paths: frozenset[Path]
      task_ids: tuple[str, ...]
      terminal_outputs: tuple[str, ...]
      doctor_reports: tuple[Mapping[str, object], ...]
      bundle: Path
      cleanup: Mapping[str, object]

  def run_two_node_lifecycle(
      *, repo_root: Path, run_id: str, host_id: str
  ) -> LifecycleResult: ...

  def run_operator_journey(
      *, repo_root: Path, run_id: str, host_id: str
  ) -> CompletedProcess[str]: ...

  def relocate_slice2_media(
      *, repo_root: Path, bundle: Path
  ) -> dict[str, object]: ...
  ```

- Every helper executes the full CLI argv through `subprocess.run`; it does not
  call controller or node implementation functions directly.

- [ ] **Step 1: Write seven failing command/await and media unit tests**

  In `tests/research/test_lab_commands.py`, write these tests before modifying
  `lab_controller.py`:

  1. `command --wait --result-file` writes canonical JSON containing one task ID,
     HTTP 202 acceptance, one exact terminal, and the matching observation
     sequence.
  2. `command --no-wait --result-file` returns after accepted evidence and
     `await --result-file` later adds the exact terminal without changing task
     ID.
  3. `--wire-copies 2` publishes identical canonical envelope bytes twice
     without `Nats-Msg-Id`, records two wire submissions, one logical task ID,
     one handler execution, and one non-conflicting logical terminal.
  4. Unknown agents, unexpected HTTP 4xx/5xx, conflicting terminals, or queue
     residue fail without writing a success result.
  5. `await --qualification-kind queued-reconnect` requires the same reservation
     ID and append-only ordering
     `retained < accepted < resumed < terminal`.
  6. `command` and `await` refuse a finalized bundle before any network or file
     mutation; result files are `O_EXCL` and never contain credentials.
  7. `relocate_slice2_media` reads the root
     `playwright-results.json`, passes its two results through Slice 2
     `passed_project_results`, calls Slice 2 `copy_media`, replaces the report
     through shared `write_json` with the returned portable mapping, and returns
     that mapping. It requires exactly four PNGs, two metadata JSON files, eight
     API JSON files, two WebM files, and two trace archives at the settled
     desktop/mobile paths; each portable project has exactly the five Slice 2
     attachment names and the two metadata task IDs are distinct.

- [ ] **Step 2: Write two failing integration tests**

  Add exactly two `@pytest.mark.lab_integration` tests:

  ```python
  def test_two_node_command_disconnect_queue_reconnect_and_duplicate_rejection():
      result = run_two_node_lifecycle(
          repo_root=REPO_ROOT,
          run_id=f"lab-life-{uuid.uuid4().hex[:8]}",
          host_id="controller-lab-01",
      )
      assert len(result.task_ids) == 4
      assert len(set(result.task_ids)) == 4
      assert result.terminal_outputs == (
          f"edgecitadel:{result.run_id}:fixture-1",
          f"edgecitadel:{result.run_id}:fixture-2",
          f"edgecitadel:{result.run_id}:duplicate-fixture-2",
          f"edgecitadel:{result.run_id}:queued-fixture-1",
      )
      assert len(result.doctor_reports) >= 3
      assert result.cleanup["owned_resources_removed"] is True


  def test_exact_slice2_operator_journey_targets_shell_1_once():
      completed = run_operator_journey(
          repo_root=REPO_ROOT,
          run_id=f"lab-ui-{uuid.uuid4().hex[:8]}",
          host_id="controller-lab-01",
      )
      assert completed.returncode == 0
      assert "1 passed" in completed.stdout
  ```

- [ ] **Step 3: Run RED**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_commands.py -q
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_lifecycle.py \
    -m lab_integration -x -q
  ```

  Expected: command tests fail because controller command APIs are absent and
  lifecycle collection fails because `scripts.research.lab_gate` is absent.

- [ ] **Step 4: Implement machine-readable command and await**

  Add these exact controller forms before implementing the gate:

  ```text
  command --run-id RUN_ID --agent-id AGENT --body NONCE
          --expected-output OUTPUT (--wait | --no-wait)
          [--wire-copies 1|2] --result-file PATH
  await --run-id RUN_ID --task-id TASK_ID --expected-output OUTPUT
        [--qualification-kind queued-reconnect] --result-file PATH
  ```

  `--wire-copies 1` uses the production HTTP command API. `--wire-copies 2`
  constructs one schema-valid canonical command envelope with one UUID task ID
  and publishes the identical bytes twice to the exact inbox without a
  `Nats-Msg-Id` header. It uses the raw credential from the journal in process
  memory only. Both paths append accepted/wire/terminal observations and query
  `/api/messages?task_id=...` plus `/api/agents/<agent>/queue`; they apply the
  Section 2.3 terminal reducer and require one non-conflicting logical terminal,
  drained pending/ack-pending counts, and the exact expected output.

  Result files are canonical, `O_EXCL`, and contain:

  ```json
  {
    "run_id": "lab-life-12345678",
    "agent_id": "fixture-1",
    "task_id": "uuid",
    "wire_copies": 1,
    "accepted_at": "UTC timestamp",
    "terminal_at": "UTC timestamp or null",
    "expected_output": "edgecitadel:nonce",
    "status": "accepted or completed"
  }
  ```

  `await --qualification-kind queued-reconnect` reads the immutable reservation
  event snapshot/API and requires the same reservation ID across retained and
  resumed events with strict timestamp/sequence ordering. Both commands check
  that `manifest.json` is absent before any mutation.

- [ ] **Step 5: Implement the complete two-node lifecycle**

  `run_two_node_lifecycle` omits all three ports for loopback, allowing Docker to
  assign them atomically, and executes:

  ```text
  scripts/research/run-python scripts/research/lab_controller.py start
    --run-id RUN_ID --host-id controller-lab-01 --lab-variant lifecycle
  scripts/research/run-python scripts/research/lab_node.py start
    --controller-config CONTROLLER_JSON --credential-file CREDENTIAL
    --host-id controller-lab-01 --agent-id fixture-1
    --behavior echo --delay-ms 250
  scripts/research/run-python scripts/research/lab_node.py start
    --controller-config CONTROLLER_JSON --credential-file CREDENTIAL
    --host-id controller-lab-01 --agent-id fixture-2
    --behavior echo --delay-ms 250
  ```

  Use the absolute checkout-local `scripts/research/run-python` path in the
  actual argv, never an arbitrary interpreter.
  After both logical IDs are simultaneously online, run `doctor --publish` for
  each and require the response reservation/host binding. Construct:

  ```python
  first_body = f"{run_id}:fixture-1"
  second_body = f"{run_id}:fixture-2"
  queued_body = f"{run_id}:queued-fixture-1"
  duplicate_body = f"{run_id}:duplicate-fixture-2"
  ```

  Send each nonce string through `lab_controller command --result-file`, with no
  shell syntax. The first two use one wire copy. `duplicate_body` uses two
  identical wire copies and must expose one logical task/terminal and one handler
  execution. For each task ID, require the exact output and queue
  `pending == 0` plus `ack_pending == 0`.

  Stop `fixture-1 --retain-reservation`, immediately submit `queued_body` before
  the offline threshold, and require HTTP 202. Restart the same node with the
  same retained reservation, run `doctor --publish` again, then call
  `lab_controller await --qualification-kind queued-reconnect` with the task ID
  read from the no-wait result JSON.

  While `fixture-2` is active, run its identical start from a distinct temporary
  `--state-root` and declared contender host. This bypasses only the local-state
  guard, reaches inventory, requires 409 text
  `agent_id has an active reservation`, and proves Docker container count did
  not increase. Remove the contender root in `finally`.

  Record observed broker endpoint plus subject as
  `tuple[str, str]`, consumer names, state paths, task IDs, and outputs. Always
  stop both nodes and the controller in `finally`; a failed assertion must not
  bypass cleanup. Controller stop is the sole finalizer. After stop, call
  `check_bundle(... expected_kind="lab" ...)`, call `require_valid()`, and return
  the bundle path; never call `finalize_bundle` from `lab_gate.py`.

- [ ] **Step 6: Implement the unchanged Slice 2 journey**

  Use a separate controller start with `--lab-variant operator-smoke` containing
  exactly one node:

  ```text
  --agent-id shell-1
  --behavior echo
  --delay-ms 1000
  ```

  Run `doctor --publish` for `shell-1` before Playwright. The controller and node
  are always stopped in `finally`, and the controller bundle is checked after its
  single finalization.

  For evidence mode, `relocate_slice2_media` loads the original JSON report,
  computes `results = passed_project_results(report)`, computes
  `portable_report = copy_media(repo_root, bundle, results)`, and calls
  `write_json(bundle / "playwright-results.json", portable_report)`. It then
  enforces the exact Slice 2 counts, portable bundle-relative attachment paths,
  distinct project task IDs, and project-local metadata/API/media correlation
  before controller stop can finalize the lab bundle.

  Invoke:

  ```python
  subprocess.run(
      [
          "npx", "--no-install", "playwright", "test",
          "--config", "playwright.config.js",
          "tests/operator-journey.spec.js",
      ],
      cwd=repo_root / "e2e",
      env={
          **os.environ,
          "APP_URL": controller.app_url,
          "AGG_URL": controller.agg_url,
      },
      check=False,
      text=True,
      capture_output=True,
  )
  ```

  Do not use Slice 3-only controller or target-agent environment variables, or a
  root-directory config path. The one `chromium` project executes the operator
  spec exactly once. Before stop, query the one completed operator task through
  the controller API and write portable `playwright-smoke.json` with the exact
  argv, normalized `cwd`, return code, `1 passed` assertion, task/context/hop
  identity, nonce, and output. This is the `operator-smoke` observation; it is
  not a substitute media report and contains no absolute path or stderr secret.

- [ ] **Step 7: Run GREEN twice**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_commands.py -q
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_lifecycle.py \
    -m lab_integration -q
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_commands.py -q
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_lifecycle.py \
    -m lab_integration -q
  ```

  Expected from each invocation: 7 unit tests and exactly 2 integration tests
  pass. Across both invocations,
  four distinct run IDs, projects, port triples, task-ID sets, and cleanup
  records are observed.

- [ ] **Step 8: Commit Task 5**

  Invoke `commit-check`, require `git diff --cached --check` to exit zero, then:

  ```bash
  git add -- scripts/research/lab_gate.py tests/research/test_lab_commands.py \
    tests/research/test_lab_lifecycle.py scripts/research/lab_controller.py
  git diff --cached -- scripts/research/lab_controller.py
  git commit -m "test(infra): prove two-node lab lifecycle"
  verify_canonical_snapshot
  ```

---

### Task 6: Prove Concurrent, Sequential, Evidence, And Clean-Checkout Isolation

**Files:**
- Modify: `scripts/research/lab_gate.py`
- Modify: `tests/research/test_lab_lifecycle.py`

**Interfaces:**
- Consumes: `LifecycleResult` and full CLI orchestration from Task 5.
- Produces:

  ```python
  def run_concurrent_pair(
      *, repo_root: Path, run_ids: tuple[str, str], host_id: str
  ) -> tuple[LifecycleResult, LifecycleResult]: ...

  def run_sequential_pair(
      *, repo_root: Path, run_ids: tuple[str, str], host_id: str
  ) -> tuple[LifecycleResult, LifecycleResult]: ...

  def assert_disjoint_runs(
      left: LifecycleResult, right: LifecycleResult
  ) -> None: ...
  ```

- A pair means exactly two complete runs. The concurrent test starts both
  controllers before either completes. The sequential test completes and cleans
  the first before starting the second.

- [ ] **Step 1: Add three failing integration tests**

  Add exactly:

  1. `test_two_concurrent_full_lifecycles_are_disjoint`
  2. `test_two_sequential_full_lifecycles_are_disjoint`
  3. `test_repeated_cleanup_preserves_foreign_resource_and_secret_hygiene`

  For both pairs, assert:

  ```python
  assert left.project != right.project
  assert set(left.ports).isdisjoint(right.ports)
  assert left.subject_scope.isdisjoint(right.subject_scope)
  assert left.consumer_names.isdisjoint(right.consumer_names)
  assert left.state_paths.isdisjoint(right.state_paths)
  assert set(left.task_ids).isdisjoint(right.task_ids)
  assert set(left.terminal_outputs).isdisjoint(right.terminal_outputs)
  assert left.cleanup["owned_resources_removed"] is True
  assert right.cleanup["owned_resources_removed"] is True
  ```

  Use run-specific terminal nonces so result sets are actually disjoint. Query
  each controller before cleanup and assert it contains none of the other run's
  task IDs or outputs.

  The cleanup test creates one Docker volume labeled
  `ai.edgecitadel.owner=foreign-control`, stops the run twice from separate CLI
  processes, proves the foreign volume still exists, and scans every node log,
  raw evidence file, manifest, Docker inspect argv, and state JSON for the
  generated token. Its outermost `finally` removes that exact foreign volume so
  the test itself leaves no resource.

- [ ] **Step 2: Run RED**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_lifecycle.py \
    -m lab_integration -k "concurrent or sequential or repeated_cleanup" -q
  ```

  Expected: exactly 3 tests fail because the pair helpers and complete
  assertions are absent.

- [ ] **Step 3: Implement full paired runs**

  `run_concurrent_pair` uses two worker threads only to execute two complete
  `run_two_node_lifecycle` calls. It joins both, propagates both exceptions, and
  runs cleanup for both run IDs even if one thread fails.

  `run_sequential_pair` calls the same complete lifecycle twice. It verifies the
  first controller, nodes, containers, network, volumes, fixture image, state
  reservations, private node paths, and JetStream consumers are gone before the
  second start.

  `subject_scope` is a set of `(controller.nats_url, observed_subject)` pairs.
  Identical subject text on isolated brokers is therefore represented by
  distinct full scopes. Consumer names themselves must still include the run ID
  and be disjoint.

- [ ] **Step 4: Check each controller-finalized bundle without refinalizing**

  Before controller stop finalizes, require raw files for:

  ```text
  preflight.json
  compose.resolved.yml
  versions.json
  images.json
  identities.json
  network-paths.json
  commands.json
  inventory.json
  cleanup.json
  ```

  The manifest must record:

  - Compose SHA-256 and every service/fixture immutable image ID or repo digest.
  - Python, Docker, Compose, Git, Node, npm, and Playwright versions.
  - Secret-free controller, node, and Playwright argv plus their working
    directories.
  - Start, readiness, node connect/disconnect/reconnect, task accepted/terminal,
    teardown, and end UTC timestamps.
  - Logical agent ID, run-qualified ID, reservation ID, declared host ID,
    machine fingerprint, hostname, OS, and architecture for each node.
  - Advertised/bound addresses and observed route/peer facts.
  - Exact task IDs and terminal outputs for both nodes and the queued reconnect.
  - Complete cleanup and foreign-resource comparison.
  - All non-manifest artifact hashes produced by `finalize_bundle`.

  Controller stop has already called `require_complete_lab_manifest` and the
  sole `finalize_bundle` after `cleanup.json` exists. The gate must import no
  finalizer and require only:

  ```python
  report = check_bundle(
      bundle,
      expected_kind="lab",
      source_root=repo_root.resolve(),
  )
  report.require_valid()
  ```

  A failed lifecycle, missing field, residual resource, or leaked secret makes
  controller finalization `INVALID`. A raw mutation after finalization leaves
  the immutable manifest unchanged and makes `CheckReport.valid` false.

- [ ] **Step 5: Run GREEN and validate all four pair bundles**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_lifecycle.py \
    -m lab_integration -q
  ```

  Expected: exactly 5 tests pass: the 2 Task 5 tests and the 3 Task 6 tests.
  The paired tests create exactly two concurrent and two sequential run bundles.
  Each bundle returns a valid `CheckReport`, `require_valid()` returns normally,
  and the controller finalizer spy count remains one.

- [ ] **Step 6: Add and run the clean-checkout lifecycle command**

  Add `--clean-checkout-gate`, `--receipt`, and optional `--retain-bundles` to
  `lab_gate.py`. It requires scoped status over
  `LAB_SOURCE_PATHS` to be empty before any output, runs one full two-node
  lifecycle plus the one-project Slice 2 operator journey, validates both
  bundles inside that source checkout. Derive exact child IDs
  `f"{run_id}-life"` and `f"{run_id}-ui"` and validate both before creating
  output. Write a canonical receipt with exactly:

  ```json
  {
    "schema_version": "1",
    "source_commit": "40-lowercase-hex",
    "source_snapshot_sha256": "64-lowercase-hex",
    "bundles": {
      "lifecycle": {
        "path": "receipt-parent-relative POSIX path",
        "checker_valid": true,
        "finalizer_count": 1
      },
      "operator_smoke": {
        "path": "receipt-parent-relative POSIX path",
        "checker_valid": true,
        "finalizer_count": 1
      }
    },
    "cleanup": {
      "complete": true,
      "owned_resources_remaining": []
    }
  }
  ```

  When `--retain-bundles` is supplied, copy only already-finalized, checked
  bundles to that nonexisting directory and update receipt paths; never mutate a
  bundle after finalization. Every receipt path is relative to the receipt's
  parent, uses POSIX separators, resolves below that parent, and contains no
  clean-clone, task-worktree, or temporary absolute root.

  Exercise it from a true local clone:

  ```bash
  cd "$TASK_ROOT"
  CLEAN_ROOT="$(mktemp -d /tmp/edgecitadel-clean.XXXXXX)"
  GATE_COMMIT="$(git stash create)"
  test -n "$GATE_COMMIT"
  GATE_REF="refs/codex/lab-gate-$$"
  git update-ref "$GATE_REF" "$GATE_COMMIT"
  trap 'git update-ref -d "$GATE_REF"; rm -rf "$CLEAN_ROOT"' EXIT
  git bundle create "$CLEAN_ROOT/lab-gate.bundle" "$GATE_REF"
  git clone "$CLEAN_ROOT/lab-gate.bundle" "$CLEAN_ROOT/edge-research"
  git -C "$CLEAN_ROOT/edge-research" checkout --detach "$GATE_COMMIT"
  test -z "$(git -C "$CLEAN_ROOT/edge-research" status --porcelain)"
  "$CLEAN_ROOT/edge-research/scripts/research/run-python" -c \
    'import sys; assert sys.version_info[:2] == (3, 12)'
  npm --prefix "$CLEAN_ROOT/edge-research/e2e" ci
  test "$(node --version)" = "v24.6.0"
  test "$(npm --version)" = "11.5.1"
  test "$(node -p "require('$CLEAN_ROOT/edge-research/e2e/node_modules/@playwright/test/package.json').version")" \
    = "1.58.2"
  npm --prefix "$CLEAN_ROOT/edge-research/e2e" exec -- \
    playwright install chromium
  "$CLEAN_ROOT/edge-research/scripts/research/run-python" \
    "$CLEAN_ROOT/edge-research/scripts/research/lab_gate.py" \
    --clean-checkout-gate --repo-root "$CLEAN_ROOT/edge-research" \
    --run-id lab-clean-01 --host-id controller-lab-01 \
    --receipt "$CLEAN_ROOT/clean-gate-receipt.json"
  "$CLEAN_ROOT/edge-research/scripts/research/run-python" -m json.tool \
    "$CLEAN_ROOT/clean-gate-receipt.json" >/dev/null
  ```

  Expected: exactly one two-node lifecycle and one `chromium` operator test
  pass; both generated bundles pass the checker; cleanup leaves zero owned
  resources. The fixture still runs by immutable Docker image ID, not by the
  virtualenv interpreter. `git stash create` does not alter the working tree; the
  temporary ref makes the complete Task 6 source reachable to the clean clone
  before Task 6 is committed. This precommit receipt is a development gate, not
  retained publication evidence. Task 8 repeats the gate from committed `HEAD`,
  retains its source bundle and finalized evidence, and validates against a
  checkout reconstructed from that source bundle.

- [ ] **Step 7: Commit Task 6**

  Invoke `commit-check`, require `git diff --cached --check` to exit zero, then:

  ```bash
  git add -- scripts/research/lab_gate.py tests/research/test_lab_lifecycle.py
  git commit -m "test(infra): isolate paired and clean lab runs"
  verify_canonical_snapshot
  ```

---

### Task 7: Require Complete Evidence For Remote Qualification

**Files:**
- Create: `scripts/research/lab_qualification.py`
- Create: `tests/research/test_lab_qualification.py`
- Create: `docs/setup-lab-node.md`
- Modify: `scripts/research/lab_controller.py`

**Interfaces:**
- Consumes: finalized lab manifest, `check_bundle`, Task 2 node reports, Task 3
  controller state, and Task 5 command observation format.
- Produces:

  ```python
  @dataclass(frozen=True)
  class LabQualification:
      status: Literal["preliminary", "remote-qualified"]
      same_host_two_node: bool
      remote_qualified: bool
      reasons: tuple[str, ...]

  def classify_lab(
      *,
      manifest: Mapping[str, object],
      check_report: CheckReport,
  ) -> LabQualification: ...
  ```

- `lab_controller.py qualify --run-id ec-remote-01` first calls
  `check_bundle(bundle, expected_kind="lab", source_root=repo_root.resolve())`
  and then `report.require_valid()`. It reads only the finalized bundle and
  never mutates finalized evidence.

- [ ] **Step 1: Write eight failing classifier and runbook tests**

  Write exactly:

  1. Two same-host reports are preliminary.
  2. Two checkout paths on one machine fingerprint remain preliminary.
  3. Distinct declared host IDs without distinct machine fingerprints remain
     preliminary.
  4. Distinct hosts with loopback, missing, or peer-mismatched route facts remain
     preliminary.
  5. Missing successful command to either host remains preliminary.
  6. Missing disconnect/accepted queue/reconnect/terminal ordering remains
     preliminary.
  7. Any invalid `CheckReport` or non-PASS manifest remains preliminary.
  8. A complete two-host manifest is remote-qualified and the runbook contains
     the Slice 1 hash-locked launcher bootstrap, exact interface binding,
     noninteractive strict SSH, source-commit equality,
     result-file task-ID extraction, remote node/image/path cleanup before
     credential deletion, controller cleanup, limitations, and labels; it
     contains no wildcard bind-host form.

  The positive fixture contains:

  ```python
  manifest["lab_variant"] = "lifecycle"
  controller = {
      "declared_host_id": "controller-lab-01",
      "machine_id_sha256": "1" * 64,
      "advertised_host": "100.64.10.10",
      "advertised_ip": "100.64.10.10",
      "agent_id": "shell-controller",
      "reservation_id": "reservation-controller-01",
      "launcher_source_commit": "3" * 40,
      "source_snapshot_sha256": "4" * 64,
  }
  remote = {
      "declared_host_id": "gateway-lab-02",
      "machine_id_sha256": "2" * 64,
      "agent_id": "shell-remote",
      "reservation_id": "reservation-remote-01",
      "launcher_source_commit": "3" * 40,
      "source_snapshot_sha256": "4" * 64,
      "server_observed_peer_ip": "100.64.10.11",
      "network_path": {
          "source_ip": "100.64.10.11",
          "destination_ip": "100.64.10.10",
          "interface": "tailscale0",
          "route_output_sha256": "5" * 64,
          "controller_dns_name": "controller-lab.internal",
      },
  }
  ```

- [ ] **Step 2: Run RED**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_qualification.py -q
  ```

  Expected: collection fails because
  `scripts.research.lab_qualification` is absent.

- [ ] **Step 3: Implement the fail-closed classifier**

  `remote_qualified` is true only when all conditions hold:

  1. `manifest["status"] == "PASS"`,
     `manifest["lab_variant"] == "lifecycle"`, and
     `check_report.valid is True`.
  2. Exactly one controller-host report and at least one second-host report are
     preflight-valid Ubuntu 24.04 x86_64 reports.
  3. Their declared host IDs differ and equal the IDs recorded by their node
     launch commands.
  4. Their machine-ID hashes differ.
  5. The remote source/destination are non-loopback, interface is not `lo`,
     source equals inventory-observed peer, and destination equals the
     controller's recorded resolved advertised IP.
  6. One command to the controller-host agent and one command to the remote-host
     agent each have HTTP 202, one exact terminal, no conflicting terminal, and
     distinct task IDs.
  7. Append-only reservation events prove remote retain precedes queued HTTP 202;
     queued acceptance precedes resume; resume precedes the exact terminal; the
     same reservation ID/host is retained and resumed.
  8. All referenced observation files occur in `manifest["artifacts"]` with
     hashes.
  9. Both node reports carry clean launcher-source commits/snapshots equal to the
     controller source provenance.

  Calculate `same_host_two_node` independently for the automated local gate.
  Never infer remote status from declared names, checkout paths, IP strings, or
  node count alone. Return every failed condition as a stable reason.

- [ ] **Step 4: Reuse command evidence and add immutable qualification**

  Reuse the machine-readable Task 5 command/await forms:

  ```text
  command --run-id ec-remote-01 --agent-id shell-controller
          --body controller-01
          --expected-output edgecitadel:controller-01 --wait
          --result-file tmp/research/ec-remote-01/controller-command.json
  command --run-id ec-remote-01 --agent-id shell-remote
          --body remote-01
          --expected-output edgecitadel:remote-01 --wait
          --result-file tmp/research/ec-remote-01/remote-command.json
  command --run-id ec-remote-01 --agent-id shell-remote
          --body queued-remote-01
          --expected-output edgecitadel:queued-remote-01 --no-wait
          --result-file tmp/research/ec-remote-01/queued-command.json
  await --run-id ec-remote-01 --task-id TASK_ID
        --expected-output edgecitadel:queued-remote-01
        --qualification-kind queued-reconnect
        --result-file tmp/research/ec-remote-01/queued-terminal.json
  qualify --run-id ec-remote-01
  ```

  Task 5 already proves `command` and `await`; do not reimplement them here.
  Extend only `qualify`, which is run after `stop` finalizes the bundle. It calls
  `check_bundle` with `expected_kind="lab"` and the absolute checkout root, then
  calls `report.require_valid()`. A caught validation failure is classified as
  preliminary and exits nonzero. Otherwise it prints exactly one line:

  ```text
  lab qualification: PRELIMINARY
  ```

  or:

  ```text
  lab qualification: REMOTE QUALIFIED
  ```

- [ ] **Step 5: Write the exact runbook**

  `docs/setup-lab-node.md` has these sections:

  ```text
  Supported Baseline
  Controller
  Same-Host Two-Node Gate
  Second Ubuntu Host Qualification
  Doctor
  Command And Reconnect Evidence
  Teardown And Qualification
  Security And Platform Limits
  ```

  Both hosts first verify an identical clean commit and warm the Slice 1
  hash-locked launcher. The remote SSH key and known-host entry are provisioned
  before this run; no command may prompt:

  ```bash
  cd /home/lab/edge-research
  test -z "$(git status --porcelain -- \
    .dockerignore aggregator frontend e2e nginx scripts/research schemas \
    docs/setup-lab-node.md \
    docs/research/task-aware-reliability-contract-design.md)"
  test "$(uv --version)" = "uv 0.8.13"
  scripts/research/run-python -c \
    'import sys; assert sys.version_info[:2] == (3, 12)'
  ```

  On the controller, execute one fail-closed shell with a cleanup trap. It binds
  only the Tailnet/LAN interface address, never `0.0.0.0`:

  ```bash
  set -euo pipefail
  umask 077
  cd /home/lab/edge-research
  REMOTE_HOST="gateway-lab.internal"
  REMOTE_REPO="/home/lab/edge-research"
  SSH_ARGS="-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10"
  ssh $SSH_ARGS "$REMOTE_HOST" true
  REMOTE_TMP="$(ssh $SSH_ARGS "$REMOTE_HOST" \
    'umask 077; mktemp -d /tmp/edgecitadel-remote.XXXXXX')"
  case "$REMOTE_TMP" in
    /tmp/edgecitadel-remote.*) ;;
    *) echo "unsafe remote path" >&2; exit 1 ;;
  esac
  REMOTE_STATE_ROOT="$REMOTE_TMP/state"
  CONTROLLER_CONFIG="$PWD/tmp/research/ec-remote-01/controller.json"
  LOCAL_CREDENTIAL=""
  REMOTE_TMP_CREATED=1
  REMOTE_FILES_READY=0

  cleanup_lab() {
    cleanup_rc=0
    if [ "$REMOTE_FILES_READY" -eq 1 ]; then
      if ! ssh $SSH_ARGS "$REMOTE_HOST" \
        "cd '$REMOTE_REPO' && \
         scripts/research/run-python scripts/research/lab_node.py stop \
           --controller-config '$REMOTE_TMP/controller.json' \
           --credential-file '$REMOTE_TMP/nats.creds' \
           --state-root '$REMOTE_STATE_ROOT' \
           --agent-id shell-remote"; then
        cleanup_rc=1
      fi
      if [ "$cleanup_rc" -eq 0 ]; then
        if ssh $SSH_ARGS "$REMOTE_HOST" "rm -rf '$REMOTE_TMP'"; then
          REMOTE_FILES_READY=0
          REMOTE_TMP_CREATED=0
        else
          cleanup_rc=1
        fi
      else
        echo "remote cleanup incomplete; recovery files retained at $REMOTE_TMP" >&2
      fi
    elif [ "$REMOTE_TMP_CREATED" -eq 1 ]; then
      if ssh $SSH_ARGS "$REMOTE_HOST" "rm -rf '$REMOTE_TMP'"; then
        REMOTE_TMP_CREATED=0
      else
        cleanup_rc=1
      fi
    fi
    if [ -f "$LOCAL_CREDENTIAL" ] && [ -f "$CONTROLLER_CONFIG" ]; then
      scripts/research/run-python scripts/research/lab_node.py stop \
        --controller-config "$CONTROLLER_CONFIG" \
        --credential-file "$LOCAL_CREDENTIAL" \
        --agent-id shell-controller || cleanup_rc=1
    fi
    scripts/research/run-python scripts/research/lab_controller.py stop \
      --run-id ec-remote-01 || cleanup_rc=1
    return "$cleanup_rc"
  }
  on_signal() {
    signal_status="$1"
    trap - EXIT HUP INT TERM
    cleanup_lab || true
    exit "$signal_status"
  }
  trap cleanup_lab EXIT
  trap 'on_signal 129' HUP
  trap 'on_signal 130' INT
  trap 'on_signal 143' TERM

  scripts/research/run-python scripts/research/lab_controller.py start \
    --run-id ec-remote-01 \
    --host-id controller-lab-01 \
    --lab-variant lifecycle \
    --bind-host 100.64.10.10 \
    --advertise-host 100.64.10.10 \
    --http-port 18080 --nats-port 14222 --monitor-port 18222 \
    --trusted-network-confirm
  LOCAL_CREDENTIAL="$(scripts/research/run-python -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["credential_file"])' \
    "$CONTROLLER_CONFIG")"
  test -f "$LOCAL_CREDENTIAL"
  scripts/research/run-python scripts/research/lab_node.py start \
    --controller-config "$CONTROLLER_CONFIG" \
    --credential-file "$LOCAL_CREDENTIAL" \
    --host-id controller-lab-01 \
    --agent-id shell-controller \
    --behavior echo --delay-ms 1000
  scripts/research/run-python scripts/research/lab_node.py doctor \
    --controller-config "$CONTROLLER_CONFIG" \
    --credential-file "$LOCAL_CREDENTIAL" \
    --host-id controller-lab-01 \
    --agent-id shell-controller --publish
  scripts/research/run-python scripts/research/lab_controller.py export-image \
    --run-id ec-remote-01 \
    --output /tmp/ec-remote-01-fixture.tar \
    --result-file tmp/research/ec-remote-01/export.json

  SOURCE_COMMIT="$(git rev-parse HEAD)"
  test "$(ssh $SSH_ARGS "$REMOTE_HOST" \
    "git -C '$REMOTE_REPO' rev-parse HEAD")" = "$SOURCE_COMMIT"
  test -z "$(ssh $SSH_ARGS "$REMOTE_HOST" \
    "git -C '$REMOTE_REPO' status --porcelain -- \
      .dockerignore aggregator frontend e2e nginx scripts/research schemas \
      docs/setup-lab-node.md \
      docs/research/task-aware-reliability-contract-design.md")"
  ssh $SSH_ARGS "$REMOTE_HOST" \
    "cd '$REMOTE_REPO' && \
     test \"\$(uv --version)\" = 'uv 0.8.13' && \
     scripts/research/run-python -c \
       'import sys; assert sys.version_info[:2] == (3, 12)'"
  scp $SSH_ARGS "$CONTROLLER_CONFIG" \
    "$REMOTE_HOST:$REMOTE_TMP/controller.json"
  scp $SSH_ARGS "$LOCAL_CREDENTIAL" \
    "$REMOTE_HOST:$REMOTE_TMP/nats.creds"
  scp $SSH_ARGS /tmp/ec-remote-01-fixture.tar \
    "$REMOTE_HOST:$REMOTE_TMP/fixture.tar"
  ssh $SSH_ARGS "$REMOTE_HOST" "chmod 0600 '$REMOTE_TMP/nats.creds'"
  REMOTE_FILES_READY=1
  ssh $SSH_ARGS "$REMOTE_HOST" \
    "docker load --input '$REMOTE_TMP/fixture.tar' && \
     cd '$REMOTE_REPO' && \
     scripts/research/run-python scripts/research/lab_node.py start \
       --controller-config '$REMOTE_TMP/controller.json' \
       --credential-file '$REMOTE_TMP/nats.creds' \
       --state-root '$REMOTE_STATE_ROOT' \
       --host-id gateway-lab-02 --agent-id shell-remote \
       --behavior echo --delay-ms 1000"
  ssh $SSH_ARGS "$REMOTE_HOST" \
    "cd '$REMOTE_REPO' && \
     scripts/research/run-python scripts/research/lab_node.py doctor \
       --controller-config '$REMOTE_TMP/controller.json' \
       --credential-file '$REMOTE_TMP/nats.creds' \
       --state-root '$REMOTE_STATE_ROOT' \
       --host-id gateway-lab-02 --agent-id shell-remote --publish"

  scripts/research/run-python scripts/research/lab_controller.py command \
    --run-id ec-remote-01 --agent-id shell-controller \
    --body controller-01 --expected-output edgecitadel:controller-01 --wait \
    --result-file tmp/research/ec-remote-01/controller-command.json
  scripts/research/run-python scripts/research/lab_controller.py command \
    --run-id ec-remote-01 --agent-id shell-remote \
    --body remote-01 --expected-output edgecitadel:remote-01 --wait \
    --result-file tmp/research/ec-remote-01/remote-command.json
  ssh $SSH_ARGS "$REMOTE_HOST" \
    "cd '$REMOTE_REPO' && \
     scripts/research/run-python scripts/research/lab_node.py stop \
       --controller-config '$REMOTE_TMP/controller.json' \
       --credential-file '$REMOTE_TMP/nats.creds' \
       --state-root '$REMOTE_STATE_ROOT' \
       --agent-id shell-remote --retain-reservation"
  scripts/research/run-python scripts/research/lab_controller.py command \
    --run-id ec-remote-01 --agent-id shell-remote \
    --body queued-remote-01 \
    --expected-output edgecitadel:queued-remote-01 --no-wait \
    --result-file tmp/research/ec-remote-01/queued-command.json
  ssh $SSH_ARGS "$REMOTE_HOST" \
    "cd '$REMOTE_REPO' && \
     scripts/research/run-python scripts/research/lab_node.py start \
       --controller-config '$REMOTE_TMP/controller.json' \
       --credential-file '$REMOTE_TMP/nats.creds' \
       --state-root '$REMOTE_STATE_ROOT' \
       --host-id gateway-lab-02 --agent-id shell-remote \
       --behavior echo --delay-ms 1000"
  ssh $SSH_ARGS "$REMOTE_HOST" \
    "cd '$REMOTE_REPO' && \
     scripts/research/run-python scripts/research/lab_node.py doctor \
       --controller-config '$REMOTE_TMP/controller.json' \
       --credential-file '$REMOTE_TMP/nats.creds' \
       --state-root '$REMOTE_STATE_ROOT' \
       --host-id gateway-lab-02 --agent-id shell-remote --publish"
  QUEUED_TASK_ID="$(scripts/research/run-python -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["task_id"])' \
    tmp/research/ec-remote-01/queued-command.json)"
  scripts/research/run-python scripts/research/lab_controller.py await \
    --run-id ec-remote-01 --task-id "$QUEUED_TASK_ID" \
    --expected-output edgecitadel:queued-remote-01 \
    --qualification-kind queued-reconnect \
    --result-file tmp/research/ec-remote-01/queued-terminal.json

  ssh $SSH_ARGS "$REMOTE_HOST" \
    "cd '$REMOTE_REPO' && \
     scripts/research/run-python scripts/research/lab_node.py stop \
       --controller-config '$REMOTE_TMP/controller.json' \
       --credential-file '$REMOTE_TMP/nats.creds' \
       --state-root '$REMOTE_STATE_ROOT' \
       --agent-id shell-remote"
  scripts/research/run-python scripts/research/lab_node.py stop \
    --controller-config "$CONTROLLER_CONFIG" \
    --credential-file "$LOCAL_CREDENTIAL" \
    --agent-id shell-controller
  scripts/research/run-python scripts/research/lab_controller.py stop \
    --run-id ec-remote-01
  scripts/research/run-python scripts/research/check_artifact.py \
    --bundle /home/lab/edge-research/docs/research/results/lab/ec-remote-01 \
    --require-kind lab \
    --source-root /home/lab/edge-research
  scripts/research/run-python scripts/research/lab_controller.py qualify \
    --run-id ec-remote-01
  trap - EXIT HUP INT TERM
  cleanup_lab
  ```

  Before node start, the launcher verifies the loaded image ID equals the
  immutable ID in `controller.json`; there is no fallback pull or host-Python
  fixture execution. Normal remote stop removes the exact loaded image and
  private node paths. The trap repeats that stop idempotently before deleting
  transferred credentials. If the strict SSH stop cannot be verified, cleanup
  returns nonzero and retains the remote recovery directory and credential
  instead of risking a live credential-less container. Controller stop removes
  the journaled export tar.

  Place this warning directly after the commands:

  ```text
  This artifact uses one run-scoped shared NATS token and an unauthenticated HTTP
  command/dashboard API on one explicitly bound trusted LAN or Tailnet address.
  It does not provide per-agent identity, TLS, HTTP authentication, revocation,
  rotation, firewall configuration, or Internet-safe access. The inventory
  prevents duplicate IDs only for maintained launchers; any process holding the
  shared credential can bypass it and claim a NATS subject or durable consumer.
  The lab-only aggregator trusts nginx's overwritten X-Forwarded-For value
  because nginx is its sole published HTTP ingress; this is an evidence
  observation boundary, not an Internet-grade identity mechanism.
  OpenClaw onboarding, MQTT firmware, macOS, ARM64 performance, and production
  fleet deployment are outside this runbook.
  ```

  State that any result short of a valid bundle plus the exact
  `REMOTE QUALIFIED` line is `remote-capable` or `preliminary`.

- [ ] **Step 6: Run GREEN**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_qualification.py -q
  ```

  Expected: exactly 8 tests pass.

- [ ] **Step 7: Commit Task 7**

  Invoke `commit-check`, require `git diff --cached --check` to exit zero, then:

  ```bash
  git add -- scripts/research/lab_qualification.py \
    tests/research/test_lab_qualification.py docs/setup-lab-node.md \
    scripts/research/lab_controller.py
  git diff --cached -- scripts/research/lab_controller.py
  git commit -m "docs(infra): enforce remote lab qualification"
  verify_canonical_snapshot
  ```

---

### Task 8: Run Final R-09 Gates And Advance Only Proven Status

**Files:**
- Modify: `docs/research/task-aware-reliability-contract-design.md`

**Interfaces:**
- Consumes: every Task 1-7 public interface, finalized bundles, the Slice 1
  checker CLI, and Slice 2 default/evidence Playwright configs.
- Produces: one verified R-09 status row. It does not change `Remote Lab
  Qualified` or `Paper Evidence Ready`.

- [ ] **Step 1: Run all focused unit and shared-contract tests**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider \
    tests/research/test_lab_config.py \
    tests/research/test_lab_controller.py \
    tests/research/test_lab_evidence.py \
    tests/research/test_lab_commands.py \
    tests/research/test_lab_node.py \
    tests/research/test_lab_qualification.py \
    aggregator/tests/test_lab_inventory.py \
    tests/research/test_artifact_env.py \
    tests/research/test_preflight.py \
    tests/research/test_evidence.py \
    tests/research/test_native_control.py -q
  ```

  Expected: the Slice 3 files contribute exactly 69 passing tests
  (`8 + 16 + 10 + 7 + 11 + 8 + 9`); shared Slice 1 files have zero failures and
  zero new skips.

- [ ] **Step 2: Run backend verification**

  Run the complete aggregator suite against a run-owned, unauthenticated
  digest-pinned NATS used only for tests; do not inherit a developer broker:

  ```bash
  cd "$TASK_ROOT"
  BACKEND_NATS="edgecitadel-backend-test-$$"
  cleanup_backend_nats() { docker rm -f "$BACKEND_NATS" >/dev/null 2>&1 || true; }
  trap cleanup_backend_nats EXIT
  NATS_IMAGE="$(scripts/research/run-python -c \
    'import json; print(json.load(open("scripts/research/toolchain.json"))["nats_image"])')"
  test "${NATS_IMAGE#*@sha256:}" != "$NATS_IMAGE"
  docker run --detach --name "$BACKEND_NATS" \
    --label ai.edgecitadel.owner=test-nats \
    --label ai.edgecitadel.run-id=backend-suite \
    --publish 127.0.0.1::4222 \
    "$NATS_IMAGE" -js >/dev/null
  NATS_PORT="$(docker port "$BACKEND_NATS" 4222/tcp | sed 's/.*://')"
  NATS_PORT="$NATS_PORT" scripts/research/run-python - <<'PY'
  import os
  import socket
  import time

  deadline = time.monotonic() + 15
  while True:
      try:
          with socket.create_connection(
              ("127.0.0.1", int(os.environ["NATS_PORT"])), timeout=1
          ):
              break
      except OSError:
          if time.monotonic() >= deadline:
              raise
          time.sleep(0.1)
  PY
  NATS_URL="nats://127.0.0.1:$NATS_PORT" \
    scripts/research/run-python - <<'PY'
  import os
  import subprocess
  import sys

  environment = dict(os.environ)
  environment.pop("NATS_TOKEN", None)
  environment["NATS_URL_TEST"] = environment["NATS_URL"]
  environment["NATS_TOKEN_TEST"] = ""
  subprocess.run(
      [
          sys.executable, "-m", "pytest", "-p", "no:cacheprovider",
          "aggregator/tests", "-q",
      ],
      check=True,
      env=environment,
      timeout=240,
  )
  PY
  cleanup_backend_nats
  trap - EXIT
  ```

  Expected: zero failures, no timeout, no new skips, and the image assertion
  rejects a tag. Then invoke `verify-backend`.

- [ ] **Step 3: Run all five lab integration tests**

  ```bash
  cd "$TASK_ROOT"
  scripts/research/run-python -m pytest \
    -p no:cacheprovider tests/research/test_lab_lifecycle.py \
    -m lab_integration -q
  ```

  Expected: exactly 5 tests pass. They execute one same-host lifecycle, one
  default operator journey, two complete concurrent runs, two complete
  sequential runs, and one idempotent cleanup run.

- [ ] **Step 4: Re-run the clean-checkout gate**

  Retain a source bundle, checked evidence, and receipt from committed current
  `HEAD`:

  ```bash
  cd "$TASK_ROOT"
  RETAIN_ROOT="$PWD/docs/research/results/lab/r09-clean-checkout"
  test ! -e "$RETAIN_ROOT"
  mkdir -p "$RETAIN_ROOT"
  SOURCE_COMMIT="$(git rev-parse HEAD)"
  git bundle create "$RETAIN_ROOT/source.bundle" HEAD
  CLEAN_ROOT="$(mktemp -d /tmp/edgecitadel-r09.XXXXXX)"
  trap 'rm -rf "$CLEAN_ROOT"' EXIT
  git clone "$RETAIN_ROOT/source.bundle" "$CLEAN_ROOT/edge-research"
  git -C "$CLEAN_ROOT/edge-research" checkout --detach "$SOURCE_COMMIT"
  test -z "$(git -C "$CLEAN_ROOT/edge-research" status --porcelain)"
  "$CLEAN_ROOT/edge-research/scripts/research/run-python" -c \
    'import sys; assert sys.version_info[:2] == (3, 12)'
  npm --prefix "$CLEAN_ROOT/edge-research/e2e" ci
  test "$(node --version)" = "v24.6.0"
  test "$(npm --version)" = "11.5.1"
  npm --prefix "$CLEAN_ROOT/edge-research/e2e" exec -- \
    playwright install chromium
  "$CLEAN_ROOT/edge-research/scripts/research/run-python" \
    "$CLEAN_ROOT/edge-research/scripts/research/lab_gate.py" \
    --clean-checkout-gate --repo-root "$CLEAN_ROOT/edge-research" \
    --run-id lab-clean-verified --host-id controller-lab-01 \
    --retain-bundles "$RETAIN_ROOT/bundles" \
    --receipt "$RETAIN_ROOT/receipt.json"
  "$CLEAN_ROOT/edge-research/scripts/research/run-python" -m json.tool \
    "$RETAIN_ROOT/receipt.json" >/dev/null
  "$CLEAN_ROOT/edge-research/scripts/research/run-python" - \
    "$RETAIN_ROOT/receipt.json" "$RETAIN_ROOT" "$CLEAN_ROOT" "$TASK_ROOT" <<'PY'
  from pathlib import Path, PurePosixPath
  import json
  import sys

  receipt_path, retain_root, clean_root, task_root = map(Path, sys.argv[1:])
  raw = receipt_path.read_text()
  assert str(clean_root) not in raw
  assert str(task_root) not in raw
  receipt = json.loads(raw)
  assert set(receipt) == {
      "schema_version",
      "source_commit",
      "source_snapshot_sha256",
      "bundles",
      "cleanup",
  }
  assert receipt["schema_version"] == "1"
  assert len(receipt["source_commit"]) == 40
  assert set(receipt["source_commit"]) <= set("0123456789abcdef")
  assert len(receipt["source_snapshot_sha256"]) == 64
  assert set(receipt["source_snapshot_sha256"]) <= set("0123456789abcdef")
  assert set(receipt["bundles"]) == {"lifecycle", "operator_smoke"}
  assert (retain_root / "source.bundle").is_file()
  for key in ("lifecycle", "operator_smoke"):
      entry = receipt["bundles"][key]
      value = entry["path"]
      assert entry["checker_valid"] is True
      assert entry["finalizer_count"] == 1
      assert "\\" not in value
      assert ".." not in PurePosixPath(value).parts
      relative = Path(value)
      assert not relative.is_absolute(), (key, value)
      resolved = (retain_root / relative).resolve()
      assert resolved.is_relative_to(retain_root.resolve()), (key, value)
      assert resolved.is_dir(), (key, value)
  assert receipt["cleanup"] == {
      "complete": True,
      "owned_resources_remaining": [],
  }
  PY
  test "$(git -C "$CLEAN_ROOT/edge-research" rev-parse HEAD)" = "$SOURCE_COMMIT"
  rm -rf "$CLEAN_ROOT"
  trap - EXIT
  ```

  Expected: scoped clean source provenance, exactly one complete two-node
  lifecycle, exactly one `chromium` operator test, two checked finalized bundles,
  one finalizer call per bundle, and zero owned resources. `source.bundle`
  remains beside the receipt so any later checker run can reconstruct the exact
  source root. The receipt contains only receipt-parent-relative retained paths
  and no `CLEAN_ROOT` or task-worktree path.

- [ ] **Step 5: Run the exact two-project evidence journey**

  Start a separate controller run with one logical node `shell-1`, then run
  Playwright from `e2e/`:

  ```bash
  cd "$TASK_ROOT"
  BUNDLE="$PWD/docs/research/results/lab/lab-evidence-01"
  CONTROLLER_CONFIG="$PWD/tmp/research/lab-evidence-01/controller.json"
  CREDENTIAL_FILE=""
  test ! -e "$BUNDLE"
  cleanup_lab_evidence() {
    cleanup_rc=0
    if [ -f "$CREDENTIAL_FILE" ] && [ -f "$CONTROLLER_CONFIG" ]; then
      scripts/research/run-python scripts/research/lab_node.py stop \
        --controller-config "$CONTROLLER_CONFIG" \
        --credential-file "$CREDENTIAL_FILE" \
        --agent-id shell-1 || cleanup_rc=1
    fi
    scripts/research/run-python scripts/research/lab_controller.py stop \
      --run-id lab-evidence-01 || cleanup_rc=1
    return "$cleanup_rc"
  }
  trap cleanup_lab_evidence EXIT
  scripts/research/run-python scripts/research/lab_controller.py start \
    --run-id lab-evidence-01 \
    --host-id controller-lab-01 \
    --lab-variant operator-evidence \
    --bind-host 127.0.0.1 \
    --advertise-host 127.0.0.1
  CREDENTIAL_FILE="$(scripts/research/run-python -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["credential_file"])' \
    "$CONTROLLER_CONFIG")"
  APP_URL="$(scripts/research/run-python -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["app_url"])' \
    "$CONTROLLER_CONFIG")"
  AGG_URL="$(scripts/research/run-python -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["agg_url"])' \
    "$CONTROLLER_CONFIG")"
  test -f "$CREDENTIAL_FILE"
  scripts/research/run-python scripts/research/lab_node.py start \
    --controller-config "$CONTROLLER_CONFIG" \
    --credential-file "$CREDENTIAL_FILE" \
    --host-id controller-lab-01 \
    --agent-id shell-1 \
    --behavior echo --delay-ms 1000
  scripts/research/run-python scripts/research/lab_node.py doctor \
    --controller-config "$CONTROLLER_CONFIG" \
    --credential-file "$CREDENTIAL_FILE" \
    --host-id controller-lab-01 --agent-id shell-1 --publish
  (
    cd e2e
    APP_URL="$APP_URL" \
    AGG_URL="$AGG_URL" \
    EVIDENCE_DIR="$BUNDLE" \
      npx --no-install playwright test \
        --config playwright.evidence.config.js \
        tests/operator-journey.spec.js
  )
  scripts/research/run-python -c \
    'from pathlib import Path; from scripts.research.lab_gate import relocate_slice2_media; relocate_slice2_media(repo_root=Path.cwd(), bundle=Path.cwd() / "docs/research/results/lab/lab-evidence-01")'
  cleanup_lab_evidence
  trap - EXIT
  test "$(find "$BUNDLE/raw/playwright" -name '*.png' -print | wc -l | tr -d ' ')" = 4
  test "$(find "$BUNDLE/raw/playwright" -name '*.webm' -print | wc -l | tr -d ' ')" = 2
  test "$(find "$BUNDLE/raw/playwright" -name 'trace.zip' -print | wc -l | tr -d ' ')" = 2
  while IFS= read -r video; do
    duration="$(ffprobe -v error -show_entries format=duration \
      -of default=noprint_wrappers=1 "$video")"
    scripts/research/run-python -c \
      'import sys; assert float(sys.argv[1].split("=", 1)[-1]) > 0' \
      "$duration"
  done < <(find "$BUNDLE/raw/playwright" -name '*.webm' -print | sort)
  while IFS= read -r trace; do
    unzip -t "$trace"
  done < <(find "$BUNDLE/raw/playwright" -name 'trace.zip' -print | sort)
  scripts/research/run-python - "$BUNDLE" <<'PY'
  import json
  import sys
  import zipfile
  from pathlib import Path, PurePosixPath

  root = Path(sys.argv[1])
  report = json.loads((root / "playwright-results.json").read_text())
  assert set(report["projects"]) == {"desktop", "mobile"}
  metadata = {}
  for project in ("desktop", "mobile"):
      project_root = root / "raw/playwright" / project
      current = json.loads(
          (project_root / "operator-metadata.json").read_text()
      )
      metadata[project] = current
      assert {
          item["name"] for item in report["projects"][project]["attachments"]
      } == {"chat", "tasks", "operator-metadata", "video", "trace"}
      chunks = []
      total = 0
      with zipfile.ZipFile(project_root / "trace.zip") as archive:
          for info in archive.infolist():
              member = PurePosixPath(info.filename)
              assert not member.is_absolute() and ".." not in member.parts
              if info.file_size <= 10_000_000:
                  total += info.file_size
                  assert total <= 50_000_000
                  chunks.append(archive.read(info))
      payload = b"\n".join(chunks)
      for value in (
          current["task_id"],
          current["nonce"],
          current["expected_output"],
      ):
          assert value.encode() in payload
  assert metadata["desktop"]["task_id"] != metadata["mobile"]["task_id"]
  print("PASS trace/project correlation")
  PY
  CONTACT_DIR="$(mktemp -d /tmp/edgecitadel-lab-contact.XXXXXX)"
  while IFS= read -r video; do
    project="$(basename "$(dirname "$video")")"
    ffmpeg -y -v error -i "$video" \
      -vf "fps=1,scale=720:-2,tile=3x2:padding=4:margin=4" \
      -frames:v 1 "$CONTACT_DIR/$project.png"
  done < <(find "$BUNDLE/raw/playwright" -name '*.webm' -print | sort)
  test "$(find "$CONTACT_DIR" -name '*.png' -print | wc -l | tr -d ' ')" = 2
  printf '%s\n' "$CONTACT_DIR"
  ```

  Expected: exactly 2 tests pass, one in `desktop` and one in `mobile`, using one
  shared controller stack. The evidence contains two distinct per-project task
  IDs, four screenshots, two videos, two traces, two metadata files, eight API
  snapshots, and a portable Playwright report at the exact Slice 2 paths. Use
  `view_image` at original detail on all four screenshots and both printed
  contact sheets. Each project must visibly match its metadata task ID, nonce,
  selected `shell-1`, and exact output; its trace must contain that same
  task/nonce/output. Remove `CONTACT_DIR` only after all six images pass visual
  inspection. Both videos have positive duration and both traces pass integrity.
  The `EXIT` trap finalizes cleanup even when Playwright or media relocation
  fails; no fixed loopback port or guessed credential path is used.

  After all six `view_image` calls pass, remove the temporary contact sheets:

  ```bash
  rm -rf "$CONTACT_DIR"
  test ! -e "$CONTACT_DIR"
  ```

- [ ] **Step 6: Validate every retained accepted bundle through the Slice 1 CLI**

  The integration tests already validate their ephemeral same-host,
  concurrent, and sequential bundles before cleanup. Reconstruct the exact
  committed source for every retained clean-checkout and evidence bundle:

  ```bash
  cd "$TASK_ROOT"
  RETAIN_ROOT="$PWD/docs/research/results/lab/r09-clean-checkout"
  VALIDATE_ROOT="$(mktemp -d /tmp/edgecitadel-validate.XXXXXX)"
  trap 'rm -rf "$VALIDATE_ROOT"' EXIT
  git clone "$RETAIN_ROOT/source.bundle" "$VALIDATE_ROOT/source"
  SOURCE_COMMIT="$(scripts/research/run-python -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["source_commit"])' \
    "$RETAIN_ROOT/receipt.json")"
  git -C "$VALIDATE_ROOT/source" checkout --detach "$SOURCE_COMMIT"
  test -z "$(git -C "$VALIDATE_ROOT/source" status --porcelain)"
  for BUNDLE in "$RETAIN_ROOT"/bundles/* \
    "$PWD/docs/research/results/lab/lab-evidence-01"; do
    test -f "$BUNDLE/manifest.json"
    "$VALIDATE_ROOT/source/scripts/research/run-python" \
      "$VALIDATE_ROOT/source/scripts/research/check_artifact.py" \
      --bundle "$BUNDLE" --require-kind lab \
      --source-root "$VALIDATE_ROOT/source"
  done
  rm -rf "$VALIDATE_ROOT"
  trap - EXIT
  ```

  Expected final line:

  ```text
  artifact: PASS
  ```

  Inspect each `manifest.json` and require complete Compose hash, image digests,
  versions, secret-free argv and working directories, UTC timestamps, logical
  and qualified identities, declared host/machine facts, network facts, command
  results, cleanup, and artifact hashes.

- [ ] **Step 7: Advance R-09 only**

  Change the requirement row to:

  ```markdown
  | R-09 | Isolated controller and multi-node lab launchers | Verified | `tests/research/test_lab_lifecycle.py`; `docs/research/results/lab/r09-clean-checkout/receipt.json` |
  ```

  Do not change `Remote Lab Qualified` without a valid two-host bundle and exact
  qualification output. Do not change `Paper Evidence Ready`.

- [ ] **Step 8: Run diff, placeholder, fence, duplicate, and interface scans**

  ```bash
  cd "$TASK_ROOT"
  git diff --check
  scripts/research/run-python - <<'PY'
  from pathlib import Path
  import re

  path = Path("docs/research/plans/2026-07-25-slice-3-multi-agent-iot-lab.md")
  text = path.read_text()
  forbidden = (
      "T" + "BD",
      "T" + "ODO",
      "implement " + "later",
      "fill in " + "details",
      "add appropriate " + "error handling",
      "write tests for " + "the above",
      "similar to Task " + "N",
  )
  hits = [term for term in forbidden if term.lower() in text.lower()]
  assert hits == [], hits
  fence = "`" * 3
  assert text.count(fence) % 2 == 0
  tasks = re.findall(r"^### Task (\d+):", text, re.MULTILINE)
  assert tasks == [str(number) for number in range(1, 9)], tasks
  assert len(tasks) == len(set(tasks))
  sections = re.split(r"^### Task \d+:", text, flags=re.MULTILINE)[1:]
  assert len(sections) == 8
  assert all("**Interfaces:**" in section for section in sections)
  for section in sections:
      steps = [
          int(value)
          for value in re.findall(
              r"^- \[ \] \*\*Step (\d+):", section, re.MULTILINE
          )
      ]
      assert steps == list(range(1, len(steps) + 1)), steps

  stale = (
      "LAB_" + "CREDENTIAL_FILE",
      "inventory_" + "observed_peer_ip",
      "EVIDENCE_DIR=/Users/yefanzhang/workplace/edge-research/"
      "docs/research/results/lab/lab-evidence-01/raw/playwright",
      "--bind-host " + "0.0.0.0",
      "generated " + "free ports",
      "exactly " + "37 passing tests",
  )
  assert not [value for value in stale if value in text]
  assert "def check_bundle(" in text
  assert (
      'expected_kind: Literal["benchmark", "operator", "lab"] | None = None'
      in text
  )
  assert "source_root: Path | None = None" in text
  assert ") -> CheckReport:" in text
  assert ") -> dict[str, object]:" in text
  assert "scripts/research/run-python" in text
  assert "one raw token" in text
  assert "separate `service.env`" in text
  assert "Controller stop is the sole finalizer" in text
  assert ("git add" + " -p") not in text
  assert ('"$LAB_' + 'PYTHON"') not in text

  allowed_scopes = {
      "aggregator", "frontend", "nats", "mqtt", "dashboard",
      "e2e", "client", "infra",
  }
  commit_prefix = "git commit" + " -m " + chr(34)
  commits = re.findall(
      re.escape(commit_prefix) + r'([^"]+)"',
      text,
  )
  assert len(commits) == 8, commits
  pattern = re.compile(
      r"^(feat|fix|docs|refactor|perf|test|chore|ci|build)"
      r"\(([a-z0-9-]+)\): .+"
  )
  for commit in commits:
      match = pattern.fullmatch(commit)
      assert match and match.group(2) in allowed_scopes, commit
  print("plan scans: PASS")
  PY
  ```

  Expected: `git diff --check` and Python scan exit zero and print
  `plan scans: PASS`.

- [ ] **Step 9: Run final infrastructure verification**

  Invoke `verify-infra`. It must restart the full stack and run the repository
  Playwright gate, not only curl health checks. Record its exact commands and
  results in the implementation handoff.

- [ ] **Step 10: Commit verified R-09 status**

  Stage the exact status file and two checked retained evidence roots, and
  require every row path to exist in the staged tree. Invoke `commit-check`,
  scan staged blobs for credential/private-key patterns, require
  `git diff --cached --check` to exit zero, then:

  ```bash
  git add -- docs/research/task-aware-reliability-contract-design.md \
    docs/research/results/lab/r09-clean-checkout \
    docs/research/results/lab/lab-evidence-01
  git diff --cached -- docs/research/task-aware-reliability-contract-design.md
  git diff --cached --name-status
  git commit -m "docs(infra): record verified R-09 lab gate"
  verify_canonical_snapshot
  ```

  After the commit, require the task worktree clean, re-source the exact chain
  record, verify that its root/branch/base and prior Slice 2 `FINAL_COMMIT`
  remain valid, and atomically advance only `FINAL_COMMIT`:

  ```bash
  set -euo pipefail
  test -n "${BASH_VERSION:-}"
  CANONICAL_ROOT=/Users/yefanzhang/workplace/edge-research
  EXPECTED_BASE="$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
  CHAIN_KEY="$(printf '%s' "$EXPECTED_BASE" | cut -c1-12)"
  CHAIN_ROOT="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY"
  HANDOFF="$CHAIN_ROOT/handoff.env"
  CANONICAL_SNAPSHOT="$CHAIN_ROOT/canonical"
  test -f "$HANDOFF"
  # shellcheck disable=SC1090
  source "$HANDOFF"
  test "$CANONICAL_ROOT" = /Users/yefanzhang/workplace/edge-research
  test "$CANONICAL_BASE" = "$EXPECTED_BASE"
  test "$(git rev-parse --show-toplevel)" = "$TASK_ROOT"
  test "$(git branch --show-current)" = "$BRANCH"
  git merge-base --is-ancestor "$FINAL_COMMIT" HEAD
  test "$(git rev-list --count "$FINAL_COMMIT..HEAD")" = 8
  test -z "$(git rev-list --merges "$FINAL_COMMIT..HEAD")"
  test -z "$(git status --porcelain --untracked-files=all)"

  verify_canonical_snapshot() (
    set -euo pipefail
    CHECK_ROOT="$(mktemp -d "$CHAIN_ROOT/slice3-canonical.XXXXXX")"
    trap 'rm -rf "$CHECK_ROOT"' EXIT
    for suffix in index-tree unstaged.patch staged.patch status.z \
      untracked.z untracked.sha256; do
      test -f "$CANONICAL_SNAPSHOT.$suffix"
    done
    git -C "$CANONICAL_ROOT" write-tree >"$CHECK_ROOT/index-tree"
    git -C "$CANONICAL_ROOT" diff --binary >"$CHECK_ROOT/unstaged.patch"
    git -C "$CANONICAL_ROOT" diff --cached --binary \
      >"$CHECK_ROOT/staged.patch"
    git -C "$CANONICAL_ROOT" status \
      --porcelain=v2 -z --untracked-files=all >"$CHECK_ROOT/status.z"
    git -C "$CANONICAL_ROOT" ls-files \
      --others --exclude-standard -z >"$CHECK_ROOT/untracked.z"
    while IFS= read -r -d '' relative; do
      test -f "$CANONICAL_ROOT/$relative"
      digest="$(shasum -a 256 "$CANONICAL_ROOT/$relative" | awk '{print $1}')"
      printf '%s  %q\n' "$digest" "$relative"
    done <"$CHECK_ROOT/untracked.z" >"$CHECK_ROOT/untracked.sha256"
    for suffix in index-tree unstaged.patch staged.patch status.z \
      untracked.z untracked.sha256; do
      cmp -s "$CANONICAL_SNAPSHOT.$suffix" "$CHECK_ROOT/$suffix"
    done
  )
  verify_canonical_snapshot
  FINAL_COMMIT="$(git rev-parse HEAD)"
  {
    printf 'CANONICAL_ROOT=%q\n' "$CANONICAL_ROOT"
    printf 'CANONICAL_BASE=%q\n' "$CANONICAL_BASE"
    printf 'TASK_ROOT=%q\n' "$TASK_ROOT"
    printf 'BRANCH=%q\n' "$BRANCH"
    printf 'FINAL_COMMIT=%q\n' "$FINAL_COMMIT"
  } >"$HANDOFF.tmp"
  mv "$HANDOFF.tmp" "$HANDOFF"
  # shellcheck disable=SC1090
  source "$HANDOFF"
  test "$(git -C "$TASK_ROOT" rev-parse HEAD)" = "$FINAL_COMMIT"
  ```

  Slice 4 sources this same file and begins at its exact `FINAL_COMMIT` in
  `TASK_ROOT`. Do not move the separate canonical checkout.

## Completion Evidence

Slice 3 is complete only when current evidence proves all of the following:

- A clean checkout starts without a prior `.env`, database, broker, backup,
  model, developer stack, or fixed port.
- The controller can be stopped by a new CLI process using persisted ownership
  state.
- Controller start/preflight and node reservation/container failures roll back
  completely.
- Two simultaneous nodes execute from the immutable Slice 1 fixture image.
- Both nodes accept a command and produce one exact, non-conflicting terminal.
- Two identical wire copies use one task ID and produce one handler execution
  and one non-conflicting logical terminal.
- One stopped node accepts a queued command and completes it after reconnect.
- An active duplicate reservation fails before Docker create.
- The unchanged Slice 2 `shell-1` operator journey passes exactly once in the
  default config and exactly twice in desktop/mobile evidence config.
- Exactly two sequential and two concurrent complete runs have disjoint
  projects, ports, full subject scopes, consumers, state, task IDs, terminal
  outputs, and cleanup.
- Repeated teardown preserves unrelated Docker resources.
- Accepted manifests contain complete provenance and pass `check_bundle`.
- One raw fixture credential and one separate private service env are removed
  before finalization. Credential content appears in none of argv, state, logs,
  Compose evidence, screenshots, or manifests; malformed credentials are never
  echoed.
- The remote runbook uses strict noninteractive SSH, deletes the transferred
  credential only after verified node/image cleanup, and fails nonzero while
  retaining recovery material if the remote stop cannot be verified.
- Normal local/remote node stop removes private state and the exact immutable
  fixture image when unused; interrupted controller phases recover from the
  atomic journal without a second finalizer call.
- Same-host evidence remains preliminary. Only a valid finalized two-host bundle
  satisfying every remote condition can print `REMOTE QUALIFIED`.
