# Hermetic Experiment Spine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a hermetic, deterministic benchmark that executes the complete four-mode W1-W8 reliability matrix, records provenance-complete raw evidence and resource costs, and reproducibly checks and analyzes valid campaigns.

**Architecture:** Keep task correctness transport-neutral in one `TaskExecutor` backed by a durable `OutcomeStore`; inject mode-specific terminal and progress publishers into the existing durable consumer. Run Central relay, Core-only, EdgeCitadel, and All-durable inside run-owned Docker Compose projects with a single deterministic worker and direct observers, then write immutable JSONL evidence that a separate checker and deterministic analyzer consume. The production aggregator and developer stack never run in benchmark repetitions.

**Tech Stack:** Python 3.12, SQLite, FastAPI, httpx, nats-py 2.14, NATS 2.10, Docker Compose v2, JSON Schema Draft 2020-12, pytest, pytest-asyncio, Linux cgroup v2, `tc netem`.

---

## Preconditions And Ownership

- Read `docs/research/task-aware-reliability-contract-design.md` before executing
  any task. Requirement IDs R-01 and R-03 through R-07 belong to this slice; R-02
  belongs to Slice 2.
- Complete Task 0 before any Python test. Every repository Python command after
  Task 0 runs through `scripts/research/run-python`, which creates an isolated
  managed Python 3.12 environment outside the checkout and synchronizes
  `scripts/research/requirements.lock.txt` with hash verification. Bare host
  `python3` is allowed only inside the digest-pinned benchmark container and for
  the fixture module command documented below.
- Use `deliberate-changes` before Tasks 1-6 because they change the common task
  contract, NATS subjects, or durable state. Use `verify-backend` after Task 3 and
  `verify-infra` after Task 14.
- Always create a dedicated clean worktree before implementation and substitute
  that worktree's absolute root for the concrete checkout path in every command
  below. This keeps the execution path identical whether the canonical checkout
  is clean or dirty. Never run a paper campaign from a dirty or untracked source
  tree. Create and record that worktree noninteractively before Task 0:

  ```bash
  set -euo pipefail
  CANONICAL_ROOT=/Users/yefanzhang/workplace/edge-research
  CANONICAL_BASE="$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
  for plan in \
    docs/research/plans/2026-07-25-slice-1-hermetic-experiment-spine.md \
    docs/research/plans/2026-07-25-slice-2-deterministic-operator-journey.md \
    docs/research/plans/2026-07-25-slice-3-multi-agent-iot-lab.md \
    docs/research/plans/2026-07-25-slice-4-documentation-artifact.md; do
    git -C "$CANONICAL_ROOT" cat-file -e "$CANONICAL_BASE:$plan"
    test "$(
      git -C "$CANONICAL_ROOT" show "$CANONICAL_BASE:$plan" |
        shasum -a 256 | awk '{print $1}'
    )" = "$(
      shasum -a 256 "$CANONICAL_ROOT/$plan" | awk '{print $1}'
    )"
  done
  CHAIN_KEY="$(printf '%s' "$CANONICAL_BASE" | cut -c1-12)"
  CHAIN_ROOT="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY"
  TASK_ROOT="$CHAIN_ROOT/repo"
  test ! -e "$TASK_ROOT"
  mkdir -p "$CHAIN_ROOT"
  CANONICAL_SNAPSHOT="$CHAIN_ROOT/canonical"
  git -C "$CANONICAL_ROOT" write-tree >"$CANONICAL_SNAPSHOT.index-tree"
  git -C "$CANONICAL_ROOT" diff --binary \
    >"$CANONICAL_SNAPSHOT.unstaged.patch"
  git -C "$CANONICAL_ROOT" diff --cached --binary \
    >"$CANONICAL_SNAPSHOT.staged.patch"
  git -C "$CANONICAL_ROOT" status \
    --porcelain=v2 -z --untracked-files=all \
    >"$CANONICAL_SNAPSHOT.status.z"
  git -C "$CANONICAL_ROOT" ls-files \
    --others --exclude-standard -z \
    >"$CANONICAL_SNAPSHOT.untracked.z"
  while IFS= read -r -d '' relative; do
    test -f "$CANONICAL_ROOT/$relative"
    digest="$(shasum -a 256 "$CANONICAL_ROOT/$relative" | awk '{print $1}')"
    printf '%s  %q\n' "$digest" "$relative"
  done <"$CANONICAL_SNAPSHOT.untracked.z" \
    >"$CANONICAL_SNAPSHOT.untracked.sha256"
  git -C "$CANONICAL_ROOT" worktree add --detach \
    "$TASK_ROOT" "$CANONICAL_BASE"
  git -C "$TASK_ROOT" switch -c "paper-autonomous-$CHAIN_KEY"
  test -z "$(git -C "$TASK_ROOT" status --porcelain)"
  BRANCH="$(git -C "$TASK_ROOT" branch --show-current)"
  HANDOFF="$CHAIN_ROOT/handoff.env"
  mkdir -p "$CHAIN_ROOT"
  {
    printf 'CANONICAL_ROOT=%q\n' "$CANONICAL_ROOT"
    printf 'CANONICAL_BASE=%q\n' "$CANONICAL_BASE"
    printf 'TASK_ROOT=%q\n' "$TASK_ROOT"
    printf 'BRANCH=%q\n' "$BRANCH"
    printf 'FINAL_COMMIT=%q\n' "$CANONICAL_BASE"
  } >"$HANDOFF.tmp"
  mv "$HANDOFF.tmp" "$HANDOFF"
  ```

  `handoff.env` contains shell-escaped values only, is mode-independent, and
  lives outside the repository. The adjacent canonical snapshot records the
  index tree, staged and unstaged binary patches, NUL-delimited status/untracked
  inventory, and every untracked file hash. Run every subsequent command with
  `cd "$TASK_ROOT"`; a later slice sources this exact record instead of creating
  another branch. At the end of every slice, atomically rewrite only
  `FINAL_COMMIT` to that slice's clean `HEAD`. Keep the linked worktree and local
  branch until Slice 4 finishes. Do not cherry-pick, merge, push, or ask the user
  to choose a destination.
- Before every commit below, run `commit-check`, stage only that task's exact
  file map with noninteractive `git add --`, and confirm
  `git diff --cached --check` exits zero. The canonical checkout already
  contains user-owned edits in `aggregator/validator.py`,
  `aggregator/tests/test_validator.py`, and other files; the clean task worktree
  must not inherit them. Verify the complete canonical snapshot byte-for-byte at
  the Slice 1 handoff. Never ask the user to resolve a staging choice and never
  run an interactive `git add -p`.
- Benchmark raw output is rooted at
  `docs/research/results/raw/{campaign}/`; operator evidence from Slice 2 may use
  `docs/research/results/operator/{run_id}/` with the same evidence API.
- The public fixture contract owned here is:

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

  async def run_fixture(
      config: NativeControlConfig,
      transport: TaskTransport,
      event_sink: EventSink,
  ) -> None: ...
  ```

- The fixture CLI is exactly:

  ```bash
  python3 -m scripts.research.fixtures.native_control \
    --config /run/config/native-control.json
  ```

  Compose service `fixture-agent` runs that command from
  `/app/scripts/research/fixtures/native_control.py`. Identity and behavior live
  in the JSON file; `NATS_URL`, optional `RELAY_URL`, and
  `EC_CREDENTIAL_FILE=/run/secrets/transport-token` are read at runtime. Tokens
  never appear in argv, JSON configuration, logs, or evidence.
- The public evidence API owned here is:

  ```python
  def write_json(path: Path, value: object) -> None: ...
  def finalize_bundle(
      bundle_dir: Path,
      manifest: Mapping[str, object],
      schema_path: Path,
  ) -> str: ...
  ```

  `write_json` writes canonical JSON. `finalize_bundle` hashes every non-manifest
  file, validates `schemas/research-manifest.v1.json`, scans names and contents
  for credential patterns, atomically writes `manifest.json`, and returns
  `PASS` or `INVALID`. The post-finalization verification API shared by Slices 2
  and 3 is:

  ```python
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

  With no keywords, `check_bundle(path)` infers the manifest kind and retains the
  Slice 1 behavior. `expected_kind` enforces the discriminator when supplied.
  `source_root` is optional at the base layer; Slice 2 reports
  `OPERATOR_SOURCE_ROOT_REQUIRED` when operator semantic validation needs it, and
  Slice 3 uses the same keyword for lab source verification. Checker-detected
  corruption never rewrites the finalized manifest.
- The public preflight API owned here is:

  ```python
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
  ```
- The public environment API owned here is:

  ```python
  @classmethod
  def ArtifactEnvironment.create(
      cls, run_id: str, mode: str, output_root: Path
  ) -> "ArtifactEnvironment": ...

  def start(self) -> None: ...
  def start_topology(
      self, compose_file: Path, env_overrides: Mapping[str, str]
  ) -> None: ...
  def stop(self) -> None: ...
  def cleanup(self) -> CleanupReport: ...
  def owned_resources(self) -> tuple[OwnedResource, ...]: ...
  ```

  `start()` wraps `start_topology()` with
  `scripts/research/docker-compose.artifact.yml`. Slice 3 may call
  `start_topology()` with its own Compose file without changing ownership logic.
  `ArtifactEnvironment.create()` writes `credential_file` as exactly
  `secrets.token_hex(32) + "\n"`: 64 lowercase hexadecimal ASCII characters,
  one newline, and mode `0600`.
- The executor-side interfaces owned by Task 3 are exact:

  ```python
  @dataclass(frozen=True)
  class PublicationReceipt:
      envelope_id: str
      accepted: bool
      transport: str
      stream: str | None
      stream_sequence: int | None
      duplicate: bool | None
      accepted_ns: int
      application_bytes: int
      wire_bytes: int | None

  class InboundDelivery(Protocol):
      worker_agent_id: str
      raw: bytes
      delivery_count: int
      stream_sequence: int | None

      async def in_progress(self) -> None: ...
      async def commit(self) -> None: ...
      async def retry(self) -> None: ...
      async def terminate(self) -> None: ...

  @dataclass(frozen=True)
  class PolicyDecision:
      accepted: bool
      reason: str | None

  class ExecutionPolicy(Protocol):
      def evaluate(
          self, envelope: Mapping[str, object], worker_agent_id: str
      ) -> PolicyDecision: ...

  class Clock(Protocol):
      def monotonic_ns(self) -> int: ...
      def now_iso(self) -> str: ...

  class UUIDFactory(Protocol):
      def uuid4(self) -> str: ...

  class CrashHook(Protocol):
      def hit(self, point: str) -> None: ...

  @dataclass
  class ExecutionContext:
      agent_id: str
      nc: object | None
      js: object | None
      delivery: InboundDelivery
      progress_publisher: ProgressPublisher

      async def in_progress(self) -> None: ...
      async def publish_progress(
          self,
          task_id: str,
          *,
          body: str = "",
          progress: int | None = None,
          extra: Mapping[str, object] | None = None,
      ) -> PublicationReceipt: ...

  Handler = Callable[
      [Mapping[str, object], ExecutionContext],
      Awaitable[tuple[Mapping[str, object], str]],
  ]

  @dataclass(frozen=True)
  class ExecutionResult:
      classification: Literal[
          "completed", "failed", "canceled", "rejected", "poison"
      ]
      terminal_envelope: Mapping[str, object] | None
      receipt: PublicationReceipt | None
      ledger_decision: str
  ```

  `InboundDelivery.raw` lets `TaskExecutor` own JSON/schema validation and poison
  termination. A unit-test crash hook raises `InjectedCrash(BaseException)` so
  ordinary handler-exception conversion cannot catch it; the runtime hook exits
  the fixture process at the named boundary. `ExecutionPolicy` checks recipient
  binding, sender allowlists, capability, hop, and cancellation rules. The stable
  terminal envelope always uses `sender_id=delivery.worker_agent_id`,
  `recipient_id=request["sender_id"]`, the request task/context identity, and one
  injected UUIDv4 reused for cached publication attempts. The existing
  `pull_consumer.Context(agent_id, nc, js, msg)` constructor and public attributes
  remain valid on the legacy path; the injected path adapts its `Msg` to
  `InboundDelivery` and supplies `ExecutionContext` without changing current
  shell, Gemma, Hermes, template, or watchdog handler signatures.

## File Map

**Pinned implementation toolchain**

- Create `scripts/research/requirements.in`: direct Python 3.12 dependencies.
- Create `scripts/research/requirements.lock.txt`: universal exact hash lock.
- Create `scripts/research/toolchain.json`: Python, uv, and NATS image pins.
- Create `scripts/research/run-python`: hash-verifying Python 3.12 launcher.
- Create `tests/research/nats_server.py`: digest-pinned authenticated Docker NATS
  fixture with dynamic loopback ports and owned cleanup.
- Create `tests/research/test_toolchain_contract.py`: launcher, lock, digest, and
  NATS lifecycle contract.

**Common task contract**

- Create `schemas/task-correlation.v1.json`: direct and delegated task correlation.
- Modify `aggregator/validator.py`: apply the correlation schema to task-bearing
  envelopes without changing the envelope wire shape.
- Modify `adapters/_common/validator.py`: re-export the common validation helpers.
- Modify `aggregator/tests/test_api.py`: preserve the current HTTP command shape.
- Modify `openclaw-client/tests/nats-session.test.js`: preserve OpenClaw command
  validation.
- Modify `adapters/watchdog/tests/test_synth.py`: preserve watchdog terminal
  validation.
- Modify `docs/05-messaging.md`: document correlation, fingerprint, terminal, and
  injected publisher rules.
- Create `adapters/_common/task_types.py`: the single publication receipt and
  shared execution dataclasses.
- Create `adapters/_common/outcome_store.py`: durable outcome-ledger protocol and
  SQLite implementation.
- Create `adapters/_common/task_publisher.py`: terminal/progress publisher
  protocols that import the shared publication receipt.
- Create `adapters/_common/task_executor.py`: validation, fingerprint, collision,
  handler, ledger, publish, and commit protocol.
- Modify `adapters/_common/pull_consumer.py`: delegate execution and publishing to
  injected common interfaces while preserving current constructor behavior.

**Modes and deterministic workloads**

- Create `scripts/research/modes/__init__.py`: benchmark mode package.
- Create `scripts/research/modes/base.py`: `Mode`, `TaskTransport`, shared receipt
  use, delivery handles, snapshots, and lifecycle protocol.
- Create `scripts/research/modes/central_relay.py`: HTTP client/worker transport.
- Create `scripts/research/modes/central_relay_server.py`: SQLite lease queue and
  transactional result/lease API.
- Create `scripts/research/modes/core_nats.py`: plain Core-NATS control.
- Create `scripts/research/modes/jetstream_config.py`: exact stream/consumer specs.
- Create `scripts/research/modes/edgecitadel.py`: durable task/result and Core
  progress/liveness.
- Create `scripts/research/modes/all_durable.py`: durable task/result/transients.
- Modify `scripts/research/fixtures/__init__.py`: export the fixture package.
- Create `scripts/research/fixtures/native_control.py`: deterministic fixture,
  heartbeat, crash hooks, fake actuator, and late-bound mode CLI.
- Create `scripts/research/workload_matrix.py`: W1-W8 definitions, variants,
  expected applicability, invariants, and trial execution.
- Create `scripts/research/preflight.py`: readiness, auth, topology, config,
  storage, observer, and network-profile validation.

**Hermetic runtime and evidence**

- Create `scripts/research/Dockerfile`: digest-pinned benchmark image.
- Create `scripts/research/docker-compose.artifact.yml`: profile-selected,
  unexposed, run-owned services.
- Create `scripts/research/configs/nats/core.conf`: authenticated Core-only broker.
- Create `scripts/research/configs/nats/jetstream.conf`: authenticated broker with
  run-owned file storage.
- Create `scripts/research/configs/schema/campaign.schema.json`: campaign contract.
- Create `scripts/research/configs/schema/hardware.schema.json`: hardware contract.
- Create `scripts/research/configs/schema/network.schema.json`: network contract.
- Create `scripts/research/configs/campaigns/preliminary-x86-lan.yaml`: fixed
  preliminary campaign.
- Create `scripts/research/artifact_env.py`: Compose isolation and cleanup.
- Create `scripts/research/metrics.py`: monotonic events, cgroup/network/storage
  samples, cost windows, and calibration.
- Create `scripts/research/evidence.py`: canonical JSON/JSONL, hashes, provenance,
  secret scan, and atomic status.
- Create `scripts/research/run_artifact.py`: CLI, schedule, repetition lifecycle,
  and cleanup recovery.
- Create `scripts/research/statistics.py`: Wilson, Newcombe, paired summaries,
  bootstrap intervals, and p99 guard.
- Create `scripts/research/analyze_artifact.py`: deterministic derived artifacts.
- Create `scripts/research/check_artifact.py`: run/campaign validation.
- Create `schemas/research-manifest.v1.json`: shared benchmark/operator/lab
  manifest with `evidence_kind`.
- Create `schemas/research-event.v1.json`: raw event schema.
- Create `schemas/research-trial.v1.json`: raw trial schema.
- Modify `docs/research/results/README.md`: prerequisites, commands, evidence
  layout, validity, and cleanup.
- Modify after clean gates
  `docs/research/task-aware-reliability-contract-design.md`: link only checked
  Slice 1 evidence and retain the paper-readiness guard.
- Modify `.gitignore`: ignore transient local run scratch without ignoring
  reviewable research results.
- Create `.dockerignore`: keep development state, secrets, and generated evidence
  out of the benchmark build context.
- Create the exact quick and matrix-smoke campaign directories reported beneath
  `docs/research/results/raw/` by the clean Task 14 captures.

**Focused tests**

- Create `schemas/tests/test_task_correlation_schema.py`.
- Modify `aggregator/tests/test_validator.py`.
- Modify `aggregator/tests/test_api.py`.
- Modify `openclaw-client/tests/nats-session.test.js`.
- Modify `adapters/watchdog/tests/test_synth.py`.
- Create `adapters/_common/tests/test_outcome_store.py`.
- Create `adapters/_common/tests/test_task_executor.py`.
- Create `adapters/_common/tests/test_pull_consumer_injection.py`.
- Create `tests/research/test_transport_contract.py`.
- Create `tests/research/test_central_relay.py`.
- Create `tests/research/test_nats_modes.py`.
- Create `tests/research/test_native_control.py`.
- Create `tests/research/test_workload_matrix.py`.
- Create `tests/research/test_preflight.py`.
- Create `tests/research/test_metrics.py`.
- Create `tests/research/test_evidence.py`.
- Create `tests/research/test_schedule.py`.
- Create `tests/research/test_statistics.py`.
- Create `tests/research/test_analysis.py`.
- Create `tests/research/test_checker.py`.
- Create `tests/research/test_artifact_env.py`.
- Create `tests/research/test_artifact_profiles.py`.

### Task 0: Pin The Python And NATS Test Toolchain

**Files:**
- Create: `scripts/research/requirements.in`
- Create: `scripts/research/requirements.lock.txt`
- Create: `scripts/research/toolchain.json`
- Create: `scripts/research/run-python`
- Create: `tests/research/nats_server.py`
- Create: `tests/research/test_toolchain_contract.py`

- [ ] **Step 1: Write the failing toolchain contract test**

  Require `toolchain.json` to contain exactly:

  ```json
  {
    "nats_image": "nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927",
    "python_version": "3.12",
    "uv_version": "0.8.13"
  }
  ```

  Parse `requirements.lock.txt` and require every non-comment requirement to use
  `==` and at least one `--hash=sha256:` entry. Execute `scripts/research/run-python`
  with `-c 'import sys; assert sys.version_info[:2] == (3, 12)'`. With an injected
  subprocess runner, assert `NatsServer.start()` uses the exact image digest,
  argv-form Docker commands, a loopback-only dynamic port, an owner label, and a
  mounted mode-0600 configuration file; `close()` must remove the exact container
  and temporary directory and be idempotent. Through `run-python`, import
  `dotenv`, `fastapi`, `httpx`, `jsonschema`, `nats`, `pydantic`,
  `pydantic_settings`, `pytest`, `pytest_asyncio`, `respx`, `sqlite_vec`,
  `uvicorn`, `websockets`, and `yaml`. This import smoke is the contract that the
  direct lock supports every Python suite named in Task 14, including the Hermes
  adapter.

- [ ] **Step 2: Run the one allowed bootstrap test and confirm failure**

  Task 0's red test is the sole pre-lock Python exception. Run it in an isolated
  uv-managed 3.12 environment:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  UV_NO_CONFIG=1 uv run --isolated --managed-python --python 3.12 \
    --with pytest -- \
    python -m pytest -p no:cacheprovider \
    tests/research/test_toolchain_contract.py -q
  ```

  Expected: assertions fail because the toolchain files and NATS helper do not
  exist.

- [ ] **Step 3: Create and hash-lock the direct dependencies**

  `scripts/research/requirements.in` contains exactly these direct requirements:

  ```text
  fastapi
  httpx
  jsonschema
  nats-py==2.14.0
  pydantic
  pydantic-settings
  pytest
  pytest-asyncio
  python-dotenv
  pyyaml
  respx
  sqlite-vec
  uvicorn[standard]
  websockets
  ```

  Verify `uv --version` is exactly `uv 0.8.13`, then generate one universal
  Python 3.12 lock:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  test "$(uv --version)" = "uv 0.8.13"
  UV_NO_CONFIG=1 uv pip compile --universal --generate-hashes \
    --python-version 3.12 scripts/research/requirements.in \
    --output-file scripts/research/requirements.lock.txt
  ```

  Do not hand-edit the generated lock.

- [ ] **Step 4: Implement the hash-verifying launcher**

  `scripts/research/run-python` is an executable Bash script with
  `set -euo pipefail`. It resolves the repository from its own path, verifies the
  exact uv version from `toolchain.json`, computes the lock SHA-256, and uses
  `${EC_RESEARCH_VENV:-${TMPDIR:-/tmp}/edgecitadel-research-py312-<lock-prefix>}`
  where `<lock-prefix>` is the first 16 hex characters computed at runtime. If
  the interpreter is absent, it runs:

  ```bash
  uv venv --managed-python --python 3.12 "$VENV"
  ```

  Before every command it runs:

  ```bash
  uv pip sync --python "$VENV/bin/python" --require-hashes \
    "$ROOT/scripts/research/requirements.lock.txt"
  ```

  It verifies `sys.version_info[:2] == (3, 12)`, exports
  `PYTHONDONTWRITEBYTECODE=1` and `PYTHONPATH="$ROOT"`, then `exec`s the managed
  interpreter with the supplied arguments. It never creates a virtual environment
  or cache inside the checkout.

- [ ] **Step 5: Implement the digest-pinned authenticated NATS fixture**

  `tests/research/nats_server.py` reads the digest from `toolchain.json` and
  exposes:

  ```python
  @dataclass
  class NatsServer:
      token: str
      jetstream: bool
      runner: CommandRunner = subprocess.run

      def start(self) -> "NatsServer": ...
      @property
      def url(self) -> str: ...
      def restart(self, *, preserve_storage: bool) -> None: ...
      def close(self) -> None: ...
  ```

  `start()` creates a mode-0700 temporary directory and mode-0600 NATS config,
  starts the digest-pinned image with `--publish 127.0.0.1::4222`, a unique
  `ai.edgecitadel.owner=test-nats` label, and no token in argv, resolves the
  assigned port with `docker port`, and waits up to ten seconds for an authenticated
  nats-py connect/flush. JetStream tests receive a fresh named Docker volume owned
  by the same label. `restart()` removes/recreates the exact container with the
  same config and, when requested, the same volume, then repeats readiness.
  `close()` removes only the recorded container and volume, removes the temporary
  directory, and succeeds twice.

  Add a `@pytest.mark.docker` integration test that starts two authenticated
  servers concurrently, proves distinct ports/storage, rejects a bad token,
  accepts the configured token, and leaves neither owned resource after both
  contexts exit.

- [ ] **Step 6: Verify and commit Task 0**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_toolchain_contract.py -q
  scripts/research/run-python -m pytest -p no:cacheprovider \
    -m docker tests/research/test_toolchain_contract.py -q
  scripts/research/run-python -m compileall -q tests/research/nats_server.py
  git diff --check
  ```

  Expected: the static and Docker-backed contracts pass, compilation is silent,
  and the Docker test leaves no `ai.edgecitadel.owner=test-nats` resource. Run
  `commit-check`, stage only the six Task 0 files, confirm the staged lock contains
  hashes and the staged scripts contain no credential, then commit:

  ```bash
  git commit -m "build(infra): pin research test toolchain"
  ```

### Task 1: Define And Validate Task Correlation

**Files:**
- Create: `schemas/task-correlation.v1.json`
- Create: `schemas/tests/test_task_correlation_schema.py`
- Modify: `aggregator/validator.py`
- Modify: `aggregator/tests/test_validator.py`
- Modify: `aggregator/tests/test_api.py`
- Modify: `adapters/_common/validator.py`
- Modify: `openclaw-client/tests/nats-session.test.js`
- Modify: `adapters/watchdog/tests/test_synth.py`
- Modify: `docs/05-messaging.md`

- [ ] **Step 1: Write the failing schema tests**

  Load the schema with `jsonschema.Draft202012Validator` and parameterize these
  cases:

  ```python
  VALID_DIRECT = {
      "type": "command",
      "sender_id": "sender-1",
      "recipient_id": "worker-1",
      "task_id": "899d8a29-8c6c-4fef-b491-1140d8371fef",
      "context_id": "6e088543-c9de-4459-a0fe-2191d20dfba1",
      "hop_count": 0,
      "payload": {"command": "printf spine:nonce"},
  }
  VALID_CHILD = {
      "type": "delegation",
      "sender_id": "sender-1",
      "recipient_id": "worker-1",
      "task_id": "70209f19-a984-47e3-8637-44428ebd8318",
      "context_id": "6e088543-c9de-4459-a0fe-2191d20dfba1",
      "hop_count": 1,
      "payload": {
          "command": "printf child:nonce",
          "parent_task_id": "899d8a29-8c6c-4fef-b491-1140d8371fef",
      },
  }
  INVALID = [
      {**VALID_DIRECT, "task_id": "not-a-uuid"},
      {**VALID_DIRECT, "hop_count": 1},
      {**VALID_CHILD, "hop_count": 0},
      {**VALID_CHILD, "payload": {"command": "missing parent"}},
  ]
  ```

  Add compatibility fixtures using the exact current shapes produced by:

  - `POST /api/command/{agent_id}`, with no caller `context_id`;
  - `buildCommandEnvelope()` in `openclaw-client/src/nats-session.js`;
  - `Context.publish_progress()` and `PullConsumer._publish_result()`;
  - `watchdog.synth.build_synth_envelope()`.

  Require all four to retain the same serialized top-level fields and pass base
  envelope validation. Require `normalize_task_correlation()` to project a direct
  command/result with missing `context_id` and `hop_count` to
  `context_id=task_id` and `hop_count=0` without mutating its input. Delegation
  and any result carrying `payload.parent_task_id` must provide explicit
  `context_id`, `hop_count >= 1`, and a UUIDv4 parent. Registration, status,
  heartbeat, log, broadcast, and progress retain their existing base-envelope
  validation; progress is correlated by `task_id` at the observer and is not a
  request fingerprint.

- [ ] **Step 2: Run the focused tests and confirm failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    schemas/tests/test_task_correlation_schema.py \
    aggregator/tests/test_validator.py -k correlation -q
  ```

  Expected: collection or assertions fail because the schema and correlation
  validator do not exist.

- [ ] **Step 3: Create the exact correlation schema**

  Use Draft 2020-12, `additionalProperties: false`, UUID string formats,
  canonical agent-ID patterns for `sender_id` and `recipient_id`,
  `type` restricted to `command`, `delegation`, `cancel`, or `result`,
  `hop_count >= 0`, and this conditional:

  ```json
  {
    "if": {"properties": {"hop_count": {"const": 0}}},
    "then": {
      "properties": {
        "payload": {
          "not": {"required": ["parent_task_id"]}
        }
      }
    },
    "else": {
      "properties": {
        "payload": {"required": ["parent_task_id"]}
      }
    }
  }
  ```

  Require `type`, `sender_id`, `recipient_id`, `task_id`, `context_id`,
  `hop_count`, and `payload`; allow workload properties inside `payload` while
  validating `parent_task_id` when present. Resolve the schema path relative to
  `aggregator/validator.py`, compile one module-level validator with
  `FormatChecker`, and return stable validation error strings.

  Export:

  ```python
  CORRELATED_TYPES = frozenset({"command", "delegation", "cancel", "result"})

  def normalize_task_correlation(
      envelope: Mapping[str, object],
  ) -> dict[str, object]:
      projected = {
          "type": envelope["type"],
          "sender_id": envelope["sender_id"],
          "recipient_id": envelope["recipient_id"],
          "task_id": envelope["task_id"],
          "context_id": envelope.get("context_id", envelope["task_id"]),
          "hop_count": envelope.get("hop_count", 0),
          "payload": dict(envelope["payload"]),
      }
      return projected
  ```

  Before applying defaults, reject a delegation or delegated result missing an
  explicit context/hop/parent field. Apply the correlation schema only to the
  exact projection, never to the full envelope, and never write normalized
  defaults back to the production wire document.

- [ ] **Step 4: Define canonical fingerprinting in the validator**

  Export:

  ```python
  def canonical_json(value: object) -> bytes:
      return json.dumps(
          value,
          sort_keys=True,
          separators=(",", ":"),
          ensure_ascii=False,
          allow_nan=False,
      ).encode("utf-8")

  def request_fingerprint(envelope: Mapping[str, object]) -> str:
      correlated = normalize_task_correlation(envelope)
      value = {
          "type": correlated["type"],
          "sender_id": correlated["sender_id"],
          "recipient_id": correlated["recipient_id"],
          "task_id": correlated["task_id"],
          "context_id": correlated["context_id"],
          "hop_count": correlated["hop_count"],
          "payload": correlated["payload"],
      }
      return hashlib.sha256(canonical_json(value)).hexdigest()
  ```

  Call `request_fingerprint` only for executable command/delegation requests.
  Cancellation resolves the existing task record through policy rather than
  masquerading as a changed request fingerprint. Re-export all three helpers
  from `adapters/_common/validator.py`.

- [ ] **Step 5: Document the contract**

  In `docs/05-messaging.md`, define wire `id`, logical `task_id`, `context_id`,
  fresh child UUIDv4, parent linkage, hop increment, fingerprint fields,
  terminal states, logical terminal identity, idempotent repeats, collision
  rejection, the direct-request compatibility defaults, and the rule that
  run/trial IDs are harness metadata. State that a worker accepts only requests
  whose `recipient_id` equals its configured agent ID, and every worker terminal
  reverses the canonical request direction: terminal sender is the worker and
  terminal recipient is the request sender.

- [ ] **Step 6: Verify and commit**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    schemas/tests/test_task_correlation_schema.py \
    aggregator/tests/test_validator.py aggregator/tests/test_api.py \
    adapters/watchdog/tests/test_synth.py -q
  scripts/research/run-python -m compileall -q aggregator adapters/_common
  cd openclaw-client && npm test && cd ..
  git diff --check
  ```

  Expected: correlation and every compatibility producer test pass, OpenClaw's
  command shape remains valid, compilation and diff checks are silent.
  Run `commit-check`, stage only Task 1 files, and commit:

  ```bash
  git commit -m "feat(nats): define task correlation contract"
  ```

### Task 2: Add The Durable Outcome Ledger

**Files:**
- Create: `adapters/_common/task_types.py`
- Create: `adapters/_common/outcome_store.py`
- Create: `adapters/_common/tests/test_outcome_store.py`

- [ ] **Step 1: Write failing ledger tests**

  Require a temporary SQLite store to:

  ```python
  key = OutcomeKey("worker-1", "899d8a29-8c6c-4fef-b491-1140d8371fef")
  prepared = PreparedOutcome(
      key=key,
      sender_id="sender-1",
      request_envelope_id="wire-1",
      request_fingerprint="a" * 64,
      terminal_envelope={"id": "terminal-1", "type": "result"},
      terminal_payload_hash="b" * 64,
      publish_state="prepared",
      completed_at="2026-07-25T12:00:00Z",
  )
  assert store.lookup(key) is None
  store.prepare(prepared)
  assert store.lookup(key) == prepared
  receipt = PublicationReceipt(
      envelope_id="terminal-1",
      accepted=True,
      transport="jetstream",
      stream="AGENT_INBOX",
      stream_sequence=7,
      duplicate=False,
      accepted_ns=42,
      application_bytes=128,
      wire_bytes=192,
  )
  store.mark_published(key, receipt)
  assert store.lookup(key).publish_state == "published"
  ```

  Close and reopen the database, require the same terminal ID and canonical JSON,
  reject a conflicting second `prepare`, and prove concurrent prepares yield one
  winner. Assert WAL mode, `synchronous=FULL`, an immediate transaction, and no
  eviction during a run.

- [ ] **Step 2: Run the tests and confirm import failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    adapters/_common/tests/test_outcome_store.py -q
  ```

  Expected: collection fails because `outcome_store.py` does not exist.

- [ ] **Step 3: Define the shared receipt, ledger types, and protocol**

  Put the one `PublicationReceipt` dataclass declared in the preconditions in
  `adapters/_common/task_types.py`. No `PublishReceipt` or `TransportReceipt`
  synonym may exist. Create frozen `OutcomeKey` and `PreparedOutcome` dataclasses
  in `outcome_store.py`, import `PublicationReceipt`, then define:

  ```python
  class OutcomeStore(Protocol):
      def lookup(self, key: OutcomeKey) -> PreparedOutcome | None: ...
      def prepare(self, outcome: PreparedOutcome) -> PreparedOutcome: ...
      def mark_published(
          self, key: OutcomeKey, receipt: PublicationReceipt
      ) -> PreparedOutcome: ...
      def close(self) -> None: ...
  ```

  Add `DisabledOutcomeStore`, which implements the protocol but always returns
  `None`, never caches, and emits the ledger decision `disabled`. Only the
  predeclared EdgeCitadel `none` and `broker-only` ablations may select it; every
  primary cell and `full-contract` must use `SQLiteOutcomeStore`.

  `SQLiteOutcomeStore(path: Path)` owns a connection opened with
  `isolation_level=None`; create one row per `(worker_agent_id, task_id)` and
  store canonical terminal JSON, publish receipt JSON, and completion time.

- [ ] **Step 4: Implement atomic persistence**

  `prepare()` must use `BEGIN IMMEDIATE`, return the existing row when all
  immutable fields match, raise `OutcomeConflict` otherwise, and commit before
  returning. `mark_published()` must preserve the prepared terminal and update
  only publish state/receipt. Reject `accepted=False` receipts in
  `mark_published`; the executor may neither mark nor commit inbound delivery
  until publication is accepted. The schema stores retention terms in a metadata
  table, but the run-owned implementation performs no deletes. Preserve the
  original request envelope ID in the row while recording later semantic-retry
  wire IDs as append-only evidence; a new wire ID never changes the cached
  terminal ID.

- [ ] **Step 5: Verify and commit**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    adapters/_common/tests/test_outcome_store.py -q
  scripts/research/run-python -m compileall -q adapters/_common
  git diff --check
  ```

  Expected: all ledger, disabled-ledger, receipt, and semantic-retry persistence
  tests pass; compilation and diff checks are silent. Run `commit-check`, stage
  the three Task 2 files, and commit:

  ```bash
  git commit -m "feat(nats): add durable task outcome ledger"
  ```

### Task 3: Centralize Execution And Inject Publishers

**Files:**
- Create: `adapters/_common/task_publisher.py`
- Create: `adapters/_common/task_executor.py`
- Create: `adapters/_common/tests/test_task_executor.py`
- Create: `adapters/_common/tests/test_pull_consumer_injection.py`
- Modify: `adapters/_common/pull_consumer.py`
- Modify: `docs/05-messaging.md`

- [ ] **Step 1: Write failing executor protocol tests**

  Use in-memory fake handler, store, publishers, delivery, event sink, and crash
  hook. Require:

  ```python
  first = await executor.execute(delivery)
  replay = await executor.execute(same_delivery)
  assert handler.calls == 1
  assert first.terminal_envelope["id"] == replay.terminal_envelope["id"]
  assert terminal_publisher.attempt_ids == ["terminal-1", "terminal-1"]
  assert delivery.commit_count == 1
  assert same_delivery.commit_count == 1
  ```

  Parameterize completed, handler exception to `failed`, well-formed policy
  rejection, identical semantic retry, changed sender, changed payload,
  `OutcomeConflict`, and malformed poison. Collision cases must emit
  `rejected/task_id_collision`, execute zero additional handlers, and never
  expose the cached terminal payload. Poison must call `delivery.terminate()`
  exactly once, never call commit/retry, and never forge a result. Require a
  request whose `recipient_id` differs from `delivery.worker_agent_id` to produce
  `rejected/recipient_mismatch`, and require every terminal to reverse the
  canonical request sender/worker direction.

  Make `PublicationReceipt.accepted=False` cause one retry, no publication mark,
  and no inbound commit. For W6b, execute one request and then a byte-different
  delivery with a new request wire ID but equal normalized fingerprint; require
  one handler call, the original stable terminal ID on both publication attempts,
  two append-only request-attempt events, and one commit per delivery.

- [ ] **Step 2: Write failing crash-boundary tests**

  Parameterize these exact hook values:

  ```python
  CRASH_POINTS = (
      "after-receive-before-handler",
      "after-side-effect-before-ledger-prepare",
      "after-ledger-prepare-before-result-publish",
      "after-result-publish-before-publish-mark",
      "after-publish-mark-before-inbound-commit",
      "during-handler-exception-conversion",
  )
  ```

  At each hook, assert ledger state, publish attempts, commit count, handler
  executions, and terminal ID. Re-enter with the same delivery and require cached
  terminal reuse after ledger preparation.

- [ ] **Step 3: Write failing PullConsumer injection tests**

  Preserve the actual keyword-only legacy construction:

  ```python
  legacy = PullConsumer(
      agent_id="shell-1",
      nc=nc,
      handler=handler,
      ack_wait_sec=300,
      max_deliver=3,
      max_ack_pending=1,
      sender_allowlist=None,
  )
  ```

  Add a mutually exclusive injected path:

  ```python
  injected = PullConsumer(
      agent_id="worker-1",
      nc=nc,
      executor=executor,
      event_sink=event_sink,
      consumer_binding=ConsumerBinding(
          stream_name="AGENT_INBOX",
          filter_subject="agents.worker-1.inbox",
          durable_name="ec_20260725_a_worker_1_inbox",
          ack_wait_seconds=30,
          max_deliver=3,
          max_ack_pending=1,
      ),
  )
  ```

  Reject construction with both `handler` and `executor`, neither one, an
  injected executor without `consumer_binding`, or a binding whose filter does
  not match `agent_id`. Legacy mode must still call current
  `ensure_stream`/`ensure_consumer` and use `<agent_id>_inbox`. Injected mode must
  never call either hard-coded helper; it binds the already-created exact stream
  and durable and verifies the live normalized consumer configuration before
  fetching.

  Assert explicit durable acknowledgement happens only after terminal publisher
  success and outcome-store publish marking. Assert all-durable progress uses the
  executor's injected progress publisher and never the legacy Core subject
  helper. Instantiate the exact current template and watchdog constructor shapes,
  and require legacy `Context(agent_id, nc, js, msg)` fields plus Gemma/Hermes
  `ctx.nc` and `ctx.publish_progress` behavior to remain unchanged.

- [ ] **Step 4: Run the focused tests and confirm failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    adapters/_common/tests/test_task_executor.py \
    adapters/_common/tests/test_pull_consumer_injection.py -q
  ```

  Expected: collection fails because the executor and publisher modules are
  absent.

- [ ] **Step 5: Define publisher and executor interfaces**

  Import the sole `PublicationReceipt` from `task_types.py`; do not define a
  second receipt. Use:

  ```python
  class TerminalPublisher(Protocol):
      async def publish_terminal(
          self, envelope: Mapping[str, object]
      ) -> PublicationReceipt: ...

  class ProgressPublisher(Protocol):
      async def publish_progress(
          self, envelope: Mapping[str, object]
      ) -> PublicationReceipt: ...

  class EventSink(Protocol):
      def emit(self, event: Mapping[str, object]) -> None: ...

  class TaskExecutor:
      def __init__(
          self,
          *,
          worker_agent_id: str,
          handler: Handler,
          outcome_store: OutcomeStore,
          terminal_publisher: TerminalPublisher,
          progress_publisher: ProgressPublisher,
          policy: ExecutionPolicy,
          event_sink: EventSink,
          clock: Clock,
          uuid_factory: UUIDFactory,
          crash_hook: CrashHook,
          nc: object | None = None,
          js: object | None = None,
      ) -> None: ...

      async def execute(self, delivery: InboundDelivery) -> ExecutionResult: ...
  ```

  Implement the exact interfaces in the preconditions. `TaskExecutor` parses
  `delivery.raw`, validates, normalizes correlation, enforces worker/recipient and
  policy, fingerprints executable requests, and looks up
  `(worker_agent_id, task_id)`. It republishes matching cached outcomes, rejects
  collisions without reading the cached payload into the rejection, executes
  once, canonicalizes one stable terminal envelope, commits the ledger, publishes,
  marks publication, then commits inbound delivery. An accepted cancellation is
  resolved by policy against the original task state before fingerprint collision
  handling and produces `canceled`; a cancellation after terminal observation is
  rejected without changing the cached outcome.

  The generated terminal contains the normalized context and hop values and, for
  delegation, `payload.parent_task_id`; compatibility normalization does not
  mutate the inbound wire object. `ExecutionContext.publish_progress()` builds a
  canonical progress envelope and delegates only to the injected
  `ProgressPublisher`. `InjectedCrash` derives directly from `BaseException`;
  only that exception bypasses failure conversion. Every other handler exception
  becomes one stable `failed` terminal and follows the same prepare/publish/mark/
  commit sequence.

- [ ] **Step 6: Adapt PullConsumer without duplicating semantics**

  Add the frozen `ConsumerBinding` shown in Step 3 and implement the two exact
  keyword-only paths. The legacy path retains current parsing, validation,
  `Context`, result mirror, sender allowlist, helper bootstrap, and exception
  behavior byte-for-byte except for internal refactoring with passing regression
  tests; it does not silently acquire an unconfigured outcome database. The
  injected path adapts each `Msg` to `InboundDelivery`, calls only `TaskExecutor`,
  and owns lease keepalive around that call. It uses
  `pull_subscribe(subject=binding.filter_subject,
  durable=binding.durable_name, stream=binding.stream_name)` after comparing the
  live consumer info to all binding fields. Central relay and Core-only never
  instantiate `PullConsumer`.

- [ ] **Step 7: Document ordering and verify**

  Add the eight executor stages, canonical identity direction, direct-correlation
  defaults, collision ownership, late semantic retry, publisher injection,
  disabled-ledger ablations, terminal-before-inbound-ack rule, and per-transport
  crash-boundary applicability to `docs/05-messaging.md`.

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    adapters/_common/tests -q
  scripts/research/run-python -m pytest -p no:cacheprovider \
    adapters/shell/tests adapters/gemma/tests \
    adapters/hermes/tests adapters/watchdog/tests -q
  scripts/research/run-python -m compileall -q adapters/_common
  git diff --check
  ```

  Expected: all common and current-adapter compatibility tests pass with no live
  constructor or context regression, and compilation is silent. Invoke
  `verify-backend`, run `commit-check`, stage only Task 3 files, and commit:

  ```bash
  git commit -m "refactor(nats): centralize task execution protocol"
  ```

### Task 4: Define The Transport-Neutral Benchmark Contract

**Files:**
- Create: `scripts/research/modes/__init__.py`
- Create: `scripts/research/modes/base.py`
- Create: `tests/research/test_transport_contract.py`

- [ ] **Step 1: Write the failing contract test**

  Create fake transport and fault-controller implementations and require each
  lifecycle call and shared receipt field:

  ```python
  faults = transport.faults
  await transport.start_terminal_observer()
  await transport.start_progress_observer()
  await transport.start_receiver("worker-1", executor)
  await transport.wait_receiver_ready("worker-1", timeout_s=5.0)
  accepted = await transport.submit_task(envelope)
  progress = await transport.publish_progress(progress_envelope)
  terminal = await transport.publish_terminal(terminal_envelope)
  observed = await transport.observe_terminal(task_id, timeout_s=5.0)
  snapshot = await transport.inspect_state()
  if observed.delivery is not None:
      await observed.delivery.ack()
  await faults.disconnect_progress_observer()
  await faults.reconnect_progress_observer()
  await faults.stop_worker("worker-1")
  await faults.start_worker("worker-1")
  await faults.restart_coordinator()
  await transport.close()

  assert accepted.envelope_id == envelope["id"]
  assert accepted.accepted_ns <= observed.observed_ns
  assert snapshot.mode is Mode.CORE_ONLY
  ```

  Assert all transport-specific values are nullable rather than fabricated, all
  timestamps are positive `perf_counter_ns` values, observer acknowledgement
  never calls worker-inbound commit, and `TaskExecutor` is the only component
  that calls `InboundDelivery.commit()`.

- [ ] **Step 2: Run the contract test and confirm import failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_transport_contract.py -q
  ```

  Expected: collection fails because `scripts.research.modes.base` is absent.

- [ ] **Step 3: Define exact shared types**

  Import the single `PublicationReceipt` from
  `adapters._common.task_types`; transport submission and publisher methods return
  it directly, so every `TaskTransport` structurally satisfies
  `TerminalPublisher` and `ProgressPublisher`. Create:

  ```python
  class Mode(str, Enum):
      CENTRAL_RELAY = "central-relay"
      CORE_ONLY = "core-only"
      EDGECITADEL = "edgecitadel"
      ALL_DURABLE = "all-durable"

  class ObserverDelivery(Protocol):
      async def ack(self) -> None: ...

  @dataclass(frozen=True)
  class ObservedEnvelope:
      envelope: Mapping[str, object]
      observed_ns: int
      observation_index: int
      stream_sequence: int | None
      delivery_count: int
      replayed: bool
      delivery: ObserverDelivery | None

  @dataclass(frozen=True)
  class TransportSnapshot:
      mode: Mode
      streams: Mapping[str, Mapping[str, object]]
      consumers: Mapping[str, Mapping[str, object]]
      pending: int | None
      ack_pending: int | None
      connection_bytes: Mapping[str, int]
      storage_bytes: int
      message_count: int

  class EventSink(Protocol):
      def emit(self, event: Mapping[str, object]) -> None: ...

  class FaultController(Protocol):
      async def disconnect_progress_observer(self) -> None: ...
      async def reconnect_progress_observer(self) -> None: ...
      async def stop_worker(self, agent_id: str) -> None: ...
      async def start_worker(self, agent_id: str) -> None: ...
      async def restart_coordinator(self) -> None: ...
  ```

  Define `TaskTransport` with exactly
  `start_terminal_observer`, `start_progress_observer`, `start_receiver`,
  `wait_receiver_ready`, `submit_task`, `publish_progress`, `publish_terminal`,
  `publish_heartbeat`, `observe_terminal`, `inspect_state`, and `close`. It exposes
  `faults: FaultController`, `mode: Mode`, and `outcome_ledger_enabled: bool`
  properties. The latter is false only for the two declared disabled-ledger
  EdgeCitadel ablations. Each method accepts or returns only the shared types.
  `start_receiver` is invoked inside the worker process; the sender runner
  constructs a separate transport instance from the same immutable resolved
  configuration, so no executor object crosses a process boundary.

  W3 uses the observer controls; W4 stops the worker, submits, then starts it; W7
  invokes `restart_coordinator` after acceptance. Core-only defines coordinator
  as the NATS broker, EdgeCitadel/all-durable as their NATS broker, and central
  relay as its relay process. Each implementation must retain its declared
  storage across the coordinator restart.

- [ ] **Step 4: Verify and commit**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_transport_contract.py -q
  scripts/research/run-python -m compileall -q scripts/research/modes
  git diff --check
  ```

  Expected: the contract test passes. Run `commit-check`, stage the three Task 4
  files, and commit:

  ```bash
  git commit -m "feat(nats): define benchmark transport contract"
  ```

### Task 5: Build The Transport-Independent Native Fixture Core

**Files:**
- Modify: `scripts/research/fixtures/__init__.py`
- Create: `scripts/research/fixtures/native_control.py`
- Create: `tests/research/test_native_control.py`

- [ ] **Step 1: Write failing configuration and card tests**

  Instantiate the public `NativeControlConfig` from the preconditions. Require
  `behavior` to be one of `echo`, `delegate`, `progress`, or `actuator`;
  `crash_point` to be absent or one of the six Task 3 values; nonnegative delay;
  and exactly 1000 ms heartbeat interval in benchmark profiles. Require
  `build_agent_card()` to return a stable native worker card with the run and
  credential values absent.

- [ ] **Step 2: Write failing behavior tests**

  With a fake transport and event sink, assert:

  - `echo` returns exactly `edgecitadel:<nonce>`.
  - `delegate` creates a fresh child UUIDv4, preserves context, increments hop
    once, repeats `parent_task_id` in child request and result, and does no second
    hop.
  - `progress` sends exactly 20 frames at 50 ms intervals with an application
    payload of exactly 256 bytes before one terminal.
  - `actuator` transactionally increments execution attempts, commits its external
    side effect in `side_effect_db`, and exposes a crash hook after that commit.
  - heartbeats emit once per second from idle-baseline start through active-window
    end.

- [ ] **Step 3: Write failing configuration-loader security tests**

  Call `parse_args(["--config", path])`, `load_native_config(path)`, and
  `read_transport_token(path)` with a temp JSON file and mode-0600 credential
  file. Require the loader to reject a missing or broader-permission credential
  and never include the token in `repr(config)`, exceptions, stdout/stderr, argv,
  card, or emitted event. Require `runtime_endpoints(config, environ)` to select
  only `RELAY_URL` for central relay and only `NATS_URL` for the three NATS modes;
  central relay must not require or synthesize a NATS URL.

- [ ] **Step 4: Run the tests and confirm import failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_native_control.py -q
  ```

  Expected: collection fails because `native_control.py` is absent.

- [ ] **Step 5: Implement the fixed fixture contract**

  Add the public dataclass/functions from the preconditions and:

  ```python
  BEHAVIORS = ("echo", "delegate", "progress", "actuator")
  CRASH_POINTS = (
      "after-receive-before-handler",
      "after-side-effect-before-ledger-prepare",
      "after-ledger-prepare-before-result-publish",
      "after-result-publish-before-publish-mark",
      "after-publish-mark-before-inbound-commit",
      "during-handler-exception-conversion",
  )
  ```

  Use injected sleep/UUID/clock functions in tests and real
  `asyncio.sleep`, `uuid.uuid4`, and `perf_counter_ns` by default. The actuator
  uses a SQLite `BEGIN IMMEDIATE` transaction and `synchronous=FULL`; it records
  attempted executions separately from committed side effects.

  `run_fixture(config, transport, event_sink)` constructs the shared handler,
  `SQLiteOutcomeStore` when `transport.outcome_ledger_enabled` is true or
  `DisabledOutcomeStore` otherwise, policy, clock/UUID/crash hooks, and
  `TaskExecutor`; starts the receiver through the supplied transport; waits for
  receiver readiness; emits one ready event; and owns heartbeat and
  graceful-close tasks. The transport object is injected, and this task imports
  no concrete mode module.

- [ ] **Step 6: Implement parsing and secure runtime-input loading**

  Parse only `--config`; read transport URLs and credential path from the
  environment; load the token only after config validation; and reject unknown
  JSON keys so secrets cannot be smuggled into the file. Expose pure
  `parse_args`, `load_native_config`, `runtime_endpoints`, and
  `read_transport_token` helpers for Task 7. Do not construct a concrete
  transport and do not add the module `__main__` entry point yet; Task 7 performs
  that wiring only after all four modes exist.

- [ ] **Step 7: Verify and commit**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_native_control.py -q
  scripts/research/run-python -m compileall -q scripts/research/fixtures
  git diff --check
  ```

  Expected: all transport-independent fixture, behavior, crash, and secure-loader
  tests pass with no import of `scripts.research.modes.<concrete-mode>`. Run
  `commit-check`, stage the three Task 5 files, and commit:

  ```bash
  git commit -m "feat(client): add deterministic fixture core"
  ```

### Task 6: Implement Central Relay And Core-Only Controls

**Files:**
- Create: `scripts/research/modes/central_relay.py`
- Create: `scripts/research/modes/central_relay_server.py`
- Create: `scripts/research/modes/core_nats.py`
- Create: `tests/research/test_central_relay.py`
- Create: `tests/research/test_nats_modes.py`

- [ ] **Step 1: Write failing central-relay tests**

  Start the FastAPI app against a temporary SQLite file and require:

  ```python
  receipt = await client.submit_task(envelope)
  assert receipt.accepted
  assert db.task(envelope["task_id"]).state == "queued"
  leased_delivery = await worker.long_poll("worker-1", timeout_s=0.1)
  prepared = await worker.publish_terminal(terminal)
  assert prepared.accepted
  assert await client.observe_terminal(envelope["task_id"], 0.01) is None
  await leased_delivery.commit()
  assert await client.observe_terminal(envelope["task_id"], 1.0)
  ```

  Assert the submit response follows a committed row; leases expire and redeliver;
  `POST /terminal` durably and idempotently prepares a terminal without completing
  the lease; `POST /commit` atomically makes that prepared terminal observable and
  completes the lease; an offline worker receives the row later; restart reopens
  the same DB; HTTP request/response bytes are measured; auth failure is 401; and
  no `PullConsumer` is constructed. Require the authenticated live event socket
  to deliver progress, heartbeat, and status only while connected: W3 disconnect
  drops the middle ten progress frames, reconnect receives the last five, and no
  replay occurs. Crash after terminal prepare or after the local ledger
  publication mark but before `/commit` must let the lease expire, redeliver,
  reuse the cached terminal ID, and complete exactly once.

- [ ] **Step 2: Write failing Core-only tests**

  Use `tests.research.nats_server.NatsServer(token=..., jetstream=False)` rather
  than a host broker. Subscribe first and assert task, progress, heartbeat, and
  result round trips. Disconnect the worker, publish and flush, reconnect, and
  require no replay. Assert acceptance is recorded only after `nc.flush()`, there
  is no stream/consumer state, NATS connection byte counters are retained, and
  `PullConsumer` is never constructed. Mark the broker-backed cases
  `@pytest.mark.docker`; they may not skip when Docker is available.

- [ ] **Step 3: Run the focused tests and confirm import failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_central_relay.py \
    tests/research/test_nats_modes.py -k central -q
  scripts/research/run-python -m pytest -p no:cacheprovider -m docker \
    tests/research/test_nats_modes.py -k core -q
  ```

  Expected: collection fails because the control transports are absent.

- [ ] **Step 4: Implement the relay lease store and API**

  Use SQLite WAL/FULL with `tasks`, `leases`, and `terminals` tables. Expose only:

  ```text
  POST /v1/tasks
  GET  /v1/workers/{agent_id}/lease?timeout_ms=<n>
  POST /v1/leases/{lease_id}/terminal
  POST /v1/leases/{lease_id}/commit
  GET  /v1/tasks/{task_id}/terminal
  POST /v1/events
  WS   /v1/events
  GET  /healthz
  ```

  Require `Authorization: Bearer <run-token>`, canonical request/response bytes,
  one active lease per task, monotonic lease deadlines, idempotent terminal
  prepare/commit/retrieval, and a uniqueness constraint binding one prepared
  terminal ID/hash to a lease. `/terminal` commits a durable `prepared_terminals`
  row and returns a `PublicationReceipt`; `/commit` transactionally inserts or
  reveals the terminal and completes the matching lease. `GET /terminal` exposes
  only committed terminals. The relay's `InboundDelivery.commit()` calls
  `/commit`, so all six W5 boundaries are executable without weakening the
  terminal/lease atomic visibility rule.

  `/v1/events` accepts only canonical progress, heartbeat, and status envelopes,
  fans them out to currently authenticated `/v1/events` WebSocket observers, and
  returns a shared receipt after the request is accepted. It has no table,
  backlog, or replay cursor; with no connected observer the accepted event is
  intentionally lost. Central relay's progress observer fault controls close and
  recreate only this socket. The client implements every `TaskTransport` method.

- [ ] **Step 5: Implement the Core-only transport**

  Use subjects scoped by `run_id`:

  ```text
  artifact.<run_id>.agents.<agent_id>.inbox
  artifact.<run_id>.agents.<agent_id>.result.<task_id>
  artifact.<run_id>.agents.<agent_id>.task_progress.<task_id>
  artifact.<run_id>.agents.<agent_id>.heartbeat
  artifact.<run_id>.agents.<agent_id>.status
  ```

  Publish canonical bytes and await `flush()` before creating the shared
  `PublicationReceipt`; use local receive order for `observation_index`; never
  call JetStream APIs. Implement the Task 4 fault controls by draining/recreating
  the worker subscription and restarting only the owned digest-pinned NATS test
  container while preserving the mode's declared Core-NATS loss semantics.

- [ ] **Step 6: Verify and commit**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_central_relay.py \
    tests/research/test_nats_modes.py -k central -q
  scripts/research/run-python -m pytest -p no:cacheprovider -m docker \
    tests/research/test_nats_modes.py -k core -q
  scripts/research/run-python -m compileall -q scripts/research/modes
  git diff --check
  ```

  Expected: selected transport tests pass. Run `commit-check`, stage only Task 6
  files, and commit:

  ```bash
  git commit -m "feat(nats): add relay and core benchmark controls"
  ```

### Task 7: Implement EdgeCitadel And All-Durable Modes

**Files:**
- Create: `scripts/research/modes/jetstream_config.py`
- Create: `scripts/research/modes/edgecitadel.py`
- Create: `scripts/research/modes/all_durable.py`
- Modify: `scripts/research/fixtures/native_control.py`
- Modify: `tests/research/test_nats_modes.py`
- Modify: `tests/research/test_native_control.py`

- [ ] **Step 1: Write failing fixed-configuration tests**

  Assert `task_stream_config(run_id)` resolves to:

  ```python
  {
      "name": "AGENT_INBOX",
      "subjects": ["agents.*.inbox"],
      "retention": "workqueue",
      "storage": "file",
      "max_age_ns": 86_400_000_000_000,
      "max_bytes": 1_073_741_824,
      "max_msg_size": 1_048_576,
      "discard": "new",
      "duplicate_window_ns": 300_000_000_000,
  }
  ```

  Require one run-unique per-agent consumer with exact inbox filter, explicit ack,
  30-second ack wait, maximum three deliveries, and one ack-pending task. Define
  consumer names exactly as:

  ```python
  def durable_name(kind: str, run_id: str, agent_id: str) -> str:
      digest = hashlib.sha256(
          f"{kind}\0{run_id}\0{agent_id}".encode("utf-8")
      ).hexdigest()[:24]
      return f"ec_{kind}_{digest}"
  ```

  Permit only `kind in {"task", "result", "transient"}`; assert the name is at
  most 64 characters and contains no NATS-forbidden character. Two run IDs for
  the same agent must produce distinct names while retaining the same exact inbox
  filter. Build a `ConsumerBinding` from the normalized live consumer config and
  assert no legacy `<agent_id>_inbox` consumer is created.

- [ ] **Step 2: Write failing split-plane tests**

  For EdgeCitadel, require task/result PubAcks and production `PullConsumer`, but
  publish progress, heartbeat, and status through Core NATS with no PubAck. Assert
  none of those subjects is captured by any stream and W3 reconnect delivers five
  live, zero replayed final-window frames, and records the ten disconnected frames
  as missing.

- [ ] **Step 3: Write failing all-durable tests**

  Require:

  ```python
  {
      "name": "TRANSIENT_EVENTS",
      "subjects": [
          "agents.*.task_progress.>",
          "agents.*.heartbeat",
          "agents.*.status",
      ],
      "retention": "limits",
      "storage": "file",
      "max_age_ns": 3_600_000_000_000,
      "max_bytes": 1_073_741_824,
      "max_msg_size": 1_048_576,
      "discard": "old",
      "duplicate_window_ns": 300_000_000_000,
  }
  ```

  Require its observer consumer to use explicit ack, 30-second ack wait, maximum
  three deliveries, and 256 ack-pending events. Every transient publisher must
  await a PubAck; W3 reconnect must classify replay-delivered frames separately.
  Require terminal and progress observers to acknowledge only through
  `ObserverDelivery.ack()` and prove those acknowledgements cannot reach the
  worker `InboundDelivery`.

- [ ] **Step 4: Write failing duplicate-ablation tests**

  Parameterize:

  ```python
  ABLATIONS = {
      "none": {"nats_msg_id": False, "outcome_ledger": False},
      "broker-only": {"nats_msg_id": True, "outcome_ledger": False},
      "full-contract": {"nats_msg_id": True, "outcome_ledger": True},
  }
  ```

  Assert only EdgeCitadel duplicate/crash workload setup accepts these ablations;
  the primary four-mode matrix uses `full-contract`; and all differences are
  recorded in resolved configuration and receipts.

- [ ] **Step 5: Write failing late CLI wiring tests**

  Invoke:

  ```python
  await main(
      ["--config", str(config_path)],
      environ={
          "NATS_URL": nats.url,
          "EC_CREDENTIAL_FILE": str(credential_file),
      },
      transport_factory=recording_factory,
  )
  ```

  for each NATS mode, and use only `RELAY_URL` for central relay. Require exactly
  one concrete transport selection, one `run_fixture` call, SIGTERM-driven
  graceful close, no token in argv/config/log/event data, and nonzero exit for
  invalid mode/auth/config. Run the actual module command in a subprocess against
  a short-lived fake transport factory entry point and require its argv to remain
  exactly `--config <path>`.

- [ ] **Step 6: Run the tests and confirm failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_native_control.py -k cli -q
  scripts/research/run-python -m pytest -p no:cacheprovider -m docker \
    tests/research/test_nats_modes.py \
    -k "edgecitadel or all_durable or ablation" -q
  ```

  Expected: tests fail because the JetStream modes, fixed configs, and late-bound
  transport factory are absent.

- [ ] **Step 7: Implement both transports and late CLI wiring**

  Use `js.publish(..., headers={"Nats-Msg-Id": envelope["id"]})` only when enabled,
  convert PubAck duplicate/sequence data into the shared `PublicationReceipt`,
  and use the exact run-unique durable-name function while retaining exact
  filters. Precreate and inspect every consumer, then pass its complete
  `ConsumerBinding` to the injected production `PullConsumer`; do not call the
  legacy bootstrap path. Sender observers use the selected result plane.

  Implement every Task 4 fault control. Worker stop/start operates only the
  fixture process. Progress observer disconnect/reconnect retains the durable
  transient observer only for all-durable. Coordinator restart uses
  `NatsServer.restart(preserve_storage=True)` in integration tests and the
  run-owned Compose service in campaigns. Wait for authenticated readiness and
  revalidate streams/consumers after restart before resuming the trial.

  Add `build_transport(config, endpoints, token, event_sink)` to
  `native_control.py`, mapping each of the four exact mode strings to its concrete
  transport. `main(argv, environ=os.environ, transport_factory=build_transport)`
  parses/validates config before reading the token, installs SIGINT/SIGTERM
  handlers, constructs one transport, calls `run_fixture`, closes it in `finally`,
  and maps validation/auth/runtime errors to stable secret-free exit messages.
  Add the exact `asyncio.run(main(sys.argv[1:]))` module entry point. This is the
  first task in which the fixture imports concrete mode modules.

- [ ] **Step 8: Verify and commit**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_native_control.py -q
  scripts/research/run-python -m pytest -p no:cacheprovider -m docker \
    tests/research/test_nats_modes.py -q
  scripts/research/run-python -m compileall -q \
    scripts/research/modes scripts/research/fixtures/native_control.py
  git diff --check
  ```

  Expected: all mode, consumer-binding, fault-control, and actual fixture CLI tests
  pass, and no owned NATS resource remains. Run `commit-check`, stage only the six
  Task 7 files, and commit:

  ```bash
  git commit -m "feat(nats): add split-plane and durable controls"
  ```

### Task 8: Own Every Runtime Resource And Prove Preflight

**Files:**
- Create: `scripts/research/artifact_env.py`
- Create: `scripts/research/preflight.py`
- Create: `scripts/research/Dockerfile`
- Create: `scripts/research/docker-compose.artifact.yml`
- Create: `scripts/research/configs/nats/core.conf`
- Create: `scripts/research/configs/nats/jetstream.conf`
- Create: `tests/research/test_artifact_env.py`
- Create: `tests/research/test_preflight.py`
- Create: `.dockerignore`

- [ ] **Step 1: Write failing ownership and cleanup tests**

  Create `tests/research/test_artifact_env.py` with `import re` and an injected
  Docker command runner. Require:

  ```python
  env = ArtifactEnvironment.create(
      "ec-20260725-test-a", "edgecitadel", tmp_path / "raw"
  )
  assert env.project == "edgecitadel-artifact-ec-20260725-test-a"
  assert env.credential_file.stat().st_mode & 0o777 == 0o600
  credential_bytes = env.credential_file.read_bytes()
  assert len(credential_bytes) == 65
  assert re.fullmatch(rb"[0-9a-f]{64}\n", credential_bytes)
  assert env.compose_env["COMPOSE_PROJECT_NAME"] == env.project
  assert env.compose_env["EC_RUN_ID"] == "ec-20260725-test-a"
  assert env.output_dir == tmp_path / "raw" / "ec-20260725-test-a"
  ```

  Reject shell metacharacters, path separators, empty IDs, reused output
  directories, and modes outside the four declared values. Exercise
  `start_topology()` with a fake runner and assert the command is an argv list,
  not a shell string. Call `cleanup()` twice and require the second call to
  report success without touching a differently labeled container, network,
  volume, or image. Require the first call to remove the credential file, mutable
  state directory, Compose scratch, and secret-free recovery record while
  preserving raw/finalized output; require the second call to report those paths
  already absent.

  Add a real-Docker test marked `@pytest.mark.docker` that starts two minimal
  topologies concurrently and requires distinct projects, networks, volumes,
  credentials, state paths, identities, and images. Send `SIGTERM` to a launcher
  child and require owned-resource cleanup. Require one campaign-owned immutable
  application image build before the repetition loop, `--no-build` on every
  repetition start, and campaign-final removal of that image after all
  repetition resources are gone. In Docker-backed parameterized tests, apply
  `lan`, `50ms-rtt`, and `1pct-loss` to both endpoint containers and assert exact
  `tc qdisc show` output plus the five-second probe before cleanup.
  Kill the child after credential creation but before Compose start and after
  Compose start in separate cases, then run `cleanup --run-id` and require both
  recovery paths to remove every owned Docker resource, credential, mutable state,
  and recovery record without deleting raw logs.

- [ ] **Step 2: Run ownership tests and verify the missing module**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_artifact_env.py -q
  ```

  Expected: collection fails because `scripts.research.artifact_env` is absent.

- [ ] **Step 3: Implement the environment API**

  Use these concrete ownership types:

  ```python
  @dataclass(frozen=True)
  class OwnedResource:
      kind: Literal["container", "network", "volume", "image"]
      name: str

  @dataclass(frozen=True)
  class CleanupReport:
      attempted: tuple[OwnedResource, ...]
      remaining: tuple[OwnedResource, ...]
      credential_removed: bool
      state_removed: bool
      scratch_removed: bool
      recovery_record_removed: bool
      completed: bool
  ```

  `ArtifactEnvironment.create()` creates a mode-specific directory with
  permissions `0700`, generates 32 random bytes as
  `secrets.token_hex(32) + "\n"`, and writes those exact 65 ASCII bytes to
  `credential_file` at mode `0600`. The token therefore has exactly 64 lowercase
  hexadecimal characters followed by one newline. It also creates a project name
  derived only from the validated run ID and labels
  `ai.edgecitadel.owner=artifact` and `ai.edgecitadel.run-id=<run-id>`.
  Mutable state and the token live below
  `${EC_ARTIFACT_SCRATCH_ROOT:-${TMPDIR:-/tmp}/edgecitadel-artifact}/<run-id>/`,
  never below raw output. Before returning, `create()` atomically writes a
  mode-0600 secret-free owner record at
  `<scratch-root>/owners/<run-id>.json` containing project name, mode, Compose
  file, credential path, mutable-state paths, output directory, ownership labels,
  and campaign-image reference, but no token or environment dump. It also records
  a canonical empty-directory inventory as `freshness_attestation` in the
  resolved configuration before any service starts.
  `start_topology()` calls:

  ```python
  [
      "docker", "compose", "--project-name", self.project,
      "--file", str(compose_file), "up", "--detach", "--no-build", "--wait",
  ]
  ```

  `cleanup()` runs Compose down with `--volumes --remove-orphans` and then lists
  containers, networks, and volumes by both ownership labels. It deliberately
  excludes the campaign-owned immutable image from repetition `remaining`; no
  repetition may pass `--rmi` or remove that shared image. It removes only
  credential/mutable paths named in the owner record after resolving and proving
  each remains under the recorded scratch root, removes the run scratch directory,
  and removes the run owner record last. It fails while any run-owned Docker or
  path resource remains and never deletes by name prefix alone. `cleanup --run-id`
  reads the record; if a crash occurred before the record write, it may query
  exact run labels and the validated deterministic scratch path, but may not
  delete any arbitrary unrecorded path.

  A campaign owner builds the content-hash-tagged application image once before
  scheduling repetitions, labels it with the campaign ID rather than a repetition
  run ID, injects that immutable tag through `EC_ARTIFACT_IMAGE`, and keeps a
  separate secret-free campaign owner record. Add
  `cleanup_campaign_image(image_ref, campaign_id) -> OwnedResource`; it verifies
  both the exact reference and campaign label, refuses removal while any container
  or another active campaign references the image, and removes it plus this
  campaign's owner record only after the campaign's final repetition.
  Campaign/signal recovery invokes repetition cleanup first and this finalizer
  last.

- [ ] **Step 4: Write failing two-phase preflight tests**

  Create fixed fixtures for all four modes. Before topology start, call
  `run_prestart_preflight(request)` and require it to reject:

  - unreadable, placeholder, or world-readable credential files;
  - a nonempty fresh-state directory;
  - a freshness attestation whose path/hash differs from the current empty
    inventory;
  - invalid resolved mode/workload configuration;
  - an unsupported host/network declaration for paper profile.

  After topology, observer, and worker start, call the existing public
  `run_preflight(request)` and require it to reject:

  - failed authentication;
  - a missing worker or observer;
  - any task stream/consumer field that differs from Section 2.4;
  - any EdgeCitadel stream capturing progress, heartbeat, or status;
  - any all-durable transient stream/publisher/consumer mismatch;
  - a network-profile mismatch, including missing endpoint qdiscs;
  - an unsupported workload/mode pair.

  Require every returned check in both phases to contain `name`, `passed`,
  `observed`, and `expected`, and require `require_valid()` to raise one combined
  error. Starting services after a passing pre-start attestation may populate
  mutable state without causing the post-start phase to call it stale; altering
  the attested pre-start inventory must still fail.

- [ ] **Step 5: Run preflight tests and confirm failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_preflight.py -q
  ```

  Expected: collection fails because `scripts.research.preflight` is absent.

- [ ] **Step 6: Implement preflight and the hermetic topology**

  Implement the public `PreflightRequest`, `PreflightReport`, and
  `run_preflight` API declared above plus:

  ```python
  async def run_prestart_preflight(
      request: PreflightRequest,
  ) -> PreflightReport: ...
  ```

  Keep `PreflightRequest` and `run_preflight(request)` unchanged for Slices 2/3.
  The pre-start function verifies the attestation in
  `request.resolved_config["freshness_attestation"]`; the post-start function
  verifies authentication, readiness, and live topology and embeds the pre-start
  check results. Compare normalized complete configuration dictionaries; do not
  accept substring or partial matches.

  `docker-compose.artifact.yml` contains no host `ports`. It uses profiles to
  start exactly one controller/transport, one `native-control` worker, one direct
  observer, one runner, and the required broker. Mount only the run-owned config,
  credential, state, and output paths. Only the two shaped endpoint services
  receive:

  ```yaml
  cap_add:
    - NET_ADMIN
  ```

  No service is privileged. NATS configuration reads the credential through a
  mounted file generated from the run secret before start.

  Build `scripts/research/Dockerfile` from a Python 3.12 image pinned by
  `@sha256`; install `iproute2` with `apt-get install --no-install-recommends`
  and remove `/var/lib/apt/lists/*` in the same layer. Compose uses the exact
  Task 0 NATS image digest. Resolve the Python immutable digest with:

  ```bash
  docker pull python:3.12-slim
  docker image inspect python:3.12-slim --format '{{index .RepoDigests 0}}'
  ```

  Copy the Task 0 lock into the image and require hash verification:

  ```dockerfile
  COPY scripts/research/requirements.lock.txt /tmp/requirements.lock.txt
  RUN python -m pip install --no-cache-dir --require-hashes \
      -r /tmp/requirements.lock.txt \
      && rm /tmp/requirements.lock.txt
  ```

  Replace the floating Python reference with the exact inspected digest before
  committing and assert Compose's NATS reference equals
  `toolchain.json["nats_image"]`. Implement network application and
  inspection with argv-form `tc qdisc replace`/`tc qdisc show`: `lan` deletes
  run-owned shaping, `50ms-rtt` applies `25ms` fixed egress delay at both
  endpoints, and `1pct-loss` applies `1%` independent egress loss at both
  endpoints. Preflight records the five-second probe and exact qdisc state.

  Ensure `.dockerignore` excludes
  `.git`, `.env*`, `data/`, `tmp/`, `docs/research/results/raw/`, credentials,
  videos, and local test output.

- [ ] **Step 7: Verify configuration and cleanup**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_artifact_env.py tests/research/test_preflight.py -q
  scripts/research/run-python -m pytest -p no:cacheprovider \
    -m docker tests/research/test_artifact_env.py -k network_profiles -q
  docker compose -f scripts/research/docker-compose.artifact.yml config --quiet
  scripts/research/run-python -m compileall -q scripts/research/artifact_env.py \
    scripts/research/preflight.py
  ```

  Expected: focused tests, both preflight phases, crash recovery, credential/path
  cleanup, and all three Docker network-profile cases pass; Compose emits no
  errors, compilation is silent, and no owner record or owned mutable path remains.

- [ ] **Step 8: Commit Task 8**

  Run `commit-check`, use hunk staging so pre-existing user changes are excluded,
  and commit:

  ```bash
  git commit -m "feat(infra): isolate research artifact stacks"
  ```

### Task 9: Execute The Complete W1-W8 Matrix

**Files:**
- Create: `scripts/research/workload_matrix.py`
- Create: `tests/research/test_workload_matrix.py`

- [ ] **Step 1: Write the failing matrix contract tests**

  Require the primary matrix plus EdgeCitadel ablations to contain exactly 46
  executable cells:

  ```python
  assert len(required_matrix_cells()) == 46
  assert {cell.workload for cell in required_matrix_cells()} == {
      "W1", "W2", "W3", "W4", "W5", "W6a", "W6b", "W6c", "W7", "W8",
  }
  assert {cell.mode for cell in required_matrix_cells()} == {
      "central-relay", "core-only", "edgecitadel", "all-durable",
  }
  ```

  Assert exactly 40 primary cells exist: four modes for each of W1, W2, W3, W4,
  W5, W6a, W6b, W6c, W7, and W8. Assert exactly six additional cells: the
  `none` and `broker-only` EdgeCitadel variants for each of W6a, W6b, and W8;
  primary EdgeCitadel uses `full-contract`.

- [ ] **Step 2: Write failing workload-semantic tests**

  Use a recording `TaskTransport` and deterministic clock to prove:

  - W1 submits once and accepts exactly one matching terminal nonce.
  - W2 creates a fresh child task, retains context, increments hop count, and
    repeats `payload.parent_task_id` in the terminal.
  - W3 emits exactly 20 256-byte progress payloads at 50 ms intervals in every
    mode and records generated, live-delivered, replay-delivered, and missing
    counts. Core-only and EdgeCitadel must miss the disconnected ten;
    all-durable must classify them as replay-delivered; central relay records its
    exact non-durable progress behavior.
  - W4 submits before worker readiness and never uses the operator REST API.
    Central relay, EdgeCitadel, and all-durable must recover one task after
    reconnect; Core-only must record transport acceptance followed by loss/no
    terminal rather than silently changing the denominator.
  - W5 executes all six named crash subtrials inside each mode cell and records
    Core-only's `after-publish-mark-before-inbound-commit` boundary explicitly
    as transport-inapplicable.
  - W6a reuses the exact serialized envelope and wire ID.
  - W6b uses a new wire ID with equal task/fingerprint, including one retry after
    the five-minute broker duplicate window but before configured ledger expiry.
  - W6c runs both changed-sender and changed-payload mutations and requires
    `task_id_collision` without cached output exposure.
  - W7 restarts the central relay or NATS broker only after acceptance and
    preserves the declared volume. Relay and JetStream modes recover one task;
    Core-only records the accepted-but-lost task.
  - W8 records handler attempts, external side effects, prepared outcomes, and
    terminals separately for all three EdgeCitadel ablations; the
    after-side-effect crash may duplicate the effect and must never be labeled
    exactly once.

  Also require all modes to receive the same payload bytes, task timeout, worker
  implementation, `TaskExecutor`, and `OutcomeStore` settings for a cell.

- [ ] **Step 3: Run the tests and verify module-not-found**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_workload_matrix.py -q
  ```

  Expected: collection fails because `workload_matrix.py` is absent.

- [ ] **Step 4: Implement fixed workload definitions**

  Define:

  ```python
  class CrashPoint(StrEnum):
      AFTER_RECEIVE = "after-receive-before-handler"
      AFTER_SIDE_EFFECT = "after-side-effect-before-ledger-prepare"
      AFTER_PREPARE = "after-ledger-prepare-before-result-publish"
      AFTER_PUBLISH = "after-result-publish-before-publish-mark"
      AFTER_MARK = "after-publish-mark-before-inbound-commit"
      DURING_EXCEPTION = "during-handler-exception-conversion"

  @dataclass(frozen=True)
  class MatrixCell:
      workload: str
      mode: str
      variant: str
      ablation: str
      timeout_seconds: int
  ```

  `run_cell(cell, transport, fixture, observers, event_sink)` owns the exact
  workload sequence but delegates every delivery operation to `TaskTransport`.
  It returns a `TrialObservation` with initiated, accepted, delivered, execution,
  side-effect, logical-terminal, distinct-terminal-ID, publication-attempt,
  wire-delivery, progress, poison, timeout, and final-transport fields. A timeout
  is a recorded valid task failure; it is never converted into a result.

- [ ] **Step 5: Run the full workload unit suite**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_workload_matrix.py -q
  scripts/research/run-python -m compileall -q scripts/research/workload_matrix.py
  ```

  Expected: every matrix, sequence, fault, and invariant test passes.

- [ ] **Step 6: Commit Task 9**

  Run `commit-check`, stage only the Task 9 files, and commit:

  ```bash
  git commit -m "feat(nats): execute the reliability workload matrix"
  ```

### Task 10: Record Comparable Metrics And Tamper-Evident Evidence

**Files:**
- Create: `schemas/research-manifest.v1.json`
- Create: `schemas/research-event.v1.json`
- Create: `schemas/research-trial.v1.json`
- Create: `scripts/research/metrics.py`
- Create: `scripts/research/evidence.py`
- Create: `tests/research/test_metrics.py`
- Create: `tests/research/test_evidence.py`

- [ ] **Step 1: Write failing schema and canonical-evidence tests**

  Validate benchmark, operator, and lab manifests through an `evidence_kind`
  discriminator. Require benchmark trial records to carry nullable values
  explicitly and to reference raw event/resource records.

  Test the declared public API:

  ```python
  write_json(bundle / "preflight.json", {"z": 1, "a": 2})
  assert (bundle / "preflight.json").read_bytes() == b'{"a":2,"z":1}\n'
  status = finalize_bundle(bundle, manifest, schema_path)
  assert status == "PASS"
  assert json.loads((bundle / "manifest.json").read_text())["status"] == "PASS"
  ```

  Require a second finalization, an existing raw path, a leaked generated token,
  a bearer/private-key pattern, a missing provenance field, an unhashable file,
  or an owned-resource cleanup failure to return `INVALID` without overwriting
  raw evidence.

  Test source capture before any output:

  ```python
  source = capture_source_provenance(clean_checkout)
  write_json(external_output / "campaign.json", {"source": source.to_dict()})
  assert verify_source_provenance(clean_checkout, source)
  (clean_checkout / "scripts/research/workload_matrix.py").write_text("changed")
  assert not verify_source_provenance(clean_checkout, source)
  ```

  Require output created under any declared results directory or external output
  root not to alter the captured source hash, while a tracked or relevant
  untracked source change does. One campaign captures exactly one immutable source
  object before creating its campaign directory and passes that object to every
  repetition manifest.

- [ ] **Step 2: Write failing metric-window tests**

  With a fake monotonic clock and cgroup reader, assert:

  - idle baseline is exactly two seconds;
  - active sampling is every 100 ms from T0 through terminal/declared timeout;
  - CPU is delta CPU-seconds;
  - memory reports peak RSS and trapezoidal RSS-seconds;
  - network reports per-component RX/TX plus application bytes, NATS connection
    byte deltas for NATS modes, and HTTP request/response bytes for relay;
  - storage reports post-flush minus pre-trial bytes plus broker message-count
    deltas;
  - component membership is identical for comparisons;
  - failed trials retain full-window resource costs;
  - sampler CPU above 0.02 CPU-seconds per wall second invalidates cost claims.

  Add workload-specific schema assertions: W5 requires crash point, ledger state,
  inbound deliveries, executions, side effects, publish attempts, logical/wire
  terminals, and final consumer state; W6a requires both PubAck latencies,
  duplicate flags, stream sequences, before/after consumer snapshots, pending,
  and ack-pending; W6b requires both IDs, fingerprint equality, ledger decision,
  execution count, and terminal counts; W6c requires both senders/fingerprints,
  collision decision, rejection, cached-output exposure count, and execution
  count.

- [ ] **Step 3: Run tests and confirm both modules are absent**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_metrics.py tests/research/test_evidence.py -q
  ```

  Expected: collection fails for missing `metrics` and `evidence` modules.

- [ ] **Step 4: Implement schemas and evidence writers**

  `research-event.v1.json` requires `run_id`, `trial_id`, `sequence`,
  `monotonic_ns`, `epoch_time`, `component`, `event`, and `data`.
  `research-trial.v1.json` requires the matrix cell, outcome classification,
  all direct counts, time fields, resource link, and invariant results.
  `research-manifest.v1.json` uses `if`/`then` branches on `evidence_kind`.
  Every kind requires:

  ```text
  schema_version, evidence_kind, status, run_id,
  source, command, timing, host, dependencies, images, compose_config_sha256,
  schemas, cleanup, artifacts
  ```

  Only benchmark evidence requires `campaign_id`, `profile`,
  `transport_config`, `workload_config`, and `metric_contract`. Operator and lab
  branches require their respective task/media or controller/node fields, so
  they can use the same finalizer without dummy benchmark values.

  Define:

  ```python
  @dataclass(frozen=True)
  class SourceProvenance:
      commit: str
      git_dirty: bool
      source_sha256: str
      paths: tuple[str, ...]

      def to_dict(self) -> dict[str, object]: ...

  def capture_source_provenance(source_root: Path) -> SourceProvenance: ...
  def verify_source_provenance(
      source_root: Path, expected: SourceProvenance
  ) -> bool: ...
  ```

  Source provenance includes commit, dirty state, and a hash over relevant
  tracked plus untracked source content. Run Git with `cwd=source_root`; the
  source set is exactly `git ls-files --cached --others --exclude-standard`,
  excluding only
  `docs/research/results/raw/`, `docs/research/results/derived/`,
  `docs/research/results/operator/`, `docs/research/results/lab/`, `tmp/`, and
  generated build/test output. It never includes secret values. Capture once
  before creating output and verify against the same immutable object at campaign
  finalization; never recompute `git_dirty` after generated output exists.

  Implement `write_json`, append-only `write_jsonl`, file hashing, strict secret
  scanning, atomic manifest replacement, and refusal to mutate a finalized
  bundle. Hashes cover every non-manifest raw file and the manifest carries its
  own canonical content hash field computed with that field absent.

- [ ] **Step 5: Implement the resource sampler**

  Use `time.perf_counter_ns()` for durations and epoch time only for correlation.
  Read cgroup/container counters through an injected provider. Record controller,
  transport/broker, worker, and observer totals with sampler cost separate.
  Provide a no-op calibration path using the same interval and window code.

- [ ] **Step 6: Run focused validation**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_metrics.py tests/research/test_evidence.py -q
  scripts/research/run-python -m json.tool \
    schemas/research-manifest.v1.json >/dev/null
  scripts/research/run-python -m json.tool \
    schemas/research-event.v1.json >/dev/null
  scripts/research/run-python -m json.tool \
    schemas/research-trial.v1.json >/dev/null
  ```

  Expected: all tests pass and every schema parses.

- [ ] **Step 7: Commit Task 10**

  Run `commit-check`, stage only Task 10 files, and commit:

  ```bash
  git commit -m "feat(nats): record verifiable experiment evidence"
  ```

### Task 11: Fix Profiles, Schedules, And Run Lifecycle

**Files:**
- Create: `scripts/research/configs/schema/campaign.schema.json`
- Create: `scripts/research/configs/schema/hardware.schema.json`
- Create: `scripts/research/configs/schema/network.schema.json`
- Create: `scripts/research/configs/campaigns/preliminary-x86-lan.yaml`
- Create: `scripts/research/run_artifact.py`
- Create: `tests/research/test_schedule.py`
- Create: `tests/research/test_artifact_profiles.py`

- [ ] **Step 1: Write failing profile and schedule tests**

  Require:

  ```python
  quick = build_schedule(profile="quick", seed=20260725)
  assert quick.warmup_count == 4
  assert quick.measured_count == 18
  assert quick.inferential is False

  smoke = build_schedule(profile="matrix-smoke", seed=20260725)
  assert len(smoke.repetitions) == 46
  assert all(not rep.measured for rep in smoke.repetitions)

  paper = build_schedule(
      profile="paper",
      campaign_config=Path(
          "scripts/research/configs/campaigns/preliminary-x86-lan.yaml"
      ),
  )
  assert paper.warmup_blocks == 5
  assert paper.measured_blocks == 30
  assert len(paper.repetitions) == 35 * 46
  ```

  Quick's 18 measured repetitions are three W1 trials for each of four modes
  plus W6a and W6b under each of three EdgeCitadel ablations. Each mode also
  receives one unmeasured W1 warmup. Test that every repetition has a fresh
  project/state/identity, block ordering is reproducible from the seed, paper
  blocks contain all 46 cells once, and neither task outcomes nor prior failures
  change the schedule.

- [ ] **Step 2: Write failing CLI/lifecycle tests**

  Invoke `main(argv, environment_factory, repetition_runner)` with fakes and
  require:

  - `run --profile quick`;
  - `run --profile matrix-smoke`;
  - `run --profile paper --campaign-config
    scripts/research/configs/campaigns/preliminary-x86-lan.yaml`;
  - `cleanup --run-id ec-20260725-example`;
  - optional absolute `--source-root`, `--output-root`, and `--scratch-root` on
    every `run`, plus `--scratch-root` on cleanup.

  Assert source provenance is captured once from `--source-root` before the output
  campaign directory exists. Then `campaign.json` and `schedule.jsonl` are written
  and hashed before the first measured block, and the campaign builds its immutable
  image once from that source root. Each repetition calls create, pre-start
  preflight, start topology, start terminal/progress observers, start worker,
  post-start preflight/readiness, calibrate, run trial, stop, cleanup, verify
  unchanged source, and finalize in that order. Finalization always receives the
  completed cleanup report and original `SourceProvenance`. Inject each stage
  failure and require raw logs plus `harness-invalid`, nonzero exit, path/Docker
  cleanup, and an `INVALID` manifest. Valid task failures stay scheduled and do
  not mark the harness invalid.

  Make paper profile fail before image build when `git status --porcelain=v1
  --untracked-files=all` is nonempty. Quick and matrix-smoke may run dirty but
  must set `source.git_dirty=true` and `publication_eligible=false`. Test both
  tracked and untracked dirty cases.

- [ ] **Step 3: Run tests and verify missing schedule implementation**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_schedule.py tests/research/test_artifact_profiles.py -q
  ```

  Expected: collection fails because `run_artifact.py` is absent.

- [ ] **Step 4: Implement validated configuration**

  The preliminary YAML fixes:

  ```yaml
  schema_version: 1
  campaign_id: preliminary-x86-lan
  seed: 20260725
  warmup_blocks: 5
  measured_blocks: 30
  bootstrap_seed: 20260725
  bootstrap_samples: 10000
  hardware_profile: x86_64-controller
  network_profile: lan
  sampler_interval_ms: 100
  idle_baseline_seconds: 2
  ```

  Include every workload timeout and the identical resource component set.
  Hardware schema accepts declared `x86_64` and `aarch64` Linux gateway profiles
  and requires Ubuntu 24.04, CPU, memory, cgroup limits, and clock source. The
  preliminary YAML selects x86_64; Paper Evidence Ready later requires complete
  checked campaigns for both architectures. Network schema fixes `lan`,
  `50ms-rtt`, and `1pct-loss` fields plus the five-second observed probe and
  endpoint qdisc snapshots. The runner applies the exact endpoint shaping from
  Task 8 and refuses a qdisc/probe or host/config mismatch for paper profile;
  quick and matrix-smoke record unsupported host facts as development evidence
  without upgrading readiness.

- [ ] **Step 5: Implement the CLI and lifecycle**

  Parse with `argparse` and expose exactly the four commands in Section 4.5/4.6.
  Every `run` accepts `--result-file`, `--source-root`, `--output-root`, and
  `--scratch-root`. The result file receives canonical JSON containing the exact
  `campaign_path`, ordered `bundle_paths`, source commit/hash, and profile for
  automation; a multi-repetition profile never reports a single representative
  bundle as its verification target.

  Resolve all roots before output, require `--source-root` to be a Git checkout,
  call `capture_source_provenance` exactly once, and only then create output.
  Generate the full schedule, canonicalize/hash it, build the campaign image once
  from the captured source, then use a fresh `ArtifactEnvironment` for every
  repetition with `EC_ARTIFACT_SCRATCH_ROOT` set to the explicit scratch root.
  Run `run_prestart_preflight` before Compose. After topology start, start direct
  observers and their fetch loops, start the worker, and run `run_preflight`
  before T0. Do not call the production aggregator or REST API inside benchmark
  repetitions. Write raw events/trials, stop and clean all owned resources,
  verify source content still matches the original capture, then finalize with
  the cleanup report and original source object. Preserve invalid logs and run
  the same cleanup/finalization sequence in `finally`. Outside the repetition
  loop, a campaign-level `finally` calls `cleanup_campaign_image` after all
  repetition cleanup attempts and records failure to remove it as
  `harness-invalid`.

- [ ] **Step 6: Run schedule/CLI tests and inspect help**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_schedule.py tests/research/test_artifact_profiles.py -q
  scripts/research/run-python scripts/research/run_artifact.py --help
  scripts/research/run-python scripts/research/run_artifact.py run --help
  scripts/research/run-python scripts/research/run_artifact.py cleanup --help
  ```

  Expected: tests pass and help lists only declared arguments with nonzero-error
  behavior for incomplete inputs.

- [ ] **Step 7: Commit Task 11**

  Run `commit-check`, stage only Task 11 files, and commit:

  ```bash
  git commit -m "feat(nats): schedule fixed artifact profiles"
  ```

### Task 12: Analyze Only Complete Valid Campaigns

**Files:**
- Create: `scripts/research/statistics.py`
- Create: `scripts/research/check_artifact.py`
- Create: `scripts/research/analyze_artifact.py`
- Create: `tests/research/test_statistics.py`
- Create: `tests/research/test_checker.py`
- Create: `tests/research/test_analysis.py`

- [x] **Step 1: Write failing statistical unit tests**

  Use fixed published/reference vectors and require:

  - Wilson 95% intervals for `0/10`, `5/10`, and `10/10`;
  - Newcombe pairwise risk-difference intervals;
  - median and nearest-rank p95;
  - paired median difference and relative change by measured block;
  - a seeded 10,000-resample percentile bootstrap interval;
  - no p99 key at `n=999`, and a p99 key at `n=1000`;
  - completed-only latency next to initiated/failure/timeout counts;
  - no imputation or silent row deletion.

- [x] **Step 2: Write failing base checker and deterministic-analysis tests**

  First require `check_bundle()`/`check_campaign()` to validate schema and hashes,
  scheduled-cell/block completeness, source readiness, cleanup, and raw
  invariants. Build a complete tiny fixture campaign, run analysis twice into
  separate directories, and require byte-identical `summary.json`, `report.md`,
  tables, and figure-data files. Delete one cell, alter the predeclared schedule,
  add one harness-invalid repetition, and change component membership in separate
  cases; the checker and analyzer must both exit nonzero before publication
  tables are written.

  Assert the exact public API:

  ```python
  report = check_bundle(bundle)
  assert isinstance(report, CheckReport)
  assert report.valid
  assert report.issues == ()
  report.require_valid()

  mismatch = check_bundle(bundle, expected_kind="operator")
  assert not mismatch.valid
  assert mismatch.issues[0].code == "ARTIFACT_KIND_MISMATCH"
  ```

  Require `expected_kind=None` and `source_root=None` to preserve the one-argument
  call. An operator or lab fixture may pass both optional keywords without a
  Python argument error; Slice 1 performs base schema/hash/kind/source checks and
  leaves kind-specific semantic extension to Slices 2/3. Invalid reports contain
  sorted, stable `ArtifactIssue(code, path, message)` values and
  `require_valid()` raises one deterministic `ArtifactInvalid` without mutating
  the manifest.

- [x] **Step 3: Run tests and verify missing modules**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_statistics.py tests/research/test_checker.py \
    tests/research/test_analysis.py -q
  ```

  Expected: collection fails for missing statistics/checker/analyzer modules.

- [x] **Step 4: Implement base validation, fixed estimators, and analyzer**

  Implement the precondition `ArtifactIssue`, `CheckReport`, and exact
  `check_bundle` signature before the analyzer, plus:

  ```python
  def check_campaign(
      path: Path,
      *,
      require_publication: bool = False,
  ) -> CheckReport: ...
  ```

  Make analysis call
  `check_campaign(..., require_publication=True)` rather than duplicating
  validity logic. Use only the estimators named in Section 4.5. Sort all inputs
  by declared block and cell keys, serialize canonical JSON, use the campaign
  bootstrap seed, and emit p99 only when a cell has at least 1,000 completed
  measurements. Write:

  ```text
  docs/research/results/derived/preliminary-x86-lan/
    summary.json
    report.md
    tables/correctness.csv
    tables/cost.csv
    figures/correctness.json
    figures/cost.json
  ```

  The analyzer refuses quick, matrix-smoke, development, incomplete, dirty,
  hash-invalid, or harness-invalid inputs for publication output.

- [x] **Step 5: Run tests and CLI help**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_statistics.py tests/research/test_checker.py \
    tests/research/test_analysis.py -q
  scripts/research/run-python scripts/research/check_artifact.py --help
  scripts/research/run-python scripts/research/analyze_artifact.py --help
  ```

  Expected: tests pass and help exposes campaign, confidence, bootstrap-samples,
  input-root, and output-root arguments.

  Local evidence (2026-08-01): the focused contract suite passed with 60 tests;
  the full research suite passed with 665 tests and 34 environment skips; Ruff
  and strict Mypy passed for the maintained modules. The analyzer is ready
  for valid input, but the paper runner remains publication-ineligible until
  Tasks 10-11 integrate real component samples and complete trial records.
  Independent re-review found no remaining blocker, critical, or important issue
  in the corrected analysis and publication-validation boundary.

- [ ] **Step 6: Commit Task 12**

  Run `commit-check`, stage only Task 12 files, and commit:

  ```bash
  git commit -m "feat(nats): validate and analyze paired campaigns"
  ```

### Task 13: Reject Corrupt, Incomplete, Or Leaky Artifacts

**Files:**
- Modify: `scripts/research/check_artifact.py`
- Modify: `tests/research/test_checker.py`
- Modify: `tests/research/test_artifact_env.py`
- Modify: `tests/research/test_artifact_profiles.py`

- [ ] **Step 1: Write failing checker fixtures**

  A valid fixture must pass. Independently mutate:

  - one raw byte after manifest finalization;
  - one schema version;
  - one source/provenance field;
  - one schedule/campaign hash;
  - one required cell or measured block;
  - one terminal logical/wire/execution/side-effect invariant;
  - one stream/consumer setting;
  - one resource component/window;
  - one cleanup result;
  - one generated credential pattern.

  Require a stable machine-readable issue code and nonzero exit for each.
  Deliberate valid task failures pass completeness checks when still present in
  their scheduled cells; harness-invalid or missing repetitions make a campaign
  incomplete.

- [ ] **Step 2: Add real isolation and recovery tests**

  Mark these Docker-backed tests explicitly:

  - run two quick fixtures concurrently and prove distinct projects, storage,
    subjects, identities, consumers, credentials, raw directories, and cleanup;
  - interrupt one run and use
    `cleanup --run-id ec-20260725-interrupted` twice;
  - leave an unrelated labeled resource and prove cleanup preserves it;
  - verify containers, networks, volumes, and locally built project images are
    absent after normal and signal teardown;
  - verify credential, state, run scratch, and owner record are absent after
    normal, pre-Compose interruption, post-Compose interruption, and repeated
    recovery cleanup while raw evidence remains.

- [ ] **Step 3: Run tests and confirm hardening cases fail**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_checker.py -q
  ```

  Expected: base valid fixtures pass, while the newly added corruption,
  invariant, secret, and cleanup assertions fail against the base checker.

- [ ] **Step 4: Implement run and campaign checks**

  Complete the already exposed optional-keyword `check_bundle` and
  `check_campaign(path, *, require_publication=False)` APIs without changing
  `ArtifactIssue`, `CheckReport`, or one-argument compatibility. Recompute hashes, validate
  every schema, compare campaign/schedule immutability,
  check expected cells/blocks and raw invariant counts, enforce source and cleanup
  readiness, scan all file names/content for credentials, and reject derived
  outputs whose input hash differs. The CLI prints one issue per line, then
  `artifact: PASS` or `artifact: INVALID`, and exits `0` only for PASS.

- [ ] **Step 5: Run checker and isolation tests**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_checker.py tests/research/test_artifact_env.py \
    tests/research/test_artifact_profiles.py -q
  scripts/research/run-python scripts/research/check_artifact.py --help
  ```

  Expected: all non-Docker tests pass; help exposes mutually exclusive
  `--bundle` and `--campaign`. Run Docker-marked tests before Task 14.

- [ ] **Step 6: Commit Task 13**

  Run `commit-check`, stage only Task 13 files, and commit:

  ```bash
  git commit -m "test(nats): enforce artifact validity"
  ```

### Task 14: Run Hermetic Gates And Document Honest Readiness

**Files:**
- Modify: `docs/research/results/README.md`
- Modify: `.gitignore`
- Modify: `tests/research/test_artifact_profiles.py`
- Modify after clean gates: `docs/research/task-aware-reliability-contract-design.md`
- Create from clean quick gate: exact campaign directory reported beneath
  `docs/research/results/raw/`
- Create from clean matrix-smoke gate: exact campaign directory reported beneath
  `docs/research/results/raw/`

- [ ] **Step 1: Write a failing documentation/profile contract test**

  Add the test to `tests/research/test_artifact_profiles.py`. Require the results
  README to contain the exact quick, matrix-smoke, paper, analyze, check, and
  cleanup commands; raw/derived layout; valid-task-failure versus harness-invalid
  distinction; quick/no-statistics and p99 guards; current readiness labels; and
  the unsupported-host rule. Require `.gitignore` to ignore only
  `tmp/research/` and transient Compose scratch, not
  `docs/research/results/raw/` or `derived/`.

  Before evidence capture, require the R-01 and R-03 through R-07 rows in the
  design spec not to use requirement status `Verified` or `Measured`, or claim
  `Paper Evidence Ready` or `Paper Ready`. Add a second test helper, initially
  unused, that validates an advanced row only when every repository-relative
  evidence path in that row exists and every referenced campaign passes
  `check_campaign`.

- [ ] **Step 2: Run the test and confirm documentation failure**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_artifact_profiles.py -k documentation -q
  ```

  Expected: the current README lacks the complete artifact contract.

- [ ] **Step 3: Write the concise artifact runbook**

  Document prerequisites, one-command profiles, expected output roots, cleanup,
  analysis reproduction, validity states, claim limits, and current readiness.
  State explicitly that ARM64/macOS quick or matrix-smoke output is development
  evidence, not the preliminary x86_64/Linux campaign. Document that clean source
  and a passing checker can establish `Artifact Verified`, but neither development
  profile establishes `Paper Evidence Ready`. Include `--source-root`,
  `--output-root`, and `--scratch-root` examples and the hash-locked
  `scripts/research/run-python` prerequisite.

- [ ] **Step 4: Run all unit and static gates**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    schemas/tests/test_task_correlation_schema.py \
    aggregator/tests/test_validator.py \
    aggregator/tests/test_api.py \
    adapters/_common/tests/test_outcome_store.py \
    adapters/_common/tests/test_task_executor.py \
    adapters/_common/tests/test_pull_consumer_injection.py \
    adapters/watchdog/tests/test_synth.py \
    tests/research -q
  scripts/research/run-python -m pytest -p no:cacheprovider \
    adapters/shell/tests adapters/gemma/tests \
    adapters/hermes/tests adapters/watchdog/tests -q
  scripts/research/run-python -m compileall -q \
    aggregator adapters/_common scripts/research tests/research
  cd openclaw-client && npm test && cd ..
  docker compose -f scripts/research/docker-compose.artifact.yml config --quiet
  scripts/research/run-python -c \
    'from pathlib import Path; text = Path("docs/research/plans/2026-07-25-slice-1-hermetic-experiment-spine.md").read_text(); forbidden = ("PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. " + "python3", "python3 -m " + "pytest", "python3 -m " + "compileall", "python3 -m " + "json.tool", "python3 scripts/research/" + "run_artifact.py", "python3 scripts/research/" + "check_artifact.py", "python3 scripts/research/" + "analyze_artifact.py"); assert not [item for item in forbidden if item in text]'
  git diff --check
  ```

  Expected: all toolchain, correlation-producer, legacy-adapter, executor,
  research, watchdog, and OpenClaw compatibility tests pass; compilation is
  silent; Compose validates; and no whitespace errors appear.

- [ ] **Step 5: Run Docker isolation tests**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider -m docker \
    tests/research/test_toolchain_contract.py \
    tests/research/test_nats_modes.py \
    tests/research/test_artifact_env.py \
    tests/research/test_artifact_profiles.py -q
  ```

  Expected: concurrent and interrupted runs pass, repeated cleanup passes, and
  the unrelated sentinel resource remains. Credentials, mutable state, scratch,
  and owner records are absent after every case.

- [ ] **Step 6: Freeze the executable clean-capture source**

  Invoke `verify-backend` and `verify-infra`. If either gate reveals a code
  defect, fix it in the task that owns the file, rerun Steps 4-5, and create a
  scoped fix commit before continuing. Then run `commit-check`, stage only the
  Task 14 runbook, ignore, and pre-evidence profile-test paths from the task file
  map with `git add --`, verify the staged design-spec rows still make no
  verified claim, and commit:

  ```bash
  git commit -m "docs(nats): document clean artifact gates"
  ```

  Record `SOURCE_COMMIT="$(git rev-parse HEAD)"`. No executable source, lock,
  schema, Compose file, or runbook may change after this point and before both
  captures finish.

- [ ] **Step 7: Run and check quick and matrix-smoke from a clean detached source**

  Materialize the exact source commit and keep all generated paths outside it:

  ```bash
  set -euo pipefail
  cd /Users/yefanzhang/workplace/edge-research
  SOURCE_COMMIT="$(git rev-parse HEAD)"
  CAPTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/edgecitadel-slice1.XXXXXX")"
  CLEAN_SOURCE="$CAPTURE_ROOT/source"
  RESULT_ROOT="$CAPTURE_ROOT/results"
  SCRATCH_ROOT="$CAPTURE_ROOT/scratch"
  OUTPUT_ROOT="$PWD/docs/research/results/raw"
  cleanup_capture() {
    capture_exit=$?
    if test -d "$CLEAN_SOURCE"; then
      git worktree remove "$CLEAN_SOURCE" || true
    fi
    rm -rf "$RESULT_ROOT" "$SCRATCH_ROOT"
    rmdir "$CAPTURE_ROOT" 2>/dev/null || true
    trap - EXIT
    exit "$capture_exit"
  }
  trap cleanup_capture EXIT
  mkdir -p "$RESULT_ROOT" "$SCRATCH_ROOT"
  git worktree add --detach "$CLEAN_SOURCE" "$SOURCE_COMMIT"
  test -z "$(git -C "$CLEAN_SOURCE" status --porcelain=v1 --untracked-files=all)"

  (
    cd "$CLEAN_SOURCE"
    scripts/research/run-python scripts/research/run_artifact.py run \
      --profile quick \
      --source-root "$CLEAN_SOURCE" \
      --output-root "$OUTPUT_ROOT" \
      --scratch-root "$SCRATCH_ROOT" \
      --result-file "$RESULT_ROOT/quick.json"
    scripts/research/run-python scripts/research/run_artifact.py run \
      --profile matrix-smoke \
      --source-root "$CLEAN_SOURCE" \
      --output-root "$OUTPUT_ROOT" \
      --scratch-root "$SCRATCH_ROOT" \
      --result-file "$RESULT_ROOT/matrix.json"
  )

  QUICK_CAMPAIGN="$("$CLEAN_SOURCE/scripts/research/run-python" -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["campaign_path"])' \
    "$RESULT_ROOT/quick.json")"
  MATRIX_CAMPAIGN="$("$CLEAN_SOURCE/scripts/research/run-python" -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["campaign_path"])' \
    "$RESULT_ROOT/matrix.json")"
  test "$QUICK_CAMPAIGN" != "$MATRIX_CAMPAIGN"
  case "$QUICK_CAMPAIGN" in "$OUTPUT_ROOT"/*) ;; *) exit 1 ;; esac
  case "$MATRIX_CAMPAIGN" in "$OUTPUT_ROOT"/*) ;; *) exit 1 ;; esac
  test -d "$QUICK_CAMPAIGN"
  test -d "$MATRIX_CAMPAIGN"
  "$CLEAN_SOURCE/scripts/research/run-python" \
    "$CLEAN_SOURCE/scripts/research/check_artifact.py" \
    --campaign "$QUICK_CAMPAIGN"
  "$CLEAN_SOURCE/scripts/research/run-python" \
    "$CLEAN_SOURCE/scripts/research/check_artifact.py" \
    --campaign "$MATRIX_CAMPAIGN"

  "$CLEAN_SOURCE/scripts/research/run-python" -c \
    'import json,pathlib,sys; expected,*roots=sys.argv[1:]; sources=[json.loads(path.read_text())["source"] for root in roots for path in pathlib.Path(root).rglob("manifest.json")]; assert sources and all(source["commit"] == expected and source["git_dirty"] is False for source in sources)' \
    "$SOURCE_COMMIT" "$QUICK_CAMPAIGN" "$MATRIX_CAMPAIGN"
  test -z "$(git -C "$CLEAN_SOURCE" status --porcelain=v1 --untracked-files=all)"
  test -z "$(find "$SCRATCH_ROOT" -mindepth 1 -print -quit)"
  git worktree remove "$CLEAN_SOURCE"
  rm -rf "$RESULT_ROOT" "$SCRATCH_ROOT"
  rmdir "$CAPTURE_ROOT"
  trap - EXIT
  ```

  Expected: quick finishes within 30 minutes with four warmups and 18 measured
  development repetitions; matrix-smoke executes all 46 cells once; both campaign
  checks validate every bundle and print `artifact: PASS`; neither emits
  inferential statistics or p99; all manifests record the exact clean source
  commit; all cleanup reports are complete; and the clean checkout remains clean.
  On this ARM64/macOS host these are development campaigns even when clean.

- [ ] **Step 8: Advance traceability only from checked evidence**

  Only after Step 7 passes, change the design-spec traceability table to columns
  `ID`, `Proposed requirement`, `Status`, and `Evidence`. Set only R-01 and R-03
  through R-07 to the defined requirement status `Verified`. Each row must link
  its exact owning test path and the repository-relative quick and/or matrix
  campaign path read from the result files. Those links establish the separate
  artifact-readiness label `Artifact Verified`; that label is not a requirement
  status:

  - R-01 links executor/outcome/PullConsumer tests and both checked campaigns.
  - R-03 links transport/mode tests and matrix-smoke.
  - R-04 links workload/checker tests and matrix-smoke W6 records.
  - R-05 links workload/checker tests and matrix-smoke W5/W8 records.
  - R-06 links evidence/checker tests and both campaign manifests.
  - R-07 links metrics/statistics tests and both development campaigns.

  Leave R-02 and R-08 through R-10 at their existing unverified status. Add a
  sentence immediately below the table: `Paper Evidence Ready remains unmet:
  no complete checked x86_64/ARM64 Linux paper campaign exists.` Replace the
  pre-evidence status assertion from Step 1 with a post-evidence test that calls
  the traceability helper using the exact six row IDs and the two campaign paths.
  Require every advanced evidence path to exist, every referenced campaign to
  return a valid `CheckReport`, and no development campaign to be labeled
  preliminary or paper evidence.

- [ ] **Step 9: Verify and commit only exact checked outputs**

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  scripts/research/run-python -m pytest -p no:cacheprovider \
    tests/research/test_artifact_profiles.py -k "documentation or traceability" -q
  ```

  Run `commit-check`. Stage only the design-spec traceability hunks, the
  traceability-test hunks, and the two exact campaign directories returned by
  Step 7. Do not stage any other existing raw result or user change. Inspect
  `git diff --cached --name-status`, rerun `check_artifact.py --campaign` against
  both staged directory paths, scan the staged blobs for credential/private-key
  patterns, and commit:

  ```bash
  git commit -m "test(nats): record clean artifact verification"
  ```

  The evidence manifests intentionally reference the preceding clean
  `SOURCE_COMMIT`; the evidence-only commit does not rewrite them. The Playwright
  gate remains owned by Slice 2 and is not replaced by curl or claimed here.

- [ ] **Step 10: Record the autonomous Slice 1 handoff**

  Require the task worktree clean, load the record created before Task 0, verify
  that its immutable base/root/branch still match, and atomically advance only
  `FINAL_COMMIT`:

  ```bash
  set -euo pipefail
  CANONICAL_ROOT=/Users/yefanzhang/workplace/edge-research
  EXPECTED_BASE="$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
  CHAIN_KEY="$(printf '%s' "$EXPECTED_BASE" | cut -c1-12)"
  HANDOFF="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY/handoff.env"
  test -f "$HANDOFF"
  # shellcheck disable=SC1090
  source "$HANDOFF"
  test "$CANONICAL_BASE" = "$EXPECTED_BASE"
  test "$(git rev-parse --show-toplevel)" = "$TASK_ROOT"
  test "$(git branch --show-current)" = "$BRANCH"
  test -z "$(git status --porcelain)"
  FINAL_COMMIT="$(git rev-parse HEAD)"
  CHAIN_ROOT="$(dirname "$HANDOFF")"
  CANONICAL_SNAPSHOT="$CHAIN_ROOT/canonical"
  git -C "$CANONICAL_ROOT" write-tree \
    >"$CANONICAL_SNAPSHOT.index-tree.final"
  git -C "$CANONICAL_ROOT" diff --binary \
    >"$CANONICAL_SNAPSHOT.unstaged.patch.final"
  git -C "$CANONICAL_ROOT" diff --cached --binary \
    >"$CANONICAL_SNAPSHOT.staged.patch.final"
  git -C "$CANONICAL_ROOT" status \
    --porcelain=v2 -z --untracked-files=all \
    >"$CANONICAL_SNAPSHOT.status.z.final"
  git -C "$CANONICAL_ROOT" ls-files \
    --others --exclude-standard -z \
    >"$CANONICAL_SNAPSHOT.untracked.z.final"
  while IFS= read -r -d '' relative; do
    test -f "$CANONICAL_ROOT/$relative"
    digest="$(shasum -a 256 "$CANONICAL_ROOT/$relative" | awk '{print $1}')"
    printf '%s  %q\n' "$digest" "$relative"
  done <"$CANONICAL_SNAPSHOT.untracked.z.final" \
    >"$CANONICAL_SNAPSHOT.untracked.sha256.final"
  for suffix in index-tree unstaged.patch staged.patch status.z \
    untracked.z untracked.sha256; do
    cmp "$CANONICAL_SNAPSHOT.$suffix" \
      "$CANONICAL_SNAPSHOT.$suffix.final"
    rm "$CANONICAL_SNAPSHOT.$suffix.final"
  done
  {
    printf 'CANONICAL_ROOT=%q\n' "$CANONICAL_ROOT"
    printf 'CANONICAL_BASE=%q\n' "$CANONICAL_BASE"
    printf 'TASK_ROOT=%q\n' "$TASK_ROOT"
    printf 'BRANCH=%q\n' "$BRANCH"
    printf 'FINAL_COMMIT=%q\n' "$FINAL_COMMIT"
  } >"$HANDOFF.tmp"
  mv "$HANDOFF.tmp" "$HANDOFF"
  ```

  Slice 2 must begin at this exact `FINAL_COMMIT` in `TASK_ROOT`; it may not
  reopen the dirty canonical checkout or create a competing autonomous branch.

## Slice 1 Completion Audit

- [ ] R-01: all four modes execute the same `TaskExecutor`, fixture, task bytes,
  timeout, and measurement hooks; only `TaskTransport` differs.
- [ ] R-03: central relay, Core-only, EdgeCitadel, and all-durable are executable
  transports behind the common contract.
- [ ] R-04: W6a wire retry, W6b semantic retry, and both W6c collision mutations
  execute with the three declared EdgeCitadel ablations where required.
- [ ] R-05: every W5 crash boundary and W8 non-idempotent side-effect boundary
  records recovery, execution, terminal, and side-effect outcomes.
- [ ] R-06: raw files, manifests, source/config/image provenance, hashes, secret
  scan, cleanup result, checker, and deterministic derived data are tested.
- [ ] R-07: direct observers and monotonic clocks own correctness/latency; CPU,
  RSS, bytes, storage, progress, and sampler calibration use identical fixed
  windows/components; quick, 46-cell matrix-smoke, and fixed
  5-warmup/30-measured paper schedules cannot omit, replace, or optionally stop
  repetitions.
- [ ] Re-running analysis from identical valid raw input produces byte-identical
  output.
- [ ] Two concurrent and one interrupted Docker runs leave no owned container,
  network, volume, image, credential, or state path.
- [ ] Quick and matrix-smoke pass on the available host and remain labeled
  development evidence when the host is not the declared x86_64 Ubuntu profile.
- [ ] R-01 and R-03 through R-07 move to requirement status `Verified` only
  after their owning test gates and both required clean campaign checks pass;
  every row links exact checked evidence, while `Paper Evidence Ready` remains
  unmet.
- [ ] No preliminary or paper-readiness claim exists without a complete checked
  campaign on every declared hardware/network profile.
