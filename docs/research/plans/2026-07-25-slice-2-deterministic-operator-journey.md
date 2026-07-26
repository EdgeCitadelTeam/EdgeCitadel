# Deterministic Operator Journey Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the production EdgeCitadel frontend and backend expose one correct
task lifecycle, prove it from an isolated deterministic stack, and capture
desktop and mobile evidence whose project-local task metadata, API snapshots,
trace, video, and screenshots agree.

**Architecture:** Keep the existing FastAPI, SQLite, React, Zustand, NATS, and
WebSocket boundaries. Add observable replay accounting, a synchronous
observation-order reducer, convergent fleet events, and one fleet-wide WebSocket.
A Node launcher owns one uniquely named Compose project per run, lets Docker
allocate loopback ports without a reservation race, and removes containers,
networks, volumes, project-owned build references, and run state. The last task alone
captures evidence: one stack run executes one desktop task and one mobile task,
and `scripts/research/check_artifact.py` validates the finalized bundle.

**Tech Stack:** Python 3.12, SQLite, FastAPI, pytest, React 18, Zustand 5,
Vite 5, Vitest 2, Testing Library, ESLint 9, `@noble/hashes` 1.7,
  Docker Compose v2, digest-pinned NATS 2.10, Playwright 1.49, Node.js.

---

## Preconditions And Safety

- Read Sections 2.3, 5, and 8-10 of
  `docs/research/task-aware-reliability-contract-design.md` before starting.
- Slice 1 must provide these public files:

  ```text
  scripts/research/fixtures/native_control.py
  scripts/research/Dockerfile
  scripts/research/evidence.py
  scripts/research/check_artifact.py
  schemas/research-manifest.v1.json
  ```

- Slice 1 Task 0 must already provide executable
  `scripts/research/run-python` plus its hash-locked Python 3.12 environment.
  Every Slice 2 host Python test, compile, capture, checker, and heredoc command
  uses that launcher. Bare `python3` is allowed only in the documented fixture
  CLI and its digest-pinned Compose container command.
- Every Slice 2 NATS container and provenance record uses
  `scripts/research/toolchain.json["nats_image"]`, whose required value is exactly
  `nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927`.
  The corresponding mutable repository-and-version tag is forbidden.
- The Slice 1 fixture command is exactly:

  ```bash
  python3 -m scripts.research.fixtures.native_control \
    --config /run/config/native-control.json
  ```

- The launcher generates `run_id` and writes this exact non-secret config. The
  two SQLite paths live in the run-owned `fixture-state` volume:

  ```json
  {
    "run_id": "value-generated-by-run-isolated",
    "agent_id": "shell-1",
    "mode": "edgecitadel",
    "behavior": "echo",
    "delay_ms": 1000,
    "crash_point": null,
    "heartbeat_interval_ms": 1000,
    "outcome_db": "/run/state/outcomes.sqlite3",
    "side_effect_db": "/run/state/side-effects.sqlite3"
  }
  ```

  The launcher writes the generated credential to a mode-0600 file and mounts it
  at `/run/secrets/transport-token`. The fixture receives
  `NATS_URL=nats://nats:4222` and
  `EC_CREDENTIAL_FILE=/run/secrets/transport-token`. No credential appears in
  argv, fixture JSON, logs, screenshots, or stored Compose output.
- Every operator command body is the nonce string only. Do not send shell
  syntax. For nonce `4c5f`, the request body is `4c5f` and the deterministic
  terminal payload body is exactly `edgecitadel:4c5f`.
- The shared evidence API is:

  ```python
  from scripts.research.evidence import finalize_bundle, write_json

  status = finalize_bundle(bundle_dir, manifest, schema_path)
  ```

  `write_json` writes canonical JSON. `finalize_bundle` hashes every
  non-manifest artifact, validates the shared schema, scans for secrets, writes
  `manifest.json` atomically, and returns `PASS` or `INVALID`.
- The post-finalization checker is always
  `scripts/research/check_artifact.py`. Preserve Slice 1's public
  `check_bundle(bundle_dir)` call unchanged. Slice 2 uses Slice 1's optional
  keyword arguments for operator validation:

  ```python
  report = check_bundle(
      bundle_dir,
      expected_kind="operator",
      source_root=source_root,
  )
  report.require_valid()
  ```

  `expected_kind` and `source_root` default to `None`. With no keywords, the
  checker infers the kind from the manifest and retains all Slice 1 behavior.
  `source_root` is required only when the inferred or explicit kind is
  `operator`; report `OPERATOR_SOURCE_ROOT_REQUIRED` instead of raising a Python
  argument error when it is absent.

  Its CLI is:

  ```bash
  scripts/research/run-python scripts/research/check_artifact.py \
    --bundle /absolute/path/to/bundle \
    --require-kind operator \
    --source-root /absolute/path/to/checkout
  ```

- Invoke `deliberate-changes` before Task 1, `verify-backend` after Task 1,
  `verify-frontend` after Tasks 4 and 6, and `verify-infra` after Tasks 5, 7,
  and 8.
- No real screenshot, trace, video, or committed operator bundle is captured in
  Tasks 1-7. Task 8 first commits all source and harness changes, reruns the
  gates, then captures from a clean detached worktree at that commit. Any later
  change under `aggregator/`, `frontend/`, `e2e/`, `scripts/research/`,
  `schemas/`, `docker-compose.yml`, `.dockerignore`, or
  `docs/05-messaging.md` invalidates the bundle and requires a complete
  recapture. Slice 1's repository-wide source provenance also conservatively
  invalidates other tracked or relevant untracked source changes before the
  final checker. The sole post-check exception is Task 8 Step 13's two-row
  traceability update: it records the already checked evidence without changing
  the bundle or the recorded source checkout.

### Autonomous Worktree Chain

The canonical repository may contain user changes. Never reset, restore, stash,
stage, or commit there. Slice 1 already created the one persistent task
worktree and recorded its exact continuation point; Slice 2 must consume that
record rather than create another branch or worktree.

- [ ] Before Task 1, load and validate Slice 1's handoff:

  ```bash
  set -euo pipefail
  CANONICAL_ROOT=/Users/yefanzhang/workplace/edge-research
  CANONICAL_BASE="$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
  CHAIN_KEY="$(printf '%s' "$CANONICAL_BASE" | cut -c1-12)"
  CHAIN_ROOT="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY"
  HANDOFF="$CHAIN_ROOT/handoff.env"
  test -f "$HANDOFF"
  # shellcheck disable=SC1090
  source "$HANDOFF"
  test "$CANONICAL_ROOT" = /Users/yefanzhang/workplace/edge-research
  test "$CANONICAL_BASE" = "$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
  test "$TASK_ROOT" = "$CHAIN_ROOT/repo"
  test "$(git -C "$TASK_ROOT" rev-parse --show-toplevel)" = "$TASK_ROOT"
  test "$(git -C "$TASK_ROOT" branch --show-current)" = "$BRANCH"
  test "$(git -C "$TASK_ROOT" rev-parse HEAD)" = "$FINAL_COMMIT"
  test -z "$(git -C "$TASK_ROOT" status --porcelain)"
  SLICE2_ENTRY_COMMIT="$FINAL_COMMIT"
  printf '%s\n' "$SLICE2_ENTRY_COMMIT" \
    > "$CHAIN_ROOT/slice2-entry-commit"
  git -C "$CANONICAL_ROOT" write-tree \
    > "$CHAIN_ROOT/slice2-canonical-index-tree"
  shasum -a 256 \
    "$CANONICAL_ROOT/aggregator/main.py" \
    "$CANONICAL_ROOT/aggregator/tests/test_api.py" \
    > "$CHAIN_ROOT/slice2-canonical-shared-files.sha256"
  git -C "$CANONICAL_ROOT" \
    status --porcelain=v2 -z --untracked-files=all \
    | shasum -a 256 \
    > "$CHAIN_ROOT/slice2-canonical-status.sha256"
  cd "$TASK_ROOT"
  ```

  Every later literal
  `/Users/yefanzhang/workplace/edge-research` command in this plan means
  `"$TASK_ROOT"`. Keep the existing `paper-autonomous-*` branch and linked
  worktree through Slice 4. Do not cherry-pick, merge, push, publish, or ask the
  user to choose an integration target.

- [ ] Before every task and commit, require `git rev-parse --show-toplevel` to
  equal `TASK_ROOT`, require the task worktree and index to contain only the
  current task's deliberate changes, and compare
  `git diff --cached --name-only` to that task's exact file map. Stage with
  noninteractive `git add -- <exact paths>` or the task's explicit
  `git apply --cached`; never use `git add -A`, `git commit -a`, or an
  interactive patch command. Inspect `git diff --cached` and run
  `git diff --cached --check`. Unrelated paths are a hard stop.

- [ ] After every task commit, mechanically prove that the canonical checkout
  remains byte-for-byte unchanged in the shared files, index, and reported
  status:

  ```bash
  shasum -a 256 -c "$CHAIN_ROOT/slice2-canonical-shared-files.sha256"
  test "$(git -C "$CANONICAL_ROOT" write-tree)" = "$(
    cat "$CHAIN_ROOT/slice2-canonical-index-tree"
  )"
  git -C "$CANONICAL_ROOT" \
    status --porcelain=v2 -z --untracked-files=all \
    | shasum -a 256 \
    | cmp - "$CHAIN_ROOT/slice2-canonical-status.sha256"
  ```

  All implementation, verification, capture, and commits remain in
  `TASK_ROOT`; canonical-checkout access is read-only verification.

## R-02 And R-08 Ownership And Status Gates

Slice 2 also owns R-02. Its executable owners are
`aggregator/tests/test_database.py`, `aggregator/tests/test_api.py`,
`frontend/src/utils/taskReducer.test.js`, and
`frontend/src/components/TaskBoard.test.jsx`. R-02 remains **Proposed** until
Task 8 Step 8 passes the cumulative backend and frontend gates. Only Task 8
Step 13 may advance its row.

Slice 2 owns R-08, the isolated deterministic operator journey and evidence
bundle. Its executable owners are
`e2e/helpers/lifecycle.integration.spec.js`,
`e2e/tests/operator-journey.spec.js`,
`tests/research/test_operator_evidence.py`, and
`tests/research/test_checker.py`. Its accepted evidence paths are
`docs/research/results/operator/<bundle>/manifest.json`,
`raw/playwright/{desktop,mobile}/`, `raw/api/{desktop,mobile}/`, and
`raw/runtime/cleanup.json` beneath that bundle. R-08 remains **Proposed** until
Task 8 Step 12 passes the finalized-bundle checker against a clean worktree at
the manifest's recorded source commit. Only Task 8 Step 13 may advance the
requirement row; passing tests without that clean checked bundle are
insufficient.

## File Map

**Backend audit contract**

- Modify `aggregator/models.py`: exact UUIDv4 validation for supplied direct
  command contexts.
- Modify `aggregator/database.py`: additive replay counter, atomic replay update,
  and insertion-order API metadata.
- Modify `aggregator/tests/test_jetstream_bootstrap.py`: explicit opt-in,
  non-reconnecting JetStream integration fixture.
- Modify `aggregator/tests/test_database.py`: replay, migration, and observation
  order tests.
- Modify `aggregator/tests/test_api.py`: focused API metadata regression hunk.
- Modify `docs/05-messaging.md`: replay and observation-order invariants.

**Frontend test and product contract**

- Modify `frontend/package.json`: unit, lint, and runtime hash dependencies.
- Modify `frontend/package-lock.json`: locked dependency graph.
- Modify `frontend/vite.config.js`: jsdom Vitest setup.
- Create `frontend/eslint.config.js`: browser/Node globals and correctness rules.
- Create `frontend/tests/tooling-contract.test.cjs`: package/tooling contract.
- Create `frontend/src/test/setup.js`: Testing Library setup and cleanup.
- Create `frontend/src/App.test.jsx`: fleet stream, name, and theme contract.
- Modify `frontend/src/App.jsx`: permanent fleet WebSocket.
- Modify `frontend/src/components/HeaderBar.jsx`: EdgeCitadel name and no theme
  switch.
- Modify `frontend/src/stores/appStore.js`: remove unsupported theme state and add
  convergent actions.
- Modify `frontend/index.html`: fixed dark class and EdgeCitadel title.

**Task and fleet reducers**

- Create `frontend/src/utils/taskReducer.js`: canonical hashing, legal
  observation-order state reduction, and conflict reporting.
- Create `frontend/src/utils/taskReducer.test.js`: full Section 2.3 matrix.
- Modify `frontend/src/components/TaskBoard.jsx`: consume the reducer and refresh
  on fleet events.
- Create `frontend/src/components/TaskBoard.test.jsx`: stale-request and
  reducer-error retention tests.
- Create `frontend/src/hooks/realtimeEvents.js`: pure WebSocket frame reducer.
- Create `frontend/src/hooks/realtimeEvents.test.js`: exact backend frame tests.
- Modify `frontend/src/hooks/useWebSocket.js`: connection-only hook using the
  pure reducer.
- Modify `frontend/src/components/StatusBadge.jsx`: accessible status name.
- Create `frontend/src/components/StatusBadge.test.jsx`: accessible badge tests.
- Modify `frontend/src/components/RegistryRow.jsx`: pass `status`.
- Create `frontend/src/components/RegistryRow.test.jsx`: row status regression.

**Owned E2E runtime**

- Modify `aggregator/main.py`: complete direct-command correlation.
- Modify `aggregator/tests/test_api.py`: assert published command
  correlation fields.
- Modify `aggregator/models.py`: reject a supplied context unless it is UUIDv4.
- Modify `aggregator/tests/test_jetstream_bootstrap.py`: skip unless an explicit
  isolated NATS test URL is supplied and disable reconnects.
- Modify `scripts/research/fixtures/native_control.py`: optional per-task
  terminal-release handshake.
- Modify `tests/research/test_native_control.py`: release-gate and path-safety
  coverage.
- Create `e2e/run-isolated.js`: CLI lifecycle owner.
- Create `e2e/helpers/stack-config.js`: validated IDs, paths, config, and dynamic
  port parsing.
- Create `e2e/helpers/stack-config.spec.js`: pure configuration tests.
- Create `e2e/helpers/owned-stack.js`: Compose process, readiness, image
  tracking, cleanup, and signal APIs.
- Create `e2e/helpers/owned-stack.spec.js`: fake-runner lifecycle tests.
- Create `e2e/helpers/lifecycle.integration.spec.js`: concurrent and signal
  lifecycle tests.
- Create `e2e/helpers/clean-checkout.js`: tracked-file-only lifecycle smoke.
- Modify `e2e/docker-compose.test.yml`: dynamic loopback ports, deterministic
  fixture, and run-owned volumes.
- Modify `e2e/playwright.config.js`: strict environment and deterministic base.
- Delete `e2e/playwright.smoke.config.js`: obsolete fallback config.
- Delete `e2e/global-setup.js`: launcher owns startup.
- Delete `e2e/global-teardown.js`: launcher owns cleanup.
- Delete `e2e/test-storage-state.json`: unauthenticated deterministic run has no
  persisted browser state.
- Modify `e2e/package.json`: isolated, lifecycle, evidence, and live scripts.
- Modify `e2e/package-lock.json`: locked script dependency changes.
- Modify `e2e/helpers/api-client.js`: environment-strict `/api` client.
- Modify `e2e/helpers/ws-client.js`: environment-strict WebSocket client.
- Modify `e2e/helpers/fixtures.js`: use only strict helper clients.
- Modify `e2e/tests/phase1-smoke.spec.js`: nonce-only deterministic round trip.
- Modify `e2e/tests/phase3-registry-tab.spec.js`: strict `shell-1` assertions.
- Modify `.gitignore`: transient E2E run and report directories only.

**Operator journey and gate classification**

- Create `e2e/helpers/operator-journey.js`: environment, polling, canonical JSON,
  and overlap helpers.
- Create `e2e/tests/operator-journey.spec.js`: one complete lifecycle test.
- Modify `frontend/src/Layout.jsx`: selected-tab semantics.
- Modify `frontend/src/components/AgentCard.jsx`: selected-agent semantics.
- Modify `frontend/src/components/CommandInput.jsx`: accessible target/input/send
  controls.
- Modify `frontend/src/components/MessageBubble.jsx`: message type identity.
- Modify `frontend/src/components/TaskCard.jsx`: task ID and state identity.
- Modify `frontend/src/components/TaskBoard.jsx`: event-triggered refresh.
- Create `e2e/playwright.live.config.js`: external live-only gate.
- Create `e2e/helpers/gate-classification.spec.js`: exact config membership.
- Modify `e2e/tests/dark-mode.spec.js`: fixed dark-theme legibility contract.
- Modify `e2e/tests/keyboard-shortcuts.spec.js`: strict shortcut assertions.
- Modify `e2e/tests/phase2-gemma-smoke.spec.js`: live-only strict URLs.
- Modify `e2e/tests/phase2.5-streaming-and-memory.spec.js`: live-only strict URLs.
- Modify `e2e/tests/phase3-watchdog-fast-path.spec.js`: live-only strict URLs.
- Modify `e2e/tests/phase6-hermes-bridge.spec.js`: live-only strict URLs.
- Modify `e2e/tests/streaming-fragmentation-regression.spec.js`: explicit live
  classification.

**Final evidence**

- Create `e2e/playwright.evidence.config.js`: two-project same-stack evidence.
- Create `e2e/helpers/evidence-artifacts.js`: paired screenshots, API snapshots,
  and project metadata.
- Create `scripts/research/capture_operator_journey.py`: final capture wrapper.
- Create `tests/research/test_operator_evidence.py`: wrapper and corruption tests.
- Modify `scripts/research/check_artifact.py`: operator project validation.
- Modify `tests/research/test_checker.py`: operator checker coverage.
- Modify `schemas/research-manifest.v1.json`: conditional operator-project
  manifest fields.
- Modify `tests/research/test_evidence.py`: shared schema operator branch.
- Modify `docs/research/results/README.md`: operator bundle layout and checker
  command.
- Modify `docs/research/task-aware-reliability-contract-design.md`: R-02 and
  R-08 status rows only, after their final gates.
- Modify `e2e/run-isolated.js`: sanitized runtime evidence copy and scratch
  deletion.
- Modify `e2e/tests/operator-journey.spec.js`: project-local evidence capture.

### Task 1: Make Audit Replay Suppression Observable

**Files:**
- Modify: `aggregator/database.py`
- Modify: `aggregator/tests/test_database.py`
- Modify: `aggregator/tests/test_api.py`
- Modify: `docs/05-messaging.md`

- [ ] **Step 1: Write three failing database tests**

  Use an inline envelope, not an undefined factory:

  ```python
  import sqlite3

  def result_envelope(envelope_id: str = "wire-1") -> dict:
      return {
          "v": 1,
          "id": envelope_id,
          "type": "result",
          "sender_id": "shell-1",
          "recipient_id": "aggregator",
          "task_id": "task-1",
          "context_id": "context-1",
          "task_state": "completed",
          "hop_count": 0,
          "timestamp": "2026-07-25T12:00:01.000Z",
          "payload": {"body": "edgecitadel:nonce-1"},
      }

  def test_insert_message_counts_replayed_envelope_once(fresh_db):
      env = result_envelope()
      db.insert_message(env)
      db.insert_message(env)
      db.insert_message(env)
      rows = db.query_messages(task_id="task-1")
      assert len(rows) == 1
      assert rows[0]["duplicate_count"] == 2

  def test_init_db_migrates_duplicate_count_without_wipe(tmp_path):
      path = tmp_path / "legacy.db"
      with sqlite3.connect(path) as conn:
          conn.execute(
              """CREATE TABLE messages (
                 id TEXT PRIMARY KEY, v INTEGER NOT NULL DEFAULT 1,
                 type TEXT NOT NULL, sender_id TEXT NOT NULL,
                 recipient_id TEXT, task_id TEXT, context_id TEXT,
                 task_state TEXT, agent_state TEXT, hop_count INTEGER,
                 timestamp TEXT NOT NULL, payload TEXT NOT NULL,
                 deployment TEXT NOT NULL DEFAULT 'default'
              )"""
          )
          row = result_envelope("legacy-wire")
          conn.execute(
              """INSERT INTO messages
                 (id, v, type, sender_id, recipient_id, task_id, context_id,
                  task_state, agent_state, hop_count, timestamp, payload,
                  deployment)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
              (
                  row["id"], row["v"], row["type"], row["sender_id"],
                  row["recipient_id"], row["task_id"], row["context_id"],
                  row["task_state"], None, row["hop_count"], row["timestamp"],
                  '{"body":"edgecitadel:nonce-1"}', "default",
              ),
          )
      db.init_db(str(path), wipe=False)
      assert "duplicate_count" in db.table_columns("messages")
      rows = db.query_messages(task_id="task-1")
      assert len(rows) == 1
      assert rows[0]["duplicate_count"] == 0

  def test_query_messages_uses_sqlite_observation_order(fresh_db):
      late_insert = result_envelope("wire-late")
      late_insert["task_id"] = "task-late"
      late_insert["timestamp"] = "2000-01-01T00:00:00.000Z"
      early_insert = result_envelope("wire-early")
      early_insert["task_id"] = "task-early"
      early_insert["timestamp"] = "2099-01-01T00:00:00.000Z"
      db.insert_message(early_insert)
      db.insert_message(late_insert)
      rows = db.query_messages(limit=2)
      assert [row["id"] for row in rows] == ["wire-late", "wire-early"]
      assert rows[0]["observation_index"] > rows[1]["observation_index"]
  ```

- [ ] **Step 2: Write the failing API regression hunk**

  Append one isolated test to `aggregator/tests/test_api.py` and do not alter
  unrelated neighboring tests:

  ```python
  def test_messages_exposes_replay_and_observation_metadata(client):
      from aggregator import database as db

      env = {
          "v": 1,
          "id": "audit-wire-1",
          "type": "result",
          "sender_id": "shell-1",
          "recipient_id": "aggregator",
          "task_id": "audit-task-1",
          "task_state": "completed",
          "timestamp": "2026-07-25T12:00:01.000Z",
          "payload": {"body": "edgecitadel:audit"},
      }
      db.insert_message(env)
      db.insert_message(env)
      response = client.get("/api/messages?task_id=audit-task-1")
      assert response.status_code == 200
      rows = response.json()
      assert len(rows) == 1
      assert rows[0]["duplicate_count"] == 1
      assert isinstance(rows[0]["observation_index"], int)
      assert rows[0]["observation_index"] > 0
  ```

- [ ] **Step 3: Run the four focused tests and verify red**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider \
    aggregator/tests/test_database.py aggregator/tests/test_api.py \
    -k "replayed_envelope or migrates_duplicate or sqlite_observation or replay_and_observation" \
    -q
  ```

  Expected: four selected tests run and at least one fails because
  `duplicate_count` or `observation_index` is absent.

- [ ] **Step 4: Implement the additive migration and atomic replay update**

  Add `duplicate_count` to `SCHEMA_SQL`, migrate after `executescript`, and keep
  the insert/update in one immediate transaction:

  ```python
  # Fresh schema column:
  # duplicate_count INTEGER NOT NULL DEFAULT 0

  columns = {
      row[1]
      for row in c.execute("PRAGMA table_info(messages)").fetchall()
  }
  if "duplicate_count" not in columns:
      c.execute(
          "ALTER TABLE messages ADD COLUMN "
          "duplicate_count INTEGER NOT NULL DEFAULT 0"
      )
  ```

  Replace `insert_message` with:

  ```python
  def insert_message(env: dict, deployment: str = "default") -> None:
      with _conn() as c:
          c.execute("BEGIN IMMEDIATE")
          cursor = c.execute(
              """INSERT OR IGNORE INTO messages
                 (id, v, type, sender_id, recipient_id, task_id, context_id,
                  task_state, agent_state, hop_count, timestamp, payload,
                  deployment)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
              (
                  env["id"],
                  env.get("v", 1),
                  env["type"],
                  env["sender_id"],
                  env.get("recipient_id"),
                  env.get("task_id"),
                  env.get("context_id"),
                  env.get("task_state"),
                  env.get("agent_state"),
                  env.get("hop_count"),
                  env["timestamp"],
                  json.dumps(env.get("payload", {})),
                  deployment,
              ),
          )
          if cursor.rowcount == 0:
              c.execute(
                  """UPDATE messages
                     SET duplicate_count = duplicate_count + 1
                     WHERE id = ?""",
                  (env["id"],),
              )
  ```

  Do not alter the wire envelope or overwrite the first stored row.

- [ ] **Step 5: Expose the trusted observation sequence**

  Start the query with:

  ```python
  q = (
      "SELECT messages.rowid AS observation_index, messages.* "
      "FROM messages WHERE 1=1"
  )
  ```

  End it with:

  ```python
  q += " ORDER BY messages.rowid DESC LIMIT ?"
  params.append(limit)
  ```

  The API stays newest-first, but now newest means aggregator observation order,
  not an untrusted envelope timestamp.

- [ ] **Step 6: Document the invariant**

  In `docs/05-messaging.md`, state all four rules:

  ```text
  messages.id is the audit idempotency key.
  A replay increments duplicate_count and does not add a visible row.
  duplicate_count is mirror replay metadata, not broker delivery count.
  observation_index is SQLite insertion order and is the only dashboard
  task-state ordering input; envelope timestamp remains display metadata.
  ```

- [ ] **Step 7: Verify green and run the backend gate**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider \
    aggregator/tests/test_database.py aggregator/tests/test_api.py \
    -k "replayed_envelope or migrates_duplicate or sqlite_observation or replay_and_observation" \
    -q
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests \
    -k "not ensure_stream_idempotent and not ensure_consumer_serialization and not stream_config_matches_spec" \
    -q
  scripts/research/run-python -m compileall -q aggregator
  ```

  Until Task 5 removes the JetStream test module's implicit
  `nats://localhost:4222` default, every Tasks 1-4 aggregate backend command
  must use this exclusion. It must never probe or delete a developer stream.

  Expected: exactly four focused tests pass; the complete aggregator suite has
  zero failures; compilation is silent. Invoke `verify-backend`.

- [ ] **Step 8: Stage safely and commit**

  Stage exactly the four Task 1 paths noninteractively, run `commit-check`,
  verify the cached map, and commit:

  ```bash
  git add -- \
    aggregator/database.py \
    aggregator/tests/test_database.py \
    aggregator/tests/test_api.py \
    docs/05-messaging.md
  git diff --cached --name-only
  git diff --cached --check
  git commit -m "fix(aggregator): expose audit replay counts"
  ```

### Task 2: Add Frontend Tests, Lint, And The Product Contract

**Files:**
- Modify: `frontend/package.json`
- Modify: `frontend/package-lock.json`
- Modify: `frontend/vite.config.js`
- Create: `frontend/eslint.config.js`
- Create: `frontend/tests/tooling-contract.test.cjs`
- Create: `frontend/src/test/setup.js`
- Create: `frontend/src/App.test.jsx`
- Modify: `frontend/src/App.jsx`
- Modify: `frontend/src/components/HeaderBar.jsx`
- Modify: `frontend/src/stores/appStore.js`
- Modify: `frontend/index.html`

- [ ] **Step 1: Write the failing tooling contract**

  Create `frontend/tests/tooling-contract.test.cjs`:

  ```javascript
  const assert = require('node:assert/strict')
  const fs = require('node:fs')
  const path = require('node:path')
  const test = require('node:test')

  const root = path.resolve(__dirname, '..')

  test('frontend exposes locked unit and lint gates', () => {
    const pkg = JSON.parse(
      fs.readFileSync(path.join(root, 'package.json'), 'utf8'),
    )
    assert.equal(pkg.scripts.test, 'vitest run')
    assert.equal(pkg.scripts.lint, 'eslint . --max-warnings=0')
    assert.equal(fs.existsSync(path.join(root, 'eslint.config.js')), true)
    assert.equal(fs.existsSync(path.join(root, 'src/test/setup.js')), true)
    assert.equal(typeof pkg.dependencies['@noble/hashes'], 'string')
    assert.equal(typeof pkg.devDependencies.eslint, 'string')
  })
  ```

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  node --test tests/tooling-contract.test.cjs
  ```

  Expected: one test fails because the scripts and files do not exist.

- [ ] **Step 2: Install and configure the exact toolchain**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm install @noble/hashes@1.7.1
  npm install --save-dev \
    vitest@2.1.8 jsdom@25.0.1 \
    @testing-library/react@16.1.0 \
    @testing-library/jest-dom@6.6.3 \
    eslint@9.17.0 globals@15.14.0 eslint-plugin-react-hooks@5.1.0
  ```

  Set these exact scripts:

  ```json
  {
    "test": "vitest run",
    "test:watch": "vitest",
    "lint": "eslint . --max-warnings=0"
  }
  ```

  Add this test block to `vite.config.js`:

  ```javascript
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.js',
    clearMocks: true,
    restoreMocks: true,
  },
  ```

  Create `frontend/src/test/setup.js`:

  ```javascript
  import '@testing-library/jest-dom/vitest'
  import { afterEach } from 'vitest'
  import { cleanup } from '@testing-library/react'

  afterEach(() => {
    cleanup()
    localStorage.clear()
  })
  ```

  Create `frontend/eslint.config.js`:

  ```javascript
  import globals from 'globals'
  import reactHooks from 'eslint-plugin-react-hooks'

  const runtimeGlobals = Object.assign({}, globals.browser, globals.node)
  const hookRules = Object.assign(
    {},
    reactHooks.configs.recommended.rules,
    {
      'no-undef': 'error',
      'no-dupe-keys': 'error',
      'no-unreachable': 'error',
    },
  )

  export default [
    {
      ignores: ['dist', 'coverage', 'node_modules'],
    },
    {
      files: ['src/**/*.{js,jsx}', 'vite.config.js'],
      languageOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
        parserOptions: {
          ecmaFeatures: { jsx: true },
        },
        globals: runtimeGlobals,
      },
      plugins: {
        'react-hooks': reactHooks,
      },
      rules: hookRules,
    },
  ]
  ```

  Run the tooling contract again. Expected: exactly one test passes.

- [ ] **Step 3: Write three failing product tests**

  In `frontend/src/App.test.jsx`, mock `Layout`, `Toast`, the API client, and
  `useWebSocket`. Reset Zustand before each test. Require:

  ```jsx
  import { beforeEach, describe, expect, it, vi } from 'vitest'
  import { render, screen } from '@testing-library/react'
  import App from './App'
  import HeaderBar from './components/HeaderBar'
  import useWebSocket from './hooks/useWebSocket'
  import useAppStore from './stores/appStore'

  vi.mock('./hooks/useWebSocket', () => ({
    default: vi.fn(),
  }))
  vi.mock('./Layout', () => ({
    default: () => <main>layout</main>,
  }))
  vi.mock('./components/Toast', () => ({
    default: () => null,
  }))
  vi.mock('./api/client', () => ({
    api: {
      systemStatus: vi.fn().mockResolvedValue({
        nats_connected: true,
        jetstream_stream_ok: true,
      }),
    },
  }))

  beforeEach(() => {
    useAppStore.setState({
      selectedAgent: null,
      agents: [],
      notifications: [],
      systemStatus: null,
      wsConnected: true,
      showTestAgents: false,
      sidebarOpen: false,
    })
  })

  it('keeps one fleet stream after agent selection', () => {
    useAppStore.setState({ selectedAgent: 'shell-1', notifications: [] })
    render(<App />)
    expect(useWebSocket).toHaveBeenCalledTimes(1)
    expect(useWebSocket).toHaveBeenCalledWith()
  })

  it('uses the EdgeCitadel product name', () => {
    render(<HeaderBar />)
    expect(
      screen.getByRole('heading', { name: 'EdgeCitadel' }),
    ).toBeInTheDocument()
  })

  it('does not claim an unsupported light theme', () => {
    render(<HeaderBar />)
    expect(screen.getAllByRole('button')).toHaveLength(2)
    expect(
      screen.getByRole('button', { name: 'Open agent list' }),
    ).toBeInTheDocument()
    expect(
      screen.getByRole('button', { name: 'Show test data' }),
    ).toBeInTheDocument()
  })
  ```

  Run:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm test -- src/App.test.jsx
  ```

  Expected: three tests run; the stream argument and product-name assertions
  fail, and the theme test fails because the current HeaderBar has three
  buttons, including the unnamed Sun/Moon control. This count makes the test
  red even though the current theme control has no accessible name.

- [ ] **Step 4: Implement the minimal product changes**

  Make `App` call the hook without a selected-agent argument:

  ```javascript
  useWebSocket()
  ```

  Remove `darkMode`, `setDarkMode`, `toggleDarkMode`, and the Sun/Moon button.
  Keep `<html class="dark">` in `frontend/index.html`, change its title to
  `EdgeCitadel`, and render this heading:

  ```jsx
  <h1 className="text-sm font-semibold text-gray-100 truncate">
    EdgeCitadel
  </h1>
  ```

  Add `aria-label="Open agent list"` to the mobile menu button and
  `aria-label={showTestAgents ? 'Hide test data' : 'Show test data'}` to the
  test-data button. Do not add another theme state or local-storage key.

- [ ] **Step 5: Run unit, lint, and build gates**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  node --test tests/tooling-contract.test.cjs
  npm test -- src/App.test.jsx
  npm run lint
  npm run build
  ```

  Expected: one tooling test and three product tests pass; ESLint reports zero
  warnings/errors; Vite exits zero.

- [ ] **Step 6: Commit**

  Run `commit-check`, stage only the eleven Task 2 files, verify the cached map,
  and commit:

  ```bash
  git commit -m "test(frontend): add deterministic quality gates"
  ```

### Task 3: Implement The Section 2.3 Task Reducer

**Files:**
- Create: `frontend/src/utils/taskReducer.js`
- Create: `frontend/src/utils/taskReducer.test.js`
- Modify: `frontend/src/components/TaskBoard.jsx`
- Create: `frontend/src/components/TaskBoard.test.jsx`

- [ ] **Step 1: Write the failing legal-transition matrix**

  Use these exact data sets in `taskReducer.test.js`:

  ```javascript
  const TERMINALS = ['completed', 'failed', 'canceled', 'rejected']

  const LEGAL_TRANSITIONS = [
    ['none', 'submitted'],
    ['submitted', 'working'],
    ['submitted', 'input-required'],
    ['submitted', 'auth-required'],
    ['submitted', 'completed'],
    ['submitted', 'failed'],
    ['submitted', 'canceled'],
    ['submitted', 'rejected'],
    ['working', 'working'],
    ['working', 'input-required'],
    ['working', 'auth-required'],
    ['working', 'completed'],
    ['working', 'failed'],
    ['working', 'canceled'],
    ['working', 'rejected'],
    ['input-required', 'working'],
    ['input-required', 'completed'],
    ['input-required', 'failed'],
    ['input-required', 'canceled'],
    ['input-required', 'rejected'],
    ['auth-required', 'working'],
    ['auth-required', 'completed'],
    ['auth-required', 'failed'],
    ['auth-required', 'canceled'],
    ['auth-required', 'rejected'],
  ]

  const INVALID_TRANSITIONS = [
    ['none', 'working'],
    ['submitted', 'submitted'],
    ['working', 'submitted'],
    ['input-required', 'input-required'],
    ['input-required', 'auth-required'],
    ['auth-required', 'auth-required'],
    ['auth-required', 'input-required'],
  ]
  ```

  A test event factory must provide a positive unique `observation_index`, stable
  sender/recipient/task IDs, a request fingerprint from a command row, and a
  JSON payload. Parameterize all 25 legal transitions and all seven invalid
  transitions.

- [ ] **Step 2: Add terminal, ordering, and malformed-input tests**

  Add exactly these additional cases:

  1. Four terminal states each accept an identical logical terminal replay with
     a new wire envelope ID and increment `terminal_replay_count`.
  2. Four terminal states each dominate a later `working` event and record
     `invalid_transition`.
  3. A later terminal with a different state records
     `conflicting_terminal`.
  4. A later terminal with the same state and a changed payload records
     `conflicting_terminal_payload_hash`.
  5. A later terminal with changed sender/recipient identity records
     `conflicting_terminal_identity`.
  6. Oldest-first and newest-first arrays derive the same result.
  7. Reversed envelope timestamps do not change the result.
  8. A row without `task_state` leaves state unchanged.
  9. A `command` row without `task_state` produces `submitted`.
  10. Missing, non-integer, or duplicate `observation_index` values throw
      `TaskObservationError`.
  11. A legacy command missing `context_id` and `hop_count` remains isolated to
      its own task, receives one `legacy_correlation_missing` violation, and
      produces the exact Slice 1-compatible fingerprint
      `8183b9af0b66433c284834a046bc57d9092be346e9ac58c082b439018cd6bafa`
      for `{type:"command", sender_id:"aggregator",
      recipient_id:"shell-1", task_id:"task-legacy",
      payload:{body:"legacy"}}`, without preventing a separate fully correlated
      task from reducing.

  Expected final count: 49 reducer Vitest cases.

- [ ] **Step 3: Run the reducer tests and verify red**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm test -- src/utils/taskReducer.test.js
  ```

  Expected: collection fails because `taskReducer.js` is absent.

- [ ] **Step 4: Implement the exact public reducer surface**

  Export:

  ```javascript
  import { sha256 } from '@noble/hashes/sha256'
  import { bytesToHex } from '@noble/hashes/utils'

  export const TERMINAL_STATES = new Set([
    'completed',
    'failed',
    'canceled',
    'rejected',
  ])

  export class TaskObservationError extends Error {
    constructor(message) {
      super(message)
      this.name = 'TaskObservationError'
    }
  }

  export function canonicalJson(value) {
    if (value === null || typeof value === 'boolean' ||
        typeof value === 'string') {
      return JSON.stringify(value)
    }
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) {
        throw new TaskObservationError('canonical JSON rejects non-finite number')
      }
      return JSON.stringify(value)
    }
    if (Array.isArray(value)) {
      return `[${value.map(canonicalJson).join(',')}]`
    }
    if (typeof value === 'object') {
      const keys = Object.keys(value).sort()
      const fields = keys.map(
        (key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`,
      )
      return `{${fields.join(',')}}`
    }
    throw new TaskObservationError('canonical JSON rejects unsupported value')
  }

  export function sha256Canonical(value) {
    const bytes = new TextEncoder().encode(canonicalJson(value))
    return bytesToHex(sha256(bytes))
  }

  export function requestFingerprint(command) {
    const contextId = command.context_id ?? command.task_id
    const hopCount = Number.isInteger(command.hop_count)
      ? command.hop_count
      : 0
    return sha256Canonical({
      type: command.type,
      sender_id: command.sender_id,
      recipient_id: command.recipient_id,
      task_id: command.task_id,
      context_id: contextId,
      hop_count: hopCount,
      payload: command.payload,
    })
  }

  export function terminalIdentity(task, event) {
    return {
      sender_id: event.sender_id,
      recipient_id: event.recipient_id,
      task_id: event.task_id,
      request_fingerprint: task.request_fingerprint,
      terminal_state: event.task_state,
      canonical_terminal_payload_hash: sha256Canonical(event.payload),
    }
  }

  const LEGAL_NEXT = {
    none: new Set(['submitted']),
    submitted: new Set([
      'working', 'input-required', 'auth-required',
      'completed', 'failed', 'canceled', 'rejected',
    ]),
    working: new Set([
      'working', 'input-required', 'auth-required',
      'completed', 'failed', 'canceled', 'rejected',
    ]),
    'input-required': new Set([
      'working', 'completed', 'failed', 'canceled', 'rejected',
    ]),
    'auth-required': new Set([
      'working', 'completed', 'failed', 'canceled', 'rejected',
    ]),
  }

  function recordViolation(task, kind, event) {
    task.contract_violations.push({
      kind,
      envelope_id: event.id,
      observation_index: event.observation_index,
      from_state: task.task_state,
      to_state: event.task_state,
    })
  }

  function terminalConflict(first, candidate) {
    if (first.terminal_state !== candidate.terminal_state) {
      return 'conflicting_terminal'
    }
    if (
      first.canonical_terminal_payload_hash !==
      candidate.canonical_terminal_payload_hash
    ) {
      return 'conflicting_terminal_payload_hash'
    }
    const identityKeys = [
      'sender_id',
      'recipient_id',
      'task_id',
      'request_fingerprint',
    ]
    const changed = identityKeys.some(
      (key) => first[key] !== candidate[key],
    )
    return changed ? 'conflicting_terminal_identity' : null
  }

  export function reduceObservedState(current, event) {
    const task = Object.assign({}, current, {
      contract_violations: current.contract_violations.slice(),
    })
    task.last_observation_index = event.observation_index
    task.last_ts = event.timestamp
    task.last_payload = event.payload

    const incoming = event.task_state
    if (!incoming) return task

    if (TERMINAL_STATES.has(task.task_state)) {
      if (!TERMINAL_STATES.has(incoming)) {
        recordViolation(task, 'invalid_transition', event)
        return task
      }
      const candidate = terminalIdentity(task, event)
      const conflict = terminalConflict(task.terminal_identity, candidate)
      if (conflict) {
        recordViolation(task, conflict, event)
      } else {
        task.terminal_replay_count += 1
      }
      return task
    }

    const legal = LEGAL_NEXT[task.task_state]
    if (!legal || !legal.has(incoming)) {
      recordViolation(task, 'invalid_transition', event)
      return task
    }

    task.task_state = incoming
    if (TERMINAL_STATES.has(incoming)) {
      task.terminal_identity = terminalIdentity(task, event)
      task.result = event.payload
    }
    return task
  }

  function initialTask(event) {
    return {
      task_id: event.task_id,
      context_id: event.context_id ?? null,
      sender_id: event.sender_id,
      recipient_id: event.recipient_id ?? null,
      task_state: 'none',
      request_fingerprint: null,
      terminal_identity: null,
      terminal_replay_count: 0,
      contract_violations: [],
      first_observation_index: event.observation_index,
      last_observation_index: event.observation_index,
      first_ts: event.timestamp,
      last_ts: event.timestamp,
      body: null,
      result: null,
      last_payload: event.payload,
    }
  }

  export function deriveTasks(messages) {
    const seenIndices = new Set()
    for (const message of messages) {
      const index = message.observation_index
      if (!Number.isInteger(index) || index <= 0) {
        throw new TaskObservationError(
          'observation_index must be a positive integer',
        )
      }
      if (seenIndices.has(index)) {
        throw new TaskObservationError(
          `duplicate observation_index ${index}`,
        )
      }
      seenIndices.add(index)
    }

    const ordered = messages.slice().sort(
      (left, right) =>
        left.observation_index - right.observation_index,
    )
    const byTask = new Map()
    for (const message of ordered) {
      if (!message.task_id) continue
      if (!byTask.has(message.task_id)) {
        byTask.set(message.task_id, initialTask(message))
      }
      let task = byTask.get(message.task_id)
      if (message.type === 'command') {
        if (
          message.context_id == null ||
          !Number.isInteger(message.hop_count)
        ) {
          recordViolation(task, 'legacy_correlation_missing', message)
        }
        if (task.request_fingerprint === null) {
          task.request_fingerprint = requestFingerprint(message)
        }
        task.body = message.payload?.body ?? task.body
        task.sender_id = message.sender_id
        task.recipient_id = message.recipient_id ?? task.recipient_id
      }
      const observed = (
        message.type === 'command' && !message.task_state
          ? Object.assign({}, message, { task_state: 'submitted' })
          : message
      )
      task = reduceObservedState(task, observed)
      byTask.set(message.task_id, task)
    }
    return Array.from(byTask.values()).sort(
      (left, right) =>
        right.last_observation_index - left.last_observation_index,
    )
  }
  ```

- [ ] **Step 5: Implement every state and conflict rule**

  Use this transition table:

  ```javascript
  const LEGAL_NEXT = {
    none: new Set(['submitted']),
    submitted: new Set([
      'working', 'input-required', 'auth-required',
      'completed', 'failed', 'canceled', 'rejected',
    ]),
    working: new Set([
      'working', 'input-required', 'auth-required',
      'completed', 'failed', 'canceled', 'rejected',
    ]),
    'input-required': new Set([
      'working', 'completed', 'failed', 'canceled', 'rejected',
    ]),
    'auth-required': new Set([
      'working', 'completed', 'failed', 'canceled', 'rejected',
    ]),
  }
  ```

  `reduceObservedState` must:

  ```text
  Return unchanged state for an event without task_state.
  Preserve the first terminal for every later event.
  Accept a later terminal only when all six terminalIdentity fields match.
  Count an identical terminal as an idempotent logical replay.
  Classify a changed payload hash separately from state or endpoint identity.
  Record every illegal nonterminal transition without advancing state.
  Store envelope_id and observation_index on every violation.
  ```

  `deriveTasks` must:

  ```text
  Copy and sort input ascending by observation_index.
  Reject missing, non-positive, non-integer, or duplicate indices.
  Ignore rows without task_id.
  Initialize each task at state "none".
  Treat a command without task_state as a submitted observation.
  Compute request_fingerprint from the first command.
  Project absent direct legacy context_id to task_id and absent hop_count to zero
  before hashing, exactly as Slice 1's normalize_task_correlation does.
  Record legacy_correlation_missing only on that task and continue reducing other
  tasks; never pass JavaScript undefined into canonicalJson.
  Record body/result metadata without using timestamp for reduction.
  Return tasks descending by last_observation_index.
  ```

  The returned task shape is exact:

  ```javascript
  {
    task_id: 'task-1',
    context_id: 'context-1',
    sender_id: 'aggregator',
    recipient_id: 'shell-1',
    task_state: 'completed',
    request_fingerprint: '64-lowercase-hex-characters',
    terminal_identity: {
      sender_id: 'shell-1',
      recipient_id: 'aggregator',
      task_id: 'task-1',
      request_fingerprint: '64-lowercase-hex-characters',
      terminal_state: 'completed',
      canonical_terminal_payload_hash: '64-lowercase-hex-characters',
    },
    terminal_replay_count: 0,
    contract_violations: [],
    first_observation_index: 10,
    last_observation_index: 12,
    first_ts: '2026-07-25T12:00:00.000Z',
    last_ts: '2026-07-25T12:00:02.000Z',
    body: 'nonce-1',
    result: { body: 'edgecitadel:nonce-1' },
    last_payload: { body: 'edgecitadel:nonce-1' },
  }
  ```

- [ ] **Step 6: Replace TaskBoard's local derivation**

  Delete the component-local `deriveTasks`, import the shared function, and use
  `last_observation_index` for task-card ordering. Timestamps remain display
  fields only. Do not catch `TaskObservationError` silently; show the existing
  error notification path and retain the previous valid task list.

  First create `TaskBoard.test.jsx`. Mock `TaskCard` to expose its task ID/state
  and mock `api.queryMessages` with explicit deferred promises. Its two tests
  are:

  ```text
  Start request A, trigger Refresh to start request B, resolve B with completed,
  then resolve A with submitted; the rendered task remains completed.
  After one valid response, resolve a later request with duplicate observation
  indices; the prior task remains rendered and addNotification receives one
  error whose title is "Task observation rejected".
  ```

  Import `useRef` from React and implement a monotonically increasing generation
  guard:

  ```jsx
  const addNotification = useAppStore((state) => state.addNotification)
  const requestGeneration = useRef(0)

  const fetchTasks = useCallback(async () => {
    const generation = ++requestGeneration.current
    try {
      const messages = await api.queryMessages()
      const next = deriveTasks(messages)
      if (generation === requestGeneration.current) setTasks(next)
    } catch (error) {
      if (generation === requestGeneration.current) {
        addNotification({
          type: 'error',
          title: 'Task observation rejected',
          message: error.message,
        })
      }
    }
  }, [addNotification])
  ```

  All initial, manual, fleet-event, and five-second fallback refreshes call this
  guarded function. On unmount increment `requestGeneration.current` so no
  pending request can update state or notify.

- [ ] **Step 7: Verify exactly 49 reducer and two component cases**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm test -- src/utils/taskReducer.test.js src/components/TaskBoard.test.jsx
  npm run lint
  npm run build
  ```

  Expected: exactly 49 reducer cases and two TaskBoard cases pass, ESLint
  reports zero warnings/errors, and the production build exits zero.

- [ ] **Step 8: Commit**

  Run `commit-check`, stage only the four Task 3 files, and commit:

  ```bash
  git commit -m "fix(frontend): enforce observed task transitions"
  ```

### Task 4: Make Registry And Status Events Converge

**Files:**
- Create: `frontend/src/hooks/realtimeEvents.js`
- Create: `frontend/src/hooks/realtimeEvents.test.js`
- Modify: `frontend/src/hooks/useWebSocket.js`
- Modify: `frontend/src/stores/appStore.js`
- Modify: `frontend/src/components/StatusBadge.jsx`
- Create: `frontend/src/components/StatusBadge.test.jsx`
- Modify: `frontend/src/components/RegistryRow.jsx`
- Create: `frontend/src/components/RegistryRow.test.jsx`

- [ ] **Step 1: Write six failing pure event tests**

  Define an `actions` object with spies for:

  ```javascript
  const actionNames = [
    'addRealtimeMessage',
    'appendStreamDelta',
    'finalizeStream',
    'updateAgentStatus',
    'upsertAgent',
    'upsertRegistryRow',
    'removeAgent',
    'addNotification',
  ]
  ```

  Cover exact backend frames:

  ```javascript
  applyRealtimeEvent(
    {
      event: 'agent_status_change',
      data: { agent_id: 'shell-1', agent_state: 'offline' },
    },
    actions,
  )
  expect(actions.updateAgentStatus)
    .toHaveBeenCalledWith('shell-1', 'offline')
  expect(actions.addNotification)
    .toHaveBeenCalledWith(expect.objectContaining({ type: 'warning' }))
  ```

  The other five cases are registration `{agent_id, card}`, deletion, normal
  message, `task.progress`, and terminal result. Assert registration upserts both
  fleet and registry with `agent_state: "online"`; deletion removes both through
  `removeAgent`; progress does not add a raw chat bubble; terminal finalizes the
  stream and adds the canonical result.

- [ ] **Step 2: Write three failing accessible status tests**

  `StatusBadge.test.jsx` has two cases:

  ```jsx
  render(<StatusBadge status="online" />)
  expect(
    screen.getByRole('status', { name: 'Status: online' }),
  ).toBeInTheDocument()

  render(<StatusBadge />)
  expect(
    screen.getByRole('status', { name: 'Status: offline' }),
  ).toBeInTheDocument()
  ```

  `RegistryRow.test.jsx` has one case:

  ```jsx
  render(
    <table>
      <tbody>
        <RegistryRow
          row={{
            agent_id: 'shell-1',
            agent_state: 'online',
            card: {
              metadata: {
                'runtime.roles': ['worker'],
                'runtime.kind': 'native',
              },
            },
            queue: { pending: 0, ack_pending: 0 },
            poison_count: 0,
          }}
          onClick={() => undefined}
          showTestAgents={false}
        />
      </tbody>
    </table>,
  )
  expect(
    screen.getByRole('status', { name: 'Status: online' }),
  ).toBeInTheDocument()
  ```

- [ ] **Step 3: Run nine tests and verify red**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm test -- \
    src/hooks/realtimeEvents.test.js \
    src/components/StatusBadge.test.jsx \
    src/components/RegistryRow.test.jsx
  ```

  Expected: nine cases run; reducer import and accessible status assertions fail.

- [ ] **Step 4: Implement the pure event reducer**

  Export one function with this dispatch:

  ```javascript
  export function applyRealtimeEvent(frame, actions) {
    const data = frame && frame.data
    if (!frame || !data) return

    if (frame.event === 'message') {
      if (data.type === 'task.progress') {
        const delta = data.payload?.message ?? data.payload?.delta ?? ''
        actions.appendStreamDelta(
          data.task_id,
          data.sender_id,
          delta,
          data.payload?.skill_id,
        )
        return
      }
      if (data.type === 'result' && data.task_id) {
        actions.finalizeStream(data.task_id, data)
      }
      if (data.type !== 'heartbeat' && data.type !== 'register') {
        actions.addRealtimeMessage(data)
      }
      return
    }

    if (frame.event === 'agent_status_change') {
      actions.updateAgentStatus(data.agent_id, data.agent_state)
      if (data.agent_state === 'offline') {
        actions.addNotification({
          type: 'warning',
          title: 'Agent Offline',
          message: `${data.agent_id} went offline`,
        })
      }
      return
    }

    if (frame.event === 'agent_registered') {
      const fleetRow = {
        agent_id: data.agent_id,
        card: data.card,
        agent_state: 'online',
      }
      const registryRow = {
        agent_id: data.agent_id,
        card: data.card,
        agent_state: 'online',
        last_heartbeat: null,
        last_register: null,
        deployment: data.card?.metadata?.['runtime.deployment'] ?? null,
        queue: { pending: 0, ack_pending: 0 },
        poison_count: 0,
      }
      actions.upsertAgent(fleetRow)
      actions.upsertRegistryRow(registryRow)
      actions.addNotification({
        type: 'info',
        title: 'Agent Registered',
        message: `${data.agent_id} connected`,
      })
      return
    }

    if (frame.event === 'agent_deleted') {
      actions.removeAgent(data.agent_id)
    }
  }
  ```

- [ ] **Step 5: Implement convergent Zustand actions**

  Use one identity helper:

  ```javascript
  const upsertByAgentId = (rows, incoming) => {
    const index = rows.findIndex(
      (row) => row.agent_id === incoming.agent_id,
    )
    if (index < 0) return rows.concat([incoming])
    return rows.map((row, rowIndex) => {
      if (rowIndex !== index) return row
      const merged = Object.assign({}, row, incoming)
      if (Object.hasOwn(row, 'queue')) merged.queue = row.queue
      if (Object.hasOwn(row, 'poison_count')) {
        merged.poison_count = row.poison_count
      }
      return merged
    })
  }
  ```

  Add these actions inside the existing Zustand store:

  ```javascript
  upsertAgent: (incoming) => set((state) => ({
    agents: upsertByAgentId(state.agents, incoming),
  })),

  upsertRegistryRow: (incoming) => set((state) => ({
    registry: upsertByAgentId(state.registry, incoming),
  })),

  updateAgentStatus: (agentId, agentState) => set((state) => ({
    agents: state.agents.map((row) =>
      row.agent_id === agentId
        ? { ...row, agent_state: agentState }
        : row
    ),
    registry: state.registry.map((row) =>
      row.agent_id === agentId
        ? { ...row, agent_state: agentState }
        : row
    ),
  })),

  removeAgent: (agentId) => set((state) => ({
    agents: state.agents.filter((row) => row.agent_id !== agentId),
    registry: state.registry.filter((row) => row.agent_id !== agentId),
    selectedAgent:
      state.selectedAgent === agentId ? null : state.selectedAgent,
  })),
  ```

  Keep `setRegistry` as the authoritative whole-array refresh from
  `/api/registry`. Delete the old agents-only `updateAgentStatus` definition so
  there is exactly one action with that name.

- [ ] **Step 6: Make useWebSocket connection-only**

  Import `useMemo` and `applyRealtimeEvent`, remove the hook argument, and read
  the eight actions explicitly:

  ```javascript
  import { useCallback, useEffect, useMemo, useRef } from 'react'
  import { applyRealtimeEvent } from './realtimeEvents'

  export default function useWebSocket() {
    const setWsConnected = useAppStore((state) => state.setWsConnected)
    const addRealtimeMessage = useAppStore(
      (state) => state.addRealtimeMessage,
    )
    const appendStreamDelta = useAppStore(
      (state) => state.appendStreamDelta,
    )
    const finalizeStream = useAppStore((state) => state.finalizeStream)
    const updateAgentStatus = useAppStore(
      (state) => state.updateAgentStatus,
    )
    const upsertAgent = useAppStore((state) => state.upsertAgent)
    const upsertRegistryRow = useAppStore(
      (state) => state.upsertRegistryRow,
    )
    const removeAgent = useAppStore((state) => state.removeAgent)
    const addNotification = useAppStore(
      (state) => state.addNotification,
    )
    const actions = useMemo(() => ({
      addRealtimeMessage,
      appendStreamDelta,
      finalizeStream,
      updateAgentStatus,
      upsertAgent,
      upsertRegistryRow,
      removeAgent,
      addNotification,
    }), [
      addRealtimeMessage,
      appendStreamDelta,
      finalizeStream,
      updateAgentStatus,
      upsertAgent,
      upsertRegistryRow,
      removeAgent,
      addNotification,
    ])
  ```

  Inside `connect`, make the URL unconditional and replace the existing frame
  dispatch with:

  ```javascript
  const url = `${protocol}//${host}/ws/stream`

  ws.onmessage = (event) => {
    try {
      const frame = JSON.parse(event.data)
      applyRealtimeEvent(frame, actions)
      if (frame.event === 'log' && frame.data?.level === 'ERROR') {
        actions.addNotification({
          type: 'error',
          title: 'Error',
          message: frame.data.message,
        })
      }
    } catch {
      // Ignore non-JSON messages.
    }
  }
  ```

  Retain the existing connection, heartbeat, close, error, and reconnect
  control flow. Include `actions` and `setWsConnected` in `connect`'s dependency
  array. Remove the selected-agent argument, all agent-specific URL selection,
  the obsolete `data.status` fallback, and the old inline frame dispatcher.

- [ ] **Step 7: Make the badge accessible and fix RegistryRow**

  Implement:

  ```jsx
  function dotClass(status, size) {
    return clsx(
      'rounded-full inline-block',
      size === 'sm' ? 'w-2.5 h-2.5' : 'w-3.5 h-3.5',
      statusColors[status] || statusColors.offline,
      glowColors[status],
    )
  }

  export default function StatusBadge({ status = 'offline', size = 'sm', label }) {
    return (
      <span
        className="inline-flex items-center gap-1.5"
        role="status"
        aria-label={`Status: ${status}`}
      >
        <span aria-hidden="true" className={dotClass(status, size)} />
        {label && <span className="text-xs text-gray-400">{label}</span>}
      </span>
    )
  }
  ```

  Keep the current `statusColors` and `glowColors` maps. Change RegistryRow to:

  ```jsx
  <StatusBadge status={row.agent_state} />
  ```

- [ ] **Step 8: Verify nine focused tests and the frontend gate**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm test -- \
    src/hooks/realtimeEvents.test.js \
    src/components/StatusBadge.test.jsx \
    src/components/RegistryRow.test.jsx
  npm test
  npm run lint
  npm run build
  ```

  Expected: exactly nine focused cases pass; the complete Vitest run, ESLint,
  and build exit zero. Invoke `verify-frontend`.

- [ ] **Step 9: Commit**

  Run `commit-check`, stage only the eight Task 4 files, and commit:

  ```bash
  git commit -m "fix(frontend): converge realtime fleet state"
  ```

### Task 5: Replace Shared E2E State With An Owned Isolated Stack

**Files:**
- Modify: `aggregator/main.py`
- Modify: `aggregator/tests/test_api.py`
- Modify: `aggregator/models.py`
- Modify: `aggregator/tests/test_jetstream_bootstrap.py`
- Modify: `scripts/research/fixtures/native_control.py`
- Modify: `tests/research/test_native_control.py`
- Create: `e2e/run-isolated.js`
- Create: `e2e/helpers/stack-config.js`
- Create: `e2e/helpers/stack-config.spec.js`
- Create: `e2e/helpers/owned-stack.js`
- Create: `e2e/helpers/owned-stack.spec.js`
- Create: `e2e/helpers/lifecycle.integration.spec.js`
- Create: `e2e/helpers/clean-checkout.js`
- Modify: `e2e/docker-compose.test.yml`
- Modify: `e2e/playwright.config.js`
- Delete: `e2e/playwright.smoke.config.js`
- Delete: `e2e/global-setup.js`
- Delete: `e2e/global-teardown.js`
- Delete: `e2e/test-storage-state.json`
- Modify: `e2e/package.json`
- Modify: `e2e/package-lock.json`
- Modify: `e2e/helpers/api-client.js`
- Modify: `e2e/helpers/ws-client.js`
- Modify: `e2e/helpers/fixtures.js`
- Modify: `e2e/tests/phase1-smoke.spec.js`
- Modify: `e2e/tests/phase3-registry-tab.spec.js`
- Modify: `.gitignore`

- [ ] **Step 1: Complete direct-command correlation before stack work**

  Slice 1's correlation validator requires `task_id`, `context_id`,
  `hop_count`, and `payload` on command, progress, and result envelopes. First
  add this parameterized async test hunk to the committed Slice 1 version of
  `aggregator/tests/test_api.py`; do not alter unrelated liveness coverage:

  ```python
  import json
  from types import SimpleNamespace
  from unittest.mock import AsyncMock
  from uuid import UUID

  from aggregator.models import CommandRequest


  @pytest.mark.asyncio
  @pytest.mark.parametrize(
      ("requested_context", "expected_context"),
      [
          (None, None),
          (
              "6e088543-c9de-4459-a0fe-2191d20dfba1",
              "6e088543-c9de-4459-a0fe-2191d20dfba1",
          ),
      ],
  )
  async def test_direct_command_publish_has_complete_correlation(
      requested_context,
      expected_context,
  ):
      from aggregator.main import (
          _build_direct_command_envelope,
          _publish_direct_command,
      )

      router = SimpleNamespace(
          js=SimpleNamespace(publish=AsyncMock()),
          nc=SimpleNamespace(publish=AsyncMock()),
      )
      request = CommandRequest(
          body="operator-nonce",
          context_id=requested_context,
      )
      envelope = _build_direct_command_envelope(
          agent_id="shell-1",
          sender_id="aggregator",
          request=request,
      )
      await _publish_direct_command(router, envelope)

      assert UUID(envelope["id"]).version == 4
      assert UUID(envelope["task_id"]).version == 4
      assert UUID(envelope["context_id"]).version == 4
      if expected_context is None:
          assert envelope["context_id"] == envelope["task_id"]
      else:
          assert envelope["context_id"] == expected_context
      assert envelope["hop_count"] == 0
      assert envelope["payload"] == {"body": "operator-nonce"}

      inbox = router.js.publish.await_args
      assert inbox.args[0] == "agents.shell-1.inbox"
      assert inbox.kwargs["headers"] == {
          "Nats-Msg-Id": envelope["id"],
      }
      outbox = router.nc.publish.await_args
      assert outbox.args[0] == "agents.aggregator.outbox"
      assert json.loads(inbox.args[1]) == envelope
      assert json.loads(outbox.args[1]) == envelope


  @pytest.mark.parametrize(
      "bad_context",
      [
          "not-a-uuid",
          "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
          "6E088543-C9DE-4459-A0FE-2191D20DFBA1",
      ],
  )
  def test_direct_command_rejects_non_uuid4_context(client, bad_context):
      response = client.post(
          "/api/command/shell-1",
          json={"body": "operator-nonce", "context_id": bad_context},
      )
      assert response.status_code == 422
  ```

  Run red:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests/test_api.py \
    -k "direct_command_publish_has_complete_correlation or direct_command_rejects_non_uuid4_context" \
    -q
  ```

  Expected: the module collects all five parameter cases. The two publish cases
  fail at their deferred helper import because the helpers do not exist, while
  all three request-validation cases execute and fail because arbitrary context
  strings are accepted. In `aggregator/models.py`, preserve the string-valued
  model surface while requiring exact UUIDv4:

  ```python
  from uuid import UUID

  from pydantic import BaseModel, field_validator


  class CommandRequest(BaseModel):
      body: str
      args: dict | None = None
      skill_id: str | None = None
      context_id: str | None = None

      @field_validator("context_id")
      @classmethod
      def context_id_must_be_uuid4(cls, value: str | None) -> str | None:
          if value is None:
              return None
          parsed = UUID(value)
          if parsed.version != 4 or str(parsed) != value:
              raise ValueError("context_id must be canonical UUIDv4")
          return str(parsed)
  ```

  Add these helpers to `aggregator/main.py`:

  ```python
  def _build_direct_command_envelope(
      *,
      agent_id: str,
      sender_id: str,
      request: CommandRequest,
  ) -> dict:
      task_id = str(uuid.uuid4())
      context_id = request.context_id or task_id
      return {
          "v": 1,
          "id": str(uuid.uuid4()),
          "type": "command",
          "sender_id": sender_id,
          "recipient_id": agent_id,
          "task_id": task_id,
          "context_id": context_id,
          "hop_count": 0,
          "timestamp": now_iso(),
          "payload": {
              "body": request.body,
              **({"args": request.args} if request.args else {}),
              **(
                  {"skill_id": request.skill_id}
                  if request.skill_id
                  else {}
              ),
          },
      }


  async def _publish_direct_command(router, envelope: dict) -> None:
      encoded = json.dumps(envelope).encode()
      await router.js.publish(
          f"agents.{envelope['recipient_id']}.inbox",
          encoded,
          headers={"Nats-Msg-Id": envelope["id"]},
      )
      await router.nc.publish(
          f"agents.{envelope['sender_id']}.outbox",
          encoded,
      )
  ```

  In `post_command`, replace only the inline envelope construction/publish block:

  ```python
  env = _build_direct_command_envelope(
      agent_id=agent_id,
      sender_id=actual_sender,
      request=req,
  )
  if agg is not None:
      await _publish_direct_command(agg.router, env)
  return CommandResponse(
      task_id=env["task_id"],
      recipient_id=agent_id,
      accepted_at=env["timestamp"],
  )
  ```

  Run all five direct-command cases green. Then run the upstream Slice 1
  fixture correlation
  tests:

  ```bash
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider \
    tests/research/test_native_control.py::test_echo_result_preserves_correlation \
    tests/research/test_native_control.py::test_progress_frames_preserve_correlation \
    -q
  ```

  Those Slice 1 tests must prove every echo result and every progress/result
  envelope preserves the received `task_id`, `context_id`, and `hop_count`.
  If either test is absent or fails, stop and repair Slice 1; do not duplicate
  fixture logic in Slice 2.

  Audit result: the operator journey uses only HTTP `/api/command/{agent_id}`.
  `MessageRouter.on_openclaw_ingress` in `aggregator/aggregator.py` is outside
  this operator path and remains part of the Slice 1 correlation migration.
  Do not modify OpenClaw ingress in Slice 2.

  In the same task, make `aggregator/tests/test_jetstream_bootstrap.py`
  hermetic and explicitly opt-in. At module scope, skip unless
  `RUN_JETSTREAM_INTEGRATION=1`; never default `NATS_URL_TEST` to localhost and
  never use `NATS_TOKEN`. Reuse Slice 1's
  `tests.research.nats_server.NatsServer` with a generated token and
  `jetstream=True`; do not add a second container launcher. That helper reads the
  exact digest from `toolchain.json`, allocates loopback ports, owns storage, and
  removes its exact container, volume, and temporary config in `finally`.
  Connect with `allow_reconnect=False` and `max_reconnect_attempts=0`. The test
  must not accept an external server URL or delete a stream on a server it did
  not create. Use `pytestmark = pytest.mark.skipif(...)` before fixture
  construction, and add a subprocess contract assertion proving an unset opt-in
  reports all module tests skipped without invoking `NatsServer.start`.

  Run it once against its disposable container:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  env -u RUN_JETSTREAM_INTEGRATION -u NATS_URL_TEST -u NATS_TOKEN \
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests/test_jetstream_bootstrap.py -q
  RUN_JETSTREAM_INTEGRATION=1 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests/test_jetstream_bootstrap.py -q
  ```

  Expected: the first command reports four skipped tests without constructing a
  NATS helper; the second passes the opt-in contract plus three integration
  tests. The digest-owned container and volume are gone, and no process attempts
  `nats://localhost:4222`.

- [ ] **Step 2: Write failing pure stack tests**

  Define and test this API:

  ```javascript
  const {
    makeStackConfig,
    parsePublishedPort,
    validateOwnedPaths,
    validateRunId,
  } = require('./stack-config')

  const config = makeStackConfig({
    runId: 'run-a001',
    repoRoot: '/repo',
    scratchRoot: '/repo/tmp/e2e',
    natsImage:
      'nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927',
  })

  assert.equal(config.project, 'edgecitadel-e2e-run-a001')
  assert.equal(config.runDir, '/repo/tmp/e2e/run-a001')
  assert.equal(config.fixtureConfig.run_id, 'run-a001')
  assert.equal(config.fixtureConfig.mode, 'edgecitadel')
  assert.equal(config.fixtureConfig.behavior, 'echo')
  assert.equal(config.fixtureConfig.delay_ms, 1000)
  assert.equal(config.fixtureConfig.crash_point, null)
  assert.equal(config.fixtureConfig.heartbeat_interval_ms, 1000)
  assert.equal(config.controlDir, '/repo/tmp/e2e/run-a001/control')
  assert.equal(config.terminalReleaseDir, config.controlDir)
  assert.equal(
    config.natsImage,
    'nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927',
  )
  assert.equal(
    parsePublishedPort('127.0.0.1:49152\n'),
    49152,
  )
  assert.throws(() => validateRunId('../escape'))
  assert.throws(() => parsePublishedPort('0.0.0.0:49152\n'))
  assert.throws(() => validateOwnedPaths(
    config.runDir,
    [config.credentialFile, config.credentialFile],
  ))
  assert.throws(() => validateOwnedPaths(
    config.runDir,
    [config.credentialFile, '/tmp/outside-control'],
  ))
  ```

  Add cases for IPv6 loopback output, duplicate path rejection, exact mode-0600
  credential/config writes, a mode-0700 control directory, the complete fixture
  JSON, rejection of a control path outside `runDir`, rejection of any mutable
  NATS reference, and exact propagation of the Slice 1 digest. Expected count:
  eleven pure tests.

- [ ] **Step 3: Write failing lifecycle unit tests**

  `owned-stack.spec.js` uses an injected fake command runner and covers:

  ```text
  start uses docker compose with shell=false and the unique project.
  published ports are queried after Compose is up.
  cleanup runs down -v --remove-orphans --rmi local exactly once.
  a second cleanup call returns the first cleanup promise.
  project containers, networks, volumes, and owned build references are checked.
  a surviving external digest-pinned NATS reference does not invalidate cleanup.
  a surviving project-owned build reference makes cleanup invalid.
  a cleanup-verification exception containing the generated token yields an
  invalid redacted report and still removes the credential and run directory.
  a persistCleanup exception still overwrites/unlinks the credential and removes
  the run directory before the cleanup promise rejects.
  a writeRunFiles failure after the credential write overwrites/unlinks the
  credential and removes the partially created run directory.
  an injected launcher stack-factory failure after writeRunFiles returns but
  before OwnedStack construction overwrites/unlinks the credential and removes
  the complete run directory.
  SIGINT exits 130 only after cleanup.
  SIGTERM exits 143 only after cleanup.
  ```

  A project-owned build image record is:

  ```javascript
  {
    service: 'backend',
    reference: 'edgecitadel-e2e-run-a001-backend:latest',
    image_id: 'sha256:0123456789abcdef',
  }
  ```

  Record the exact Slice 1 NATS digest in all-image provenance but never classify
  it as an owned build reference. Verify project-owned references, not global
  image IDs, after teardown. Concurrent projects may share a cached image ID, but
  each project-owned reference must be gone. These are thirteen lifecycle
  tests; the stack-factory case calls `main(argv, { createStack })` and proves
  the outer `finally` owns pre-construction cleanup.

- [ ] **Step 4: Run 24 unit tests and verify red**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/e2e
  node --test \
    helpers/stack-config.spec.js \
    helpers/owned-stack.spec.js
  ```

  Expected: all 24 tests fail: the eleven pure cases cannot import
  `stack-config.js`, and the thirteen lifecycle cases cannot import their
  implementation modules.

- [ ] **Step 5: Make Compose use Docker-assigned loopback ports**

  Use no preselected host ports. Each published service uses an empty host port:

  ```yaml
  services:
    nats:
      image: ${NATS_IMAGE:?required}
      ports:
        - "127.0.0.1::4222"
        - "127.0.0.1::8222"

    backend:
      ports:
        - "127.0.0.1::8000"

    frontend:
      ports:
        - "127.0.0.1::80"
  ```

  Docker allocates and binds each port atomically. After `up`, the launcher runs:

  ```text
  docker compose -p PROJECT -f COMPOSE_FILE port frontend 80
  docker compose -p PROJECT -f COMPOSE_FILE port backend 8000
  docker compose -p PROJECT -f COMPOSE_FILE port nats 4222
  docker compose -p PROJECT -f COMPOSE_FILE port nats 8222
  ```

  Reject any result that is not loopback. Do not open and close a temporary
  socket to "reserve" a port. `NATS_IMAGE` comes only from
  `toolchain.json["nats_image"]`; the Compose file has no mutable fallback.

- [ ] **Step 6: Add the exact deterministic fixture service**

  Add:

  ```yaml
  fixture-agent:
    build:
      context: ..
      dockerfile: scripts/research/Dockerfile
    command:
      - python3
      - -m
      - scripts.research.fixtures.native_control
      - --config
      - /run/config/native-control.json
    environment:
      NATS_URL: nats://nats:4222
      EC_CREDENTIAL_FILE: /run/secrets/transport-token
      EC_TERMINAL_RELEASE_DIR: /run/control
    volumes:
      - ${E2E_FIXTURE_CONFIG:?required}:/run/config/native-control.json:ro
      - ${E2E_CREDENTIAL_FILE:?required}:/run/secrets/transport-token:ro
      - ${E2E_CONTROL_DIR:?required}:/run/control:ro
      - fixture-state:/run/state
    depends_on:
      backend:
        condition: service_healthy
  ```

  Require launcher-provided `NATS_TOKEN`; remove the `test-token` default.
  Retain project-owned `test-data` and `nats-test-data`, add `fixture-state`,
  and never mount the developer `data/` directory.

  Extend Slice 1's fixture without changing its config schema, public CLI, or
  default behavior. After it publishes `working` and before it sleeps or builds
  a terminal result, call:

  ```python
  async def _wait_for_terminal_release(
      task_id: str,
      command_body: str,
      sleep=asyncio.sleep,
  ) -> None:
      configured = os.environ.get("EC_TERMINAL_RELEASE_DIR")
      if not configured:
          return
      root = Path(configured).resolve(strict=True)
      if not root.is_dir():
          raise RuntimeError("terminal release root must be a directory")
      try:
          nonce = UUID(command_body)
      except ValueError:
          return
      if nonce.version != 4 or str(nonce) != command_body:
          return
      try:
          task = UUID(task_id)
      except ValueError as error:
          raise RuntimeError("terminal release task ID must be UUIDv4") from error
      if task.version != 4 or str(task) != task_id:
          raise RuntimeError("terminal release task ID must be UUIDv4")
      hold = root / f"{command_body}.hold"
      release = root / f"{task_id}.release"
      if hold.parent != root or release.parent != root:
          raise RuntimeError("terminal release path escaped its root")
      try:
          hold_entry = hold.lstat()
      except FileNotFoundError:
          return
      if (
          stat.S_ISLNK(hold_entry.st_mode)
          or not stat.S_ISREG(hold_entry.st_mode)
      ):
          raise RuntimeError("terminal hold must be a regular file")
      while True:
          try:
              entry = release.lstat()
              if (
                  stat.S_ISLNK(entry.st_mode)
                  or not stat.S_ISREG(entry.st_mode)
              ):
                  raise RuntimeError("terminal release must be a regular file")
              return
          except FileNotFoundError:
              await sleep(0.025)
  ```

  Import `UUID` plus the `stat` module and use
  `stat.S_ISLNK`/`stat.S_ISREG`. Call the helper with the exact command body. It
  returns immediately unless the body is canonical UUIDv4 and both the
  environment variable and a pre-command `<command-body>.hold` file exist; this
  isolates the gate from every non-operator smoke test sharing the stack. With a valid hold,
  it waits indefinitely for `<task-id>.release`. Add focused async tests proving
  the absent-variable path, missing-hold path, delayed release, and rejection of
  symlink/non-regular hold or release entries. Those tests inject `sleep` and use
  only a temporary directory.

- [ ] **Step 7: Implement the concrete lifecycle APIs**

  `stack-config.js` implements:

  ```javascript
  const fs = require('node:fs/promises')
  const path = require('node:path')

  function validateRunId(runId) {
    if (!/^[a-z0-9][a-z0-9-]{7,63}$/.test(runId)) {
      throw new Error(`invalid run ID: ${runId}`)
    }
    return runId
  }

  function validateOwnedPaths(runDir, ownedPaths) {
    const root = path.resolve(runDir)
    const resolved = ownedPaths.map((value) => path.resolve(value))
    if (new Set(resolved).size !== resolved.length) {
      throw new Error('owned run paths must be distinct')
    }
    if (resolved.some(
      (value) => !value.startsWith(`${root}${path.sep}`),
    )) {
      throw new Error('owned path escaped run directory')
    }
    return resolved
  }

  function makeStackConfig({ runId, repoRoot, scratchRoot, natsImage }) {
    validateRunId(runId)
    if (
      natsImage !==
      'nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927'
    ) {
      throw new Error('NATS image must match the Slice 1 toolchain digest')
    }
    const runDir = path.resolve(scratchRoot, runId)
    const scratch = path.resolve(scratchRoot)
    if (!runDir.startsWith(`${scratch}${path.sep}`)) {
      throw new Error('run directory escaped scratch root')
    }
    const credentialFile = path.join(runDir, 'transport-token')
    const fixtureConfigFile = path.join(runDir, 'native-control.json')
    const summaryFile = path.join(runDir, 'launcher-summary.json')
    const controlDir = path.join(runDir, 'control')
    const ownedPaths = [
      credentialFile,
      fixtureConfigFile,
      summaryFile,
      controlDir,
    ]
    validateOwnedPaths(runDir, ownedPaths)
    return {
      runId,
      project: `edgecitadel-e2e-${runId}`,
      natsImage,
      repoRoot: path.resolve(repoRoot),
      runDir,
      composeFile: path.join(repoRoot, 'e2e/docker-compose.test.yml'),
      credentialFile,
      fixtureConfigFile,
      summaryFile,
      controlDir,
      terminalReleaseDir: controlDir,
      fixtureConfig: {
        run_id: runId,
        agent_id: 'shell-1',
        mode: 'edgecitadel',
        behavior: 'echo',
        delay_ms: 1000,
        crash_point: null,
        heartbeat_interval_ms: 1000,
        outcome_db: '/run/state/outcomes.sqlite3',
        side_effect_db: '/run/state/side-effects.sqlite3',
      },
    }
  }

  function parsePublishedPort(output) {
    const text = output.trim()
    const match = text.match(
      /^(?:127\.0\.0\.1|\[::1\]):([1-9][0-9]{0,4})$/,
    )
    if (!match) throw new Error(`non-loopback published port: ${text}`)
    const port = Number(match[1])
    if (port > 65535) throw new Error(`invalid published port: ${port}`)
    return port
  }

  async function scrubRunFiles(config) {
    let firstError = null
    let handle
    try {
      handle = await fs.open(config.credentialFile, 'r+')
      const { size } = await handle.stat()
      if (size > 0) {
        const zeros = Buffer.alloc(size)
        let offset = 0
        while (offset < zeros.length) {
          const { bytesWritten } = await handle.write(
            zeros,
            offset,
            zeros.length - offset,
            offset,
          )
          if (bytesWritten === 0) {
            throw new Error('credential overwrite made no progress')
          }
          offset += bytesWritten
        }
        await handle.sync()
      }
    } catch (error) {
      if (error.code !== 'ENOENT') firstError = error
    } finally {
      if (handle) {
        try {
          await handle.close()
        } catch (error) {
          firstError ||= error
        }
      }
      try {
        await fs.rm(config.credentialFile, { force: true })
      } catch (error) {
        firstError ||= error
      }
      try {
        await fs.rm(config.runDir, { recursive: true, force: true })
      } catch (error) {
        firstError ||= error
      }
    }
    if (firstError) throw firstError
  }

  async function writeRunFiles(config, randomBytes) {
    const token = randomBytes(32).toString('hex')
    try {
      await fs.mkdir(path.dirname(config.runDir), {
        recursive: true,
        mode: 0o700,
      })
      await fs.mkdir(config.runDir, { recursive: false, mode: 0o700 })
      await fs.mkdir(config.controlDir, { recursive: false, mode: 0o700 })
      await fs.writeFile(config.credentialFile, `${token}\n`, { mode: 0o600 })
      await fs.writeFile(
        config.fixtureConfigFile,
        `${JSON.stringify(config.fixtureConfig, null, 2)}\n`,
        { mode: 0o600 },
      )
      return {
        token,
        composeEnvironment: {
          E2E_CREDENTIAL_FILE: config.credentialFile,
          E2E_FIXTURE_CONFIG: config.fixtureConfigFile,
          E2E_CONTROL_DIR: config.controlDir,
          NATS_IMAGE: config.natsImage,
          NATS_TOKEN: token,
        },
      }
    } catch (error) {
      let scrubError = null
      try {
        await scrubRunFiles(config)
      } catch (caught) {
        scrubError = caught
      }
      const setup = String(error.stack || error.message)
        .split(token).join('<generated-per-run-token>')
      const scrub = scrubError
        ? String(scrubError.stack || scrubError.message)
          .split(token).join('<generated-per-run-token>')
        : null
      throw new Error(
        scrub ? `${setup}; setup scrub failed: ${scrub}` : setup,
      )
    }
  }

  module.exports = {
    makeStackConfig,
    parsePublishedPort,
    scrubRunFiles,
    validateOwnedPaths,
    validateRunId,
    writeRunFiles,
  }
  ```

  The token return value remains in process memory only. Redact it to the fixed
  marker `<generated-per-run-token>` before returning child stdout/stderr,
  building an exception, or persisting a summary/report. Do not store a token
  hash. Unit tests inject the token into stdout, stderr, a spawn error, and a
  cleanup-verification error, then recursively scan every returned/persisted
  string and assert the raw token is absent.

  `owned-stack.js` defines `runCommand(command, args, options)` with
  `child_process.spawn`, captured stdout/stderr, `shell: false`, and an
  `allowFailure` option. It then implements this lifecycle:

  ```javascript
  const { spawn } = require('node:child_process')
  const fs = require('node:fs/promises')
  const path = require('node:path')
  const {
    parsePublishedPort,
    scrubRunFiles,
  } = require('./stack-config')

  const sleep = (milliseconds) =>
    new Promise((resolve) => setTimeout(resolve, milliseconds))
  const OWNED_BUILD_SERVICES = new Set([
    'backend',
    'frontend',
    'fixture-agent',
  ])
  const SECRET_MARKER = '<generated-per-run-token>'

  function redactSecrets(value, secrets = []) {
    return secrets.reduce(
      (text, secret) =>
        secret ? text.split(secret).join(SECRET_MARKER) : text,
      String(value),
    )
  }

  function normalizeRuntimeText(value, config) {
    const replacements = [
      [config.evidenceRuntimeDir, '$EVIDENCE_DIR/raw/runtime'],
      [config.fixtureConfigFile, '<fixture-config>'],
      [config.credentialFile, '<credential-file>'],
      [config.controlDir, '<control-dir>'],
      [config.repoRoot, '$SOURCE_ROOT'],
      [config.runDir, '<run-owned-path>'],
    ].filter(([source]) => source)
    replacements.sort((left, right) => right[0].length - left[0].length)
    return replacements.reduce(
      (text, [source, replacement]) =>
        text.split(source).join(replacement),
      String(value),
    )
  }

  function runCommand(command, args, options) {
    return new Promise((resolve, reject) => {
      const child = spawn(command, args, {
        cwd: options.cwd,
        env: options.env,
        shell: false,
        stdio: ['ignore', 'pipe', 'pipe'],
      })
      let stdout = ''
      let stderr = ''
      child.stdout.on('data', (chunk) => {
        stdout += chunk.toString()
      })
      child.stderr.on('data', (chunk) => {
        stderr += chunk.toString()
      })
      child.once('error', (error) => {
        reject(new Error(redactSecrets(
          error.message,
          options.redactions,
        )))
      })
      child.once('close', (code) => {
        const result = {
          code: code ?? 1,
          stdout: redactSecrets(stdout, options.redactions),
          stderr: redactSecrets(stderr, options.redactions),
        }
        if (result.code !== 0 && !options.allowFailure) {
          reject(new Error(
            `${command} exited ${result.code}: ${result.stderr.trim()}`,
          ))
        } else {
          resolve(result)
        }
      })
    })
  }

  class OwnedStack {
    constructor({ config, runCommand, fetchImpl, exit }) {
      this.config = config
      this.runCommand = runCommand
      this.fetch = fetchImpl
      this.exit = exit
      this.ports = null
      this.allImages = []
      this.ownedBuildImages = []
      this.runtimeSummary = null
      this.cleanupPromise = null
      this.startedAt = new Date().toISOString()
    }

    composeArgs(args) {
      return [
        'compose',
        '-p', this.config.project,
        '-f', this.config.composeFile,
      ].concat(args)
    }

    async docker(args, allowFailure = false) {
      return this.runCommand(
        'docker',
        this.composeArgs(args),
        {
          cwd: this.config.repoRoot,
          env: Object.assign(
            {},
            process.env,
            this.config.composeEnvironment,
          ),
          shell: false,
          allowFailure,
          redactions: this.config.secretValues,
        },
      )
    }

    async start() {
      await this.docker(['up', '--build', '-d', '--wait'])
      this.ports = await this.resolvePorts()
      this.allImages = await this.readProjectImages()
      this.ownedBuildImages = this.allImages.filter((image) => (
        OWNED_BUILD_SERVICES.has(image.service) &&
        image.reference.startsWith(
          `${this.config.project}-${image.service}:`,
        )
      ))
      const found = new Set(
        this.ownedBuildImages.map((image) => image.service),
      )
      for (const service of OWNED_BUILD_SERVICES) {
        if (!found.has(service)) {
          throw new Error(`missing owned build image for ${service}`)
        }
      }
      await this.waitReady()
      return this.ports
    }

    async resolvePort(service, containerPort) {
      const result = await this.docker([
        'port',
        service,
        String(containerPort),
      ])
      return parsePublishedPort(result.stdout)
    }

    async resolvePorts() {
      return {
        app: await this.resolvePort('frontend', 80),
        api: await this.resolvePort('backend', 8000),
        nats: await this.resolvePort('nats', 4222),
        monitor: await this.resolvePort('nats', 8222),
      }
    }

    urls() {
      if (!this.ports) throw new Error('stack ports are unresolved')
      return {
        APP_URL: `http://127.0.0.1:${this.ports.app}`,
        AGG_URL: `http://127.0.0.1:${this.ports.api}`,
        NATS_URL: `nats://127.0.0.1:${this.ports.nats}`,
        MONITOR_URL: `http://127.0.0.1:${this.ports.monitor}`,
        WS_BASE_URL: `ws://127.0.0.1:${this.ports.api}/ws`,
      }
    }

    async waitReady() {
      const urls = this.urls()
      const deadline = Date.now() + 180_000
      while (Date.now() < deadline) {
        try {
          const app = await this.fetch(urls.APP_URL)
          const health = await this.fetch(
            `${urls.AGG_URL}/api/system/status`,
          )
          const monitor = await this.fetch(`${urls.MONITOR_URL}/healthz`)
          const registryResponse = await this.fetch(
            `${urls.AGG_URL}/api/registry`,
          )
          const status = await health.json()
          const registry = await registryResponse.json()
          const rows = registry.filter(
            (row) => row.agent_id === 'shell-1',
          )
          const extensions =
            rows[0]?.card?.capabilities?.extensions ?? []
          const l1 = extensions.some(
            (entry) =>
              entry.uri ===
              'https://edgecitadel.local/ext/nats-binding/v1',
          )
          const ready =
            app.ok &&
            monitor.ok &&
            status.nats_connected === true &&
            status.jetstream_stream_ok === true &&
            rows.length === 1 &&
            rows[0].agent_state === 'online' &&
            rows[0].card.metadata['runtime.kind'] === 'native' &&
            rows[0].card.metadata['runtime.roles'].includes('worker') &&
            rows[0].card.metadata['runtime.conformance'] === 'L1' &&
            l1
          if (ready) return
        } catch (error) {
          if (Date.now() >= deadline) throw error
        }
        await sleep(250)
      }
      throw new Error('stack readiness timed out')
    }

    async readProjectImages() {
      const result = await this.docker(['images', '--format', 'json'])
      const parsed = JSON.parse(result.stdout)
      const rows = Array.isArray(parsed) ? parsed : [parsed]
      return rows.map((row) => ({
        service: row.Service,
        reference: (
          row.Service === 'nats'
            ? this.config.natsImage
            : `${row.Repository}:${row.Tag}`
        ),
        image_id: row.ID,
      }))
    }

    async runPlaywright(configPath, forwardedArgs) {
      const args = [
        'playwright',
        'test',
        '--config',
        path.resolve(configPath),
      ].concat(forwardedArgs)
      return this.runCommand(
        'npx',
        args,
        {
          cwd: path.join(this.config.repoRoot, 'e2e'),
          env: Object.assign(
            {},
            process.env,
            this.urls(),
            {
              E2E_RUN_ID: this.config.runId,
              E2E_TERMINAL_RELEASE_DIR:
                this.config.terminalReleaseDir,
            },
          ),
          shell: false,
          allowFailure: true,
          redactions: this.config.secretValues,
        },
      )
    }

    async collectRuntimeSummary(token) {
      const result = await this.docker(['config'])
      const sanitizedCompose = redactSecrets(result.stdout, [token])
      const summary = {
        run_id: this.config.runId,
        project: this.config.project,
        run_dir: this.config.runDir,
        started_at: this.startedAt,
        captured_at: new Date().toISOString(),
        urls: this.urls(),
        images: {
          all: this.allImages,
          owned_build_references: this.ownedBuildImages,
        },
        compose_config: sanitizedCompose,
      }
      this.runtimeSummary = summary
      await fs.writeFile(
        this.config.summaryFile,
        `${JSON.stringify(summary, null, 2)}\n`,
        { mode: 0o600 },
      )
      return summary
    }

    async verifyCleanup() {
      const label =
        `label=com.docker.compose.project=${this.config.project}`
      const checks = {}
      for (const pair of [
        ['containers', ['ps', '-aq', '--filter', label]],
        ['networks', ['network', 'ls', '-q', '--filter', label]],
        ['volumes', ['volume', 'ls', '-q', '--filter', label]],
      ]) {
        const result = await this.runCommand(
          'docker',
          pair[1],
          {
            shell: false,
            allowFailure: false,
            redactions: this.config.secretValues,
          },
        )
        checks[pair[0]] = result.stdout.trim()
          ? result.stdout.trim().split('\n')
          : []
      }
      checks.owned_build_images = []
      for (const image of this.ownedBuildImages) {
        const result = await this.runCommand(
          'docker',
          ['image', 'inspect', image.reference],
          {
            shell: false,
            allowFailure: true,
            redactions: this.config.secretValues,
          },
        )
        if (result.code === 0) {
          checks.owned_build_images.push(image.reference)
        }
      }
      return {
        valid: Object.values(checks).every(
          (resources) => resources.length === 0,
        ),
        resources: checks,
      }
    }

    async persistCleanup(report) {
      const completedSummary = Object.assign(
        {
          run_id: this.config.runId,
          project: this.config.project,
        },
        this.runtimeSummary || {},
        {
          completed_at: new Date().toISOString(),
          run_directory: '<run-owned-path>',
          scratch_removed: true,
          cleanup: report,
        },
      )
      delete completedSummary.run_dir
      completedSummary.compose_config = normalizeRuntimeText(
        completedSummary.compose_config,
        this.config,
      )
      completedSummary.urls = {
        APP_URL: 'http://127.0.0.1:<loopback-port:app>',
        AGG_URL: 'http://127.0.0.1:<loopback-port:api>',
        NATS_URL: 'nats://127.0.0.1:<loopback-port:nats>',
        MONITOR_URL: 'http://127.0.0.1:<loopback-port:monitor>',
        WS_BASE_URL: 'ws://127.0.0.1:<loopback-port:api>/ws',
      }
      const externalSummary = !path.resolve(
        this.config.summaryFile,
      ).startsWith(`${path.resolve(this.config.runDir)}${path.sep}`)
      if (externalSummary) {
        await fs.writeFile(
          this.config.summaryFile,
          `${JSON.stringify(completedSummary, null, 2)}\n`,
          { mode: 0o600 },
        )
      }
      if (this.config.evidenceRuntimeDir) {
        await fs.mkdir(this.config.evidenceRuntimeDir, {
          recursive: true,
          mode: 0o700,
        })
        await fs.writeFile(
          path.join(
            this.config.evidenceRuntimeDir,
            'launcher-summary.json',
          ),
          `${JSON.stringify(completedSummary, null, 2)}\n`,
          { mode: 0o600 },
        )
        await fs.writeFile(
          path.join(this.config.evidenceRuntimeDir, 'cleanup.json'),
          `${JSON.stringify(report, null, 2)}\n`,
          { mode: 0o600 },
        )
      }
    }

    async cleanup(reason) {
      if (this.cleanupPromise) return this.cleanupPromise
      this.cleanupPromise = (async () => {
        let down = { code: 1 }
        let verification = {
          valid: false,
          resources: {
            containers: [],
            networks: [],
            volumes: [],
            owned_build_images: [],
          },
        }
        let verificationError = null
        try {
          down = await this.docker(
            ['down', '-v', '--remove-orphans', '--rmi', 'local'],
            true,
          )
          verification = await this.verifyCleanup()
        } catch (error) {
          verificationError = redactSecrets(
            error.stack || error.message,
            this.config.secretValues,
          )
        }
        const report = {
          reason,
          down_exit_code: down.code,
          all_images: this.allImages,
          owned_build_images: this.ownedBuildImages,
          valid:
            verificationError === null &&
            down.code === 0 &&
            verification.valid,
          resources: verification.resources,
          ...(verificationError
            ? { verification_error: verificationError }
            : {}),
        }
        let scrubError = null
        try {
          await scrubRunFiles(this.config)
        } catch (error) {
          scrubError = redactSecrets(
            error.stack || error.message,
            this.config.secretValues,
          )
        }
        if (scrubError) {
          report.valid = false
          report.scrub_error = scrubError
        }
        await this.persistCleanup(report)
        return report
      })()
      return this.cleanupPromise
    }

    installSignalHandlers(processObject) {
      for (const pair of [['SIGINT', 130], ['SIGTERM', 143]]) {
        processObject.once(pair[0], () => {
          void this.cleanup(pair[0]).finally(() => this.exit(pair[1]))
        })
      }
    }
  }

  module.exports = {
    OwnedStack,
    normalizeRuntimeText,
    redactSecrets,
    runCommand,
  }
  ```

  `waitReady` therefore requires:

  ```text
  GET APP_URL returns 2xx.
  GET AGG_URL/api/system/status returns nats_connected=true and
  jetstream_stream_ok=true.
  GET MONITOR_URL/healthz returns 2xx.
  GET AGG_URL/api/registry contains exactly one shell-1 row that is online,
  native, worker, has `runtime.conformance="L1"`, and advertises the L1 binding
  extension.
  ```

  `collectRuntimeSummary` obtains sanitized `docker compose config`, all actual
  image references/IDs from `docker compose images --format json`, the separate
  project-owned backend/frontend/fixture build-reference list, resolved ports,
  project/run IDs, and start time. Replace the in-memory token before writing
  Compose output. The external digest-pinned NATS row remains in `images.all`
  for provenance but is absent from `images.owned_build_references`.

  `cleanup` caches one promise and runs:

  ```text
  docker compose -p PROJECT -f COMPOSE_FILE down
  -v --remove-orphans --rmi local
  ```

  It then verifies zero resources for
  `com.docker.compose.project=PROJECT` and verifies every project-owned build
  reference fails `docker image inspect`. It does not require an external image
  such as the digest-pinned NATS image to be deleted. Any cleanup-verification error is
  redacted and produces `valid=false`. `persistCleanup` copies only the sanitized
  completed launcher summary and cleanup report to the optional evidence runtime
  directory. Before any external persistence, cleanup always attempts to
  overwrite, fsync, and unlink the generated credential and recursively deletes
  the scratch run directory; persistence therefore cannot bypass scrubbing.
  Neither the credential nor the mounted native-control config is retained as
  evidence.

- [ ] **Step 8: Implement the launcher CLI and strict Playwright config**

  Supported launcher options are:

  ```text
  --config playwright.config.js
  --probe-only
  --summary-file /absolute/path/to/summary.json
  --hold-after-ready
  --release-file /absolute/path/to/release
  --evidence-runtime-dir /absolute/path/to/raw/runtime
  --
  any remaining arguments forwarded to Playwright
  ```

  `--hold-after-ready` is accepted only with `--probe-only` and an absolute
  `--release-file`; normal concurrent probes create that file to release the
  launcher. Signals remain a separate teardown test, not the normal release
  mechanism. `--evidence-runtime-dir` copies only sanitized JSON after cleanup
  and never retains the scratch directory. The launcher generates the run ID,
  owns all cleanup, and propagates Playwright exit status. Implement its main
  flow exactly:

  ```javascript
  const crypto = require('node:crypto')
  const fs = require('node:fs/promises')
  const path = require('node:path')
  const {
    makeStackConfig,
    scrubRunFiles,
    writeRunFiles,
  } = require('./helpers/stack-config')
  const {
    OwnedStack,
    runCommand,
  } = require('./helpers/owned-stack')

  function parseLauncherArgs(argv) {
    const options = {
      configPath: null,
      probeOnly: false,
      summaryFile: null,
      holdAfterReady: false,
      releaseFile: null,
      evidenceRuntimeDir: null,
      forwarded: [],
    }
    for (let index = 0; index < argv.length; index += 1) {
      const value = argv[index]
      if (value === '--') {
        options.forwarded = argv.slice(index + 1)
        break
      }
      if ([
        '--config',
        '--summary-file',
        '--release-file',
        '--evidence-runtime-dir',
      ].includes(value)) {
        const argument = argv[index + 1]
        if (!argument) throw new Error(`${value} requires a value`)
        if (value === '--config') options.configPath = argument
        if (value === '--summary-file') options.summaryFile = argument
        if (value === '--release-file') options.releaseFile = argument
        if (value === '--evidence-runtime-dir') {
          options.evidenceRuntimeDir = argument
        }
        index += 1
      } else if (value === '--probe-only') {
        options.probeOnly = true
      } else if (value === '--hold-after-ready') {
        options.holdAfterReady = true
      } else {
        options.forwarded.push(value)
      }
    }
    if (!options.probeOnly && !options.configPath) {
      throw new Error('--config is required unless --probe-only is set')
    }
    if (options.holdAfterReady) {
      if (!options.probeOnly || !options.releaseFile) {
        throw new Error(
          '--hold-after-ready requires --probe-only and --release-file',
        )
      }
    } else if (options.releaseFile) {
      throw new Error('--release-file requires --hold-after-ready')
    }
    for (const pair of [
      ['--summary-file', options.summaryFile],
      ['--release-file', options.releaseFile],
      ['--evidence-runtime-dir', options.evidenceRuntimeDir],
    ]) {
      if (pair[1] && !path.isAbsolute(pair[1])) {
        throw new Error(`${pair[0]} must be absolute`)
      }
    }
    return options
  }

  async function waitForRelease(releaseFile) {
    for (;;) {
      try {
        await fs.access(releaseFile)
        return
      } catch (error) {
        if (error.code !== 'ENOENT') throw error
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  async function main(argv, dependencies = {}) {
    const randomBytes = dependencies.randomBytes || crypto.randomBytes
    const createStack = dependencies.createStack ||
      ((stackOptions) => new OwnedStack(stackOptions))
    const options = parseLauncherArgs(argv)
    const runId = [
      'run',
      Date.now().toString(36),
      randomBytes(6).toString('hex'),
    ].join('-')
    const repoRoot = path.resolve(__dirname, '..')
    const toolchain = JSON.parse(await fs.readFile(
      path.join(repoRoot, 'scripts/research/toolchain.json'),
      'utf8',
    ))
    const config = makeStackConfig({
      runId,
      repoRoot,
      scratchRoot: path.join(repoRoot, 'tmp/e2e'),
      natsImage: toolchain.nats_image,
    })
    let runFiles = null
    let stack = null
    let testExitCode = 0
    let cleanupReport = null
    try {
      runFiles = await writeRunFiles(config, randomBytes)
      config.composeEnvironment = runFiles.composeEnvironment
      config.secretValues = [runFiles.token]
      if (options.summaryFile) {
        config.summaryFile = options.summaryFile
      }
      if (options.evidenceRuntimeDir) {
        config.evidenceRuntimeDir = options.evidenceRuntimeDir
      }
      stack = createStack({
        config,
        runCommand,
        fetchImpl: fetch,
        exit: (code) => process.exit(code),
      })
      stack.installSignalHandlers(process)
      await stack.start()
      await stack.collectRuntimeSummary(runFiles.token)
      if (options.holdAfterReady) {
        await waitForRelease(options.releaseFile)
      } else if (!options.probeOnly) {
        const result = await stack.runPlaywright(
          path.resolve(__dirname, options.configPath),
          options.forwarded,
        )
        testExitCode = result.code
      }
    } finally {
      if (stack) {
        cleanupReport = await stack.cleanup('normal-exit')
      } else {
        await scrubRunFiles(config)
      }
    }
    if (!cleanupReport.valid) return 1
    return testExitCode
  }

  if (require.main === module) {
    void main(process.argv.slice(2)).then(
      (code) => {
        process.exitCode = code
      },
      (error) => {
        process.stderr.write(`${error.message}\n`)
        process.exitCode = 1
      },
    )
  }

  module.exports = { main, parseLauncherArgs }
  ```

  The ready summary is written before `waitForRelease`. Creating the release
  file lets `main` reach its normal `finally`, perform cleanup, and exit zero.
  SIGINT/SIGTERM can interrupt the wait, invoke the installed cleanup handler,
  rewrite the external summary with the cleanup report, and terminate with the
  specified signal exit code.

  `playwright.config.js` throws if `APP_URL` or `AGG_URL` is absent, uses one
  worker, zero retries, no storage state, and no global setup/teardown:

  ```javascript
  if (!process.env.APP_URL || !process.env.AGG_URL) {
    throw new Error('APP_URL and AGG_URL are required')
  }

  module.exports = defineConfig({
    testDir: './tests',
    fullyParallel: false,
    workers: 1,
    retries: 0,
    timeout: 60_000,
    use: {
      baseURL: process.env.APP_URL,
      trace: 'off',
      video: 'off',
      screenshot: 'off',
      actionTimeout: 10_000,
      navigationTimeout: 30_000,
    },
    projects: [
      {
        name: 'chromium',
        use: {
          browserName: 'chromium',
          viewport: { width: 1440, height: 900 },
        },
      },
    ],
  })
  ```

- [ ] **Step 9: Remove fallbacks and update deterministic smoke specs**

  `api-client.js` requires `AGG_URL` and uses `${AGG_URL}/api`.
  `ws-client.js` requires `WS_BASE_URL`. Neither helper may evaluate a
  `localhost` fallback.

  Update `phase1-smoke.spec.js` to post a UUID nonce only and require exact
  `edgecitadel:${nonce}`. Update all registry expectations to `shell-1`; remove
  conditional branches, comments that instruct later adjustment, and
  hardcoded ports. Delete the old setup, teardown, smoke config, and storage
  state files.

- [ ] **Step 10: Wire exact package scripts**

  ```json
  {
    "test": "node run-isolated.js --config playwright.config.js",
    "test:stack-unit": "node --test helpers/stack-config.spec.js helpers/owned-stack.spec.js",
    "test:stack-integration": "node --test --test-concurrency=1 helpers/lifecycle.integration.spec.js",
    "test:clean-checkout": "node helpers/clean-checkout.js"
  }
  ```

  Set the description to
  `Deterministic end-to-end tests for the EdgeCitadel dashboard`.

- [ ] **Step 11: Add concurrent, signal, and clean-checkout integration gates**

  `lifecycle.integration.spec.js` uses real child processes and Docker. Start
  every held probe with both a summary and a release path:

  ```javascript
  const assert = require('node:assert/strict')
  const crypto = require('node:crypto')
  const { spawn } = require('node:child_process')
  const fs = require('node:fs/promises')
  const os = require('node:os')
  const path = require('node:path')
  const test = require('node:test')

  const e2eRoot = path.resolve(__dirname, '..')

  async function waitForJson(filePath, predicate, timeoutMs = 180_000) {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      try {
        const value = JSON.parse(await fs.readFile(filePath, 'utf8'))
        if (predicate(value)) return value
      } catch (error) {
        if (!['ENOENT', 'EACCES'].includes(error.code) &&
            !(error instanceof SyntaxError)) {
          throw error
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    throw new Error(`timed out waiting for ${filePath}`)
  }

  function startHeldProbe(root, name) {
    const summaryFile = path.join(root, `${name}-summary.json`)
    const releaseFile = path.join(root, `${name}-release`)
    const child = spawn(
      process.execPath,
      [
        'run-isolated.js',
        '--probe-only',
        '--hold-after-ready',
        '--release-file', releaseFile,
        '--summary-file', summaryFile,
      ],
      {
        cwd: e2eRoot,
        shell: false,
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    )
    let stderr = ''
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })
    const exited = new Promise((resolve, reject) => {
      child.once('error', reject)
      child.once('exit', (code, signal) => {
        resolve({ code, signal, stderr })
      })
    })
    return { child, exited, releaseFile, summaryFile }
  }

  async function waitForExit(probe, timeoutMs) {
    let timeout
    try {
      return await Promise.race([
        probe.exited,
        new Promise((resolve) => {
          timeout = setTimeout(() => resolve(null), timeoutMs)
        }),
      ])
    } finally {
      clearTimeout(timeout)
    }
  }

  async function stopHeldProbe(probe) {
    if (probe.child.exitCode !== null ||
        probe.child.signalCode !== null) {
      return probe.exited
    }
    let releaseError = null
    try {
      await fs.writeFile(
        probe.releaseFile,
        'release\n',
        { mode: 0o600, flag: 'a' },
      )
    } catch (error) {
      releaseError = error
    }
    let result = releaseError
      ? null
      : await waitForExit(probe, 15_000)
    if (result) return result
    probe.child.kill('SIGTERM')
    result = await waitForExit(probe, 10_000)
    if (!result) {
      probe.child.kill('SIGKILL')
      result = await probe.exited
    }
    if (releaseError) throw releaseError
    return result
  }

  async function postCommand(summary, nonce) {
    const response = await fetch(
      `${summary.urls.AGG_URL}/api/command/shell-1`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ body: nonce }),
      },
    )
    assert.equal(response.status, 202)
    return (await response.json()).task_id
  }

  async function pollMessages(summary, taskId) {
    const deadline = Date.now() + 15_000
    while (Date.now() < deadline) {
      const response = await fetch(
        `${summary.urls.AGG_URL}/api/messages?task_id=` +
        encodeURIComponent(taskId),
      )
      assert.equal(response.ok, true)
      const rows = await response.json()
      if (rows.some((row) => row.type === 'result')) return rows
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    throw new Error(`timed out waiting for task ${taskId}`)
  }

  function assertClean(summary) {
    assert.equal(summary.cleanup.valid, true)
    for (const resources of Object.values(summary.cleanup.resources)) {
      assert.deepEqual(resources, [])
    }
  }

  test('concurrent held probes isolate ports and database state',
    async (t) => {
      const root = await fs.mkdtemp(
        path.join(os.tmpdir(), 'edgecitadel-concurrent-'),
      )
      const probes = []
      t.after(async () => {
        let outcomes
        try {
          outcomes = await Promise.allSettled(
            probes.map(stopHeldProbe),
          )
        } finally {
          await fs.rm(root, { recursive: true, force: true })
        }
        const failed = outcomes.find(
          (outcome) => outcome.status === 'rejected',
        )
        if (failed) throw failed.reason
      })
      const left = startHeldProbe(root, 'left')
      probes.push(left)
      const right = startHeldProbe(root, 'right')
      probes.push(right)
      const [leftReady, rightReady] = await Promise.all([
        waitForJson(left.summaryFile, (value) => value.urls),
        waitForJson(right.summaryFile, (value) => value.urls),
      ])

      assert.notEqual(leftReady.run_id, rightReady.run_id)
      assert.notEqual(leftReady.project, rightReady.project)
      assert.notEqual(leftReady.run_dir, rightReady.run_dir)
      for (const key of [
        'APP_URL',
        'AGG_URL',
        'NATS_URL',
        'MONITOR_URL',
      ]) {
        assert.notEqual(leftReady.urls[key], rightReady.urls[key])
      }

      const leftRegistry = await (
        await fetch(`${leftReady.urls.AGG_URL}/api/registry`)
      ).json()
      const rightRegistry = await (
        await fetch(`${rightReady.urls.AGG_URL}/api/registry`)
      ).json()
      assert.deepEqual(
        leftRegistry.map((row) => row.agent_id),
        ['shell-1'],
      )
      assert.deepEqual(
        rightRegistry.map((row) => row.agent_id),
        ['shell-1'],
      )

      const leftTask = await postCommand(
        leftReady,
        `left-${crypto.randomUUID()}`,
      )
      const rightTask = await postCommand(
        rightReady,
        `right-${crypto.randomUUID()}`,
      )
      await Promise.all([
        pollMessages(leftReady, leftTask),
        pollMessages(rightReady, rightTask),
      ])
      assert.deepEqual(
        await (
          await fetch(
            `${rightReady.urls.AGG_URL}/api/messages?task_id=${leftTask}`,
          )
        ).json(),
        [],
      )
      assert.deepEqual(
        await (
          await fetch(
            `${leftReady.urls.AGG_URL}/api/messages?task_id=${rightTask}`,
          )
        ).json(),
        [],
      )

      await Promise.all([
        fs.writeFile(left.releaseFile, 'release\n', { mode: 0o600 }),
        fs.writeFile(right.releaseFile, 'release\n', { mode: 0o600 }),
      ])
      const [leftExit, rightExit] = await Promise.all([
        left.exited,
        right.exited,
      ])
      assert.deepEqual(
        [leftExit.code, rightExit.code],
        [0, 0],
        `${leftExit.stderr}\n${rightExit.stderr}`,
      )
      assertClean(await waitForJson(
        left.summaryFile,
        (value) => value.cleanup,
      ))
      assertClean(await waitForJson(
        right.summaryFile,
        (value) => value.cleanup,
      ))
    },
  )

  test('SIGTERM cleans a held probe before exit 143', async (t) => {
    const root = await fs.mkdtemp(
      path.join(os.tmpdir(), 'edgecitadel-signal-'),
    )
    let probe = null
    t.after(async () => {
      try {
        if (probe) await stopHeldProbe(probe)
      } finally {
        await fs.rm(root, { recursive: true, force: true })
      }
    })
    probe = startHeldProbe(root, 'signal')
    await waitForJson(probe.summaryFile, (value) => value.urls)
    probe.child.kill('SIGTERM')
    const result = await probe.exited
    assert.equal(result.code, 143, result.stderr)
    const completed = await waitForJson(
      probe.summaryFile,
      (value) => value.cleanup,
    )
    assert.equal(completed.cleanup.reason, 'SIGTERM')
    assertClean(completed)
  })
  ```

  Register each asynchronous `t.after` before the first spawn and add every
  returned probe to its cleanup set immediately. Therefore a later spawn error,
  readiness timeout, failed isolation assertion, fetch exception, or signal
  assertion still releases or terminates every existing child and awaits all
  exits before deleting the temporary root.

  `clean-checkout.js` accepts `--tree OID`, then
  `E2E_CANDIDATE_TREE`, then `HEAD` in that order. It resolves the value to a
  tree, archives that exact tree into a staged temporary checkout, runs a probe,
  and validates the completed external summary:

  ```javascript
  const assert = require('node:assert/strict')
  const { execFile } = require('node:child_process')
  const fs = require('node:fs/promises')
  const os = require('node:os')
  const path = require('node:path')
  const { promisify } = require('node:util')

  const exec = promisify(execFile)

  function requestedTree(argv) {
    if (argv.length === 0) {
      return process.env.E2E_CANDIDATE_TREE || 'HEAD'
    }
    if (argv.length !== 2 || argv[0] !== '--tree') {
      throw new Error('usage: clean-checkout.js [--tree TREEISH]')
    }
    return argv[1]
  }

  async function main(argv) {
    const repoRoot = path.resolve(__dirname, '../..')
    const root = await fs.mkdtemp(
      path.join(os.tmpdir(), 'edgecitadel-clean-checkout-'),
    )
    try {
      const treeish = requestedTree(argv)
      const resolved = await exec(
        'git',
        ['rev-parse', `${treeish}^{tree}`],
        { cwd: repoRoot },
      )
      const tree = resolved.stdout.trim()
      if (!/^[0-9a-f]{40,64}$/.test(tree)) {
        throw new Error(`invalid resolved tree: ${tree}`)
      }
      const archive = path.join(root, 'candidate.tar')
      const checkout = path.join(root, 'checkout')
      const summaryFile = path.join(root, 'summary.json')
      await fs.mkdir(checkout)
      await exec(
        'git',
        ['archive', '--format=tar', `--output=${archive}`, tree],
        { cwd: repoRoot },
      )
      await exec('tar', ['-xf', archive, '-C', checkout])
      await exec(
        process.execPath,
        [
          'e2e/run-isolated.js',
          '--probe-only',
          '--summary-file', summaryFile,
        ],
        { cwd: checkout, maxBuffer: 10 * 1024 * 1024 },
      )
      const summary = JSON.parse(
        await fs.readFile(summaryFile, 'utf8'),
      )
      assert.equal(summary.cleanup.valid, true)
      for (const resources of Object.values(summary.cleanup.resources)) {
        assert.deepEqual(resources, [])
      }
      process.stdout.write('PASS clean-checkout\n')
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  }

  void main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack || error.message}\n`)
    process.exitCode = 1
  })
  ```

  The helper never creates a worktree, mutates the caller's index, or includes
  untracked local files.

- [ ] **Step 12: Verify unit and real lifecycle gates**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  git add -- \
    .gitignore \
    aggregator/main.py \
    aggregator/models.py \
    aggregator/tests/test_api.py \
    aggregator/tests/test_jetstream_bootstrap.py \
    e2e/run-isolated.js \
    e2e/helpers/stack-config.js \
    e2e/helpers/stack-config.spec.js \
    e2e/helpers/owned-stack.js \
    e2e/helpers/owned-stack.spec.js \
    e2e/helpers/lifecycle.integration.spec.js \
    e2e/helpers/clean-checkout.js \
    e2e/docker-compose.test.yml \
    e2e/playwright.config.js \
    e2e/playwright.smoke.config.js \
    e2e/global-setup.js \
    e2e/global-teardown.js \
    e2e/test-storage-state.json \
    e2e/package.json \
    e2e/package-lock.json \
    e2e/helpers/api-client.js \
    e2e/helpers/ws-client.js \
    e2e/helpers/fixtures.js \
    e2e/tests/phase1-smoke.spec.js \
    e2e/tests/phase3-registry-tab.spec.js \
    scripts/research/fixtures/native_control.py \
    tests/research/test_native_control.py
  git diff --cached --name-only
  git diff --cached --check
  export TASK5_CANDIDATE_TREE="$(git write-tree)"
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests/test_api.py \
    -k "direct_command_publish_has_complete_correlation or direct_command_rejects_non_uuid4_context" \
    -q
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider \
    tests/research/test_native_control.py::test_echo_result_preserves_correlation \
    tests/research/test_native_control.py::test_progress_frames_preserve_correlation \
    tests/research/test_native_control.py::test_terminal_release_absent_is_noop \
    tests/research/test_native_control.py::test_terminal_release_missing_hold_is_noop \
    tests/research/test_native_control.py::test_terminal_release_waits_for_file \
    tests/research/test_native_control.py::test_terminal_release_rejects_unsafe_entry \
    -q
  RUN_JETSTREAM_INTEGRATION=1 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests/test_jetstream_bootstrap.py -q
  EXPECTED_NATS_IMAGE="nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927"
  ACTUAL_NATS_IMAGE="$(
    scripts/research/run-python - <<'PY'
  import json
  from pathlib import Path

  print(json.loads(
      Path("scripts/research/toolchain.json").read_text()
  )["nats_image"])
  PY
  )"
  test "$ACTUAL_NATS_IMAGE" = "$EXPECTED_NATS_IMAGE"
  test -z "$(
    rg -n 'nats:2[.]10-alpine' \
      aggregator e2e scripts tests docker-compose.yml .dockerignore \
      docs/research/plans/2026-07-25-slice-2-deterministic-operator-journey.md \
      || true
  )"
  cd /Users/yefanzhang/workplace/edge-research/e2e
  npm run test:stack-unit
  npm run test:stack-integration
  E2E_CANDIDATE_TREE="$TASK5_CANDIDATE_TREE" npm run test:clean-checkout
  npm test -- tests/phase1-smoke.spec.js tests/phase3-registry-tab.spec.js
  ```

  Expected: seven correlation/validation cases, four release-gate cases, and four
  opt-in disposable-container JetStream tests pass; 24 stack unit tests pass; two
  integration scenarios pass; the clean staged candidate tree prints one PASS
  line; eight deterministic smoke/registry Playwright tests pass; every cleanup
  report has zero containers, networks, volumes, and owned build references.
  Keep the index staged for Step 13 and invoke `verify-backend` plus
  `verify-infra`.

- [ ] **Step 13: Commit**

  Run `commit-check`, verify the exact Task 5 map including four deletions, and
  require that the staged tree is still the tree tested in Step 12. If the shell
  no longer has `TASK5_CANDIDATE_TREE`, repeat Step 12 rather than guessing it.
  Commit that same index:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  test "$(git write-tree)" = "$TASK5_CANDIDATE_TREE"
  git diff --cached --name-only
  git diff --cached --check
  git commit -m "test(e2e): own isolated stack lifecycle"
  ```

### Task 6: Add The Paper-Supporting Operator Journey

**Files:**
- Create: `e2e/helpers/operator-journey.js`
- Create: `e2e/tests/operator-journey.spec.js`
- Modify: `frontend/src/components/AgentCard.jsx`
- Modify: `frontend/src/components/CommandInput.jsx`
- Modify: `frontend/src/components/MessageBubble.jsx`
- Modify: `frontend/src/components/TaskCard.jsx`
- Modify: `frontend/src/components/TaskBoard.jsx`

- [ ] **Step 1: Write the failing helper and journey tests first**

  `operator-journey.js` exports complete, independently tested helpers:

  ```javascript
  function requireEnvironment(name) {
    const value = process.env[name]
    if (!value) throw new Error(`${name} is required`)
    return value
  }

  async function pollJson(request, url, predicate, timeoutMs = 15_000) {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const response = await request.get(url)
      if (response.ok()) {
        const value = await response.json()
        if (predicate(value)) return value
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    throw new Error(`timed out polling ${url}`)
  }

  function canonicalJson(value) {
    if (Array.isArray(value)) {
      return `[${value.map(canonicalJson).join(',')}]`
    }
    if (value && typeof value === 'object') {
      const keys = Object.keys(value).sort()
      return `{${keys.map(
        (key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`,
      ).join(',')}}`
    }
    return JSON.stringify(value)
  }

  async function assertNoOverlap(locators) {
    for (const locator of locators) {
      await locator.scrollIntoViewIfNeeded()
    }
    const viewport = await locators[0].evaluate(() => ({
      width: window.innerWidth,
      height: window.innerHeight,
    }))
    const boxes = []
    for (const locator of locators) {
      const box = await locator.boundingBox()
      if (!box) throw new Error('evidence locator has no bounding box')
      const insideViewport =
        box.x >= 0 &&
        box.y >= 0 &&
        box.x + box.width <= viewport.width &&
        box.y + box.height <= viewport.height
      if (!insideViewport) {
        throw new Error('evidence locator is outside the viewport')
      }
      boxes.push(box)
    }
    for (let left = 0; left < boxes.length; left += 1) {
      for (let right = left + 1; right < boxes.length; right += 1) {
        const a = boxes[left]
        const b = boxes[right]
        const separated =
          a.x + a.width <= b.x ||
          b.x + b.width <= a.x ||
          a.y + a.height <= b.y ||
          b.y + b.height <= a.y
        if (!separated) throw new Error(`overlap at ${left}/${right}`)
      }
    }
  }

  module.exports = {
    assertNoOverlap,
    canonicalJson,
    pollJson,
    requireEnvironment,
  }
  ```

  The single Playwright test must, in order:

  ```text
  collect console errors, page errors, request failures, and every unexpected
  HTTP 4xx/5xx outside the explicit allowlist;
  require healthy NATS and JetStream;
  require exactly one online native worker shell-1 with
  runtime.conformance=L1 and the L1 extension;
  open "/" and select shell-1 while fleet status remains Connected;
  create nonce = crypto.randomUUID();
  atomically create E2E_TERMINAL_RELEASE_DIR/nonce.hold before sending;
  fill the command input with exactly nonce and click Send command;
  capture the 202 response task_id;
  observe submitted or working in the Tasks view before a terminal;
  atomically create E2E_TERMINAL_RELEASE_DIR/task_id.release only after that
  nonterminal assertion;
  observe completed in the Tasks view;
  return to Chat and see shell-1 selected, exact nonce command, full task ID,
  and exact edgecitadel:${nonce} result;
  require one API command row and one logical terminal result row;
  require the command context to be UUIDv4 with hop_count=0 and every
  progress/result row to preserve both values;
  require no conflicting terminal state or canonical payload;
  poll queue until pending=0 and ack_pending=0;
  require the collected error list to equal [];
  attach canonical operator-metadata.json with complete task, context, hop,
  endpoint identity, envelope ID, and observation-index correlation;
  only after optional evidence capture and attachments, require the collected
  error list to equal [].
  ```

  Implement that sequence as one test:

  ```javascript
  const crypto = require('node:crypto')
  const fs = require('node:fs/promises')
  const path = require('node:path')
  const { test, expect } = require('@playwright/test')
  const {
    canonicalJson,
    pollJson,
    requireEnvironment,
  } = require('../helpers/operator-journey')

  const AGG_URL = requireEnvironment('AGG_URL')
  const TERMINAL_RELEASE_DIR = requireEnvironment(
    'E2E_TERMINAL_RELEASE_DIR',
  )
  const TERMINAL_STATES = new Set([
    'completed',
    'failed',
    'canceled',
    'rejected',
  ])
  const ALLOWED_HTTP_ERRORS = new Set([
    '404 /favicon.ico',
  ])
  const UUID_V4 = (
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
  )

  test('operator observes one deterministic task lifecycle',
    async ({ page, request }, testInfo) => {
      const errors = []
      page.on('console', (message) => {
        if (message.type() === 'error') {
          errors.push(`console:${message.text()}`)
        }
      })
      page.on('pageerror', (error) => {
        errors.push(`pageerror:${error.message}`)
      })
      page.on('requestfailed', (failedRequest) => {
        errors.push(
          `requestfailed:${failedRequest.method()}:${failedRequest.url()}`,
        )
      })
      page.on('response', (response) => {
        if (response.status() >= 400) {
          const pathname = new URL(response.url()).pathname
          const key = `${response.status()} ${pathname}`
          if (!ALLOWED_HTTP_ERRORS.has(key)) {
            errors.push(`http:${response.status()}:${response.url()}`)
          }
        }
      })

      const healthResponse = await request.get(
        `${AGG_URL}/api/system/status`,
      )
      expect(healthResponse.ok()).toBe(true)
      expect(await healthResponse.json()).toEqual(
        expect.objectContaining({
          nats_connected: true,
          jetstream_stream_ok: true,
        }),
      )

      const registry = await pollJson(
        request,
        `${AGG_URL}/api/registry`,
        (rows) => rows.some((row) => row.agent_id === 'shell-1'),
      )
      const shellRows = registry.filter(
        (row) => row.agent_id === 'shell-1',
      )
      expect(shellRows).toHaveLength(1)
      const shell = shellRows[0]
      expect(shell.agent_state).toBe('online')
      expect(shell.card.metadata['runtime.kind']).toBe('native')
      expect(shell.card.metadata['runtime.roles']).toContain('worker')
      expect(shell.card.metadata['runtime.conformance']).toBe('L1')
      expect(shell.card.capabilities.extensions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            uri: 'https://edgecitadel.local/ext/nats-binding/v1',
          }),
        ]),
      )

      await page.goto('/')
      await expect(page.getByText('Connected', { exact: true }))
        .toBeVisible()
      const agent = page.locator('[data-agent-id="shell-1"]')
      if (testInfo.project.name === 'mobile') {
        await page.getByRole('button', {
          name: 'Open agent list',
        }).click()
      }
      await expect(agent).toBeInViewport()
      await agent.click()
      await expect(agent).toHaveAttribute('aria-pressed', 'true')
      if (testInfo.project.name === 'mobile') {
        await expect(agent).not.toBeInViewport()
      }
      await expect(page.getByText('Connected', { exact: true }))
        .toBeVisible()
      await expect(page.getByLabel('Command target'))
        .toHaveValue('shell-1')
      await expect(
        page.getByRole('status', {
          name: 'Selected agent shell-1: online',
        }),
      ).toBeVisible()

      const nonce = crypto.randomUUID()
      const holdPath = path.join(
        TERMINAL_RELEASE_DIR,
        `${nonce}.hold`,
      )
      await fs.writeFile(
        holdPath,
        'hold\n',
        { encoding: 'utf8', flag: 'wx', mode: 0o600 },
      )
      const responsePromise = page.waitForResponse((response) => {
        const url = new URL(response.url())
        return (
          response.request().method() === 'POST' &&
          url.pathname === '/api/command/shell-1'
        )
      })
      await page.getByLabel('Command body').fill(nonce)
      await page.getByRole('button', { name: 'Send command' }).click()
      const commandResponse = await responsePromise
      expect(commandResponse.status()).toBe(202)
      expect(commandResponse.request().postDataJSON())
        .toEqual({ body: nonce })
      const accepted = await commandResponse.json()
      const taskId = accepted.task_id
      expect(taskId).toMatch(UUID_V4)

      await page.getByRole('button', { name: /^Tasks/ }).click()
      const nonterminal = page.locator(
        `[data-task-id="${taskId}"][data-task-state="submitted"],` +
        `[data-task-id="${taskId}"][data-task-state="working"]`,
      )
      await expect(nonterminal).toBeVisible({ timeout: 15_000 })
      const releasePath = path.join(
        TERMINAL_RELEASE_DIR,
        `${taskId}.release`,
      )
      await fs.writeFile(
        releasePath,
        'release\n',
        { encoding: 'utf8', flag: 'wx', mode: 0o600 },
      )
      const completed = page.locator(
        `[data-task-id="${taskId}"][data-task-state="completed"]`,
      )
      await expect(completed).toBeVisible({ timeout: 15_000 })
      await Promise.all([
        fs.rm(holdPath, { force: true }),
        fs.rm(releasePath, { force: true }),
      ])

      await page.getByRole('button', { name: /^Chat/ }).click()
      const command = page.locator(
        `[data-task-id="${taskId}"][data-message-type="command"]`,
      )
      const result = page.locator(
        `[data-task-id="${taskId}"][data-message-type="result"]`,
      )
      await expect(command).toContainText(nonce)
      await expect(result).toContainText(`edgecitadel:${nonce}`)
      await expect(
        page.getByText(`Tracking task: ${taskId}`, { exact: true }),
      ).toBeVisible()
      await expect(agent).toHaveAttribute('aria-pressed', 'true')

      const messages = await pollJson(
        request,
        `${AGG_URL}/api/messages?task_id=${encodeURIComponent(taskId)}`,
        (rows) => rows.some(
          (row) =>
            row.type === 'result' &&
            TERMINAL_STATES.has(row.task_state),
        ),
      )
      const commands = messages.filter((row) => row.type === 'command')
      const terminals = messages.filter(
        (row) =>
          row.type === 'result' &&
          TERMINAL_STATES.has(row.task_state),
      )
      const progress = messages.filter(
        (row) => row.type === 'task.progress',
      ).sort(
        (left, right) =>
          left.observation_index - right.observation_index,
      )
      expect(commands).toHaveLength(1)
      expect(commands[0].payload.body).toBe(nonce)
      expect(commands[0].context_id).toMatch(UUID_V4)
      expect(commands[0].hop_count).toBe(0)
      expect(commands[0].sender_id).toBe('aggregator')
      expect(commands[0].recipient_id).toBe('shell-1')
      expect(terminals).toHaveLength(1)
      expect(terminals[0].task_state).toBe('completed')
      expect(terminals[0].payload.body).toBe(`edgecitadel:${nonce}`)
      expect(terminals[0].sender_id).toBe('shell-1')
      expect(terminals[0].recipient_id).toBe('aggregator')
      expect(progress.length).toBeGreaterThan(0)
      for (const row of progress.concat(terminals)) {
        expect(row.context_id).toBe(commands[0].context_id)
        expect(row.hop_count).toBe(commands[0].hop_count)
        expect(row.sender_id).toBe('shell-1')
        expect(row.recipient_id).toBe('aggregator')
      }
      for (const row of progress) {
        expect(row.observation_index)
          .toBeGreaterThan(commands[0].observation_index)
        expect(row.observation_index)
          .toBeLessThan(terminals[0].observation_index)
      }
      const logicalTerminals = new Set(
        terminals.map(
          (row) =>
            `${row.task_state}:${canonicalJson(row.payload)}`,
        ),
      )
      expect(logicalTerminals.size).toBe(1)
      expect(Number.isInteger(commands[0].observation_index)).toBe(true)
      expect(Number.isInteger(terminals[0].observation_index)).toBe(true)
      expect(terminals[0].observation_index)
        .toBeGreaterThan(commands[0].observation_index)

      await pollJson(
        request,
        `${AGG_URL}/api/agents/shell-1/queue`,
        (queue) =>
          queue.pending === 0 &&
          queue.ack_pending === 0,
      )
      const metadata = {
        project: testInfo.project.name,
        task_id: taskId,
        nonce,
        command_body: nonce,
        expected_output: `edgecitadel:${nonce}`,
        context_id: commands[0].context_id,
        hop_count: commands[0].hop_count,
        command_envelope_id: commands[0].id,
        terminal_envelope_id: terminals[0].id,
        progress_envelope_ids: progress.map((row) => row.id),
        command_sender_id: commands[0].sender_id,
        command_recipient_id: commands[0].recipient_id,
        terminal_sender_id: terminals[0].sender_id,
        terminal_recipient_id: terminals[0].recipient_id,
        browser_name: testInfo.project.use.browserName || 'chromium',
        browser_version: page.context().browser().version(),
        command_observation_index: commands[0].observation_index,
        progress_observation_indices: progress.map(
          (row) => row.observation_index,
        ),
        terminal_observation_index: terminals[0].observation_index,
      }
      const metadataPath = testInfo.outputPath('operator-metadata.json')
      await fs.writeFile(
        metadataPath,
        `${canonicalJson(metadata)}\n`,
        'utf8',
      )
      await testInfo.attach('operator-metadata', {
        path: metadataPath,
        contentType: 'application/json',
      })
      expect(errors).toEqual([])
    },
  )
  ```

- [ ] **Step 2: Run red before adding selectors**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/e2e
  npm test -- tests/operator-journey.spec.js
  ```

  Expected: exactly one test runs and fails on missing selected-agent,
  command-control, message-type, or task-state semantics. Do not add selectors
  before observing this failure.

- [ ] **Step 3: Add only the required semantic controls**

  Implement:

  ```jsx
  // AgentCard
  <button
    data-agent-id={agent.agent_id}
    aria-pressed={selected}
    aria-label={`Select agent ${agent.agent_id}`}
  >

  // CommandInput
  <select aria-label="Command target">
  <input aria-label="Command body">
  <button aria-label="Send command">

  // MessageBubble
  <div
    data-task-id={message.task_id || ''}
    data-message-type={message.type}
  >

  // TaskCard
  <div
    data-task-id={task.task_id}
    data-task-state={task.task_state}
  >
  ```

  In `CommandInput`, derive and render compact selected-agent state directly
  below the command-control row so it remains visible at the mobile viewport:

  ```jsx
  const selectedRow = agents.find(
    (row) => row.agent_id === effectiveTarget,
  )
  const selectedState = selectedRow?.agent_state || 'offline'

  {effectiveTarget && (
    <span
      data-selected-agent-status
      role="status"
      aria-label={
        `Selected agent ${effectiveTarget}: ${selectedState}`
      }
      className="inline-flex min-w-0 items-center gap-1.5 text-xs text-gray-400"
    >
      <span
        aria-hidden="true"
        className={
          selectedState === 'online'
            ? 'h-2 w-2 shrink-0 rounded-full bg-status-online'
            : 'h-2 w-2 shrink-0 rounded-full bg-status-offline'
        }
      />
      <span className="truncate font-mono">{effectiveTarget}</span>
      <span>{selectedState}</span>
    </span>
  )}
  ```

  Keep all existing visible text. The `shell-1 online` state is operational
  state, not instructional copy. Do not add any other explanatory UI text.

- [ ] **Step 4: Make nonterminal state observable without aggressive polling**

  Subscribe TaskBoard to the fleet `realtimeMessages` revision and call
  Task 3's generation-guarded `fetchTasks` when a command, progress, or result
  frame changes that revision. All overlapping event, manual, initial, and
  fallback requests share the same generation counter, so an older completion
  cannot regress state. Keep the five-second fallback interval. The one-second
  fixture delay must remain unchanged. Do not introduce a sub-second permanent
  poll.

- [ ] **Step 5: Run the journey twice from independent stacks**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/e2e
  npm test -- tests/operator-journey.spec.js
  npm test -- tests/operator-journey.spec.js
  ```

  Expected: one test passes in each run; run/project/port/task IDs differ; each
  command body is a UUID only; every cleanup removes its owned build references.

- [ ] **Step 6: Run frontend and infrastructure verification**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm test
  npm run lint
  npm run build
  cd /Users/yefanzhang/workplace/edge-research/e2e
  npm run test:stack-unit
  ```

  Expected: all commands exit zero. Invoke `verify-frontend`.

- [ ] **Step 7: Commit**

  Run `commit-check`, stage only the seven Task 6 files, and commit:

  ```bash
  git commit -m "test(e2e): prove the operator task lifecycle"
  ```

### Task 7: Separate Deterministic Gates From Optional Live Systems

**Files:**
- Create: `e2e/playwright.live.config.js`
- Create: `e2e/helpers/gate-classification.spec.js`
- Modify: `frontend/src/Layout.jsx`
- Modify: `e2e/playwright.config.js`
- Modify: `e2e/package.json`
- Modify: `e2e/tests/dark-mode.spec.js`
- Modify: `e2e/tests/keyboard-shortcuts.spec.js`
- Modify: `e2e/tests/phase2-gemma-smoke.spec.js`
- Modify: `e2e/tests/phase2.5-streaming-and-memory.spec.js`
- Modify: `e2e/tests/phase3-watchdog-fast-path.spec.js`
- Modify: `e2e/tests/phase6-hermes-bridge.spec.js`
- Modify: `e2e/tests/streaming-fragmentation-regression.spec.js`

- [ ] **Step 1: Write the failing static classification test**

  Set synthetic environment URLs before loading both configs. Assert exact
  membership:

  ```javascript
  const deterministic = [
    'phase1-smoke.spec.js',
    'phase3-registry-tab.spec.js',
    'dark-mode.spec.js',
    'keyboard-shortcuts.spec.js',
    'operator-journey.spec.js',
  ]

  const live = [
    'phase2-gemma-smoke.spec.js',
    'phase2.5-streaming-and-memory.spec.js',
    'phase3-watchdog-fast-path.spec.js',
    'phase6-hermes-bridge.spec.js',
    'streaming-fragmentation-regression.spec.js',
  ]
  ```

  Assert default workers `1`, retries `0`, no global lifecycle, no storage
  state, and no live spec. Assert the live config requires all three
  `LIVE_APP_URL`, `LIVE_AGG_URL`, and `LIVE_NATS_URL` values and has no Compose
  lifecycle.

- [ ] **Step 2: Run the classification test and verify red**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/e2e
  node --test helpers/gate-classification.spec.js
  ```

  Expected: one test fails because the live config and exact matches are absent.

- [ ] **Step 3: Declare the deterministic config**

  Set:

  ```javascript
  testMatch: [
    'phase1-smoke.spec.js',
    'phase3-registry-tab.spec.js',
    'dark-mode.spec.js',
    'keyboard-shortcuts.spec.js',
    'operator-journey.spec.js',
  ],
  ```

  Rewrite `dark-mode.spec.js` as one strict test: `html` retains class `dark`,
  the heading is `EdgeCitadel`, the theme button is absent, and header/chat/task
  text has nontransparent foreground/background colors.

  Rewrite all three keyboard tests without `if`, fallback locators, or sleeps:
  keys 1-5 select their named tabs; focus in Command body prevents switching;
  the selected tab has `aria-current="page"`. The red keyboard assertion proves
  the attribute is currently absent; then add it unconditionally to the existing
  tab button:

  ```jsx
  <button
    key={tab.key}
    aria-current={activeTab === tab.key ? 'page' : undefined}
    onClick={() => setActiveTab(tab.key)}
  >
  ```

- [ ] **Step 4: Declare the live config and explicit fragmentation decision**

  `playwright.live.config.js`:

  ```javascript
  const { defineConfig } = require('@playwright/test')

  for (const name of ['LIVE_APP_URL', 'LIVE_AGG_URL', 'LIVE_NATS_URL']) {
    if (!process.env[name]) throw new Error(`${name} is required`)
  }

  module.exports = defineConfig({
    testDir: './tests',
    testMatch: [
      'phase2-gemma-smoke.spec.js',
      'phase2.5-streaming-and-memory.spec.js',
      'phase3-watchdog-fast-path.spec.js',
      'phase6-hermes-bridge.spec.js',
      'streaming-fragmentation-regression.spec.js',
    ],
    workers: 1,
    retries: 0,
    use: {
      baseURL: process.env.LIVE_APP_URL,
    },
  })
  ```

  Classification decision: `streaming-fragmentation-regression.spec.js` is
  live-only because `shell-1` uses `behavior="echo"` and does not emit the many
  streaming chunks that define that regression. Converting it to the default
  gate would require a second fixture behavior and is outside Slice 2.

  Remove every localhost fallback from the five live specs. Read live values
  from the required environment only. Do not skip when Gemma, Hermes, watchdog,
  Ollama, or NATS is absent; the operator chose the live gate and receives a
  clear failure.

- [ ] **Step 5: Wire exact scripts**

  ```json
  {
    "test": "node run-isolated.js --config playwright.config.js",
    "test:classification": "node --test helpers/gate-classification.spec.js",
    "test:live": "npx playwright test --config playwright.live.config.js"
  }
  ```

  `npm test` must never invoke the live config.

- [ ] **Step 6: Run red-green regression and the full deterministic gate**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research/e2e
  npm run test:classification
  npm test
  npm test
  npm run test:stack-integration
  npm run test:clean-checkout
  ```

  Expected: one classification test passes; exactly 13 deterministic Playwright
  tests pass twice in separate stacks; two lifecycle integration scenarios pass;
  clean checkout prints one PASS line; every run has zero project resource or
  owned-build-reference residue.

- [ ] **Step 7: Run cumulative pre-evidence verification**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests -q
  scripts/research/run-python -m compileall -q aggregator
  cd /Users/yefanzhang/workplace/edge-research/frontend
  node --test tests/tooling-contract.test.cjs
  npm test
  npm run lint
  npm run build
  cd /Users/yefanzhang/workplace/edge-research/e2e
  npm run test:stack-unit
  npm run test:classification
  npm test
  ```

  Expected: zero failures; 24 stack unit tests, one classification test, and
  13 deterministic Playwright tests pass. Invoke `verify-infra`.

- [ ] **Step 8: Commit**

  Run `commit-check`, stage only the twelve Task 7 files, including
  `frontend/src/Layout.jsx`, and commit:

  ```bash
  git commit -m "test(e2e): separate deterministic and live gates"
  ```

### Task 8: Capture And Check Final Desktop And Mobile Evidence

**Files:**
- Create: `e2e/playwright.evidence.config.js`
- Create: `e2e/helpers/evidence-artifacts.js`
- Create: `scripts/research/capture_operator_journey.py`
- Create: `tests/research/test_operator_evidence.py`
- Modify: `scripts/research/check_artifact.py`
- Modify: `tests/research/test_checker.py`
- Modify: `schemas/research-manifest.v1.json`
- Modify: `tests/research/test_evidence.py`
- Modify: `docs/research/results/README.md`
- Modify after final gates: `docs/research/task-aware-reliability-contract-design.md`
- Modify: `e2e/run-isolated.js`
- Modify: `e2e/tests/operator-journey.spec.js`

- [ ] **Step 1: Write failing two-project evidence tests**

  Use temporary directories and an injected fake launcher. Require this exact
  project mapping:

  ```python
  expected_projects = {
      "desktop": {
          "task_id": "11111111-1111-4111-8111-111111111111",
          "nonce": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
          "command_body": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
          "expected_output": "edgecitadel:dddddddd-dddd-4ddd-8ddd-dddddddddddd",
          "context_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
          "hop_count": 0,
          "command_envelope_id": "11111111-aaaa-4111-8111-111111111111",
          "terminal_envelope_id": "11111111-bbbb-4111-8111-111111111111",
          "progress_envelope_ids": [
              "11111111-cccc-4111-8111-111111111111"
          ],
          "command_sender_id": "aggregator",
          "command_recipient_id": "shell-1",
          "terminal_sender_id": "shell-1",
          "terminal_recipient_id": "aggregator",
          "browser_name": "chromium",
          "browser_version": "test-browser-version",
          "command_observation_index": 1,
          "progress_observation_indices": [2],
          "terminal_observation_index": 3,
      },
      "mobile": {
          "task_id": "22222222-2222-4222-8222-222222222222",
          "nonce": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
          "command_body": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
          "expected_output": "edgecitadel:eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
          "context_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
          "hop_count": 0,
          "command_envelope_id": "22222222-aaaa-4222-8222-222222222222",
          "terminal_envelope_id": "22222222-bbbb-4222-8222-222222222222",
          "progress_envelope_ids": [
              "22222222-cccc-4222-8222-222222222222"
          ],
          "command_sender_id": "aggregator",
          "command_recipient_id": "shell-1",
          "terminal_sender_id": "shell-1",
          "terminal_recipient_id": "aggregator",
          "browser_name": "chromium",
          "browser_version": "test-browser-version",
          "command_observation_index": 4,
          "progress_observation_indices": [5],
          "terminal_observation_index": 6,
      },
  }
  assert expected_projects["desktop"]["task_id"] != (
      expected_projects["mobile"]["task_id"]
  )
  ```

  Require one passing stack run and, per project:

  ```text
  chat.png
  tasks.png
  video.webm
  trace.zip
  operator-metadata.json
  api/system-status.json
  api/registry.json
  api/messages.json
  api/queue.json
  ```

  Each project's metadata, task-filtered API snapshot, screenshots, trace, and
  video must positively identify that project's task ID. Because both projects
  intentionally share one stack, a later automatic trace may also contain prior
  stack traffic; project ownership comes from the passed-result attachment
  mapping plus a positive task/nonce/output match. No assertion or manifest
  field claims one task ID is shared across projects.

- [ ] **Step 2: Write failing checker corruption tests**

  Build a complete temporary operator bundle using only test-local source and
  artifact fixtures, finalize it, then call
  `scripts.research.check_artifact.check_bundle`. Provide a test-only
  `rehash_bundle_for_test(bundle, mutation)`: load the valid manifest, apply the
  mutation, rebuild its artifact records from every non-manifest file with the
  exact Slice 1 path/hash/size shape, validate only the JSON schema, and
  canonically rewrite `manifest.json`. It deliberately does not call the secret
  scanner or operator checker. This keeps every semantic-invalid fixture,
  including the secret case, internally hash-consistent so each test reaches
  the intended checker rule instead of a generic digest failure. Parameterize
  these corruptions and exact codes:

  ```text
  delete desktop/tasks.png and rehash -> OPERATOR_ARTIFACT_MISSING
  modify mobile/chat.png after hashing only -> ARTIFACT_HASH_MISMATCH
  put desktop task_id in mobile/api/messages.json and rehash
    -> OPERATOR_CROSS_PROJECT_TASK
  make desktop and mobile task IDs equal and rehash
    -> OPERATOR_TASK_IDS_NOT_DISTINCT
  put a generated token pattern in launcher-summary Compose text and rehash
    -> SECRET_PATTERN_FOUND
  mark each cleanup resource nonempty in manifest and both runtime copies,
    then rehash
    -> OPERATOR_CLEANUP_RESIDUE
  alter correlation identity, UUIDv4 context, hop, or observation index and
    rehash -> OPERATOR_CORRELATION_MISMATCH
  alter runtime/project/compose values and rehash
    -> OPERATOR_RUNTIME_MISMATCH
  rewrite mobile trace as a valid safe ZIP containing desktop correlation only
    and rehash -> OPERATOR_TRACE_MISMATCH
  make a portable-report attachment absolute and rehash
    -> OPERATOR_ATTACHMENT_PATH_INVALID
  change a relevant source fixture after source_sha256 is recorded
    -> OPERATOR_SOURCE_SNAPSHOT_MISMATCH
  change root .dockerignore after source_sha256 is recorded
    -> OPERATOR_SOURCE_SNAPSHOT_MISMATCH
  ```

  Assert the named code is present rather than asserting it is the only issue.
  Every case must produce `report.valid is False`. These are checker tests, not
  a second wrapper-only validator. The fixture initializes its own temporary Git
  repository and requires no network, Docker, browser, ambient checkout state,
  or pre-existing Slice 1 output.

- [ ] **Step 3: Run the evidence tests and verify red**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider \
    tests/research/test_operator_evidence.py \
    tests/research/test_checker.py \
    tests/research/test_evidence.py \
    -k operator -q
  ```

  Expected: operator cases fail because the wrapper, manifest branch, and
  checker support are absent.

- [ ] **Step 4: Define the evidence Playwright configuration**

  Extend the strict deterministic config:

  ```javascript
  const path = require('node:path')
  const { defineConfig } = require('@playwright/test')
  const base = require('./playwright.config')

  const evidenceDir = process.env.EVIDENCE_DIR
  if (!evidenceDir) throw new Error('EVIDENCE_DIR is required')

  module.exports = defineConfig(Object.assign({}, base, {
    testMatch: ['operator-journey.spec.js'],
    workers: 1,
    retries: 0,
    reporter: [
      ['list'],
      ['json', {
        outputFile: path.join(evidenceDir, 'playwright-results.json'),
      }],
    ],
    use: Object.assign({}, base.use, {
      trace: 'on',
      video: 'on',
      screenshot: 'off',
    }),
    projects: [
      {
        name: 'desktop',
        use: {
          browserName: 'chromium',
          viewport: { width: 1440, height: 900 },
        },
      },
      {
        name: 'mobile',
        use: {
          browserName: 'chromium',
          viewport: { width: 390, height: 844 },
        },
      },
    ],
  }))
  ```

  `Object.assign` copies the complete exported base Playwright config before
  overriding evidence-only values.

- [ ] **Step 5: Capture explicit paired screenshots and project snapshots**

  `evidence-artifacts.js` implements the function with no shared mutable task
  state:

  ```javascript
  const fs = require('node:fs/promises')
  const path = require('node:path')
  const { expect } = require('@playwright/test')
  const {
    assertNoOverlap,
    canonicalJson,
  } = require('./operator-journey')

  async function readJson(response, label) {
    if (!response.ok()) {
      throw new Error(`${label} returned ${response.status()}`)
    }
    return response.json()
  }

  async function writeCanonical(filePath, value) {
    await fs.mkdir(path.dirname(filePath), { recursive: true })
    await fs.writeFile(filePath, `${canonicalJson(value)}\n`, 'utf8')
  }

  async function captureProjectEvidence({
    page,
    request,
    testInfo,
    aggUrl,
    metadata,
  }) {
    const project = testInfo.project.name
    if (!['desktop', 'mobile'].includes(project)) {
      throw new Error(`unsupported evidence project ${project}`)
    }
    const evidenceDir = process.env.EVIDENCE_DIR
    if (!evidenceDir) throw new Error('EVIDENCE_DIR is required')
    if (metadata.project !== project) {
      throw new Error('journey metadata project does not match Playwright')
    }
    const taskId = metadata.task_id
    const nonce = metadata.nonce
    const projectDir = path.join(
      evidenceDir,
      'raw',
      'playwright',
      project,
    )
    const apiDir = path.join(evidenceDir, 'raw', 'api', project)
    await fs.mkdir(projectDir, { recursive: true })
    await fs.mkdir(apiDir, { recursive: true })

    const chatTab = page.getByRole('button', { name: /^Chat/ })
    const target = page.getByLabel('Command target')
    const selectedStatus = page.getByRole('status', {
      name: 'Selected agent shell-1: online',
    })
    const command = page.locator(
      `[data-task-id="${taskId}"][data-message-type="command"]`,
    )
    const result = page.locator(
      `[data-task-id="${taskId}"][data-message-type="result"]`,
    )
    const tracking = page.getByText(
      `Tracking task: ${taskId}`,
      { exact: true },
    )
    await expect(chatTab).toHaveAttribute('aria-current', 'page')
    await expect(target).toHaveValue('shell-1')
    await expect(selectedStatus).toBeVisible()
    await expect(command).toContainText(nonce)
    await expect(result).toContainText(`edgecitadel:${nonce}`)
    await expect(tracking).toBeVisible()
    await assertNoOverlap([
      target,
      selectedStatus,
      command,
      tracking,
      result,
    ])

    const chatPath = path.join(projectDir, 'chat.png')
    await page.screenshot({ path: chatPath, fullPage: false })

    await page.getByRole('button', { name: /^Tasks/ }).click()
    const completed = page.getByText('Completed', { exact: true })
    const taskCard = page.locator(
      `[data-task-id="${taskId}"][data-task-state="completed"]`,
    )
    await expect(completed).toBeVisible()
    await expect(taskCard).toBeVisible()
    await assertNoOverlap([
      page.getByRole('button', { name: /^Tasks/ }),
      completed,
      taskCard,
    ])
    const tasksPath = path.join(projectDir, 'tasks.png')
    await page.screenshot({ path: tasksPath, fullPage: false })

    const snapshots = {
      'system-status.json': await readJson(
        await request.get(`${aggUrl}/api/system/status`),
        'system status',
      ),
      'registry.json': await readJson(
        await request.get(`${aggUrl}/api/registry`),
        'registry',
      ),
      'messages.json': await readJson(
        await request.get(
          `${aggUrl}/api/messages?task_id=${encodeURIComponent(taskId)}`,
        ),
        'messages',
      ),
      'queue.json': await readJson(
        await request.get(`${aggUrl}/api/agents/shell-1/queue`),
        'queue',
      ),
    }
    const messageTaskIds = new Set(
      snapshots['messages.json'].map((row) => row.task_id),
    )
    if (messageTaskIds.size !== 1 || !messageTaskIds.has(taskId)) {
      throw new Error(`${project} API messages do not match ${taskId}`)
    }
    for (const entry of Object.entries(snapshots)) {
      await writeCanonical(path.join(apiDir, entry[0]), entry[1])
    }

    const projectMetadata = {
      ...metadata,
      api_directory: path.relative(evidenceDir, apiDir),
      chat_screenshot: path.relative(evidenceDir, chatPath),
      tasks_screenshot: path.relative(evidenceDir, tasksPath),
    }
    const metadataPath = path.join(
      projectDir,
      'operator-metadata.json',
    )
    await writeCanonical(metadataPath, projectMetadata)
    await testInfo.attach('chat', {
      path: chatPath,
      contentType: 'image/png',
    })
    await testInfo.attach('tasks', {
      path: tasksPath,
      contentType: 'image/png',
    })
    await testInfo.attach('operator-metadata', {
      path: metadataPath,
      contentType: 'application/json',
    })
    return projectMetadata
  }

  module.exports = { captureProjectEvidence }
  ```

  In `operator-journey.spec.js`, build the complete Task 6 `metadata` object
  once. For ordinary runs, write/attach it as before. For evidence runs, pass
  that same object to the helper instead of constructing a reduced replacement.
  Call the helper after the Chat tab is selected and immediately before the
  final error-list assertion:

  ```javascript
  const {
    captureProjectEvidence,
  } = require('../helpers/evidence-artifacts')

  if (process.env.EVIDENCE_DIR) {
    await captureProjectEvidence({
      page,
      request,
      testInfo,
      aggUrl: AGG_URL,
      metadata,
    })
  }
  expect(errors).toEqual([])
  ```

  The function satisfies these assertions:

  ```text
  project = testInfo.project.name and must be desktop or mobile.
  projectDir = EVIDENCE_DIR/raw/playwright/project.
  apiDir = EVIDENCE_DIR/raw/api/project.
  Assert Chat is selected.
  Assert Command target value is shell-1.
  Assert Selected agent shell-1: online is visible.
  Assert command bubble data-task-id=taskId contains nonce.
  Assert result bubble data-task-id=taskId contains edgecitadel:nonce.
  Assert the full tracking task ID is visible.
  Run assertNoOverlap on target, selected-agent status, command, tracking ID,
  and result.
  Write projectDir/chat.png at the configured viewport.
  Select Tasks and assert task card taskId/completed is visible.
  Run assertNoOverlap on tab bar, completed heading, and task card.
  Write projectDir/tasks.png at the configured viewport.
  Fetch and canonically write system status, registry, task messages, and queue.
  Write operator-metadata.json with the full context, hop, endpoint identity,
  envelope ID, and observation-index mapping from the journey.
  Attach chat, tasks, and metadata to the Playwright result.
  Playwright attaches one automatic trace and video to this project's result.
  ```

  On mobile, the journey unconditionally opens `Open agent list`, requires the
  `shell-1` row to enter the viewport, selects it, and requires the row to leave
  the viewport as the overlay closes before capture. Do not use `isVisible()` for
  this decision: Playwright considers a transformed off-canvas element visible.
  The compact `shell-1 online` status and selected `Command target` are both
  visible selected-agent evidence; do not leave the overlay over the chat. The
  two screenshots intentionally separate Chat provenance/output
  from Task state. No test demands that both tabs be visible in one impossible
  frame.

- [ ] **Step 6: Define the operator manifest branch and checker**

  Preserve the Slice 1 top-level fields and record operator-specific provenance
  in this shape:

  ```json
  {
    "schema_version": "research-manifest.v1",
    "evidence_kind": "operator",
    "run_id": "launcher-generated-run-id",
    "status": "PASS",
    "source": {
      "commit": "40-or-64-lowercase-hex-characters",
      "git_dirty": false,
      "source_sha256": "64-lowercase-hex-characters",
      "paths": [
        ".dockerignore",
        "<every remaining Slice 1 source path in lexical order>"
      ]
    },
    "images": {
      "all": [
        {
          "service": "nats",
          "reference": "nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927",
          "image_id": "sha256:value"
        }
      ],
      "owned_build_references": [
        {
          "service": "backend",
          "reference": "edgecitadel-e2e-RUN-backend:latest",
          "image_id": "sha256:value"
        }
      ]
    },
    "cleanup": {
      "valid": true,
      "resources": {
        "containers": [],
        "networks": [],
        "volumes": [],
        "owned_build_images": []
      }
    },
    "projects": {
      "desktop": {
        "task_id": "11111111-1111-4111-8111-111111111111",
        "nonce": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "command_body": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "expected_output": "edgecitadel:dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "context_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "hop_count": 0,
        "command_envelope_id": "11111111-aaaa-4111-8111-111111111111",
        "terminal_envelope_id": "11111111-bbbb-4111-8111-111111111111",
        "progress_envelope_ids": [
          "11111111-cccc-4111-8111-111111111111"
        ],
        "command_sender_id": "aggregator",
        "command_recipient_id": "shell-1",
        "terminal_sender_id": "shell-1",
        "terminal_recipient_id": "aggregator",
        "browser_name": "chromium",
        "browser_version": "captured-browser-version",
        "command_observation_index": 1,
        "progress_observation_indices": [2],
        "terminal_observation_index": 3
      },
      "mobile": {
        "task_id": "22222222-2222-4222-8222-222222222222",
        "nonce": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "command_body": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "expected_output": "edgecitadel:eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "context_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "hop_count": 0,
        "command_envelope_id": "22222222-aaaa-4222-8222-222222222222",
        "terminal_envelope_id": "22222222-bbbb-4222-8222-222222222222",
        "progress_envelope_ids": [
          "22222222-cccc-4222-8222-222222222222"
        ],
        "command_sender_id": "aggregator",
        "command_recipient_id": "shell-1",
        "terminal_sender_id": "shell-1",
        "terminal_recipient_id": "aggregator",
        "browser_name": "chromium",
        "browser_version": "captured-browser-version",
        "command_observation_index": 4,
        "progress_observation_indices": [5],
        "terminal_observation_index": 6
      }
    }
  }
  ```

  The angle-bracket `paths` entry is display-only and must be replaced by the
  complete file-level tuple returned by `SourceProvenance.to_dict()`; it is
  never written to a real manifest.

  Retain the Slice 1 requirements for `command`, `timing`, `host`,
  `dependencies`, `compose_config_sha256`, `schemas`, and `artifacts`. Add this
  conditional fragment to `research-manifest.v1.json`; use the schema's existing
  64-hex definition where available:

  ```json
  {
    "$defs": {
      "operatorProject": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "task_id",
          "nonce",
          "command_body",
          "expected_output",
          "context_id",
          "hop_count",
          "command_envelope_id",
          "terminal_envelope_id",
          "progress_envelope_ids",
          "command_sender_id",
          "command_recipient_id",
          "terminal_sender_id",
          "terminal_recipient_id",
          "browser_name",
          "browser_version",
          "command_observation_index",
          "progress_observation_indices",
          "terminal_observation_index"
        ],
        "properties": {
          "task_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
          },
          "nonce": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
          },
          "command_body": { "type": "string", "minLength": 1 },
          "expected_output": { "type": "string", "minLength": 1 },
          "context_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
          },
          "hop_count": { "const": 0 },
          "command_envelope_id": { "type": "string", "minLength": 1 },
          "terminal_envelope_id": { "type": "string", "minLength": 1 },
          "progress_envelope_ids": {
            "type": "array",
            "minItems": 1,
            "items": { "type": "string", "minLength": 1 }
          },
          "command_sender_id": { "const": "aggregator" },
          "command_recipient_id": { "const": "shell-1" },
          "terminal_sender_id": { "const": "shell-1" },
          "terminal_recipient_id": { "const": "aggregator" },
          "browser_name": { "const": "chromium" },
          "browser_version": { "type": "string", "minLength": 1 },
          "command_observation_index": {
            "type": "integer",
            "minimum": 1
          },
          "progress_observation_indices": {
            "type": "array",
            "items": { "type": "integer", "minimum": 1 }
          },
          "terminal_observation_index": {
            "type": "integer",
            "minimum": 1
          }
        }
      }
    },
    "allOf": [
      {
        "if": {
          "properties": {
            "evidence_kind": { "const": "operator" }
          },
          "required": ["evidence_kind"]
        },
        "then": {
          "required": ["projects"],
          "properties": {
            "projects": {
              "type": "object",
              "additionalProperties": false,
              "required": ["desktop", "mobile"],
              "properties": {
                "desktop": { "$ref": "#/$defs/operatorProject" },
                "mobile": { "$ref": "#/$defs/operatorProject" }
              }
            }
          }
        }
      }
    ]
  }
  ```

  JSON Schema cannot compare sibling values, so distinct task IDs, exact
  `command_body == nonce`, and exact
  `expected_output == "edgecitadel:" + nonce` are checker rules. The schema
  still requires exactly `desktop` and `mobile`, one stack run ID, cleanup
  shape, git commit/dirty flag, source hashes, sanitized Compose hash, all-image
  provenance, owned-build references, tool versions, OS, architecture, argv,
  UTC start/end times, and every non-manifest artifact hash.

  Reuse Slice 1's exact `SourceProvenance`,
  `capture_source_provenance`, and `verify_source_provenance` APIs from
  `scripts/research/evidence.py`; do not introduce an operator-only source hash
  or alternate field names. The manifest therefore carries exactly `commit`,
  `git_dirty`, `source_sha256`, and `paths`.

  Slice 1's source set is the sorted repository-wide
  `git ls-files --cached --others --exclude-standard` result, excluding only the
  four declared result roots, `tmp/`, and generated build/test output. This set
  necessarily includes root `.dockerignore`, which controls the root-context
  backend and fixture image builds. Add a focused regression that mutates
  `.dockerignore` after capture and proves `verify_source_provenance` fails.

  Extend the existing Slice 1 `check_bundle` dispatcher, rather than creating a
  wrapper-only validator:

  ```python
  OPERATOR_PROJECTS = ("desktop", "mobile")
  TERMINAL_STATES = {"completed", "failed", "canceled", "rejected"}


  def _operator_issues(
      bundle: Path,
      manifest: Mapping[str, object],
      source_root: Path | None,
  ) -> list[ArtifactIssue]:
      issues: list[ArtifactIssue] = []

      def reject(code: str, relative: str, message: str) -> None:
          issues.append(ArtifactIssue(code, relative, message))

      projects = manifest["projects"]
      task_ids = [projects[name]["task_id"] for name in OPERATOR_PROJECTS]
      if len(set(task_ids)) != 2:
          reject(
              "OPERATOR_TASK_IDS_NOT_DISTINCT",
              "manifest.json",
              "desktop and mobile task IDs must differ",
          )

      for name in OPERATOR_PROJECTS:
          expected = projects[name]
          if expected["command_body"] != expected["nonce"]:
              reject(
                  "OPERATOR_COMMAND_BODY_MISMATCH",
                  "manifest.json",
                  f"{name} command body differs from nonce",
              )
          if expected["expected_output"] != (
              f"edgecitadel:{expected['nonce']}"
          ):
              reject(
                  "OPERATOR_OUTPUT_MISMATCH",
                  "manifest.json",
                  f"{name} expected output is not deterministic",
              )

          project_root = Path("raw/playwright") / name
          api_root = Path("raw/api") / name
          required = (
              project_root / "chat.png",
              project_root / "tasks.png",
              project_root / "video.webm",
              project_root / "trace.zip",
              project_root / "operator-metadata.json",
              api_root / "system-status.json",
              api_root / "registry.json",
              api_root / "messages.json",
              api_root / "queue.json",
          )
          for relative in required:
              if not (bundle / relative).is_file():
                  reject(
                      "OPERATOR_ARTIFACT_MISSING",
                      relative.as_posix(),
                      "required operator artifact is absent",
                  )
          if any(not (bundle / relative).is_file() for relative in required):
              continue

          metadata = json.loads(
              (bundle / project_root / "operator-metadata.json")
              .read_text()
          )
          for key in (
              "task_id",
              "nonce",
              "command_body",
              "expected_output",
              "context_id",
              "hop_count",
              "command_envelope_id",
              "terminal_envelope_id",
              "progress_envelope_ids",
              "command_sender_id",
              "command_recipient_id",
              "terminal_sender_id",
              "terminal_recipient_id",
              "browser_name",
              "browser_version",
              "command_observation_index",
              "progress_observation_indices",
              "terminal_observation_index",
          ):
              if metadata.get(key) != expected[key]:
                  reject(
                      "OPERATOR_METADATA_MISMATCH",
                      (project_root / "operator-metadata.json").as_posix(),
                      f"{name} {key} does not match manifest",
                  )

          messages = json.loads(
              (bundle / api_root / "messages.json").read_text()
          )
          if any(row.get("task_id") != expected["task_id"]
                 for row in messages):
              reject(
                  "OPERATOR_CROSS_PROJECT_TASK",
                  (api_root / "messages.json").as_posix(),
                  f"{name} contains another project's task",
              )
          commands = [
              row for row in messages if row.get("type") == "command"
          ]
          terminals = [
              row for row in messages
              if row.get("type") == "result"
              and row.get("task_state") in TERMINAL_STATES
          ]
          if len(commands) != 1 or (
              commands[0].get("payload", {}).get("body")
              != expected["command_body"]
          ):
              reject(
                  "OPERATOR_COMMAND_COUNT_OR_BODY",
                  (api_root / "messages.json").as_posix(),
                  f"{name} must contain one exact command",
              )
          if len(terminals) != 1 or (
              terminals[0].get("task_state") != "completed"
              or terminals[0].get("payload", {}).get("body")
              != expected["expected_output"]
          ):
              reject(
                  "OPERATOR_TERMINAL_COUNT_OR_BODY",
                  (api_root / "messages.json").as_posix(),
                  f"{name} must contain one exact completed result",
              )
          if len(commands) == 1:
              context_id = commands[0].get("context_id")
              hop_count = commands[0].get("hop_count")
              correlated = [
                  row for row in messages
                  if row.get("type") in {"task.progress", "result"}
              ]
              if not context_id or hop_count != 0 or any(
                  row.get("context_id") != context_id
                  or row.get("hop_count") != hop_count
                  for row in correlated
              ):
                  reject(
                      "OPERATOR_CORRELATION_MISMATCH",
                      (api_root / "messages.json").as_posix(),
                      f"{name} task correlation is not preserved",
                  )

          status = json.loads(
              (bundle / api_root / "system-status.json").read_text()
          )
          if status.get("nats_connected") is not True or (
              status.get("jetstream_stream_ok") is not True
          ):
              reject(
                  "OPERATOR_SYSTEM_UNHEALTHY",
                  (api_root / "system-status.json").as_posix(),
                  f"{name} system status is unhealthy",
              )
          registry = json.loads(
              (bundle / api_root / "registry.json").read_text()
          )
          shell = [
              row for row in registry if row.get("agent_id") == "shell-1"
          ]
          if len(shell) != 1 or shell[0].get("agent_state") != "online":
              reject(
                  "OPERATOR_SHELL_NOT_ONLINE",
                  (api_root / "registry.json").as_posix(),
                  f"{name} lacks one online shell-1",
              )
          elif (
              shell[0].get("card", {}).get("metadata", {})
              .get("runtime.conformance") != "L1"
          ):
              reject(
                  "OPERATOR_CONFORMANCE_MISMATCH",
                  (api_root / "registry.json").as_posix(),
                  f"{name} shell-1 is not L1",
              )
          queue = json.loads(
              (bundle / api_root / "queue.json").read_text()
          )
          if queue.get("pending") != 0 or queue.get("ack_pending") != 0:
              reject(
                  "OPERATOR_QUEUE_NOT_DRAINED",
                  (api_root / "queue.json").as_posix(),
                  f"{name} queue is not drained",
              )

      cleanup = manifest["cleanup"]
      for resource in (
          "containers",
          "networks",
          "volumes",
          "owned_build_images",
      ):
          if cleanup["resources"].get(resource) != []:
              reject(
                  "OPERATOR_CLEANUP_RESIDUE",
                  "raw/runtime/cleanup.json",
                  f"cleanup left {resource}",
              )
      if cleanup.get("valid") is not True:
          reject(
              "OPERATOR_CLEANUP_INVALID",
              "raw/runtime/cleanup.json",
              "launcher cleanup is invalid",
          )

      if source_root is None:
          reject(
              "OPERATOR_SOURCE_ROOT_REQUIRED",
              "manifest.json",
              "operator source verification requires source_root",
          )
      else:
          actual_source = capture_source_provenance(source_root)
          expected_source = manifest["source"]
          if actual_source.commit != expected_source["commit"]:
              reject(
                  "OPERATOR_SOURCE_COMMIT_MISMATCH",
                  "manifest.json",
                  "source HEAD differs from capture commit",
              )
          if actual_source.git_dirty:
              reject(
                  "OPERATOR_SOURCE_DIRTY",
                  "manifest.json",
                  "operator source paths are dirty",
              )
          if (
              actual_source.source_sha256 != expected_source["source_sha256"]
              or list(actual_source.paths) != expected_source["paths"]
          ):
              reject(
                  "OPERATOR_SOURCE_SNAPSHOT_MISMATCH",
                  "manifest.json",
                  "relevant source differs from capture source",
              )
      return issues
  ```

  The shown loop is not the whole operator checker. `_operator_issues` must also
  be self-contained and enforce all of these rules with stable issue codes:

  ```text
  Require playwright-results.json, raw/runtime/launcher-summary.json, and
  raw/runtime/cleanup.json in addition to every per-project artifact.
  Parse all JSON locally and report malformed JSON as OPERATOR_JSON_INVALID.
  Require exactly one passed desktop result and one passed mobile result, no
  failed/skipped/retried result, using
  schema_version=playwright-operator-results.v1 and exactly the desktop/mobile
  project keys; retry must be zero and duration_ms a positive integer. Require
  exact relative attachment paths and MIME types for each
  project's chat, tasks, metadata, video, and trace files.
  Require every attachment path to be bundle-relative, normalized, and confined
  beneath raw/playwright/<project>; reject absolute paths or "..".
  Compare runtime run_id, images, cleanup, timing, normalized Compose text/hash,
  and project name with the manifest; require the two cleanup JSON copies to
  equal each other byte-for-byte after canonical parsing.
  Require command.argv to be a flat string list with only $SOURCE_ROOT and
  $EVIDENCE_DIR path placeholders. Require the exact dependency keys python,
  node, npm, git, docker_client, docker_server, docker_compose, playwright,
  chromium, ffmpeg, and ffprobe; every value is nonempty and chromium equals
  both project metadata versions.
  Require runtime URLs and Compose config to contain only documented container
  endpoints or <loopback-port>/<run-owned-path>/<generated-per-run-token>
  placeholders, never capture-machine absolute paths, ports, or credentials.
  Require source_root HEAD to equal manifest.source.commit, require the Slice 1
  source paths to be clean, and then compare source_sha256 and the exact ordered
  paths list; use
  OPERATOR_SOURCE_COMMIT_MISMATCH, OPERATOR_SOURCE_DIRTY, and
  OPERATOR_SOURCE_SNAPSHOT_MISMATCH respectively.
  Require manifest.source.git_dirty=false and require `.dockerignore` in
  manifest.source.paths.
  Require task_id, nonce, and context_id to be canonical UUIDv4.
  Require operator-metadata.project to equal its containing desktop/mobile
  directory and require every remaining manifest project field to match it.
  Require command identity aggregator -> shell-1, terminal/progress identity
  shell-1 -> aggregator, hop_count=0, and exact metadata/API envelope IDs.
  Require unique positive observation indices ordered command < every progress
  < terminal, and require metadata's progress index array to equal the API rows.
  Require one completed terminal, exact bodies, and no conflicting terminal.
  Require exactly one online shell-1 whose runtime.kind is native,
  runtime.roles contains worker, runtime.conformance is L1, and whose extensions
  contain https://edgecitadel.local/ext/nats-binding/v1.
  Open each trace.zip with zipfile, reject unsafe members, concatenate bounded
  text/JSON members, and require that project's task_id, nonce, and expected
  output.
  Require each PNG, WebM, and trace to be nonempty and uniquely owned by one
  Playwright project result. Base artifact hashing remains authoritative for
  byte integrity.
  ```

  Implement these rules in `check_artifact.py`, not in the capture wrapper.
  The wrapper may fail early for operator convenience, but every semantic
  corruption test must call the public checker directly and receive the same
  result without relying on wrapper state.

  Call `_operator_issues` only after the existing base checker has validated the
  manifest schema, artifact hashes, and secret scan. Preserve the Slice 1
  positional call by keeping both existing keywords optional:

  ```python
  def check_bundle(
      path: Path,
      *,
      expected_kind: Literal["benchmark", "operator", "lab"] | None = None,
      source_root: Path | None = None,
  ) -> CheckReport:
      manifest = load_and_validate_base_bundle(path)
      actual_kind = manifest["evidence_kind"]
      issues = list(base_issues(path, manifest))
      if expected_kind is not None and expected_kind != actual_kind:
          issues.append(ArtifactIssue(
              "ARTIFACT_KIND_MISMATCH",
              "manifest.json",
              f"expected {expected_kind}, found {actual_kind}",
          ))
      if actual_kind == "operator":
          issues.extend(_operator_issues(path, manifest, source_root))
      return CheckReport(valid=not issues, issues=tuple(issues))
  ```

  `load_and_validate_base_bundle`, `base_issues`, and the actual `CheckReport`
  constructor names should match Slice 1's implementation; the compatibility
  behavior and stable issue code are normative. Add regression tests that
  `check_bundle(benchmark_bundle)` still works, that explicit matching
  `expected_kind` works, and that an operator call without `source_root` returns
  `OPERATOR_SOURCE_ROOT_REQUIRED`.

  `CheckReport.valid` is false when any issue exists, `issues` is a tuple of
  stable `ArtifactIssue` values, and `require_valid()` raises when invalid.
  These checks run only after `finalize_bundle`.

- [ ] **Step 7: Implement the capture wrapper**

  Public CLI:

  ```bash
  scripts/research/run-python scripts/research/capture_operator_journey.py \
    --output-root /absolute/path/to/docs/research/results/operator \
    --source-root /absolute/path/to/clean-checkout
  ```

  Implement these concrete helpers in
  `scripts/research/capture_operator_journey.py`:

  ```python
  import argparse
  import hashlib
  import json
  import os
  import platform
  import shutil
  import subprocess
  from datetime import UTC, datetime
  from pathlib import Path
  from typing import Iterable, Mapping

  from scripts.research.check_artifact import check_bundle
  from scripts.research.evidence import (
      SourceProvenance,
      capture_source_provenance,
      finalize_bundle,
      verify_source_provenance,
      write_json,
  )

  PROJECTS = ("desktop", "mobile")
  SCHEMA_PATH = Path("schemas/research-manifest.v1.json")


  def tool_version(
      argv: list[str],
      *,
      cwd: Path | None = None,
  ) -> str:
      completed = subprocess.run(
          argv,
          cwd=cwd,
          check=True,
          capture_output=True,
          text=True,
      )
      lines = (completed.stdout or completed.stderr).splitlines()
      if not lines:
          raise RuntimeError(f"empty version output from {argv[0]}")
      return lines[0].strip()


  def source_provenance(source_root: Path) -> SourceProvenance:
      source = capture_source_provenance(source_root)
      if source.git_dirty:
          raise RuntimeError("operator capture source must be clean")
      if ".dockerignore" not in source.paths:
          raise RuntimeError("operator source must include .dockerignore")
      return source


  def iter_report_tests(
      suites: Iterable[Mapping[str, object]],
  ) -> Iterable[Mapping[str, object]]:
      for suite in suites:
          yield from iter_report_tests(suite.get("suites", []))
          for spec in suite.get("specs", []):
              yield from spec.get("tests", [])


  def passed_project_results(
      report: Mapping[str, object],
  ) -> dict[str, Mapping[str, object]]:
      selected: dict[str, Mapping[str, object]] = {}
      for test_case in iter_report_tests(report.get("suites", [])):
          project = test_case.get("projectName")
          if project not in PROJECTS:
              continue
          project_results = test_case.get("results", [])
          if (
              len(project_results) != 1
              or project_results[0].get("status") != "passed"
              or project_results[0].get("retry", 0) != 0
              or project in selected
          ):
              raise RuntimeError(
                  f"expected one passed result for {project}"
              )
          selected[project] = project_results[0]
      if set(selected) != set(PROJECTS):
          raise RuntimeError("desktop and mobile results are required")
      return selected


  def copy_media(
      source_root: Path,
      bundle: Path,
      results: Mapping[str, Mapping[str, object]],
  ) -> dict[str, object]:
      portable: dict[str, object] = {}
      for project in PROJECTS:
          attachments = results[project].get("attachments", [])
          by_name: dict[str, Mapping[str, object]] = {}
          for required_name in (
              "chat",
              "tasks",
              "operator-metadata",
              "video",
              "trace",
          ):
              matching = [
                  attachment for attachment in attachments
                  if attachment.get("name") == required_name
                  and attachment.get("path")
              ]
              if len(matching) != 1:
                  raise RuntimeError(
                      f"{project} requires one {required_name} attachment"
                  )
              by_name[required_name] = matching[0]
          for name, destination in (
              ("chat", "chat.png"),
              ("tasks", "tasks.png"),
              ("operator-metadata", "operator-metadata.json"),
          ):
              attached = Path(by_name[name]["path"])
              if not attached.is_absolute():
                  attached = source_root / "e2e" / attached
              expected = (
                  bundle / "raw" / "playwright" / project / destination
              )
              if attached.resolve() != expected.resolve():
                  raise RuntimeError(
                      f"{project} {name} attachment points elsewhere"
                  )
          for name, destination in (
              ("video", "video.webm"),
              ("trace", "trace.zip"),
          ):
              attachment = by_name.get(name)
              if attachment is None:
                  raise RuntimeError(
                      f"{project} is missing its {name} attachment"
                  )
              source = Path(attachment["path"])
              if not source.is_absolute():
                  source = source_root / "e2e" / source
              target = (
                  bundle / "raw" / "playwright" / project / destination
              )
              target.parent.mkdir(parents=True, exist_ok=True)
              shutil.copyfile(source, target)
          portable[project] = {
              "project": project,
              "title": "operator observes one deterministic task lifecycle",
              "status": "passed",
              "retry": results[project].get("retry", 0),
              "duration_ms": results[project].get("duration"),
              "attachments": [
                  {
                      "name": name,
                      "path": (
                          Path("raw/playwright")
                          / project
                          / destination
                      ).as_posix(),
                      "content_type": content_type,
                  }
                  for name, destination, content_type in (
                      ("chat", "chat.png", "image/png"),
                      ("tasks", "tasks.png", "image/png"),
                      (
                          "operator-metadata",
                          "operator-metadata.json",
                          "application/json",
                      ),
                      ("video", "video.webm", "video/webm"),
                      ("trace", "trace.zip", "application/zip"),
                  )
              ],
          }
      return {
          "schema_version": "playwright-operator-results.v1",
          "projects": portable,
      }


  def require_exact_artifacts(bundle: Path) -> None:
      expected_counts = {
          "png": 4,
          "webm": 2,
          "trace.zip": 2,
          "api-json": 8,
          "metadata-json": 2,
          "playwright-json": 1,
          "runtime-json": 2,
      }
      actual_counts = {
          "png": len(list(
              (bundle / "raw/playwright").glob("*/*.png")
          )),
          "webm": len(list(
              (bundle / "raw/playwright").glob("*/*.webm")
          )),
          "trace.zip": len(list(
              (bundle / "raw/playwright").glob("*/trace.zip")
          )),
          "api-json": len(list(
              (bundle / "raw/api").glob("*/*.json")
          )),
          "metadata-json": len(list(
              (bundle / "raw/playwright")
              .glob("*/operator-metadata.json")
          )),
          "playwright-json": int(
              (bundle / "playwright-results.json").is_file()
          ),
          "runtime-json": len(list(
              (bundle / "raw/runtime").glob("*.json")
          )),
      }
      if actual_counts != expected_counts:
          raise RuntimeError(
              f"unexpected evidence counts: {actual_counts}"
          )


  def require_cleanup(runtime: Mapping[str, object]) -> None:
      cleanup = runtime["cleanup"]
      if cleanup.get("valid") is not True:
          raise RuntimeError("launcher cleanup is invalid")
      for name in (
          "containers",
          "networks",
          "volumes",
          "owned_build_images",
      ):
          if cleanup["resources"].get(name) != []:
              raise RuntimeError(f"launcher left {name}")
      if runtime.get("run_directory") != "<run-owned-path>":
          raise RuntimeError("runtime directory was not normalized")
      if runtime.get("scratch_removed") is not True:
          raise RuntimeError("credential scratch directory was not removed")
  ```

  Before writing external runtime evidence, `OwnedStack.cleanup` must delete the
  scratch tree, set `scratch_removed=true`, replace `run_dir` with
  `run_directory="<run-owned-path>"`, and then persist the external sanitized
  copies. Normalize the Compose text by replacing, longest value first, the
  source checkout, run directory, fixture config, credential, control, and
  evidence directories with `$SOURCE_ROOT`, `<run-owned-path>`,
  `<fixture-config>`, `<credential-file>`, `<control-dir>`, and `$EVIDENCE_DIR`.
  Replace the token with `<generated-per-run-token>` and each resolved loopback
  port in runtime URLs with `<loopback-port:service>`. The in-memory pre-cleanup
  summary may contain live values; neither external JSON file may.

  After replacing `playwright-results.json` with `portable_report`, recursively
  scan every retained JSON string. Reject strings beginning with `/`, containing
  the source checkout, temporary/capture/run directory, or matching a Windows
  absolute path. The only path-bearing values allowed are bundle-relative paths,
  `$SOURCE_ROOT/...`, `$EVIDENCE_DIR/...`, and documented angle-bracket
  placeholders. Tests construct source and output roots containing spaces and
  assert no absolute root survives in the bundle.

  Then implement one capture function with this control flow:

  ```python
  def capture_operator_journey(
      output_root: Path,
      source_root: Path,
      *,
      runner=subprocess.run,
  ) -> Path:
      output_root = output_root.resolve()
      source_root = source_root.resolve()
      before = source_provenance(source_root)
      stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
      bundle = output_root / f"{stamp}-{before.commit[:12]}"
      bundle.mkdir(parents=True, exist_ok=False)
      runtime_dir = bundle / "raw/runtime"
      runtime_dir.mkdir(parents=True)
      launcher_argv = [
          "node",
          str(source_root / "e2e/run-isolated.js"),
          "--config",
          str(source_root / "e2e/playwright.evidence.config.js"),
          "--evidence-runtime-dir",
          str(runtime_dir),
      ]
      environment = dict(os.environ)
      environment["EVIDENCE_DIR"] = str(bundle)

      try:
          completed = runner(
              launcher_argv,
              cwd=source_root / "e2e",
              env=environment,
              check=False,
              capture_output=True,
              text=True,
          )
          if completed.returncode != 0:
              raise RuntimeError(
                  f"evidence launcher failed: {completed.stderr}"
              )

          report_path = bundle / "playwright-results.json"
          report = json.loads(report_path.read_text())
          results = passed_project_results(report)
          portable_report = copy_media(source_root, bundle, results)
          write_json(report_path, portable_report)

          metadata = {
              project: json.loads(
                  (
                      bundle
                      / "raw/playwright"
                      / project
                      / "operator-metadata.json"
                  ).read_text()
              )
              for project in PROJECTS
          }
          if len({
              metadata[project]["task_id"] for project in PROJECTS
          }) != 2:
              raise RuntimeError(
                  "desktop and mobile task IDs must be distinct"
              )
          if len({
              metadata[project]["browser_version"]
              for project in PROJECTS
          }) != 1:
              raise RuntimeError(
                  "desktop and mobile Chromium versions must match"
              )

          runtime = json.loads(
              (runtime_dir / "launcher-summary.json").read_text()
          )
          cleanup = json.loads(
              (runtime_dir / "cleanup.json").read_text()
          )
          if runtime["cleanup"] != cleanup:
              raise RuntimeError("runtime cleanup copies disagree")
          require_cleanup(runtime)
          require_exact_artifacts(bundle)

          if not verify_source_provenance(source_root, before):
              raise RuntimeError("source changed during evidence capture")

          compose_text = runtime["compose_config"].encode()
          recorded_argv = [
              "node",
              "$SOURCE_ROOT/e2e/run-isolated.js",
              "--config",
              "$SOURCE_ROOT/e2e/playwright.evidence.config.js",
              "--evidence-runtime-dir",
              "$EVIDENCE_DIR/raw/runtime",
          ]
          manifest = {
              "schema_version": "research-manifest.v1",
              "evidence_kind": "operator",
              "status": "PASS",
              "run_id": runtime["run_id"],
              "source": before.to_dict(),
              "command": {"argv": recorded_argv},
              "timing": {
                  "started_at": runtime["started_at"],
                  "ended_at": runtime["completed_at"],
              },
              "host": {
                  "os": platform.system(),
                  "architecture": platform.machine(),
              },
              "dependencies": {
                  "python": f"Python {platform.python_version()}",
                  "node": tool_version(["node", "--version"]),
                  "npm": tool_version(["npm", "--version"]),
                  "git": tool_version(["git", "--version"]),
                  "docker_client": tool_version([
                      "docker", "version", "--format",
                      "{{.Client.Version}}",
                  ]),
                  "docker_server": tool_version([
                      "docker", "version", "--format",
                      "{{.Server.Version}}",
                  ]),
                  "docker_compose": tool_version([
                      "docker", "compose", "version", "--short",
                  ]),
                  "playwright": tool_version(
                      ["npx", "--no-install", "playwright", "--version"],
                      cwd=source_root / "e2e",
                  ),
                  "chromium":
                      metadata["desktop"]["browser_version"],
                  "ffmpeg": tool_version(["ffmpeg", "-version"]),
                  "ffprobe": tool_version(["ffprobe", "-version"]),
              },
              "images": runtime["images"],
              "compose_config_sha256":
                  hashlib.sha256(compose_text).hexdigest(),
              "schemas": {
                  "manifest": SCHEMA_PATH.as_posix(),
              },
              "cleanup": cleanup,
              "projects": {
                  project: {
                      key: metadata[project][key]
                      for key in (
                          "task_id",
                          "nonce",
                          "command_body",
                          "expected_output",
                          "context_id",
                          "hop_count",
                          "command_envelope_id",
                          "terminal_envelope_id",
                          "progress_envelope_ids",
                          "command_sender_id",
                          "command_recipient_id",
                          "terminal_sender_id",
                          "terminal_recipient_id",
                          "browser_name",
                          "browser_version",
                          "command_observation_index",
                          "progress_observation_indices",
                          "terminal_observation_index",
                      )
                  }
                  for project in PROJECTS
              },
              "artifacts": {},
          }
          schema_path = source_root / SCHEMA_PATH
          status = finalize_bundle(bundle, manifest, schema_path)
          if status != "PASS":
              raise RuntimeError(f"finalization returned {status}")
          report = check_bundle(
              bundle,
              expected_kind="operator",
              source_root=source_root,
          )
          report.require_valid()
      except BaseException:
          shutil.rmtree(bundle, ignore_errors=True)
          raise

      print(bundle.resolve())
      print("PASS")
      return bundle
  ```

  The CLI parses both required absolute paths and calls this function once.
  `run-isolated.js` copies only `launcher-summary.json` and `cleanup.json` under
  `raw/runtime/`; the sanitized summary contains Compose config, all image
  provenance, owned build references, normalized loopback endpoint placeholders,
  and run/project IDs.
  After copying them, the launcher overwrites and unlinks the credential and
  deletes its entire live scratch directory. The wrapper asserts that deletion
  before finalization.

  The recorded manifest argv is one flat string array, not a nested array, and
  uses `$SOURCE_ROOT`/`$EVIDENCE_DIR` placeholders. Dependency provenance is
  complete for Python, Node, npm, Git, Docker client/server/Compose, Playwright,
  the actual Chromium version reported by both projects, ffmpeg, and ffprobe.
  Require both projects to report the same Chromium version. Tests assert every
  version is nonempty and that no source, output, run, reporter, or attachment
  absolute path survives in any finalized JSON artifact.

- [ ] **Step 8: Run unit tests, cumulative gates, and red-green regression**

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider \
    tests/research/test_operator_evidence.py \
    tests/research/test_checker.py \
    tests/research/test_evidence.py \
    -k operator -q
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. scripts/research/run-python -m pytest \
    -p no:cacheprovider aggregator/tests tests/research -q
  scripts/research/run-python -m compileall -q aggregator scripts/research
  BARE_HOST_PYTHON="$(
    rg -n '(^|[[:space:]])python3?([[:space:]]|$)' \
      docs/research/plans/2026-07-25-slice-2-deterministic-operator-journey.md \
      | rg -v \
        'python3 -m scripts\.research\.fixtures\.native_control|:[[:space:]]+- python3$' \
      || true
  )"
  test -z "$BARE_HOST_PYTHON"
  EXPECTED_NATS_IMAGE="nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927"
  ACTUAL_NATS_IMAGE="$(
    scripts/research/run-python - <<'PY'
  import json
  from pathlib import Path

  print(json.loads(
      Path("scripts/research/toolchain.json").read_text()
  )["nats_image"])
  PY
  )"
  test "$ACTUAL_NATS_IMAGE" = "$EXPECTED_NATS_IMAGE"
  MUTABLE_NATS_REFS="$(
    rg -n 'nats:2[.]10-alpine' \
      aggregator e2e scripts tests docker-compose.yml .dockerignore \
      docs/research/plans/2026-07-25-slice-2-deterministic-operator-journey.md \
      || true
  )"
  test -z "$MUTABLE_NATS_REFS"
  cd /Users/yefanzhang/workplace/edge-research/frontend
  npm test
  npm run lint
  npm run build
  cd /Users/yefanzhang/workplace/edge-research/e2e
  npm run test:stack-unit
  npm run test:classification
  npm run test:stack-integration
  npm test
  ```

  Expected: operator corruption cases pass, all Python suites have zero
  failures, compilation is silent, the bare-host-Python scan is empty, frontend
  gates pass, 24 stack unit tests and
  one classification test pass, two lifecycle scenarios pass, and exactly
  13 deterministic Playwright tests pass. Invoke `verify-infra`.

- [ ] **Step 9: Commit all source and harness changes before capture**

  Run `commit-check`, stage only the eleven pre-capture Task 8
  source/test/schema/docs files, exclude
  `docs/research/task-aware-reliability-contract-design.md` and
  `docs/research/results/operator/`, verify the cached map, and commit:

  ```bash
  git commit -m "test(e2e): add operator evidence capture"
  ```

- [ ] **Step 10: Capture once from a clean detached worktree**

  Use a clean detached worktree so canonical-checkout edits never enter the
  image, source hash, or manifest:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  SOURCE_COMMIT="$(git rev-parse HEAD)"
  CAPTURE_ROOT="$(mktemp -d)"
  git worktree add --detach "$CAPTURE_ROOT/repo" "$SOURCE_COMMIT"
  cd "$CAPTURE_ROOT/repo/frontend"
  npm ci
  cd "$CAPTURE_ROOT/repo/e2e"
  npm ci
  cd "$CAPTURE_ROOT/repo"
  CAPTURE_OUTPUT="$(
    scripts/research/run-python \
      scripts/research/capture_operator_journey.py \
      --output-root /Users/yefanzhang/workplace/edge-research/docs/research/results/operator \
      --source-root "$CAPTURE_ROOT/repo"
  )"
  printf '%s\n' "$CAPTURE_OUTPUT"
  RUN_DIR="$(printf '%s\n' "$CAPTURE_OUTPUT" | sed -n '1p')"
  test "$(printf '%s\n' "$CAPTURE_OUTPUT" | sed -n '2p')" = "PASS"
  test -d "$RUN_DIR"
  cd /Users/yefanzhang/workplace/edge-research
  ```

  Expected: output is one absolute bundle path followed by `PASS`; the run has
  two distinct task IDs in one stack, and cleanup removes owned build images.
  `RUN_DIR` is the printed absolute path. Keep `$CAPTURE_ROOT/repo` intact for
  Step 11; do not point the checker at the canonical checkout. If capture fails,
  remove only the failed temporary worktree after inspecting its logs; the
  wrapper removes its partial bundle.

- [ ] **Step 11: Inspect media and verify the finalized bundle**

  Set `RUN_DIR` to the exact path printed by Step 10, then run:

  ```bash
  cd "$CAPTURE_ROOT/repo"
  scripts/research/run-python scripts/research/check_artifact.py \
    --bundle "$RUN_DIR" \
    --require-kind operator \
    --source-root "$CAPTURE_ROOT/repo"
  find "$RUN_DIR/raw/playwright" -name '*.png' -print | sort
  find "$RUN_DIR/raw/playwright" -name '*.webm' -print | sort
  find "$RUN_DIR/raw/playwright" -name 'trace.zip' -print | sort
  scripts/research/run-python - "$RUN_DIR" <<'PY'
  import json
  import sys
  import zipfile
  from pathlib import Path, PurePosixPath

  root = Path(sys.argv[1])
  metadata = {
      project: json.loads(
          (
              root / "raw/playwright" / project / "operator-metadata.json"
          ).read_text()
      )
      for project in ("desktop", "mobile")
  }
  for project in ("desktop", "mobile"):
      trace = root / "raw/playwright" / project / "trace.zip"
      chunks = []
      total = 0
      with zipfile.ZipFile(trace) as archive:
          for info in archive.infolist():
              member = PurePosixPath(info.filename)
              if member.is_absolute() or ".." in member.parts:
                  raise SystemExit(f"unsafe trace member: {info.filename}")
              if info.file_size <= 10_000_000:
                  total += info.file_size
                  if total > 50_000_000:
                      raise SystemExit(f"{project} trace text is too large")
                  chunks.append(archive.read(info))
      payload = b"\n".join(chunks)
      current = metadata[project]
      for value in (
          current["task_id"],
          current["nonce"],
          current["expected_output"],
      ):
          if value.encode() not in payload:
              raise SystemExit(f"{project} trace lacks {value}")
  print("PASS trace/project correlation")
  PY
  ```

  Expected counts are four PNGs, two WebM files, and two trace archives. Inspect
  all four PNGs at original size. Desktop and mobile Chat images must show
  selected `shell-1`, exact nonce command, full task ID, and exact output.
  Desktop and mobile Task images must show the same project-local task under
  Completed. No key elements overlap.

  Verify media integrity and build contact sheets outside the bundle:

  ```bash
  while IFS= read -r video; do
    ffprobe -v error -show_entries format=duration \
      -of default=noprint_wrappers=1 "$video"
  done < <(find "$RUN_DIR/raw/playwright" -name '*.webm' -print | sort)
  while IFS= read -r trace; do
    unzip -t "$trace"
  done < <(find "$RUN_DIR/raw/playwright" -name 'trace.zip' -print | sort)
  CONTACT_DIR="$CAPTURE_ROOT/contact-sheets"
  mkdir -p "$CONTACT_DIR"
  while IFS= read -r video; do
    project="$(basename "$(dirname "$video")")"
    ffmpeg -y -v error -i "$video" \
      -vf "fps=1,scale=720:-2,tile=3x2:padding=4:margin=4" \
      -frames:v 1 "$CONTACT_DIR/$project.png"
    test -s "$CONTACT_DIR/$project.png"
  done < <(find "$RUN_DIR/raw/playwright" -name '*.webm' -print | sort)
  test "$(find "$CONTACT_DIR" -name '*.png' -print | wc -l | tr -d ' ')" = 2
  ```

  Use `view_image` at original detail on all four bundle screenshots and both
  contact sheets. For each contact sheet, compare the adjacent project's
  `operator-metadata.json`: at least one frame must visibly show that task's
  selected `shell-1`, nonce, full task ID, and exact output. Expected: two
  positive durations, two successful
  ZIP integrity checks, `PASS trace/project correlation`, two matching contact
  sheets, and a final checker `PASS`. Contact sheets are inspection scratch and
  are deleted rather than finalized. Only after all six `view_image`
  inspections and both media-integrity checks pass, remove the retained source
  worktree and scratch:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  rm -rf "$CONTACT_DIR"
  git worktree remove "$CAPTURE_ROOT/repo"
  rmdir "$CAPTURE_ROOT"
  ```

- [ ] **Step 12: Commit evidence only and prove no recapture is needed**

  Stage only the printed bundle directory. Run `commit-check`, inspect the
  cached manifest and path list, then commit:

  ```bash
  git add -- "$RUN_DIR"
  git diff --cached --name-only
  git diff --cached --check
  git commit -m "docs(e2e): record operator journey evidence"
  ```

  Read the recorded source commit from the committed manifest, verify no
  committed relevant source differs between that commit and the evidence-only
  commit, recreate a clean detached worktree at that exact commit, and run the
  checker there:

  ```bash
  cd /Users/yefanzhang/workplace/edge-research
  SOURCE_COMMIT="$(
    scripts/research/run-python - "$RUN_DIR/manifest.json" <<'PY'
  import json
  import sys
  from pathlib import Path

  print(json.loads(Path(sys.argv[1]).read_text())["source"]["commit"])
  PY
  )"
  scripts/research/run-python - "$RUN_DIR/manifest.json" <<'PY'
  import json
  import sys
  from pathlib import Path

  from scripts.research.evidence import capture_source_provenance

  expected = json.loads(Path(sys.argv[1]).read_text())["source"]
  actual = capture_source_provenance(Path.cwd())
  if actual.git_dirty:
      raise SystemExit("current repository-wide source set is dirty")
  if actual.source_sha256 != expected["source_sha256"]:
      raise SystemExit("repository-wide source hash changed after capture")
  if list(actual.paths) != expected["paths"]:
      raise SystemExit("repository-wide source path set changed after capture")
  if ".dockerignore" not in actual.paths:
      raise SystemExit("repository-wide source set omits .dockerignore")
  PY
  test -z "$(
    git diff --name-only "$SOURCE_COMMIT"..HEAD -- \
      aggregator frontend e2e scripts/research schemas \
      docker-compose.yml .dockerignore docs/05-messaging.md
  )"
  VERIFY_ROOT="$(mktemp -d)"
  git worktree add --detach "$VERIFY_ROOT/repo" "$SOURCE_COMMIT"
  cd "$VERIFY_ROOT/repo"
  scripts/research/run-python scripts/research/check_artifact.py \
    --bundle "$RUN_DIR" \
    --require-kind operator \
    --source-root "$VERIFY_ROOT/repo"
  cd /Users/yefanzhang/workplace/edge-research
  git worktree remove "$VERIFY_ROOT/repo"
  rmdir "$VERIFY_ROOT"
  ```

  Expected: the checker prints `PASS`. The evidence-only commit may make Git
  `HEAD` newer than the recorded source commit, but it must not change any
  relevant source path. Never use the canonical checkout as the checker source
  root.

  If any relevant source, test, config, schema, or launcher file changes after
  Step 10 and before this check passes, the existing bundle is invalid. Return
  to Step 8, commit the change, and recapture both projects from a new clean
  worktree. Never patch a finalized manifest or replace one project's media in
  place. Apply Step 13's status-only documentation commit only after this check
  passes; that traceability update does not retroactively invalidate evidence
  tied to `SOURCE_COMMIT`.

- [ ] **Step 13: Advance only the test-gated R-02 and evidence-gated R-08 statuses**

  Only after Step 8's cumulative backend/frontend gates and Step 12's
  clean-source checker both pass, change exactly two requirement rows in
  `docs/research/task-aware-reliability-contract-design.md` to:

  ```markdown
  | R-02 | Legal state reducer and idempotent audit persistence | Verified | `aggregator/tests/test_database.py`; `aggregator/tests/test_api.py`; `frontend/src/utils/taskReducer.test.js`; `frontend/src/components/TaskBoard.test.jsx` |
  | R-08 | Isolated deterministic operator journey and evidence bundle | Verified | `e2e/tests/operator-journey.spec.js`; `e2e/helpers/lifecycle.integration.spec.js`; `tests/research/test_operator_evidence.py`; `tests/research/test_checker.py`; `docs/research/results/operator/<bundle>/manifest.json` |
  ```

  Replace `<bundle>` with the committed `RUN_DIR` basename. Do not advance R-02
  if any named backend/frontend gate failed. Do not advance R-08 for unit tests
  alone, a partial bundle, an unchecked bundle, or a checker run against the
  canonical checkout. Noninteractively patch-stage only those two rows, invoke
  `commit-check`, run `git diff --cached --check`, and commit:

  ```bash
  git diff --binary -- \
    docs/research/task-aware-reliability-contract-design.md \
    > /tmp/edgecitadel-slice2-status.patch
  git apply --cached --check /tmp/edgecitadel-slice2-status.patch
  git apply --cached /tmp/edgecitadel-slice2-status.patch
  git diff --cached --name-only
  git diff --cached --check
  git commit -m "docs(e2e): record verified R-02 and R-08 gates"
  ```

  After this final commit, atomically advance the existing chain handoff so
  Slice 3 starts at Slice 2's exact clean `HEAD` in the same worktree:

  ```bash
  set -euo pipefail
  EXPECTED_ROOT=/Users/yefanzhang/workplace/edge-research
  EXPECTED_BASE="$(git -C "$EXPECTED_ROOT" rev-parse HEAD)"
  CHAIN_KEY="$(printf '%s' "$EXPECTED_BASE" | cut -c1-12)"
  CHAIN_ROOT="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY"
  HANDOFF="$CHAIN_ROOT/handoff.env"
  test -f "$HANDOFF"
  # shellcheck disable=SC1090
  source "$HANDOFF"
  test "$CANONICAL_ROOT" = "$EXPECTED_ROOT"
  test "$CANONICAL_BASE" = "$EXPECTED_BASE"
  test "$(git rev-parse --show-toplevel)" = "$TASK_ROOT"
  test "$(git branch --show-current)" = "$BRANCH"
  test "$FINAL_COMMIT" = "$(cat "$CHAIN_ROOT/slice2-entry-commit")"
  git merge-base --is-ancestor "$FINAL_COMMIT" HEAD
  test -z "$(git status --porcelain)"
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
  test "$FINAL_COMMIT" = "$(git rev-parse HEAD)"
  ```

## Slice 2 Completion Audit

- [ ] R-02: one audit row survives replay, `duplicate_count` is observable, and
  task state follows the complete Section 2.3 observation-order reducer.
- [ ] `none`, `submitted`, `working`, `input-required`, `auth-required`, all four
  terminals, invalid transitions, terminal replay identity, and payload-hash
  conflicts have focused tests.
- [ ] StatusBadge exposes `Status: value` accessibly and RegistryRow passes the
  actual `agent_state`.
- [ ] Fleet WebSocket remains connected while selecting an agent; registration,
  status, offline notification, deletion, progress, and result frames converge.
- [ ] The audited product name is EdgeCitadel and no unsupported theme or stale
  storage-state file remains.
- [ ] The launcher uses Docker-assigned loopback ports, supports concurrent runs,
  passes staged-tree clean-checkout and signal tests, tears down idempotently
  with `--rmi local`, and verifies containers, networks, volumes, and
  project-owned build references while retaining all-image provenance.
- [ ] Direct HTTP commands publish a fresh UUIDv4 context when absent and
  `hop_count=0`; fixture progress/results preserve task, context, and hop fields.
- [ ] The exact fixture config uses launcher run ID, EdgeCitadel echo mode,
  1000 ms delay/heartbeat, null crash point, and run-owned outcome/side-effect
  databases.
- [ ] The operator sends a nonce only, observes nonterminal then completed,
  proves exact output, one command, one logical terminal, drained queue, and no
  browser/request errors.
- [ ] Fragmentation is explicitly live-only; default gate has exactly 13
  deterministic tests, one worker, zero retries, and no external model.
- [ ] Final evidence was captured only after all source/harness commits and gates.
  One passing stack contains distinct desktop/mobile task IDs, with each
  project's Chat screenshot, Task screenshot, API files, metadata, trace, and
  video internally matching.
- [ ] All four screenshots pass overlap and visual inspection; exactly two
  videos and two traces pass integrity checks, each trace contains its project's
  task/nonce/output, and both video contact sheets visually match project
  metadata.
- [ ] `scripts/research/check_artifact.py` passes the finalized bundle and its
  corruption tests catch missing, changed, cross-project, secret, cleanup, and
  source-hash failures while all retained provenance paths remain portable.
- [ ] R-02 is marked Verified only after its named backend and frontend gates
  pass.
- [ ] R-08 is marked Verified only after the committed operator manifest passes
  the checker against its recorded clean source commit.
- [ ] Every commit uses the stated Conventional Commit message, an exact staged
  file map, `git diff --cached --check`, and `commit-check`; the canonical
  checkout and its user-owned `aggregator/tests/test_api.py` work remain
  byte-for-byte unchanged.
