# Merge notes: `codex/pi-jev-root` into `containment/sprint`

## Inputs

| Side | Source | Commit in this repository |
|---|---|---|
| main | `/home/claude/ec/src`, GitHub main at `4d464fe` | `55bcba2` "Import EdgeCitadel main at 4d464fe" (first parent) |
| branch | `/home/claude/ec/wt`, `codex/pi-jev-root` at `8aa296b` plus its uncommitted docs, imported as one snapshot commit `9d888f9` | fetched read-only as `refs/imports/codex-pi-jev-root` (second parent) |

The two imports share no history, so the merge used `--allow-unrelated-histories`. Git
therefore had no merge base: every file present on both sides with different content became
an add/add conflict, and every file present on only one side was kept automatically. Each
conflict was resolved by reading both versions and deciding which side made the change.

## How the direction of each difference was established

Every file that differs between the two snapshots was compared. In all 25 conflicted
files, the branch side is a strict subset of main: it lacks hunks that main added after the
branch was cut (JEV support, the JEV UI, the four-tab navigation and the caller-reserved
child task ID). Nowhere did the branch side contain a hunk that main lacks, apart from the
two documentation additions listed below. The branch's actual work is in files that exist
only on the branch.

## Conflict resolutions

### Core runtime files: keep main

| File | Branch side lacks | Why main's version is required, including by the Pi packages |
|---|---|---|
| `agent-runtime/src/edgecitadel_agentd/trace_dispatch.py` | `child_id = params.get("child_task_id") or uuid4()` | `pi-root` reserves the child ID in its journal before dispatch and passes `child_task_id` (`pi_runtime/__main__.py`, `Harness.delegate`). JEV does the same (`coordinator.py:372-390`). |
| `schemas/trace-dispatch-request.v1.json` | the optional `child_task_id` property | The request schema has `additionalProperties: false`. Checked directly: the branch schema rejects a `pi-root` dispatch request that carries `child_task_id`, and main's schema accepts it. |
| `agent-runtime/src/edgecitadel_agentd/service.py` | `"agent.list"` in the Managed Agent operation set | `pi-root` is a Managed Agent and calls `agent.list` in `Harness.route`. On the branch, only native connectors with `edgecitadel_agents` could call it, so `pi-root` routing would fail authorization. JEV calls it too. |
| `agent-runtime/src/edgecitadel_agentd/managed_runtime.py` | the optional `on_start` keyword of `run()` | JEV's `__main__` passes `on_start=coordinator.reconcile`. `pi-root` calls `run(config, handler)`, which main's signature still accepts, so keeping main serves both packages. |
| `agent-runtime/tests/agentd/test_trace_dispatch_store.py` | `test_caller_reserved_child_id_is_atomic_and_cannot_be_reused` | This test covers the `child_task_id` behaviour kept above. |

Conclusion: the branch's three diverged core files are older copies of main's. They are not
alternative implementations. Keeping main is the only resolution under which both packages
work. The deployed `pi-root` must have run against main-equivalent agentd code, because the
branch's own core rejects its requests.

### Frontend and e2e files: keep main

`frontend/src/{App.jsx,Layout.jsx,api/client.js,components/*.jsx,hooks/realtimeEvents*.js,traces/TraceExplorer.jsx,traces/trace.css}`
and `e2e/tests/{keyboard-shortcuts,operator-journey,phase3-registry-tab,trace-communication}.spec.js`:
the branch side is main's earlier UI (six tabs, no JEV run view, no `skill_id` in `sendCommand`,
smaller header). Main's later JEV and navigation work is kept. The branch's only frontend
addition, `frontend/public/pi.html` (the Pi chat page), applied cleanly because it exists only on the branch.

### Documentation: take the branch's additions

- `AGENTS.md`: the branch adds a pointer to `docs/research-context.zh.md` under
  "Working references". Main's text is otherwise identical, so the branch version is a strict superset.
- `agent-packages/README.md`: the branch adds the `pi-root/` package entry. Main's text is
  otherwise identical, so the branch version is a strict superset.

### Kept automatically (branch-only files)

- `agent-packages/pi-root/**` (root and worker Managed Agent package. The worker package
  is generated from the same runtime by `tools/pi/build-package.py`)
- `tools/pi/**` (build, provisioning, chat client and robustness probes)
- `tests/pi/test_managed_pi.py`, `tests/pi/test_chat.py`
- `frontend/public/pi.html`
- `docs/{pi-harness.md,robustness-20260928.md,manual-robustness-and-controls.zh.md,research-context.zh.md,containment-experiments-v1.zh.md,adversarial-source-audit-20261012.zh.md}`

Main-only files are kept: `agent-packages/jev/**`, `agent-runtime/tests/agentd/test_jev.py`,
`plugins/adversarial-source/**`, the JEV UI files and `docs/acceptance/jev-20260926.md`.

## Verification of the merge

- `validate_package` succeeds for both `agent-packages/pi-root` (`edgecitadel.pi-root`) and
  `agent-packages/jev` (`edgecitadel.jev`). Their locks match their files.
- `pytest tests/pi agent-runtime/tests/agentd/test_jev.py agent-runtime/tests/agentd/test_trace_dispatch_store.py agent-runtime/tests/agentd/test_managed_runtime.py`
  passes: 62 tests.

## Environment and baseline (before containment changes)

Setup, run from the repository root:

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e 'agent-runtime[test,type]' -r aggregator/requirements.txt -r scripts/requirements-test.txt
```

Commands:

```bash
# agent-runtime
cd agent-runtime && ../.venv/bin/python -m pytest -q
# aggregator. PYTHONSAFEPATH stops `python -c` crash-test subprocesses from importing
# aggregator/aggregator.py as the `aggregator` package. Without it, 14 owned-SIGKILL tests
# fail with "attempted relative import with no known parent package".
cd aggregator && PYTHONSAFEPATH=1 PYTHONPATH=$PWD/.. ../.venv/bin/python -m pytest -q
# Pi package tests merged from the branch
.venv/bin/python -m pytest -q tests/pi
```

| Suite | Tree | Result |
|---|---|---|
| agent-runtime | main import `55bcba2` | 1355 passed, 56 skipped, 1 error |
| aggregator | main import `55bcba2` | 915 passed, 18 skipped, 0 failed |
| agent-runtime | this merge | 1355 passed, 56 skipped, 1 error (the same test) |
| aggregator | this merge | 915 passed, 18 skipped, 0 failed |
| tests/pi | this merge | 7 passed |

The merge changes no file that the agent-runtime or aggregator suites import, so their
results are identical to main's. This merged tree is the baseline for the containment work.

The one agent-runtime error is a teardown race in `tests/agentd/test_service.py::test_local_rpc_records_real_message_bodies_without_nats_and_deduplicates_reads`:
"NATS transport did not stop within 10 seconds" while the suite ran in parallel with the
aggregator suite on 2 CPUs. The test passes in isolation. Every skip is an explicit opt-in
gate: owned NATS or JetStream (`RUN_AGENTD_NATS_INTEGRATION`, `RUN_JETSTREAM_INTEGRATION`),
jim-eq quota fixtures, macOS storage, or the telemetry leaf. None of these can run locally
without network access to the lab machines, so they are verification gaps, not passes.
