# Slice 4 Documentation and Artifact Organization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Verify R-10 by replacing contradictory active documentation with a checked current/experimental/proposed/historical split, reproducible artifact runbooks, and an indexed decision/evidence history.

**Architecture:** A small Python documentation checker is the enforcement boundary. It validates maintained Markdown links, schema-tagged JSON examples, ADR identity/status, generated FastAPI/OpenAPI and WebSocket reference blocks, runbook command paths, research-result labels, dummy credentials, personal paths, and forbidden pre-v0.1 claims. Documentation changes then proceed in narrow commits against that checker; archived material is excluded from current-behavior checks but remains linkable and visibly historical.

**Tech Stack:** Markdown, Python 3.12 standard library, `pytest`, `jsonschema`, FastAPI OpenAPI generation, Git, Docker Compose v2, npm/Vite, Playwright, and the Obsidian vault schema.

---

## Execution Boundary

This plan implements only Slice 4 and R-10 from
`docs/research/task-aware-reliability-contract-design.md`. Execute it after Slices
1-3 are merged and verified because the maintained artifact and lab runbooks must
document executable entrypoints rather than proposed paths.

The implementation must not:

- change messaging, task-state, benchmark, frontend, or deployment behavior;
- repair `join.sh`, `add-agent.sh`, OpenClaw authentication, MQTT firmware, or the
  general host installer;
- change or delete existing raw result JSON;
- claim `Remote Lab Qualified`, `Preliminary Measured`, or `Paper Evidence Ready`
  unless the corresponding validated artifact already exists;
- rewrite archived prose as if it were current.

Continue in the persistent clean worktree created by Slice 1 and advanced by
Slices 2-3. Resolve it only from the shared handoff, and require its clean `HEAD`
to equal the recorded Slice 3 `FINAL_COMMIT` before adopting any file:

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
test "$(git -C "$TASK_ROOT" branch --show-current)" = "$BRANCH"
test "$(git -C "$TASK_ROOT" rev-parse HEAD)" = "$FINAL_COMMIT"
test -z "$(git -C "$TASK_ROOT" status --porcelain)"
cd "$TASK_ROOT"
```

Record the canonical checkout's `git status --short` first and do not stage,
move, merge, or clean any pre-existing path there. Because every task runs in
the shared clean worktree and starts after the preceding task commit, stage new
and modified files noninteractively with `git add -- <exact paths>` from that
task's file map. Before every commit, inspect
`git diff --cached --name-only` and `git diff --cached`, run
`git diff --cached --check`, and invoke the `commit-check` skill. Never use
interactive staging or ask the user to choose hunks.

Nine provenance/governance files are currently untracked in the original checkout
but are inputs to this slice. A clean worktree does not inherit them. Adopt only
this allowlist before Task 1, preserve the source checkout byte-for-byte, and keep
the generated status/hash records in a worktree-qualified directory under
`/tmp`, never under the repository. The external state remains available through
the canonical-checkout handoff in Task 10:

```bash
ORIGINAL_ROOT=/Users/yefanzhang/workplace/edge-research
SLICE4_ROOT="$(git rev-parse --show-toplevel)"
test "$SLICE4_ROOT" != "$ORIGINAL_ROOT"
WORKTREE_KEY="$(
  printf '%s\0' "$(git rev-parse --absolute-git-dir)" |
    shasum -a 256 | awk '{print substr($1, 1, 16)}'
)"
SLICE4_STATE_ROOT="/tmp/edgecitadel-slice4-adoption-$WORKTREE_KEY"
test ! -e "$SLICE4_STATE_ROOT"
mkdir -p "$SLICE4_STATE_ROOT"
git rev-parse HEAD > "$SLICE4_STATE_ROOT/base-commit.txt"
git -C "$ORIGINAL_ROOT" status --short \
  > "$SLICE4_STATE_ROOT/original-status.txt"
: > "$SLICE4_STATE_ROOT/sha256.txt"
ADOPTED_PATHS=(
  AGENTS.md
  docs/research/future-directions-roadmap.html
  docs/research/nats-agent-communication-research-plan.md
  docs/research/nats-agent-communication-experiment-matrix.md
  docs/research/nats-agent-communication-literature-review.md
  docs/research/nats-agent-communication-baseline-results-2026-07-04.md
  docs/research/results/.gitkeep
  docs/research/results/20260712T052153Z-security_temporal-security_temporal.json
  docs/research/results/20260712T052153Z-security_temporal-security_temporal.md
)
for relative in "${ADOPTED_PATHS[@]}"; do
  if git -C "$ORIGINAL_ROOT" ls-files --error-unmatch "$relative" \
      >/dev/null 2>&1; then
    test -f "$SLICE4_ROOT/$relative"
    continue
  fi
  test -f "$ORIGINAL_ROOT/$relative"
  test ! -e "$SLICE4_ROOT/$relative"
  mkdir -p "$(dirname "$SLICE4_ROOT/$relative")"
  source_sha="$(shasum -a 256 "$ORIGINAL_ROOT/$relative" | awk '{print $1}')"
  cp -p "$ORIGINAL_ROOT/$relative" "$SLICE4_ROOT/$relative"
  cmp "$ORIGINAL_ROOT/$relative" "$SLICE4_ROOT/$relative"
  adopted_sha="$(shasum -a 256 "$SLICE4_ROOT/$relative" | awk '{print $1}')"
  test "$source_sha" = "$adopted_sha"
  printf '%s  %s\n' "$source_sha" "$relative" \
    >> "$SLICE4_STATE_ROOT/sha256.txt"
done
LC_ALL=C sort -o "$SLICE4_STATE_ROOT/sha256.txt" \
  "$SLICE4_STATE_ROOT/sha256.txt"
git -C "$ORIGINAL_ROOT" status --short \
  > "$SLICE4_STATE_ROOT/original-status-after.txt"
cmp "$SLICE4_STATE_ROOT/original-status.txt" \
  "$SLICE4_STATE_ROOT/original-status-after.txt"
```

Expected: every copied file compares byte-for-byte and its two SHA-256 values
match; the original checkout has no new tracked diff. A path already tracked by
the merged Slice 1-3 commit is required to exist in the clean worktree and is not
copied. At the owning task, stage each adopted untracked file by its exact
allowlisted path with `git add --`.
`git status --porcelain` in the task worktree remains empty apart from the
allowlisted copied inputs because no adoption record is created beneath it.

Stop before Task 1 if any required Slice 1-3 file or public interface is absent:

```bash
test -f scripts/research/run_artifact.py
test -f scripts/research/analyze_artifact.py
test -f scripts/research/check_artifact.py
test -f scripts/research/capture_operator_journey.py
test -f scripts/research/evidence.py
test -f scripts/research/preflight.py
test -f scripts/research/artifact_env.py
test -x scripts/research/run-python
test -f scripts/research/requirements.lock.txt
test -f scripts/research/fixtures/native_control.py
test -f scripts/research/lab_controller.py
test -f scripts/research/lab_node.py
test -f scripts/research/docker-compose.artifact.yml
test -f scripts/research/docker-compose.lab.yml
test -f schemas/research-manifest.v1.json
test -f schemas/task-correlation.v1.json
test -f e2e/playwright.evidence.config.js
scripts/research/run-python - <<'PY'
from scripts.research.artifact_env import ArtifactEnvironment
from scripts.research.check_artifact import check_bundle
from scripts.research.evidence import finalize_bundle, write_json
from scripts.research.preflight import PreflightReport

assert callable(write_json)
assert callable(finalize_bundle)
assert callable(check_bundle)
assert hasattr(PreflightReport, "require_valid")
assert hasattr(ArtifactEnvironment, "start_topology")
PY
scripts/research/run-python scripts/research/run_artifact.py run --help
scripts/research/run-python scripts/research/run_artifact.py cleanup --help
scripts/research/run-python scripts/research/analyze_artifact.py --help
scripts/research/run-python scripts/research/check_artifact.py --help
scripts/research/run-python scripts/research/capture_operator_journey.py --help
scripts/research/run-python scripts/research/lab_controller.py start --help
scripts/research/run-python scripts/research/lab_controller.py status --help
scripts/research/run-python scripts/research/lab_controller.py command --help
scripts/research/run-python scripts/research/lab_controller.py await --help
scripts/research/run-python scripts/research/lab_controller.py export-image --help
scripts/research/run-python scripts/research/lab_controller.py qualify --help
scripts/research/run-python scripts/research/lab_controller.py stop --help
scripts/research/run-python scripts/research/lab_node.py start --help
scripts/research/run-python scripts/research/lab_node.py status --help
scripts/research/run-python scripts/research/lab_node.py doctor --help
scripts/research/run-python scripts/research/lab_node.py stop --help
```

Expected: file/import checks exit `0`; help commands print the agreed subcommands
and options, including checker `--bundle`, `--campaign`, `--require-kind`, and
`--source-root`. The settled benchmark `run --result-file` JSON always contains
absolute `campaign_path`, ordered `bundle_paths`, source commit/hash, and
profile; even quick never reports one representative `bundle_path`.
Slice 3 must use `finalize_bundle`/`write_json` and `check_bundle(...)`; legacy
writer and positional checker interfaces are not accepted. Any failure means the
owning earlier slice is incomplete; stop rather than documenting it as current.

## R-10 Traceability

| R-10 obligation | Implemented in | Verified by |
| --- | --- | --- |
| Four-way document classification | `docs/README.md`, root `README.md`; exact historical developer-record exclusion inventory | `test_document_index_has_four_status_sections`; `test_historical_superpowers_inventory_is_exact_when_present`; final complete-contract test |
| Exact historical moves | `docs/archive/` | `test_exact_archive_contract` |
| Current architecture and messaging | `docs/01-architecture.md`, `docs/agent-contract.md`, `docs/05-messaging.md` | schema, forbidden-claim, and link checks |
| Current API and WebSocket surface | `docs/08-api-reference.md` | generated-reference check against `make_app(...).openapi()` and FastAPI routes |
| Current dashboard and test gates | `docs/04-dashboard.md`, `docs/10-testing.md` | maintained-doc and runbook-command checks |
| Supported development/lab setup | `docs/setup-development.md`, `docs/setup-lab-controller.md`, `docs/setup-lab-node.md` | command resolution plus clean quick/lab dry runs |
| Artifact use and evidence semantics | `docs/research/artifact.md`, `docs/research/results/README.md` | result inventory and evidence-label checks |
| Unique, accurate decision history | `docs/adr/README.md`, `docs/adr/*.md` | ADR identifier/header/status/index check |
| No local-machine leakage | maintained docs and runbooks | personal-path and dummy-credential checks |
| Vault synchronization | EdgeCitadel docs wiki page, `.manifest.json`, `log.md` | vault schema read, JSON parse, and exact-source lookup |

## File Map

**Create:**

- `scripts/docs/__init__.py`
- `scripts/docs/check_docs.py`
- `scripts/docs/render_api_reference.py`
- `tests/docs/test_check_docs.py`
- `tests/docs/test_render_api_reference.py`
- `tests/docs/test_documentation_contract.py`
- `docs/README.md`
- `docs/archive/README.md`
- `docs/archive/provenance.json`
- `docs/archive/media/2026-03/README.md`
- `docs/setup-development.md`
- `docs/setup-lab-controller.md`
- `docs/research/artifact.md`
- `docs/research/r10-implementation-log.md`
- `docs/adr/README.md`

**Move exactly:**

- `docs/NATS_ARCHITECTURE.md` -> `docs/archive/pre-v0.1/NATS_ARCHITECTURE.md`
- `docs/06-p2p-delegation.md` -> `docs/archive/pre-v0.1/06-p2p-delegation.md`
- `docs/07-task-management.md` -> `docs/archive/pre-v0.1/07-task-management.md`
- `docs/11-future-potential.md` -> `docs/archive/pre-v0.1/11-future-potential.md`
- `docs/research/future-directions-roadmap.html` -> `docs/archive/research/future-directions-roadmap.html`
- `docs/demo.gif` -> `docs/archive/media/2026-03/demo.gif`
- `docs/demo.mp4` -> `docs/archive/media/2026-03/demo.mp4`
- `docs/adr/0009-bridge-adapter-memory-ownership.md` ->
  `docs/adr/0012-bridge-adapter-memory-ownership.md`

**Rewrite as maintained current documentation:**

- `README.md`
- `docs/01-architecture.md`
- `docs/agent-contract.md`
- `docs/04-dashboard.md`
- `docs/05-messaging.md`
- `docs/08-api-reference.md`
- `docs/10-testing.md`

**Rewrite as compatibility/supplemental pages with explicit status:**

- `docs/02-server-setup.md`
- `docs/02-server-setup-linux.md`
- `docs/02-server-setup-macos.md`
- `docs/03-agent-registration.md`
- `docs/09-monitoring.md`
- `docs/agent-setup.md`
- `docs/setup_hermes.md`
- `docs/roadmap.md`
- `docs/setup-lab-node.md` (created and tested by Slice 3)

**Relabel research material without changing recorded measurements:**

- `docs/research/nats-agent-communication-research-plan.md`
- `docs/research/nats-agent-communication-experiment-matrix.md`
- `docs/research/nats-agent-communication-literature-review.md`
- `docs/research/nats-agent-communication-baseline-results-2026-07-04.md`
- `docs/research/results/README.md`
- `docs/research/results/20260712T052153Z-security_temporal-security_temporal.md`

**Preserve byte-for-byte and add to provenance inventory if currently untracked:**

- `docs/research/results/.gitkeep`
- `docs/research/results/20260712T052153Z-security_temporal-security_temporal.json`

**Preserve byte-for-byte as the exact historical developer-record exclusion
inventory:**

- `docs/superpowers/plans/2026-04-29-phase-3-watchdog-and-registry.md`
- `docs/superpowers/plans/2026-04-30-gemma-enhancements.md`
- `docs/superpowers/plans/2026-05-03-claude-md-system.md`
- `docs/superpowers/plans/2026-05-06-hermes-bridge.md`
- `docs/superpowers/plans/2026-05-16-phase4-umbrella.md`
- `docs/superpowers/plans/2026-05-19-hermes-financial-agent.md`
- `docs/superpowers/plans/2026-05-31-enterprise-trading-agent-mvp.md`
- `docs/superpowers/plans/2026-07-12-agentic-edge-benchmark-suite-e1-e12.md`
- `docs/superpowers/specs/2026-04-29-phase-3-watchdog-and-registry-design.md`
- `docs/superpowers/specs/2026-04-30-gemma-enhancements-design.md`
- `docs/superpowers/specs/2026-05-03-claude-md-system-design.md`
- `docs/superpowers/specs/2026-05-05-hermes-bridge-design.md`
- `docs/superpowers/specs/2026-05-10-phase4-fleet-orchestration-umbrella-design.md`
- `docs/superpowers/specs/2026-05-18-phase4.1-bridge-adapter-template-design.md`

**Update active references and governance:**

- `AGENTS.md`
- `CLAUDE.md`
- `CONTRIBUTING.md`
- `PROGRESS.md`
- `deploy/README.md`
- `docs/CHANGELOG.md`
- `docs/adr/template.md`
- `docs/adr/0001-nats-over-mqtt-broker.md`
- `docs/adr/0002-nats-jetstream-workqueue.md`
- `docs/adr/0003-a2a-v1-vocabulary-adoption.md`
- `docs/adr/0004-mqtt-ingress-opt-in.md`
- `docs/adr/0005-browser-scoped-token.md`
- `docs/adr/0006-outbox-mirror-authoritative.md`
- `docs/adr/0007-watchdog-trigger-model.md`
- `docs/adr/0008-centralized-memory-service.md`
- `docs/adr/0009-host-deploy-architecture.md`
- `docs/adr/0010-nats-native-l2-delegation.md`
- `docs/adr/0011-mcp-for-tool-exposure.md`
- `adapters/hermes/README.md`
- `adapters/hermes/adapter.py`
- `adapters/hermes/tests/test_adapter_handle.py`

### Task 1: Add the tested documentation-contract checker

**Files:**

- Create: `scripts/docs/__init__.py`
- Create: `scripts/docs/check_docs.py`
- Create: `tests/docs/test_check_docs.py`

- [ ] **Step 1: Add the failing checker unit tests**

Create `tests/docs/test_check_docs.py`:

```python
import json
from pathlib import Path

from scripts.docs.check_docs import (
    ADR_EXPECTED,
    DOCUMENT_CLASSIFICATION,
    check_adr_set,
    check_commands,
    check_document_classification,
    check_internal_links,
    check_no_bare_host_python,
    check_result_labels,
    check_schema_examples,
)


def messages(issues):
    return [issue.message for issue in issues]


def test_internal_link_checker_reports_missing_file_and_anchor(tmp_path: Path):
    docs = tmp_path / "docs"
    docs.mkdir()
    source = docs / "index.md"
    target = docs / "target.md"
    source.write_text(
        "[missing](missing.md) [bad anchor](target.md#absent) "
        "[good](target.md#present)\n"
    )
    target.write_text("# Present\n")

    issues = check_internal_links(tmp_path, [source])

    assert messages(issues) == [
        "target does not exist: docs/missing.md",
        "anchor does not exist: docs/target.md#absent",
    ]


def test_schema_checker_validates_tagged_json(tmp_path: Path):
    schema_dir = tmp_path / "schemas"
    docs = tmp_path / "docs"
    schema_dir.mkdir()
    docs.mkdir()
    (schema_dir / "value.json").write_text(json.dumps({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["value"],
        "properties": {"value": {"type": "integer"}},
        "additionalProperties": False,
    }))
    page = docs / "contract.md"
    page.write_text(
        "<!-- schema: schemas/value.json -->\n"
        "```json\n"
        '{"value": "wrong"}\n'
        "```\n"
    )

    issues = check_schema_examples(tmp_path, [page])

    assert len(issues) == 1
    assert "is not of type 'integer'" in issues[0].message


def test_adr_checker_rejects_duplicate_id_and_missing_index_row(tmp_path: Path):
    adr_dir = tmp_path / "docs" / "adr"
    adr_dir.mkdir(parents=True)
    (adr_dir / "0001-first.md").write_text(
        "# ADR-0001: First\n\n## Status\n\nAccepted\n\n"
        "## Implementation\n\nCurrent\n"
    )
    (adr_dir / "0001-second.md").write_text(
        "# ADR-0001: Second\n\n## Status\n\nProposed\n\n"
        "## Implementation\n\nNot implemented\n"
    )
    (adr_dir / "README.md").write_text("# ADR Index\n")

    issues = check_adr_set(tmp_path)

    assert any("duplicate ADR id 0001" in item.message for item in issues)
    assert any("missing from docs/adr/README.md" in item.message for item in issues)


def test_command_checker_rejects_unknown_repo_script(tmp_path: Path):
    docs = tmp_path / "docs"
    docs.mkdir()
    page = docs / "runbook.md"
    page.write_text(
        "<!-- doc-command -->\n"
        "```bash\n"
        "python3 scripts/missing.py --help\n"
        "```\n"
    )

    issues = check_commands(tmp_path, [page])

    assert messages(issues) == ["command path does not exist: scripts/missing.py"]


def test_command_checker_rejects_option_absent_from_help(tmp_path: Path):
    scripts = tmp_path / "scripts"
    docs = tmp_path / "docs"
    scripts.mkdir()
    docs.mkdir()
    (scripts / "tool.py").write_text(
        "import argparse\n"
        "p = argparse.ArgumentParser()\n"
        "p.add_argument('--known')\n"
        "p.parse_args()\n"
    )
    page = docs / "runbook.md"
    page.write_text(
        "<!-- doc-command -->\n"
        "```bash\npython3 scripts/tool.py --missing value\n```\n"
    )

    assert messages(check_commands(tmp_path, [page])) == [
        "documented option absent from help: --missing",
    ]


def test_classification_reports_missing_and_unclassified_docs(tmp_path: Path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "README.md").write_text(
        "\n".join(f"## {label}" for label in DOCUMENT_CLASSIFICATION)
    )
    (docs / "unexpected.md").write_text("# Unexpected\n")

    issues = check_document_classification(tmp_path)

    assert any(item.code == "DOC-MISSING" for item in issues)
    assert any("unexpected.md" in str(item.path) for item in issues)


def test_adr_checker_rejects_metadata_index_disagreement(tmp_path: Path):
    adr_dir = tmp_path / "docs" / "adr"
    adr_dir.mkdir(parents=True)
    for name, (title, status, implementation) in ADR_EXPECTED.items():
        adr_id = name[:4]
        (adr_dir / name).write_text(
            f"# ADR-{adr_id}: {title}\n\n"
            f"## Status\n\n{status}\n\n"
            f"## Implementation\n\n{implementation}\n"
        )
    (adr_dir / "README.md").write_text("# ADR Index\n")

    issues = check_adr_set(tmp_path)

    assert issues
    assert any("generated ADR index is stale" in item.message for item in issues)


def test_command_checker_rejects_untagged_bash_block(tmp_path: Path):
    docs = tmp_path / "docs"
    docs.mkdir()
    page = docs / "runbook.md"
    page.write_text("```bash\npython3 scripts/tool.py --help\n```\n")

    assert messages(check_commands(tmp_path, [page])) == [
        "bash block must have doc-command or doc-command-ignore: REASON",
    ]


def test_command_checker_requires_settled_lab_start_options(tmp_path: Path):
    scripts = tmp_path / "scripts" / "research"
    docs = tmp_path / "docs"
    scripts.mkdir(parents=True)
    docs.mkdir()
    (scripts / "lab_controller.py").write_text(
        "import argparse\n"
        "p = argparse.ArgumentParser()\n"
        "sub = p.add_subparsers(dest='command', required=True)\n"
        "start = sub.add_parser('start')\n"
        "start.add_argument('--run-id', required=True)\n"
        "start.add_argument('--host-id', required=True)\n"
        "p.parse_args()\n"
    )
    page = docs / "runbook.md"
    page.write_text(
        "<!-- doc-command -->\n"
        "```bash\n"
        "python3 scripts/research/lab_controller.py start "
        "--run-id ec-lab-01\n"
        "```\n"
    )

    assert messages(check_commands(tmp_path, [page])) == [
        "documented command lacks required option: --host-id, --lab-variant",
    ]


def test_command_checker_requires_settled_lab_node_behavior(tmp_path: Path):
    scripts = tmp_path / "scripts" / "research"
    docs = tmp_path / "docs"
    scripts.mkdir(parents=True)
    docs.mkdir()
    (scripts / "lab_node.py").write_text(
        "import argparse\n"
        "p = argparse.ArgumentParser()\n"
        "sub = p.add_subparsers(dest='command', required=True)\n"
        "start = sub.add_parser('start')\n"
        "for option in ('--controller-config', '--credential-file', "
        "'--host-id', '--agent-id', '--behavior', '--delay-ms'):\n"
        "    start.add_argument(option, required=True)\n"
        "p.parse_args()\n"
    )
    page = docs / "runbook.md"
    page.write_text(
        "<!-- doc-command -->\n"
        "```bash\n"
        "python3 scripts/research/lab_node.py start "
        "--controller-config controller.json --credential-file nats.creds "
        "--host-id controller-lab-01 --agent-id fixture-1\n"
        "```\n"
    )

    assert messages(check_commands(tmp_path, [page])) == [
        "documented command lacks required option: --behavior, --delay-ms",
    ]


def test_stale_host_python_scan_rejects_repository_test_commands(tmp_path: Path):
    docs = tmp_path / "docs"
    docs.mkdir()
    page = docs / "runbook.md"
    page.write_text(
        "<!-- doc-command -->\n"
        "```bash\npython3 -m pytest tests/docs -q\n```\n"
    )

    assert messages(check_no_bare_host_python([page])) == [
        "repository Python command must use scripts/research/run-python",
    ]


def test_result_checker_labels_gitkeep_and_checks_legacy_provenance(
    tmp_path: Path,
):
    result_dir = tmp_path / "docs" / "research" / "results"
    result_dir.mkdir(parents=True)
    (result_dir / ".gitkeep").write_text("\n")
    (result_dir / "README.md").write_text(
        "# Results\n\n| `.gitkeep` | Directory placeholder |\n"
    )
    (result_dir / "20260712T052153Z-security_temporal-security_temporal.json"
     ).write_text("{}\n")

    issues = check_result_labels(tmp_path)

    assert any(item.code == "DOC-PROVENANCE" for item in issues)
```

- [ ] **Step 2: Run the tests to verify the checker does not exist**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_check_docs.py -q
```

Expected: collection fails with
`ModuleNotFoundError: No module named 'scripts.docs'`.

- [ ] **Step 3: Verify the Slice 1 lock owns the checker dependency**

Do not add a second unpinned dependency surface. Verify the hash-locked Slice 1
environment already provides `jsonschema`:

```bash
scripts/research/run-python -c \
  'from importlib.metadata import version; print(version("jsonschema"))'
```

Expected: the command prints the locked version and exits `0`.

- [ ] **Step 4: Implement the minimal checker**

Create an empty `scripts/docs/__init__.py`.

Create `scripts/docs/check_docs.py`:

```python
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[2]

HISTORICAL_SUPERPOWERS_MARKDOWN = (
    "docs/superpowers/plans/2026-04-29-phase-3-watchdog-and-registry.md",
    "docs/superpowers/plans/2026-04-30-gemma-enhancements.md",
    "docs/superpowers/plans/2026-05-03-claude-md-system.md",
    "docs/superpowers/plans/2026-05-06-hermes-bridge.md",
    "docs/superpowers/plans/2026-05-16-phase4-umbrella.md",
    "docs/superpowers/plans/2026-05-19-hermes-financial-agent.md",
    "docs/superpowers/plans/2026-05-31-enterprise-trading-agent-mvp.md",
    "docs/superpowers/plans/2026-07-12-agentic-edge-benchmark-suite-e1-e12.md",
    "docs/superpowers/specs/2026-04-29-phase-3-watchdog-and-registry-design.md",
    "docs/superpowers/specs/2026-04-30-gemma-enhancements-design.md",
    "docs/superpowers/specs/2026-05-03-claude-md-system-design.md",
    "docs/superpowers/specs/2026-05-05-hermes-bridge-design.md",
    "docs/superpowers/specs/2026-05-10-phase4-fleet-orchestration-umbrella-design.md",
    "docs/superpowers/specs/2026-05-18-phase4.1-bridge-adapter-template-design.md",
)

DOCUMENT_CLASSIFICATION = {
    "Current": (
        "docs/README.md",
        "docs/01-architecture.md",
        "docs/02-server-setup.md",
        "docs/02-server-setup-linux.md",
        "docs/04-dashboard.md",
        "docs/05-messaging.md",
        "docs/08-api-reference.md",
        "docs/09-monitoring.md",
        "docs/10-testing.md",
        "docs/CHANGELOG.md",
        "docs/agent-contract.md",
        "docs/agent-setup.md",
        "docs/setup-development.md",
        "docs/setup-lab-controller.md",
        "docs/setup-lab-node.md",
        "docs/research/artifact.md",
        "docs/research/results/README.md",
        "docs/adr/README.md",
        "docs/adr/template.md",
        "docs/adr/0001-nats-over-mqtt-broker.md",
        "docs/adr/0002-nats-jetstream-workqueue.md",
        "docs/adr/0003-a2a-v1-vocabulary-adoption.md",
        "docs/adr/0004-mqtt-ingress-opt-in.md",
        "docs/adr/0005-browser-scoped-token.md",
        "docs/adr/0006-outbox-mirror-authoritative.md",
        "docs/adr/0007-watchdog-trigger-model.md",
        "docs/adr/0008-centralized-memory-service.md",
        "docs/adr/0009-host-deploy-architecture.md",
        "docs/adr/0012-bridge-adapter-memory-ownership.md",
    ),
    "Experimental": (
        "docs/setup_hermes.md",
        "docs/research/task-aware-reliability-contract-design.md",
        "docs/research/nats-agent-communication-literature-review.md",
        "docs/research/r10-implementation-log.md",
    ),
    "Proposed": (
        "docs/02-server-setup-macos.md",
        "docs/roadmap.md",
        "docs/adr/0010-nats-native-l2-delegation.md",
        "docs/adr/0011-mcp-for-tool-exposure.md",
    ),
    "Historical": (
        "docs/03-agent-registration.md",
        "docs/research/nats-agent-communication-research-plan.md",
        "docs/research/nats-agent-communication-experiment-matrix.md",
        "docs/research/nats-agent-communication-baseline-results-2026-07-04.md",
        "docs/research/results/20260712T052153Z-security_temporal-security_temporal.md",
        "docs/research/plans/2026-07-25-slice-1-hermetic-experiment-spine.md",
        "docs/research/plans/2026-07-25-slice-2-deterministic-operator-journey.md",
        "docs/research/plans/2026-07-25-slice-3-multi-agent-iot-lab.md",
        "docs/research/plans/2026-07-25-slice-4-documentation-artifact.md",
    ),
}

ROOT_GOVERNANCE = ("README.md", "AGENTS.md", "CLAUDE.md", "CONTRIBUTING.md",
                   "PROGRESS.md", "deploy/README.md")
MAINTAINED_MARKDOWN = ROOT_GOVERNANCE + tuple(
    name
    for paths in DOCUMENT_CLASSIFICATION.values()
    for name in paths
)
# AGENTS.md and CLAUDE.md intentionally contain local workflow paths; they remain
# link/classification inputs but are not product-behavior claims.
CURRENT_CLAIM_MARKDOWN = (
    "README.md",
    "CONTRIBUTING.md",
    "PROGRESS.md",
    "deploy/README.md",
) + DOCUMENT_CLASSIFICATION["Current"]

RUNBOOKS = (
    "README.md",
    "docs/setup-development.md",
    "docs/setup-lab-controller.md",
    "docs/setup-lab-node.md",
    "docs/research/artifact.md",
    "docs/10-testing.md",
)

ARCHIVE_MOVES = {
    "docs/NATS_ARCHITECTURE.md":
        "docs/archive/pre-v0.1/NATS_ARCHITECTURE.md",
    "docs/06-p2p-delegation.md":
        "docs/archive/pre-v0.1/06-p2p-delegation.md",
    "docs/07-task-management.md":
        "docs/archive/pre-v0.1/07-task-management.md",
    "docs/11-future-potential.md":
        "docs/archive/pre-v0.1/11-future-potential.md",
    "docs/research/future-directions-roadmap.html":
        "docs/archive/research/future-directions-roadmap.html",
    "docs/demo.gif": "docs/archive/media/2026-03/demo.gif",
    "docs/demo.mp4": "docs/archive/media/2026-03/demo.mp4",
}

ARCHIVE_TRANSFORMS = {
    "docs/NATS_ARCHITECTURE.md": "historical-markdown-header-v1",
    "docs/06-p2p-delegation.md": "historical-markdown-header-v1",
    "docs/07-task-management.md": "historical-markdown-header-v1",
    "docs/11-future-potential.md": "historical-markdown-header-v1",
    "docs/research/future-directions-roadmap.html":
        "historical-html-aside-v1",
    "docs/demo.gif": "none",
    "docs/demo.mp4": "none",
}

ADR_EXPECTED = {
    "0001-nats-over-mqtt-broker.md":
        ("NATS over a separate MQTT broker", "Accepted",
         "Current: NATS is the broker; MQTT ingress is optional"),
    "0002-nats-jetstream-workqueue.md":
        ("JetStream WorkQueue inboxes", "Accepted",
         "Current: `AGENT_INBOX` WorkQueue stream and exact per-agent consumers"),
    "0003-a2a-v1-vocabulary-adoption.md":
        ("A2A lifecycle vocabulary", "Accepted",
         "Current: schema vocabulary is implemented; A2A transport conformance is not claimed"),
    "0004-mqtt-ingress-opt-in.md":
        ("MQTT ingress opt-in", "Accepted",
         "Current: the MQTT listener is deploy-time opt-in"),
    "0005-browser-scoped-token.md":
        ("Browser-scoped token", "Accepted",
         "Not implemented: login returns an opaque non-broker token"),
    "0006-outbox-mirror-authoritative.md":
        ("Best-effort outbox audit", "Accepted",
         "Current: best-effort audit only; not task-completion evidence"),
    "0007-watchdog-trigger-model.md":
        ("Watchdog trigger model", "Accepted",
         "Current product behavior; excluded from the paper mechanism"),
    "0008-centralized-memory-service.md":
        ("Centralized conversation memory", "Accepted",
         "Current: aggregator-hosted conversation memory is implemented"),
    "0009-host-deploy-architecture.md":
        ("Linux host deployment", "Accepted",
         "Current on the supported Linux host path; not the paper lab launcher"),
    "0010-nats-native-l2-delegation.md":
        ("NATS-native L2 delegation", "Proposed", "Not implemented"),
    "0011-mcp-for-tool-exposure.md":
        ("MCP tool exposure", "Proposed", "Not implemented"),
    "0012-bridge-adapter-memory-ownership.md":
        ("Bridge adapters retain upstream memory ownership", "Accepted",
         "Current optional-live Hermes bridge behavior"),
}

ADR_INDEX_PREAMBLE = """# Architecture Decision Records

ADRs explain why a decision was made. Runtime code, schemas, generated API
metadata, and tested configuration remain authoritative for current behavior.

| ID | Decision | Status | Implementation |
| --- | --- | --- | --- |"""


def render_adr_index() -> str:
    rows = [ADR_INDEX_PREAMBLE]
    for name, (title, status, implementation) in ADR_EXPECTED.items():
        rows.append(
            f"| [{name[:4]}]({name}) | {title} | {status} | {implementation} |"
        )
    rows.extend([
        "",
        "Use [the ADR template](template.md) for new decisions. IDs are unique "
        "and are never reused after supersession.",
        "",
    ])
    return "\n".join(rows)

FORBIDDEN_CURRENT_PATTERNS = {
    r"/api/tasks(?:\b|/)": "removed task API",
    r"tasks\.\*": "removed task subject wildcard",
    r"\bCONVERSATIONS\b": "removed conversation stream",
    r"\bAGENT_STATE\b": "removed agent-state KV bucket",
    r"mqtt-listener\.js": "removed MQTT listener",
}

EXTERNAL_COMMANDS = {
    ".", "cd", "chmod", "cp", "curl", "docker", "git", "mkdir", "rm", "scp",
    "systemctl", "test",
}

REQUIRED_RUNBOOK_OPTIONS = {
    ("run_artifact.py", "run"): {"--profile"},
    ("run_artifact.py", "cleanup"): {"--run-id"},
    ("lab_controller.py", "start"):
        {"--run-id", "--host-id", "--lab-variant"},
    ("lab_controller.py", "status"): {"--run-id"},
    ("lab_controller.py", "command"):
        {"--run-id", "--agent-id", "--body", "--expected-output"},
    ("lab_controller.py", "await"):
        {"--run-id", "--task-id", "--expected-output", "--qualification-kind"},
    ("lab_controller.py", "export-image"): {"--run-id", "--output"},
    ("lab_controller.py", "qualify"): {"--run-id"},
    ("lab_controller.py", "stop"): {"--run-id"},
    ("lab_node.py", "start"):
        {
            "--controller-config", "--credential-file", "--host-id", "--agent-id",
            "--behavior", "--delay-ms",
        },
    ("lab_node.py", "status"): {"--controller-config", "--agent-id"},
    ("lab_node.py", "doctor"):
        {"--controller-config", "--credential-file", "--host-id", "--agent-id"},
    ("lab_node.py", "stop"):
        {"--controller-config", "--credential-file", "--agent-id"},
}

RESULT_DIRECTORY_INVENTORY = {
    "raw/": ("Validated raw bundles", "Use only after checker PASS"),
    "derived/": ("Deterministic derivative", "Not independent evidence"),
    "operator/": (
        "Operator evidence",
        "Operator Evidence Ready only for a checked PASS bundle",
    ),
    "lab/": (
        "Lab evidence",
        "Preliminary unless the exact remote qualification gate passes",
    ),
}

LEGACY_RESULT_INVENTORY = {
    ".gitkeep": ("Directory placeholder", "Not evidence"),
    "20260712T052153Z-security_temporal-security_temporal.json": (
        "Functional probe",
        "Synthetic evaluator debugging only",
    ),
    "20260712T052153Z-security_temporal-security_temporal.md": (
        "Functional probe",
        "Human-readable synthetic evaluator debugging only",
    ),
}

ARCHIVE_PROVENANCE = "docs/archive/provenance.json"


@dataclass(frozen=True)
class Issue:
    code: str
    path: Path
    message: str

    def render(self, root: Path) -> str:
        try:
            path = self.path.relative_to(root)
        except ValueError:
            path = self.path
        return f"{self.code} {path}: {self.message}"


def _without_fenced_code(text: str) -> str:
    lines: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            lines.append(line)
    return "\n".join(lines)


def _heading_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for line in path.read_text().splitlines():
        match = re.match(r"^#{1,6}\s+(.+?)\s*#*$", line)
        if not match:
            continue
        heading = re.sub(r"`([^`]*)`", r"\1", match.group(1)).lower()
        heading = re.sub(r"[^\w\- ]", "", heading, flags=re.UNICODE)
        slug = re.sub(r"\s+", "-", heading.strip())
        suffix = counts.get(slug, 0)
        counts[slug] = suffix + 1
        anchors.add(slug if suffix == 0 else f"{slug}-{suffix}")
    return anchors


def check_internal_links(root: Path, paths: list[Path]) -> list[Issue]:
    issues: list[Issue] = []
    pattern = re.compile(r"!?\[[^\]]*]\(([^)]+)\)")
    for path in paths:
        prose = _without_fenced_code(path.read_text())
        for raw_target in pattern.findall(prose):
            target = raw_target.strip().strip("<>").split(" ", 1)[0]
            if target.startswith(("http://", "https://", "mailto:", "app://")):
                continue
            file_part, separator, anchor = target.partition("#")
            resolved = path if not file_part else (
                root / file_part.lstrip("/")
                if file_part.startswith("/")
                else path.parent / unquote(file_part)
            )
            resolved = resolved.resolve()
            if not resolved.exists():
                relative = resolved.relative_to(root.resolve())
                issues.append(Issue(
                    "DOC-LINK", path,
                    f"target does not exist: {relative.as_posix()}",
                ))
                continue
            if separator and anchor and resolved.suffix.lower() == ".md":
                if unquote(anchor).lower() not in _heading_anchors(resolved):
                    relative = resolved.relative_to(root.resolve())
                    issues.append(Issue(
                        "DOC-ANCHOR", path,
                        "anchor does not exist: "
                        f"{relative.as_posix()}#{unquote(anchor).lower()}",
                    ))
    return issues


SCHEMA_BLOCK = re.compile(
    r"<!--\s*schema:\s*([^\s]+)\s*-->\s*"
    r"```json\s*\n(.*?)\n```",
    re.DOTALL,
)


def check_schema_examples(root: Path, paths: list[Path]) -> list[Issue]:
    issues: list[Issue] = []
    for path in paths:
        for schema_name, payload_text in SCHEMA_BLOCK.findall(path.read_text()):
            schema_path = root / schema_name
            if not schema_path.exists():
                issues.append(Issue(
                    "DOC-SCHEMA", path,
                    f"schema does not exist: {schema_name}",
                ))
                continue
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError as error:
                issues.append(Issue("DOC-JSON", path, str(error)))
                continue
            schema = json.loads(schema_path.read_text())
            errors = sorted(
                Draft202012Validator(schema).iter_errors(payload),
                key=lambda error: list(error.absolute_path),
            )
            for error in errors:
                location = ".".join(str(part) for part in error.absolute_path)
                prefix = f"{location}: " if location else ""
                issues.append(Issue(
                    "DOC-SCHEMA", path, f"{prefix}{error.message}",
                ))
    return issues


def _adr_section(text: str, heading: str) -> str | None:
    match = re.search(
        rf"^## {re.escape(heading)}\s*$\n+([^\n]+)\s*$",
        text,
        flags=re.MULTILINE,
    )
    return match.group(1).strip() if match else None


def check_adr_set(root: Path) -> list[Issue]:
    issues: list[Issue] = []
    adr_dir = root / "docs" / "adr"
    index = adr_dir / "README.md"
    index_text = index.read_text() if index.exists() else ""
    by_id: dict[str, list[Path]] = {}
    actual_names = {
        path.name for path in adr_dir.glob("[0-9][0-9][0-9][0-9]-*.md")
    }
    for name in sorted(set(ADR_EXPECTED) - actual_names):
        issues.append(Issue("DOC-ADR", adr_dir / name, "expected ADR is missing"))
    for name in sorted(actual_names - set(ADR_EXPECTED)):
        issues.append(Issue("DOC-ADR", adr_dir / name, "unexpected numbered ADR"))
    for path in sorted(adr_dir.glob("[0-9][0-9][0-9][0-9]-*.md")):
        adr_id = path.name[:4]
        by_id.setdefault(adr_id, []).append(path)
        text = path.read_text()
        title = re.match(r"^# ADR-(\d{4}): (.+)$", text)
        expected = ADR_EXPECTED.get(path.name)
        if (
            not title
            or title.group(1) != adr_id
            or (expected is not None and title.group(2) != expected[0])
        ):
            issues.append(Issue(
                "DOC-ADR", path,
                f"H1 must be '# ADR-{adr_id}: "
                f"{expected[0] if expected else '<decision>'}'",
            ))
        status = _adr_section(text, "Status")
        implementation = _adr_section(text, "Implementation")
        if status is None:
            issues.append(Issue("DOC-ADR", path, "missing ## Status value"))
        if implementation is None:
            issues.append(Issue(
                "DOC-ADR", path, "missing ## Implementation value",
            ))
        if expected and (status, implementation) != expected[1:]:
            issues.append(Issue(
                "DOC-ADR", path,
                f"metadata must be exactly status={expected[1]!r}, "
                f"implementation={expected[2]!r}",
            ))
        index_pattern = re.compile(
            rf"^\| \[{adr_id}\]\({re.escape(path.name)}\) \| "
            rf"{re.escape(expected[0]) if expected else '[^|]+'} \| "
            rf"{re.escape(expected[1]) if expected else '[^|]+'} \| "
            rf"{re.escape(expected[2]) if expected else '[^|]+'} \|$",
            flags=re.MULTILINE,
        )
        if not index_pattern.search(index_text):
            issues.append(Issue(
                "DOC-ADR", path,
                "index row is missing or disagrees with exact ADR metadata",
            ))
    for adr_id, paths in by_id.items():
        if len(paths) > 1:
            issues.append(Issue(
                "DOC-ADR", adr_dir,
                "duplicate ADR id "
                f"{adr_id}: {', '.join(path.name for path in paths)}",
            ))
    if index_text != render_adr_index():
        issues.append(Issue(
            "DOC-ADR", index,
            "generated ADR index is stale; render it from ADR_EXPECTED",
        ))
    return issues


COMMAND_FENCE = re.compile(
    r"(?:(<!--\s*doc-command\s*-->)|"
    r"<!--\s*doc-command-ignore:\s*([^>]+?)\s*-->)\s*)?"
    r"```bash\s*\n(.*?)\n```",
    flags=re.DOTALL,
)


def _command_blocks(path: Path) -> tuple[list[str], list[Issue]]:
    blocks: list[str] = []
    issues: list[Issue] = []
    for marker, ignore_reason, body in COMMAND_FENCE.findall(path.read_text()):
        if marker:
            blocks.append(body)
        elif ignore_reason.strip():
            continue
        else:
            issues.append(Issue(
                "DOC-COMMAND", path,
                "bash block must have doc-command or doc-command-ignore: REASON",
            ))
    return blocks, issues


def _logical_command_lines(block: str) -> list[str]:
    commands: list[str] = []
    pending = ""
    for source_line in block.splitlines():
        line = source_line.strip()
        if not line or line.startswith("#"):
            continue
        pending = f"{pending} {line}".strip()
        if pending.endswith("\\"):
            pending = pending[:-1].rstrip()
            continue
        commands.append(pending)
        pending = ""
    if pending:
        commands.append(pending)
    return commands


def _strip_assignments(tokens: list[str]) -> list[str]:
    while tokens and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", tokens[0]):
        tokens.pop(0)
    return tokens


def _probe_python_help(root: Path, tokens: list[str]) -> str | None:
    if tokens[0] == "scripts/research/run-python":
        wrapper = root / tokens[0]
        if not wrapper.is_file() or not os.access(wrapper, os.X_OK):
            return (
                "command path is missing or not executable: "
                "scripts/research/run-python"
            )
        tokens = ["python3", *tokens[1:]]
    if tokens[:2] == ["python3", "-m"]:
        if len(tokens) < 3 or importlib.util.find_spec(tokens[2]) is None:
            return f"Python module does not exist: {tokens[2] if len(tokens) > 2 else ''}"
        prefix = [sys.executable, *tokens[1:3]]
        remainder = tokens[3:]
    else:
        script = root / tokens[1]
        if not script.exists():
            return f"command path does not exist: {tokens[1]}"
        prefix = [sys.executable, tokens[1]]
        remainder = tokens[2:]
    declared_subcommands = {
        "run_artifact.py": {"run", "cleanup"},
        "lab_controller.py": {
            "start", "status", "command", "await", "export-image", "qualify",
            "stop",
        },
        "lab_node.py": {"start", "status", "doctor", "stop"},
    }
    program = Path(prefix[-1]).name
    subcommand = (
        remainder[0]
        if remainder and remainder[0] in declared_subcommands.get(program, set())
        else None
    )
    probe = [*prefix, *([subcommand] if subcommand else []), "--help"]
    env = os.environ.copy()
    env.pop("LAB_RUN_ID", None)
    with tempfile.TemporaryDirectory() as temporary:
        env["DB_PATH"] = str(Path(temporary) / "docs-command.db")
        result = subprocess.run(
            probe, cwd=root, env=env, text=True, capture_output=True,
            timeout=20, check=False,
        )
    help_text = result.stdout + result.stderr
    if result.returncode != 0:
        return f"help probe failed ({result.returncode}): {' '.join(probe)}"
    missing = sorted({
        token.split("=", 1)[0]
        for token in remainder
        if token.startswith("--") and token.split("=", 1)[0] not in help_text
    })
    if missing:
        return f"documented option absent from help: {', '.join(missing)}"
    present = {
        token.split("=", 1)[0]
        for token in remainder
        if token.startswith("--")
    }
    required_missing = sorted(
        REQUIRED_RUNBOOK_OPTIONS.get((program, subcommand), set()) - present
    )
    if required_missing:
        return (
            "documented command lacks required option: "
            + ", ".join(required_missing)
        )
    return None


def check_commands(root: Path, paths: list[Path]) -> list[Issue]:
    issues: list[Issue] = []
    for path in paths:
        blocks, tag_issues = _command_blocks(path)
        issues.extend(tag_issues)
        for block in blocks:
            for command in _logical_command_lines(block):
                try:
                    tokens = _strip_assignments(shlex.split(command))
                except ValueError as error:
                    issues.append(Issue("DOC-COMMAND", path, str(error)))
                    continue
                if not tokens:
                    continue
                executable = tokens[0]
                if executable in {
                    "python3", "scripts/research/run-python",
                } and len(tokens) > 1:
                    error = _probe_python_help(root, tokens)
                    if error:
                        issues.append(Issue(
                            "DOC-COMMAND", path, error,
                        ))
                elif executable == "npm" and "--prefix" in tokens:
                    prefix = tokens[tokens.index("--prefix") + 1]
                    package_path = root / prefix / "package.json"
                    if not package_path.exists():
                        issues.append(Issue(
                            "DOC-COMMAND", path,
                            f"package does not exist: {prefix}/package.json",
                        ))
                        continue
                    if "run" in tokens:
                        script_name = tokens[tokens.index("run") + 1]
                        package = json.loads(package_path.read_text())
                        if script_name not in package.get("scripts", {}):
                            issues.append(Issue(
                                "DOC-COMMAND", path,
                                f"npm script does not exist: "
                                f"{prefix}:{script_name}",
                            ))
                elif executable.startswith("./"):
                    script = root / executable[2:]
                    if not script.exists():
                        issues.append(Issue(
                            "DOC-COMMAND", path,
                            f"command path does not exist: {executable[2:]}",
                        ))
                elif executable not in EXTERNAL_COMMANDS:
                    issues.append(Issue(
                        "DOC-COMMAND", path,
                        f"unsupported documented command: {executable}",
                    ))
    return issues


def check_no_bare_host_python(paths: list[Path]) -> list[Issue]:
    issues: list[Issue] = []
    for path in paths:
        blocks, _ = _command_blocks(path)
        for block in blocks:
            for command in _logical_command_lines(block):
                try:
                    tokens = _strip_assignments(shlex.split(command))
                except ValueError:
                    continue
                stale = (
                    tokens[:3] == ["python3", "-m", "pytest"]
                    or (
                        len(tokens) > 1
                        and tokens[0] == "python3"
                        and tokens[1].startswith(
                            ("scripts/docs/", "scripts/research/")
                        )
                    )
                )
                if stale:
                    issues.append(Issue(
                        "DOC-COMMAND", path,
                        "repository Python command must use "
                        "scripts/research/run-python",
                    ))
    return issues


def check_exact_archives(root: Path) -> list[Issue]:
    issues: list[Issue] = []
    provenance_path = root / ARCHIVE_PROVENANCE
    provenance: dict[str, object] = {}
    if not provenance_path.exists():
        issues.append(Issue(
            "DOC-ARCHIVE", provenance_path,
            "archive provenance manifest is missing",
        ))
    else:
        try:
            provenance = json.loads(provenance_path.read_text())
        except (json.JSONDecodeError, OSError) as error:
            issues.append(Issue(
                "DOC-ARCHIVE", provenance_path,
                f"archive provenance is invalid JSON: {error}",
            ))
    entries = provenance.get("entries", []) if isinstance(provenance, dict) else []
    if not isinstance(entries, list):
        entries = []
        issues.append(Issue(
            "DOC-ARCHIVE", provenance_path,
            "archive provenance entries must be an array",
        ))
    by_source = {
        entry.get("source"): entry
        for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("source"), str)
    }
    if set(by_source) != set(ARCHIVE_MOVES):
        issues.append(Issue(
            "DOC-ARCHIVE", provenance_path,
            "archive provenance source set must equal ARCHIVE_MOVES exactly",
        ))
    for source_name, destination_name in ARCHIVE_MOVES.items():
        source = root / source_name
        destination = root / destination_name
        if source.exists():
            issues.append(Issue(
                "DOC-ARCHIVE", source,
                f"historical source still active; move to {destination_name}",
            ))
        if not destination.exists():
            issues.append(Issue(
                "DOC-ARCHIVE", destination,
                f"archive destination is missing: {destination_name}",
            ))
        entry = by_source.get(source_name)
        if entry is None:
            continue
        expected_fields = {
            "source", "destination", "source_sha256", "archived_sha256",
            "transformation",
        }
        if set(entry) != expected_fields:
            issues.append(Issue(
                "DOC-ARCHIVE", provenance_path,
                f"{source_name} provenance fields must be exact",
            ))
            continue
        if (
            entry["destination"] != destination_name
            or entry["transformation"] != ARCHIVE_TRANSFORMS[source_name]
        ):
            issues.append(Issue(
                "DOC-ARCHIVE", provenance_path,
                f"{source_name} destination/transformation disagrees with contract",
            ))
        for field in ("source_sha256", "archived_sha256"):
            if not re.fullmatch(r"[0-9a-f]{64}", str(entry[field])):
                issues.append(Issue(
                    "DOC-ARCHIVE", provenance_path,
                    f"{source_name} has invalid {field}",
                ))
        if destination.exists():
            actual = hashlib.sha256(destination.read_bytes()).hexdigest()
            if actual != entry["archived_sha256"]:
                issues.append(Issue(
                    "DOC-ARCHIVE", destination,
                    "archive bytes disagree with archived_sha256",
                ))
        if (
            entry["transformation"] == "none"
            and entry["source_sha256"] != entry["archived_sha256"]
        ):
            issues.append(Issue(
                "DOC-ARCHIVE", destination,
                "byte-preserved archive changed from its source hash",
            ))
    return issues


def check_current_claims(root: Path, paths: list[Path]) -> list[Issue]:
    issues: list[Issue] = []
    for path in paths:
        text = path.read_text()
        prose = _without_fenced_code(text)
        for pattern, description in FORBIDDEN_CURRENT_PATTERNS.items():
            if re.search(pattern, prose):
                issues.append(Issue(
                    "DOC-LEGACY", path,
                    f"active prose contains {description}",
                ))
        if re.search(r"/Users/[A-Za-z0-9._-]+/", text):
            issues.append(Issue(
                "DOC-PATH", path, "active prose contains a personal macOS path",
            ))
        if re.search(r"/home/[A-Za-z0-9._-]+/", text):
            issues.append(Issue(
                "DOC-PATH", path, "active prose contains a personal Linux path",
            ))
        if re.search(
            r"(?i)(?:token|password)\s*[:=]\s*"
            r"(?:changeme|replace-me|your-token|example-token)",
            text,
        ):
            issues.append(Issue(
                "DOC-CREDENTIAL", path,
                "active prose contains a literal dummy credential",
            ))
        if re.search(
            r"(?i)(authorization:\s*bearer\s+\S+|"
            r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----)",
            text,
        ):
            issues.append(Issue(
                "DOC-CREDENTIAL", path,
                "active document contains bearer/private-key material",
            ))
    return issues


def _inventory_rows(text: str) -> dict[str, tuple[str, str]]:
    rows: dict[str, tuple[str, str]] = {}
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 3 or cells[0] in {"File", "Path", "---"}:
            continue
        if all(set(cell) <= {"-", ":"} for cell in cells):
            continue
        name = cells[0].strip("`")
        rows[name] = (cells[1], cells[2])
    return rows


def _validate_checked_bundle(
    root: Path, manifest: Path,
) -> list[Issue]:
    issues: list[Issue] = []
    try:
        payload = json.loads(manifest.read_text())
    except (json.JSONDecodeError, OSError) as error:
        return [Issue(
            "DOC-PROVENANCE", manifest, f"manifest is unreadable: {error}",
        )]
    evidence_kind = payload.get("evidence_kind")
    commit = payload.get("source", {}).get("commit")
    if evidence_kind not in {"benchmark", "operator", "lab"}:
        return [Issue(
            "DOC-PROVENANCE", manifest,
            "manifest evidence_kind must be benchmark, operator, or lab",
        )]
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        return [Issue(
            "DOC-PROVENANCE", manifest,
            "manifest source.commit must be a full Git object ID",
        )]
    reachable = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
        cwd=root, capture_output=True, text=True, check=False,
    )
    if reachable.returncode != 0:
        return [Issue(
            "DOC-PROVENANCE", manifest,
            f"recorded source commit is unavailable: {commit}",
        )]
    from scripts.research.check_artifact import check_bundle
    with tempfile.TemporaryDirectory(prefix="edgecitadel-doc-source-") as temporary:
        source_root = Path(temporary) / "source"
        added = subprocess.run(
            ["git", "worktree", "add", "--detach", str(source_root), commit],
            cwd=root, capture_output=True, text=True, check=False,
        )
        if added.returncode != 0:
            return [Issue(
                "DOC-PROVENANCE", manifest,
                f"cannot materialize recorded source commit: {added.stderr.strip()}",
            )]
        try:
            report = check_bundle(
                manifest.parent,
                expected_kind=evidence_kind,
                source_root=source_root.resolve(),
            )
            report.require_valid()
        except Exception as error:
            issues.append(Issue(
                "DOC-PROVENANCE", manifest,
                f"checked-in evidence bundle is invalid: {error}",
            ))
        finally:
            removed = subprocess.run(
                ["git", "worktree", "remove", "--force", str(source_root)],
                cwd=root, capture_output=True, text=True, check=False,
            )
            if removed.returncode != 0:
                issues.append(Issue(
                    "DOC-PROVENANCE", manifest,
                    f"temporary source cleanup failed: {removed.stderr.strip()}",
                ))
    return issues


def check_result_labels(root: Path) -> list[Issue]:
    issues: list[Issue] = []
    result_dir = root / "docs" / "research" / "results"
    index = result_dir / "README.md"
    if not result_dir.exists() or not index.exists():
        return [Issue(
            "DOC-MISSING", index, "research result inventory is missing",
        )]
    index_text = index.read_text() if index.exists() else ""
    inventory = _inventory_rows(index_text)
    required_inventory = {
        **RESULT_DIRECTORY_INVENTORY,
        **LEGACY_RESULT_INVENTORY,
    }
    for name, expected in required_inventory.items():
        if inventory.get(name) != expected:
            issues.append(Issue(
                "DOC-RESULT", index,
                f"{name} inventory row must be exactly {expected!r}",
            ))
    for path in sorted(result_dir.iterdir()):
        if path.name == "README.md":
            continue
        inventory_name = f"{path.name}/" if path.is_dir() else path.name
        if inventory_name not in required_inventory:
            issues.append(Issue(
                "DOC-RESULT", path,
                "result path is absent from the exact evidence inventory",
            ))
        if path.suffix == ".md" and (
            "> **Evidence status:** Functional probe" not in path.read_text()
        ):
            issues.append(Issue(
                "DOC-RESULT", path,
                "legacy Markdown result lacks Functional probe label",
            ))
    baseline = (
        root / "docs" / "research"
        / "nats-agent-communication-baseline-results-2026-07-04.md"
    )
    if baseline.exists() and (
        "> **Evidence status:** Functional probe" not in baseline.read_text()
    ):
        issues.append(Issue(
            "DOC-RESULT", baseline,
            "baseline result lacks Functional probe label",
        ))
    legacy_json = (
        result_dir
        / "20260712T052153Z-security_temporal-security_temporal.json"
    )
    if legacy_json.exists():
        payload = json.loads(legacy_json.read_text())
        required = {
            "run_id", "started_at", "ended_at", "environment",
            "mode", "workload", "suite_version",
        }
        missing = sorted(required - payload.keys())
        if missing:
            issues.append(Issue(
                "DOC-PROVENANCE", legacy_json,
                f"legacy functional probe lacks provenance: {', '.join(missing)}",
            ))
    for manifest in sorted(result_dir.rglob("manifest.json")):
        issues.extend(_validate_checked_bundle(root, manifest))
    return issues


def check_document_classification(root: Path) -> list[Issue]:
    issues: list[Issue] = []
    for name in MAINTAINED_MARKDOWN:
        if not (root / name).exists():
            issues.append(Issue(
                "DOC-MISSING", root / name, "maintained file is missing",
            ))
    flattened = [
        name for names in DOCUMENT_CLASSIFICATION.values() for name in names
    ]
    for name, count in Counter(flattened).items():
        if count != 1:
            issues.append(Issue(
                "DOC-CLASS", root / name,
                f"document appears in {count} classifications",
            ))
    discovered = {
        relative
        for path in (root / "docs").rglob("*.md")
        if "archive" not in path.relative_to(root / "docs").parts
        if (
            relative := path.relative_to(root).as_posix()
        ) not in HISTORICAL_SUPERPOWERS_MARKDOWN
    }
    expected = set(flattened)
    for name in sorted(discovered - expected):
        issues.append(Issue(
            "DOC-CLASS", root / name,
            "active Markdown is absent from DOCUMENT_CLASSIFICATION",
        ))
    index = root / "docs" / "README.md"
    if not index.exists():
        return [*issues, Issue("DOC-MISSING", index, "classification index is missing")]
    text = index.read_text()
    sections = {
        label: text.split(f"## {label}", 1)[1].split("\n## ", 1)[0]
        if f"## {label}" in text else ""
        for label in DOCUMENT_CLASSIFICATION
    }
    for label, names in DOCUMENT_CLASSIFICATION.items():
        for name in names:
            if name == "docs/README.md":
                continue
            target = name.removeprefix("docs/")
            counts = {
                section_label: section.count(f"]({target})")
                for section_label, section in sections.items()
            }
            if counts[label] != 1 or sum(counts.values()) != 1:
                issues.append(Issue(
                    "DOC-CLASS", index,
                    f"{name} must appear exactly once under ## {label}",
                ))
    return issues


def maintained_paths(root: Path) -> list[Path]:
    return [root / name for name in MAINTAINED_MARKDOWN if (root / name).exists()]


def run_all(root: Path = ROOT) -> list[Issue]:
    paths = maintained_paths(root)
    runbooks = [root / name for name in RUNBOOKS if (root / name).exists()]
    current_paths = [
        root / name for name in CURRENT_CLAIM_MARKDOWN if (root / name).exists()
    ]
    issues: list[Issue] = []
    issues.extend(check_document_classification(root))
    issues.extend(check_internal_links(root, paths))
    issues.extend(check_schema_examples(root, paths))
    issues.extend(check_adr_set(root))
    issues.extend(check_commands(root, runbooks))
    issues.extend(check_no_bare_host_python(runbooks))
    issues.extend(check_exact_archives(root))
    issues.extend(check_current_claims(root, current_paths))
    issues.extend(check_result_labels(root))
    return sorted(
        issues,
        key=lambda item: (item.code, str(item.path), item.message),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the maintained EdgeCitadel documentation contract.",
    )
    parser.add_argument(
        "--root", type=Path, default=ROOT,
        help="Repository root; defaults to the checkout containing this script.",
    )
    args = parser.parse_args()
    issues = run_all(args.root.resolve())
    for issue in issues:
        print(issue.render(args.root.resolve()))
    if issues:
        print(f"documentation checks failed: {len(issues)} issue(s)")
        return 1
    print(
        "documentation checks passed: "
        f"{len(MAINTAINED_MARKDOWN)} maintained files",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run the checker unit tests**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_check_docs.py -q
```

Expected:

```text
12 passed
```

- [ ] **Step 6: Commit the checker**

```bash
git add -- scripts/docs/__init__.py scripts/docs/check_docs.py \
  tests/docs/test_check_docs.py
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "test(infra): add documentation contract checker"
```

### Task 2: Generate and check the HTTP/WebSocket reference

**Files:**

- Create: `scripts/docs/render_api_reference.py`
- Create: `tests/docs/test_render_api_reference.py`
- Modify: `docs/08-api-reference.md`

- [ ] **Step 1: Write the renderer tests**

Create `tests/docs/test_render_api_reference.py`:

```python
from pathlib import Path

from scripts.docs.render_api_reference import (
    replace_generated_block,
    render_reference,
)


def test_replace_generated_block_is_deterministic():
    original = (
        "# API\n\n"
        "<!-- BEGIN GENERATED: API SURFACE -->\n"
        "old\n"
        "<!-- END GENERATED: API SURFACE -->\n"
    )
    expected = (
        "# API\n\n"
        "<!-- BEGIN GENERATED: API SURFACE -->\n"
        "new\n"
        "<!-- END GENERATED: API SURFACE -->\n"
    )

    assert replace_generated_block(original, "new") == expected


def test_rendered_reference_contains_current_surface(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "openapi.db"))

    rendered, counts = render_reference()

    assert counts["default_http"] > 0
    assert counts["lab_only_http"] > 0
    assert counts["websocket"] > 0
    assert counts["events"] > 0
    assert "| `GET` | `/api/system/status` |" in rendered
    assert "| `POST` | `/api/command/{agent_id}` |" in rendered
    assert "| `GET` | `/api/lab/status` |" in rendered
    assert "| `POST` | `/api/lab/reservations` |" in rendered
    assert "| `DELETE` | `/api/lab/reservations/{agent_id}` |" in rendered
    assert "| `POST` | `/api/lab/node-reports` |" in rendered
    assert "| `/ws/stream` |" in rendered
    assert "| `agent_status_change` |" in rendered


def test_rendered_reference_is_independent_of_ambient_lab_env(monkeypatch):
    monkeypatch.setenv("DB_PATH", "/unusable/ambient.db")
    monkeypatch.setenv("LAB_RUN_ID", "ambient-run")
    monkeypatch.setenv("LAB_TOKEN_SHA256", "invalid")
    monkeypatch.setenv("LAB_INVENTORY_PATH", "relative.db")

    first, first_counts = render_reference()
    second, second_counts = render_reference()

    assert first == second
    assert first_counts == second_counts
```

- [ ] **Step 2: Run the tests to verify the renderer is absent**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_render_api_reference.py -q
```

Expected: collection fails with
`ModuleNotFoundError: No module named 'scripts.docs.render_api_reference'`.

- [ ] **Step 3: Implement the deterministic renderer**

Create `scripts/docs/render_api_reference.py`:

```python
from __future__ import annotations

import argparse
import ast
import os
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / "docs" / "08-api-reference.md"
BEGIN = "<!-- BEGIN GENERATED: API SURFACE -->"
END = "<!-- END GENERATED: API SURFACE -->"
METHOD_ORDER = {"get": 0, "post": 1, "put": 2, "patch": 3, "delete": 4}


def _application(*, lab: bool):
    database = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    inventory = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    database.close()
    inventory.close()
    keys = (
        "DB_PATH", "LAB_RUN_ID", "LAB_TOKEN_SHA256", "LAB_INVENTORY_PATH",
    )
    previous = {key: os.environ.get(key) for key in keys}
    try:
        os.environ["DB_PATH"] = database.name
        if lab:
            os.environ["LAB_RUN_ID"] = "docs-reference"
            os.environ["LAB_TOKEN_SHA256"] = "0" * 64
            os.environ["LAB_INVENTORY_PATH"] = str(Path(inventory.name).resolve())
        else:
            for key in keys[1:]:
                os.environ.pop(key, None)
        from aggregator.main import make_app
        app = make_app(for_testing=True)
        return app, (Path(database.name), Path(inventory.name))
    except BaseException:
        Path(database.name).unlink(missing_ok=True)
        Path(inventory.name).unlink(missing_ok=True)
        raise
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _event_names() -> list[str]:
    names = {"message"}
    for source_name in ("aggregator/main.py", "aggregator/aggregator.py"):
        tree = ast.parse((ROOT / source_name).read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            function = node.func
            if not isinstance(function, ast.Attribute):
                continue
            if function.attr not in {"broadcast_event", "_hub_event"}:
                continue
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                names.add(first.value)
    return sorted(names)


def _operations(app) -> list[tuple[str, str, str]]:
    operations: list[tuple[str, str, str]] = []
    for path, methods in app.openapi()["paths"].items():
        for method, operation in methods.items():
            if method not in METHOD_ORDER:
                continue
            operations.append((
                method.upper(), path, operation.get("summary", ""),
            ))
    return sorted(
        operations, key=lambda row: (row[1], METHOD_ORDER[row[0].lower()]),
    )


def render_reference() -> tuple[str, dict[str, int]]:
    default_app, default_paths = _application(lab=False)
    lab_app, lab_paths = _application(lab=True)
    try:
        operations = _operations(default_app)
        default_keys = {(method, path) for method, path, _ in operations}
        lab_only = [
            row for row in _operations(lab_app)
            if (row[0], row[1]) not in default_keys
        ]

        websocket_paths = sorted(
            route.path
            for route in default_app.routes
            if route.__class__.__name__ == "APIWebSocketRoute"
        )
        events = _event_names()

        lines = [
            "### HTTP operations",
            "",
            "| Method | Path | FastAPI summary |",
            "| --- | --- | --- |",
        ]
        lines.extend(
            f"| `{method}` | `{path}` | {summary} |"
            for method, path, summary in operations
        )
        lines.extend([
            "",
            "### Conditional lab HTTP operations",
            "",
            "These routes are present only when `LAB_RUN_ID` is set.",
            "",
            "| Method | Path | FastAPI summary |",
            "| --- | --- | --- |",
        ])
        lines.extend(
            f"| `{method}` | `{path}` | {summary} |"
            for method, path, summary in lab_only
        )
        lines.extend([
            "",
            "### WebSocket routes",
            "",
            "| Path | Scope |",
            "| --- | --- |",
        ])
        for path in websocket_paths:
            scope = (
                "Fleet-wide events"
                if path == "/ws/stream"
                else "Events matching the selected agent"
            )
            lines.append(f"| `{path}` | {scope} |")
        lines.extend([
            "",
            "### WebSocket event names",
            "",
            "| Event | Payload |",
            "| --- | --- |",
        ])
        payloads = {
            "message": "Canonical envelope",
            "agent_registered": "Agent ID and Agent Card",
            "agent_status_change": "Agent ID and canonical agent state",
            "agent_deleted": "Agent ID",
            "log": "Level, message, source, and agent ID",
        }
        lines.extend(
            f"| `{event}` | {payloads[event]} |" for event in events
        )
        return "\n".join(lines), {
            "default_http": len(operations),
            "lab_only_http": len(lab_only),
            "websocket": len(websocket_paths),
            "events": len(events),
        }
    finally:
        for path in (*default_paths, *lab_paths):
            path.unlink(missing_ok=True)


def replace_generated_block(document: str, generated: str) -> str:
    if BEGIN not in document or END not in document:
        raise ValueError(
            f"{REFERENCE.relative_to(ROOT)} must contain {BEGIN!r} and {END!r}",
        )
    prefix, remainder = document.split(BEGIN, 1)
    _, suffix = remainder.split(END, 1)
    return f"{prefix}{BEGIN}\n{generated}\n{END}{suffix}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render or check the generated API surface reference.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()

    generated, counts = render_reference()
    current = REFERENCE.read_text()
    expected = replace_generated_block(current, generated)
    if args.write:
        REFERENCE.write_text(expected)
        print(
            "updated docs/08-api-reference.md: "
            f"{counts['default_http']} default HTTP operations, "
            f"{counts['lab_only_http']} conditional lab operations, "
            f"{counts['websocket']} WebSocket routes, "
            f"{counts['events']} WebSocket events",
        )
        return 0
    if current != expected:
        print(
            "generated API surface is stale; run "
            "scripts/research/run-python "
            "scripts/docs/render_api_reference.py --write",
        )
        return 1
    print(
        "generated API surface is current: "
        f"{counts['default_http']} default HTTP operations, "
        f"{counts['lab_only_http']} conditional lab operations, "
        f"{counts['websocket']} WebSocket routes, "
        f"{counts['events']} WebSocket events",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the renderer unit tests**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_render_api_reference.py -q
```

Expected:

```text
3 passed
```

- [ ] **Step 5: Establish the generated block in the API reference**

Replace the existing endpoint index and WebSocket event prose in
`docs/08-api-reference.md` with:

````markdown
## Generated Surface

This block is generated from FastAPI routes and literal WebSocket event emitters.
Run
`scripts/research/run-python scripts/docs/render_api_reference.py --check`
to detect drift.

<!-- BEGIN GENERATED: API SURFACE -->
<!-- END GENERATED: API SURFACE -->
````

Retain hand-written endpoint details below the generated block, but correct them
to use `{agent_id}`, the `sender_id` command query parameter, `context_id`,
`skill_id`, the offline `409`, message `deployment`/`exclude_deployment` filters,
`/api/conversations`, `/api/openclaw/login`, `/ws/stream`, and
`/ws/agent/{agent_id}`. Label the OpenClaw login response as a current
non-broker-provisioning stub; do not describe the returned opaque token as a
working scoped NATS credential.

Run:

```bash
scripts/research/run-python scripts/docs/render_api_reference.py --write
scripts/research/run-python scripts/docs/render_api_reference.py --check
```

Expected: both commands report derived nonzero default HTTP/WebSocket/event
counts and the complete conditional Slice 3 lab route set. Do not freeze the
default route or event totals in prose; runtime metadata owns those counts.

- [ ] **Step 6: Commit the generated-reference contract**

```bash
git add -- scripts/docs/render_api_reference.py \
  tests/docs/test_render_api_reference.py
git add -- docs/08-api-reference.md
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(aggregator): generate the API surface reference"
```

### Task 3: Move the exact historical set and label it

**Files:**

- Create: `tests/docs/test_documentation_contract.py`
- Create: `docs/archive/README.md`
- Create: `docs/archive/provenance.json`
- Create: `docs/archive/media/2026-03/README.md`
- Move: the seven historical paths listed in the File Map; the ADR renumber is
  Task 4
- Modify: `PROGRESS.md`

- [ ] **Step 1: Write the failing archive-contract test**

Create `tests/docs/test_documentation_contract.py`:

```python
import subprocess
from pathlib import Path

from scripts.docs.check_docs import (
    CURRENT_CLAIM_MARKDOWN,
    HISTORICAL_SUPERPOWERS_MARKDOWN,
    MAINTAINED_MARKDOWN,
    check_adr_set,
    check_commands,
    check_current_claims,
    check_document_classification,
    check_exact_archives,
    check_internal_links,
    check_no_bare_host_python,
    check_result_labels,
    check_schema_examples,
)
from scripts.docs.render_api_reference import main as render_api_main


ROOT = Path(__file__).resolve().parents[2]


def assert_clean(issues):
    assert not issues, "\n".join(
        issue.render(ROOT) for issue in issues
    )


def test_exact_archive_contract():
    assert_clean(check_exact_archives(ROOT))


def test_historical_superpowers_inventory_is_exact_when_present():
    local_root = ROOT / "docs" / "superpowers"
    local_markdown = tuple(sorted(
        path.relative_to(ROOT).as_posix()
        for path in local_root.rglob("*.md")
    )) if local_root.exists() else ()
    if local_markdown:
        assert local_markdown == HISTORICAL_SUPERPOWERS_MARKDOWN
    tracked = subprocess.run(
        ["git", "ls-files", "--", "docs/superpowers"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.splitlines()
    tracked_markdown = tuple(sorted(
        name for name in tracked if name.endswith(".md")
    ))
    assert set(tracked_markdown).issubset(HISTORICAL_SUPERPOWERS_MARKDOWN)
    assert set(HISTORICAL_SUPERPOWERS_MARKDOWN).isdisjoint(
        MAINTAINED_MARKDOWN
    )
```

- [ ] **Step 2: Run the archive test and verify it lists every source/destination**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_exact_archive_contract -q
```

Expected: `FAIL`; the output lists seven active source files, seven missing
archive destinations, and the missing archive provenance manifest.

- [ ] **Step 3: Move only the design-approved historical files**

Run:

```bash
mkdir -p docs/archive/pre-v0.1 docs/archive/research \
  docs/archive/media/2026-03
WORKTREE_KEY="$(
  printf '%s\0' "$(git rev-parse --absolute-git-dir)" |
    shasum -a 256 | awk '{print substr($1, 1, 16)}'
)"
SLICE4_STATE_ROOT="/tmp/edgecitadel-slice4-adoption-$WORKTREE_KEY"
export SLICE4_STATE_ROOT
test -f "$SLICE4_STATE_ROOT/base-commit.txt"
scripts/research/run-python - <<'PY'
import hashlib
import json
import os
from pathlib import Path

root = Path.cwd()
moves = {
    "docs/NATS_ARCHITECTURE.md":
        ("docs/archive/pre-v0.1/NATS_ARCHITECTURE.md",
         "historical-markdown-header-v1"),
    "docs/06-p2p-delegation.md":
        ("docs/archive/pre-v0.1/06-p2p-delegation.md",
         "historical-markdown-header-v1"),
    "docs/07-task-management.md":
        ("docs/archive/pre-v0.1/07-task-management.md",
         "historical-markdown-header-v1"),
    "docs/11-future-potential.md":
        ("docs/archive/pre-v0.1/11-future-potential.md",
         "historical-markdown-header-v1"),
    "docs/research/future-directions-roadmap.html":
        ("docs/archive/research/future-directions-roadmap.html",
         "historical-html-aside-v1"),
    "docs/demo.gif": ("docs/archive/media/2026-03/demo.gif", "none"),
    "docs/demo.mp4": ("docs/archive/media/2026-03/demo.mp4", "none"),
}
source = {}
for name, (destination, transformation) in moves.items():
    path = root / name
    assert path.is_file(), name
    source[name] = {
        "destination": destination,
        "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "transformation": transformation,
    }
(Path(os.environ["SLICE4_STATE_ROOT"]) / "archive-source.json").write_text(
    json.dumps(source, indent=2, sort_keys=True) + "\n"
)
PY
git mv docs/NATS_ARCHITECTURE.md \
  docs/archive/pre-v0.1/NATS_ARCHITECTURE.md
git mv docs/06-p2p-delegation.md \
  docs/archive/pre-v0.1/06-p2p-delegation.md
git mv docs/07-task-management.md \
  docs/archive/pre-v0.1/07-task-management.md
git mv docs/11-future-potential.md \
  docs/archive/pre-v0.1/11-future-potential.md
if git ls-files --error-unmatch \
  docs/research/future-directions-roadmap.html >/dev/null 2>&1; then
  git mv docs/research/future-directions-roadmap.html \
    docs/archive/research/future-directions-roadmap.html
else
  mv docs/research/future-directions-roadmap.html \
    docs/archive/research/future-directions-roadmap.html
  git add -- docs/archive/research/future-directions-roadmap.html
fi
git mv docs/demo.gif docs/archive/media/2026-03/demo.gif
git mv docs/demo.mp4 docs/archive/media/2026-03/demo.mp4
```

Expected: all seven moves exit `0`. The roadmap uses `mv` plus an exact add when
it is an untracked provenance file; no broad add or clean operation is permitted.

- [ ] **Step 4: Add visible historical labels**

Insert this block immediately below the H1 in each archived Markdown file:

````markdown
> **Document status:** Historical pre-v0.1 material.
>
> This file is retained for decision archaeology. It does not describe the
> current runtime. Start at [the documentation index](../README.md).
````

For files under `docs/archive/pre-v0.1/`, the relative index link is
`../../README.md`, not `../README.md`.

Insert this exact element immediately after `<body>` in
`docs/archive/research/future-directions-roadmap.html`:

```html
<aside role="note">
  Historical research roadmap: retained for provenance and not publication
  evidence. Current artifact design:
  <code>docs/research/task-aware-reliability-contract-design.md</code>.
</aside>
```

Create `docs/archive/README.md`:

````markdown
# Documentation Archive

> **Document status:** Historical.

Archived files preserve superseded architecture, product, research-planning, and
media context. They are not authorities for current behavior and are excluded from
the maintained-document contract.

| Area | Contents |
| --- | --- |
| `pre-v0.1/` | Superseded architecture, delegation, task-management, and future-feature prose |
| `research/` | Superseded research roadmap presentations |
| `media/2026-03/` | Dashboard media captured before the task-aware artifact |

Use [the current documentation index](../README.md) for supported behavior,
runbooks, and evidence labels.
````

Create `docs/archive/media/2026-03/README.md`:

````markdown
# March 2026 Dashboard Media

> **Evidence status:** Historical demonstration.

`demo.gif` and `demo.mp4` predate the deterministic operator journey. They are
retained for provenance and must not be presented as current operator or paper
evidence.
````

Update the completed architecture-document item in `PROGRESS.md` to link to
`docs/archive/pre-v0.1/NATS_ARCHITECTURE.md` and label it historical.

Generate the archive provenance manifest only after every label is in its final
form. This is a mechanical hash inventory, not a hand-edited claim:

```bash
WORKTREE_KEY="$(
  printf '%s\0' "$(git rev-parse --absolute-git-dir)" |
    shasum -a 256 | awk '{print substr($1, 1, 16)}'
)"
SLICE4_STATE_ROOT="/tmp/edgecitadel-slice4-adoption-$WORKTREE_KEY"
export SLICE4_STATE_ROOT
test -f "$SLICE4_STATE_ROOT/archive-source.json"
scripts/research/run-python - <<'PY'
import hashlib
import json
import os
from pathlib import Path

root = Path.cwd()
source = json.loads(
    (Path(os.environ["SLICE4_STATE_ROOT"]) / "archive-source.json").read_text()
)
entries = []
for name in sorted(source):
    record = source[name]
    destination = root / record["destination"]
    assert destination.is_file(), destination
    entries.append({
        "source": name,
        "destination": record["destination"],
        "source_sha256": record["source_sha256"],
        "archived_sha256":
            hashlib.sha256(destination.read_bytes()).hexdigest(),
        "transformation": record["transformation"],
    })
(root / "docs/archive/provenance.json").write_text(
    json.dumps(
        {"schema_version": 1, "entries": entries},
        indent=2,
        sort_keys=True,
    ) + "\n"
)
PY
```

- [ ] **Step 5: Re-run the archive test**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_exact_archive_contract -q
```

Expected:

```text
1 passed
```

- [ ] **Step 6: Commit the archive move**

```bash
git add -- docs/archive/pre-v0.1/NATS_ARCHITECTURE.md \
  docs/archive/pre-v0.1/06-p2p-delegation.md \
  docs/archive/pre-v0.1/07-task-management.md \
  docs/archive/pre-v0.1/11-future-potential.md \
  docs/archive/research/future-directions-roadmap.html \
  docs/archive/media/2026-03/demo.gif \
  docs/archive/media/2026-03/demo.mp4 \
  docs/archive/README.md docs/archive/provenance.json \
  docs/archive/media/2026-03/README.md \
  tests/docs/test_documentation_contract.py
git add -- PROGRESS.md
git diff --cached --name-only
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(infra): archive superseded architecture and media"
```

### Task 4: Resolve ADR identity and implementation-status drift

**Files:**

- Create: `docs/adr/README.md`
- Move: `docs/adr/0009-bridge-adapter-memory-ownership.md` ->
  `docs/adr/0012-bridge-adapter-memory-ownership.md`
- Modify: `docs/adr/template.md`
- Modify: every numbered `docs/adr/*.md`
- Modify: `adapters/hermes/README.md`
- Modify: `adapters/hermes/adapter.py`
- Modify: `adapters/hermes/tests/test_adapter_handle.py`
- Modify: `docs/CHANGELOG.md`
- Modify: `docs/roadmap.md`

- [ ] **Step 1: Add the failing ADR contract test**

Append to `tests/docs/test_documentation_contract.py`:

```python
def test_adr_ids_statuses_and_index_are_consistent():
    assert_clean(check_adr_set(ROOT))
```

- [ ] **Step 2: Run the ADR test and verify the duplicate**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_adr_ids_statuses_and_index_are_consistent \
  -q
```

Expected: `FAIL` with `duplicate ADR id 0009`, missing implementation sections,
and missing `docs/adr/README.md` rows.

- [ ] **Step 3: Renumber the later bridge-memory ADR without shifting 0010/0011**

Run:

```bash
git mv docs/adr/0009-bridge-adapter-memory-ownership.md \
  docs/adr/0012-bridge-adapter-memory-ownership.md
```

Change its H1 to:

````markdown
# ADR-0012: Bridge adapters retain upstream memory ownership
````

Replace bridge-memory references from ADR-0009 to ADR-0012 in:

- `adapters/hermes/README.md`
- `adapters/hermes/adapter.py`
- `adapters/hermes/tests/test_adapter_handle.py`
- `docs/CHANGELOG.md`
- `docs/roadmap.md`
- `docs/adr/0010-nats-native-l2-delegation.md`

Do not change host-deployment references to
`docs/adr/0009-host-deploy-architecture.md`.

- [ ] **Step 4: Standardize decision and implementation metadata**

Every numbered ADR must have both sections immediately after its title/date
metadata:

````markdown
## Status

Accepted

## Implementation

Current
````

`scripts/docs/check_docs.py::ADR_EXPECTED` is the only table of exact ADR
titles, statuses, and implementation strings. For every mapping entry, use
`apply_patch` to make the file H1, `## Status`, and `## Implementation` equal the
three mapped values. Do not maintain a second literal metadata table in this plan
or in the index generator.

In ADR-0006, replace `EC_INBOX` with `AGENT_INBOX` and
`agents.{self}.inbox.>` with `agents.{self}.inbox`. State explicitly that the
outbox mirror can be lost while the aggregator is unavailable.

Update `docs/adr/template.md` so a new record contains:

````markdown
## Status

Proposed

## Implementation

Not implemented
````

- [ ] **Step 5: Create the complete ADR index**

Render the one canonical index to stdout:

```bash
scripts/research/run-python - <<'PY'
from scripts.docs.check_docs import render_adr_index

print(render_adr_index(), end="")
PY
```

Use `apply_patch` to create `docs/adr/README.md` with exactly that output.
`check_adr_set` compares the entire file to `render_adr_index()`, so a copied or
shortened status cannot diverge silently.

- [ ] **Step 6: Re-run ADR and Hermes regression tests**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_adr_ids_statuses_and_index_are_consistent \
  adapters/hermes/tests/test_adapter_handle.py -q
```

Expected: the ADR contract passes and the existing Hermes tests pass.

- [ ] **Step 7: Commit the ADR correction**

```bash
git add -- docs/adr/README.md
git add -- docs/adr/template.md docs/adr/[0-9][0-9][0-9][0-9]-*.md \
  docs/CHANGELOG.md docs/roadmap.md adapters/hermes/README.md \
  adapters/hermes/adapter.py adapters/hermes/tests/test_adapter_handle.py \
  tests/docs/test_documentation_contract.py
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(infra): index decisions and resolve duplicate id"
```

### Task 5: Replace the product and architecture entrypoints

**Files:**

- Create: `docs/README.md`
- Modify: `README.md`
- Modify: `docs/01-architecture.md`
- Modify: `tests/docs/test_documentation_contract.py`

- [ ] **Step 1: Add the failing document-index test**

Append to `tests/docs/test_documentation_contract.py`:

```python
def test_document_index_has_four_status_sections():
    index = ROOT / "docs" / "README.md"
    assert index.exists()
    text = index.read_text()
    for label in DOCUMENT_CLASSIFICATION:
        assert f"## {label}" in text
```

- [ ] **Step 2: Run the index test and verify it fails**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_document_index_has_four_status_sections \
  -q
```

Expected: `FAIL` because `docs/README.md` does not exist.

- [ ] **Step 3: Rewrite the root README as the current product entrypoint**

Replace `README.md` with these sections and no feature claim outside them:

1. `# EdgeCitadel`
2. `## Current Scope`
3. `## Quick Start`
4. `## Verify`
5. `## Task-Aware Reliability Artifact`
6. `## Development Gates`
7. `## Limitations`
8. `## Documentation`
9. `## License`

The current-scope bullets must state:

- commands/results use the durable `AGENT_INBOX` JetStream WorkQueue;
- progress, liveness, registration, status, logs, and outbox audit use Core NATS;
- Compose services are exactly `nats`, `aggregator`, `dashboard`, and `nginx`;
  FastAPI runs inside `aggregator`, React/Vite builds `dashboard`, and SQLite is
  embedded aggregator storage rather than a service;
- the outbox/database/dashboard path is best-effort operator audit, not task
  completion evidence.

Use these command blocks, each preceded by `<!-- doc-command -->`:

````markdown
<!-- doc-command -->
```bash
git clone https://github.com/zhonghaozhan/EdgeCitadel.git
```

<!-- doc-command -->
```bash
cd EdgeCitadel
```

<!-- doc-command -->
```bash
cp .env.example .env
```

<!-- doc-command -->
```bash
docker compose up --build -d
```

<!-- doc-command -->
```bash
curl http://localhost:8222/healthz
```

<!-- doc-command -->
```bash
curl http://localhost/api/system/status
```

<!-- doc-command -->
```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  aggregator/tests schemas/tests adapters -q
```

<!-- doc-command -->
```bash
npm --prefix frontend run build
```

<!-- doc-command -->
```bash
npm --prefix e2e test
```
````

The Quick Start instructs the operator to generate `NATS_TOKEN` and
`OPENCLAW_TOKEN` locally in `.env`; it must not show a literal token. Remove the
old demo embed, MQTT-default claim, autonomous delegation claim, persistent task
board claim, replay claim, `join.sh` onboarding, obsolete subject table, and
nonexistent lint/mypy commands.

The limitations section must state:

- MQTT is optional ingress and has no maintained constrained-device client;
- OpenClaw login does not yet provision scoped broker credentials;
- `join.sh` and `add-agent.sh` are not paper-artifact onboarding;
- NATS-native L2 orchestration, MCP exposure, A2A transport conformance,
  production per-agent authorization, and Internet-facing deployment are not
  current paper contributions;
- macOS and ARM64 are not part of the initial verified x86_64 lab baseline.

- [ ] **Step 4: Create the four-way documentation index**

Create `docs/README.md` with:

- the source-of-truth table from design Section 7.1;
- `## Current` links to architecture, contract, messaging, API, dashboard,
  testing, development, lab controller, lab node, artifact, results inventory,
  and ADR index;
- `## Experimental` links to the task-aware design and any validated quick,
  matrix-smoke, operator, or lab outputs, using only evidence that exists;
- `## Proposed` links only to `docs/roadmap.md`, the unsupported macOS setup,
  ADR-0010, and ADR-0011;
- `## Historical` links to `docs/archive/README.md`, functional probes, the
  superseded research plan/matrix, the Slice 1-4 implementation plans, and the
  compatibility-only registration page;
- a `### Historical developer records (excluded from the maintained contract)`
  subsection lists these exact paths individually:
  `superpowers/plans/2026-04-29-phase-3-watchdog-and-registry.md`,
  `superpowers/plans/2026-04-30-gemma-enhancements.md`,
  `superpowers/plans/2026-05-03-claude-md-system.md`,
  `superpowers/plans/2026-05-06-hermes-bridge.md`,
  `superpowers/plans/2026-05-16-phase4-umbrella.md`,
  `superpowers/plans/2026-05-19-hermes-financial-agent.md`,
  `superpowers/plans/2026-05-31-enterprise-trading-agent-mvp.md`,
  `superpowers/plans/2026-07-12-agentic-edge-benchmark-suite-e1-e12.md`,
  `superpowers/specs/2026-04-29-phase-3-watchdog-and-registry-design.md`,
  `superpowers/specs/2026-04-30-gemma-enhancements-design.md`,
  `superpowers/specs/2026-05-03-claude-md-system-design.md`,
  `superpowers/specs/2026-05-05-hermes-bridge-design.md`,
  `superpowers/specs/2026-05-10-phase4-fleet-orchestration-umbrella-design.md`,
  and
  `superpowers/specs/2026-05-18-phase4.1-bridge-adapter-template-design.md`.
  These 14 byte-preserved developer plans/specifications are not
  `DOCUMENT_CLASSIFICATION`, maintained-link, current-claim, schema, or command
  inputs. They are literal-path labels rather than links because the files may be
  ignored local developer records rather than clean-checkout content. The exact
  on-disk inventory test and recursive checker prevent the exclusion from growing
  implicitly;
- `## Evidence Labels` defining:
  `Functional probe`, `Artifact verified`, `Preliminary measured`,
  `Remote lab qualified`, and `Paper evidence ready`.

List every path in `DOCUMENT_CLASSIFICATION` exactly once under its declared
section; do not substitute an indirect directory link for an individual active
Markdown file. Do not put a document in more than one status section. The design
specification is `Experimental` until all cumulative readiness gates pass; it is
not a statement that every proposed mechanism is current.

- [ ] **Step 5: Rewrite current architecture from executable topology**

Replace `docs/01-architecture.md` with these exact sections:

1. `# Current Architecture`
2. `## Authority and Scope`
3. `## Compose Services`
4. `## Task-Aware Data Path`
5. `## Durable and Ephemeral Planes`
6. `## Persistence Boundaries`
7. `## Authentication Boundary`
8. `## Paper Artifact Topologies`
9. `## Unsupported and Proposed Surfaces`

The Compose table must be derived from `docker-compose.yml` and name `nats`,
`aggregator`, `dashboard`, and `nginx`. The data path must distinguish:

1. dashboard HTTP command acceptance;
2. JetStream PubAck to an agent inbox;
3. pull-consumer handling and terminal publication;
4. sender inbox observation;
5. plain-NATS outbox audit and WebSocket/UI propagation.

State that only task-bearing inbox traffic is captured by `AGENT_INBOX`; there is
no current conversation stream or agent-state KV bucket. Describe port 1883 as a
bound port whose MQTT protocol listener is controlled by rendered NATS
configuration, not as proof that MQTT is healthy.

The artifact-topology section links to the design and says the central relay,
Core-only, all-durable control, outcome ledger, and lab launchers are research
mechanisms verified by their own gates. It must not imply that those modes are
production Compose services.

- [ ] **Step 6: Re-run the document-index test**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_document_index_has_four_status_sections \
  -q
```

Expected:

```text
1 passed
```

- [ ] **Step 7: Commit the entrypoint rewrite**

```bash
git add -- docs/README.md
git add -- README.md docs/01-architecture.md \
  tests/docs/test_documentation_contract.py
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(infra): establish current architecture entrypoints"
```

### Task 6: Rewrite the current contract and messaging guides

**Files:**

- Modify: `docs/agent-contract.md`
- Modify: `docs/05-messaging.md`
- Modify: `tests/docs/test_documentation_contract.py`

- [ ] **Step 1: Add failing schema/current-claim tests**

Append to `tests/docs/test_documentation_contract.py`:

```python
def test_contract_examples_validate_against_canonical_schemas():
    paths = [
        ROOT / "docs" / "agent-contract.md",
        ROOT / "docs" / "05-messaging.md",
    ]
    assert_clean(check_schema_examples(ROOT, paths))
    contract = paths[0].read_text()
    assert contract.count("<!-- schema: schemas/envelope.v1.json -->") >= 2
    assert contract.count("<!-- schema: schemas/agent-card.v1.json -->") >= 1


def test_contract_and_messaging_contain_no_pre_v0_1_claims():
    paths = [
        ROOT / "docs" / "agent-contract.md",
        ROOT / "docs" / "05-messaging.md",
    ]
    assert_clean(check_current_claims(ROOT, paths))
```

- [ ] **Step 2: Run the focused tests and verify current prose fails**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_contract_examples_validate_against_canonical_schemas \
  tests/docs/test_documentation_contract.py::test_contract_and_messaging_contain_no_pre_v0_1_claims \
  -q
```

Expected: `FAIL`; the contract lacks tagged examples and maintained prose still
contains pre-v0.1 claims.

- [ ] **Step 3: Rewrite `docs/agent-contract.md` around canonical grammar**

Use these sections:

1. `# EdgeCitadel Agent Contract v0.1`
2. `## Status and Authorities`
3. `## Envelope Grammar`
4. `## Type Requirements`
5. `## Task Observation and Terminality`
6. `## Correlation and Delegation`
7. `## Agent Card`
8. `## Current Conformance`
9. `## Research Extension`
10. `## Non-Goals`

The status section says JSON schemas own grammar, current adapter code owns
implemented behavior, and the task-aware design owns proposed/experimental
semantics. Do not copy future L2/MCP/gateway prose into current conformance.

Include this schema-tagged command:

````markdown
<!-- schema: schemas/envelope.v1.json -->
```json
{
  "v": 1,
  "id": "11111111-1111-4111-8111-111111111111",
  "type": "command",
  "sender_id": "aggregator",
  "recipient_id": "shell-1",
  "timestamp": "2026-07-25T12:00:00.000Z",
  "task_id": "22222222-2222-4222-8222-222222222222",
  "context_id": "33333333-3333-4333-8333-333333333333",
  "payload": {
    "body": "printf artifact-ok"
  }
}
```
````

Include this schema-tagged result:

````markdown
<!-- schema: schemas/envelope.v1.json -->
```json
{
  "v": 1,
  "id": "44444444-4444-4444-8444-444444444444",
  "type": "result",
  "sender_id": "shell-1",
  "recipient_id": "aggregator",
  "timestamp": "2026-07-25T12:00:01.000Z",
  "task_id": "22222222-2222-4222-8222-222222222222",
  "context_id": "33333333-3333-4333-8333-333333333333",
  "task_state": "completed",
  "payload": {
    "body": "artifact-ok"
  }
}
```
````

Include this schema-tagged Agent Card:

````markdown
<!-- schema: schemas/agent-card.v1.json -->
```json
{
  "name": "shell-1",
  "description": "Deterministic shell fixture",
  "version": "1.0.0",
  "url": "nats://edgecitadel/agents.shell-1.inbox",
  "provider": {
    "organization": "EdgeCitadel"
  },
  "capabilities": {
    "streaming": true
  },
  "securitySchemes": {},
  "metadata": {
    "runtime.kind": "native",
    "runtime.roles": ["worker"],
    "runtime.conformance": "L1",
    "runtime.heartbeat_interval_sec": 30,
    "runtime.deployment": "test"
  }
}
```
````

Describe `payload.parent_task_id` only through
`schemas/task-correlation.v1.json` and only if that schema and its validator tests
pass. State that run/trial IDs remain artifact metadata rather than production
envelope fields.

- [ ] **Step 4: Rewrite `docs/05-messaging.md` from code/config**

Use these sections:

1. `# Messaging`
2. `## Status and Authorities`
3. `## Subject Inventory`
4. `## AGENT_INBOX Stream`
5. `## Per-Agent Consumer`
6. `## Publish and Acknowledge Semantics`
7. `## Ephemeral Audit and Progress`
8. `## Poison and Watchdog Behavior`
9. `## Memory Request/Reply`
10. `## Optional MQTT Ingress`
11. `## Experiment-Only Modes`
12. `## Verification`

The current subject table must include:

- `agents.<agent_id>.inbox` as JetStream task-bearing command, delegation,
  cancellation, and terminal-result traffic;
- `agents.<agent_id>.register`, `.heartbeat`, `.status`, `.log`, and `.outbox`
  as Core NATS;
- `agents.<agent_id>.task_progress.<task_id>` as Core NATS;
- `system.broadcast` as Core NATS;
- `memory.turns.get`, `.put`, and `.delete` as Core NATS request/reply;
- `openclaw.<session>.*` as a current translation/stub surface, not secure
  browser-to-broker authentication.

Copy `AGENT_INBOX` values from `aggregator/jetstream_bootstrap.py`: WorkQueue,
file-backed default storage, 24-hour max age, 1 GiB max bytes, 1 MiB max message,
discard-new, five-minute duplicate window, exact consumer filter, explicit ack,
and production defaults for ack wait, max pending, and max deliveries.

State that:

- a PubAck means durable storage, not task completion;
- a worker ack follows terminal-result publication;
- the plain-NATS outbox is best-effort and cannot replay aggregator downtime;
- `task.progress` is not durable in the production split plane;
- current watchdog synthesis is product behavior outside the paper mechanism;
- disabled MQTT can still have a bound host port that refuses protocol traffic;
- the NATS container does not ship the `nats` CLI, so verification commands use
  tested Python or HTTP entrypoints rather than `docker compose exec nats nats`.

Move all future A2A gateway, MCP server, AG2/L2 helper, and all-durable claims to a
short `Experiment-Only Modes` section linking the design and ADR index.

- [ ] **Step 5: Run schema and current-claim checks**

Run:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_contract_examples_validate_against_canonical_schemas \
  tests/docs/test_documentation_contract.py::test_contract_and_messaging_contain_no_pre_v0_1_claims \
  -q
```

Expected:

```text
2 passed
```

- [ ] **Step 6: Commit the contract rewrite**

```bash
git add -- docs/agent-contract.md docs/05-messaging.md \
  tests/docs/test_documentation_contract.py
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(nats): align contract and messaging with runtime"
```

### Task 7: Align dashboard and test-gate documentation

**Files:**

- Modify: `docs/04-dashboard.md`
- Modify: `docs/10-testing.md`
- Modify: `tests/docs/test_documentation_contract.py`

- [ ] **Step 1: Add failing dashboard/API contract tests**

Append:

```python
def test_generated_api_reference_matches_runtime():
    from scripts.docs.render_api_reference import (
        REFERENCE,
        render_reference,
        replace_generated_block,
    )

    generated, counts = render_reference()
    assert counts["default_http"] > 0
    assert counts["lab_only_http"] > 0
    assert counts["websocket"] > 0
    assert counts["events"] > 0
    assert REFERENCE.read_text() == replace_generated_block(
        REFERENCE.read_text(), generated,
    )


def test_dashboard_and_testing_docs_name_current_surfaces():
    dashboard = (ROOT / "docs" / "04-dashboard.md").read_text()
    testing = (ROOT / "docs" / "10-testing.md").read_text()
    for value in ("Chat", "Flow", "Logs", "Tasks", "Registry"):
        assert value in dashboard
    for value in (
        "online agents", "NATS", "JetStream", "agent_status_change",
        "/ws/stream", "/ws/agent/{agent_id}",
    ):
        assert value in dashboard
    for value in (
        "aggregator/tests", "schemas/tests", "adapters",
        "npm --prefix frontend run build", "npm --prefix e2e test",
        "--profile quick", "--profile matrix-smoke",
    ):
        assert value in testing
```

- [ ] **Step 2: Run the tests and verify stale dashboard/testing prose**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_generated_api_reference_matches_runtime \
  tests/docs/test_documentation_contract.py::test_dashboard_and_testing_docs_name_current_surfaces \
  -q
```

Expected: the generated API test passes and the dashboard/testing test fails.

- [ ] **Step 3: Rewrite `docs/04-dashboard.md`**

Use sections `Tabs`, `Header`, `Agent Selection`, `Task State`, `Realtime Events`,
`Test Deployment Filter`, and `Limitations`. Document five tabs with shortcuts
1-5; the eight-state message-derived task view; header metrics `online agents`,
`NATS`, and `JetStream`; global `/ws/stream`; selected-agent
`/ws/agent/{agent_id}`; and event names from the generated API block. State that
the Tasks view derives state from envelopes and has no task CRUD API. Do not claim
that an API interceptor adds test filtering; document the actual
`deployment`/`exclude_deployment` query parameters and client toggle.

- [ ] **Step 4: Rewrite `docs/10-testing.md`**

Use sections `Default Gates`, `Research Profiles`, `Optional Live Suites`,
`Isolation`, `Evidence`, and `Troubleshooting`. Enumerate actual files under
`e2e/tests/` rather than hard-coding a test count. Classify Gemma, Hermes, Ollama,
and external-service cases as optional live tests; default gates use deterministic
fixtures and isolated base URLs.

Classify `aggregator/tests/test_jetstream_bootstrap.py` as an explicit opt-in live
suite. It runs only when `NATS_URL_TEST` and `NATS_TOKEN_TEST` point to a
test-owned broker. The clean R-10 static gate excludes that file so it cannot
probe ambient `localhost`; the isolated Slice 3 lab gate owns JetStream runtime
verification.

Include these checked blocks:

````markdown
<!-- doc-command -->
```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  aggregator/tests schemas/tests adapters -q
```

<!-- doc-command -->
```bash
npm --prefix frontend run build
```

<!-- doc-command -->
```bash
npm --prefix e2e test
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/run_artifact.py run --profile quick
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/run_artifact.py run --profile matrix-smoke
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/docs/check_docs.py
```
````

State that a default E2E run is valid only when specs use the configured isolated
base URL and NATS endpoint; hard-coded development ports invalidate the gate.

- [ ] **Step 5: Run focused tests and the API check**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_generated_api_reference_matches_runtime \
  tests/docs/test_documentation_contract.py::test_dashboard_and_testing_docs_name_current_surfaces \
  -q
scripts/research/run-python scripts/docs/render_api_reference.py --check
```

Expected: `2 passed`; the generated API report contains derived current counts
and the complete conditional lab route set.

- [ ] **Step 6: Commit**

```bash
git add -- docs/04-dashboard.md docs/10-testing.md \
  tests/docs/test_documentation_contract.py
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(dashboard): document current UI and test gates"
```

### Task 8: Write executable development and lab runbooks

**Files:**

- Create: `docs/setup-development.md`
- Create: `docs/setup-lab-controller.md`
- Modify: `docs/setup-lab-node.md` (preserve the Slice 3 qualification contract)
- Modify: `docs/02-server-setup.md`
- Modify: `docs/02-server-setup-linux.md`
- Modify: `docs/02-server-setup-macos.md`
- Modify: `docs/03-agent-registration.md`
- Modify: `docs/09-monitoring.md`
- Modify: `docs/agent-setup.md`
- Modify: `docs/setup_hermes.md`
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Modify: `CONTRIBUTING.md`
- Modify: `deploy/README.md`
- Modify: `tests/docs/test_documentation_contract.py`

- [ ] **Step 1: Add the failing runbook contract test**

Append:

```python
def test_maintained_runbook_commands_resolve_to_real_entrypoints():
    runbooks = [
        ROOT / "README.md",
        ROOT / "docs" / "setup-development.md",
        ROOT / "docs" / "setup-lab-controller.md",
        ROOT / "docs" / "setup-lab-node.md",
        ROOT / "docs" / "10-testing.md",
    ]
    assert all(path.exists() for path in runbooks)
    assert_clean(check_commands(ROOT, runbooks))
    assert_clean(check_no_bare_host_python(runbooks))
    current_paths = [
        ROOT / name for name in CURRENT_CLAIM_MARKDOWN
        if (ROOT / name).exists()
    ]
    assert_clean(check_current_claims(ROOT, current_paths))
    development = (ROOT / "docs" / "setup-development.md").read_text()
    controller = (ROOT / "docs" / "setup-lab-controller.md").read_text()
    node = (ROOT / "docs" / "setup-lab-node.md").read_text()
    for value in (
        "python3 -m venv .venv",
        "python3 -m pip install -r aggregator/requirements.txt "
        "-r tests/requirements.txt",
        "npm --prefix frontend ci",
        "npm --prefix e2e ci",
        "playwright install chromium",
        "NATS_URL=nats://127.0.0.1:4222",
        "NATS_TOKEN=\"$(sed -n 's/^NATS_TOKEN=//p' .env)\"",
        "DB_PATH=tmp/development/openclaw.db",
    ):
        assert value in development
    assert (
        "lab_controller.py start --run-id ec-lab-01 "
        "--host-id controller-lab-01"
    ) in controller
    assert (
        "--host-id controller-lab-01 --agent-id fixture-1 "
        "--behavior echo --delay-ms 125"
    ) in node
```

- [ ] **Step 2: Verify the missing runbooks fail**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_maintained_runbook_commands_resolve_to_real_entrypoints \
  -q
```

Expected: `FAIL` because the two new setup files do not yet exist. The Slice 3
node runbook already exists and must not be replaced with a weaker contract.

- [ ] **Step 3: Create `docs/setup-development.md`**

Document prerequisites Docker Engine/Compose v2, Python 3.12, Git, and Node/npm;
`.env` creation without literal credentials; Compose start/stop; backend tests;
frontend build; E2E; and docs checks. Use only command blocks already listed in
README/`docs/10-testing.md`, plus:

````markdown
<!-- doc-command -->
```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r aggregator/requirements.txt -r tests/requirements.txt
```

<!-- doc-command -->
```bash
npm --prefix frontend ci
npm --prefix e2e ci
npm --prefix e2e exec -- playwright install chromium
```
````

State that every later Python command runs in the same activated shell. The
checked dependency block must precede backend tests or `uvicorn`; the npm block
must precede frontend/E2E gates in a new checkout.

````markdown
<!-- doc-command -->
```bash
docker compose config -q
```

<!-- doc-command -->
```bash
docker compose down
```
````

Use these checked commands from the repository root for backend development:
Compose NATS must already be running, and `.env` must contain a locally generated,
nonempty `NATS_TOKEN`; do not put the token itself in documentation.

````markdown
<!-- doc-command -->
```bash
mkdir -p tmp/development
```

<!-- doc-command -->
```bash
test -n "$(sed -n 's/^NATS_TOKEN=//p' .env)"
```

<!-- doc-command -->
```bash
NATS_URL=nats://127.0.0.1:4222 NATS_TOKEN="$(sed -n 's/^NATS_TOKEN=//p' .env)" DB_PATH=tmp/development/openclaw.db python3 -m uvicorn aggregator.main:app --host 0.0.0.0 --port 8000 --reload
```
````

Do not retain
`cd aggregator && uvicorn main:app`.

- [ ] **Step 4: Create the controller and node runbooks**

`docs/setup-lab-controller.md` must name the supported baseline Ubuntu 24.04 LTS,
x86_64, Docker Compose v2, Python 3.12, Git, trusted LAN/Tailnet, and native NATS
fixtures. Include:

````markdown
<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/lab_controller.py start --run-id ec-lab-01 --host-id controller-lab-01 --lab-variant lifecycle --bind-host 127.0.0.1 --advertise-host 127.0.0.1
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/lab_controller.py status --run-id ec-lab-01
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/lab_controller.py stop --run-id ec-lab-01
```
````

Extend the Slice 3 `docs/setup-lab-node.md` without removing its exact same-host,
remote, `status`, `doctor --publish`, `stop`, or controller `qualify` commands,
or the literal `PRELIMINARY` and `REMOTE QUALIFIED` result labels. It must document
controller-config and credential-file
transfer without placing a token in shell history, deterministic agent IDs,
disconnect/reconnect, doctor diagnostics, duplicate-ID inventory rejection, and
the shared-credential limitation. Include:

````markdown
<!-- doc-command-ignore: derives and verifies the run-owned credential path from controller output -->
```bash
CREDENTIAL_FILE="$(scripts/research/run-python -c 'import json,sys; print(json.load(open(sys.argv[1]))["credential_file"])' tmp/research/ec-lab-01/controller.json)"
test -f "$CREDENTIAL_FILE"
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/lab_node.py start --controller-config tmp/research/ec-lab-01/controller.json --credential-file "$CREDENTIAL_FILE" --host-id controller-lab-01 --agent-id fixture-1 --behavior echo --delay-ms 125
```
````

At the start of both lab runbooks, use this checked repository-root block:

````markdown
<!-- doc-command -->
```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
```
````

Replace every `/Users/<name>/...` and `/home/<name>/...` example inherited from
Slice 3 with `$REPO_ROOT`, `$PWD`, or a run-owned `$REMOTE_TMP` path. Mark every
single-command Bash fence with `<!-- doc-command -->`. The compound `set`/`trap`/
`scp` credential-transfer block may instead use exactly
`<!-- doc-command-ignore: exercised by tests/research/test_lab_qualification.py -->`;
all Python controller/node/checker commands around it remain individually tagged
and checked. Preserve the workflow and result labels, not machine-specific
absolute paths.

Both runbooks state `remote-capable` until the declared second-host qualification
passes. Neither claims ARM64, production identity, TLS fleet authorization,
Internet exposure, MQTT-device support, or upgrade management.

- [ ] **Step 5: Reclassify existing setup/monitoring pages**

- `docs/02-server-setup.md`: setup router linking development, paper lab, and
  current Linux host deployment.
- `docs/02-server-setup-linux.md`: current supplemental host-deploy guide outside
  the paper artifact; derive dependencies from `deploy/manifest.toml`, use the
  actual 120-second check timeout, actual health JSON, and manifest model.
- `docs/02-server-setup-macos.md`: unsupported/forward-looking; do not present
  systemd deployment as executable on macOS.
- `docs/03-agent-registration.md`: historical compatibility page pointing to
  `setup-lab-node.md`; state that `join.sh`/`add-agent.sh` are not current paper
  onboarding.
- `docs/09-monitoring.md`: current supplemental observability page using actual
  `/api/system/status`, registry/queue/poison APIs, NATS `/healthz`, effective
  liveness, and watchdog boundaries; remove nonexistent endpoints.
- `docs/agent-setup.md`: compatibility pointer to `setup-development.md`.
- `docs/setup_hermes.md`: optional-live bridge runbook, not a deterministic
  default gate.

Update `AGENTS.md`, `CLAUDE.md`, and `CONTRIBUTING.md` to link
`docs/setup-development.md`; use lowercase `.codex` paths in `AGENTS.md`. Update
`deploy/README.md` to distinguish the Linux host path from the paper lab path.

- [ ] **Step 6: Run command and static configuration checks**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_maintained_runbook_commands_resolve_to_real_entrypoints \
  -q
docker compose config -q
scripts/research/run-python scripts/research/lab_controller.py --help
scripts/research/run-python scripts/research/lab_node.py --help
```

Expected: `1 passed`, Compose exits `0`, and both Python commands print usage and
exit `0`.

- [ ] **Step 7: Commit**

```bash
git add -- AGENTS.md docs/setup-development.md docs/setup-lab-controller.md
git add -- docs/setup-lab-node.md docs/02-server-setup.md \
  docs/02-server-setup-linux.md docs/02-server-setup-macos.md \
  docs/03-agent-registration.md docs/09-monitoring.md docs/agent-setup.md \
  docs/setup_hermes.md CLAUDE.md CONTRIBUTING.md \
  deploy/README.md tests/docs/test_documentation_contract.py
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(infra): add supported development and lab runbooks"
```

### Task 9: Write the artifact guide and label every existing result

**Files:**

- Create: `docs/research/artifact.md`
- Modify: `docs/research/results/README.md`
- Modify: `docs/research/nats-agent-communication-research-plan.md`
- Modify: `docs/research/nats-agent-communication-experiment-matrix.md`
- Modify: `docs/research/nats-agent-communication-literature-review.md`
- Modify: `docs/research/nats-agent-communication-baseline-results-2026-07-04.md`
- Modify: `docs/research/results/20260712T052153Z-security_temporal-security_temporal.md`
- Preserve/add without modification: `docs/research/results/.gitkeep`
- Preserve/add without modification: `docs/research/results/20260712T052153Z-security_temporal-security_temporal.json`
- Modify: `docs/roadmap.md`
- Modify: `tests/docs/test_documentation_contract.py`

- [ ] **Step 1: Add the failing result-label test**

Append:

```python
def test_existing_research_results_are_labeled_and_inventoried():
    assert_clean(check_result_labels(ROOT))
    legacy = (
        ROOT / "docs/research/results"
        / "20260712T052153Z-security_temporal-security_temporal.md"
    )
    assert legacy.read_text().startswith(
        "# SECURITY_TEMPORAL Functional Probe\n\n"
        "> **Evidence status:** Functional probe.\n"
    )


def test_artifact_runbook_commands_resolve_to_real_entrypoints():
    path = ROOT / "docs" / "research" / "artifact.md"
    assert path.exists()
    assert_clean(check_commands(ROOT, [path]))
```

- [ ] **Step 2: Verify unlabeled probes fail**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_existing_research_results_are_labeled_and_inventoried \
  tests/docs/test_documentation_contract.py::test_artifact_runbook_commands_resolve_to_real_entrypoints \
  -q
```

Expected: `FAIL` for the missing artifact runbook, baseline, temporal Markdown
result, `.gitkeep` classification, and unindexed result files.

- [ ] **Step 3: Create `docs/research/artifact.md`**

Use sections `Prerequisites`, `Profiles`, `Quick Verification`, `Full Matrix`,
`Campaign Analysis`, `Output Layout`, `Evidence Labels`, `Validity and
Exclusions`, `Cleanup`, and `Limitations`. Include:

````markdown
<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/run_artifact.py run --profile quick
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/run_artifact.py run --profile matrix-smoke
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/run_artifact.py run --profile paper --campaign-config scripts/research/configs/campaigns/preliminary-x86-lan.yaml
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/analyze_artifact.py --campaign preliminary-x86-lan --confidence 0.95 --bootstrap-samples 10000
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/check_artifact.py --campaign preliminary-x86-lan
```

<!-- doc-command -->
```bash
scripts/research/run-python scripts/research/run_artifact.py cleanup --run-id ec-20260725-example
```
````

State that quick is installation evidence only; matrix-smoke is coverage evidence
only; publication figures require a complete validated campaign; invalid or
missing cells cannot be omitted; the production aggregator is excluded from
publication benchmark clocks; screenshots are operator evidence only.

- [ ] **Step 4: Extend the result inventory without mutating raw JSON**

Preserve the Slice 1 campaign/profile and Slice 2 operator-bundle sections. Add
this legacy-probe inventory and the existing raw/derived explanation:

````markdown
# Research Result Inventory

No checked-in July 2026 file is publication-grade evidence.

| File | Evidence status | Permitted use |
| --- | --- | --- |
| `raw/` | Validated raw bundles | Use only after checker PASS |
| `derived/` | Deterministic derivative | Not independent evidence |
| `operator/` | Operator evidence | Operator Evidence Ready only for a checked PASS bundle |
| `lab/` | Lab evidence | Preliminary unless the exact remote qualification gate passes |
| `.gitkeep` | Directory placeholder | Not evidence |
| `20260712T052153Z-security_temporal-security_temporal.json` | Functional probe | Synthetic evaluator debugging only |
| `20260712T052153Z-security_temporal-security_temporal.md` | Functional probe | Human-readable synthetic evaluator debugging only |

Publication campaigns live under `raw/<campaign>/`; deterministic derivatives live
under `derived/<campaign>/`. A campaign is usable only when
`scripts/research/check_artifact.py` exits zero.
````

In
`docs/research/nats-agent-communication-baseline-results-2026-07-04.md`,
add this immediately below its existing H1:

````markdown
> **Evidence status:** Functional probe.
>
> This output is retained for provenance and cannot support publication claims.
````

The temporal Markdown result has no H1. Prepend this exact heading, blank line,
and label block before its existing table; preserve every existing table byte
after the inserted block:

````markdown
# SECURITY_TEMPORAL Functional Probe

> **Evidence status:** Functional probe.
>
> This output is retained for provenance and cannot support publication claims.

````

Do not edit
`docs/research/results/20260712T052153Z-security_temporal-security_temporal.json`.

- [ ] **Step 5: Label research plans**

- Old research plan: `Historical planning document; superseded by
  task-aware-reliability-contract-design.md`.
- Old experiment matrix: `Historical experiment proposal; not the W1-W8
  publication matrix`.
- Literature review: `Research background; not experimental evidence`.
- `docs/roadmap.md`: `Proposed product roadmap; not current behavior or measured
  evidence`.

- [ ] **Step 6: Run result, command, and artifact help checks**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_existing_research_results_are_labeled_and_inventoried \
  tests/docs/test_documentation_contract.py::test_artifact_runbook_commands_resolve_to_real_entrypoints \
  tests/docs/test_documentation_contract.py::test_maintained_runbook_commands_resolve_to_real_entrypoints \
  -q
scripts/research/run-python scripts/research/run_artifact.py --help
scripts/research/run-python scripts/research/analyze_artifact.py --help
scripts/research/run-python scripts/research/check_artifact.py --help
```

Expected: `3 passed`; each CLI prints usage and exits `0`.

- [ ] **Step 7: Commit**

```bash
git add -- docs/research/artifact.md \
  docs/research/nats-agent-communication-research-plan.md \
  docs/research/nats-agent-communication-experiment-matrix.md \
  docs/research/nats-agent-communication-literature-review.md \
  docs/research/nats-agent-communication-baseline-results-2026-07-04.md \
  docs/research/results/.gitkeep \
  docs/research/results/20260712T052153Z-security_temporal-security_temporal.json \
  docs/research/results/20260712T052153Z-security_temporal-security_temporal.md
git add -- docs/research/results/README.md \
  docs/roadmap.md tests/docs/test_documentation_contract.py
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(nats): label evidence and add artifact runbook"
```

### Task 10: Close links, references, and the full R-10 gate

**Files:**

- Modify: all active Markdown files reported by the checker
- Modify: `docs/CHANGELOG.md`
- Modify: `docs/research/task-aware-reliability-contract-design.md`
- Create: `docs/research/r10-implementation-log.md`
- Modify: `tests/docs/test_documentation_contract.py`

- [ ] **Step 1: Add the final contract tests**

Append:

```python
def test_maintained_internal_links_resolve():
    paths = [
        ROOT / name for name in MAINTAINED_MARKDOWN
        if (ROOT / name).exists()
    ]
    assert_clean(check_internal_links(ROOT, paths))


def test_document_index_classifies_every_maintained_doc():
    assert_clean(check_document_classification(ROOT))


def test_complete_documentation_contract():
    from scripts.docs.check_docs import run_all

    assert_clean(run_all(ROOT))
```

- [ ] **Step 2: Run the final contract and capture every failure**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider \
  tests/docs/test_documentation_contract.py::test_maintained_internal_links_resolve \
  tests/docs/test_documentation_contract.py::test_complete_documentation_contract \
  -q
```

Expected: `FAIL` until all stale links, command blocks, labels, and current claims
are corrected.

- [ ] **Step 3: Repair only reported active references**

Update links to moved archive files in `README.md`, `docs/roadmap.md`,
`PROGRESS.md`, and any checker-reported maintained page. Update all bridge-memory
links to ADR-0012. Do not suppress a checker finding with a broad exclusion; either
correct the active claim/link or classify and move it according to the approved
archive contract.

Add a dated `R-10 Documentation and artifact organization` entry to
`docs/CHANGELOG.md` listing the maintained index, archive, generated API
reference, lab/artifact runbooks, ADR correction, result labels, and checks.

Create `docs/research/r10-implementation-log.md` with one row per static,
quick, matrix-smoke, same-host lab, and operator gate. Each row records the exact
command, UTC start/end, exit code, emitted bundle or campaign path, metadata file
and SHA-256, checker status, and permitted evidence label. Initially mark all five
local rows `Pending clean-candidate gate`; do not invent timestamps, paths, hashes,
or passing statuses. Add explicit `Not run` rows for remote-host, ARM64,
controlled-network, preliminary-campaign, and paper gates. Those rows retain
`remote-capable`/`preliminary` language and may not claim qualification.

Only after every required local row has exit code zero and a checker-valid
artifact, replace the four-column R-10 traceability row in design Section 1.1
with:

````markdown
| R-10 | Current/experimental/proposed/historical documentation split | Verified | `tests/docs/test_documentation_contract.py`; `scripts/docs/check_docs.py`; `docs/research/r10-implementation-log.md` |
````

Do not add a second prose-only status that leaves the authoritative row
unchanged.

- [ ] **Step 4: Run the complete static gate**

```bash
scripts/research/run-python -m pytest -p no:cacheprovider tests/docs -q
scripts/research/run-python scripts/docs/render_api_reference.py --check
scripts/research/run-python scripts/docs/check_docs.py
```

Expected: all docs tests pass; API counts are derived from the controlled default
and lab applications; checker prints its computed maintained-file count, and the
printed integer equals `len(MAINTAINED_MARKDOWN)`.

- [ ] **Step 5: Commit the clean verification candidate**

Stage each checker-reported modified file individually with `git add --`; stage
only the new implementation log with
`git add -- docs/research/r10-implementation-log.md`. Confirm
`git diff --cached --name-only` is a subset of this task's file map, run
`git diff --cached --check` and `commit-check`, inspect the cached diff, then run:

```bash
git commit -m "docs(infra): close the R-10 documentation contract"
```

- [ ] **Step 6: Run local artifact, lab, and operator verification**

Run this non-interactive gate from the task worktree. It creates a clean detached
worktree at the candidate commit, synchronizes the Slice 1 hash-locked external
Python environment through that checkout's `scripts/research/run-python`, retains
one machine-readable JSON object and stdout log per gate under an external
`/tmp` directory, runs the Slice 3 clean-checkout lab gate before quick output
dirties the checkout, and checks every emitted artifact against the same source
checkout.

```bash
set -euo pipefail
TASK_ROOT="$(git rev-parse --show-toplevel)"
CANDIDATE_COMMIT="$(git rev-parse HEAD)"
test -z "$(git status --porcelain)"
VERIFY_TMP="$(mktemp -d /tmp/edgecitadel-r10.XXXXXX)"
CLEAN_ROOT="$VERIFY_TMP/repo"
RETAINED="/tmp/edgecitadel-r10-verification/$CANDIDATE_COMMIT"
RECORDS="$RETAINED/gate-records.jsonl"
test ! -e "$RETAINED"
mkdir -p "$RETAINED"
: >"$RECORDS"
git worktree add --detach "$CLEAN_ROOT" "$CANDIDATE_COMMIT"
cleanup_r10_gate() {
  git -C "$TASK_ROOT" worktree remove --force "$CLEAN_ROOT" >/dev/null 2>&1 || true
  rm -rf "$VERIFY_TMP"
}
trap cleanup_r10_gate EXIT

test -x "$CLEAN_ROOT/scripts/research/run-python"
test -f "$CLEAN_ROOT/scripts/research/requirements.lock.txt"
PY="$CLEAN_ROOT/scripts/research/run-python"
"$PY" -c 'import sys; assert sys.version_info[:2] == (3, 12)'
npm --prefix "$CLEAN_ROOT/frontend" ci
npm --prefix "$CLEAN_ROOT/e2e" ci
npm --prefix "$CLEAN_ROOT/e2e" exec -- playwright install chromium
test -z "$(git -C "$CLEAN_ROOT" status --porcelain)"

run_gate() {
  gate="$1"
  label="$2"
  descriptor="$3"
  command="$4"
  log="$RETAINED/$gate.log"
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  bash -lc "$command" >"$log" 2>&1
  rc=$?
  set -e
  ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  GATE="$gate" LABEL="$label" DESCRIPTOR="$descriptor" COMMAND="$command" \
    LOG="$log" STARTED="$started" ENDED="$ended" RC="$rc" \
    SOURCE_COMMIT="$CANDIDATE_COMMIT" RECORDS="$RECORDS" \
    RETAINED="$RETAINED" "$PY" - <<'PY'
import hashlib
import json
import os
import shutil
from pathlib import Path

artifact = metadata = checker = retained_metadata = "N/A"
metadata_sha256 = "N/A"
descriptor = Path(os.environ["DESCRIPTOR"])
if descriptor.exists():
    values = descriptor.read_text().splitlines()
    assert len(values) == 3, values
    artifact, metadata, checker = values
    if metadata != "N/A":
        source = Path(metadata)
        metadata_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
        retained = Path(os.environ["RETAINED"]) / f"{os.environ['GATE']}-metadata.json"
        shutil.copy2(source, retained)
        assert hashlib.sha256(retained.read_bytes()).hexdigest() == metadata_sha256
        retained_metadata = str(retained)
log = Path(os.environ["LOG"])
record = {
    "gate": os.environ["GATE"],
    "source_commit": os.environ["SOURCE_COMMIT"],
    "command": os.environ["COMMAND"],
    "started_utc": os.environ["STARTED"],
    "ended_utc": os.environ["ENDED"],
    "exit_code": int(os.environ["RC"]),
    "stdout_path": str(log),
    "stdout_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
    "artifact_path": artifact,
    "metadata_path": metadata,
    "retained_metadata_path": retained_metadata,
    "metadata_sha256": metadata_sha256,
    "checker": checker,
    "evidence_label": os.environ["LABEL"],
}
with Path(os.environ["RECORDS"]).open("a") as handle:
    handle.write(json.dumps(record, sort_keys=True) + "\n")
PY
  test "$rc" -eq 0
}

printf 'N/A\nN/A\nN/A\n' >"$VERIFY_TMP/static.descriptor"
STATIC_COMMAND="cd '$CLEAN_ROOT' && \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$CLEAN_ROOT' '$PY' -m pytest \
-p no:cacheprovider aggregator/tests schemas/tests adapters tests/research \
--ignore=aggregator/tests/test_jetstream_bootstrap.py -q && \
npm --prefix '$CLEAN_ROOT/frontend' run build && \
npm --prefix '$CLEAN_ROOT/e2e' test && \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$CLEAN_ROOT' '$PY' -m pytest \
-p no:cacheprovider tests/docs -q && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/docs/render_api_reference.py --check && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/docs/check_docs.py"
run_gate static "Static verification; not experimental evidence" \
  "$VERIFY_TMP/static.descriptor" "$STATIC_COMMAND"

LAB_ROOT="$RETAINED/same-host-lab"
LAB_RECEIPT="$LAB_ROOT/receipt.json"
LAB_BUNDLES="$LAB_ROOT/bundles"
mkdir -p "$LAB_ROOT"
test ! -e "$LAB_RECEIPT"
test ! -e "$LAB_BUNDLES"
LAB_VALIDATE="$VERIFY_TMP/validate-lab-receipt.py"
cat >"$LAB_VALIDATE" <<'PY'
import json
import sys
from pathlib import Path

receipt_path = Path(sys.argv[1]).resolve()
retained = Path(sys.argv[2]).resolve()
source_commit = sys.argv[3]
receipt = json.loads(receipt_path.read_text())
assert set(receipt) == {
    "schema_version",
    "source_commit",
    "source_snapshot_sha256",
    "bundles",
    "cleanup",
}
assert receipt["schema_version"] == "1"
assert receipt["source_commit"] == source_commit
assert len(receipt["source_snapshot_sha256"]) == 64
assert set(receipt["source_snapshot_sha256"]) <= set("0123456789abcdef")
assert set(receipt["bundles"]) == {"lifecycle", "operator_smoke"}
assert receipt["cleanup"] == {
    "complete": True,
    "owned_resources_remaining": [],
}
for row in receipt["bundles"].values():
    assert set(row) == {"path", "checker_valid", "finalizer_count"}
    assert "\\" not in row["path"]
    assert not Path(row["path"]).is_absolute()
    bundle = (receipt_path.parent / row["path"]).resolve()
    bundle.relative_to(receipt_path.parent)
    bundle.relative_to(retained)
    assert row["checker_valid"] is True
    assert row["finalizer_count"] == 1
    assert (bundle / "manifest.json").is_file()
PY
LAB_COMMAND="cd '$CLEAN_ROOT' && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/lab_gate.py \
--clean-checkout-gate --repo-root '$CLEAN_ROOT' --run-id r10-lab-01 \
--host-id controller-lab-01 --receipt '$LAB_RECEIPT' \
--retain-bundles '$LAB_BUNDLES' && \
'$PY' -m json.tool '$LAB_RECEIPT' >/dev/null && \
'$PY' '$LAB_VALIDATE' '$LAB_RECEIPT' '$LAB_BUNDLES' '$CANDIDATE_COMMIT' && \
test \"\$(find '$LAB_BUNDLES' -mindepth 1 -maxdepth 1 -type d \
  | wc -l | tr -d ' ')\" = 2 && \
for LAB_BUNDLE in '$LAB_BUNDLES'/*; do \
  PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/check_artifact.py \
    --bundle \"\$LAB_BUNDLE\" --require-kind lab \
    --source-root '$CLEAN_ROOT' || exit 1; \
done && \
printf '%s\\n%s\\nPASS\\n' '$LAB_BUNDLES' '$LAB_RECEIPT' \
>'$VERIFY_TMP/lab.descriptor'"
run_gate same-host-lab "Same-host lab evidence; not remote qualification" \
  "$VERIFY_TMP/lab.descriptor" "$LAB_COMMAND"

QUICK_RESULT="$VERIFY_TMP/quick-result.json"
QUICK_COMMAND="cd '$CLEAN_ROOT' && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/run_artifact.py run \
--profile quick --result-file '$QUICK_RESULT' && \
QUICK_CAMPAIGN=\"\$('$PY' -c \
'import json; print(json.load(open(\"$QUICK_RESULT\"))[\"campaign_path\"])')\" && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/check_artifact.py \
--campaign \"\$QUICK_CAMPAIGN\" --source-root '$CLEAN_ROOT' && \
printf '%s\\n%s\\nPASS\\n' \"\$QUICK_CAMPAIGN\" \
\"\$QUICK_CAMPAIGN/campaign.json\" >'$VERIFY_TMP/quick.descriptor'"
run_gate quick "Development benchmark evidence; not preliminary evidence" \
  "$VERIFY_TMP/quick.descriptor" "$QUICK_COMMAND"

MATRIX_RESULT="$VERIFY_TMP/matrix-result.json"
MATRIX_COMMAND="cd '$CLEAN_ROOT' && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/run_artifact.py run \
--profile matrix-smoke --result-file '$MATRIX_RESULT' && \
MATRIX_CAMPAIGN=\"\$('$PY' -c \
'import json; print(json.load(open(\"$MATRIX_RESULT\"))[\"campaign_path\"])')\" && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/check_artifact.py \
--campaign \"\$MATRIX_CAMPAIGN\" --source-root '$CLEAN_ROOT' && \
printf '%s\\n%s\\nPASS\\n' \"\$MATRIX_CAMPAIGN\" \
\"\$MATRIX_CAMPAIGN/campaign.json\" >'$VERIFY_TMP/matrix-smoke.descriptor'"
run_gate matrix-smoke "Development matrix smoke; not preliminary evidence" \
  "$VERIFY_TMP/matrix-smoke.descriptor" "$MATRIX_COMMAND"

OPERATOR_ROOT="$VERIFY_TMP/operator"
OPERATOR_COMMAND="cd '$CLEAN_ROOT' && mkdir -p '$OPERATOR_ROOT' && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/capture_operator_journey.py \
--output-root '$OPERATOR_ROOT' --source-root '$CLEAN_ROOT' \
>'$VERIFY_TMP/operator-output.txt' && \
OPERATOR_BUNDLE=\"\$(sed -n '1p' '$VERIFY_TMP/operator-output.txt')\" && \
test \"\$(sed -n '2p' '$VERIFY_TMP/operator-output.txt')\" = PASS && \
PYTHONPATH='$CLEAN_ROOT' '$PY' scripts/research/check_artifact.py \
--bundle \"\$OPERATOR_BUNDLE\" --require-kind operator \
--source-root '$CLEAN_ROOT' && \
printf '%s\\n%s\\nPASS\\n' \"\$OPERATOR_BUNDLE\" \
\"\$OPERATOR_BUNDLE/manifest.json\" >'$VERIFY_TMP/operator.descriptor'"
run_gate operator "Deterministic operator evidence" \
  "$VERIFY_TMP/operator.descriptor" "$OPERATOR_COMMAND"

test "$(wc -l <"$RECORDS" | tr -d ' ')" = 5
printf '%s\n' "$CANDIDATE_COMMIT" >"$RETAINED/source-commit.txt"
```

Expected: Python suites pass, Vite exits `0`, deterministic Playwright passes, and
both research profiles, the same-host multi-node lifecycle, and a fresh
desktop/mobile operator bundle pass their artifact checkers. Exactly five JSONL
records, five stdout logs, four copied metadata files, the original lab receipt,
and exactly two retained checked lab bundles remain under
`/tmp/edgecitadel-r10-verification/<candidate-commit>/` after the trap removes
the temporary worktree, operator output, and non-retained run-owned results. The
lock-keyed managed Python environment remains only in the external location
owned by `run-python`. The static suite never contacts an ambient JetStream
endpoint; live JetStream remains explicit opt-in and the isolated lab gate
verifies the runtime path. Remote-host, ARM64, controlled-network, preliminary,
and paper gates remain explicitly not run.

- [ ] **Step 7: Record and commit verified R-10 status**

Render the five local Markdown rows from the retained JSONL with this read-only
command. It rejects a missing gate, nonzero exit, wrong source commit, bad retained
metadata hash, or checker result other than `PASS`; it prints Markdown and does
not modify repository files:

```bash
TASK_ROOT="$(git rev-parse --show-toplevel)"
CANDIDATE_COMMIT="$(git rev-parse HEAD)"
RECORDS="/tmp/edgecitadel-r10-verification/$CANDIDATE_COMMIT/gate-records.jsonl"
RECORDS="$RECORDS" CANDIDATE_COMMIT="$CANDIDATE_COMMIT" \
  scripts/research/run-python - <<'PY'
import hashlib
import json
import os
from pathlib import Path

expected = ["static", "quick", "matrix-smoke", "same-host-lab", "operator"]
records = [
    json.loads(line)
    for line in Path(os.environ["RECORDS"]).read_text().splitlines()
]
by_gate = {record["gate"]: record for record in records}
assert len(records) == len(by_gate) == 5
assert sorted(by_gate) == sorted(expected)
for gate in expected:
    row = by_gate[gate]
    assert row["source_commit"] == os.environ["CANDIDATE_COMMIT"]
    assert row["exit_code"] == 0
    if gate == "static":
        assert row["artifact_path"] == row["metadata_path"] == "N/A"
        assert row["checker"] == "N/A"
    else:
        retained = Path(row["retained_metadata_path"])
        assert hashlib.sha256(retained.read_bytes()).hexdigest() == row["metadata_sha256"]
        assert row["checker"] == "PASS"
    cells = [
        gate, row["command"], row["started_utc"], row["ended_utc"],
        str(row["exit_code"]), row["artifact_path"], row["metadata_path"],
        row["metadata_sha256"], row["checker"], row["evidence_label"],
    ]
    print("| " + " | ".join(cell.replace("|", "\\|") for cell in cells) + " |")
PY
```

Use `apply_patch` to replace only the five pending local rows in the implementation
log with the exact printed rows. Leave the five `Not run` rows unchanged. Use
`apply_patch` again to replace the R-10 traceability row with the exact
four-column `Verified` row from Step 3. Then invoke the project
`verify-backend`, `verify-frontend`, and `verify-infra` skills in that order.
Follow each skill's commands and fallbacks exactly; `verify-infra` must perform
its full-stack restart and Playwright gate, so curl alone is not sufficient.
Use `apply_patch` to add a `Skill verification` subsection to the implementation
log with each skill name, exact commands, UTC start/end, result, and any
explicitly documented fallback. Rerun the complete documentation gate after
that log update, then commit:

```bash
scripts/research/run-python -m pytest -p no:cacheprovider tests/docs -q
scripts/research/run-python scripts/docs/render_api_reference.py --check
scripts/research/run-python scripts/docs/check_docs.py
git add -- docs/research/r10-implementation-log.md \
  docs/research/task-aware-reliability-contract-design.md
git diff --cached --name-only
git diff --cached --check
# Invoke commit-check and inspect git diff --cached before committing.
git commit -m "docs(infra): verify the R-10 documentation contract"
```

- [ ] **Step 8: Record the verified persistent handoff**

Do not fast-forward, merge, stash, reset, clean, or overwrite the canonical
checkout: it may still contain unrelated user state. Instead, preserve the
completed local branch and linked worktree as the authoritative implementation
source, create a self-contained Git bundle beside it, and atomically advance the
shared handoff:

```bash
set -euo pipefail
TASK_ROOT="$(git rev-parse --show-toplevel)"
CANONICAL_ROOT=/Users/yefanzhang/workplace/edge-research
CANONICAL_BASE="$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
CHAIN_KEY="$(printf '%s' "$CANONICAL_BASE" | cut -c1-12)"
CHAIN_ROOT="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY"
HANDOFF="$CHAIN_ROOT/handoff.env"
test "$TASK_ROOT" = "$CHAIN_ROOT/repo"
test -f "$HANDOFF"
# shellcheck disable=SC1090
source "$HANDOFF"
test "$CANONICAL_ROOT" = /Users/yefanzhang/workplace/edge-research
test "$CANONICAL_BASE" = "$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
test "$TASK_ROOT" = "$(git rev-parse --show-toplevel)"
test "$(git branch --show-current)" = "$BRANCH"
test -z "$(git status --porcelain)"
git merge-base --is-ancestor "$FINAL_COMMIT" HEAD
FINAL_COMMIT="$(git rev-parse HEAD)"
CANONICAL_SNAPSHOT="$CHAIN_ROOT/canonical"
git -C "$CANONICAL_ROOT" write-tree \
  >"$CANONICAL_SNAPSHOT.index-tree.slice4-final"
git -C "$CANONICAL_ROOT" diff --binary \
  >"$CANONICAL_SNAPSHOT.unstaged.patch.slice4-final"
git -C "$CANONICAL_ROOT" diff --cached --binary \
  >"$CANONICAL_SNAPSHOT.staged.patch.slice4-final"
git -C "$CANONICAL_ROOT" status \
  --porcelain=v2 -z --untracked-files=all \
  >"$CANONICAL_SNAPSHOT.status.z.slice4-final"
git -C "$CANONICAL_ROOT" ls-files \
  --others --exclude-standard -z \
  >"$CANONICAL_SNAPSHOT.untracked.z.slice4-final"
while IFS= read -r -d '' relative; do
  test -f "$CANONICAL_ROOT/$relative"
  digest="$(shasum -a 256 "$CANONICAL_ROOT/$relative" | awk '{print $1}')"
  printf '%s  %q\n' "$digest" "$relative"
done <"$CANONICAL_SNAPSHOT.untracked.z.slice4-final" \
  >"$CANONICAL_SNAPSHOT.untracked.sha256.slice4-final"
for suffix in index-tree unstaged.patch staged.patch status.z \
  untracked.z untracked.sha256; do
  cmp "$CANONICAL_SNAPSHOT.$suffix" \
    "$CANONICAL_SNAPSHOT.$suffix.slice4-final"
  rm "$CANONICAL_SNAPSHOT.$suffix.slice4-final"
done
BUNDLE="$CHAIN_ROOT/edgecitadel-paper-$FINAL_COMMIT.bundle"
test ! -e "$BUNDLE"
git bundle create "$BUNDLE" "$BRANCH"
git bundle verify "$BUNDLE"
BUNDLE_SHA256="$(shasum -a 256 "$BUNDLE" | awk '{print $1}')"
CANONICAL_STATUS_SHA256="$(
  git -C "$CANONICAL_ROOT" status --porcelain=v2 -z --untracked-files=all |
    shasum -a 256 | awk '{print $1}'
)"
{
  printf 'CANONICAL_ROOT=%q\n' "$CANONICAL_ROOT"
  printf 'CANONICAL_BASE=%q\n' "$CANONICAL_BASE"
  printf 'TASK_ROOT=%q\n' "$TASK_ROOT"
  printf 'BRANCH=%q\n' "$BRANCH"
  printf 'FINAL_COMMIT=%q\n' "$FINAL_COMMIT"
  printf 'BUNDLE=%q\n' "$BUNDLE"
  printf 'BUNDLE_SHA256=%q\n' "$BUNDLE_SHA256"
  printf 'CANONICAL_STATUS_SHA256=%q\n' "$CANONICAL_STATUS_SHA256"
} >"$HANDOFF.tmp"
mv "$HANDOFF.tmp" "$HANDOFF"
```

Expected: the task worktree is clean at the verified R-10 commit, the bundle
verifies and hashes, the persistent handoff names that exact commit and source,
and the canonical checkout's branch, index, files, and untracked state are not
mutated. Task 11 uses `TASK_ROOT` from this handoff for every vault source key.

### Task 11: Sync R-10 to the Obsidian vault

**Files outside the repository:**

- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/codebases/edge-research/modules/docs.md`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/_entities/EdgeCitadel.md`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/sources/nats-agent-communication/summary.md`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/sources/nats-agent-communication/future-directions.md`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/sources/nats-agent-communication/benchmark-suite-e1-e12.md`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/sources/nats-agent-communication/paper-outline.md`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/sources/nats-agent-communication/sources.md`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json`
- Modify: `/Users/yefanzhang/Documents/Obsidian Vault/log.md`
- Create or update: `/Users/yefanzhang/Documents/Obsidian Vault/_meta/lint-2026-07-25.md`

- [ ] **Step 1: Re-read the vault contract**

```bash
test -f '/Users/yefanzhang/Documents/Obsidian Vault/schema.md'
sed -n '1,180p' '/Users/yefanzhang/Documents/Obsidian Vault/schema.md'
CANONICAL_ROOT=/Users/yefanzhang/workplace/edge-research
CANONICAL_BASE="$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
CHAIN_KEY="$(printf '%s' "$CANONICAL_BASE" | cut -c1-12)"
HANDOFF="/Users/yefanzhang/workplace/edge-research-worktrees/paper-$CHAIN_KEY/handoff.env"
test -f "$HANDOFF"
# shellcheck disable=SC1090
source "$HANDOFF"
test "$CANONICAL_ROOT" = /Users/yefanzhang/workplace/edge-research
test "$CANONICAL_BASE" = "$(git -C "$CANONICAL_ROOT" rev-parse HEAD)"
test "$TASK_ROOT" = "$(git rev-parse --show-toplevel)"
test "$FINAL_COMMIT" = "$(git rev-parse HEAD)"
test -z "$(git status --porcelain)"
test "$(shasum -a 256 "$BUNDLE" | awk '{print $1}')" = "$BUNDLE_SHA256"
for relative in \
  docs/research/task-aware-reliability-contract-design.md \
  docs/research/plans/2026-07-25-slice-4-documentation-artifact.md \
  docs/research/r10-implementation-log.md \
  docs/archive/research/future-directions-roadmap.html; do
  test -f "$TASK_ROOT/$relative"
done
```

Expected: the file exists and prints the required frontmatter, provenance, source,
manifest, and log rules. Task 10's guarded handoff identifies the persistent
clean worktree and verified R-10 commit used by the new vault source keys, while
the canonical checkout remains untouched. Stop before vault sync if any
assertion fails.

- [ ] **Step 2: Update every page referenced by the changed source keys**

In `codebases/edge-research/modules/docs.md`, replace the stale numbered-doc list
with the maintained/current, experimental, proposed, and historical structure from
`docs/README.md`. Add an `R-10 verification` section naming the docs checker,
generated API reference, exact archive root, artifact runbook, ADR index, and
Functional probe rule.

In `_entities/EdgeCitadel.md`, replace the stale phase/status paragraph with the
verified Slice 4 status and retain readiness limits exactly as stated in the
design. Add the design, Slice 4 plan, and R-10 implementation-log sources to
every page listed for them in the manifest, using one execution timestamp.

Use the absolute `$TASK_ROOT` prefix from the verified handoff for these three
source keys:

```text
$TASK_ROOT/docs/research/task-aware-reliability-contract-design.md
$TASK_ROOT/docs/research/plans/2026-07-25-slice-4-documentation-artifact.md
$TASK_ROOT/docs/research/r10-implementation-log.md
```

Replace any existing canonical-checkout key for the same relative file; do not
retain duplicate manifest or page-source entries for both checkout roots.

In all six pages referenced by the old
`docs/research/future-directions-roadmap.html` manifest entry, replace that source
path with `docs/archive/research/future-directions-roadmap.html` and use the same
execution timestamp. Recount `provenance.extracted`, `provenance.inferred`, and
`provenance.ambiguous`; update every touched page's frontmatter `updated`
timestamp. Do not advance any readiness label beyond the implementation log.

- [ ] **Step 3: Update manifest and log atomically**

Here, "atomically" means preparing and reviewing one `apply_patch` change set
covering all touched pages, `.manifest.json`, and `log.md`, then applying it once.
Do not use an ad hoc script to write vault files. Compute one timestamp and the
four exact source hashes with read-only commands:

```bash
TASK_ROOT="$(git rev-parse --show-toplevel)"
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'STAMP=%s\n' "$STAMP"
shasum -a 256 \
  "$TASK_ROOT/docs/research/task-aware-reliability-contract-design.md" \
  "$TASK_ROOT/docs/research/plans/2026-07-25-slice-4-documentation-artifact.md" \
  "$TASK_ROOT/docs/research/r10-implementation-log.md" \
  "$TASK_ROOT/docs/archive/research/future-directions-roadmap.html"
```

Use `apply_patch` once to make these exact coordinated changes:

- update every touched page's frontmatter `updated` value to `$STAMP`;
- add or update each page's source entry with the exact source path and `$STAMP`,
  and recount nonnegative integer `provenance.extracted`,
  `provenance.inferred`, and `provenance.ambiguous` values;
- remove the old roadmap source key and every superseded canonical-checkout key
  for the design, plan, and implementation log from `.manifest.json`; add their
  `$TASK_ROOT` keys, including the archived roadmap key with its existing
  six-page list, the page lists specified in Step 2, the printed SHA-256 values,
  `"type": "source"`, and `"ingested_at": "$STAMP"`;
- insert the R-10 sync line immediately after the YAML delimiter in `log.md`, so
  it is the newest entry.

Expected: the patch applies as one reviewed unit. No Python or shell command
writes a vault file.

- [ ] **Step 4: Verify vault bookkeeping**

```bash
TASK_ROOT="$(git rev-parse --show-toplevel)"
scripts/research/run-python -m json.tool \
  '/Users/yefanzhang/Documents/Obsidian Vault/.manifest.json' >/dev/null
TASK_ROOT="$TASK_ROOT" scripts/research/run-python - <<'PY'
import hashlib
import json
import os
import re
from pathlib import Path

repo = Path(os.environ["TASK_ROOT"])
canonical = Path("/Users/yefanzhang/workplace/edge-research")
vault = Path("/Users/yefanzhang/Documents/Obsidian Vault")
manifest = json.loads((vault / ".manifest.json").read_text())["sources"]
paths = {
    repo / "docs/research/task-aware-reliability-contract-design.md": {
        "_entities/EdgeCitadel.md",
        "sources/nats-agent-communication/summary.md",
        "sources/nats-agent-communication/paper-outline.md",
        "codebases/edge-research/modules/docs.md",
    },
    repo / "docs/research/plans/2026-07-25-slice-4-documentation-artifact.md": {
        "codebases/edge-research/modules/docs.md",
    },
    repo / "docs/research/r10-implementation-log.md": {
        "_entities/EdgeCitadel.md",
        "codebases/edge-research/modules/docs.md",
    },
    repo / "docs/archive/research/future-directions-roadmap.html": {
        "sources/nats-agent-communication/summary.md",
        "sources/nats-agent-communication/future-directions.md",
        "sources/nats-agent-communication/benchmark-suite-e1-e12.md",
        "sources/nats-agent-communication/paper-outline.md",
        "sources/nats-agent-communication/sources.md",
        "codebases/edge-research/modules/docs.md",
    },
}
old = str(repo / "docs/research/future-directions-roadmap.html")
assert old not in manifest
assert str(canonical / "docs/research/future-directions-roadmap.html") not in manifest
for relative in (
    "docs/research/task-aware-reliability-contract-design.md",
    "docs/research/plans/2026-07-25-slice-4-documentation-artifact.md",
    "docs/research/r10-implementation-log.md",
):
    assert str(canonical / relative) not in manifest
stamps = set()
for path, expected_pages in paths.items():
    record = manifest[str(path)]
    assert record["type"] == "source"
    assert set(record["pages"]) == expected_pages
    assert record["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    stamps.add(record["ingested_at"])
    for page_name in record["pages"]:
        page = (vault / page_name).read_text()
        assert str(path) in page
        assert record["ingested_at"] in page
        frontmatter = page.split("---", 2)[1]
        assert re.search(
            rf"(?m)^updated:\s*['\"]?{re.escape(record['ingested_at'])}['\"]?\s*$",
            frontmatter,
        )
        for field in ("extracted", "inferred", "ambiguous"):
            match = re.search(rf"(?m)^\s*{field}:\s*(\d+)\s*$", frontmatter)
            assert match and int(match.group(1)) >= 0
assert len(stamps) == 1
log_lines = (vault / "log.md").read_text().split("---\n", 1)[1].strip().splitlines()
assert "R-10 documentation contract" in log_lines[0]
assert sum("R-10 documentation contract" in line for line in log_lines) == 1
PY
```

Expected: JSON validation and exact key/hash/timestamp/page checks exit `0`; the
old source key is absent and, before lint runs, the newest log entry is the one
R-10 sync. Vault files are external knowledge artifacts and are not added to the
Git commit.

- [ ] **Step 5: Run lint and verify final log chronology**

Invoke only the gather/report phase of the `source-command-wiki-lint` skill.
Under the approved no-interaction execution mode, decline its optional fix phase
without prompting and do not change unrelated pages. Require zero lint findings
attributable to the nine touched vault files or their four source keys; retain
unrelated pre-existing findings in the dated report rather than expanding scope.
The lint run writes its dated report and inserts its newer log event ahead of the
R-10 sync. Verify that final order:

```bash
test -f \
  '/Users/yefanzhang/Documents/Obsidian Vault/_meta/lint-2026-07-25.md'
scripts/research/run-python - <<'PY'
from pathlib import Path

vault = Path("/Users/yefanzhang/Documents/Obsidian Vault")
lines = (vault / "log.md").read_text().split("---\n", 1)[1].strip().splitlines()
assert "lint" in lines[0]
assert "_meta/lint-2026-07-25.md" in lines[0]
assert "R-10 documentation contract" in lines[1]
assert sum("R-10 documentation contract" in line for line in lines) == 1
PY
```

Expected: the lint report exists, the lint event is the newest log entry, the
single R-10 sync is immediately below it, and no optional fixes or unrelated page
changes occur.

## Final Verification

Run from a clean checkout at the R-10 commit:

```bash
git status --short
scripts/research/run-python -m pytest -p no:cacheprovider tests/docs -q
scripts/research/run-python scripts/docs/render_api_reference.py --check
scripts/research/run-python scripts/docs/check_docs.py
docker compose config -q
```

Expected: `git status --short` is empty; all tests/checks exit `0`; generated API
counts are current; Compose validation emits no error.

Plan complete and saved to
`docs/research/plans/2026-07-25-slice-4-documentation-artifact.md`.

Execution uses `superpowers:subagent-driven-development` task-by-task, with review
after every narrow commit. Use `superpowers:executing-plans` only when running the
same task sequence inline with checkpoints.
