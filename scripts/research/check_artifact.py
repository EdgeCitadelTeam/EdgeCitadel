"""Validate finalized research evidence bundles without executing a workload."""

from __future__ import annotations

import argparse
import json
import math
import random
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError

from scripts.research.evidence import (
    capture_source_provenance,
    file_sha256,
    manifest_sha256,
)
from scripts.research.lab_contract import lab_semantic_issues
from scripts.research.workload_matrix import (
    MatrixCell,
    classify_outcome,
    required_matrix_cells,
)

OPERATOR_PROJECTS = ("desktop", "mobile")
TERMINAL_STATES = {"completed", "failed", "canceled", "rejected"}
_OBSERVATION_COUNTS = (
    "initiated",
    "accepted",
    "delivered",
    "handler_attempts",
    "executions",
    "side_effects",
    "prepared_outcomes",
    "logical_terminals",
    "distinct_terminal_ids",
    "publication_attempts",
    "wire_deliveries",
    "progress_generated",
    "progress_live_delivered",
    "progress_replay_delivered",
    "progress_missing",
    "poison",
)
_RESOURCE_METRICS = (
    "cpu_seconds",
    "peak_rss_bytes",
    "rss_seconds",
    "rx_bytes",
    "tx_bytes",
    "application_bytes",
    "nats_connection_bytes",
    "http_bytes",
    "storage_bytes",
    "message_count_delta",
    "sampler_cpu_seconds",
)


@dataclass(frozen=True)
class ArtifactIssue:
    code: str
    path: str
    message: str


class ArtifactInvalid(RuntimeError):
    """Raised when a caller requires an invalid immutable artifact."""


@dataclass(frozen=True)
class CheckReport:
    valid: bool
    issues: tuple[ArtifactIssue, ...]

    def require_valid(self) -> None:
        if not self.valid:
            raise ArtifactInvalid(
                "; ".join(
                    f"{item.code}: {item.path}: {item.message}" for item in self.issues
                )
            )


def _report(issues: Sequence[ArtifactIssue]) -> CheckReport:
    ordered = tuple(
        sorted(issues, key=lambda item: (item.code, item.path, item.message))
    )
    return CheckReport(not ordered, ordered)


def _read_json(path: Path, issues: list[ArtifactIssue]) -> object | None:
    try:
        value: object = json.loads(path.read_text())
        return value
    except (OSError, json.JSONDecodeError):
        issues.append(ArtifactIssue("OPERATOR_JSON_INVALID", path.name, "invalid JSON"))
        return None


def _issue(
    issues: list[ArtifactIssue], code: str, path: Path | str, message: str
) -> None:
    issues.append(ArtifactIssue(code, str(path), message))


def _operator_issues(
    bundle: Path, manifest: Mapping[str, object], source_root: Path | None
) -> list[ArtifactIssue]:
    issues: list[ArtifactIssue] = []
    projects = manifest.get("projects")
    if not isinstance(projects, Mapping) or set(projects) != set(OPERATOR_PROJECTS):
        _issue(
            issues,
            "OPERATOR_PROJECTS_INVALID",
            "manifest.json",
            "desktop and mobile projects are required",
        )
        return issues
    task_ids = [
        projects[name].get("task_id")
        for name in OPERATOR_PROJECTS
        if isinstance(projects[name], Mapping)
    ]
    if len(task_ids) != 2 or len(set(task_ids)) != 2:
        _issue(
            issues,
            "OPERATOR_TASK_IDS_NOT_DISTINCT",
            "manifest.json",
            "desktop and mobile task IDs must differ",
        )
    for name in OPERATOR_PROJECTS:
        expected = projects[name]
        if not isinstance(expected, Mapping):
            continue
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
        missing = False
        for relative in required:
            if not (bundle / relative).is_file():
                _issue(
                    issues,
                    "OPERATOR_ARTIFACT_MISSING",
                    relative,
                    "required operator artifact is absent",
                )
                missing = True
        if missing:
            continue
        metadata = _read_json(bundle / project_root / "operator-metadata.json", issues)
        messages = _read_json(bundle / api_root / "messages.json", issues)
        status = _read_json(bundle / api_root / "system-status.json", issues)
        registry = _read_json(bundle / api_root / "registry.json", issues)
        queue = _read_json(bundle / api_root / "queue.json", issues)
        if not isinstance(metadata, Mapping) or not isinstance(messages, list):
            continue
        if metadata.get("project") != name:
            _issue(
                issues,
                "OPERATOR_METADATA_MISMATCH",
                project_root / "operator-metadata.json",
                "metadata project differs from directory",
            )
        for key, value in expected.items():
            if metadata.get(key) != value:
                _issue(
                    issues,
                    "OPERATOR_METADATA_MISMATCH",
                    project_root / "operator-metadata.json",
                    f"{name} {key} does not match manifest",
                )
        if expected.get("command_body") != expected.get("nonce"):
            _issue(
                issues,
                "OPERATOR_COMMAND_BODY_MISMATCH",
                "manifest.json",
                f"{name} command body differs from nonce",
            )
        if expected.get("expected_output") != f"edgecitadel:{expected.get('nonce')}":
            _issue(
                issues,
                "OPERATOR_OUTPUT_MISMATCH",
                "manifest.json",
                f"{name} output is not deterministic",
            )
        if any(
            not isinstance(row, Mapping)
            or row.get("task_id") != expected.get("task_id")
            for row in messages
        ):
            _issue(
                issues,
                "OPERATOR_CROSS_PROJECT_TASK",
                api_root / "messages.json",
                f"{name} contains another task",
            )
        commands = [
            row
            for row in messages
            if isinstance(row, Mapping) and row.get("type") == "command"
        ]
        terminals = [
            row
            for row in messages
            if isinstance(row, Mapping)
            and row.get("type") == "result"
            and row.get("task_state") in TERMINAL_STATES
        ]
        if len(commands) != 1 or commands[0].get("payload", {}).get(
            "body"
        ) != expected.get("command_body"):
            _issue(
                issues,
                "OPERATOR_COMMAND_COUNT_OR_BODY",
                api_root / "messages.json",
                f"{name} must contain one exact command",
            )
        if (
            len(terminals) != 1
            or terminals[0].get("task_state") != "completed"
            or terminals[0].get("payload", {}).get("body")
            != expected.get("expected_output")
        ):
            _issue(
                issues,
                "OPERATOR_TERMINAL_COUNT_OR_BODY",
                api_root / "messages.json",
                f"{name} must contain one completed result",
            )
        if len(commands) == 1:
            context_id = commands[0].get("context_id")
            correlated = [
                row
                for row in messages
                if isinstance(row, Mapping)
                and row.get("type") in {"task.progress", "result"}
            ]
            if (
                not context_id
                or commands[0].get("hop_count") != 0
                or any(
                    row.get("context_id") != context_id or row.get("hop_count") != 0
                    for row in correlated
                )
            ):
                _issue(
                    issues,
                    "OPERATOR_CORRELATION_MISMATCH",
                    api_root / "messages.json",
                    f"{name} correlation is not preserved",
                )
        if (
            not isinstance(status, Mapping)
            or status.get("nats_connected") is not True
            or status.get("jetstream_stream_ok") is not True
        ):
            _issue(
                issues,
                "OPERATOR_SYSTEM_UNHEALTHY",
                api_root / "system-status.json",
                f"{name} system status is unhealthy",
            )
        shell = (
            [
                row
                for row in registry
                if isinstance(row, Mapping) and row.get("agent_id") == "shell-1"
            ]
            if isinstance(registry, list)
            else []
        )
        if len(shell) != 1 or shell[0].get("agent_state") != "online":
            _issue(
                issues,
                "OPERATOR_SHELL_NOT_ONLINE",
                api_root / "registry.json",
                f"{name} requires one online shell-1",
            )
        elif (
            shell[0].get("card", {}).get("metadata", {}).get("runtime.conformance")
            != "L1"
        ):
            _issue(
                issues,
                "OPERATOR_CONFORMANCE_MISMATCH",
                api_root / "registry.json",
                f"{name} shell-1 is not L1",
            )
        if (
            not isinstance(queue, Mapping)
            or queue.get("pending") != 0
            or queue.get("ack_pending") != 0
        ):
            _issue(
                issues,
                "OPERATOR_QUEUE_NOT_DRAINED",
                api_root / "queue.json",
                f"{name} queue is not drained",
            )
    runtime_path = bundle / "raw/runtime/launcher-summary.json"
    cleanup_path = bundle / "raw/runtime/cleanup.json"
    runtime = _read_json(runtime_path, issues)
    cleanup = _read_json(cleanup_path, issues)
    if not isinstance(runtime, Mapping) or not isinstance(cleanup, Mapping):
        return issues
    if runtime.get("cleanup") != cleanup:
        _issue(
            issues,
            "OPERATOR_RUNTIME_MISMATCH",
            runtime_path,
            "runtime cleanup copies disagree",
        )
    resources = cleanup.get("resources")
    if cleanup.get("valid") is not True or not isinstance(resources, Mapping):
        _issue(issues, "OPERATOR_CLEANUP_INVALID", cleanup_path, "cleanup is invalid")
    elif any(
        resources.get(name) != []
        for name in ("containers", "networks", "volumes", "owned_build_images")
    ):
        _issue(
            issues,
            "OPERATOR_CLEANUP_RESIDUE",
            cleanup_path,
            "cleanup left owned resources",
        )
    report = _read_json(bundle / "playwright-results.json", issues)
    if (
        not isinstance(report, Mapping)
        or report.get("schema_version") != "playwright-operator-results.v1"
    ):
        _issue(
            issues,
            "OPERATOR_REPORT_INVALID",
            "playwright-results.json",
            "portable Playwright report is invalid",
        )
    if source_root is None:
        _issue(
            issues,
            "OPERATOR_SOURCE_ROOT_REQUIRED",
            "manifest.json",
            "operator source verification requires source_root",
        )
    else:
        source = capture_source_provenance(source_root)
        expected_source = manifest.get("source", {})
        if not isinstance(
            expected_source, Mapping
        ) or source.commit != expected_source.get("commit"):
            _issue(
                issues,
                "OPERATOR_SOURCE_COMMIT_MISMATCH",
                "manifest.json",
                "source HEAD differs from capture commit",
            )
        if source.git_dirty:
            _issue(
                issues,
                "OPERATOR_SOURCE_DIRTY",
                "manifest.json",
                "operator source paths are dirty",
            )
        if (
            not isinstance(expected_source, Mapping)
            or source.source_sha256 != expected_source.get("source_sha256")
            or list(source.paths) != expected_source.get("paths")
        ):
            _issue(
                issues,
                "OPERATOR_SOURCE_SNAPSHOT_MISMATCH",
                "manifest.json",
                "relevant source differs from capture source",
            )
    return issues


def _lab_issues(
    bundle: Path, manifest: Mapping[str, object], source_root: Path | None
) -> list[ArtifactIssue]:
    if source_root is None:
        return [
            ArtifactIssue(
                "LAB_SOURCE_ROOT_REQUIRED",
                "manifest.json",
                "lab source verification requires source_root",
            )
        ]
    return [
        ArtifactIssue(issue.code, issue.relative_path, issue.message)
        for issue in lab_semantic_issues(bundle, manifest, source_root)
    ]


def check_bundle(
    bundle: Path, *, expected_kind: str | None = None, source_root: Path | None = None
) -> CheckReport:
    bundle = bundle.resolve()
    issues: list[ArtifactIssue] = []
    manifest_path = bundle / "manifest.json"
    manifest = _read_json(manifest_path, issues)
    if not isinstance(manifest, Mapping):
        return _report(issues)
    schema_path = Path(__file__).parents[2] / "schemas/research-manifest.v1.json"
    try:
        Draft202012Validator(json.loads(schema_path.read_text())).validate(manifest)
    except (OSError, json.JSONDecodeError, ValidationError) as error:
        _issue(issues, "MANIFEST_SCHEMA_INVALID", manifest_path, str(error))
    if manifest.get("manifest_sha256") != manifest_sha256(manifest):
        _issue(
            issues,
            "MANIFEST_HASH_MISMATCH",
            manifest_path,
            "manifest digest does not match canonical contents",
        )
    kind = manifest.get("evidence_kind")
    if expected_kind is not None and kind != expected_kind:
        _issue(
            issues,
            "ARTIFACT_KIND_MISMATCH",
            manifest_path,
            "bundle kind differs from request",
        )
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping):
        _issue(
            issues, "ARTIFACTS_INVALID", manifest_path, "artifact digest map is invalid"
        )
    else:
        actual = {
            path.relative_to(bundle).as_posix(): file_sha256(path)
            for path in bundle.rglob("*")
            if path.is_file() and path.name != "manifest.json"
        }
        if actual != dict(artifacts):
            _issue(
                issues,
                "ARTIFACT_HASH_MISMATCH",
                manifest_path,
                "artifact digest map does not match bundle",
            )
    base_valid = not issues
    if manifest.get("status") != "PASS":
        _issue(
            issues,
            "EVIDENCE_STATUS_INVALID",
            manifest_path,
            "bundle is explicitly invalid",
        )
    if kind == "operator":
        issues.extend(
            _operator_issues(
                bundle, manifest, source_root.resolve() if source_root else None
            )
        )
    if kind == "lab" and base_valid:
        issues.extend(
            _lab_issues(
                bundle, manifest, source_root.resolve() if source_root else None
            )
        )
    return _report(issues)


def _campaign_json(
    path: Path,
    issues: list[ArtifactIssue],
    *,
    code: str,
) -> Mapping[str, object] | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        _issue(issues, code, path, "required canonical JSON is missing or invalid")
        return None
    if not isinstance(value, Mapping):
        _issue(issues, code, path, "required canonical JSON must be an object")
        return None
    return value


def _campaign_jsonl(
    path: Path,
    issues: list[ArtifactIssue],
) -> tuple[Mapping[str, object], ...] | None:
    try:
        lines = path.read_text().splitlines()
        values = tuple(json.loads(line) for line in lines)
    except (OSError, json.JSONDecodeError):
        _issue(
            issues,
            "CAMPAIGN_SCHEDULE_INVALID",
            path,
            "schedule JSONL is missing or invalid",
        )
        return None
    if not values or any(not isinstance(value, Mapping) for value in values):
        _issue(
            issues,
            "CAMPAIGN_SCHEDULE_INVALID",
            path,
            "schedule must contain object rows",
        )
        return None
    return tuple(value for value in values if isinstance(value, Mapping))


def _cell_key(value: object) -> tuple[str, str, str, str, int] | None:
    if not isinstance(value, Mapping):
        return None
    fields = (
        value.get("workload"),
        value.get("mode"),
        value.get("variant"),
        value.get("ablation"),
        value.get("timeout_seconds"),
    )
    if (
        any(type(field) is not str or not field for field in fields[:4])
        or type(fields[4]) is not int
        or fields[4] <= 0
    ):
        return None
    return (
        str(fields[0]),
        str(fields[1]),
        str(fields[2]),
        str(fields[3]),
        int(fields[4]),
    )


def _trial_row(
    bundle: Path,
    issues: list[ArtifactIssue],
) -> Mapping[str, object] | None:
    path = bundle / "trials.jsonl"
    try:
        lines = path.read_text().splitlines()
        values = tuple(json.loads(line) for line in lines)
    except (OSError, json.JSONDecodeError):
        _issue(
            issues,
            "CAMPAIGN_TRIAL_INVALID",
            path,
            "trial JSONL is missing or invalid",
        )
        return None
    if len(values) != 1 or not isinstance(values[0], Mapping):
        _issue(
            issues,
            "CAMPAIGN_TRIAL_INVALID",
            path,
            "each repetition must contain exactly one trial row",
        )
        return None
    trial = values[0]
    schema_path = Path(__file__).parents[2] / "schemas/research-trial.v1.json"
    try:
        schema = json.loads(schema_path.read_text())
        schema_errors = tuple(Draft202012Validator(schema).iter_errors(trial))
    except (OSError, json.JSONDecodeError, ValueError) as error:
        _issue(issues, "CAMPAIGN_TRIAL_SCHEMA_INVALID", schema_path, str(error))
        return trial
    for validation_error in schema_errors:
        _issue(
            issues,
            "CAMPAIGN_TRIAL_SCHEMA_INVALID",
            path,
            validation_error.message,
        )
    return trial


def _workload_evidence_error(
    workload: str,
    evidence: object,
    counts: Mapping[str, object],
) -> str | None:
    if workload not in {"W5", "W6a", "W6b", "W6c"}:
        return None
    if not isinstance(evidence, Mapping):
        return "workload evidence must be an object"
    if workload == "W5":
        subtrials = evidence.get("crash_subtrials")
        if not isinstance(subtrials, (list, tuple)) or len(subtrials) != 6:
            return "W5 must retain all six crash subtrials"
        fields = (
            "accepted",
            "delivered",
            "executions",
            "side_effects",
            "logical_terminals",
            "distinct_terminal_ids",
            "publication_attempts",
            "wire_deliveries",
            "poison",
        )
        if any(not isinstance(subtrial, Mapping) for subtrial in subtrials):
            return "W5 crash subtrials must be objects"
        subtrial_rows = [
            subtrial for subtrial in subtrials if isinstance(subtrial, Mapping)
        ]
        for field in fields:
            values = [subtrial.get(field) for subtrial in subtrial_rows]
            if any(type(value) is not int or value < 0 for value in values):
                return f"W5 crash subtrial {field} values are invalid"
            int_values = [value for value in values if type(value) is int]
            if counts.get(field) != sum(int_values):
                return f"W5 crash subtrial {field} total disagrees with observation"
        return None
    if workload == "W6a":
        retry = evidence.get("wire_retry")
        if not isinstance(retry, Mapping):
            return "W6a must retain wire retry evidence"
        envelope_ids = retry.get("envelope_ids")
        accepted = retry.get("accepted")
        sequences = retry.get("stream_sequences")
        duplicates = retry.get("duplicate_flags")
        if (
            not isinstance(envelope_ids, (list, tuple))
            or not isinstance(accepted, (list, tuple))
            or not isinstance(sequences, (list, tuple))
            or not isinstance(duplicates, (list, tuple))
            or any(
                len(value) != 2
                for value in (envelope_ids, accepted, sequences, duplicates)
            )
        ):
            return "W6a retry evidence must contain two publication receipts"
        if (
            not all(type(value) is str and value for value in envelope_ids)
            or envelope_ids[0] != envelope_ids[1]
            or accepted != [True, True]
            or duplicates != [False, True]
            or any(type(value) is not int or value < 1 for value in sequences)
            or sequences[0] != sequences[1]
        ):
            return "W6a retry receipts do not prove a duplicate acknowledgement"
        return None
    if workload == "W6b":
        retry = evidence.get("semantic_retry")
        if not isinstance(retry, Mapping):
            return "W6b must retain semantic retry evidence"
        first = retry.get("first_envelope_id")
        second = retry.get("second_envelope_id")
        task_id = retry.get("task_id")
        window = retry.get("retry_window")
        if (
            type(first) is not str
            or not first
            or type(second) is not str
            or not second
            or first == second
            or type(task_id) is not str
            or not task_id
            or not isinstance(window, Mapping)
        ):
            return "W6b retry identities are invalid"
        broker = window.get("broker_duplicate_window_seconds")
        elapsed = window.get("retry_elapsed_seconds")
        retention = window.get("ledger_retention_seconds")
        if (
            any(
                type(value) is not int or value < 0
                for value in (broker, elapsed, retention)
            )
            or elapsed <= broker  # type: ignore[operator]
            or elapsed >= retention  # type: ignore[operator]
            or counts.get("accepted") != 2
            or counts.get("publication_attempts") != 2
        ):
            return "W6b retry window or aggregate publication counts are invalid"
        return None
    if workload == "W6c":
        collision = evidence.get("collision")
        if not isinstance(collision, Mapping):
            return "W6c must retain collision evidence"
        if (
            collision.get("rejections") != 2
            or collision.get("executions") != 0
            or collision.get("cached_output_exposure") != 0
            or counts.get("poison") != collision.get("rejections")
            or counts.get("executions") != collision.get("executions")
        ):
            return "W6c collision evidence disagrees with observation"
    return None


def _observation_issues(
    observation: Mapping[str, object],
    cell: tuple[str, str, str, str, int] | None,
    *,
    require_publication: bool,
    path: Path,
) -> list[ArtifactIssue]:
    issues: list[ArtifactIssue] = []
    invalid_counts = tuple(
        name
        for name in _OBSERVATION_COUNTS
        if name not in observation
        or (
            observation[name] is not None
            and (type(observation[name]) is not int or observation[name] < 0)  # type: ignore[operator]
        )
    )
    if invalid_counts:
        _issue(
            issues,
            "CAMPAIGN_TRIAL_SCHEMA_INVALID",
            path,
            "observation count fields must be nonnegative integers or null: "
            + ", ".join(invalid_counts),
        )
        return issues
    if not require_publication or cell is None:
        return issues

    workload = cell[0]
    outcome = observation.get("outcome")
    inferred_outcome = classify_outcome(MatrixCell(*cell), observation)
    if outcome != inferred_outcome:
        _issue(
            issues,
            "CAMPAIGN_OUTCOME_MISMATCH",
            path,
            f"declared outcome {outcome!r} contradicts observed {inferred_outcome!r}",
        )
    counts = {name: observation[name] for name in _OBSERVATION_COUNTS}
    evidence_error = _workload_evidence_error(
        workload,
        observation.get("workload_evidence"),
        counts,
    )
    if evidence_error is not None:
        _issue(issues, "CAMPAIGN_WORKLOAD_EVIDENCE_INVALID", path, evidence_error)
    violations: list[str] = []
    if outcome == "completed":
        initiated = counts["initiated"]
        logical = counts["logical_terminals"]
        distinct = counts["distinct_terminal_ids"]
        delivered = counts["delivered"]
        executions = counts["executions"]
        wire = counts["wire_deliveries"]
        publications = counts["publication_attempts"]
        if type(initiated) is not int or initiated < 1:
            violations.append("completed repetition must initiate work")
        if type(counts["accepted"]) is not int or counts["accepted"] < 1:
            violations.append("completed repetition must be accepted")
        if workload == "W6c":
            if any(
                counts[name] != expected
                for name, expected in (
                    ("delivered", 0),
                    ("executions", 0),
                    ("logical_terminals", 0),
                    ("distinct_terminal_ids", 0),
                )
            ):
                violations.append("W6c must reject collisions without execution")
        elif type(initiated) is int:
            if logical != initiated or distinct != logical:
                violations.append("logical terminal identity is not exactly once")
            if delivered != logical or type(wire) is not int or wire < logical:  # type: ignore[operator]
                violations.append("terminal delivery counts contradict completion")
            if workload == "W8":
                if type(executions) is not int or executions < 1:
                    violations.append("W8 must record actuator execution")
            elif workload == "W2":
                if executions != 2:
                    violations.append("W2 must execute one parent and one child task")
            elif executions != initiated:
                violations.append("worker execution count is not exactly once")
        if type(publications) is not int or (
            type(initiated) is int and publications < initiated
        ):
            violations.append("terminal publication count is incomplete")
        if workload != "W6c" and observation.get("timed_out") is True:
            violations.append("completed repetition cannot be timed out")
        if workload == "W3":
            generated = counts["progress_generated"]
            delivered_progress = (
                counts["progress_live_delivered"],
                counts["progress_replay_delivered"],
                counts["progress_missing"],
            )
            if (
                generated != 20
                or any(type(value) is not int for value in delivered_progress)
                or sum(value for value in delivered_progress if type(value) is int)
                != 20
            ):
                violations.append("W3 progress accounting is inconsistent")
        if workload == "W8" and (
            type(counts["side_effects"]) is not int
            or counts["side_effects"] < 1
            or type(counts["prepared_outcomes"]) is not int
            or counts["prepared_outcomes"] < 1
        ):
            violations.append("W8 side-effect accounting is incomplete")
    if violations:
        _issue(
            issues,
            "CAMPAIGN_TRIAL_INVARIANT_FAILED",
            path,
            "; ".join(violations),
        )
    return issues


def _validate_config(
    config_path: Path,
    config: Mapping[str, object] | None,
    issues: list[ArtifactIssue],
) -> None:
    if config is None:
        return
    schema_path = Path(__file__).parent / "configs/schema/campaign.schema.json"
    try:
        schema = json.loads(schema_path.read_text())
        errors = tuple(Draft202012Validator(schema).iter_errors(config))
    except (OSError, json.JSONDecodeError, ValueError) as error:
        _issue(issues, "CAMPAIGN_CONFIG_SCHEMA_INVALID", schema_path, str(error))
        return
    for validation_error in errors:
        _issue(
            issues,
            "CAMPAIGN_CONFIG_INVALID",
            config_path,
            validation_error.message,
        )


def _schedule_issues(
    schedule: Sequence[Mapping[str, object]],
    config: Mapping[str, object],
    *,
    require_publication: bool,
    path: Path,
) -> list[ArtifactIssue]:
    issues: list[ArtifactIssue] = []
    warmup = config.get("warmup_blocks")
    measured = config.get("measured_blocks")
    if type(warmup) is not int or type(measured) is not int:
        return issues
    if require_publication and (warmup != 5 or measured != 30):
        _issue(
            issues,
            "CAMPAIGN_BLOCK_CONTRACT_MISMATCH",
            path,
            "publication campaigns require exactly 5 warmup and 30 measured blocks",
        )
    total_blocks = warmup + measured
    block_cells: dict[int, list[tuple[str, str, str, str, int]]] = {}
    run_ids: list[str] = []
    normalized_rows: list[tuple[str, int, bool, tuple[str, str, str, str, int]]] = []
    timeouts = config.get("workload_timeouts")
    timeout_mismatch = False
    for row in schedule:
        run_id = row.get("run_id")
        block = row.get("block")
        measured_row = row.get("measured")
        cell = _cell_key(row.get("cell"))
        if (
            type(run_id) is not str
            or not run_id
            or type(block) is not int
            or block < 0
            or type(measured_row) is not bool
            or cell is None
        ):
            _issue(
                issues,
                "CAMPAIGN_SCHEDULE_ROW_INVALID",
                path,
                "schedule row has an invalid run, block, measured flag, or cell",
            )
            continue
        run_ids.append(run_id)
        block_cells.setdefault(block, []).append(cell)
        normalized_rows.append((run_id, block, measured_row, cell))
        if not isinstance(timeouts, Mapping) or timeouts.get(cell[0]) != cell[4]:
            timeout_mismatch = True
        if block >= total_blocks or measured_row != (block >= warmup):
            _issue(
                issues,
                "CAMPAIGN_BLOCK_MISMATCH",
                path,
                f"run {run_id} disagrees with declared warmup/measured blocks",
            )
    if timeout_mismatch:
        _issue(
            issues,
            "CAMPAIGN_TIMEOUT_MISMATCH",
            path,
            "scheduled workload timeout differs from campaign config",
        )
    if len(run_ids) != len(set(run_ids)):
        _issue(issues, "CAMPAIGN_RUN_ID_DUPLICATE", path, "run IDs must be unique")
    if set(block_cells) != set(range(total_blocks)):
        _issue(
            issues,
            "CAMPAIGN_BLOCKS_INCOMPLETE",
            path,
            "declared blocks are missing or extra",
        )
    if block_cells:
        expected = Counter(next(iter(block_cells.values())))
        if any(Counter(cells) != expected for cells in block_cells.values()):
            _issue(
                issues,
                "CAMPAIGN_CELLS_INCOMPLETE",
                path,
                "each block must contain the identical cell multiset",
            )
        if any(count != 1 for count in expected.values()):
            _issue(
                issues,
                "CAMPAIGN_CELL_DUPLICATE",
                path,
                "each block must contain each cell exactly once",
            )
        if require_publication:
            required_cells = required_matrix_cells()
            required = Counter(
                (
                    cell.workload,
                    cell.mode,
                    cell.variant,
                    cell.ablation,
                    cell.timeout_seconds,
                )
                for cell in required_cells
            )
            if expected != required:
                _issue(
                    issues,
                    "CAMPAIGN_CELL_SET_MISMATCH",
                    path,
                    "publication schedule differs from the fixed matrix",
                )
            seed = config.get("seed")
            if type(seed) is int:
                expected_rows: list[
                    tuple[str, int, bool, tuple[str, str, str, str, int]]
                ] = []
                for block in range(total_blocks):
                    ordered = list(required_cells)
                    random.Random(seed + block).shuffle(ordered)
                    for required_cell in ordered:
                        expected_rows.append(
                            (
                                f"ec-{seed}-{len(expected_rows):05d}",
                                block,
                                block >= warmup,
                                (
                                    required_cell.workload,
                                    required_cell.mode,
                                    required_cell.variant,
                                    required_cell.ablation,
                                    required_cell.timeout_seconds,
                                ),
                            )
                        )
                if normalized_rows != expected_rows:
                    _issue(
                        issues,
                        "CAMPAIGN_SCHEDULE_ORDER_MISMATCH",
                        path,
                        "schedule differs from the seeded block order and run IDs",
                    )
    return issues


def check_campaign(
    path: Path,
    *,
    require_publication: bool = False,
) -> CheckReport:
    """Validate a complete immutable benchmark campaign without writing output."""
    campaign = path.resolve()
    issues: list[ArtifactIssue] = []
    metadata_path = campaign / "campaign.json"
    config_path = campaign / "campaign-config.json"
    schedule_path = campaign / "schedule.jsonl"
    metadata = _campaign_json(
        metadata_path,
        issues,
        code="CAMPAIGN_METADATA_INVALID",
    )
    config = _campaign_json(
        config_path,
        issues,
        code="CAMPAIGN_CONFIG_INVALID",
    )
    schedule = _campaign_jsonl(schedule_path, issues)
    if metadata is not None and metadata.get("profile") == "paper":
        _validate_config(config_path, config, issues)
    if metadata is None or config is None or schedule is None:
        return _report(issues)
    if metadata.get("schema_version") != "research-campaign.v1":
        _issue(
            issues,
            "CAMPAIGN_METADATA_INVALID",
            metadata_path,
            "unsupported campaign metadata schema",
        )
    if metadata.get("campaign_path") != str(campaign):
        _issue(
            issues,
            "CAMPAIGN_PATH_MISMATCH",
            metadata_path,
            "campaign path differs from the captured path",
        )
    if metadata.get("campaign_id") != config.get("campaign_id"):
        _issue(
            issues,
            "CAMPAIGN_ID_MISMATCH",
            metadata_path,
            "campaign metadata and configuration disagree",
        )
    if metadata.get("config_sha256") != file_sha256(config_path):
        _issue(
            issues,
            "CAMPAIGN_CONFIG_HASH_MISMATCH",
            config_path,
            "campaign configuration changed after capture",
        )
    if metadata.get("schedule_sha256") != file_sha256(schedule_path):
        _issue(
            issues,
            "CAMPAIGN_SCHEDULE_HASH_MISMATCH",
            schedule_path,
            "campaign schedule changed after capture",
        )
    if require_publication and metadata.get("profile") != "paper":
        _issue(
            issues,
            "CAMPAIGN_PROFILE_INELIGIBLE",
            metadata_path,
            "publication analysis accepts only the paper profile",
        )
    if metadata.get("profile") == "paper":
        issues.extend(
            _schedule_issues(
                schedule,
                config,
                require_publication=require_publication,
                path=schedule_path,
            )
        )
    declared_bundle_paths = metadata.get("bundle_paths")
    expected_bundle_paths = [
        str((campaign / "bundles" / str(row.get("run_id"))).resolve())
        for row in schedule
    ]
    if declared_bundle_paths != expected_bundle_paths:
        _issue(
            issues,
            "CAMPAIGN_BUNDLE_INDEX_MISMATCH",
            metadata_path,
            "bundle index differs from the immutable schedule",
        )
    campaign_hash = file_sha256(metadata_path)
    expected_source = metadata.get("source")
    if require_publication and (
        not isinstance(expected_source, Mapping)
        or expected_source.get("git_dirty") is not False
        or not isinstance(expected_source.get("commit"), str)
        or re.fullmatch(r"[0-9a-f]{40}", str(expected_source.get("commit"))) is None
        or not isinstance(expected_source.get("source_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(expected_source.get("source_sha256")))
        is None
        or not isinstance(expected_source.get("paths"), list)
        or not expected_source.get("paths")
        or any(
            type(item) is not str or not item
            for item in expected_source.get("paths", [])
        )
    ):
        _issue(
            issues,
            "CAMPAIGN_SOURCE_INELIGIBLE",
            metadata_path,
            "publication source must be a clean committed source snapshot",
        )
    config_components = config.get("resource_components")
    campaign_images: dict[object, object] | None = None
    expected_runs = {str(row.get("run_id")) for row in schedule}
    bundles_root = campaign / "bundles"
    actual_runs = (
        {item.name for item in bundles_root.iterdir() if item.is_dir()}
        if bundles_root.is_dir()
        else set()
    )
    for extra in sorted(actual_runs - expected_runs):
        _issue(
            issues,
            "CAMPAIGN_BUNDLE_EXTRA",
            bundles_root / extra,
            "bundle is not present in the schedule",
        )
    for row in schedule:
        run_id = row.get("run_id")
        if type(run_id) is not str or not run_id:
            continue
        bundle = bundles_root / run_id
        if not bundle.is_dir():
            _issue(
                issues,
                "CAMPAIGN_BUNDLE_MISSING",
                bundle,
                "scheduled bundle is absent",
            )
            continue
        bundle_report = check_bundle(bundle, expected_kind="benchmark")
        for issue in bundle_report.issues:
            _issue(
                issues,
                f"CAMPAIGN_BUNDLE_{issue.code}",
                bundle / issue.path,
                issue.message,
            )
        manifest = _campaign_json(
            bundle / "manifest.json",
            issues,
            code="CAMPAIGN_BUNDLE_MANIFEST_INVALID",
        )
        trial = _trial_row(bundle, issues)
        if manifest is None or trial is None:
            continue
        if manifest.get("run_id") != run_id or trial.get("run_id") != run_id:
            _issue(
                issues,
                "CAMPAIGN_RUN_ID_MISMATCH",
                bundle,
                "schedule, manifest, and trial run IDs differ",
            )
        if manifest.get("campaign_id") != metadata.get("campaign_id") or manifest.get(
            "profile"
        ) != metadata.get("profile"):
            _issue(
                issues,
                "CAMPAIGN_BUNDLE_METADATA_MISMATCH",
                bundle / "manifest.json",
                "bundle campaign identity differs from metadata",
            )
        if manifest.get("source") != expected_source:
            _issue(
                issues,
                "CAMPAIGN_SOURCE_MISMATCH",
                bundle / "manifest.json",
                "bundle source differs from campaign source",
            )
        if require_publication:
            host = manifest.get("host")
            if (
                not isinstance(host, Mapping)
                or host.get("system") != "Linux"
                or host.get("architecture") != "x86_64"
                or host.get("os_id") != "ubuntu"
                or host.get("os_version") != "24.04"
            ):
                _issue(
                    issues,
                    "CAMPAIGN_HOST_INELIGIBLE",
                    bundle / "manifest.json",
                    "publication host must be Ubuntu 24.04 x86_64",
                )
        images = manifest.get("images")
        if require_publication:
            if (
                not isinstance(images, Mapping)
                or not images
                or any(
                    type(value) is not str
                    or re.fullmatch(
                        r"(?:[a-z0-9._/-]+@)?sha256:[0-9a-f]{64}",
                        value,
                    )
                    is None
                    for value in images.values()
                )
            ):
                _issue(
                    issues,
                    "CAMPAIGN_IMAGES_INELIGIBLE",
                    bundle / "manifest.json",
                    "publication images must use immutable SHA-256 identities",
                )
            elif campaign_images is None:
                campaign_images = dict(images)
            elif dict(images) != campaign_images:
                _issue(
                    issues,
                    "CAMPAIGN_IMAGES_MISMATCH",
                    bundle / "manifest.json",
                    "publication image identities differ between repetitions",
                )
        contract = manifest.get("campaign_contract")
        expected_contract = {
            "block": row.get("block"),
            "measured": row.get("measured"),
            "config_sha256": metadata.get("config_sha256"),
            "schedule_sha256": metadata.get("schedule_sha256"),
            "campaign_sha256": campaign_hash,
        }
        if contract != expected_contract:
            _issue(
                issues,
                "CAMPAIGN_CONTRACT_MISMATCH",
                bundle / "manifest.json",
                "bundle does not bind the immutable campaign inputs",
            )
        if (
            trial.get("block") != row.get("block")
            or trial.get("measured") != row.get("measured")
            or _cell_key(trial.get("cell")) != _cell_key(row.get("cell"))
            or _cell_key(manifest.get("workload_config")) != _cell_key(row.get("cell"))
        ):
            _issue(
                issues,
                "CAMPAIGN_TRIAL_SCHEDULE_MISMATCH",
                bundle / "trials.jsonl",
                "trial or manifest differs from its scheduled cell",
            )
        observation = trial.get("observation")
        if not isinstance(observation, Mapping):
            _issue(
                issues,
                "CAMPAIGN_TRIAL_INVALID",
                bundle / "trials.jsonl",
                "trial observation must be an object",
            )
            continue
        expected_timing = {
            "started_monotonic_ns": observation.get("started_monotonic_ns"),
            "ended_monotonic_ns": observation.get("ended_monotonic_ns"),
        }
        if trial.get("timing") != expected_timing:
            _issue(
                issues,
                "CAMPAIGN_TRIAL_TIMING_MISMATCH",
                bundle / "trials.jsonl",
                "trial timing differs from the observed monotonic interval",
            )
        references = {
            "events_artifact": "events.jsonl",
            "resource_artifact": "resources.json",
        }
        if any(
            trial.get(field) != expected or not (bundle / expected).is_file()
            for field, expected in references.items()
        ):
            _issue(
                issues,
                "CAMPAIGN_TRIAL_REFERENCE_INVALID",
                bundle / "trials.jsonl",
                "trial artifact references must bind existing bundle-local files",
            )
        invariant_results = trial.get("invariant_results")
        if (
            not isinstance(invariant_results, Mapping)
            or invariant_results.get("outcome_consistent") is not True
        ):
            _issue(
                issues,
                "CAMPAIGN_TRIAL_INVARIANT_FAILED",
                bundle / "trials.jsonl",
                "trial invariant results do not attest outcome consistency",
            )
        issues.extend(
            _observation_issues(
                observation,
                _cell_key(row.get("cell")),
                require_publication=require_publication,
                path=bundle / "trials.jsonl",
            )
        )
        outcome = observation.get("outcome")
        if outcome == "harness-invalid":
            _issue(
                issues,
                "CAMPAIGN_HARNESS_INVALID",
                bundle / "trials.jsonl",
                "harness-invalid repetitions invalidate the campaign",
            )
        elif outcome not in {"completed", "failed", "timeout"}:
            _issue(
                issues,
                "CAMPAIGN_OUTCOME_INVALID",
                bundle / "trials.jsonl",
                "trial outcome is absent or invalid",
            )
        if require_publication:
            cell = _cell_key(row.get("cell"))
            workload = cell[0] if cell is not None else None
            evidence = observation.get("workload_evidence")
            expected_evidence = (
                {
                    "W5": "crash_subtrials",
                    "W6a": "wire_retry",
                    "W6b": "semantic_retry",
                    "W6c": "collision",
                }.get(workload)
                if workload is not None
                else None
            )
            if expected_evidence is not None and (
                not isinstance(evidence, Mapping)
                or not isinstance(
                    evidence.get(expected_evidence), (Mapping, list, tuple)
                )
            ):
                _issue(
                    issues,
                    "CAMPAIGN_WORKLOAD_EVIDENCE_INVALID",
                    bundle / "trials.jsonl",
                    "trial is missing its workload-specific raw evidence",
                )
        metric_contract = manifest.get("metric_contract")
        resources = observation.get("resources")
        actual_components = (
            resources.get("components") if isinstance(resources, Mapping) else None
        )
        if require_publication:
            if (
                not isinstance(metric_contract, Mapping)
                or metric_contract.get("components") != config_components
                or actual_components != config_components
            ):
                _issue(
                    issues,
                    "CAMPAIGN_COMPONENTS_MISMATCH",
                    bundle,
                    "resource component membership differs from campaign config",
                )
            if (
                not isinstance(metric_contract, Mapping)
                or metric_contract.get("status") != "collected"
                or metric_contract.get("sampler_interval_ms")
                != config.get("sampler_interval_ms")
                or metric_contract.get("idle_baseline_seconds")
                != config.get("idle_baseline_seconds")
            ):
                _issue(
                    issues,
                    "CAMPAIGN_METRICS_INELIGIBLE",
                    bundle / "manifest.json",
                    "publication metrics contract is incomplete",
                )
            if (
                not isinstance(resources, Mapping)
                or resources.get("cost_claims_valid") is not True
                or any(
                    not isinstance(value := resources.get(metric), (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(value)
                    or value < 0
                    for metric in _RESOURCE_METRICS
                )
            ):
                _issue(
                    issues,
                    "CAMPAIGN_COST_INVALID",
                    bundle / "trials.jsonl",
                    "resource sampler invalidated cost claims",
                )
            latency = observation.get("latency_ns")
            started = observation.get("started_monotonic_ns")
            ended = observation.get("ended_monotonic_ns")
            expected_latency = (
                ended - started
                if type(started) is int
                and type(ended) is int
                and started >= 0
                and ended >= started
                else None
            )
            latency_valid = (
                outcome == "completed"
                and expected_latency is not None
                and type(latency) is int
                and latency == expected_latency
            ) or (outcome != "completed" and latency is None)
            if not latency_valid:
                _issue(
                    issues,
                    "CAMPAIGN_LATENCY_INVALID",
                    bundle / "trials.jsonl",
                    "trial latency disagrees with its in-container monotonic interval",
                )
    return _report(issues)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    targets = parser.add_mutually_exclusive_group(required=True)
    targets.add_argument("--bundle", type=Path)
    targets.add_argument("--campaign", type=Path)
    parser.add_argument("--require-kind", choices=("benchmark", "operator", "lab"))
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--publication", action="store_true")
    arguments = parser.parse_args(argv)
    report = (
        check_campaign(
            arguments.campaign,
            require_publication=arguments.publication,
        )
        if arguments.campaign is not None
        else check_bundle(
            arguments.bundle,
            expected_kind=arguments.require_kind,
            source_root=arguments.source_root,
        )
    )
    for issue in report.issues:
        print(f"{issue.code}: {issue.path}: {issue.message}")
    print(f"artifact: {'PASS' if report.valid else 'INVALID'}")
    return 0 if report.valid else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ArtifactInvalid",
    "ArtifactIssue",
    "CheckReport",
    "check_bundle",
    "check_campaign",
]
