"""Pure semantic validation for finalized or about-to-be-finalized lab bundles."""

from __future__ import annotations

import json
import ipaddress
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from scripts.research.evidence import file_sha256
from scripts.research.lab_config import LabConfigError
from scripts.research.lab_runtime import LAB_SOURCE_PATHS, capture_clean_source_provenance


_WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_OBSERVATION_KEYS = (
    "reservation_events",
    "node_reports",
    "controller_commands",
    "cleanup",
)


@dataclass(frozen=True)
class LabContractIssue:
    code: str
    relative_path: str
    message: str


def _issue(code: str, path: str, message: str) -> LabContractIssue:
    return LabContractIssue(code, path, message)


def _portable_issues(value: object, path: str = "manifest.json") -> list[LabContractIssue]:
    issues: list[LabContractIssue] = []
    if isinstance(value, Mapping):
        for name, item in value.items():
            issues.extend(_portable_issues(item, f"{path}.{name}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            issues.extend(_portable_issues(item, f"{path}[{index}]"))
    elif isinstance(value, str):
        if value.startswith("/") or _WINDOWS_PATH.match(value):
            issues.append(
                _issue("LAB_NONPORTABLE_PATH", path, "absolute transient paths are forbidden")
            )
    return issues


def _is_ipv4(value: object) -> bool:
    try:
        return isinstance(value, str) and isinstance(
            ipaddress.ip_address(value), ipaddress.IPv4Address
        )
    except ValueError:
        return False


def _safe_relative_path(value: object) -> Path | None:
    if not isinstance(value, str) or not value or "\\" in value:
        return None
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        return None
    return path


def _safe_regular_file(bundle: Path, relative: Path) -> bool:
    current = bundle
    try:
        for part in relative.parts[:-1]:
            current = current / part
            metadata = current.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                return False
        path = bundle / relative
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode)


def _observation_refs(
    bundle: Path,
    manifest: Mapping[str, object],
) -> tuple[list[LabContractIssue], dict[str, object]]:
    issues: list[LabContractIssue] = []
    loaded: dict[str, object] = {}
    observations = manifest.get("observations")
    if not isinstance(observations, Mapping):
        return (
            [_issue("LAB_OBSERVATIONS_INVALID", "manifest.json", "observation map is required")],
            loaded,
        )
    refs: list[tuple[str, object]] = [
        (name, observations.get(name)) for name in _OBSERVATION_KEYS
    ]
    playwright = observations.get("playwright")
    if not isinstance(playwright, list):
        issues.append(
            _issue("LAB_OBSERVATIONS_INVALID", "manifest.json", "Playwright observations are invalid")
        )
    else:
        refs.extend((f"playwright[{index}]", item) for index, item in enumerate(playwright))
    artifacts = manifest.get("artifacts")
    finalized = isinstance(manifest.get("manifest_sha256"), str)
    for name, value in refs:
        if not isinstance(value, Mapping) or set(value) != {"path", "sha256"}:
            issues.append(
                _issue("LAB_OBSERVATIONS_INVALID", "manifest.json", f"{name} reference is invalid")
            )
            continue
        relative = _safe_relative_path(value.get("path"))
        expected_hash = value.get("sha256")
        if relative is None:
            issues.append(
                _issue("LAB_NONPORTABLE_PATH", "manifest.json", f"{name} path is not portable")
            )
            continue
        relative_text = relative.as_posix()
        if not _safe_regular_file(bundle, relative):
            issues.append(
                _issue("LAB_OBSERVATION_PATH_MISSING", relative_text, f"{name} evidence is unavailable")
            )
            continue
        actual_hash = file_sha256(bundle / relative)
        if not isinstance(expected_hash, str) or expected_hash != actual_hash:
            issues.append(
                _issue("LAB_OBSERVATION_HASH_MISMATCH", relative_text, f"{name} hash differs")
            )
        if finalized and (
            not isinstance(artifacts, Mapping)
            or artifacts.get(relative_text) != actual_hash
        ):
            issues.append(
                _issue(
                    "LAB_OBSERVATION_ARTIFACT_MISSING",
                    relative_text,
                    f"{name} is absent from the finalized artifact map",
                )
            )
        try:
            loaded[name] = json.loads((bundle / relative).read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            issues.append(
                _issue("LAB_OBSERVATION_JSON_INVALID", relative_text, f"{name} JSON is invalid")
            )
    return issues, loaded


def _source_issues(
    manifest: Mapping[str, object], source_root: Path
) -> list[LabContractIssue]:
    try:
        observed = capture_clean_source_provenance(source_root.resolve())
    except (OSError, subprocess.CalledProcessError, LabConfigError):
        return [_issue("LAB_SOURCE_DIRTY", "manifest.json", "lab source paths are not clean")]
    expected = manifest.get("source")
    if not isinstance(expected, Mapping):
        return [_issue("LAB_SOURCE_SNAPSHOT_MISMATCH", "manifest.json", "source facts are invalid")]
    issues: list[LabContractIssue] = []
    if observed.commit != expected.get("commit"):
        issues.append(
            _issue("LAB_SOURCE_COMMIT_MISMATCH", "manifest.json", "source HEAD differs")
        )
    if (
        observed.source_snapshot_sha256 != expected.get("source_sha256")
        or expected.get("paths") != list(LAB_SOURCE_PATHS)
        or expected.get("git_dirty") is not False
    ):
        issues.append(
            _issue(
                "LAB_SOURCE_SNAPSHOT_MISMATCH",
                "manifest.json",
                "scoped source snapshot differs",
            )
        )
    return issues


def _lifecycle_issues(
    manifest: Mapping[str, object], loaded: Mapping[str, object]
) -> list[LabContractIssue]:
    issues: list[LabContractIssue] = []
    nodes = manifest.get("nodes")
    evidence = loaded.get("controller_commands")
    commands = evidence.get("commands") if isinstance(evidence, Mapping) else None
    events = loaded.get("reservation_events")
    if not isinstance(nodes, list) or len(nodes) < 2:
        issues.append(_issue("LAB_LIFECYCLE_TOPOLOGY_INVALID", "manifest.json", "two nodes are required"))
    node_bindings = {
        (item.get("agent_id"), item.get("reservation_id"))
        for item in nodes or []
        if isinstance(item, Mapping)
    }
    successful = [
        item
        for item in commands or []
        if isinstance(item, Mapping)
        and item.get("status") == "completed"
        and isinstance(item.get("task_id"), str)
        and isinstance(item.get("accepted_at"), str)
        and isinstance(item.get("terminal_at"), str)
        and str(item["accepted_at"]) < str(item["terminal_at"])
        and isinstance(item.get("expected_output"), str)
        and str(item["expected_output"]).startswith("edgecitadel:")
        and item.get("wire_copies") in {1, 2}
        and (
            (item.get("wire_copies") == 1 and item.get("http_status") == 202)
            or (item.get("wire_copies") == 2 and item.get("http_status") is None)
        )
        and item.get("terminal_output") == item.get("expected_output")
        and item.get("terminal_count") == 1
        and item.get("conflicting_terminal") is False
        and (item.get("agent_id"), item.get("reservation_id")) in node_bindings
    ]
    if (
        not isinstance(commands, list)
        or len(successful) < 3
        or len({item.get("task_id") for item in successful}) < 3
        or not node_bindings.issubset(
            {(item.get("agent_id"), item.get("reservation_id")) for item in successful}
        )
        or not any(item.get("wire_copies") == 2 for item in successful)
    ):
        issues.append(_issue("LAB_LIFECYCLE_COMMANDS_INCOMPLETE", "raw/lab/controller-commands.json", "three tasks and duplicate wire evidence are required"))
    groups: dict[tuple[object, object], list[Mapping[str, object]]] = {}
    if isinstance(events, list):
        for item in events:
            if isinstance(item, Mapping):
                groups.setdefault((item.get("agent_id"), item.get("reservation_id")), []).append(item)
    history: list[Mapping[str, object]] | None = None
    history_binding: tuple[object, object] | None = None
    required = ("reserved", "retained", "resumed", "released")
    for rows in groups.values():
        by_event = {item.get("event"): item for item in rows}
        if all(name in by_event for name in required):
            ordered = [by_event[name] for name in required]
            sequences = [item.get("sequence") for item in ordered]
            times = [item.get("observed_at") for item in ordered]
            if all(type(value) is int for value in sequences) and sequences == sorted(sequences) and len(set(sequences)) == 4 and all(isinstance(value, str) for value in times) and times == sorted(times):
                history = ordered
                history_binding = (
                    ordered[0].get("agent_id"),
                    ordered[0].get("reservation_id"),
                )
                break
    if history is None:
        issues.append(_issue("LAB_RESERVATION_HISTORY_INCOMPLETE", "raw/lab/reservation-events.json", "reserved/retained/resumed/released history is required"))
        return issues
    queued = [
        item for item in successful
        if item.get("qualification_kind") == "queued-reconnect"
        and (item.get("agent_id"), item.get("reservation_id")) == history_binding
    ]
    if len(queued) != 1:
        issues.append(_issue("LAB_RECONNECT_ORDER_INVALID", "raw/lab/controller-commands.json", "one queued reconnect task is required"))
    else:
        retained_at = history[1].get("observed_at")
        resumed_at = history[2].get("observed_at")
        accepted_at = queued[0].get("accepted_at")
        terminal_at = queued[0].get("terminal_at")
        if not all(isinstance(value, str) for value in (retained_at, accepted_at, resumed_at, terminal_at)) or not (str(retained_at) < str(accepted_at) < str(resumed_at) < str(terminal_at)):
            issues.append(_issue("LAB_RECONNECT_ORDER_INVALID", "raw/lab/controller-commands.json", "disconnect < accepted < reconnect < terminal is required"))
    return issues


def _node_binding_issues(
    manifest: Mapping[str, object], loaded: Mapping[str, object]
) -> list[LabContractIssue]:
    nodes = manifest.get("nodes")
    evidence = loaded.get("controller_commands")
    launches = evidence.get("launches") if isinstance(evidence, Mapping) else None
    reports = loaded.get("node_reports")
    if not isinstance(nodes, list) or not isinstance(launches, list) or not isinstance(reports, list):
        return [_issue("LAB_NODE_BINDING_INVALID", "raw/lab/node-reports.json", "node binding evidence is invalid")]
    identity_names = ("agent_id", "qualified_agent_id", "reservation_id", "declared_host_id")
    retained_names = (
        *identity_names,
        "machine_id_sha256",
        "hostname",
        "os_release",
        "architecture",
        "launcher_source_commit",
        "source_snapshot_sha256",
        "preflight_valid",
        "lifecycle_state",
        "server_observed_peer_ip",
        "network_path",
    )
    if len(launches) != len(nodes) or len(reports) != len(nodes):
        return [_issue("LAB_NODE_BINDING_INVALID", "raw/lab/node-reports.json", "node evidence is not one-to-one")]
    source = manifest.get("source")
    images = manifest.get("images")
    controller = manifest.get("controller")
    for node in nodes:
        if not isinstance(node, Mapping):
            return [_issue("LAB_NODE_BINDING_INVALID", "manifest.json", "node facts are invalid")]
        launch_matches = [
            item for item in launches
            if isinstance(item, Mapping)
            and all(item.get(name) == node.get(name) for name in identity_names)
        ]
        report_matches = [
            item for item in reports
            if isinstance(item, Mapping)
            and all(item.get(name) == node.get(name) for name in identity_names)
        ]
        if (
            len(launch_matches) != 1
            or len(report_matches) != 1
            or any(report_matches[0].get(name) != node.get(name) for name in retained_names)
            or node.get("preflight_valid") is not True
            or not isinstance(source, Mapping)
            or node.get("launcher_source_commit") != source.get("commit")
            or node.get("source_snapshot_sha256") != source.get("source_sha256")
            or not isinstance(images, Mapping)
            or node.get("fixture_image_id") != images.get("fixture")
        ):
            return [_issue("LAB_NODE_BINDING_INVALID", "raw/lab/node-reports.json", "node report differs from launch ownership")]
        network = node.get("network_path")
        if (
            not isinstance(network, Mapping)
            or not isinstance(controller, Mapping)
            or not _is_ipv4(node.get("server_observed_peer_ip"))
            or not _is_ipv4(network.get("source_ip"))
            or not _is_ipv4(network.get("destination_ip"))
            or node.get("server_observed_peer_ip") != network.get("source_ip")
            or network.get("destination_ip") != controller.get("advertised_ip")
            or network.get("controller_dns_name") != controller.get("advertised_host")
        ):
            return [_issue("LAB_NODE_BINDING_INVALID", "raw/lab/node-reports.json", "node network facts are invalid")]
    return []


def _operator_smoke_issues(
    manifest: Mapping[str, object], loaded: Mapping[str, object]
) -> list[LabContractIssue]:
    evidence = loaded.get("controller_commands")
    commands = evidence.get("commands") if isinstance(evidence, Mapping) else None
    smoke = loaded.get("playwright[0]")
    if not isinstance(commands, list) or len(commands) != 1 or not isinstance(smoke, Mapping):
        return [_issue("LAB_OPERATOR_SMOKE_INVALID", "playwright-smoke.json", "one task and one smoke record are required")]
    command = commands[0]
    argv = smoke.get("argv")
    valid = (
        isinstance(command, Mapping)
        and command.get("status") == "completed"
        and isinstance(command.get("terminal_at"), str)
        and command.get("agent_id") == "shell-1"
        and isinstance(argv, list)
        and argv == ["npx", "--no-install", "playwright", "test", "--config", "playwright.config.js", "tests/operator-journey.spec.js"]
        and smoke.get("cwd") == "e2e"
        and smoke.get("returncode") == 0
        and smoke.get("assertion") == "1 passed"
        and smoke.get("task_id") == command.get("task_id")
        and smoke.get("context_id") == command.get("task_id")
        and smoke.get("hop_count") == 0
        and isinstance(smoke.get("nonce"), str)
        and smoke.get("output") == f"edgecitadel:{smoke.get('nonce')}"
        and smoke.get("output") == command.get("expected_output")
    )
    return [] if valid else [_issue("LAB_OPERATOR_SMOKE_INVALID", "playwright-smoke.json", "operator smoke correlation is invalid")]


def _operator_evidence_issues(
    bundle: Path, manifest: Mapping[str, object], loaded: Mapping[str, object]
) -> list[LabContractIssue]:
    report = loaded.get("playwright[0]")
    declared = manifest.get("operator_evidence")
    observations = manifest.get("observations")
    playwright = observations.get("playwright") if isinstance(observations, Mapping) else None
    if (
        not isinstance(report, Mapping)
        or not isinstance(declared, Mapping)
        or not isinstance(playwright, list)
        or len(playwright) != 1
        or declared.get("report") != playwright[0]
        or report.get("schema_version") != "playwright-operator-results.v1"
    ):
        return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", "playwright-results.json", "operator report is required")]
    projects = report.get("projects")
    if not isinstance(projects, Mapping) or set(projects) != {"desktop", "mobile"}:
        return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", "playwright-results.json", "desktop/mobile projects are required")]
    expected_attachments = {
        "chat": ("chat.png", "image/png"),
        "tasks": ("tasks.png", "image/png"),
        "operator-metadata": ("operator-metadata.json", "application/json"),
        "video": ("video.webm", "video/webm"),
        "trace": ("trace.zip", "application/zip"),
    }
    task_ids: list[str] = []
    for name in ("desktop", "mobile"):
        project = projects.get(name)
        attachments = project.get("attachments") if isinstance(project, Mapping) else None
        if (
            not isinstance(project, Mapping)
            or project.get("project") != name
            or project.get("status") != "passed"
            or project.get("retry") != 0
            or not isinstance(attachments, list)
            or len(attachments) != 5
            or {
                item.get("name") for item in attachments if isinstance(item, Mapping)
            } != set(expected_attachments)
        ):
            return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", "playwright-results.json", f"{name} attachments are invalid")]
        for value in attachments:
            if not isinstance(value, Mapping) or not isinstance(value.get("name"), str):
                return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", "playwright-results.json", f"{name} attachment is invalid")]
            filename, content_type = expected_attachments[str(value["name"])]
            expected_path = f"raw/playwright/{name}/{filename}"
            relative = _safe_relative_path(value.get("path"))
            if (
                relative is None
                or relative.as_posix() != expected_path
                or value.get("content_type") != content_type
                or not _safe_regular_file(bundle, relative)
            ):
                return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", "playwright-results.json", f"{name} attachment is unavailable")]
        metadata_path = bundle / f"raw/playwright/{name}/operator-metadata.json"
        try:
            metadata = json.loads(metadata_path.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", metadata_path.relative_to(bundle).as_posix(), f"{name} metadata is invalid")]
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("project") != name
            or not isinstance(metadata.get("task_id"), str)
            or not isinstance(metadata.get("command_body"), str)
            or metadata.get("expected_output")
            != f"edgecitadel:{metadata.get('command_body')}"
        ):
            return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", metadata_path.relative_to(bundle).as_posix(), f"{name} metadata is invalid")]
        task_ids.append(str(metadata["task_id"]))
    if len(set(task_ids)) != 2:
        return [_issue("LAB_OPERATOR_EVIDENCE_INVALID", "playwright-results.json", "project task IDs must be distinct")]
    return []


def _cleanup_issues(manifest: Mapping[str, object], loaded: Mapping[str, object]) -> list[LabContractIssue]:
    cleanup = manifest.get("cleanup")
    raw = loaded.get("cleanup")
    valid = (
        isinstance(cleanup, Mapping)
        and raw == cleanup
        and cleanup.get("completed") is True
        and cleanup.get("remaining") == []
        and cleanup.get("owned_resources_removed") is True
        and cleanup.get("foreign_resources_touched") is False
        and cleanup.get("credential_removed") is True
        and cleanup.get("artifact_state_removed") is True
        and cleanup.get("artifact_scratch_removed") is True
        and cleanup.get("artifact_recovery_record_removed") is True
    )
    return [] if valid else [_issue("LAB_CLEANUP_RESIDUE", "raw/lab/cleanup.json", "cleanup is incomplete or left residue")]


def lab_semantic_issues(
    bundle: Path,
    manifest: Mapping[str, object],
    source_root: Path,
) -> tuple[LabContractIssue, ...]:
    """Return stable semantic issues without mutating evidence or finalizing it."""
    bundle = bundle.resolve()
    issues = _portable_issues(manifest)
    issues.extend(_source_issues(manifest, source_root))
    observation_issues, loaded = _observation_refs(bundle, manifest)
    issues.extend(observation_issues)
    for name, value in loaded.items():
        issues.extend(_portable_issues(value, f"observations.{name}"))
    controller = manifest.get("controller")
    if (
        not isinstance(controller, Mapping)
        or not _is_ipv4(controller.get("bind_host"))
        or not _is_ipv4(controller.get("advertised_ip"))
    ):
        issues.append(_issue("LAB_CONTROLLER_NETWORK_INVALID", "manifest.json", "controller IPv4 facts are invalid"))
    issues.extend(_cleanup_issues(manifest, loaded))
    issues.extend(_node_binding_issues(manifest, loaded))
    variant = manifest.get("lab_variant")
    nodes = manifest.get("nodes")
    if variant == "lifecycle":
        issues.extend(_lifecycle_issues(manifest, loaded))
        if "operator_evidence" in manifest:
            issues.append(_issue("LAB_VARIANT_FIELDS_INVALID", "manifest.json", "lifecycle cannot claim operator evidence"))
    elif variant == "operator-smoke":
        if not isinstance(nodes, list) or len(nodes) != 1 or not isinstance(nodes[0], Mapping) or nodes[0].get("agent_id") != "shell-1" or "operator_evidence" in manifest:
            issues.append(_issue("LAB_VARIANT_FIELDS_INVALID", "manifest.json", "operator smoke requires one shell-1 node"))
        issues.extend(_operator_smoke_issues(manifest, loaded))
    elif variant == "operator-evidence":
        if not isinstance(nodes, list) or len(nodes) != 1 or not isinstance(nodes[0], Mapping) or nodes[0].get("agent_id") != "shell-1":
            issues.append(_issue("LAB_VARIANT_FIELDS_INVALID", "manifest.json", "operator evidence requires one shell-1 node"))
        issues.extend(_operator_evidence_issues(bundle, manifest, loaded))
    else:
        issues.append(_issue("LAB_VARIANT_INVALID", "manifest.json", "lab variant is invalid"))
    unique: dict[tuple[str, str, str], LabContractIssue] = {}
    for issue in issues:
        unique[(issue.code, issue.relative_path, issue.message)] = issue
    return tuple(unique.values())


def require_complete_lab_manifest(
    bundle: Path,
    manifest: Mapping[str, object],
    source_root: Path,
) -> None:
    issues = lab_semantic_issues(bundle, manifest, source_root)
    if issues:
        raise LabConfigError(
            "; ".join(f"{issue.code}: {issue.message}" for issue in issues)
        )


__all__ = [
    "LabContractIssue",
    "lab_semantic_issues",
    "require_complete_lab_manifest",
]
