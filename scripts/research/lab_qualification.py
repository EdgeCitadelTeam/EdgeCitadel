"""Fail-closed labels for finalized multi-host lab evidence."""

from __future__ import annotations

import argparse
import ipaddress
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping, Sequence

from scripts.research.check_artifact import CheckReport, check_bundle


@dataclass(frozen=True)
class LabQualification:
    status: Literal["preliminary", "remote-qualified"]
    same_host_two_node: bool
    remote_qualified: bool
    reasons: tuple[str, ...]


def _mapping(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _mappings(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _is_non_loopback_ip(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return address.version == 4 and not address.is_loopback and not address.is_unspecified


def _valid_host_report(report: Mapping[str, object]) -> bool:
    return (
        report.get("preflight_valid") is True
        and isinstance(report.get("os_release"), str)
        and str(report["os_release"]).startswith("Ubuntu 24.04")
        and report.get("architecture") == "x86_64"
    )


def _observation_refs_complete(manifest: Mapping[str, object]) -> bool:
    observations = _mapping(manifest.get("observations"))
    artifacts = _mapping(manifest.get("artifacts"))
    if observations is None or artifacts is None:
        return False
    refs: list[object] = [
        observations.get(name)
        for name in (
            "reservation_events",
            "node_reports",
            "controller_commands",
            "cleanup",
        )
    ]
    playwright = observations.get("playwright")
    if not isinstance(playwright, list):
        return False
    refs.extend(playwright)
    for value in refs:
        ref = _mapping(value)
        if ref is None:
            return False
        path = ref.get("path")
        sha256 = ref.get("sha256")
        if (
            not isinstance(path, str)
            or not isinstance(sha256, str)
            or artifacts.get(path) != sha256
        ):
            return False
    return True


def _command_evidence(
    manifest: Mapping[str, object],
) -> tuple[list[Mapping[str, object]], list[Mapping[str, object]]]:
    evidence = _mapping(manifest.get("controller_commands"))
    if evidence is None:
        return [], []
    return _mappings(evidence.get("launches")), _mappings(evidence.get("commands"))


def _launch_binding_complete(
    launches: Sequence[Mapping[str, object]],
    reports: Sequence[Mapping[str, object]],
) -> bool:
    return all(
        sum(
            1
            for launch in launches
            if all(
                launch.get(name) == report.get(name)
                for name in ("agent_id", "reservation_id", "declared_host_id")
            )
        )
        == 1
        for report in reports
    )


def _successful_commands(
    commands: Sequence[Mapping[str, object]],
    report: Mapping[str, object],
    *,
    qualification_kind: str,
) -> list[Mapping[str, object]]:
    return [
        item
        for item in commands
        if item.get("agent_id") == report.get("agent_id")
        and item.get("reservation_id") == report.get("reservation_id")
        and item.get("qualification_kind") == qualification_kind
        and item.get("status") == "completed"
        and item.get("wire_copies") == 1
        and item.get("http_status") == 202
        and isinstance(item.get("task_id"), str)
        and bool(item.get("task_id"))
        and isinstance(item.get("expected_output"), str)
        and str(item["expected_output"]).startswith("edgecitadel:")
        and item.get("terminal_output") == item.get("expected_output")
        and item.get("terminal_count") == 1
        and item.get("conflicting_terminal") is False
        and isinstance(item.get("accepted_at"), str)
        and isinstance(item.get("terminal_at"), str)
        and str(item["accepted_at"]) < str(item["terminal_at"])
    ]


def _commands_complete(
    commands: Sequence[Mapping[str, object]],
    controller: Mapping[str, object],
    remote: Mapping[str, object],
) -> bool:
    local_commands = _successful_commands(
        commands, controller, qualification_kind="direct"
    )
    remote_commands = _successful_commands(
        commands, remote, qualification_kind="direct"
    )
    return any(
        left.get("task_id") != right.get("task_id")
        for left in local_commands
        for right in remote_commands
    )


def _queued_reconnect_complete(
    commands: Sequence[Mapping[str, object]],
    events: Sequence[Mapping[str, object]],
    remote: Mapping[str, object],
) -> bool:
    matching_events = [
        item
        for item in events
        if item.get("agent_id") == remote.get("agent_id")
        and item.get("reservation_id") == remote.get("reservation_id")
        and item.get("declared_host_id") == remote.get("declared_host_id")
    ]
    retained = [item for item in matching_events if item.get("event") == "retained"]
    resumed = [item for item in matching_events if item.get("event") == "resumed"]
    queued = [
        item
        for item in _successful_commands(
            commands, remote, qualification_kind="queued-reconnect"
        )
    ]
    for command in queued:
        for before in retained:
            for after in resumed:
                values = (
                    before.get("observed_at"),
                    command.get("accepted_at"),
                    after.get("observed_at"),
                    command.get("terminal_at"),
                )
                sequences = (before.get("sequence"), after.get("sequence"))
                if (
                    all(isinstance(value, str) for value in values)
                    and str(values[0]) < str(values[1]) < str(values[2]) < str(values[3])
                    and all(type(value) is int for value in sequences)
                    and int(sequences[0]) < int(sequences[1])
                ):
                    return True
    return False


def _remote_reasons(
    manifest: Mapping[str, object],
    local: Mapping[str, object],
    remote: Mapping[str, object],
    launches: Sequence[Mapping[str, object]],
    commands: Sequence[Mapping[str, object]],
    events: Sequence[Mapping[str, object]],
) -> list[str]:
    reasons: list[str] = []
    if not _valid_host_report(local) or not _valid_host_report(remote):
        reasons.append("supported_host_reports_required")
    if local.get("machine_id_sha256") == remote.get("machine_id_sha256"):
        reasons.append("distinct_machine_ids_required")
    if not _launch_binding_complete(launches, (local, remote)):
        reasons.append("launch_host_binding_mismatch")
    source = _mapping(manifest.get("source"))
    if source is None or any(
        item.get("launcher_source_commit") != source.get("commit")
        or item.get("source_snapshot_sha256") != source.get("source_sha256")
        for item in (local, remote)
    ):
        reasons.append("source_provenance_mismatch")
    controller = _mapping(manifest.get("controller")) or {}
    route = _mapping(remote.get("network_path"))
    if (
        route is None
        or not _is_non_loopback_ip(route.get("source_ip"))
        or not _is_non_loopback_ip(route.get("destination_ip"))
        or route.get("interface") == "lo"
    ):
        reasons.append("remote_route_invalid")
    else:
        if route.get("source_ip") != remote.get("server_observed_peer_ip"):
            reasons.append("remote_peer_mismatch")
        if route.get("destination_ip") != controller.get("advertised_ip"):
            reasons.append("remote_destination_mismatch")
        if route.get("controller_dns_name") != controller.get("advertised_host"):
            reasons.append("remote_controller_name_mismatch")
    if not _commands_complete(commands, local, remote):
        reasons.append("host_command_evidence_missing")
    if not _queued_reconnect_complete(commands, events, remote):
        reasons.append("queued_reconnect_order_missing")
    return reasons


def classify_lab(
    *, manifest: Mapping[str, object], check_report: CheckReport
) -> LabQualification:
    """Classify retained evidence only; absent or inconsistent facts fail closed."""
    reasons: list[str] = []
    if check_report.valid is not True:
        reasons.append("artifact_check_failed")
    if manifest.get("status") != "PASS":
        reasons.append("manifest_not_pass")
    if manifest.get("lab_variant") != "lifecycle":
        reasons.append("lifecycle_variant_required")
    if not _observation_refs_complete(manifest):
        reasons.append("observation_artifacts_incomplete")

    controller = _mapping(manifest.get("controller"))
    reports = _mappings(manifest.get("nodes"))
    machine_ids = {
        item.get("machine_id_sha256")
        for item in reports
        if isinstance(item.get("machine_id_sha256"), str)
    }
    same_host_two_node = len(reports) >= 2 and len(machine_ids) == 1
    controller_host = controller.get("declared_host_id") if controller else None
    controller_reports = [
        item for item in reports if item.get("declared_host_id") == controller_host
    ]
    remote_reports = [
        item for item in reports if item.get("declared_host_id") != controller_host
    ]
    if controller is None or len(controller_reports) != 1 or not remote_reports:
        reasons.append("distinct_declared_hosts_required")
    else:
        launches, commands = _command_evidence(manifest)
        events = _mappings(manifest.get("reservation_events"))
        candidate_reasons = [
            _remote_reasons(
                manifest,
                controller_reports[0],
                remote,
                launches,
                commands,
                events,
            )
            for remote in remote_reports
        ]
        reasons.extend(min(candidate_reasons, key=len))

    ordered_reasons = tuple(dict.fromkeys(reasons))
    remote_qualified = not ordered_reasons
    return LabQualification(
        "remote-qualified" if remote_qualified else "preliminary",
        same_host_two_node,
        remote_qualified,
        ordered_reasons,
    )


def _load_manifest_with_observations(bundle: Path) -> Mapping[str, object]:
    manifest_path = bundle / "manifest.json"
    value = json.loads(manifest_path.read_text())
    if not isinstance(value, dict):
        raise ValueError("manifest is not an object")
    observations = _mapping(value.get("observations"))
    if observations is None:
        return value
    for name, target in (
        ("controller_commands", "controller_commands"),
        ("reservation_events", "reservation_events"),
    ):
        ref = _mapping(observations.get(name))
        relative = ref.get("path") if ref else None
        if not isinstance(relative, str):
            continue
        path = bundle / relative
        loaded = json.loads(path.read_text())
        value[target] = loaded
    return value


def qualify_bundle(*, bundle: Path, source_root: Path) -> tuple[LabQualification, bool]:
    report = check_bundle(
        bundle.resolve(), expected_kind="lab", source_root=source_root.resolve()
    )
    try:
        report.require_valid()
        manifest = _load_manifest_with_observations(bundle.resolve())
    except (OSError, ValueError, json.JSONDecodeError, RuntimeError):
        manifest = {}
    return classify_lab(manifest=manifest, check_report=report), report.valid


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    arguments = parser.parse_args(argv)
    qualification, checker_valid = qualify_bundle(
        bundle=arguments.bundle, source_root=arguments.source_root
    )
    print(
        "lab qualification: "
        f"{'REMOTE QUALIFIED' if qualification.remote_qualified else 'PRELIMINARY'}"
    )
    if not checker_valid:
        return 2
    return 0 if qualification.remote_qualified else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["LabQualification", "classify_lab", "main", "qualify_bundle"]
