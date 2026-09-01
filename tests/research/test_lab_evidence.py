"""Strict schema and semantic contracts for multi-agent lab evidence."""

from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from scripts.research import evidence as evidence_module
from scripts.research.check_artifact import CheckReport, check_bundle
from scripts.research.evidence import (
    file_sha256,
    finalize_bundle,
    write_json,
)
from scripts.research.lab_contract import (
    lab_semantic_issues,
    require_complete_lab_manifest,
)
from scripts.research.lab_runtime import (
    LAB_SOURCE_PATHS,
    capture_clean_source_provenance,
)


SCHEMA = Path("schemas/research-manifest.v1.json")
TASKS = (
    "10000000-0000-4000-8000-000000000001",
    "10000000-0000-4000-8000-000000000002",
    "10000000-0000-4000-8000-000000000003",
)


def _source(root: Path, value: str = "VALUE = 1\n") -> Path:
    (root / "scripts/research").mkdir(parents=True)
    subprocess.run(["git", "init", "--quiet"], cwd=root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "tests@example.invalid"], cwd=root, check=True
    )
    subprocess.run(["git", "config", "user.name", "tests"], cwd=root, check=True)
    (root / "scripts/research/fixture.py").write_text(value)
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "initial"], cwd=root, check=True)
    return root


def _node(
    agent_id: str, reservation_id: str, host_id: str, machine: str
) -> dict[str, object]:
    return {
        "agent_id": agent_id,
        "qualified_agent_id": f"ec-lab-01--{agent_id}",
        "reservation_id": reservation_id,
        "declared_host_id": host_id,
        "machine_id_sha256": machine,
        "hostname": host_id,
        "os_release": "Ubuntu 24.04 LTS",
        "architecture": "x86_64",
        "fixture_image_id": "sha256:" + "5" * 64,
        "launcher_source_commit": "3" * 40,
        "source_snapshot_sha256": "4" * 64,
        "preflight_valid": True,
        "lifecycle_state": "active",
        "server_observed_peer_ip": "100.64.10.11",
        "network_path": {
            "source_ip": "100.64.10.11",
            "destination_ip": "100.64.10.10",
            "interface": "tailscale0",
            "route_output_sha256": "6" * 64,
            "controller_dns_name": "controller-lab.internal",
        },
    }


def _cleanup() -> dict[str, object]:
    return {
        "completed": True,
        "attempted": [
            {"kind": "network", "name": "edgecitadel-artifact-ec-lab-01_default"}
        ],
        "remaining": [],
        "owned_resources_removed": True,
        "foreign_resources_touched": False,
        "credential_removed": True,
        "artifact_state_removed": True,
        "artifact_scratch_removed": True,
        "artifact_recovery_record_removed": True,
        "completed_at": "2026-07-27T00:00:06Z",
    }


def _ref(bundle: Path, relative: str) -> dict[str, str]:
    return {"path": relative, "sha256": file_sha256(bundle / relative)}


def _replace_json(path: Path, value: object) -> None:
    path.unlink()
    write_json(path, value)


def _fixture(
    root: Path, source_root: Path, variant: str
) -> tuple[Path, dict[str, object]]:
    bundle = root / variant
    bundle.mkdir(parents=True)
    provenance = capture_clean_source_provenance(source_root)
    operator = variant != "lifecycle"
    nodes = (
        [_node("shell-1", "reservation-shell", "controller-lab-01", "1" * 64)]
        if operator
        else [
            _node("fixture-1", "reservation-1", "controller-lab-01", "1" * 64),
            _node("fixture-2", "reservation-2", "gateway-lab-02", "2" * 64),
        ]
    )
    for item in nodes:
        item["launcher_source_commit"] = provenance.commit
        item["source_snapshot_sha256"] = provenance.source_snapshot_sha256
    agent = str(nodes[0]["agent_id"])
    reservation = str(nodes[0]["reservation_id"])
    events = [
        {
            "sequence": 1,
            "agent_id": agent,
            "reservation_id": reservation,
            "declared_host_id": nodes[0]["declared_host_id"],
            "event": "reserved",
            "observed_at": "2026-07-27T00:00:00Z",
        },
        {
            "sequence": 2,
            "agent_id": agent,
            "reservation_id": reservation,
            "declared_host_id": nodes[0]["declared_host_id"],
            "event": "retained",
            "observed_at": "2026-07-27T00:00:01Z",
        },
        {
            "sequence": 3,
            "agent_id": agent,
            "reservation_id": reservation,
            "declared_host_id": nodes[0]["declared_host_id"],
            "event": "resumed",
            "observed_at": "2026-07-27T00:00:03Z",
        },
        {
            "sequence": 4,
            "agent_id": agent,
            "reservation_id": reservation,
            "declared_host_id": nodes[0]["declared_host_id"],
            "event": "released",
            "observed_at": "2026-07-27T00:00:05Z",
        },
    ]
    commands = [
        {
            "agent_id": agent,
            "reservation_id": reservation,
            "task_id": TASKS[0],
            "accepted_at": "2026-07-27T00:00:00.500Z",
            "terminal_at": "2026-07-27T00:00:00.900Z",
            "expected_output": "edgecitadel:one",
            "terminal_output": "edgecitadel:one",
            "terminal_count": 1,
            "conflicting_terminal": False,
            "wire_copies": 1,
            "http_status": 202,
            "qualification_kind": "direct",
            "status": "completed",
        },
    ]
    if variant == "lifecycle":
        commands = [
            {**commands[0], "agent_id": "fixture-1", "reservation_id": "reservation-1"},
            {
                **commands[0],
                "agent_id": "fixture-2",
                "reservation_id": "reservation-2",
                "task_id": TASKS[1],
                "wire_copies": 2,
                "http_status": None,
            },
            {
                **commands[0],
                "agent_id": "fixture-1",
                "reservation_id": "reservation-1",
                "task_id": TASKS[2],
                "accepted_at": "2026-07-27T00:00:02Z",
                "terminal_at": "2026-07-27T00:00:04Z",
                "qualification_kind": "queued-reconnect",
            },
        ]
    if variant == "operator-smoke":
        nonce = "20000000-0000-4000-8000-000000000001"
        commands[0]["expected_output"] = f"edgecitadel:{nonce}"
        commands[0]["terminal_output"] = f"edgecitadel:{nonce}"
    launches = [
        {
            "agent_id": item["agent_id"],
            "qualified_agent_id": item["qualified_agent_id"],
            "reservation_id": item["reservation_id"],
            "declared_host_id": item["declared_host_id"],
        }
        for item in nodes
    ]
    reports = [{key: value for key, value in item.items()} for item in nodes]
    write_json(bundle / "raw/lab/reservation-events.json", events)
    write_json(bundle / "raw/lab/node-reports.json", reports)
    write_json(
        bundle / "raw/lab/controller-commands.json",
        {"launches": launches, "commands": commands},
    )
    cleanup = _cleanup()
    write_json(bundle / "raw/lab/cleanup.json", cleanup)
    playwright_refs: list[dict[str, str]] = []
    if variant == "operator-smoke":
        smoke = {
            "argv": [
                "npx",
                "--no-install",
                "playwright",
                "test",
                "--config",
                "playwright.config.js",
                "tests/operator-journey.spec.js",
            ],
            "cwd": "e2e",
            "returncode": 0,
            "assertion": "1 passed",
            "task_id": TASKS[0],
            "context_id": TASKS[0],
            "hop_count": 0,
            "nonce": nonce,
            "output": f"edgecitadel:{nonce}",
        }
        write_json(bundle / "playwright-smoke.json", smoke)
        playwright_refs.append(_ref(bundle, "playwright-smoke.json"))
    if variant == "operator-evidence":
        projects: dict[str, object] = {}
        for index, project in enumerate(("desktop", "mobile"), start=1):
            task_id = TASKS[index - 1]
            attachments = []
            attachment_specs = (
                ("chat", "chat.png", "image/png"),
                ("tasks", "tasks.png", "image/png"),
                ("operator-metadata", "operator-metadata.json", "application/json"),
                ("video", "video.webm", "video/webm"),
                ("trace", "trace.zip", "application/zip"),
            )
            for name, filename, content_type in attachment_specs:
                path = bundle / "raw/playwright" / project / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                if name == "operator-metadata":
                    write_json(
                        path,
                        {
                            "project": project,
                            "task_id": task_id,
                            "command_body": f"{project}-nonce",
                            "expected_output": f"edgecitadel:{project}-nonce",
                        },
                    )
                else:
                    path.write_bytes(f"{project}:{filename}".encode())
                attachments.append(
                    {
                        "name": name,
                        "path": f"raw/playwright/{project}/{filename}",
                        "content_type": content_type,
                    }
                )
            projects[project] = {
                "project": project,
                "title": "operator observes one deterministic task lifecycle",
                "status": "passed",
                "retry": 0,
                "duration_ms": 12,
                "attachments": attachments,
            }
        report = {
            "schema_version": "playwright-operator-results.v1",
            "projects": projects,
        }
        write_json(bundle / "playwright-results.json", report)
        playwright_refs.append(_ref(bundle, "playwright-results.json"))
    manifest: dict[str, object] = {
        "schema_version": "research-manifest.v1",
        "evidence_kind": "lab",
        "status": "PENDING",
        "run_id": "ec-lab-01",
        "lab_variant": variant,
        "source": {
            "commit": provenance.commit,
            "git_dirty": provenance.dirty,
            "source_sha256": provenance.source_snapshot_sha256,
            "paths": list(LAB_SOURCE_PATHS),
        },
        "command": [
            [
                "$SOURCE_ROOT/scripts/research/lab_controller.py",
                "start",
                "--run-id",
                "ec-lab-01",
            ]
        ],
        "timing": {
            "started_at": "2026-07-27T00:00:00Z",
            "completed_at": "2026-07-27T00:00:06Z",
        },
        "host": {"os": "Linux", "architecture": "x86_64"},
        "dependencies": {
            name: value
            for name, value in (
                ("python", "Python 3.12.11"),
                ("docker", "Docker 28.0.0"),
                ("docker_compose", "Docker Compose 2.38.2"),
                ("git", "git 2.50.1"),
                ("node", "v22.17.0"),
                ("npm", "11.4.2"),
                ("playwright", "Version 1.54.1"),
            )
        },
        "images": {
            "nats": "nats@sha256:" + "a" * 64,
            "aggregator": "sha256:" + "b" * 64,
            "dashboard": "sha256:" + "c" * 64,
            "nginx": "nginx@sha256:" + "d" * 64,
            "fixture": "sha256:" + "5" * 64,
        },
        "compose_config_sha256": "e" * 64,
        "schemas": {"manifest": "schemas/research-manifest.v1.json"},
        "cleanup": cleanup,
        "artifacts": {},
        "controller": {
            "project": "edgecitadel-artifact-ec-lab-01",
            "bind_host": "127.0.0.1",
            "advertised_host": "controller-lab.internal",
            "advertised_ip": "100.64.10.10",
            "app_url": "http://100.64.10.10:18080",
            "nats_url": "nats://100.64.10.10:14222",
            "monitor_url": "http://127.0.0.1:18222",
            "inventory_url": "http://100.64.10.10:18080/api/lab/status",
            "declared_host_id": "controller-lab-01",
            "machine_id_sha256": "1" * 64,
            "hostname": "controller-lab-01",
            "os_release": "Ubuntu 24.04 LTS",
            "architecture": "x86_64",
        },
        "nodes": nodes,
        "observations": {
            "reservation_events": _ref(bundle, "raw/lab/reservation-events.json"),
            "node_reports": _ref(bundle, "raw/lab/node-reports.json"),
            "controller_commands": _ref(bundle, "raw/lab/controller-commands.json"),
            "playwright": playwright_refs,
            "cleanup": _ref(bundle, "raw/lab/cleanup.json"),
        },
    }
    if variant == "operator-evidence":
        manifest["operator_evidence"] = {"report": playwright_refs[0]}
    return bundle, manifest


def _seal(bundle: Path, manifest: dict[str, object], source_root: Path) -> CheckReport:
    require_complete_lab_manifest(bundle, manifest, source_root)
    assert finalize_bundle(bundle, manifest, SCHEMA) == "PASS"
    report = check_bundle(bundle, expected_kind="lab", source_root=source_root)
    report.require_valid()
    return report


def _schema_accepts_final_candidate(bundle: Path, manifest: dict[str, object]) -> bool:
    candidate = deepcopy(manifest)
    candidate["status"] = "PASS"
    candidate["artifacts"] = {
        path.relative_to(bundle).as_posix(): file_sha256(path)
        for path in sorted(bundle.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }
    candidate["manifest_sha256"] = "f" * 64
    schema = json.loads(SCHEMA.read_text())
    return Draft202012Validator(schema).is_valid(candidate)


def _operator_project(task_id: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "nonce": "20000000-0000-4000-8000-000000000001",
        "command_body": "nonce",
        "expected_output": "edgecitadel:nonce",
        "context_id": task_id,
        "hop_count": 0,
        "command_envelope_id": "command-1",
        "terminal_envelope_id": "terminal-1",
        "progress_envelope_ids": ["progress-1"],
        "command_sender_id": "aggregator",
        "command_recipient_id": "shell-1",
        "terminal_sender_id": "shell-1",
        "terminal_recipient_id": "aggregator",
        "browser_name": "chromium",
        "browser_version": "chromium-1",
        "command_observation_index": 1,
        "progress_observation_indices": [2],
        "terminal_observation_index": 3,
    }


def test_all_lab_variants_finalize_and_source_root_remains_optional_at_api_boundary(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path / "source")
    for variant in ("lifecycle", "operator-smoke", "operator-evidence"):
        bundle, manifest = _fixture(tmp_path, source, variant)
        assert isinstance(_seal(bundle, manifest, source), CheckReport)
        missing = check_bundle(bundle, expected_kind="lab")
        assert missing.valid is False
        assert [issue.code for issue in missing.issues] == ["LAB_SOURCE_ROOT_REQUIRED"]


def test_explicit_invalid_manifest_is_validated_hashed_and_atomically_sealed(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "lifecycle")
    manifest["status"] = "INVALID"

    assert finalize_bundle(bundle, manifest, SCHEMA) == "INVALID"

    sealed = json.loads((bundle / "manifest.json").read_text())
    assert sealed["status"] == "INVALID"
    assert sealed["artifacts"] == {
        path.relative_to(bundle).as_posix(): file_sha256(path)
        for path in sorted(bundle.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }
    assert sealed["manifest_sha256"] == evidence_module.manifest_sha256(sealed)
    assert not (bundle / "manifest.tmp").exists()

    early_bundle, early_manifest = _fixture(
        tmp_path / "early-failure", source, "lifecycle"
    )
    early_manifest["status"] = "INVALID"
    early_manifest["nodes"] = []
    early_manifest["images"] = {
        name: "unavailable"
        for name in ("nats", "aggregator", "dashboard", "nginx", "fixture")
    }
    assert finalize_bundle(early_bundle, early_manifest, SCHEMA) == "INVALID"
    assert (early_bundle / "manifest.json").is_file()

    unavailable_pass_bundle, unavailable_pass = _fixture(
        tmp_path / "unavailable-pass", source, "lifecycle"
    )
    unavailable_pass["images"] = {
        name: "unavailable"
        for name in ("nats", "aggregator", "dashboard", "nginx", "fixture")
    }
    assert (
        finalize_bundle(unavailable_pass_bundle, unavailable_pass, SCHEMA) == "INVALID"
    )
    assert not (unavailable_pass_bundle / "manifest.json").exists()

    invalid_bundle, invalid_manifest = _fixture(
        tmp_path / "invalid-schema", source, "lifecycle"
    )
    invalid_manifest["status"] = "INVALID"
    invalid_manifest.pop("nodes")
    assert finalize_bundle(invalid_bundle, invalid_manifest, SCHEMA) == "INVALID"
    assert not (invalid_bundle / "manifest.json").exists()

    secret_bundle, secret_manifest = _fixture(tmp_path / "secret", source, "lifecycle")
    secret_manifest["status"] = "INVALID"
    write_json(secret_bundle / "raw/lab/leaked.json", {"token": "a" * 64})
    assert finalize_bundle(secret_bundle, secret_manifest, SCHEMA) == "INVALID"
    assert not (secret_bundle / "manifest.json").exists()


def test_checker_reports_manifest_hash_mismatch(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "lifecycle")
    _seal(bundle, manifest, source)
    sealed = json.loads((bundle / "manifest.json").read_text())
    sealed["manifest_sha256"] = "0" * 64
    (bundle / "manifest.json").write_text(json.dumps(sealed))

    report = check_bundle(bundle, expected_kind="lab", source_root=source)

    assert "MANIFEST_HASH_MISMATCH" in {issue.code for issue in report.issues}


def test_lab_schema_rejects_missing_required_structures(tmp_path: Path) -> None:
    for field in ("nodes", "controller", "observations"):
        case = tmp_path / field
        source = _source(case / "source")
        bundle, manifest = _fixture(case, source, "lifecycle")
        manifest.pop(field)
        assert _schema_accepts_final_candidate(bundle, manifest) is False
        assert not (bundle / "manifest.json").exists()


def test_lab_schema_rejects_benchmark_and_operator_fields(tmp_path: Path) -> None:
    fields = (
        "campaign_id",
        "profile",
        "transport_config",
        "workload_config",
        "metric_contract",
        "projects",
    )
    for field in fields:
        case = tmp_path / field
        source = _source(case / "source")
        bundle, manifest = _fixture(case, source, "operator-smoke")
        manifest[field] = {} if field not in {"campaign_id", "profile"} else "forbidden"
        assert _schema_accepts_final_candidate(bundle, manifest) is False

    source = _source(tmp_path / "settled/source")
    bundle, lab = _fixture(tmp_path / "settled", source, "operator-smoke")
    common = deepcopy(lab)
    for field in (
        "lab_variant",
        "controller",
        "nodes",
        "observations",
        "operator_evidence",
    ):
        common.pop(field, None)
    common["command"] = ["settled", "argv"]

    benchmark = deepcopy(common)
    benchmark.update(
        {
            "evidence_kind": "benchmark",
            "campaign_id": "campaign-1",
            "profile": "baseline",
            "transport_config": {},
            "workload_config": {},
            "metric_contract": {},
        }
    )
    assert _schema_accepts_final_candidate(bundle, benchmark) is True
    benchmark["command"] = [["nested", "lab-argv"]]
    assert _schema_accepts_final_candidate(bundle, benchmark) is False

    operator = deepcopy(common)
    operator.update(
        {
            "evidence_kind": "operator",
            "task": {},
            "media": {},
            "projects": {
                "desktop": _operator_project(TASKS[0]),
                "mobile": _operator_project(TASKS[1]),
            },
        }
    )
    assert _schema_accepts_final_candidate(bundle, operator) is True
    operator["lab_variant"] = "operator-smoke"
    assert _schema_accepts_final_candidate(bundle, operator) is False


def test_post_finalization_raw_mutation_does_not_rewrite_manifest(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "lifecycle")
    _seal(bundle, manifest, source)
    sealed = (bundle / "manifest.json").read_bytes()
    (bundle / "raw/lab/node-reports.json").write_text("[]\n")
    assert check_bundle(bundle, expected_kind="lab", source_root=source).valid is False
    assert (bundle / "manifest.json").read_bytes() == sealed


def test_different_scoped_source_snapshot_has_stable_issue(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    other = _source(tmp_path / "other", "VALUE = 2\n")
    bundle, manifest = _fixture(tmp_path, source, "operator-smoke")
    _seal(bundle, manifest, source)
    report = check_bundle(bundle, expected_kind="lab", source_root=other)
    assert "LAB_SOURCE_SNAPSHOT_MISMATCH" in {issue.code for issue in report.issues}


def test_missing_reservation_history_has_stable_issue(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "lifecycle")
    commands_path = bundle / "raw/lab/controller-commands.json"
    original_commands = json.loads(commands_path.read_text())
    for mutation in ("incomplete", "single-node"):
        commands = deepcopy(original_commands)
        if mutation == "incomplete":
            commands["commands"][0]["status"] = "accepted"
        else:
            for item in commands["commands"]:
                item["agent_id"] = "fixture-1"
                item["reservation_id"] = "reservation-1"
        _replace_json(commands_path, commands)
        manifest["observations"]["controller_commands"] = _ref(
            bundle, "raw/lab/controller-commands.json"
        )
        assert "LAB_LIFECYCLE_COMMANDS_INCOMPLETE" in {
            issue.code for issue in lab_semantic_issues(bundle, manifest, source)
        }
    _replace_json(commands_path, original_commands)
    manifest["observations"]["controller_commands"] = _ref(
        bundle, "raw/lab/controller-commands.json"
    )
    write_path = bundle / "raw/lab/reservation-events.json"
    events = json.loads(write_path.read_text())
    _replace_json(write_path, [item for item in events if item["event"] != "resumed"])
    manifest["observations"]["reservation_events"] = _ref(
        bundle, "raw/lab/reservation-events.json"
    )
    assert finalize_bundle(bundle, manifest, SCHEMA) == "PASS"
    report = check_bundle(bundle, expected_kind="lab", source_root=source)
    assert "LAB_RESERVATION_HISTORY_INCOMPLETE" in {
        issue.code for issue in report.issues
    }


def test_queued_acceptance_outside_disconnect_reconnect_terminal_order_is_invalid(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "lifecycle")
    path = bundle / "raw/lab/controller-commands.json"
    evidence = json.loads(path.read_text())
    evidence["commands"][-1]["accepted_at"] = "2026-07-27T00:00:00.500Z"
    path.unlink()
    write_json(path, evidence)
    manifest["observations"]["controller_commands"] = _ref(
        bundle, "raw/lab/controller-commands.json"
    )
    assert finalize_bundle(bundle, manifest, SCHEMA) == "PASS"
    report = check_bundle(bundle, expected_kind="lab", source_root=source)
    assert "LAB_RECONNECT_ORDER_INVALID" in {issue.code for issue in report.issues}


def test_node_report_must_match_launch_reservation_and_host(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "lifecycle")
    mutations = (
        ("reservation_id", "reservation-other"),
        ("machine_id_sha256", "9" * 64),
        ("hostname", "other-host"),
        ("os_release", "Other Linux"),
        ("architecture", "aarch64"),
        ("launcher_source_commit", "9" * 40),
        ("source_snapshot_sha256", "9" * 64),
        ("preflight_valid", False),
        ("server_observed_peer_ip", "100.64.10.99"),
    )
    for field, value in mutations:
        broken = deepcopy(manifest)
        broken["nodes"][0][field] = value
        assert "LAB_NODE_BINDING_INVALID" in {
            issue.code for issue in lab_semantic_issues(bundle, broken, source)
        }
    broken = deepcopy(manifest)
    broken["nodes"][0]["network_path"]["destination_ip"] = "100.64.10.99"
    assert "LAB_NODE_BINDING_INVALID" in {
        issue.code for issue in lab_semantic_issues(bundle, broken, source)
    }

    path = bundle / "raw/lab/node-reports.json"
    reports = json.loads(path.read_text())
    reports[0]["reservation_id"] = "reservation-other"
    _replace_json(path, reports)
    manifest["observations"]["node_reports"] = _ref(bundle, "raw/lab/node-reports.json")
    assert finalize_bundle(bundle, manifest, SCHEMA) == "PASS"
    report = check_bundle(bundle, expected_kind="lab", source_root=source)
    assert "LAB_NODE_BINDING_INVALID" in {issue.code for issue in report.issues}


def test_cleanup_observation_and_portable_path_failures_have_stable_issues(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "lifecycle")
    broken = deepcopy(manifest)
    broken["cleanup"]["remaining"] = [{"kind": "network", "name": "leftover"}]
    assert "LAB_CLEANUP_RESIDUE" in {
        issue.code for issue in lab_semantic_issues(bundle, broken, source)
    }
    broken = deepcopy(manifest)
    broken["observations"]["node_reports"]["path"] = "raw/lab/missing.json"
    assert "LAB_OBSERVATION_PATH_MISSING" in {
        issue.code for issue in lab_semantic_issues(bundle, broken, source)
    }
    for value in (
        "/source/repo",
        "/tmp/live",
        "/var/folders/live",
        "C:\\worktree\\live",
        "/secrets/transport-token",
    ):
        broken = deepcopy(manifest)
        broken["command"][0].append(value)
        assert "LAB_NONPORTABLE_PATH" in {
            issue.code for issue in lab_semantic_issues(bundle, broken, source)
        }
    broken = deepcopy(manifest)
    broken["controller"]["advertised_ip"] = "999.999.999.999"
    assert "LAB_CONTROLLER_NETWORK_INVALID" in {
        issue.code for issue in lab_semantic_issues(bundle, broken, source)
    }

    smoke_bundle, smoke_manifest = _fixture(
        tmp_path / "smoke-case", source, "operator-smoke"
    )
    commands_path = smoke_bundle / "raw/lab/controller-commands.json"
    commands = json.loads(commands_path.read_text())
    commands["commands"][0]["status"] = "accepted"
    _replace_json(commands_path, commands)
    smoke_manifest["observations"]["controller_commands"] = _ref(
        smoke_bundle, "raw/lab/controller-commands.json"
    )
    assert "LAB_OPERATOR_SMOKE_INVALID" in {
        issue.code
        for issue in lab_semantic_issues(smoke_bundle, smoke_manifest, source)
    }

    operator_bundle, operator_manifest = _fixture(
        tmp_path / "operator-case", source, "operator-evidence"
    )
    assert lab_semantic_issues(operator_bundle, operator_manifest, source) == ()
    broken = deepcopy(operator_manifest)
    broken["operator_evidence"]["report"] = {
        **broken["operator_evidence"]["report"],
        "sha256": "0" * 64,
    }
    assert "LAB_OPERATOR_EVIDENCE_INVALID" in {
        issue.code for issue in lab_semantic_issues(operator_bundle, broken, source)
    }
    broken = deepcopy(operator_manifest)
    broken["observations"]["playwright"].append(
        deepcopy(broken["observations"]["playwright"][0])
    )
    assert "LAB_OPERATOR_EVIDENCE_INVALID" in {
        issue.code for issue in lab_semantic_issues(operator_bundle, broken, source)
    }
    _seal(bundle, manifest, source)
    finalized = json.loads((bundle / "manifest.json").read_text())
    finalized["artifacts"].pop("raw/lab/node-reports.json")
    assert "LAB_OBSERVATION_ARTIFACT_MISSING" in {
        issue.code for issue in lab_semantic_issues(bundle, finalized, source)
    }


def test_checker_is_pure_and_never_invokes_finalizer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path / "source")
    bundle, manifest = _fixture(tmp_path, source, "operator-smoke")
    calls: list[str] = []

    def finalizer(*_args: object, **_kwargs: object) -> str:
        calls.append("finalize")
        return finalize_bundle(*_args, **_kwargs)

    assert finalizer(bundle, manifest, SCHEMA) == "PASS"
    assert calls == ["finalize"]
    monkeypatch.setattr(
        "scripts.research.evidence.finalize_bundle",
        lambda *_args, **_kwargs: calls.append("checker-finalize"),
    )
    check_bundle(bundle, expected_kind="lab", source_root=source).require_valid()
    assert calls == ["finalize"]

    finalized = json.loads((bundle / "manifest.json").read_text())
    finalized.pop("nodes")
    (bundle / "manifest.json").write_text(json.dumps(finalized))
    monkeypatch.setattr(
        "scripts.research.check_artifact.lab_semantic_issues",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("semantic checker")
        ),
    )
    base_failure = check_bundle(bundle, expected_kind="lab", source_root=source)
    assert "MANIFEST_SCHEMA_INVALID" in {issue.code for issue in base_failure.issues}

    from scripts.research import lab_gate

    run_id = "ec-lab-gate"
    gate_root = tmp_path / "gate-root"
    state_file = gate_root / "tmp/research/lab" / run_id / "controller-state.json"
    controller_file = state_file.with_name("controller.json")
    state_file.parent.mkdir(parents=True)
    state_file.write_text("{}\n")
    controller_file.write_text("{}\n")
    gate_bundle = tmp_path / "gate-bundle"
    gate_bundle.mkdir()
    node_state = tmp_path / "gate-node"
    (node_state / "terminal-release").mkdir(parents=True)
    credential = tmp_path / "gate-credential"
    checked: list[tuple[Path, str | None, Path | None]] = []

    config = {
        "credential_file": str(credential.resolve()),
        "app_url": "http://127.0.0.1:18080",
        "agg_url": "http://127.0.0.1:18080",
        "evidence_dir": str(gate_bundle.resolve()),
    }
    messages = [
        {
            "id": "command-1",
            "type": "command",
            "sender_id": "aggregator",
            "recipient_id": "shell-1",
            "task_id": TASKS[0],
            "context_id": TASKS[0],
            "hop_count": 0,
            "payload": {"body": "gate-nonce"},
        },
        {
            "id": "terminal-1",
            "type": "result",
            "task_state": "completed",
            "sender_id": "shell-1",
            "recipient_id": "aggregator",
            "task_id": TASKS[0],
            "context_id": TASKS[0],
            "hop_count": 0,
            "payload": {"body": "edgecitadel:gate-nonce"},
        },
    ]

    def fake_run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        stdout = "1 passed\n" if argv and argv[0] == "npx" else "{}\n"
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    def fake_check(
        checked_bundle: Path,
        *,
        expected_kind: str | None = None,
        source_root: Path | None = None,
    ) -> CheckReport:
        checked.append((checked_bundle, expected_kind, source_root))
        return CheckReport(True, ())

    monkeypatch.setattr(lab_gate, "_run", fake_run)
    monkeypatch.setattr(
        lab_gate,
        "_load_json",
        lambda path: {"state_dir": str(node_state)}
        if path.name == "node-state.json"
        else config,
    )
    monkeypatch.setattr(
        lab_gate,
        "_node_start",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, "node: ready\n", ""
        ),
    )
    monkeypatch.setattr(
        lab_gate,
        "_node_stop",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, "node: stopped\n", ""
        ),
    )
    monkeypatch.setattr(lab_gate, "_wait_online", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(lab_gate, "_doctor", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(lab_gate, "_container_count", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(lab_gate, "_request_json", lambda *_args, **_kwargs: messages)
    monkeypatch.setattr(lab_gate, "check_bundle", fake_check)

    completed = lab_gate.run_operator_journey(
        repo_root=gate_root,
        run_id=run_id,
        host_id="controller-lab-01",
    )

    assert completed.returncode == 0
    assert checked == [(gate_bundle.resolve(), "lab", gate_root.resolve())]
    assert calls == ["finalize"]
    assert not hasattr(lab_gate, "finalize_bundle")
