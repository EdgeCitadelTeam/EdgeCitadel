"""Ubuntu-only maintained-CLI lifecycle gates."""

from __future__ import annotations

import platform
import subprocess
import uuid
from pathlib import Path

import pytest

import scripts.research.lab_gate as gate_module
from scripts.research.check_artifact import CheckReport
from scripts.research.lab_config import LabConfigError
from scripts.research.lab_gate import (
    LifecycleResult,
    assert_disjoint_runs,
    require_single_handler_started,
    run_clean_checkout_gate,
    run_concurrent_pair,
)
from scripts.research.lab_runtime import SourceProvenance


REPO_ROOT = Path(__file__).resolve().parents[2]


def _result(tmp_path: Path, run_id: str, offset: int) -> LifecycleResult:
    bundle = tmp_path / "bundles" / run_id
    command_root = bundle / "raw/lab"
    command_root.mkdir(parents=True, exist_ok=True)
    (command_root / "controller-commands.json").write_text(
        __import__("json").dumps(
            {
                "launches": [],
                "commands": [
                    {
                        "task_id": f"{run_id}-task",
                        "expected_output": f"edgecitadel:{run_id}",
                    }
                ],
            }
        )
        + "\n"
    )
    return LifecycleResult(
        run_id=run_id,
        project=f"edgecitadel-artifact-{run_id}",
        ports=(18080 + offset, 14222 + offset, 18222 + offset),
        subject_scope=frozenset(
            {(f"nats://127.0.0.1:{14222 + offset}", "agents.fixture-1.inbox")}
        ),
        consumer_names=frozenset({f"ec_task_{run_id}_fixture_1"}),
        state_paths=frozenset({tmp_path / run_id}),
        task_ids=(f"{run_id}-task",),
        terminal_outputs=(f"edgecitadel:{run_id}",),
        doctor_reports=(),
        bundle=bundle,
        cleanup={
            "completed": True,
            "owned_resources_removed": True,
            "remaining": [],
            "foreign_resources_touched": False,
        },
    )


def test_duplicate_handler_log_requires_one_authoritative_execution(
    tmp_path: Path,
) -> None:
    task_id = "10000000-0000-4000-8000-000000000001"
    log = tmp_path / "fixture.log"
    event = {"event": "fixture.handler_started", "data": {"task_id": task_id}}
    log.write_text(f"{__import__('json').dumps(event)}\n")
    assert require_single_handler_started(log, task_id) == event
    log.write_text(
        f"{__import__('json').dumps(event)}\n{__import__('json').dumps(event)}\n"
    )
    with pytest.raises(LabConfigError, match="handler"):
        require_single_handler_started(log, task_id)


def test_disjoint_assertion_covers_every_run_owned_namespace(tmp_path: Path) -> None:
    left = _result(tmp_path, "lab-left", 1)
    right = _result(tmp_path, "lab-right", 2)
    assert_disjoint_runs(left, right)
    with pytest.raises(LabConfigError, match="ports"):
        assert_disjoint_runs(left, _result(tmp_path, "lab-right", 1))
    evidence = __import__("json").loads(
        (right.bundle / "raw/lab/controller-commands.json").read_text()
    )
    evidence["commands"].append(
        {"task_id": left.task_ids[0], "expected_output": left.terminal_outputs[0]}
    )
    (right.bundle / "raw/lab/controller-commands.json").write_text(
        __import__("json").dumps(evidence) + "\n"
    )
    with pytest.raises(LabConfigError, match="command snapshot"):
        assert_disjoint_runs(left, right)


def test_concurrent_pair_starts_both_runs_and_repeats_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stopped: list[str] = []
    live_queries: list[str] = []

    def request(url: str):
        live_queries.append(url)
        return []

    def lifecycle(
        *, repo_root: Path, run_id: str, host_id: str, _pair_probe=None
    ) -> LifecycleResult:
        assert repo_root == tmp_path.resolve()
        assert host_id == "controller-lab-01"
        assert _pair_probe is not None
        _pair_probe.controller_started()
        result = _result(tmp_path, run_id, 1 if run_id.endswith("a") else 2)
        _pair_probe.verify_live_scope(
            run_id=run_id,
            agg_url=f"http://127.0.0.1:{result.ports[0]}",
            task_ids=result.task_ids,
            terminal_outputs=result.terminal_outputs,
        )
        return result

    monkeypatch.setattr(gate_module, "run_two_node_lifecycle", lifecycle)
    monkeypatch.setattr(gate_module, "_request_json", request)
    monkeypatch.setattr(
        gate_module,
        "_stop_run_if_present",
        lambda _root, run_id: stopped.append(run_id),
    )
    left, right = run_concurrent_pair(
        repo_root=tmp_path,
        run_ids=("pair-a", "pair-b"),
        host_id="controller-lab-01",
    )
    assert (left.run_id, right.run_id) == ("pair-a", "pair-b")
    assert stopped == ["pair-a", "pair-b"]
    assert sorted(live_queries) == [
        "http://127.0.0.1:18081/api/messages",
        "http://127.0.0.1:18082/api/messages",
    ]


def test_concurrent_pair_cleans_both_runs_when_one_worker_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stopped: list[str] = []

    def lifecycle(
        *, repo_root: Path, run_id: str, host_id: str, _pair_probe=None
    ) -> LifecycleResult:
        assert _pair_probe is not None
        if run_id == "pair-a":
            raise LabConfigError("injected lifecycle failure")
        _pair_probe.controller_started()
        return _result(tmp_path, run_id, 2)

    monkeypatch.setattr(gate_module, "run_two_node_lifecycle", lifecycle)
    monkeypatch.setattr(
        gate_module,
        "_stop_run_if_present",
        lambda _root, run_id: stopped.append(run_id),
    )
    with pytest.raises(ExceptionGroup, match="concurrent lab pair failed"):
        run_concurrent_pair(
            repo_root=tmp_path,
            run_ids=("pair-a", "pair-b"),
            host_id="controller-lab-01",
        )
    assert stopped == ["pair-a", "pair-b"]


def test_sequential_pair_checks_cleanup_before_the_second_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    timeline: list[str] = []

    def lifecycle(*, repo_root: Path, run_id: str, host_id: str) -> LifecycleResult:
        assert repo_root == tmp_path.resolve()
        assert host_id == "controller-lab-01"
        timeline.append(f"start:{run_id}")
        return _result(tmp_path, run_id, 1 if run_id.endswith("a") else 2)

    monkeypatch.setattr(gate_module, "run_two_node_lifecycle", lifecycle)
    monkeypatch.setattr(
        gate_module,
        "_assert_run_clean",
        lambda _root, result: timeline.append(f"clean:{result.run_id}"),
    )
    monkeypatch.setattr(gate_module, "_stop_run_if_present", lambda *_args: None)

    left, right = gate_module.run_sequential_pair(
        repo_root=tmp_path,
        run_ids=("pair-a", "pair-b"),
        host_id="controller-lab-01",
    )

    assert (left.run_id, right.run_id) == ("pair-a", "pair-b")
    assert timeline == [
        "start:pair-a",
        "clean:pair-a",
        "start:pair-b",
        "clean:pair-b",
    ]


def test_clean_checkout_receipt_is_relative_and_uses_finalized_bundles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "edge-research"
    repo_root.mkdir()
    life = _result(repo_root, "lab-clean-life", 1)
    life.bundle.mkdir(parents=True, exist_ok=True)
    (life.bundle / "manifest.json").write_text("{}\n")
    operator_bundle = repo_root / "data/research/results/lab/lab-clean-ui"
    operator_bundle.mkdir(parents=True)
    (operator_bundle / "manifest.json").write_text("{}\n")
    receipt = tmp_path / "receipt.json"

    monkeypatch.setattr(
        gate_module,
        "capture_clean_source_provenance",
        lambda _root: SourceProvenance("a" * 40, False, "b" * 64, "c" * 64),
    )
    monkeypatch.setattr(gate_module, "run_two_node_lifecycle", lambda **_kwargs: life)
    monkeypatch.setattr(
        gate_module,
        "run_operator_journey",
        lambda **_kwargs: subprocess.CompletedProcess([], 0, "1 passed", ""),
    )
    monkeypatch.setattr(
        gate_module, "check_bundle", lambda *_args, **_kwargs: CheckReport(True, ())
    )
    monkeypatch.setattr(gate_module, "_finalizer_count", lambda *_args: 1)
    monkeypatch.setattr(
        gate_module,
        "_cleanup_for_run",
        lambda *_args: {
            "completed": True,
            "owned_resources_removed": True,
            "foreign_resources_touched": False,
            "remaining": [],
        },
    )

    value = run_clean_checkout_gate(
        repo_root=repo_root,
        run_id="lab-clean",
        host_id="controller-lab-01",
        receipt=receipt,
    )

    assert value == __import__("json").loads(receipt.read_text())
    assert value["source_commit"] == "a" * 40
    assert set(value["bundles"]) == {"lifecycle", "operator_smoke"}
    for entry in value["bundles"].values():
        assert not Path(entry["path"]).is_absolute()
        assert entry["checker_valid"] is True
        assert entry["finalizer_count"] == 1
    assert value["cleanup"] == {"complete": True, "owned_resources_remaining": []}


@pytest.mark.lab_integration
@pytest.mark.skipif(platform.system() != "Linux", reason="lab requires Ubuntu/Linux")
def test_two_node_command_disconnect_queue_reconnect_and_duplicate_rejection() -> None:
    from scripts.research.lab_gate import run_two_node_lifecycle

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


@pytest.mark.lab_integration
@pytest.mark.skipif(platform.system() != "Linux", reason="lab requires Ubuntu/Linux")
def test_exact_slice2_operator_journey_targets_shell_1_once() -> None:
    from scripts.research.lab_gate import run_operator_journey

    completed = run_operator_journey(
        repo_root=REPO_ROOT,
        run_id=f"lab-ui-{uuid.uuid4().hex[:8]}",
        host_id="controller-lab-01",
    )
    assert completed.returncode == 0
    assert "1 passed" in completed.stdout


@pytest.mark.lab_integration
@pytest.mark.skipif(platform.system() != "Linux", reason="lab requires Ubuntu/Linux")
def test_two_concurrent_full_lifecycles_are_disjoint() -> None:
    left, right = gate_module.run_concurrent_pair(
        repo_root=REPO_ROOT,
        run_ids=(
            f"lab-concurrent-a-{uuid.uuid4().hex[:8]}",
            f"lab-concurrent-b-{uuid.uuid4().hex[:8]}",
        ),
        host_id="controller-lab-01",
    )
    assert_disjoint_runs(left, right)


@pytest.mark.lab_integration
@pytest.mark.skipif(platform.system() != "Linux", reason="lab requires Ubuntu/Linux")
def test_two_sequential_full_lifecycles_are_disjoint() -> None:
    left, right = gate_module.run_sequential_pair(
        repo_root=REPO_ROOT,
        run_ids=(
            f"lab-sequential-a-{uuid.uuid4().hex[:8]}",
            f"lab-sequential-b-{uuid.uuid4().hex[:8]}",
        ),
        host_id="controller-lab-01",
    )
    assert_disjoint_runs(left, right)


@pytest.mark.lab_integration
@pytest.mark.skipif(platform.system() != "Linux", reason="lab requires Ubuntu/Linux")
def test_repeated_cleanup_preserves_foreign_resource_and_secret_hygiene(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = f"lab-cleanup-{uuid.uuid4().hex[:8]}"
    foreign = f"edgecitadel-foreign-{uuid.uuid4().hex[:8]}"
    token: list[str] = []
    active_surfaces: list[str] = []
    original_stop = gate_module._node_stop

    def capture_token(*args, **kwargs):
        credential = Path(args[2])
        if credential.is_file() and not token:
            token.append(credential.read_text().strip())
        agent_id = str(args[3])
        state_file = gate_module._node_state_file(run_id, agent_id)
        if state_file.is_file():
            node_state = __import__("json").loads(state_file.read_text())
            active_surfaces.append(state_file.read_text())
            log_path = Path(node_state["log_path"])
            if log_path.is_file():
                active_surfaces.append(log_path.read_text(errors="ignore"))
            container_name = node_state.get("container_name")
            if container_name:
                inspected = subprocess.run(
                    ["docker", "inspect", str(container_name)],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                active_surfaces.append(inspected.stdout)
        return original_stop(*args, **kwargs)

    subprocess.run(
        [
            "docker",
            "volume",
            "create",
            "--label",
            "ai.edgecitadel.owner=foreign-control",
            foreign,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    monkeypatch.setattr(gate_module, "_node_stop", capture_token)
    try:
        result = gate_module.run_two_node_lifecycle(
            repo_root=REPO_ROOT,
            run_id=run_id,
            host_id="controller-lab-01",
        )
        state_file = REPO_ROOT / "tmp/research/lab" / run_id / "controller-state.json"
        for _ in range(2):
            stopped = subprocess.run(
                [
                    str(REPO_ROOT / "scripts/research/run-python"),
                    str(REPO_ROOT / "scripts/research/lab_controller.py"),
                    "stop",
                    "--state-file",
                    str(state_file),
                ],
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            assert stopped.returncode == 0
        assert (
            subprocess.run(
                ["docker", "volume", "inspect", foreign],
                check=False,
                capture_output=True,
                text=True,
            ).returncode
            == 0
        )
        assert token and token[0]
        assert active_surfaces
        assert all(token[0] not in value for value in active_surfaces)
        existing = [
            path for path in (result.bundle, state_file.parent) if path.exists()
        ]
        for root in existing:
            for path in root.rglob("*"):
                if path.is_file():
                    assert token[0] not in path.read_text(errors="ignore")
    finally:
        subprocess.run(
            ["docker", "volume", "rm", "--force", foreign],
            check=False,
            capture_output=True,
            text=True,
        )
