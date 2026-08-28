"""Maintained-CLI gates for the two-node lab and exact operator journey."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from scripts.research.capture_operator_journey import copy_media, passed_project_results
from scripts.research.check_artifact import check_bundle
from scripts.research.evidence import write_json
from scripts.research.lab_config import LabConfigError, validate_run_id
from scripts.research.lab_runtime import capture_clean_source_provenance
from scripts.research.modes.jetstream_config import durable_name


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


class _PairProbe:
    def __init__(self) -> None:
        self._started = threading.Barrier(2)
        self._observed = threading.Barrier(2)
        self._lock = threading.Lock()
        self._scopes: dict[str, tuple[str, tuple[str, ...], tuple[str, ...]]] = {}

    @staticmethod
    def _wait(barrier: threading.Barrier) -> None:
        try:
            barrier.wait(timeout=180)
        except threading.BrokenBarrierError:
            raise LabConfigError("concurrent lifecycle synchronization failed") from None

    def controller_started(self) -> None:
        self._wait(self._started)

    def verify_live_scope(
        self,
        *,
        run_id: str,
        agg_url: str,
        task_ids: tuple[str, ...],
        terminal_outputs: tuple[str, ...],
    ) -> None:
        with self._lock:
            self._scopes[run_id] = (agg_url, task_ids, terminal_outputs)
        self._wait(self._observed)
        with self._lock:
            others = [value for key, value in self._scopes.items() if key != run_id]
        if len(others) != 1:
            raise LabConfigError("concurrent peer scope is unavailable")
        other_task_ids = set(others[0][1])
        other_outputs = set(others[0][2])
        messages = _request_json(f"{agg_url}/api/messages")
        if not isinstance(messages, list):
            raise LabConfigError("concurrent controller message snapshot is invalid")
        contaminated = any(
            isinstance(item, Mapping)
            and (
                item.get("task_id") in other_task_ids
                or (
                    isinstance(item.get("payload"), Mapping)
                    and item["payload"].get("body") in other_outputs
                )
            )
            for item in messages
        )
        if contaminated:
            raise LabConfigError("concurrent controller contains peer observations")

    def abort(self) -> None:
        self._started.abort()
        self._observed.abort()


def _run(
    argv: list[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        env=dict(env) if env is not None else None,
        check=check,
        text=True,
        capture_output=True,
    )


def _python_argv(repo_root: Path, script: str, *arguments: object) -> list[str]:
    return [
        str((repo_root / "scripts/research/run-python").resolve()),
        str((repo_root / script).resolve()),
        *(str(item) for item in arguments),
    ]


def _load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError(f"required JSON is unavailable: {path.name}") from error
    if not isinstance(value, dict):
        raise LabConfigError(f"required JSON is invalid: {path.name}")
    return value


def _controller_paths(repo_root: Path, run_id: str) -> tuple[Path, Path]:
    state_dir = repo_root / "tmp/research/lab" / run_id
    return state_dir / "controller-state.json", state_dir / "controller.json"


def _config_path(value: object, label: str) -> Path:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise LabConfigError(f"controller {label} is invalid")
    return Path(value)


def _request_json(url: str) -> object:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            if response.status != 200:
                raise LabConfigError(f"unexpected HTTP status {response.status}")
            return json.loads(response.read())
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError("lab API response is unavailable") from error


def _wait_online(agg_url: str, expected: frozenset[str]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        agents = _request_json(f"{agg_url}/api/agents")
        online = {
            str(item["agent_id"])
            for item in agents
            if isinstance(agents, list) and isinstance(item, Mapping)
            and item.get("agent_state") == "online" and isinstance(item.get("agent_id"), str)
        } if isinstance(agents, list) else set()
        if expected.issubset(online):
            return
        time.sleep(0.25)
    raise LabConfigError("lab nodes did not become simultaneously online")


def _command_argv(
    repo_root: Path,
    run_id: str,
    agent_id: str,
    body: str,
    expected_output: str,
    result_file: Path,
    *,
    wait: bool,
    wire_copies: int = 1,
) -> list[str]:
    return _python_argv(
        repo_root,
        "scripts/research/lab_controller.py",
        "command", "--run-id", run_id,
        "--agent-id", agent_id,
        "--body", body,
        "--expected-output", expected_output,
        "--wait" if wait else "--no-wait",
        "--wire-copies", wire_copies,
        "--result-file", result_file,
    )


def _node_argv(
    repo_root: Path,
    command: str,
    controller_config: Path,
    credential: Path,
    agent_id: str,
    *extra: object,
) -> list[str]:
    return _python_argv(
        repo_root,
        "scripts/research/lab_node.py",
        command,
        "--controller-config", controller_config,
        "--credential-file", credential,
        "--agent-id", agent_id,
        *extra,
    )


def _node_start(
    repo_root: Path,
    controller_config: Path,
    credential: Path,
    host_id: str,
    agent_id: str,
    *,
    delay_ms: int,
    state_root: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    extra: list[object] = [
        "--host-id", host_id,
        "--behavior", "echo",
        "--delay-ms", delay_ms,
    ]
    if state_root is not None:
        extra.extend(("--state-root", state_root))
    return _run(
        _node_argv(repo_root, "start", controller_config, credential, agent_id, *extra),
        cwd=repo_root,
        check=check,
    )


def _node_stop(
    repo_root: Path,
    controller_config: Path,
    credential: Path,
    agent_id: str,
    *,
    retain: bool = False,
) -> subprocess.CompletedProcess[str]:
    extra: tuple[object, ...] = ("--retain-reservation",) if retain else ()
    return _run(
        _node_argv(repo_root, "stop", controller_config, credential, agent_id, *extra),
        cwd=repo_root,
        check=False,
    )


def _doctor(
    repo_root: Path,
    controller_config: Path,
    credential: Path,
    host_id: str,
    agent_id: str,
) -> Mapping[str, object]:
    completed = _run(
        _node_argv(
            repo_root, "doctor", controller_config, credential, agent_id,
            "--host-id", host_id, "--publish",
        ),
        cwd=repo_root,
    )
    try:
        value = json.loads(completed.stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as error:
        raise LabConfigError("node doctor output is invalid") from error
    if (
        not isinstance(value, Mapping)
        or value.get("agent_id") != agent_id
        or value.get("declared_host_id") != host_id
        or not isinstance(value.get("reservation_id"), str)
    ):
        raise LabConfigError("node doctor reservation binding is invalid")
    return value


def _container_count(run_id: str, repo_root: Path) -> int:
    completed = _run(
        [
            "docker", "ps", "--all", "--quiet",
            "--filter", f"label=ai.edgecitadel.run-id={run_id}",
            "--filter", "label=ai.edgecitadel.owner=research-lab-node",
        ],
        cwd=repo_root,
    )
    return len([line for line in completed.stdout.splitlines() if line])


def require_single_handler_started(
    log_path: Path, task_id: str
) -> Mapping[str, object]:
    """Return the sole authoritative fixture handler event for a task."""
    try:
        records = [
            json.loads(line) for line in log_path.read_text().splitlines() if line
        ]
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError("fixture handler log is invalid") from error
    matches = [
        item for item in records
        if isinstance(item, Mapping)
        and item.get("event") == "fixture.handler_started"
        and isinstance(item.get("data"), Mapping)
        and item["data"].get("task_id") == task_id
    ]
    if len(matches) != 1:
        raise LabConfigError("duplicate command handler execution is invalid")
    return matches[0]


def _actual_terminal_output(agg_url: str, task_id: str) -> str:
    messages = _request_json(f"{agg_url}/api/messages?task_id={task_id}")
    terminals = [
        item for item in messages
        if isinstance(messages, list) and isinstance(item, Mapping)
        and item.get("type") == "result"
        and item.get("task_state") == "completed"
        and item.get("task_id") == task_id
        and isinstance(item.get("payload"), Mapping)
        and isinstance(item["payload"].get("body"), str)
    ] if isinstance(messages, list) else []
    logical = {str(item["payload"]["body"]) for item in terminals}
    if len(logical) != 1:
        raise LabConfigError("actual task terminal output is not unique")
    return logical.pop()


def _node_state_file(run_id: str, agent_id: str) -> Path:
    return (
        Path("/tmp/edgecitadel-lab-node") / run_id
        / f"{run_id}--{agent_id}" / "node-state.json"
    )


def _live_task_consumers(
    monitor_url: str, run_id: str, agent_ids: tuple[str, ...]
) -> tuple[frozenset[str], frozenset[str]]:
    snapshot = _request_json(
        f"{monitor_url.removesuffix('/')}/jsz?consumers=true&config=true"
    )
    accounts = snapshot.get("account_details") if isinstance(snapshot, Mapping) else None
    if not isinstance(accounts, list):
        raise LabConfigError("live consumer snapshot is invalid")
    bindings: set[tuple[str, str]] = set()
    found_agent_inbox = False
    for account in accounts:
        streams = account.get("stream_detail") if isinstance(account, Mapping) else None
        if not isinstance(streams, list):
            raise LabConfigError("live consumer snapshot is invalid")
        for stream in streams:
            if not isinstance(stream, Mapping) or stream.get("name") != "AGENT_INBOX":
                continue
            found_agent_inbox = True
            consumers = stream.get("consumer_detail")
            if not isinstance(consumers, list):
                raise LabConfigError("live consumer snapshot is invalid")
            for consumer in consumers:
                config = consumer.get("config") if isinstance(consumer, Mapping) else None
                name = consumer.get("name") if isinstance(consumer, Mapping) else None
                subject = config.get("filter_subject") if isinstance(config, Mapping) else None
                configured_name = config.get("durable_name") if isinstance(config, Mapping) else None
                if (
                    not isinstance(name, str)
                    or not name
                    or not isinstance(subject, str)
                    or not subject
                    or (configured_name is not None and configured_name != name)
                ):
                    raise LabConfigError("live consumer snapshot is invalid")
                bindings.add((name, subject))
    expected = {
        (durable_name("task", run_id, agent_id), f"agents.{agent_id}.inbox")
        for agent_id in agent_ids
    }
    if not found_agent_inbox or not expected.issubset(bindings):
        raise LabConfigError("live task consumer bindings are incomplete")
    return (
        frozenset(name for name, _ in bindings & expected),
        frozenset(subject for _, subject in bindings & expected),
    )


def run_two_node_lifecycle(
    *,
    repo_root: Path,
    run_id: str,
    host_id: str,
    _pair_probe: _PairProbe | None = None,
) -> LifecycleResult:
    """Exercise two active nodes, replay, retained queueing, and cleanup."""
    repo_root = repo_root.resolve()
    state_file, controller_config = _controller_paths(repo_root, run_id)
    controller_started = False
    started_nodes: set[str] = set()
    cleanup: Mapping[str, object] = {}
    doctor_reports: list[Mapping[str, object]] = []
    task_ids: list[str] = []
    outputs: list[str] = []
    observed_state_paths: set[Path] = {state_file}
    consumer_names: frozenset[str] = frozenset()
    filter_subjects: frozenset[str] = frozenset()
    contender_root: Path | None = None
    config: dict[str, object] = {}
    try:
        _run(_python_argv(
            repo_root, "scripts/research/lab_controller.py", "start",
            "--run-id", run_id, "--host-id", host_id,
            "--lab-variant", "lifecycle",
        ), cwd=repo_root)
        controller_started = True
        config = _load_json(controller_config)
        if _pair_probe is not None:
            _pair_probe.controller_started()
        credential = _config_path(config.get("credential_file"), "credential file")
        for agent_id in ("fixture-1", "fixture-2"):
            _node_start(
                repo_root, controller_config, credential, host_id, agent_id,
                delay_ms=250,
            )
            started_nodes.add(agent_id)
        _wait_online(str(config["agg_url"]), frozenset(started_nodes))
        doctor_reports.extend(
            _doctor(repo_root, controller_config, credential, host_id, agent_id)
            for agent_id in ("fixture-1", "fixture-2")
        )

        bodies = (
            ("fixture-1", f"{run_id}:fixture-1", 1),
            ("fixture-2", f"{run_id}:fixture-2", 1),
            ("fixture-2", f"{run_id}:duplicate-fixture-2", 2),
        )
        command_root = _config_path(config.get("evidence_dir"), "evidence directory") / "raw/lab/commands"
        for index, (agent_id, body, wire_copies) in enumerate(bodies, start=1):
            result_file = command_root / f"command-{index}.json"
            _run(_command_argv(
                repo_root, run_id, agent_id, body, f"edgecitadel:{body}",
                result_file, wait=True, wire_copies=wire_copies,
            ), cwd=repo_root)
            result = _load_json(result_file)
            task_ids.append(str(result["task_id"]))
            outputs.append(_actual_terminal_output(
                str(config["agg_url"]), str(result["task_id"])
            ))
            node_state = _load_json(_node_state_file(run_id, agent_id))
            observed_state_paths.update(
                Path(str(node_state[name]))
                for name in ("state_dir", "config_path", "log_path")
            )
            if wire_copies == 2:
                require_single_handler_started(
                    Path(str(node_state["log_path"])), str(result["task_id"])
                )

        before = _container_count(run_id, repo_root)
        contender_root = Path(tempfile.mkdtemp(prefix=f"{run_id}-contender-"))
        contender = _node_start(
            repo_root, controller_config, credential, "contender-lab-02", "fixture-2",
            delay_ms=250, state_root=contender_root, check=False,
        )
        if (
            contender.returncode == 0
            or "agent_id has an active reservation" not in contender.stderr
        ):
            raise LabConfigError("duplicate reservation was not rejected by inventory")
        if _container_count(run_id, repo_root) != before:
            raise LabConfigError("duplicate reservation created a container")

        retained = _node_stop(
            repo_root, controller_config, credential, "fixture-1", retain=True,
        )
        if retained.returncode != 0:
            raise LabConfigError("fixture-1 reservation was not retained")
        started_nodes.discard("fixture-1")
        queued_body = f"{run_id}:queued-fixture-1"
        accepted_file = command_root / "command-4-accepted.json"
        _run(_command_argv(
            repo_root, run_id, "fixture-1", queued_body,
            f"edgecitadel:{queued_body}", accepted_file, wait=False,
        ), cwd=repo_root)
        accepted = _load_json(accepted_file)
        _node_start(
            repo_root, controller_config, credential, host_id, "fixture-1",
            delay_ms=250,
        )
        started_nodes.add("fixture-1")
        doctor_reports.append(_doctor(
            repo_root, controller_config, credential, host_id, "fixture-1"
        ))
        completed_file = command_root / "command-4-completed.json"
        _run(_python_argv(
            repo_root, "scripts/research/lab_controller.py", "await",
            "--run-id", run_id, "--task-id", accepted["task_id"],
            "--expected-output", f"edgecitadel:{queued_body}",
            "--qualification-kind", "queued-reconnect",
            "--result-file", completed_file,
        ), cwd=repo_root)
        completed = _load_json(completed_file)
        task_ids.append(str(completed["task_id"]))
        outputs.append(_actual_terminal_output(
            str(config["agg_url"]), str(completed["task_id"])
        ))
        resumed_state = _load_json(_node_state_file(run_id, "fixture-1"))
        observed_state_paths.update(
            Path(str(resumed_state[name]))
            for name in ("state_dir", "config_path", "log_path")
        )
        consumer_names, filter_subjects = _live_task_consumers(
            str(config["monitor_url"]), run_id, ("fixture-1", "fixture-2")
        )
        if _pair_probe is not None:
            _pair_probe.verify_live_scope(
                run_id=run_id,
                agg_url=str(config["agg_url"]),
                task_ids=tuple(task_ids),
                terminal_outputs=tuple(outputs),
            )
    finally:
        primary = sys.exc_info()[1]
        if primary is not None and _pair_probe is not None:
            _pair_probe.abort()
        cleanup_errors: list[Exception] = []
        if controller_started:
            credential: Path | None = None
            if controller_config.is_file():
                try:
                    current = _load_json(controller_config)
                    credential_value = current.get("credential_file")
                    if not isinstance(credential_value, str):
                        raise LabConfigError("controller credential path is invalid")
                    credential = Path(credential_value)
                except Exception as error:
                    cleanup_errors.append(error)
            else:
                cleanup_errors.append(
                    LabConfigError("controller config disappeared before node cleanup")
                )
            if credential is not None:
                for agent_id in ("fixture-1", "fixture-2"):
                    try:
                        receipt = _node_stop(
                            repo_root, controller_config, credential, agent_id
                        )
                        if (
                            receipt.returncode != 0
                            or not receipt.stdout.startswith("node:")
                        ):
                            raise LabConfigError(
                                f"{agent_id} cleanup failed: {receipt.stderr.strip()}"
                            )
                    except Exception as error:
                        cleanup_errors.append(error)
            try:
                if _container_count(run_id, repo_root) != 0:
                    raise LabConfigError("run-owned node containers remain")
            except Exception as error:
                cleanup_errors.append(error)
            if state_file.is_file():
                try:
                    stopped = _run(_python_argv(
                        repo_root, "scripts/research/lab_controller.py", "stop",
                        "--state-file", state_file,
                    ), cwd=repo_root, check=False)
                    if stopped.returncode != 0:
                        raise LabConfigError(
                            f"controller stop failed: {stopped.stderr.strip()}"
                        )
                    if not stopped.stdout.strip():
                        raise LabConfigError("controller stop returned no cleanup receipt")
                    cleanup_value = json.loads(stopped.stdout.splitlines()[-1])
                    if not isinstance(cleanup_value, Mapping):
                        raise LabConfigError("controller cleanup receipt is invalid")
                    cleanup = cleanup_value
                except Exception as error:
                    cleanup_errors.append(error)
            else:
                cleanup_errors.append(LabConfigError("controller state disappeared before cleanup"))
        if contender_root is not None:
            shutil.rmtree(contender_root, ignore_errors=True)
        if cleanup_errors:
            if isinstance(primary, Exception):
                cleanup_errors.insert(0, primary)
            raise ExceptionGroup("lab lifecycle and cleanup failed", cleanup_errors) from None

    if not config:
        raise LabConfigError("controller did not produce configuration")
    bundle = _config_path(config.get("evidence_dir"), "evidence directory")
    required = (
        "preflight.json",
        "compose.resolved.yml",
        "versions.json",
        "images.json",
        "identities.json",
        "network-paths.json",
        "commands.json",
        "inventory.json",
        "cleanup.json",
    )
    missing = [name for name in required if not (bundle / name).is_file()]
    if missing:
        raise LabConfigError(
            f"lifecycle evidence surface is incomplete: {', '.join(missing)}"
        )
    check_bundle(bundle, expected_kind="lab", source_root=repo_root).require_valid()
    ports = tuple(
        int(str(config[name]).rsplit(":", 1)[-1])
        for name in ("app_url", "nats_url", "monitor_url")
    )
    subject_scope = frozenset(
        (str(config["nats_url"]), subject) for subject in filter_subjects
    )
    state_paths = frozenset(observed_state_paths)
    return LifecycleResult(
        run_id=run_id,
        project=str(config["compose_project"]),
        ports=(ports[0], ports[1], ports[2]),
        subject_scope=subject_scope,
        consumer_names=consumer_names,
        state_paths=state_paths,
        task_ids=tuple(task_ids),
        terminal_outputs=tuple(outputs),
        doctor_reports=tuple(doctor_reports),
        bundle=bundle,
        cleanup=cleanup,
    )


def assert_disjoint_runs(left: LifecycleResult, right: LifecycleResult) -> None:
    """Require every run-owned namespace and observation to be disjoint."""
    for result in (left, right):
        evidence = _load_json(
            result.bundle / "raw/lab/controller-commands.json"
        )
        commands = evidence.get("commands")
        recorded = [
            item for item in commands if isinstance(item, Mapping)
        ] if isinstance(commands, list) else []
        if (
            len(recorded) != len(result.task_ids)
            or {item.get("task_id") for item in recorded} != set(result.task_ids)
            or {item.get("expected_output") for item in recorded}
            != set(result.terminal_outputs)
        ):
            raise LabConfigError(
                f"{result.run_id} controller command snapshot is contaminated"
            )
    checks = {
        "projects": left.project != right.project,
        "ports": set(left.ports).isdisjoint(right.ports),
        "subjects": left.subject_scope.isdisjoint(right.subject_scope),
        "consumers": left.consumer_names.isdisjoint(right.consumer_names),
        "state paths": left.state_paths.isdisjoint(right.state_paths),
        "task IDs": set(left.task_ids).isdisjoint(right.task_ids),
        "terminal outputs": set(left.terminal_outputs).isdisjoint(
            right.terminal_outputs
        ),
    }
    failed = [name for name, valid in checks.items() if not valid]
    if failed:
        raise LabConfigError(f"paired runs share {', '.join(failed)}")
    for result in (left, right):
        if (
            result.cleanup.get("completed") is not True
            or result.cleanup.get("owned_resources_removed") is not True
            or result.cleanup.get("remaining") != []
            or result.cleanup.get("foreign_resources_touched") is not False
        ):
            raise LabConfigError(f"{result.run_id} cleanup is incomplete")


def _stop_run_if_present(repo_root: Path, run_id: str) -> None:
    state_file, _ = _controller_paths(repo_root, run_id)
    if not state_file.is_file():
        return
    completed = _run(
        _python_argv(
            repo_root,
            "scripts/research/lab_controller.py",
            "stop",
            "--state-file",
            state_file,
        ),
        cwd=repo_root,
        check=False,
    )
    if completed.returncode != 0:
        raise LabConfigError(
            f"{run_id} repeated cleanup failed: {completed.stderr.strip()}"
        )


def _owned_docker_resources(repo_root: Path, run_id: str) -> tuple[str, ...]:
    commands = (
        ("container", ["docker", "ps", "--all", "--quiet"]),
        ("network", ["docker", "network", "ls", "--quiet"]),
        ("volume", ["docker", "volume", "ls", "--quiet"]),
        ("image", ["docker", "image", "ls", "--quiet"]),
    )
    remaining: list[str] = []
    for kind, argv in commands:
        completed = _run(
            [*argv, "--filter", f"label=ai.edgecitadel.run-id={run_id}"],
            cwd=repo_root,
        )
        remaining.extend(
            f"{kind}:{value}"
            for value in completed.stdout.splitlines()
            if value
        )
    return tuple(remaining)


def _assert_run_clean(repo_root: Path, result: LifecycleResult) -> None:
    if (
        result.cleanup.get("completed") is not True
        or result.cleanup.get("owned_resources_removed") is not True
        or result.cleanup.get("remaining") != []
        or result.cleanup.get("foreign_resources_touched") is not False
    ):
        raise LabConfigError(f"{result.run_id} cleanup is incomplete")
    if _container_count(result.run_id, repo_root) != 0:
        raise LabConfigError(f"{result.run_id} node containers remain")
    remaining = _owned_docker_resources(repo_root, result.run_id)
    if remaining:
        raise LabConfigError(
            f"{result.run_id} Docker resources remain: {', '.join(remaining)}"
        )
    state_file, _ = _controller_paths(repo_root, result.run_id)
    state = _load_json(state_file)
    steps = state.get("completed_cleanup_steps")
    if (
        state.get("phase") != "stopped"
        or not isinstance(steps, list)
        or steps.count("manifest-finalized") != 1
    ):
        raise LabConfigError(f"{result.run_id} controller is not cleanly stopped")
    private_root = Path("/tmp/edgecitadel-lab-node") / result.run_id
    if private_root.exists() and any(private_root.rglob("*")):
        raise LabConfigError(f"{result.run_id} private node state remains")


def _repeat_pair_cleanup(repo_root: Path, run_ids: tuple[str, str]) -> None:
    errors: list[Exception] = []
    for run_id in run_ids:
        try:
            _stop_run_if_present(repo_root, run_id)
        except Exception as error:
            errors.append(error)
    if errors:
        raise ExceptionGroup("paired run cleanup failed", errors)


def run_concurrent_pair(
    *, repo_root: Path, run_ids: tuple[str, str], host_id: str
) -> tuple[LifecycleResult, LifecycleResult]:
    """Execute exactly two complete lifecycles concurrently."""
    root = repo_root.resolve()
    if len(run_ids) != 2 or run_ids[0] == run_ids[1]:
        raise LabConfigError("paired run IDs must be distinct")
    results: list[LifecycleResult | None] = [None, None]
    errors: list[Exception] = []
    pair_probe = _PairProbe()
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    run_two_node_lifecycle,
                    repo_root=root,
                    run_id=run_id,
                    host_id=host_id,
                    _pair_probe=pair_probe,
                )
                for run_id in run_ids
            ]
            for future in futures:
                future.add_done_callback(
                    lambda completed: pair_probe.abort()
                    if not completed.cancelled() and completed.exception() is not None
                    else None
                )
            for index, future in enumerate(futures):
                try:
                    results[index] = future.result()
                except Exception as error:
                    errors.append(error)
    finally:
        try:
            _repeat_pair_cleanup(root, run_ids)
        except Exception as error:
            errors.append(error)
    if errors:
        raise ExceptionGroup("concurrent lab pair failed", errors)
    left, right = results
    if left is None or right is None:
        raise LabConfigError("concurrent lab pair returned no result")
    assert_disjoint_runs(left, right)
    return left, right


def run_sequential_pair(
    *, repo_root: Path, run_ids: tuple[str, str], host_id: str
) -> tuple[LifecycleResult, LifecycleResult]:
    """Execute two complete lifecycles with a cleanup gate between starts."""
    root = repo_root.resolve()
    if len(run_ids) != 2 or run_ids[0] == run_ids[1]:
        raise LabConfigError("paired run IDs must be distinct")
    results: list[LifecycleResult] = []
    primary: Exception | None = None
    try:
        first = run_two_node_lifecycle(
            repo_root=root, run_id=run_ids[0], host_id=host_id
        )
        results.append(first)
        _assert_run_clean(root, first)
        second = run_two_node_lifecycle(
            repo_root=root, run_id=run_ids[1], host_id=host_id
        )
        results.append(second)
        _assert_run_clean(root, second)
    except Exception as error:
        primary = error
    try:
        _repeat_pair_cleanup(root, run_ids)
    except Exception as error:
        if primary is None:
            primary = error
        else:
            primary = ExceptionGroup("sequential lab pair failed", [primary, error])
    if primary is not None:
        raise primary
    if len(results) != 2:
        raise LabConfigError("sequential lab pair returned no result")
    left, right = results
    assert_disjoint_runs(left, right)
    return left, right


def _operator_pair(
    messages: object, *, metadata: Mapping[str, object] | None = None
) -> tuple[Mapping[str, object], Mapping[str, object]]:
    error = "operator command/terminal evidence is invalid"
    if not isinstance(messages, list) or any(not isinstance(item, Mapping) for item in messages):
        raise LabConfigError(error)
    commands = [item for item in messages if item.get("type") == "command"]
    terminals = [
        item for item in messages
        if item.get("type") == "result"
        and item.get("task_state") in {"completed", "failed", "canceled", "rejected"}
    ]
    if len(commands) != 1 or len(terminals) != 1:
        raise LabConfigError(error)
    command = commands[0]
    terminal = terminals[0]
    command_payload = command.get("payload")
    terminal_payload = terminal.get("payload")
    task_id = command.get("task_id")
    context_id = command.get("context_id")
    nonce = command_payload.get("body") if isinstance(command_payload, Mapping) else None
    if (
        not isinstance(command.get("id"), str)
        or not command["id"]
        or command.get("sender_id") != "aggregator"
        or command.get("recipient_id") != "shell-1"
        or not isinstance(task_id, str)
        or not task_id
        or not isinstance(context_id, str)
        or not context_id
        or type(command.get("hop_count")) is not int
        or command.get("hop_count") != 0
        or not isinstance(command_payload, Mapping)
        or not isinstance(nonce, str)
        or not nonce
        or dict(command_payload) != {"body": nonce}
        or not isinstance(terminal.get("id"), str)
        or not terminal["id"]
        or terminal.get("sender_id") != "shell-1"
        or terminal.get("recipient_id") != "aggregator"
        or terminal.get("task_id") != task_id
        or terminal.get("context_id") != context_id
        or type(terminal.get("hop_count")) is not int
        or terminal.get("hop_count") != 0
        or terminal.get("task_state") != "completed"
        or not isinstance(terminal_payload, Mapping)
        or dict(terminal_payload) != {"body": f"edgecitadel:{nonce}"}
    ):
        raise LabConfigError(error)
    for item in messages:
        if item.get("task_id") != task_id:
            raise LabConfigError(error)
        if item.get("type") == "task.progress" and (
            item.get("sender_id") != "shell-1"
            or item.get("recipient_id") != "aggregator"
            or item.get("context_id") != context_id
            or type(item.get("hop_count")) is not int
            or item.get("hop_count") != 0
        ):
            raise LabConfigError(error)
    if metadata is not None:
        expected = {
            "task_id": task_id,
            "nonce": nonce,
            "command_body": nonce,
            "expected_output": f"edgecitadel:{nonce}",
            "context_id": context_id,
            "hop_count": 0,
            "command_envelope_id": command["id"],
            "terminal_envelope_id": terminal["id"],
            "command_sender_id": "aggregator",
            "command_recipient_id": "shell-1",
            "terminal_sender_id": "shell-1",
            "terminal_recipient_id": "aggregator",
        }
        if any(metadata.get(name) != value for name, value in expected.items()):
            raise LabConfigError(error)
    return command, terminal


def _safe_artifact_file(bundle: Path, relative: Path) -> bool:
    if relative.is_absolute() or ".." in relative.parts:
        return False
    current = bundle
    try:
        for part in relative.parts[:-1]:
            current = current / part
            mode = current.lstat().st_mode
            if current.is_symlink() or not stat.S_ISDIR(mode):
                return False
        path = bundle / relative
        mode = path.lstat().st_mode
    except OSError:
        return False
    return not path.is_symlink() and stat.S_ISREG(mode)


def _validate_portable_media(bundle: Path, portable: Mapping[str, object]) -> None:
    project_values = portable.get("projects")
    if portable.get("schema_version") != "playwright-operator-results.v1" or not isinstance(project_values, Mapping) or set(project_values) != {"desktop", "mobile"}:
        raise LabConfigError("portable Playwright report is invalid")
    expected_attachments = {
        "chat": ("chat.png", "image/png"),
        "tasks": ("tasks.png", "image/png"),
        "operator-metadata": ("operator-metadata.json", "application/json"),
        "video": ("video.webm", "video/webm"),
        "trace": ("trace.zip", "application/zip"),
    }
    task_ids: list[str] = []
    for project in ("desktop", "mobile"):
        value = project_values.get(project)
        attachments = value.get("attachments") if isinstance(value, Mapping) else None
        if (
            not isinstance(attachments, list)
            or len(attachments) != 5
            or {item.get("name") for item in attachments if isinstance(item, Mapping)}
            != set(expected_attachments)
        ):
            raise LabConfigError(f"{project} portable attachments are invalid")
        for item in attachments:
            if not isinstance(item, Mapping) or not isinstance(item.get("name"), str):
                raise LabConfigError(f"{project} portable attachments are invalid")
            filename, content_type = expected_attachments[str(item["name"])]
            if (
                item.get("path") != f"raw/playwright/{project}/{filename}"
                or item.get("content_type") != content_type
            ):
                raise LabConfigError(f"{project} portable attachment path is invalid")
        media_root = Path("raw/playwright") / project
        api_root_relative = Path("raw/api") / project
        try:
            media_files = {item.name for item in (bundle / media_root).iterdir()}
            api_files = {item.name for item in (bundle / api_root_relative).iterdir()}
        except OSError as error:
            raise LabConfigError(f"{project} portable attachments are invalid") from error
        expected_media_files = {filename for filename, _ in expected_attachments.values()}
        expected_api_files = {"system-status.json", "registry.json", "messages.json", "queue.json"}
        if (
            media_files != expected_media_files
            or api_files != expected_api_files
            or any(
                not _safe_artifact_file(bundle, media_root / filename)
                for filename in expected_media_files
            )
            or any(
                not _safe_artifact_file(bundle, api_root_relative / filename)
                for filename in expected_api_files
            )
        ):
            raise LabConfigError(f"{project} portable attachments are invalid")
        metadata = _load_json(bundle / media_root / "operator-metadata.json")
        if (
            metadata.get("project") != project
            or not isinstance(metadata.get("task_id"), str)
            or not isinstance(metadata.get("command_body"), str)
            or metadata.get("expected_output")
            != f"edgecitadel:{metadata.get('command_body')}"
        ):
            raise LabConfigError(f"{project} operator metadata is invalid")
        task_ids.append(str(metadata["task_id"]))
        api_root = bundle / api_root_relative
        try:
            status = json.loads((api_root / "system-status.json").read_text())
            registry = json.loads((api_root / "registry.json").read_text())
            messages = json.loads((api_root / "messages.json").read_text())
            queue = json.loads((api_root / "queue.json").read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise LabConfigError(f"{project} API evidence is invalid") from error
        if (
            not isinstance(status, Mapping)
            or status.get("nats_connected") is not True
            or status.get("jetstream_stream_ok") is not True
        ):
            raise LabConfigError(f"{project} system status is invalid")
        shell = [
            item for item in registry
            if isinstance(registry, list) and isinstance(item, Mapping)
            and item.get("agent_id") == "shell-1"
        ] if isinstance(registry, list) else []
        if (
            len(shell) != 1
            or shell[0].get("agent_state") != "online"
            or not isinstance(shell[0].get("card"), Mapping)
            or not isinstance(shell[0]["card"].get("metadata"), Mapping)
            or shell[0]["card"]["metadata"].get("runtime.conformance") != "L1"
        ):
            raise LabConfigError(f"{project} registry is invalid")
        if (
            not isinstance(messages, list)
            or not messages
            or any(
                not isinstance(item, Mapping)
                or item.get("task_id") != metadata["task_id"]
                for item in messages
            )
        ):
            raise LabConfigError(f"{project} messages are invalid")
        try:
            _operator_pair(messages, metadata=metadata)
        except LabConfigError:
            raise LabConfigError(f"{project} messages are invalid")
        if (
            not isinstance(queue, Mapping)
            or queue.get("pending") != 0
            or queue.get("ack_pending") != 0
        ):
            raise LabConfigError(f"{project} queue is invalid")
    counts = {
        "png": len(list((bundle / "raw/playwright").glob("*/*.png"))),
        "metadata": len(list((bundle / "raw/playwright").glob("*/operator-metadata.json"))),
        "api": len(list((bundle / "raw/api").glob("*/*.json"))),
        "webm": len(list((bundle / "raw/playwright").glob("*/*.webm"))),
        "trace": len(list((bundle / "raw/playwright").glob("*/trace.zip"))),
    }
    if counts != {"png": 4, "metadata": 2, "api": 8, "webm": 2, "trace": 2}:
        raise LabConfigError(f"unexpected Slice 2 media counts: {counts}")
    if len(set(task_ids)) != 2:
        raise LabConfigError("operator metadata task IDs must be distinct")


def relocate_slice2_media(*, repo_root: Path, bundle: Path) -> dict[str, object]:
    """Relocate the exact Slice 2 report and media into a portable lab bundle."""
    report_path = repo_root.resolve() / "playwright-results.json"
    target = bundle.resolve() / "playwright-results.json"
    if target.exists():
        raise LabConfigError("portable Playwright report already exists")
    report = _load_json(report_path)
    results = passed_project_results(report)
    portable = copy_media(repo_root.resolve(), bundle.resolve(), results)
    if not isinstance(portable, dict):
        raise LabConfigError("portable Playwright mapping is invalid")
    _validate_portable_media(bundle.resolve(), portable)
    try:
        write_json(target, portable)
    except FileExistsError:
        raise LabConfigError("portable Playwright report already exists") from None
    return portable


def run_operator_journey(
    *, repo_root: Path, run_id: str, host_id: str
) -> subprocess.CompletedProcess[str]:
    """Run the unchanged operator spec against one maintained shell fixture."""
    repo_root = repo_root.resolve()
    state_file, controller_config = _controller_paths(repo_root, run_id)
    controller_started = False
    completed: subprocess.CompletedProcess[str] | None = None
    config: dict[str, object] = {}
    try:
        _run(_python_argv(
            repo_root, "scripts/research/lab_controller.py", "start",
            "--run-id", run_id, "--host-id", host_id,
            "--lab-variant", "operator-smoke",
        ), cwd=repo_root)
        controller_started = True
        config = _load_json(controller_config)
        credential = _config_path(config.get("credential_file"), "credential file")
        _node_start(
            repo_root, controller_config, credential, host_id, "shell-1",
            delay_ms=1000,
        )
        shell_state = _load_json(_node_state_file(run_id, "shell-1"))
        terminal_release_dir = Path(str(shell_state["state_dir"])) / "terminal-release"
        if not terminal_release_dir.is_dir():
            raise LabConfigError("operator terminal-release directory is unavailable")
        _wait_online(str(config["agg_url"]), frozenset({"shell-1"}))
        _doctor(repo_root, controller_config, credential, host_id, "shell-1")
        argv = [
            "npx", "--no-install", "playwright", "test",
            "--config", "playwright.config.js", "tests/operator-journey.spec.js",
        ]
        completed = _run(
            argv,
            cwd=repo_root / "e2e",
            env={
                **os.environ,
                "APP_URL": str(config["app_url"]),
                "AGG_URL": str(config["agg_url"]),
                "E2E_TERMINAL_RELEASE_DIR": str(terminal_release_dir),
            },
            check=False,
        )
        if completed.returncode != 0 or "1 passed" not in completed.stdout:
            raise LabConfigError("operator Playwright journey failed")
        bundle = _config_path(config.get("evidence_dir"), "evidence directory")
        messages = _request_json(f"{config['agg_url']}/api/messages?agent_id=shell-1")
        command, terminal = _operator_pair(messages)
        command_payload = command["payload"]
        terminal_payload = terminal["payload"]
        smoke = {
            "argv": argv,
            "cwd": "e2e",
            "returncode": completed.returncode,
            "assertion": "1 passed",
            "task_id": command["task_id"],
            "context_id": command["context_id"],
            "hop_count": command["hop_count"],
            "nonce": command_payload["body"],
            "output": terminal_payload["body"],
        }
        write_json(bundle / "playwright-smoke.json", smoke)
    finally:
        primary = sys.exc_info()[1]
        cleanup_errors: list[Exception] = []
        if controller_started:
            credential: Path | None = None
            if controller_config.is_file():
                try:
                    current = _load_json(controller_config)
                    credential_value = current.get("credential_file")
                    if not isinstance(credential_value, str):
                        raise LabConfigError("controller credential path is invalid")
                    credential = Path(credential_value)
                except Exception as error:
                    cleanup_errors.append(error)
            else:
                cleanup_errors.append(
                    LabConfigError("controller config disappeared before node cleanup")
                )
            if credential is not None:
                try:
                    receipt = _node_stop(
                        repo_root, controller_config, credential, "shell-1"
                    )
                    if receipt.returncode != 0 or not receipt.stdout.startswith("node:"):
                        raise LabConfigError(
                            f"shell-1 cleanup failed: {receipt.stderr.strip()}"
                        )
                except Exception as error:
                    cleanup_errors.append(error)
            try:
                if _container_count(run_id, repo_root) != 0:
                    raise LabConfigError("run-owned node containers remain")
            except Exception as error:
                cleanup_errors.append(error)
            if state_file.is_file():
                try:
                    stopped = _run(_python_argv(
                        repo_root, "scripts/research/lab_controller.py", "stop",
                        "--state-file", state_file,
                    ), cwd=repo_root, check=False)
                    if stopped.returncode != 0:
                        raise LabConfigError(
                            f"controller stop failed: {stopped.stderr.strip()}"
                        )
                except Exception as error:
                    cleanup_errors.append(error)
            else:
                cleanup_errors.append(LabConfigError("controller state disappeared before cleanup"))
        if cleanup_errors:
            if isinstance(primary, Exception):
                cleanup_errors.insert(0, primary)
            raise ExceptionGroup("operator journey and cleanup failed", cleanup_errors) from None
    if completed is None or not config:
        raise LabConfigError("operator journey did not run")
    bundle = _config_path(config.get("evidence_dir"), "evidence directory")
    check_bundle(bundle, expected_kind="lab", source_root=repo_root).require_valid()
    return completed


def _finalizer_count(repo_root: Path, run_id: str) -> int:
    state_file, _ = _controller_paths(repo_root, run_id)
    state = _load_json(state_file)
    steps = state.get("completed_cleanup_steps")
    if not isinstance(steps, list):
        raise LabConfigError("controller cleanup journal is invalid")
    return steps.count("manifest-finalized")


def _cleanup_for_run(repo_root: Path, run_id: str) -> Mapping[str, object]:
    state_file, _ = _controller_paths(repo_root, run_id)
    return _load_json(state_file.parent / "cleanup.json")


def _receipt_relative_path(path: Path, receipt: Path) -> str:
    try:
        relative = path.resolve().relative_to(receipt.parent.resolve())
    except ValueError as error:
        raise LabConfigError(
            "receipt bundle paths must resolve below the receipt parent"
        ) from error
    if ".." in relative.parts:
        raise LabConfigError("receipt bundle path is not portable")
    return relative.as_posix()


def run_clean_checkout_gate(
    *,
    repo_root: Path,
    run_id: str,
    host_id: str,
    receipt: Path,
    retain_bundles: Path | None = None,
) -> dict[str, object]:
    """Run one lifecycle and operator journey from a clean scoped checkout."""
    root = repo_root.resolve()
    provenance = capture_clean_source_provenance(root)
    base_run_id = validate_run_id(run_id)
    lifecycle_id = validate_run_id(f"{base_run_id}-life")
    operator_id = validate_run_id(f"{base_run_id}-ui")
    receipt_path = receipt.resolve()
    if receipt_path.exists():
        raise LabConfigError("clean-checkout receipt already exists")
    if retain_bundles is not None and retain_bundles.resolve().exists():
        raise LabConfigError("retained bundle directory already exists")

    lifecycle = run_two_node_lifecycle(
        repo_root=root,
        run_id=lifecycle_id,
        host_id=host_id,
    )
    run_operator_journey(
        repo_root=root,
        run_id=operator_id,
        host_id=host_id,
    )
    source_bundles = {
        "lifecycle": lifecycle.bundle.resolve(),
        "operator_smoke": (
            root / "data/research/results/lab" / operator_id
        ).resolve(),
    }
    for bundle in source_bundles.values():
        check_bundle(
            bundle, expected_kind="lab", source_root=root
        ).require_valid()

    finalizer_counts = {
        "lifecycle": _finalizer_count(root, lifecycle_id),
        "operator_smoke": _finalizer_count(root, operator_id),
    }
    if set(finalizer_counts.values()) != {1}:
        raise LabConfigError("each controller must finalize exactly once")
    cleanups = (
        _cleanup_for_run(root, lifecycle_id),
        _cleanup_for_run(root, operator_id),
    )
    remaining: list[object] = []
    cleanup_invalid = False
    for cleanup in cleanups:
        values = cleanup.get("remaining")
        if not isinstance(values, list):
            cleanup_invalid = True
        else:
            remaining.extend(values)
        cleanup_invalid = cleanup_invalid or (
            cleanup.get("completed") is not True
            or cleanup.get("owned_resources_removed") is not True
            or cleanup.get("foreign_resources_touched") is not False
        )
    if cleanup_invalid or remaining:
        raise LabConfigError("clean-checkout cleanup is incomplete")

    retained = source_bundles
    if retain_bundles is not None:
        retain_root = retain_bundles.resolve()
        retain_root.mkdir(parents=True)
        retained = {}
        for name, bundle in source_bundles.items():
            target = retain_root / name.replace("_", "-")
            shutil.copytree(bundle, target)
            retained[name] = target

    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    value: dict[str, object] = {
        "schema_version": "1",
        "source_commit": provenance.commit,
        "source_snapshot_sha256": provenance.source_snapshot_sha256,
        "bundles": {
            name: {
                "path": _receipt_relative_path(retained[name], receipt_path),
                "checker_valid": True,
                "finalizer_count": finalizer_counts[name],
            }
            for name in ("lifecycle", "operator_smoke")
        },
        "cleanup": {
            "complete": True,
            "owned_resources_remaining": [],
        },
    }
    write_json(receipt_path, value)
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clean-checkout-gate", action="store_true", required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--host-id", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--retain-bundles", type=Path)
    arguments = parser.parse_args(argv)
    run_clean_checkout_gate(
        repo_root=arguments.repo_root,
        run_id=arguments.run_id,
        host_id=arguments.host_id,
        receipt=arguments.receipt,
        retain_bundles=arguments.retain_bundles,
    )
    print(arguments.receipt.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "LifecycleResult", "assert_disjoint_runs", "relocate_slice2_media",
    "require_single_handler_started", "run_clean_checkout_gate",
    "run_concurrent_pair", "run_operator_journey", "run_sequential_pair",
    "run_two_node_lifecycle",
]
