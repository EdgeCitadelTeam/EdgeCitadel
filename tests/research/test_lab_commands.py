"""Run-scoped command, await, and operator-media contracts."""

from __future__ import annotations

import json
import asyncio
import subprocess
import threading
import urllib.error
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from scripts.research.artifact_env import OwnedResource
from scripts.research.lab_config import (
    ControllerConfig,
    LabConfigError,
    credential_sha256,
    write_credential_file,
)
from scripts.research.lab_controller import (
    ControllerOwnershipState,
    _publish_duplicate_wires_async,
    await_command,
    await_run_command,
    command_run,
    load_controller_state,
    submit_command,
    write_controller_state,
)


class _Response:
    def __init__(self, status: int, value: object) -> None:
        self.status = status
        self._payload = json.dumps(value).encode()

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


class _Replies:
    def __init__(self, replies: list[tuple[str, int, object]]) -> None:
        self.replies = list(replies)
        self.calls: list[str] = []

    def __call__(self, request: object, **_kwargs: object) -> _Response:
        url = request.full_url if hasattr(request, "full_url") else str(request)
        self.calls.append(url)
        if not self.replies:
            raise AssertionError(f"unexpected request: {url}")
        expected, status, value = self.replies.pop(0)
        assert expected in url
        return _Response(status, value)


def _state(tmp_path: Path) -> tuple[Path, ControllerConfig]:
    credential = tmp_path / "scratch/transport-token"
    write_credential_file(credential, "4" * 64)
    config = ControllerConfig(
        run_id="ec-lab-01", lab_variant="lifecycle",
        controller_host_id="controller-lab-01",
        compose_project="edgecitadel-artifact-ec-lab-01",
        bind_host="127.0.0.1", advertised_host="127.0.0.1",
        advertised_ip="127.0.0.1", app_url="http://127.0.0.1:18080",
        agg_url="http://127.0.0.1:18080",
        nats_url="nats://127.0.0.1:14222",
        monitor_url="http://127.0.0.1:18222",
        inventory_url="http://127.0.0.1:18080/api/lab/status",
        controller_machine_id_sha256="a" * 64,
        source_commit="d" * 40, source_snapshot_sha256="e" * 64,
        credential_sha256=credential_sha256(credential),
        credential_file=credential, fixture_image_id="sha256:" + "c" * 64,
        state_dir=tmp_path / "lab/ec-lab-01",
        evidence_dir=tmp_path / "evidence",
    )
    state_file = config.state_dir / "controller-state.json"
    write_controller_state(state_file, ControllerOwnershipState(
        schema_version="lab-controller-state.v1", phase="active", config=config,
        compose_file=tmp_path / "docker-compose.lab.yml",
        compose_environment={}, artifact_scratch_root=tmp_path / "scratch",
        raw_credential_file=credential,
        service_env_file=tmp_path / "service.env",
        owned_resources=(OwnedResource("network", "lab-net"),),
        completed_cleanup_steps=(), exported_image_paths=(), controller_argv=(),
        started_at="2026-07-27T00:00:00Z",
    ))
    return state_file, config


def _inventory(*, events: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "reservations": [{
            "agent_id": "fixture-1", "reservation_id": "reservation-1",
            "declared_host_id": "controller-lab-01", "state": "active",
        }],
        "reservation_events": events or [],
        "node_reports": [],
    }


def _messages(
    body: str = "edgecitadel:nonce",
    *,
    terminal_id: str = "result-1",
    task_id: str = "10000000-0000-4000-8000-000000000001",
) -> list[dict[str, object]]:
    return [
        {"id": "command-1", "type": "command", "sender_id": "aggregator", "recipient_id": "fixture-1", "task_id": task_id, "context_id": task_id, "hop_count": 0, "payload": {"body": "nonce"}},
        {"id": "progress-1", "type": "task.progress", "sender_id": "fixture-1", "recipient_id": "aggregator", "task_id": task_id, "context_id": task_id, "hop_count": 0, "task_state": "working", "payload": {"body": "working"}},
        {"id": terminal_id, "type": "result", "sender_id": "fixture-1", "recipient_id": "aggregator", "task_id": task_id, "context_id": task_id, "hop_count": 0, "task_state": "completed", "payload": {"body": body}, "timestamp": "2026-07-27T00:00:01.000Z"},
    ]


def test_command_wait_writes_one_exact_terminal_and_observation_sequence(tmp_path: Path) -> None:
    state_file, config = _state(tmp_path)
    opener = _Replies([
        ("/api/lab/status", 200, _inventory()),
        ("/api/command/fixture-1", 202, {"task_id": "10000000-0000-4000-8000-000000000001", "recipient_id": "fixture-1", "accepted_at": "2026-07-27T00:00:00.000Z"}),
        ("/api/messages?task_id=", 200, _messages()),
        ("/api/agents/fixture-1/queue", 200, {"pending": 0, "ack_pending": 0}),
    ])
    result_file = tmp_path / "command.json"

    result = command_run(
        state_file, "fixture-1", "nonce", "edgecitadel:nonce", result_file,
        wait=True, opener=opener, sleeper=lambda _: None,
    )

    assert result["status"] == "completed"
    assert result["task_id"] == "10000000-0000-4000-8000-000000000001"
    assert result["reservation_id"] == "reservation-1"
    assert result["qualification_kind"] == "direct"
    assert result["http_status"] == 202
    assert result["terminal_output"] == "edgecitadel:nonce"
    assert result["terminal_count"] == 1
    assert result["conflicting_terminal"] is False
    assert json.loads(result_file.read_text()) == result
    assert ("4" * 64) not in result_file.read_text()
    observations = [json.loads(line) for line in (config.evidence_dir / "lab-observations.jsonl").read_text().splitlines()]
    assert [row["event"] for row in observations] == ["command.accepted", "command.wire_submitted", "command.terminal"]
    assert [row["sequence"] for row in observations] == [1, 2, 3]
    assert len(observations[0]["data"]["request_body_sha256"]) == 64
    assert "body" not in observations[0]["data"]


def test_no_wait_then_await_preserves_task_identity(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    task_id = "10000000-0000-4000-8000-000000000001"
    accepted = command_run(
        state_file, "fixture-1", "nonce", "edgecitadel:nonce", tmp_path / "accepted.json",
        wait=False, opener=_Replies([
            ("/api/lab/status", 200, _inventory()),
            ("/api/command/fixture-1", 202, {"task_id": task_id, "recipient_id": "fixture-1", "accepted_at": "2026-07-27T00:00:00.000Z"}),
        ]),
    )
    completed = await_run_command(
        state_file, task_id, "edgecitadel:nonce", tmp_path / "completed.json",
        opener=_Replies([
            ("/api/messages?task_id=", 200, _messages()),
            ("/api/agents/fixture-1/queue", 200, {"pending": 0, "ack_pending": 0}),
        ]), sleeper=lambda _: None,
    )
    assert accepted["status"] == "accepted"
    assert accepted["reservation_id"] == "reservation-1"
    assert accepted["http_status"] == 202
    assert completed["status"] == "completed"
    assert accepted["task_id"] == completed["task_id"] == task_id
    assert completed["accepted_at"] == accepted["accepted_at"]


def test_await_retries_network_and_server_failures_to_completion(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    task_id = "10000000-0000-4000-8000-000000000001"
    command_run(
        state_file,
        "fixture-1",
        "nonce",
        "edgecitadel:nonce",
        tmp_path / "accepted.json",
        wait=False,
        opener=_Replies([
            ("/api/lab/status", 200, _inventory()),
            ("/api/command/fixture-1", 202, {
                "task_id": task_id,
                "recipient_id": "fixture-1",
                "accepted_at": "2026-07-27T00:00:00.000Z",
            }),
        ]),
    )
    outcomes: list[object] = [
        urllib.error.URLError("offline"),
        _Response(503, {}),
        _Response(200, _messages()),
        _Response(200, {"pending": 0, "ack_pending": 0}),
    ]

    def opener(_request: object, **_kwargs: object) -> _Response:
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    sleeps: list[float] = []
    result = await_run_command(
        state_file,
        task_id,
        "edgecitadel:nonce",
        tmp_path / "completed.json",
        opener=opener,
        timeout_s=1,
        poll_interval_s=0.01,
        sleeper=sleeps.append,
    )

    assert result["status"] == "completed"
    assert result["reservation_id"] == "reservation-1"
    assert result["qualification_kind"] == "direct"
    assert result["http_status"] == 202
    assert result["terminal_output"] == "edgecitadel:nonce"
    assert result["terminal_count"] == 1
    assert result["conflicting_terminal"] is False
    assert sleeps == [0.01, 0.01]
    assert outcomes == []


def test_two_wire_copies_are_identical_and_header_free(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    published: list[tuple[str, bytes, object]] = []
    calls: list[str] = []

    def opener(request: object, **_kwargs: object) -> _Response:
        url = request.full_url if hasattr(request, "full_url") else str(request)
        calls.append(url)
        if url.endswith("/api/lab/status"):
            return _Response(200, _inventory())
        if "/api/messages?task_id=" in url:
            task_id = json.loads(published[0][1])["task_id"]
            logical = _messages(task_id=task_id)
            terminal = logical[-1]
            return _Response(200, [
                logical[1], terminal, {**terminal, "id": "result-2"},
            ])
        if url.endswith("/api/agents/fixture-1/queue"):
            return _Response(200, {"pending": 0, "ack_pending": 0})
        raise AssertionError(f"unexpected request: {url}")

    result = command_run(
        state_file, "fixture-1", "nonce", "edgecitadel:nonce", tmp_path / "duplicate.json",
        wait=True, wire_copies=2,
        opener=opener,
        wire_publisher=lambda subject, payload, headers: published.append((subject, payload, headers)),
        sleeper=lambda _: None,
    )

    inbox = [item for item in published if item[0] == "agents.fixture-1.inbox"]
    assert len(inbox) == 2
    assert inbox[0][1] == inbox[1][1]
    assert inbox[0][2] is None and inbox[1][2] is None
    envelope = json.loads(inbox[0][1])
    assert envelope["task_id"] == result["task_id"]
    assert result["wire_copies"] == 2
    observations = [json.loads(line) for line in (state_file.parent.parent.parent / "evidence/lab-observations.jsonl").read_text().splitlines()]
    assert [row["event"] for row in observations].count("command.wire_submitted") == 2
    assert [row["event"] for row in observations].count("command.terminal") == 1
    assert len([url for url in calls if "/api/messages?task_id=" in url]) == 1

    class Connection:
        def __init__(self) -> None:
            self.published: list[tuple[str, bytes, object]] = []

        async def connect(self, **_kwargs: object) -> None:
            return None

        def jetstream(self) -> "Connection":
            return self

        async def publish(self, subject: str, payload: bytes, headers: object = None) -> None:
            self.published.append((subject, payload, headers))

        async def flush(self) -> None:
            return None

        async def drain(self) -> None:
            return None

    connection = Connection()
    asyncio.run(_publish_duplicate_wires_async(
        load_controller_state(state_file), "agents.fixture-1.inbox", b"canonical",
        connection_factory=lambda: connection,
    ))
    assert connection.published == [
        ("agents.fixture-1.inbox", b"canonical", None),
        ("agents.fixture-1.inbox", b"canonical", None),
    ]


def test_duplicate_publish_preserves_primary_error_when_drain_also_fails(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    primary = RuntimeError("publish failed")
    drained: list[bool] = []

    class Connection:
        async def connect(self, **_kwargs: object) -> None:
            return None

        def jetstream(self) -> "Connection":
            return self

        async def publish(self, _subject: str, _payload: bytes) -> None:
            raise primary

        async def drain(self) -> None:
            drained.append(True)
            raise RuntimeError("drain failed")

    try:
        asyncio.run(_publish_duplicate_wires_async(
            load_controller_state(state_file), "agents.fixture-1.inbox", b"canonical",
            connection_factory=Connection,
        ))
    except RuntimeError as error:
        assert error is primary
        assert error.__notes__ == ["NATS drain failed: RuntimeError('drain failed')"]
    else:
        raise AssertionError("publish failure must escape")
    assert drained == [True]


@pytest.mark.parametrize(
    ("task_id", "accepted_at"),
    (
        ("10000000-0000-1000-8000-000000000001", "2026-07-27T00:00:00.000Z"),
        ("not-a-uuid", "2026-07-27T00:00:00.000Z"),
        ("10000000-0000-4000-8000-000000000001", "2026-07-27T00:00:00.000+00:00"),
        ("10000000-0000-4000-8000-000000000001", "2026-07-27 00:00:00Z"),
    ),
)
def test_http_acceptance_requires_uuid4_and_utc_timestamp(
    tmp_path: Path, task_id: str, accepted_at: str
) -> None:
    state_file, _ = _state(tmp_path)
    result_file = tmp_path / "invalid.json"
    with pytest.raises(LabConfigError, match="command response is invalid"):
        command_run(
            state_file, "fixture-1", "nonce", "edgecitadel:nonce", result_file,
            wait=False,
            opener=_Replies([
                ("/api/lab/status", 200, _inventory()),
                ("/api/command/fixture-1", 202, {
                    "task_id": task_id,
                    "recipient_id": "fixture-1",
                    "accepted_at": accepted_at,
                }),
            ]),
        )
    assert not result_file.exists()

    valid = command_run(
        state_file, "fixture-1", "nonce", "edgecitadel:nonce", result_file,
        wait=False,
        opener=_Replies([
            ("/api/lab/status", 200, _inventory()),
            ("/api/command/fixture-1", 202, {
                "task_id": "10000000-0000-4000-8000-000000000001",
                "recipient_id": "fixture-1",
                "accepted_at": "2026-07-27T00:00:00.000Z",
            }),
        ]),
    )
    assert valid["status"] == "accepted"


def test_commands_fail_closed_on_agent_http_terminal_and_queue_errors(tmp_path: Path) -> None:
    for label, messages, queue, match in (
        ("conflict", _messages() + [{**_messages("wrong", terminal_id="result-2")[-1]}], {"pending": 0, "ack_pending": 0}, "terminal"),
        ("identity", [*_messages()[:-1], {**_messages()[-1], "recipient_id": "other"}], {"pending": 0, "ack_pending": 0}, "identity"),
        ("residue", _messages(), {"pending": 1, "ack_pending": 0}, "drained"),
    ):
        root = tmp_path / label
        state_file, _ = _state(root)
        task_id = "10000000-0000-4000-8000-000000000001"
        command_run(
            state_file, "fixture-1", "nonce", "edgecitadel:nonce", root / "accepted.json",
            wait=False, opener=_Replies([
                ("/api/lab/status", 200, _inventory()),
                ("/api/command/fixture-1", 202, {"task_id": task_id, "recipient_id": "fixture-1", "accepted_at": "2026-07-27T00:00:00.000Z"}),
            ]),
        )
        result_file = root / "failed.json"
        with pytest.raises(LabConfigError, match=match):
            await_run_command(
                state_file, task_id, "edgecitadel:nonce", result_file,
                opener=_Replies([
                    ("/api/messages?task_id=", 200, messages),
                    ("/api/agents/fixture-1/queue", 200, queue),
                ]), timeout_s=0, sleeper=lambda _: None,
            )
        assert not result_file.exists()

    for label, replies, match in (
        ("unknown", [("/api/lab/status", 200, {"reservations": [], "reservation_events": [], "node_reports": []})], "unknown"),
        ("http", [("/api/lab/status", 200, _inventory()), ("/api/command/fixture-1", 500, {})], "HTTP status"),
    ):
        root = tmp_path / label
        state_file, _ = _state(root)
        with pytest.raises(LabConfigError, match=match):
            command_run(
                state_file, "fixture-1", "nonce", "edgecitadel:nonce", root / "failed.json",
                wait=False, opener=_Replies(replies),
            )
        assert not (root / "failed.json").exists()


def test_queued_reconnect_requires_same_reservation_and_strict_order(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    task_id = "10000000-0000-4000-8000-000000000001"
    command_run(
        state_file, "fixture-1", "nonce", "edgecitadel:nonce", tmp_path / "accepted.json",
        wait=False, opener=_Replies([
            ("/api/lab/status", 200, _inventory(events=[
                {"sequence": 1, "agent_id": "fixture-1", "reservation_id": "reservation-1", "event": "retained", "observed_at": "2026-07-27T00:00:00.000Z"},
            ])),
            ("/api/command/fixture-1", 202, {"task_id": task_id, "recipient_id": "fixture-1", "accepted_at": "2026-07-27T00:00:01.000Z"}),
        ]),
    )
    result = await_run_command(
        state_file, task_id, "edgecitadel:nonce", tmp_path / "completed.json",
        qualification_kind="queued-reconnect",
        opener=_Replies([
            ("/api/messages?task_id=", 200, [
                *_messages()[:-1],
                {**_messages()[-1], "timestamp": "2026-07-27T00:00:03.000Z"},
            ]),
            ("/api/agents/fixture-1/queue", 200, {"pending": 0, "ack_pending": 0}),
            ("/api/lab/status", 200, _inventory(events=[
                {"sequence": 1, "agent_id": "fixture-1", "reservation_id": "reservation-1", "event": "retained", "observed_at": "2026-07-27T00:00:00.000Z"},
                {"sequence": 2, "agent_id": "fixture-1", "reservation_id": "reservation-1", "event": "resumed", "observed_at": "2026-07-27T00:00:02.000Z"},
            ])),
        ]), sleeper=lambda _: None,
    )
    assert result["status"] == "completed"
    assert result["reservation_id"] == "reservation-1"
    assert result["qualification_kind"] == "queued-reconnect"
    assert result["http_status"] == 202
    assert result["terminal_output"] == "edgecitadel:nonce"
    assert result["terminal_count"] == 1
    assert result["conflicting_terminal"] is False

    for label, resumed_reservation, resumed_at in (
        ("changed", "reservation-2", "2026-07-27T00:00:02.000Z"),
        ("out-of-order", "reservation-1", "2026-07-27T00:00:00.500Z"),
    ):
        root = tmp_path / label
        other_state, _ = _state(root)
        command_run(
            other_state, "fixture-1", "nonce", "edgecitadel:nonce", root / "accepted.json",
            wait=False, opener=_Replies([
                ("/api/lab/status", 200, _inventory(events=[
                    {"sequence": 1, "agent_id": "fixture-1", "reservation_id": "reservation-1", "event": "retained", "observed_at": "2026-07-27T00:00:00.000Z"},
                ])),
                ("/api/command/fixture-1", 202, {"task_id": task_id, "recipient_id": "fixture-1", "accepted_at": "2026-07-27T00:00:01.000Z"}),
            ]),
        )
        with pytest.raises(LabConfigError, match="reconnect"):
            await_run_command(
                other_state, task_id, "edgecitadel:nonce", root / "failed.json",
                qualification_kind="queued-reconnect",
                opener=_Replies([
                    ("/api/messages?task_id=", 200, [*_messages()[:-1], {**_messages()[-1], "timestamp": "2026-07-27T00:00:03.000Z"}]),
                    ("/api/agents/fixture-1/queue", 200, {"pending": 0, "ack_pending": 0}),
                    ("/api/lab/status", 200, _inventory(events=[
                        {"sequence": 1, "agent_id": "fixture-1", "reservation_id": "reservation-1", "event": "retained", "observed_at": "2026-07-27T00:00:00.000Z"},
                        {"sequence": 2, "agent_id": "fixture-1", "reservation_id": resumed_reservation, "event": "resumed", "observed_at": resumed_at},
                    ])),
                ]), sleeper=lambda _: None,
            )
        assert not (root / "failed.json").exists()


def test_queued_reconnect_retries_transient_inventory_failures_to_deadline(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    task_id = "10000000-0000-4000-8000-000000000001"
    retained = {
        "sequence": 1, "agent_id": "fixture-1", "reservation_id": "reservation-1",
        "event": "retained", "observed_at": "2026-07-27T00:00:00.000Z",
    }
    resumed = {
        "sequence": 2, "agent_id": "fixture-1", "reservation_id": "reservation-1",
        "event": "resumed", "observed_at": "2026-07-27T00:00:02.000Z",
    }
    command_run(
        state_file, "fixture-1", "nonce", "edgecitadel:nonce", tmp_path / "accepted.json",
        wait=False,
        opener=_Replies([
            ("/api/lab/status", 200, _inventory(events=[retained])),
            ("/api/command/fixture-1", 202, {
                "task_id": task_id, "recipient_id": "fixture-1",
                "accepted_at": "2026-07-27T00:00:01.000Z",
            }),
        ]),
    )
    inventory_outcomes: list[object] = [
        urllib.error.URLError("offline"),
        _Response(503, {}),
        _Response(200, _inventory(events=[retained, resumed])),
    ]
    calls = {"messages": 0, "queue": 0, "inventory": 0}

    def opener(request: object, **_kwargs: object) -> _Response:
        url = request.full_url if hasattr(request, "full_url") else str(request)
        if "/api/messages?task_id=" in url:
            calls["messages"] += 1
            return _Response(200, [
                *_messages()[:-1],
                {**_messages()[-1], "timestamp": "2026-07-27T00:00:03.000Z"},
            ])
        if url.endswith("/api/agents/fixture-1/queue"):
            calls["queue"] += 1
            return _Response(200, {"pending": 0, "ack_pending": 0})
        assert url.endswith("/api/lab/status")
        calls["inventory"] += 1
        outcome = inventory_outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    sleeps: list[float] = []
    result = await_run_command(
        state_file, task_id, "edgecitadel:nonce", tmp_path / "completed.json",
        qualification_kind="queued-reconnect", opener=opener,
        timeout_s=1, poll_interval_s=0.01, sleeper=sleeps.append,
    )

    assert result["status"] == "completed"
    assert calls == {"messages": 1, "queue": 1, "inventory": 3}
    assert sleeps == [0.01, 0.01]
    assert inventory_outcomes == []


def test_finalized_bundle_and_existing_results_fail_before_io(tmp_path: Path) -> None:
    state_file, config = _state(tmp_path)
    config.evidence_dir.mkdir(parents=True)
    (config.evidence_dir / "manifest.json").write_text("{}\n")
    calls: list[object] = []
    with pytest.raises(LabConfigError, match="finalized"):
        command_run(
            state_file, "fixture-1", "nonce", "edgecitadel:nonce", tmp_path / "result.json",
            wait=False, opener=lambda request, **_: calls.append(request),
        )
    assert calls == []

    await_root = tmp_path / "await"
    await_state, await_config = _state(await_root)
    task_id = "10000000-0000-4000-8000-000000000001"
    accepted_path = await_root / "accepted.json"
    command_run(
        await_state, "fixture-1", "nonce", "edgecitadel:nonce", accepted_path,
        wait=False, opener=_Replies([
            ("/api/lab/status", 200, _inventory()),
            ("/api/command/fixture-1", 202, {"task_id": task_id, "recipient_id": "fixture-1", "accepted_at": "2026-07-27T00:00:00.000Z"}),
        ]),
    )
    assert ("4" * 64) not in accepted_path.read_text()
    await_config.evidence_dir.mkdir(parents=True, exist_ok=True)
    (await_config.evidence_dir / "manifest.json").write_text("{}\n")
    with pytest.raises(LabConfigError, match="finalized"):
        await_run_command(
            await_state, task_id, "edgecitadel:nonce", await_root / "terminal.json",
            opener=lambda request, **_: calls.append(request),
        )
    assert calls == []

    cli_root = tmp_path / "cli"
    _, cli_config = _state(cli_root)
    cli_config.evidence_dir.mkdir(parents=True, exist_ok=True)
    (cli_config.evidence_dir / "manifest.json").write_text("{}\n")
    repo_root = Path(__file__).resolve().parents[2]
    prefix = [
        str(repo_root / "scripts/research/run-python"),
        str(repo_root / "scripts/research/lab_controller.py"),
    ]
    command_argv = [
        *prefix, "command", "--run-id", "ec-lab-01",
        "--state-root", str(cli_root / "lab"), "--agent-id", "fixture-1",
        "--body", "nonce", "--expected-output", "edgecitadel:nonce",
        "--no-wait", "--result-file", str(cli_root / "cli-command.json"),
    ]
    completed = subprocess.run(command_argv, cwd=repo_root, text=True, capture_output=True)
    assert completed.returncode != 0 and "finalized" in completed.stderr
    exclusive = subprocess.run(
        [*command_argv[:-3], "--wait", *command_argv[-3:]],
        cwd=repo_root, text=True, capture_output=True,
    )
    assert exclusive.returncode == 2
    await_completed = subprocess.run([
        *prefix, "await", "--run-id", "ec-lab-01",
        "--state-root", str(cli_root / "lab"), "--task-id", task_id,
        "--expected-output", "edgecitadel:nonce",
        "--result-file", str(cli_root / "cli-await.json"),
    ], cwd=repo_root, text=True, capture_output=True)
    assert await_completed.returncode != 0 and "finalized" in await_completed.stderr

    (config.evidence_dir / "manifest.json").unlink()
    result_file = tmp_path / "result.json"
    result_file.write_text("sentinel")
    with pytest.raises(LabConfigError, match="exists"):
        command_run(
            state_file, "fixture-1", "nonce", "edgecitadel:nonce", result_file,
            wait=False, opener=lambda request, **_: calls.append(request),
        )
    assert result_file.read_text() == "sentinel"
    assert calls == []


def test_result_file_is_reserved_before_any_command_io(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    result_file = tmp_path / "result.json"
    request_started = threading.Event()
    release_request = threading.Event()

    def blocking_opener(request: object, **_kwargs: object) -> _Response:
        url = request.full_url if hasattr(request, "full_url") else str(request)
        if url.endswith("/api/lab/status"):
            request_started.set()
            assert release_request.wait(timeout=2)
            return _Response(200, _inventory())
        assert url.endswith("/api/command/fixture-1")
        return _Response(202, {
            "task_id": "10000000-0000-4000-8000-000000000001",
            "recipient_id": "fixture-1",
            "accepted_at": "2026-07-27T00:00:00.000Z",
        })

    second_io: list[object] = []
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(
            command_run,
            state_file,
            "fixture-1",
            "nonce",
            "edgecitadel:nonce",
            result_file,
            wait=False,
            opener=blocking_opener,
        )
        assert request_started.wait(timeout=2)
        try:
            with pytest.raises(LabConfigError, match="result file is active"):
                command_run(
                    state_file,
                    "fixture-1",
                    "nonce",
                    "edgecitadel:nonce",
                    result_file,
                    wait=False,
                    opener=lambda request, **_: second_io.append(request),
                )
            assert second_io == []
        finally:
            release_request.set()
        assert first.result(timeout=2)["status"] == "accepted"


def test_relocate_slice2_media_replaces_report_and_validates_correlation(tmp_path: Path, monkeypatch) -> None:
    from scripts.research import lab_gate

    repo_root = tmp_path / "repo"
    bundle = tmp_path / "bundle"
    repo_root.mkdir()
    bundle.mkdir()
    (repo_root / "playwright-results.json").write_text(json.dumps({"suites": []}))
    selected = {"desktop": {"status": "passed"}, "mobile": {"status": "passed"}}
    portable = {
        "schema_version": "playwright-operator-results.v1",
        "projects": {
            project: {
                "attachments": [{"name": name, "path": f"raw/playwright/{project}/{filename}", "content_type": content_type}
                                for name, filename, content_type in (("chat", "chat.png", "image/png"), ("tasks", "tasks.png", "image/png"), ("operator-metadata", "operator-metadata.json", "application/json"), ("video", "video.webm", "video/webm"), ("trace", "trace.zip", "application/zip"))]
            }
            for project in ("desktop", "mobile")
        },
    }

    def copy_media(_root: Path, target: Path, results: object) -> dict[str, object]:
        assert results == selected
        for index, project in enumerate(("desktop", "mobile"), start=1):
            media = target / "raw/playwright" / project
            api = target / "raw/api" / project
            media.mkdir(parents=True, exist_ok=True)
            api.mkdir(parents=True, exist_ok=True)
            task_id = f"10000000-0000-4000-8000-00000000000{index}"
            nonce = f"20000000-0000-4000-8000-00000000000{index}"
            (media / "chat.png").write_bytes(b"png")
            (media / "tasks.png").write_bytes(b"png")
            (media / "video.webm").write_bytes(b"webm")
            (media / "trace.zip").write_bytes(b"zip")
            (media / "operator-metadata.json").write_text(json.dumps({
                "project": project, "task_id": task_id, "nonce": nonce,
                "command_body": nonce, "expected_output": f"edgecitadel:{nonce}",
                "context_id": task_id, "hop_count": 0,
                "command_envelope_id": f"command-{index}",
                "terminal_envelope_id": f"result-{index}",
                "command_sender_id": "aggregator", "command_recipient_id": "shell-1",
                "terminal_sender_id": "shell-1", "terminal_recipient_id": "aggregator",
            }))
            (api / "system-status.json").write_text(json.dumps({"nats_connected": True, "jetstream_stream_ok": True}))
            (api / "registry.json").write_text(json.dumps([{"agent_id": "shell-1", "agent_state": "online", "card": {"metadata": {"runtime.conformance": "L1"}}}]))
            (api / "messages.json").write_text(json.dumps([
                {"id": f"command-{index}", "type": "command", "sender_id": "aggregator", "recipient_id": "shell-1", "task_id": task_id, "context_id": task_id, "hop_count": 0, "payload": {"body": nonce}},
                {"id": f"result-{index}", "type": "result", "sender_id": "shell-1", "recipient_id": "aggregator", "task_id": task_id, "context_id": task_id, "hop_count": 0, "task_state": "completed", "payload": {"body": f"edgecitadel:{nonce}"}},
            ]))
            (api / "queue.json").write_text(json.dumps({"pending": 0, "ack_pending": 0}))
        return portable

    monkeypatch.setattr(lab_gate, "passed_project_results", lambda report: selected)
    monkeypatch.setattr(lab_gate, "copy_media", copy_media)

    portable["projects"]["desktop"]["attachments"][0]["path"] = "raw/playwright/mobile/chat.png"
    with pytest.raises(LabConfigError, match="path"):
        lab_gate.relocate_slice2_media(repo_root=repo_root, bundle=bundle)
    assert not (bundle / "playwright-results.json").exists()
    portable["projects"]["desktop"]["attachments"][0]["path"] = "raw/playwright/desktop/chat.png"

    mapping = lab_gate.relocate_slice2_media(repo_root=repo_root, bundle=bundle)
    assert mapping == portable
    assert json.loads((bundle / "playwright-results.json").read_text()) == portable

    original_report = (bundle / "playwright-results.json").read_bytes()
    (bundle / "raw/playwright/desktop/chat.png").write_bytes(b"sentinel")
    with pytest.raises(LabConfigError, match="already exists"):
        lab_gate.relocate_slice2_media(repo_root=repo_root, bundle=bundle)
    assert (bundle / "playwright-results.json").read_bytes() == original_report
    assert (bundle / "raw/playwright/desktop/chat.png").read_bytes() == b"sentinel"

    (bundle / "raw/api/desktop/messages.json").write_text(json.dumps([{"type": "result", "task_id": "wrong", "task_state": "completed", "payload": {"body": "wrong"}}]))
    with pytest.raises(LabConfigError, match="messages"):
        lab_gate._validate_portable_media(bundle, portable)
    (bundle / "raw/api/desktop/messages.json").write_text("{}\n")
    with pytest.raises(LabConfigError, match="messages"):
        lab_gate._validate_portable_media(bundle, portable)


def test_portable_media_rejects_symlinks_and_undeclared_files(tmp_path: Path) -> None:
    from scripts.research import lab_gate

    bundle = tmp_path / "bundle"
    source = tmp_path / "outside.png"
    source.write_bytes(b"outside")
    project = bundle / "raw/playwright/desktop"
    project.mkdir(parents=True)
    (project / "chat.png").symlink_to(source)
    portable = {
        "schema_version": "playwright-operator-results.v1",
        "projects": {
            "desktop": {"attachments": [
                {"name": "chat", "path": "raw/playwright/desktop/chat.png", "content_type": "image/png"},
            ]},
            "mobile": {"attachments": []},
        },
    }
    with pytest.raises(LabConfigError, match="attachments"):
        lab_gate._validate_portable_media(bundle, portable)


def test_operator_pair_rejects_each_identity_and_correlation_mismatch() -> None:
    from scripts.research.lab_gate import _operator_pair

    task_id = "10000000-0000-4000-8000-000000000001"
    nonce = "20000000-0000-4000-8000-000000000001"
    messages = [
        {"id": "command-1", "type": "command", "sender_id": "aggregator", "recipient_id": "shell-1", "task_id": task_id, "context_id": task_id, "hop_count": 0, "payload": {"body": nonce}},
        {"id": "result-1", "type": "result", "sender_id": "shell-1", "recipient_id": "aggregator", "task_id": task_id, "context_id": task_id, "hop_count": 0, "task_state": "completed", "payload": {"body": f"edgecitadel:{nonce}"}},
    ]
    metadata = {
        "task_id": task_id, "nonce": nonce, "command_body": nonce,
        "expected_output": f"edgecitadel:{nonce}", "context_id": task_id, "hop_count": 0,
        "command_envelope_id": "command-1", "terminal_envelope_id": "result-1",
        "command_sender_id": "aggregator", "command_recipient_id": "shell-1",
        "terminal_sender_id": "shell-1", "terminal_recipient_id": "aggregator",
    }
    command, terminal = _operator_pair(messages, metadata=metadata)
    assert command["payload"]["body"] == nonce
    assert terminal["payload"]["body"] == f"edgecitadel:{nonce}"

    mutations = (
        (1, "sender_id", "other"),
        (0, "recipient_id", "other"),
        (1, "task_id", "30000000-0000-4000-8000-000000000001"),
        (1, "context_id", "30000000-0000-4000-8000-000000000001"),
        (1, "hop_count", 1),
        (1, "payload", {"body": "wrong"}),
    )
    for index, field, value in mutations:
        malformed = deepcopy(messages)
        malformed[index][field] = value
        with pytest.raises(LabConfigError, match="operator command/terminal"):
            _operator_pair(malformed, metadata=metadata)
    wrong_nonce = dict(metadata, nonce="30000000-0000-4000-8000-000000000001")
    with pytest.raises(LabConfigError, match="operator command/terminal"):
        _operator_pair(messages, metadata=wrong_nonce)


def test_live_consumers_use_only_agent_inbox_jsz_rows(monkeypatch) -> None:
    from scripts.research import lab_gate
    from scripts.research.modes.jetstream_config import durable_name

    expected_rows = [
        {
            "name": durable_name("task", "ec-lab-01", agent_id),
            "config": {"filter_subject": f"agents.{agent_id}.inbox"},
        }
        for agent_id in ("fixture-1", "fixture-2")
    ]
    unrelated = {"account_details": [{"stream_detail": [{
        "name": "OTHER", "consumer_detail": expected_rows,
    }]}]}
    monkeypatch.setattr(lab_gate, "_request_json", lambda _url: unrelated)
    with pytest.raises(LabConfigError, match="bindings"):
        lab_gate._live_task_consumers(
            "http://127.0.0.1:8222", "ec-lab-01", ("fixture-1", "fixture-2")
        )

    observed_rows = [
        *expected_rows,
        {"name": "observed-extra", "config": {"filter_subject": "agents.extra.inbox"}},
    ]
    snapshot = {"account_details": [{"stream_detail": [
        {"name": "OTHER", "consumer_detail": expected_rows},
        {"name": "AGENT_INBOX", "consumer_detail": observed_rows},
    ]}]}
    monkeypatch.setattr(lab_gate, "_request_json", lambda _url: snapshot)
    names, subjects = lab_gate._live_task_consumers(
        "http://127.0.0.1:8222", "ec-lab-01", ("fixture-1", "fixture-2")
    )
    assert names == frozenset(row["name"] for row in expected_rows)
    assert subjects == frozenset(row["config"]["filter_subject"] for row in expected_rows)


def test_gate_cleanup_stops_controller_from_state_when_config_disappears(tmp_path: Path, monkeypatch) -> None:
    from scripts.research import lab_gate

    for entrypoint in (lab_gate.run_two_node_lifecycle, lab_gate.run_operator_journey):
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            calls.append(list(argv))
            if "lab_controller.py" in " ".join(argv) and "start" in argv:
                state_file = tmp_path / "tmp/research/lab/ec-lab-01/controller-state.json"
                state_file.parent.mkdir(parents=True, exist_ok=True)
                state_file.write_text("{}\n")
            if "stop" in argv:
                return subprocess.CompletedProcess(argv, 0, '{"owned_resources_removed":true}\n', "")
            return subprocess.CompletedProcess(argv, 0, "", "")

        monkeypatch.setattr(lab_gate, "_run", runner)
        with pytest.raises(BaseException):
            entrypoint(repo_root=tmp_path, run_id="ec-lab-01", host_id="controller-lab-01")
        controller_stops = [argv for argv in calls if "lab_controller.py" in " ".join(argv) and "stop" in argv]
        assert len(controller_stops) == 1
        assert controller_stops[0][-2:] == [
            "--state-file", str(tmp_path / "tmp/research/lab/ec-lab-01/controller-state.json")
        ]


def test_command_records_production_http_acceptance_exclusively(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    captured: dict[str, object] = {}

    class Response:
        status = 202

        def read(self) -> bytes:
            return b'{"task_id":"10000000-0000-4000-8000-000000000001","recipient_id":"fixture-1","accepted_at":"2026-07-27T00:00:00Z"}'

        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *_args: object) -> bool:
            return False

    def opener(request: object, **_kwargs: object) -> Response:
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data)
        return Response()

    result_file = tmp_path / "command.json"
    result = submit_command(
        state_file,
        "fixture-1",
        "nonce",
        "edgecitadel:nonce",
        result_file,
        opener=opener,
    )

    assert captured == {
        "url": "http://127.0.0.1:18080/api/command/fixture-1",
        "body": {"body": "nonce"},
    }
    assert result == {
        "run_id": "ec-lab-01",
        "agent_id": "fixture-1",
        "task_id": "10000000-0000-4000-8000-000000000001",
        "wire_copies": 1,
        "accepted_at": "2026-07-27T00:00:00Z",
        "terminal_at": None,
        "expected_output": "edgecitadel:nonce",
        "status": "accepted",
    }
    assert json.loads(result_file.read_text()) == result


def test_await_requires_one_exact_terminal_and_a_drained_queue(tmp_path: Path) -> None:
    state_file, _ = _state(tmp_path)
    replies = [
        b'[{"type":"result","task_state":"completed","payload":{"body":"edgecitadel:nonce"},"timestamp":"2026-07-27T00:00:01Z"}]',
        b'{"pending":0,"ack_pending":0}',
    ]

    class Response:
        status = 200

        def __init__(self, payload: bytes) -> None:
            self.payload = payload

        def read(self) -> bytes:
            return self.payload

        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *_args: object) -> bool:
            return False

    result = await_command(
        state_file,
        "fixture-1",
        "task-1",
        "edgecitadel:nonce",
        tmp_path / "terminal.json",
        opener=lambda *_args, **_kwargs: Response(replies.pop(0)),
    )

    assert result["status"] == "completed"
    assert result["terminal_at"] == "2026-07-27T00:00:01Z"
