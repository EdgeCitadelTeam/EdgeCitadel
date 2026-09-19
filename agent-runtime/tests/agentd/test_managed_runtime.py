from __future__ import annotations

import asyncio
import json
import os
import threading
from collections.abc import Mapping
from contextlib import closing
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.managed_runtime import run
from service_test_support import serve
from edgecitadel_agentd.service import socket_path_for
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_correlation import TaskTraceContext


@pytest.mark.parametrize("trace_available", [False, True])
@pytest.mark.parametrize("delegated", [False, True])
@pytest.mark.asyncio
async def test_managed_runtime_executes_through_agentd_without_nats_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    delegated: bool,
    trace_available: bool,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    if trace_available:
        (state_dir / "node.json").write_text('{"agent_id":"owned-managed"}')
    service_dir = state_dir / "agentd"
    stop = threading.Event()
    service_thread = threading.Thread(
        target=serve, args=(service_dir, stop), daemon=True
    )
    service_thread.start()
    socket_path = socket_path_for(service_dir)
    for _ in range(100):
        if socket_path.exists():
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("agentd socket did not become ready")

    config = tmp_path / "config.yaml"
    config.write_text(
        """agent_id: gemma-1
name: gemma-1
description: test
runtime:
  kind: native
  roles: [reasoner]
  conformance: L1
  heartbeat_interval_sec: 30
skills:
  - id: reasoning.chat
    name: chat
    description: chat
    system_prompt: private instructions
"""
    )
    monkeypatch.setenv("EDGECITADEL_STATE_DIR", str(state_dir))
    monkeypatch.setenv("EDGECITADEL_AGENTD_SOCKET", str(socket_path))
    monkeypatch.setenv("EDGECITADEL_CONNECTOR_ID", "managed-gemma-1")
    monkeypatch.delenv("NATS_TOKEN", raising=False)

    admin = AgentdClient(
        socket_path,
        admin_token=(service_dir / "admin.token").read_text().strip(),
    )
    registration = cast(
        Mapping[str, object],
        admin.call(
            "connector.register",
            connector_id="managed-gemma-1",
            host_type="managed-agent",
            agent_id="gemma-1",
            capabilities=["reasoning.chat"],
        ),
    )
    token_path = state_dir / "connectors/managed-gemma-1.token"
    token_path.parent.mkdir(mode=0o700, parents=True)
    token_path.write_text(str(registration["token"]) + "\n")
    token_path.chmod(0o600)

    correlation = TaskTraceContext(
        task_id=str(uuid4()),
        context_id="30000000-0000-4000-8000-000000000001",
        trace_id=uuid4().hex,
        parent_task_id=str(uuid4()),
        hop_count=2,
    )

    async def handler(
        envelope: dict[str, Any], _context: Any
    ) -> tuple[dict[str, Any], str]:
        assert "NATS_TOKEN" not in os.environ
        assert (_context.trace.binding_id is not None) == trace_available
        assert _context.trace.dropped_observations == (0 if trace_available else 1)
        assert envelope["context_id"] == "30000000-0000-4000-8000-000000000001"
        if delegated:
            assert envelope["type"] == "delegation"
            assert TaskTraceContext.from_envelope(envelope) == correlation
        async with _context.trace.operation("model", "owned-model") as model:
            model.input_tokens, model.output_tokens = 3, 2
            async with _context.trace.operation(
                "tool", "owned-tool", parent_span_id=model.span_id
            ):
                await asyncio.sleep(0.003)
        return {"body": envelope["payload"]["request"]}, "completed"

    runtime = asyncio.create_task(run(config, handler))
    try:
        for _ in range(100):
            connectors = cast(list[Mapping[str, object]], admin.call("connector.list"))
            managed = next(
                (
                    connector
                    for connector in connectors
                    if connector["connector_id"] == "managed-gemma-1"
                ),
                None,
            )
            if managed is not None and managed["session_active"]:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("Managed Agent session did not become active")
        assert "system_prompt" not in str(managed["card"])

        registration = cast(
            Mapping[str, object],
            admin.call(
                "connector.register",
                connector_id="pi-local",
                host_type="pi",
                agent_id="edge-one-pi",
                capabilities=["edgecitadel_delegate", "edgecitadel_task_status"],
            ),
        )
        sender = AgentdClient(
            socket_path,
            connector_id="pi-local",
            token=str(registration["token"]),
        )
        sender.call("session.open")
        if delegated:
            with closing(AgentdStore(service_dir / "agentd.sqlite3")) as owned_store:
                task = owned_store.create_task(
                    task_id=correlation.task_id,
                    sender_id="edge-one-pi",
                    recipient_id="gemma-1",
                    payload={"request": "hello"},
                    trace_id=correlation.trace_id,
                    context_id=correlation.context_id,
                    correlation=correlation,
                )
        else:
            task = cast(
                Mapping[str, object],
                sender.call(
                    "task.create",
                    recipient_id="gemma-1",
                    payload={"request": "hello"},
                    context_id="30000000-0000-4000-8000-000000000001",
                ),
            )
        for _ in range(100):
            current = cast(
                Mapping[str, object],
                sender.call("task.get", task_id=task["task_id"]),
            )
            if current["state"] == "completed":
                break
            await asyncio.sleep(0.02)
        assert current["result"] == {"body": "hello"}
        if trace_available:
            for _ in range(100):
                with closing(
                    AgentdStore(service_dir / "agentd.sqlite3")
                ) as observed_store:
                    run_events = [
                        json.loads(row[0])
                        for row in observed_store._connection.execute(
                            "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='run' ORDER BY source_seq"
                        )
                    ]
                if [event["phase"] for event in run_events] == ["started", "completed"]:
                    break
                await asyncio.sleep(0.02)
            assert [event["phase"] for event in run_events] == ["started", "completed"]
            with closing(AgentdStore(service_dir / "agentd.sqlite3")) as observed_store:
                operations = [
                    json.loads(row[0])
                    for row in observed_store._connection.execute(
                        "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind') IN ('model','tool') ORDER BY source_seq"
                    )
                ]
            assert [(e["kind"], e["phase"]) for e in operations] == [
                ("model", "started"),
                ("tool", "started"),
                ("tool", "finished"),
                ("model", "finished"),
            ]
            assert operations[1]["parent_span_id"] == operations[0]["span_id"]
            assert operations[2]["span_id"] == operations[1]["span_id"]
            assert operations[2]["event_id"] != operations[1]["event_id"]
            assert operations[2]["duration_ms"] >= 1
            assert operations[-1]["attributes"]["input_tokens"] == 3
            assert operations[-1]["attributes"]["usage_unavailable_reason"] is None
            assert all(event["task_id"] == task["task_id"] for event in run_events)
            assert (
                run_events[0]["execution_attempt_id"]
                == run_events[1]["execution_attempt_id"]
            )
            if delegated:
                assert all(
                    event["parent_task_id"] == correlation.parent_task_id
                    for event in run_events
                )
    finally:
        runtime.cancel()
        await asyncio.gather(runtime, return_exceptions=True)
        stop.set()
        service_thread.join(timeout=5)
        assert not service_thread.is_alive()
