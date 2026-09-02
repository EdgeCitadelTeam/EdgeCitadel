from __future__ import annotations

import asyncio
import os
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.managed_runtime import run
from edgecitadel_agentd.service import serve, socket_path_for


@pytest.mark.asyncio
async def test_managed_runtime_executes_through_agentd_without_nats_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_dir = tmp_path / "state"
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

    async def handler(
        envelope: dict[str, Any], _context: object
    ) -> tuple[dict[str, Any], str]:
        assert "NATS_TOKEN" not in os.environ
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
        task = cast(
            Mapping[str, object],
            sender.call(
                "task.create",
                recipient_id="gemma-1",
                payload={"request": "hello"},
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
    finally:
        runtime.cancel()
        await asyncio.gather(runtime, return_exceptions=True)
        stop.set()
        service_thread.join(timeout=5)
        assert not service_thread.is_alive()
