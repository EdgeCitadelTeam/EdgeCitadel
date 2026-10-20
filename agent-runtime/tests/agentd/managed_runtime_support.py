"""A real agentd daemon and a Managed Agent runtime joined by the private socket.

The daemon runs in a thread on an ordinary store; the runtime is the production
``managed_runtime.run`` loop with a scripted handler. Tests read and change
state through the daemon's admin RPCs or a second store connection, never by
reaching into the runtime.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from edgecitadel_agentd import managed_runtime
from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.managed_runtime import Handler, run
from edgecitadel_agentd.service import socket_path_for
from edgecitadel_agentd.store import AgentdStore
from service_test_support import serve

AGENT = "worker-1"
CONNECTOR = f"managed-{AGENT}"


@dataclass
class ManagedHarness:
    service_dir: Path
    admin: AgentdClient

    def store(self) -> closing[AgentdStore]:
        return closing(AgentdStore(self.service_dir / "agentd.sqlite3"))

    def submit(self, body: str = "work") -> str:
        """An operator command addressed to the managed agent (a root task)."""
        with self.store() as store:
            task = store.create_task(
                sender_id="operator", recipient_id=AGENT, payload={"body": body}
            )
        return str(task["task_id"])

    def task(self, task_id: str) -> dict[str, Any]:
        with self.store() as store:
            return store.get_task(task_id)

    def hold(self, task_id: str, version: int = 1, kind: str = "task") -> object:
        return self.admin.call(
            "containment.hold",
            kind=kind,
            object_id=task_id,
            owner="core-monitor",
            policy_version=version,
        )

    def release(self, task_id: str, version: int = 1, kind: str = "task") -> object:
        return self.admin.call(
            "containment.release",
            kind=kind,
            object_id=task_id,
            owner="core-monitor",
            policy_version=version,
        )

    def journal(self, kind: str) -> list[dict[str, Any]]:
        import json

        with self.store() as store:
            rows = store._connection.execute(
                "SELECT event_json FROM trace_journal WHERE "
                "json_extract(event_json,'$.kind')=? ORDER BY source_seq",
                (kind,),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]


async def eventually(predicate: Callable[[], object], *, timeout: float = 10) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0.02)


@asynccontextmanager
async def managed_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    handler: Handler,
    *,
    package: Mapping[str, object] | None = None,
) -> AsyncIterator[ManagedHarness]:
    """Serve agentd, install ``worker-1`` as a running package, and run it."""
    # Containment reactions are polled; keep the tests fast but real.
    monkeypatch.setattr(managed_runtime, "WATCH_INTERVAL_SECONDS", 0.05)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "node.json").write_text('{"agent_id":"worker-node"}')
    service_dir = state_dir / "agentd"
    stop = threading.Event()
    service_thread = threading.Thread(
        target=serve, args=(service_dir, stop), daemon=True
    )
    service_thread.start()
    socket_path = socket_path_for(service_dir)
    await eventually(socket_path.exists, timeout=5)
    admin = AgentdClient(
        socket_path, admin_token=(service_dir / "admin.token").read_text().strip()
    )
    registration = cast(
        Mapping[str, object],
        admin.call(
            "connector.register",
            connector_id=CONNECTOR,
            host_type="managed-agent",
            agent_id=AGENT,
            capabilities=["work"],
        ),
    )
    admin.call(
        "managed.reconcile",
        records=[
            {
                "package_id": "test.worker",
                "desired_state": "running",
                "agent_ids": [AGENT],
                "outbound_agents": [],
                **(package or {}),
            }
        ],
    )
    token_path = state_dir / f"connectors/{CONNECTOR}.token"
    token_path.parent.mkdir(mode=0o700, parents=True)
    token_path.write_text(str(registration["token"]) + "\n")
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""agent_id: {AGENT}
name: {AGENT}
description: test
runtime:
  kind: native
  roles: [worker]
  conformance: L1
  heartbeat_interval_sec: 30
skills:
  - id: work
    name: work
    description: work
"""
    )
    monkeypatch.setenv("EDGECITADEL_STATE_DIR", str(state_dir))
    monkeypatch.setenv("EDGECITADEL_CONNECTOR_ID", CONNECTOR)
    runtime = asyncio.create_task(run(config, handler))
    harness = ManagedHarness(service_dir, admin)
    try:

        def session_active() -> bool:
            connectors = cast(list[Mapping[str, object]], admin.call("connector.list"))
            return any(
                c["connector_id"] == CONNECTOR and c["session_active"]
                for c in connectors
            )

        await eventually(session_active)
        yield harness
        assert not runtime.done(), "the runtime loop exited"
    finally:
        runtime.cancel()
        await asyncio.gather(runtime, return_exceptions=True)
        stop.set()
        service_thread.join(timeout=5)
