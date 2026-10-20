"""Two in-process agentd nodes joined only by their real outbox and ingest boundary.

Each node is an ordinary AgentdStore with its own enrolled identity. Agents call
the private RPC switch (``service.dispatch``) exactly as their runtimes do; the
"broker" moves committed outbox envelopes into the peer's ingest, so no NATS is
needed and no store internals are bypassed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from edgecitadel_agentd.service import dispatch as rpc_dispatch
from edgecitadel_agentd.store import AgentdStore


@dataclass
class Agent:
    connector_id: str
    token: str
    session_id: str


class Node:
    def __init__(self, root: Path, node_id: str) -> None:
        root.mkdir(parents=True, exist_ok=True)
        (root / "node.json").write_text(json.dumps({"agent_id": node_id}))
        self.node_id = node_id
        self.store = AgentdStore(root / "agentd" / "agentd.sqlite3")
        self.agents: dict[str, Agent] = {}
        self._grants: dict[str, list[str]] = {}

    def close(self) -> None:
        self.store.close()

    def managed(self, agent_id: str, *, outbound: list[str] = ()) -> Agent:
        connector_id = f"managed-{agent_id}"
        token = self.store.register_connector(
            connector_id=connector_id,
            host_type="managed-agent",
            agent_id=agent_id,
            capabilities=[],
        )
        session = self.store.open_session(connector_id=connector_id, token=token)
        self._grants[agent_id] = list(outbound)
        self.store.reconcile_managed_agents(
            [
                {
                    "package_id": f"test.{name}",
                    "desired_state": "running",
                    "agent_ids": [name],
                    "outbound_agents": grant,
                }
                for name, grant in self._grants.items()
            ]
        )
        self.agents[agent_id] = Agent(connector_id, token, str(session["session_id"]))
        return self.agents[agent_id]

    def rpc(self, agent_id: str, operation: str, **params: Any) -> Any:
        agent = self.agents[agent_id]
        return rpc_dispatch(
            self.store,
            {
                "version": 1,
                "operation": operation,
                "connector_id": agent.connector_id,
                "token": agent.token,
                "params": params,
            },
        )

    def submit(self, agent_id: str, body: str = "root request") -> str:
        """An operator command addressed to a local agent (a root task)."""
        task = self.store.create_task(
            sender_id="operator", recipient_id=agent_id, payload={"body": body}
        )
        return str(task["task_id"])

    def start(self, agent_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Claim, bind and run the next task, as managed_runtime does."""
        session = self.agents[agent_id].session_id
        task = self.rpc(agent_id, "task.claim", session_id=session)
        assert task is not None, "no claimable task"
        binding = self.rpc(
            agent_id,
            "trace.bind",
            schema_version=1,
            request_id=str(uuid4()),
            session_id=session,
            task_id=task["task_id"],
            context_id=None,
        )["result"]
        self.rpc(
            agent_id,
            "task.transition",
            task_id=task["task_id"],
            state="running",
            session_id=session,
        )
        return task, binding

    def dispatch(
        self, agent_id: str, binding: dict[str, Any], recipient: str, **extra: Any
    ) -> dict[str, Any]:
        return self.rpc(
            agent_id,
            "trace.dispatch",
            schema_version=1,
            request_id=extra.pop("request_id", str(uuid4())),
            binding_id=binding["binding_id"],
            recipient_id=recipient,
            request=extra.pop("request", "child request"),
            skill_id=None,
            deadline_at_ms=None,
            **extra,
        )

    def complete(self, agent_id: str, task_id: str, result: dict[str, Any]) -> None:
        self.rpc(
            agent_id,
            "task.transition",
            task_id=task_id,
            state="completed",
            session_id=self.agents[agent_id].session_id,
            result=result,
        )

    def events(self, kind: str, phase: str | None = None) -> list[dict[str, Any]]:
        rows = self.store._connection.execute(
            "SELECT event_json FROM trace_journal_all WHERE json_extract(event_json,'$.kind')=? "
            "ORDER BY received_at_ms,source_seq",
            (kind,),
        ).fetchall()
        events = [json.loads(row[0]) for row in rows]
        return [e for e in events if phase is None or e["phase"] == phase]


def deliver(source: Node, target: Node) -> int:
    """Publish every unheld outbox envelope of source into target's ingest."""
    pending = source.store.pending_transport()
    for message in pending:
        target.store.ingest_transport_envelope(message["envelope"])
        source.store.mark_transport_published(str(message["message_id"]))
    return len(pending)


def ref(event: dict[str, Any]) -> dict[str, str]:
    return {key: event[key] for key in ("node_id", "source_epoch", "event_id")}
