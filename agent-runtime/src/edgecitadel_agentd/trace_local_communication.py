"""Observe committed same-host RPC delivery without involving the NATS outbox."""

from __future__ import annotations

import hashlib
import logging
from uuid import UUID

from .node_state import read_node
from .trace_transport import TransportTrace

log = logging.getLogger(__name__)


def identity(task_id, message_type, phase):
    return str(
        UUID(
            bytes=hashlib.sha256(f"{task_id}/{message_type}/{phase}".encode()).digest()[
                :16
            ],
            version=4,
        )
    )


class LocalCommunicationTrace:
    def __init__(self, store):
        self.store = store
        self.trace = None

    def observe(self, request, response):
        """Called only after an authenticated RPC committed successfully.

        Stable task-owned identities deduplicate repeated reads and RPC retries.
        Observation has a separate optional transaction: quota pressure must not
        roll back task completion or turn its success response into a failure.
        """
        operation = request.get("operation")
        if operation not in {
            "task.create",
            "trace.dispatch",
            "task.transition",
            "task.get",
        }:
            return
        try:
            value = response
            if operation == "trace.dispatch":
                if response.get("status") != "ok":
                    return
                value = response["result"]
            task_id = value.get("task_id")
            if not task_id:
                return
            task = self.store.get_task(task_id)
            with self.store._lock:
                if not all(
                    self.store._agent_is_local_locked(task[key])
                    for key in ("sender_id", "recipient_id")
                ):
                    return
            actor = self.store.authenticate(request["connector_id"], request["token"])[
                "agent_id"
            ]
            if operation in {"task.create", "trace.dispatch"}:
                message_type, phase = "command", "durably_accepted"
                sender, recipient = task["sender_id"], task["recipient_id"]
                content = task["payload"]
            elif task["state"] in {"completed", "failed", "rejected"}:
                if operation == "task.get" and actor != task["sender_id"]:
                    return
                message_type = "result"
                phase = (
                    "caller_accepted" if operation == "task.get" else "durably_accepted"
                )
                sender, recipient = task["recipient_id"], task["sender_id"]
                content = task["result"] or {}
            else:
                return
            node = read_node(self.store.state_directory.parent)
            if node is None:
                return
            if self.trace is None:
                self.trace = TransportTrace(self.store, node["agent_id"])
            envelope = {
                "v": 1,
                "timestamp": self.store._iso_timestamp(task["updated_at_ms"]),
                "id": identity(task_id, message_type, "message"),
                "type": message_type,
                "sender_id": sender,
                "recipient_id": recipient,
                "task_id": task_id,
                "payload": {**content, "trace_id": task["trace_id"]},
            }
            if message_type == "result":
                envelope["task_state"] = task["state"]
            if task.get("context_id"):
                envelope["context_id"] = task["context_id"]
            self.trace.record(
                phase,
                envelope=envelope,
                attributes={"provenance": "agentd_sqlite"},
                content={"body": content},
                event_id=identity(task_id, message_type, phase),
            )
        except Exception:  # noqa: BLE001 - optional observation cannot undo a committed RPC
            log.warning("Local communication observation unavailable")
