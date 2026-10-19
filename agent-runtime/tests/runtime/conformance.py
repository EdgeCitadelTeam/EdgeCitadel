"""Conformance suite every Plugin runtime runs against its own NATS connection.

Exported as pytest-fn builders so Plugin-specific test files can do:

    from edgecitadel_plugin_runtime.tests.conformance import build_conformance_cases
    for name, env, expect in build_conformance_cases():
        ...
"""

from __future__ import annotations
import uuid


def _base(**o):
    e = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": "heartbeat",
        "sender_id": "tester",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": {},
    }
    e.update(o)
    return e


def build_conformance_cases() -> list[tuple[str, dict, str]]:
    """Returns list of (name, envelope, "accept"|"reject")."""
    return [
        ("heartbeat-minimal", _base(), "accept"),
        ("status-online", _base(type="status", agent_state="online"), "accept"),
        (
            "command-ok",
            _base(
                type="command",
                recipient_id="r",
                task_id=str(uuid.uuid4()),
                payload={"body": "x"},
            ),
            "accept",
        ),
        (
            "result-completed",
            _base(
                type="result",
                recipient_id="r",
                task_id=str(uuid.uuid4()),
                task_state="completed",
                payload={"body": "ok"},
            ),
            "accept",
        ),
        (
            "cancel-ok",
            _base(
                type="cancel",
                recipient_id="r",
                task_id=str(uuid.uuid4()),
                payload={"task_id": str(uuid.uuid4())},
            ),
            "accept",
        ),
        ("reject-legacy-receiver_id", {**_base(), "receiver_id": "x"}, "reject"),
        ("reject-legacy-message_type", {**_base(), "message_type": "info"}, "reject"),
        (
            "reject-missing-task_id-on-command",
            _base(type="command", recipient_id="r", payload={"body": "x"}),
            "reject",
        ),
        (
            "reject-bad-state-on-result",
            _base(
                type="result",
                recipient_id="r",
                task_id=str(uuid.uuid4()),
                task_state="done",
                payload={},
            ),
            "reject",
        ),
        ("reject-v2", _base(v=2), "reject"),
    ]
