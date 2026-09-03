"""JetStream topology for the deterministic E2E fixture agent."""

from __future__ import annotations

import hashlib
import re

_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_KINDS = frozenset({"task", "result", "transient"})


def _require_identifier(value: object, label: str) -> str:
    if type(value) is not str or _ID_PATTERN.fullmatch(value) is None:
        raise ValueError(f"invalid {label}")
    return value


def task_stream_config(run_id: str) -> dict[str, object]:
    _require_identifier(run_id, "run_id")
    return {
        "name": "AGENT_INBOX",
        "subjects": ["agents.*.inbox"],
        "retention": "workqueue",
        "storage": "file",
        "max_age_ns": 86_400_000_000_000,
        "max_bytes": 1_073_741_824,
        "max_msg_size": 1_048_576,
        "discard": "new",
        "duplicate_window_ns": 300_000_000_000,
    }


def transient_stream_config(run_id: str) -> dict[str, object]:
    _require_identifier(run_id, "run_id")
    return {
        "name": "TRANSIENT_EVENTS",
        "subjects": [
            "agents.*.task_progress.>",
            "agents.*.heartbeat",
            "agents.*.status",
        ],
        "retention": "limits",
        "storage": "file",
        "max_age_ns": 3_600_000_000_000,
        "max_bytes": 1_073_741_824,
        "max_msg_size": 1_048_576,
        "discard": "old",
        "duplicate_window_ns": 300_000_000_000,
    }


def durable_name(kind: str, run_id: str, agent_id: str) -> str:
    if kind not in _KINDS:
        raise ValueError("invalid durable kind")
    normalized_run_id = _require_identifier(run_id, "run_id")
    normalized_agent_id = _require_identifier(agent_id, "agent_id")
    digest = hashlib.sha256(
        f"{kind}\0{normalized_run_id}\0{normalized_agent_id}".encode()
    ).hexdigest()[:24]
    return f"ec_{kind}_{digest}"


__all__ = ["durable_name", "task_stream_config", "transient_stream_config"]
