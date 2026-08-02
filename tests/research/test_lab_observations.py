"""Append-only evidence contracts for lab controller observations."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.research.lab_config import LabConfigError
from scripts.research.lab_observations import append_observation


def test_append_observation_assigns_ordered_sequences_and_explicit_nulls(tmp_path: Path) -> None:
    path = tmp_path / "lab-observations.jsonl"
    append_observation(path, {"event": "controller.started", "agent_id": None, "reservation_id": None, "task_id": None, "data": {}})
    append_observation(path, {"event": "node.reserved", "agent_id": "shell-1", "reservation_id": "r-1", "task_id": None, "data": {"state": "active"}})
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["sequence"] for row in rows] == [1, 2]
    assert rows[0]["schema_version"] == "lab-observation.v1"
    assert rows[0]["task_id"] is None
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "data",
    [
        {"token": "secret"},
        {"nested": {"authorization": "Bearer value"}},
        {"message": "Bearer live-credential"},
        {"message": "NATS_TOKEN=live-credential"},
        {"message": "-----BEGIN PRIVATE KEY-----"},
        {"values": ("Bearer live-credential",)},
        {"path": "/tmp/live"},
    ],
)
def test_append_observation_rejects_secrets_and_absolute_paths(tmp_path: Path, data: dict[str, object]) -> None:
    with pytest.raises(LabConfigError):
        append_observation(tmp_path / "lab-observations.jsonl", {"event": "bad", "agent_id": None, "reservation_id": None, "task_id": None, "data": data})
