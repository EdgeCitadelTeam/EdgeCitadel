"""Secret-safe append-only observation journal for lab evidence."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping

from scripts.research.lab_config import LabConfigError

_SECRET_KEY = re.compile(r"token|secret|password|authorization|credential", re.I)
_SECRET_VALUE = re.compile(
    r"(?:\bbearer\s+\S+|\bnats_token\s*=|-----BEGIN [A-Z ]*PRIVATE KEY-----)",
    re.I,
)
_WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]")


def _reject_unsafe(value: object, key: str = "") -> None:
    if _SECRET_KEY.search(key):
        raise LabConfigError("observation contains a secret-shaped key")
    if isinstance(value, Mapping):
        for child_key, child_value in value.items():
            _reject_unsafe(child_value, str(child_key))
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_unsafe(child, key)
    elif isinstance(value, str):
        if _SECRET_VALUE.search(value):
            raise LabConfigError("observation contains a secret-shaped value")
        if value.startswith("/") or _WINDOWS_PATH.match(value):
            raise LabConfigError("observation contains an absolute transient path")


def _acquire_lock(path: Path) -> int:
    lock_path = path.with_suffix(path.suffix + ".lock")
    for _ in range(200):
        try:
            return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            time.sleep(0.01)
    raise LabConfigError("observation journal lock is unavailable")


def append_observation(path: Path, observation: Mapping[str, object]) -> None:
    required = ("event", "agent_id", "reservation_id", "task_id", "data")
    if not isinstance(path, Path) or any(name not in observation for name in required):
        raise LabConfigError("observation is incomplete")
    _reject_unsafe(observation)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    lock = _acquire_lock(path)
    try:
        sequence = 1
        if path.exists():
            try:
                sequence = sum(1 for line in path.read_text().splitlines() if line) + 1
            except UnicodeDecodeError as error:
                raise LabConfigError("observation journal is invalid") from error
        record = {
            "schema_version": "lab-observation.v1",
            "sequence": sequence,
            "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            **dict(observation),
        }
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        descriptor = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.chmod(path, 0o600)
    finally:
        os.close(lock)
        path.with_suffix(path.suffix + ".lock").unlink(missing_ok=True)


__all__ = ["append_observation"]
