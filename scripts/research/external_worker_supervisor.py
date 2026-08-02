"""Supervise one crashable native fixture child from run-owned state."""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

from scripts.research.fixtures.native_control import load_native_config

_POLL_SECONDS = 0.05


def _read_config(path: Path) -> bytes:
    # Native-control validates the contents; this read gives the restart generation.
    load_native_config(path)
    return path.read_bytes()


def _write_status(
    path: Path,
    status: str,
    pid: int | None,
    exit_code: int | None,
    generation: str | None,
) -> None:
    pid_text = "" if pid is None else str(pid)
    exit_text = "" if exit_code is None else str(exit_code)
    generation_text = "" if generation is None else generation
    payload = (
        f"status={status}\npid={pid_text}\nexit_code={exit_text}\n"
        f"generation={generation_text}\n"
    )
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, payload.encode("ascii"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def supervise(
    config_path: Path,
    event_path: Path,
    status_path: Path,
    environ: Mapping[str, str] = os.environ,
) -> None:
    """Run a child until configuration replacement; never hide a crash by looping."""
    child: subprocess.Popen[bytes] | None = None
    generation: bytes | None = None
    generation_id: str | None = None
    while True:
        try:
            candidate = _read_config(config_path)
        except (OSError, ValueError):
            _write_status(status_path, "invalid-config", None, None, None)
            time.sleep(_POLL_SECONDS)
            continue
        if candidate != generation:
            if child is not None and child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
            child_environment = dict(environ)
            child_environment["EC_EVENT_LOG"] = str(event_path)
            child = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "scripts.research.fixtures.native_control",
                    "--config",
                    str(config_path),
                ],
                env=child_environment,
            )
            generation = candidate
            generation_id = hashlib.sha256(candidate).hexdigest()
            _write_status(status_path, "running", child.pid, None, generation_id)
        if child is not None:
            exit_code = child.poll()
            if exit_code is not None:
                _write_status(
                    status_path, "exited", child.pid, exit_code, generation_id
                )
                child = None
        time.sleep(_POLL_SECONDS)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Supervise a crashable native fixture."
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--status", required=True, type=Path)
    arguments = parser.parse_args(argv)
    if not all(
        path.is_absolute()
        for path in (arguments.config, arguments.events, arguments.status)
    ):
        return 2
    try:
        supervise(arguments.config, arguments.events, arguments.status)
    except (OSError, subprocess.SubprocessError):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
