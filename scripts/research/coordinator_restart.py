"""One-shot runner-to-host restart handshake for W7 artifact trials."""

from __future__ import annotations

import asyncio
import os
import stat
import time
from pathlib import Path

_REQUEST_NAME = "coordinator-restart.request"
_ACKNOWLEDGEMENT_NAME = "coordinator-restart.complete"
_REQUEST_CONTENTS = b"restart\n"
_ACKNOWLEDGEMENT_CONTENTS = b"complete\n"
_POLL_SECONDS = 0.01


def _path(control_dir: Path, name: str) -> Path:
    if not isinstance(control_dir, Path) or not control_dir.is_dir():
        raise ValueError("invalid restart control directory")
    return control_dir / name


def _read_regular(path: Path, expected: bytes, error: str) -> bool:
    try:
        info = path.stat()
        contents = path.read_bytes()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise RuntimeError(error) from exc
    if not stat.S_ISREG(info.st_mode) or contents != expected:
        raise RuntimeError(error)
    return True


def _write_new(path: Path, contents: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise RuntimeError("coordinator restart is already requested") from None
    try:
        os.write(descriptor, contents)
    finally:
        os.close(descriptor)


async def request_restart(control_dir: Path, timeout_seconds: float) -> None:
    """Ask the host executor to restart the declared coordinator and await completion."""
    if type(timeout_seconds) not in {int, float} or timeout_seconds <= 0:
        raise ValueError("invalid restart timeout")
    request = _path(control_dir, _REQUEST_NAME)
    acknowledgement = _path(control_dir, _ACKNOWLEDGEMENT_NAME)
    if _read_regular(
        acknowledgement, _ACKNOWLEDGEMENT_CONTENTS, "invalid restart acknowledgement"
    ):
        raise RuntimeError("coordinator restart was already completed")
    _write_new(request, _REQUEST_CONTENTS)
    deadline = time.monotonic() + float(timeout_seconds)
    while True:
        if _read_regular(
            acknowledgement,
            _ACKNOWLEDGEMENT_CONTENTS,
            "invalid restart acknowledgement",
        ):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("coordinator restart acknowledgement timed out")
        await asyncio.sleep(_POLL_SECONDS)


def wait_for_restart_request(control_dir: Path, timeout_seconds: float) -> None:
    """Block the host executor until its runner requests the one declared restart."""
    if type(timeout_seconds) not in {int, float} or timeout_seconds <= 0:
        raise ValueError("invalid restart timeout")
    request = _path(control_dir, _REQUEST_NAME)
    deadline = time.monotonic() + float(timeout_seconds)
    while True:
        if _read_regular(
            request, _REQUEST_CONTENTS, "invalid coordinator restart request"
        ):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("coordinator restart request timed out")
        time.sleep(_POLL_SECONDS)


def acknowledge_restart(control_dir: Path) -> None:
    """Persist host completion after the declared coordinator has been recreated."""
    request = _path(control_dir, _REQUEST_NAME)
    acknowledgement = _path(control_dir, _ACKNOWLEDGEMENT_NAME)
    if not _read_regular(
        request, _REQUEST_CONTENTS, "invalid coordinator restart request"
    ):
        raise RuntimeError("coordinator restart was not requested")
    _write_new(acknowledgement, _ACKNOWLEDGEMENT_CONTENTS)


__all__ = ["acknowledge_restart", "request_restart", "wait_for_restart_request"]
