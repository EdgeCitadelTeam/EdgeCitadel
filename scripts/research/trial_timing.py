"""One-shot runner/host acknowledgements for resource-window boundaries."""

from __future__ import annotations

import asyncio
import os
import stat
from pathlib import Path

_READY_CONTENTS = b"ready\n"
_ACKNOWLEDGEMENT_CONTENTS = b"ack\n"
_POLL_SECONDS = 0.01


class TrialWindowSignal:
    def __init__(self, control_dir: Path, *, timeout_seconds: float = 10) -> None:
        if not control_dir.is_dir() or timeout_seconds <= 0:
            raise ValueError("invalid trial timing control")
        self._control_dir = control_dir
        self._timeout_seconds = timeout_seconds

    async def await_start_acknowledgement(self) -> None:
        await self._await_acknowledgement("start")

    async def await_end_acknowledgement(self) -> None:
        await self._await_acknowledgement("end")

    async def _await_acknowledgement(self, phase: str) -> None:
        ready = self._control_dir / f"trial-window.{phase}.ready"
        acknowledgement = self._control_dir / f"trial-window.{phase}.ack"
        _write_new(ready, _READY_CONTENTS)
        deadline = asyncio.get_running_loop().time() + self._timeout_seconds
        while True:
            if _has_exact_contents(acknowledgement, _ACKNOWLEDGEMENT_CONTENTS):
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("trial timing acknowledgement timed out")
            await asyncio.sleep(_POLL_SECONDS)


def _has_exact_contents(path: Path, expected: bytes) -> bool:
    try:
        info = path.stat()
        contents = path.read_bytes()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise RuntimeError("invalid trial timing acknowledgement") from error
    if not stat.S_ISREG(info.st_mode) or contents != expected:
        raise RuntimeError("invalid trial timing acknowledgement")
    return True


def _write_new(path: Path, contents: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise RuntimeError("trial timing signal already exists") from None
    try:
        os.write(descriptor, contents)
    finally:
        os.close(descriptor)


__all__ = ["TrialWindowSignal"]
