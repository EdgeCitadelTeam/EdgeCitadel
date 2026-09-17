"""Process-lifetime ownership of one local agentd state directory."""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class WriterActiveError(RuntimeError):
    """Another daemon or maintenance operation owns this state directory."""


@contextmanager
def exclusive_writer(state_dir: Path) -> Iterator[None]:
    state_dir.mkdir(parents=True, exist_ok=True)
    state_dir.chmod(0o700)
    descriptor = os.open(state_dir / "writer.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise WriterActiveError(
                "agentd state directory already has an active writer"
            ) from error
        yield
    finally:
        # Keep the inode: unlinking it would permit a competing replacement lock.
        os.close(descriptor)
