"""Physical rollback workspace held across the source's complete write cycle."""

from __future__ import annotations

import fcntl
import os
import sqlite3
import stat
import time
from pathlib import Path
from typing import Any

WORKSPACE_BYTES = 32 * 1024 * 1024


class CompletionWorkspace:
    """The reserve inode is also the cross-connection writer lock.

    Never modifies a SQLite journal. Its own allocation is released only while
    an owned completion transaction uses preallocated database records. Ownership
    extends through SQLite completion and reserve restoration, preventing another
    handle from entering the release/reallocation interval.
    """

    def __init__(self, path: Path) -> None:
        self.descriptor = os.open(
            path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600
        )
        self.owned = False
        self.borrowed = False
        try:
            info = os.fstat(self.descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise sqlite3.OperationalError("invalid completion workspace ownership")
            directory = os.open(
                path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self.acquire()
        except BaseException:
            os.close(self.descriptor)
            raise

    def acquire(self) -> None:
        if self.owned:
            return
        deadline = time.monotonic() + 5
        while True:
            try:
                fcntl.flock(self.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise sqlite3.OperationalError(
                        "completion workspace writer is busy"
                    )
                time.sleep(0.005)
        self.owned = True

    def borrow(self) -> None:
        if not self.owned:
            raise sqlite3.ProgrammingError("completion workspace ownership is required")
        if not self.borrowed:
            os.ftruncate(self.descriptor, 0)
            os.fsync(self.descriptor)
            self.borrowed = True

    @property
    def reserved(self) -> bool:
        info = os.fstat(self.descriptor)
        return (
            info.st_size == WORKSPACE_BYTES + 1
            and info.st_blocks * 512 >= WORKSPACE_BYTES
        )

    def restore(self) -> None:
        if not self.owned:
            raise sqlite3.ProgrammingError("completion workspace ownership is required")
        if self.borrowed or not self.reserved:
            os.posix_fallocate(self.descriptor, 0, WORKSPACE_BYTES)
            os.fsync(self.descriptor)
            # One sparse byte beyond the reserved range is a completion marker.
            # Publish it only after fallocate and fsync succeed. File length and
            # st_blocks alone can mistake a failed partial allocation plus extent
            # metadata for a completely allocated range after another writer dies.
            os.ftruncate(self.descriptor, WORKSPACE_BYTES + 1)
            os.fsync(self.descriptor)
        self.borrowed = False

    def release(self) -> None:
        if self.owned:
            fcntl.flock(self.descriptor, fcntl.LOCK_UN)
            self.owned = False

    def close(self) -> None:
        self.release()
        os.close(self.descriptor)


class ReservedConnection(sqlite3.Connection):
    """Keep allocation ownership around explicit and implicit source transactions.

    A workspace is installed only after storage layout and schema verification.
    Callers use connection execute/executemany and transaction contexts; raw
    cursors/executescript are not writer APIs after installation.
    """

    workspace: CompletionWorkspace | None = None
    _initial_pages: int | None = None
    _closed = False

    def _recover_workspace(self, workspace: CompletionWorkspace) -> None:
        """Let SQLite recover/reuse its own journals before refilling the reserve."""
        workspace.borrow()
        try:
            super().execute("BEGIN IMMEDIATE")
            version = super().execute("PRAGMA main.user_version").fetchone()[0]
            super().execute(f"PRAGMA main.user_version={int(version)}")
            super().commit()
        except BaseException:
            super().rollback()
            raise
        workspace.restore()

    def install_workspace(self, workspace: CompletionWorkspace) -> None:
        """Recover under writer ownership before restoring physical reservation.

        A killed writer may leave a hot *or incomplete* journal occupying the
        borrowed allocation. Refilling first can deadlock recovery at quota.
        SQLite owns journal recovery/reuse: a header-only transaction cleans an
        incomplete journal too. Attached pair members must already be opened.
        """
        if self.workspace is not None or self.in_transaction or not workspace.owned:
            raise sqlite3.ProgrammingError("invalid workspace installation boundary")
        try:
            if super().execute("PRAGMA main.page_size").fetchone()[0] != 4096:
                raise sqlite3.NotSupportedError(
                    "completion workspace requires 4096-byte trace pages"
                )
            self._recover_workspace(workspace)
            self.workspace = workspace
        finally:
            workspace.release()

    def _acquire(self) -> None:
        if self.workspace is not None and not self.workspace.owned:
            self.workspace.acquire()
            try:
                if self.in_transaction:
                    raise sqlite3.ProgrammingError(
                        "SQLite transaction has no workspace owner"
                    )
                # An already-open handle can outlive a different writer process.
                # It needs the same recovery ordering as a fresh connection.
                if not self.workspace.reserved:
                    self._recover_workspace(self.workspace)
                else:
                    self.workspace.restore()
                self._initial_pages = (
                    super().execute("PRAGMA main.page_count").fetchone()[0]
                )
            except BaseException:
                self.workspace.release()
                raise

    def _finish(self) -> None:
        if self.workspace is not None and self.workspace.owned:
            try:
                self.workspace.restore()
            finally:
                self.workspace.release()
                self._initial_pages = None

    def use_completion_workspace(self) -> None:
        if self.workspace is None or not self.in_transaction:
            raise sqlite3.ProgrammingError(
                "reserved completion requires its transaction"
            )
        if not self.workspace.owned:
            raise sqlite3.ProgrammingError("reserved completion has no writer owner")
        self.workspace.borrow()

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
        # Source statements are trusted program SQL. SELECT/EXPLAIN/PRAGMA outside
        # transactions do not write application rows; other statements include
        # BEGIN and WITH-prefixed writes and must acquire before SQLite ownership.
        keyword = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        if (
            self.workspace is not None
            and keyword == "SAVEPOINT"
            and not self.in_transaction
        ):
            raise sqlite3.ProgrammingError(
                "source savepoints require an outer transaction"
            )
        if self.workspace is not None and keyword in {"COMMIT", "END"}:
            raise sqlite3.ProgrammingError("source commits must use commit()")
        if keyword not in {"SELECT", "EXPLAIN", "PRAGMA", ""}:
            self._acquire()
        try:
            return super().execute(sql, parameters)
        finally:
            if not self.in_transaction:
                self._finish()

    def executemany(self, sql: str, parameters: Any, /) -> sqlite3.Cursor:
        self._acquire()
        try:
            return super().executemany(sql, parameters)
        finally:
            if not self.in_transaction:
                self._finish()

    def executescript(self, sql: str, /) -> sqlite3.Cursor:
        if self.workspace is not None:
            raise sqlite3.ProgrammingError(
                "source scripts cannot bypass transaction ownership"
            )
        return super().executescript(sql)

    def cursor(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
        if self.workspace is not None:
            raise sqlite3.ProgrammingError(
                "source writers must use connection execution"
            )
        return super().cursor(*args, **kwargs)

    def __enter__(self) -> ReservedConnection:
        self._acquire()
        return self

    def __exit__(self, kind: Any, value: Any, traceback: Any) -> bool:
        if kind is not None:
            self.rollback()
        else:
            try:
                self.commit()
            except BaseException:
                self.rollback()
                raise
        return False

    def commit(self) -> None:
        if self.workspace is not None and self.workspace.borrowed:
            pages = super().execute("PRAGMA main.page_count").fetchone()[0]
            if pages != self._initial_pages:
                raise sqlite3.IntegrityError(
                    "reserved completion changed database allocation"
                )
        try:
            super().commit()
        finally:
            if not self.in_transaction:
                self._finish()

    def rollback(self) -> None:
        try:
            super().rollback()
        finally:
            if not self.in_transaction:
                self._finish()

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.rollback()
        finally:
            try:
                super().close()
                self._closed = True
            finally:
                if self.workspace is not None:
                    self.workspace.close()
                    self.workspace = None
