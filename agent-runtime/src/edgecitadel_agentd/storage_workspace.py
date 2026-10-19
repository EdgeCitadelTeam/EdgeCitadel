"""Physical rollback workspace held across the source's complete write cycle."""

from __future__ import annotations

import fcntl
import os
import re
import sqlite3
import stat
import time
from pathlib import Path
from typing import Any

from . import storage_geometry

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
        self.inode_paths = tuple(
            path.with_name(f"{path.name}.inode-{index}") for index in range(2)
        )
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

    def _inode_reserved(self, path: Path) -> bool:
        try:
            info = path.lstat()
        except FileNotFoundError:
            return False
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_size != 0
            or info.st_dev != os.fstat(self.descriptor).st_dev
        ):
            raise sqlite3.OperationalError("invalid completion inode reservation")
        return True

    def _sync_directory(self) -> None:
        directory = os.open(
            self.inode_paths[0].parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def borrow(self) -> None:
        if not self.owned:
            raise sqlite3.ProgrammingError("completion workspace ownership is required")
        if not self.borrowed:
            # Validate every placeholder before deleting any. The service-owned
            # directory and writer lock exclude replacement during this cycle.
            present = [path for path in self.inode_paths if self._inode_reserved(path)]
            # A failed release still enters completion mode: cached ordinary SQL
            # must never remain authorized after we release any allocation.
            self.borrowed = True
            os.ftruncate(self.descriptor, 0)
            os.fsync(self.descriptor)
            for path in present:
                path.unlink()
            self._sync_directory()

    @property
    def reserved(self) -> bool:
        inodes = [self._inode_reserved(path) for path in self.inode_paths]
        info = os.fstat(self.descriptor)
        return (
            all(inodes)
            and info.st_size == WORKSPACE_BYTES + 1
            and info.st_blocks * 512 >= WORKSPACE_BYTES
        )

    def restore(self) -> None:
        if not self.owned:
            raise sqlite3.ProgrammingError("completion workspace ownership is required")
        if self.borrowed or not self.reserved:
            # Remove the publication marker before either kind of allocation.
            # A killed or failed partial refill must remain visibly incomplete.
            os.ftruncate(self.descriptor, WORKSPACE_BYTES)
            os.fsync(self.descriptor)
            os.posix_fallocate(self.descriptor, 0, WORKSPACE_BYTES)
            os.fsync(self.descriptor)
            # The main rollback journal and attached-transaction super-journal
            # each need one inode here. The task journal is outside this quota.
            for path in self.inode_paths:
                if not self._inode_reserved(path):
                    descriptor = os.open(
                        path,
                        os.O_CREAT
                        | os.O_EXCL
                        | os.O_WRONLY
                        | os.O_NOFOLLOW
                        | os.O_CLOEXEC,
                        0o600,
                    )
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
            self._sync_directory()
            # One sparse byte publishes durable byte AND inode reservations.
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


_LEADING_SQL = re.compile(
    r"(?:[\s\ufeff;]+|--[^\n]*(?:\n|$)|/\*.*?(?:\*/|$))*", re.DOTALL
)
_READ_PRAGMAS = frozenset(
    {
        "page_count",
        "page_size",
        "freelist_count",
        "foreign_keys",
        "user_version",
        "journal_mode",
        "synchronous",
        "temp_store",
        "cache_spill",
        "compile_options",
        "database_list",
        "busy_timeout",
        "max_page_count",
    }
)
_INSPECTION_PRAGMAS = frozenset(
    {
        "table_info",
        "table_xinfo",
        "index_list",
        "index_info",
        "index_xinfo",
        "integrity_check",
        "quick_check",
        "foreign_key_check",
    }
)


_FIXED_MAIN_UPDATES = frozenset(
    {
        ("trace_completion_slots", "filled"),
        ("trace_completion_slots", "record"),
        ("trace_sources", "next_source_seq_bytes"),
        ("trace_export_generations", "next_export_seq_bytes"),
        ("trace_presence_counter", "next_id"),
    }
)


class ResultCursor(sqlite3.Cursor):
    """A connection result supports fetching, never a second writer entrypoint."""

    def _check_execution(self) -> None:
        if self.connection.workspace is not None:
            raise sqlite3.ProgrammingError(
                "source writers must use connection execution"
            )

    def execute(self, sql: str, parameters: Any = (), /) -> ResultCursor:
        self._check_execution()
        return super().execute(sql, parameters)

    def executemany(self, sql: str, parameters: Any, /) -> ResultCursor:
        self._check_execution()
        return super().executemany(sql, parameters)

    def executescript(self, sql: str, /) -> ResultCursor:
        self._check_execution()
        return super().executescript(sql)


class ReservedConnection(sqlite3.Connection):
    """Keep allocation ownership around explicit and implicit source transactions.

    A workspace is installed only after storage layout and schema verification.
    Callers use connection execute/executemany and transaction contexts; raw
    cursors/executescript are not writer APIs after installation.
    """

    workspace: CompletionWorkspace | None = None
    _initial_pages: int | None = None
    _closed = False
    _ordinary_main_write = False
    _completion_write_violation = False

    def _authorize_write(
        self,
        action: int,
        table: str | None,
        column: str | None,
        database: str | None,
        trigger: str | None,
    ) -> int:
        if storage_geometry.protects_schema(action, table, column, database):
            if self.workspace is not None and self.workspace.borrowed:
                self._completion_write_violation = True
            return sqlite3.SQLITE_DENY
        if database != "main" or action not in {
            sqlite3.SQLITE_INSERT,
            sqlite3.SQLITE_UPDATE,
            sqlite3.SQLITE_DELETE,
        }:
            return sqlite3.SQLITE_OK
        fixed_update = (
            action == sqlite3.SQLITE_UPDATE and (table, column) in _FIXED_MAIN_UPDATES
        )
        if self.workspace is not None and self.workspace.borrowed:
            if fixed_update:
                return sqlite3.SQLITE_OK
            self._completion_write_violation = True
            return sqlite3.SQLITE_DENY
        # Even fixed-column writes before borrowing are ordinary: they need not
        # have come from a bounded completion, and may have touched many rows.
        self._ordinary_main_write = True
        return sqlite3.SQLITE_OK

    def set_authorizer(self, authorizer: Any) -> None:
        if self.workspace is not None:
            raise sqlite3.ProgrammingError(
                "completion workspace owns SQL authorization"
            )
        super().set_authorizer(authorizer)

    def _recover_workspace(self, workspace: CompletionWorkspace) -> None:
        """Let SQLite recover/reuse its own journals before refilling the reserve."""
        workspace.borrow()
        try:
            super().execute("BEGIN IMMEDIATE")
            version = super().execute("PRAGMA main.user_version").fetchone()[0]
            super().execute(f"PRAGMA main.user_version={int(version)}")
            storage_geometry.verify_journal(self)
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
            if super().execute("PRAGMA temp_store").fetchone()[0] != 2:
                raise sqlite3.NotSupportedError(
                    "completion workspace requires memory scratch"
                )
            for _, schema, _ in super().execute("PRAGMA database_list"):
                if schema == "temp":
                    continue
                if schema not in {"main", "task_state"}:
                    raise sqlite3.NotSupportedError(
                        "completion workspace has an unexpected database"
                    )
                for setting, expected in (
                    ("journal_mode", "delete"),
                    ("synchronous", 3),
                    ("cache_spill", 0),
                ):
                    if (
                        super().execute(f"PRAGMA {schema}.{setting}").fetchone()[0]
                        != expected
                    ):
                        raise sqlite3.NotSupportedError(
                            f"completion workspace requires qualified {schema}.{setting}"
                        )
            self._recover_workspace(workspace)
            storage_geometry.install(self)
            self.workspace = workspace
            super().set_authorizer(self._authorize_write)
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
                self._ordinary_main_write = False
                self._completion_write_violation = False
                # Authorizers run when SQL is prepared, not on every step.
                # Reinstall at each ownership boundary to reauthorize cached SQL.
                super().set_authorizer(self._authorize_write)
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
                self._ordinary_main_write = False
                self._completion_write_violation = False

    def use_completion_workspace(self) -> None:
        if self.workspace is None or not self.in_transaction:
            raise sqlite3.ProgrammingError(
                "reserved completion requires its transaction"
            )
        if not self.workspace.owned:
            raise sqlite3.ProgrammingError("reserved completion has no writer owner")
        if self._ordinary_main_write:
            raise sqlite3.ProgrammingError(
                "completion cannot borrow after ordinary trace writes"
            )
        if not self.workspace.borrowed:
            try:
                self.workspace.borrow()
            finally:
                # Release can fail after freeing space; invalidate cached ordinary
                # statements even when the caller catches that failure.
                super().set_authorizer(self._authorize_write)

    def _check_statement(self, sql: str) -> str:
        # SQLite accepts comments, empty statements and a UTF-8 BOM before SQL.
        # Classify that first real statement, not its first whitespace token.
        statement = _LEADING_SQL.sub("", sql, count=1)
        match = re.match(r"[A-Za-z_]+", statement)
        keyword = match[0].upper() if match else ""
        if self.workspace is None:
            return keyword
        if keyword == "SAVEPOINT" and not self.in_transaction:
            raise sqlite3.ProgrammingError(
                "source savepoints require an outer transaction"
            )
        if keyword in {"COMMIT", "END"}:
            raise sqlite3.ProgrammingError("source commits must use commit()")
        if keyword in {"ATTACH", "DETACH", "VACUUM"}:
            raise sqlite3.ProgrammingError(
                "source layout changes require offline ownership"
            )
        if (
            keyword == "EXPLAIN"
            and re.match(
                r"EXPLAIN(?:\s+QUERY\s+PLAN)?\s+(?:SELECT|WITH)\b",
                statement,
                re.IGNORECASE,
            )
            is None
        ):
            # Some PRAGMAs act at prepare time, even beneath EXPLAIN.
            raise sqlite3.ProgrammingError(
                "source EXPLAIN is outside the qualified SQLite policy"
            )
        if keyword == "PRAGMA":
            pragma = re.fullmatch(
                r'PRAGMA\s+(?:(?:main|task_state|temp|"main"|"task_state"|"temp")\s*\.\s*)?([a-z_]+)\s*(.*?)\s*;?\s*',
                statement,
                re.IGNORECASE | re.DOTALL,
            )
            name, argument = (
                (pragma[1].lower(), pragma[2].rstrip("; \t\r\n"))
                if pragma
                else ("", "")
            )
            if name in _READ_PRAGMAS and not argument:
                return keyword
            if name in _INSPECTION_PRAGMAS:
                return keyword
            if name == "busy_timeout" and re.fullmatch(r"=\s*[0-9]+", argument):
                # The bounded reader changes only this connection wait policy.
                return keyword
            # A lower page ceiling is useful for admission/allocation probes;
            # it cannot change the qualified pager or journal policy. Like any
            # write, it still takes workspace ownership before execution.
            if (
                name == "max_page_count"
                and re.fullmatch(r"=\s*[0-9]+", argument)
                and not self.workspace.borrowed
            ):
                return "PRAGMA_WRITE"
            raise sqlite3.ProgrammingError(
                "source pragma is outside the qualified SQLite policy"
            )
        return keyword

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
        keyword = self._check_statement(sql)
        if keyword not in {"SELECT", "EXPLAIN", "PRAGMA", ""}:
            self._acquire()
        try:
            cursor = super().cursor(factory=ResultCursor)
            return sqlite3.Cursor.execute(cursor, sql, parameters)
        finally:
            if not self.in_transaction:
                self._finish()

    def executemany(self, sql: str, parameters: Any, /) -> sqlite3.Cursor:
        self._check_statement(sql)
        self._acquire()
        try:
            cursor = super().cursor(factory=ResultCursor)
            return sqlite3.Cursor.executemany(cursor, sql, parameters)
        finally:
            if not self.in_transaction:
                self._finish()

    def executescript(self, sql: str, /) -> sqlite3.Cursor:
        if self.workspace is not None:
            raise sqlite3.ProgrammingError(
                "source scripts cannot bypass transaction ownership"
            )
        cursor = super().cursor(factory=ResultCursor)
        return sqlite3.Cursor.executescript(cursor, sql)

    def cursor(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
        if self.workspace is not None:
            raise sqlite3.ProgrammingError(
                "source writers must use connection execution"
            )
        if not args and "factory" not in kwargs:
            kwargs["factory"] = ResultCursor
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
        if self.workspace is not None and self._completion_write_violation:
            raise sqlite3.IntegrityError(
                "completion transaction attempted an ordinary trace write"
            )
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
