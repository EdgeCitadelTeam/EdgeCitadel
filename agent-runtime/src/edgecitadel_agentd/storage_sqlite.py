"""Pager scratch policy shared by source writers and offline/read-only tools."""

from __future__ import annotations

import sqlite3


def configure_scratch(db: sqlite3.Connection) -> None:
    """Call immediately after opening, before TEMP objects or transactions exist.

    Rollback and super-journals remain durable files beside their databases.
    Sorts, savepoints and VACUUM scratch must not escape to the system temp
    filesystem. Keeping dirty pages until commit also avoids spill-cycle journal
    headers; admission must separately bound transaction memory and page growth.
    This policy supplies neither that bound nor a completion reservation.
    """
    options = {row[0] for row in db.execute("PRAGMA compile_options")}
    # TEMP_STORE=0 overrides the runtime pragma. Missing compile diagnostics
    # cannot establish whether this SQLite build honors MEMORY either.
    if not options or "TEMP_STORE=0" in options:
        raise sqlite3.NotSupportedError(
            "agentd requires verifiable SQLite memory temporary storage"
        )
    db.execute("PRAGMA temp_store=MEMORY")
    # The boolean setting applies to subsequently attached databases as well.
    db.execute("PRAGMA cache_spill=OFF")
    if (
        db.execute("PRAGMA temp_store").fetchone()[0] != 2
        or db.execute("PRAGMA cache_spill").fetchone()[0] != 0
    ):
        raise sqlite3.NotSupportedError("agentd SQLite scratch policy was not applied")
