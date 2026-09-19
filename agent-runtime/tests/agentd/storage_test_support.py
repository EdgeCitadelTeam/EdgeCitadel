"""Owned database fixtures for paired reads and genuine shared-schema migrations."""

import sqlite3
from pathlib import Path

from edgecitadel_agentd.storage_pair import TASK_TABLES, task_database_path
from edgecitadel_agentd.storage_layout import StorageLayout


def paired_connect(path, **kwargs):
    db = sqlite3.connect(path, **kwargs)
    if db.execute("PRAGMA user_version").fetchone()[0] >= 24:
        filename = db.execute("PRAGMA database_list").fetchone()[2]
        db.execute(
            "ATTACH DATABASE ? AS task_state",
            (str(task_database_path(Path(filename))),),
        )
    return db


def flatten_connection(db):
    """Build an old shared fixture before downgrading its schema version.

    Current runtime never writes this layout. All files belong to the test.
    Preserve rowids and rows; leave the paired destination empty for migration.
    """
    if not db.execute(
        "SELECT 1 FROM main.sqlite_schema WHERE name='storage_pair'"
    ).fetchone():
        return
    schemas = {row[1] for row in db.execute("PRAGMA database_list")}
    if "task_state" not in schemas:
        filename = db.execute("PRAGMA database_list").fetchone()[2]
        db.execute(
            "ATTACH DATABASE ? AS task_state",
            (str(task_database_path(Path(filename))),),
        )
    for (name,) in db.execute(
        "SELECT name FROM temp.sqlite_schema WHERE type='trigger' AND name LIKE 'pair_%'"
    ).fetchall():
        db.execute(f'DROP TRIGGER temp."{name}"')
    for table in TASK_TABLES:
        objects = db.execute(
            "SELECT type,sql FROM task_state.sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END",
            (table,),
        ).fetchall()
        if not objects:
            continue
        for _, sql in objects:
            db.execute(sql)
        columns = ",".join(
            '"' + row[1] + '"'
            for row in db.execute(f'PRAGMA task_state.table_info("{table}")')
        )
        db.execute(
            f'INSERT INTO main."{table}"(rowid,{columns}) SELECT rowid,{columns} FROM task_state."{table}"'
        )
    for table in reversed(TASK_TABLES):
        db.execute(f'DROP TABLE IF EXISTS task_state."{table}"')
    db.execute("DROP TABLE main.storage_pair")
    db.execute("DROP TABLE task_state.storage_pair")


class FixtureLayout(StorageLayout):
    """Owned ordinary-directory fixture; never production quota evidence."""

    @property
    def trace_directory(self):
        return self.state_directory

    def verify(self):
        pass


def stage_restore(**kwargs):
    from edgecitadel_agentd.restore import stage_restore as restore

    return restore(
        **kwargs, destination_layout=FixtureLayout(kwargs["destination_dir"].resolve())
    )


def activate_restored_state(**kwargs):
    from edgecitadel_agentd.restore_activation import (
        activate_restored_state as activate,
    )

    return activate(**kwargs, layout=FixtureLayout(kwargs["state_dir"].resolve()))


def compact_database(state_dir):
    from edgecitadel_agentd.storage_maintenance import compact_database as compact

    return compact(state_dir, layout=FixtureLayout(state_dir.resolve()))
