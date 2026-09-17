import os
import sqlite3
from contextlib import closing
from uuid import uuid4

import pytest

from aggregator import database, trace_payloads, trace_store
from aggregator.core_storage import StorageManifest, StorageUnavailable, storage_lease
from aggregator.core_storage_identity import (
    FUNCTION,
    IDENTITY_SCHEMA,
    IDENTITY_TABLE,
    MIRROR_TABLES,
    TRACE_TABLES,
    attest_role,
    storage_fences,
)


def provision(path, manifest, role, retained=False):
    if role == "mirror" or retained:
        database.init_db(str(path))
    with closing(sqlite3.connect(path)) as db:
        if role == "trace":
            trace_store.initialize(db)
            trace_payloads.prepare(db)
            while not trace_payloads.migrate_batch(db):
                pass
        tables = {IDENTITY_TABLE} | (TRACE_TABLES if role == "trace" else MIRROR_TABLES)
        if retained:
            tables |= MIRROR_TABLES
        with db:
            db.execute(IDENTITY_SCHEMA)
            db.execute(
                "INSERT INTO core_storage_identity VALUES(?,?)",
                (manifest.generation, role),
            )
            for sql in storage_fences(tables, manifest.generation).values():
                db.execute(sql)
    return tables


@pytest.fixture
def pair(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "mirror.db"))
    manifest = StorageManifest(str(uuid4()), "trace.db", "mirror.db")
    for role in ("trace", "mirror"):
        provision(tmp_path / getattr(manifest, role), manifest, role)
    for name in ("lifecycle.lock", "collector.lock"):
        (tmp_path / name).touch()
    return tmp_path, manifest


def files(root):
    return {p.name: p.read_bytes() for p in root.iterdir()}


@pytest.mark.parametrize("retained", [False, True])
def test_actual_schemas_are_attested_without_writes(tmp_path, monkeypatch, retained):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "mirror.db"))
    manifest = StorageManifest(str(uuid4()), "trace.db", "mirror.db")
    provision(tmp_path / "trace.db", manifest, "trace", retained)
    before = files(tmp_path)
    with closing(
        sqlite3.connect((tmp_path / "trace.db").as_uri() + "?mode=ro", uri=True)
    ) as db:
        db.execute("BEGIN")
        actions = []

        def authorizer(action, *_):
            actions.append(action)
            return sqlite3.SQLITE_OK

        db.set_authorizer(authorizer)
        attest_role(db, manifest, "trace")
        assert set(actions) <= {
            sqlite3.SQLITE_SELECT,
            sqlite3.SQLITE_READ,
            sqlite3.SQLITE_FUNCTION,
        }
        db.set_authorizer(None)
        db.rollback()
    assert files(tmp_path) == before


def test_pair_and_hardlink_alias(pair):
    root, manifest = pair
    with storage_lease(root, "reader"):
        paths = manifest.paths(root)
        for role, path in paths.items():
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
                db.execute("BEGIN")
                attest_role(db, manifest, role)
    (root / "mirror.db").unlink()
    os.link(root / "trace.db", root / "mirror.db")
    before = files(root)
    with pytest.raises(StorageUnavailable, match="storage_roles_overlap"):
        manifest.paths(root)
    assert files(root) == before


@pytest.mark.parametrize(
    "damage",
    [
        "generation",
        "role",
        "missing_guard",
        "altered_guard",
        "extra_identity",
        "extra_table",
        "view",
        "partial_mirror",
    ],
)
def test_invalid_roles_refuse_without_mutation(pair, damage):
    root, manifest = pair
    path = root / "trace.db"
    with closing(sqlite3.connect(path)) as db:
        db.create_function(FUNCTION, 0, lambda: manifest.generation)
        with db:
            if damage == "generation":
                db.execute(
                    "UPDATE core_storage_identity SET generation=?", (str(uuid4()),)
                )
            elif damage == "role":
                db.execute("UPDATE core_storage_identity SET role='mirror'")
            elif damage == "extra_identity":
                db.execute(
                    "INSERT INTO core_storage_identity SELECT * FROM core_storage_identity"
                )
            elif damage in ("missing_guard", "altered_guard"):
                name = next(
                    iter(
                        storage_fences(
                            TRACE_TABLES | {IDENTITY_TABLE}, manifest.generation
                        )
                    )
                )
                db.execute(f"DROP TRIGGER {name}")
                if damage == "altered_guard":
                    db.execute(
                        f"CREATE TRIGGER {name} BEFORE INSERT ON core_storage_identity BEGIN SELECT 1; END"
                    )
            elif damage == "extra_table":
                db.execute("CREATE TABLE sqlitex_unexpected(value)")
            elif damage == "view":
                db.execute("CREATE VIEW unexpected AS SELECT 1")
            elif damage == "partial_mirror":
                db.execute("CREATE TABLE messages(value)")
    before = files(root)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        with pytest.raises(StorageUnavailable):
            attest_role(db, manifest, "trace")
    assert files(root) == before


@pytest.mark.parametrize("capability", ["absent", "wrong", "null"])
def test_unrecognized_writer_cannot_change_identity(pair, capability):
    root, manifest = pair
    for role in ("trace", "mirror"):
        with closing(sqlite3.connect(root / getattr(manifest, role))) as db:
            if capability != "absent":
                db.create_function(
                    FUNCTION, 0, lambda: None if capability == "null" else "wrong"
                )
            before = list(db.iterdump())
            with pytest.raises(sqlite3.Error), db:
                db.execute("UPDATE core_storage_identity SET role='wrong'")
            assert list(db.iterdump()) == before


def test_requires_snapshot_and_ignores_temp_identity(pair):
    root, manifest = pair
    with closing(sqlite3.connect(root / "trace.db")) as db:
        with pytest.raises(StorageUnavailable, match="storage_snapshot_required"):
            attest_role(db, manifest, "trace")
        db.execute("CREATE TEMP TABLE core_storage_identity(generation,role)")
        db.execute("INSERT INTO temp.core_storage_identity VALUES('wrong','wrong')")
        attest_role(db, manifest, "trace")


def test_committed_wal_identity_is_used(pair):
    root, manifest = pair
    path = root / "trace.db"
    with closing(sqlite3.connect(path)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.create_function(FUNCTION, 0, lambda: manifest.generation)
        with writer:
            writer.execute(
                "UPDATE core_storage_identity SET generation=?", (str(uuid4()),)
            )
        assert path.with_name(path.name + "-wal").stat().st_size > 0
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
            reader.execute("BEGIN")
            with pytest.raises(StorageUnavailable, match="storage_identity_mismatch"):
                attest_role(reader, manifest, "trace")


def test_swapped_roles_are_refused(pair):
    root, manifest = pair
    swapped = StorageManifest(manifest.generation, manifest.mirror, manifest.trace)
    before = files(root)
    with storage_lease(root, "reader"):
        for role, path in swapped.paths(root).items():
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
                db.execute("BEGIN")
                with pytest.raises(StorageUnavailable):
                    attest_role(db, swapped, role)
    assert files(root) == before


def test_excess_schema_objects_refused(pair):
    root, manifest = pair
    with closing(sqlite3.connect(root / "mirror.db")) as db:
        for i in range(256):
            db.execute(f"CREATE INDEX extra_{i} ON messages(id)")
        db.execute("BEGIN")
        with pytest.raises(StorageUnavailable, match="storage_schema_unavailable"):
            attest_role(db, manifest, "mirror")
