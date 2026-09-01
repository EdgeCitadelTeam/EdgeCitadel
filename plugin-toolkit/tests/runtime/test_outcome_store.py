from __future__ import annotations

import os
import queue
import sqlite3
import stat
import threading
from collections.abc import Mapping
from dataclasses import FrozenInstanceError, fields, replace
from pathlib import Path
from typing import Literal, cast

import pytest

import edgecitadel_plugin_runtime.outcome_store as outcome_store_module
import edgecitadel_plugin_runtime.task_types as task_types_module
from edgecitadel_plugin_runtime.outcome_store import (
    DisabledOutcomeStore,
    OutcomeConflict,
    OutcomeKey,
    OutcomeNotFound,
    OutcomeSchemaError,
    OutcomeStore,
    OutcomeStoreClosed,
    OutcomeStoreDisabled,
    OutcomeStoreError,
    OutcomeValidationError,
    PreparedOutcome,
    SQLiteOutcomeStore,
)
from edgecitadel_plugin_runtime.task_types import PublicationReceipt
from edgecitadel_plugin_runtime.validator import canonical_json

TASK_ID = "899d8a29-8c6c-4fef-b491-1140d8371fef"
RETENTION_FORMULA = (
    "max(stream max_age, maximum retry horizon, maximum task deadline) + "
    "duplicate_window + one hour"
)


def make_outcome(
    *,
    worker_agent_id: str = "worker-1",
    task_id: str = TASK_ID,
    sender_id: str = "sender-1",
    wire_id: str = "wire-1",
    request_fingerprint: str = "a" * 64,
    terminal_id: str = "terminal-1",
    terminal_value: object = "ok",
    terminal_payload_hash: str = "b" * 64,
    publish_state: Literal["prepared", "published"] = "prepared",
    completed_at: str = "2026-07-25T12:00:00Z",
    receipt: PublicationReceipt | None = None,
) -> PreparedOutcome:
    return PreparedOutcome(
        key=OutcomeKey(worker_agent_id, task_id),
        sender_id=sender_id,
        request_envelope_id=wire_id,
        request_fingerprint=request_fingerprint,
        terminal_envelope={
            "id": terminal_id,
            "type": "result",
            "payload": {"value": terminal_value},
        },
        terminal_payload_hash=terminal_payload_hash,
        publish_state=publish_state,
        completed_at=completed_at,
        receipt=receipt,
    )


def make_receipt(
    *,
    envelope_id: str = "terminal-1",
    accepted: bool = True,
    transport: str = "jetstream",
    stream: str | None = "AGENT_INBOX",
    stream_sequence: int | None = 7,
    duplicate: bool | None = False,
    accepted_ns: int = 42,
    application_bytes: int = 128,
    wire_bytes: int | None = 192,
) -> PublicationReceipt:
    return PublicationReceipt(
        envelope_id=envelope_id,
        accepted=accepted,
        transport=transport,
        stream=stream,
        stream_sequence=stream_sequence,
        duplicate=duplicate,
        accepted_ns=accepted_ns,
        application_bytes=application_bytes,
        wire_bytes=wire_bytes,
    )


def connection_for(store: SQLiteOutcomeStore) -> sqlite3.Connection:
    return store._connection


def mutate_frozen(instance: object, name: str, value: object) -> None:
    setattr(instance, name, value)


def read_attempts(path: Path, key: OutcomeKey) -> list[str]:
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            """
            SELECT wire_id
            FROM request_attempts
            WHERE worker_agent_id = ? AND task_id = ?
            ORDER BY attempt_ordinal
            """,
            (key.worker_agent_id, key.task_id),
        ).fetchall()
    return [cast(str, row[0]) for row in rows]


def read_outcome_count(path: Path) -> int:
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()
    assert row is not None
    return cast(int, row[0])


def test_public_types_are_frozen_exact_and_have_one_receipt_name() -> None:
    assert [field.name for field in fields(PublicationReceipt)] == [
        "envelope_id",
        "accepted",
        "transport",
        "stream",
        "stream_sequence",
        "duplicate",
        "accepted_ns",
        "application_bytes",
        "wire_bytes",
    ]
    assert [field.name for field in fields(OutcomeKey)] == [
        "worker_agent_id",
        "task_id",
    ]
    assert [field.name for field in fields(PreparedOutcome)] == [
        "key",
        "sender_id",
        "request_envelope_id",
        "request_fingerprint",
        "terminal_envelope",
        "terminal_payload_hash",
        "publish_state",
        "completed_at",
        "receipt",
    ]
    assert not hasattr(task_types_module, "PublishReceipt")
    assert not hasattr(task_types_module, "TransportReceipt")
    assert not hasattr(outcome_store_module, "PublishReceipt")
    assert not hasattr(outcome_store_module, "TransportReceipt")

    receipt = make_receipt()
    key = OutcomeKey("worker-1", TASK_ID)
    outcome = make_outcome()
    with pytest.raises(FrozenInstanceError):
        mutate_frozen(receipt, "accepted", False)
    with pytest.raises(FrozenInstanceError):
        mutate_frozen(key, "task_id", "changed")
    with pytest.raises(FrozenInstanceError):
        mutate_frozen(outcome, "publish_state", "published")


def test_store_protocol_exposes_enabled_capability(tmp_path: Path) -> None:
    disabled: OutcomeStore = DisabledOutcomeStore()
    enabled: OutcomeStore = SQLiteOutcomeStore(tmp_path / "outcomes.db")
    try:
        assert disabled.enabled is False
        assert enabled.enabled is True
    finally:
        disabled.close()
        enabled.close()


def test_disabled_store_is_stateless_and_detaches_terminal_mappings() -> None:
    store = DisabledOutcomeStore()
    source: dict[str, object] = {
        "id": "terminal-1",
        "type": "result",
        "payload": {"items": [1, 2]},
    }
    outcome = replace(make_outcome(), terminal_envelope=source)

    first = store.prepare(outcome)
    source["id"] = "mutated"
    source_payload = cast(dict[str, object], source["payload"])
    cast(list[object], source_payload["items"]).append(3)

    assert first.terminal_envelope == {
        "id": "terminal-1",
        "type": "result",
        "payload": {"items": [1, 2]},
    }
    assert first.terminal_envelope is not source
    assert store.lookup(outcome.key) is None

    store.close()
    second = store.prepare(outcome)
    assert second.terminal_envelope == source
    assert second.terminal_envelope is not source
    assert store.lookup(outcome.key) is None
    store.close()
    with pytest.raises(OutcomeStoreDisabled, match="outcome ledger is disabled"):
        store.mark_published(outcome.key, make_receipt())


def test_disabled_store_canonical_round_trip_fails_closed() -> None:
    store = DisabledOutcomeStore()
    invalid = replace(
        make_outcome(),
        terminal_envelope={"id": "terminal-1", "payload": float("nan")},
    )
    with pytest.raises(OutcomeValidationError, match="invalid prepared outcome"):
        store.prepare(invalid)
    assert store.lookup(invalid.key) is None


def test_sqlite_store_prepares_marks_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    outcome = make_outcome()
    receipt = make_receipt()

    store = SQLiteOutcomeStore(path)
    assert store.lookup(outcome.key) is None
    prepared = store.prepare(outcome)
    assert prepared == outcome
    assert prepared.terminal_envelope is not outcome.terminal_envelope
    assert store.lookup(outcome.key) == outcome
    published = store.mark_published(outcome.key, receipt)
    assert published == replace(outcome, publish_state="published", receipt=receipt)
    store.close()

    reopened = SQLiteOutcomeStore(path)
    try:
        assert reopened.lookup(outcome.key) == published
        assert read_attempts(path, outcome.key) == ["wire-1"]
    finally:
        reopened.close()


def test_sqlite_configuration_schema_metadata_and_blob_encoding(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    outcome = make_outcome(terminal_value={"z": 1, "a": "λ"})
    receipt = make_receipt(stream=None, stream_sequence=None, duplicate=None)
    store = SQLiteOutcomeStore(path)
    try:
        connection = connection_for(store)
        assert connection.isolation_level is None
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
        synchronous = connection.execute("PRAGMA synchronous").fetchone()
        foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()
        assert journal_mode is not None and journal_mode[0] == "wal"
        assert synchronous is not None and synchronous[0] == 2
        assert foreign_keys is not None and foreign_keys[0] == 1
        busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()
        assert busy_timeout is not None
        assert cast(int, busy_timeout[0]) > 0
        user_version = connection.execute("PRAGMA user_version").fetchone()
        assert user_version is not None and user_version[0] == 1

        store.prepare(outcome)
        store.mark_published(outcome.key, receipt)
        row = connection.execute(
            """
            SELECT terminal_envelope, typeof(terminal_envelope),
                   receipt, typeof(receipt)
            FROM outcomes
            """
        ).fetchone()
        assert row is not None
        terminal_bytes = cast(bytes, row[0])
        receipt_bytes = cast(bytes, row[2])
        assert row[1] == "blob"
        assert row[3] == "blob"
        assert terminal_bytes == canonical_json(outcome.terminal_envelope)
        assert receipt_bytes == canonical_json(
            {
                "accepted": True,
                "accepted_ns": 42,
                "application_bytes": 128,
                "duplicate": None,
                "envelope_id": "terminal-1",
                "stream": None,
                "stream_sequence": None,
                "transport": "jetstream",
                "wire_bytes": 192,
            }
        )

        metadata = dict(
            cast(
                list[tuple[str, str]],
                connection.execute(
                    "SELECT key, value FROM metadata ORDER BY key"
                ).fetchall(),
            )
        )
        assert metadata == {
            "eviction_policy": "no_eviction_during_run",
            "minimum_retention_formula": RETENTION_FORMULA,
            "schema_version": "1",
        }
        assert not any(character.isdigit() for character in RETENTION_FORMULA)

        tables = {
            cast(str, row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        assert tables == {"metadata", "outcomes", "request_attempts"}
        foreign_keys = connection.execute(
            "PRAGMA foreign_key_list(request_attempts)"
        ).fetchall()
        assert len(foreign_keys) == 2
    finally:
        store.close()


def test_new_database_is_0600_and_existing_mode_is_preserved(tmp_path: Path) -> None:
    new_path = tmp_path / "new.db"
    new_store = SQLiteOutcomeStore(new_path)
    new_store.close()
    assert stat.S_IMODE(new_path.stat().st_mode) == 0o600

    existing_path = tmp_path / "existing.db"
    existing_path.touch(mode=0o640)
    existing_path.chmod(0o640)
    existing_store = SQLiteOutcomeStore(existing_path)
    existing_store.close()
    assert stat.S_IMODE(existing_path.stat().st_mode) == 0o640


def test_invalid_database_paths_fail_closed(tmp_path: Path) -> None:
    for path_kind in (
        "memory",
        "directory",
        "missing_parent",
        "fifo",
        "unwritable_parent",
    ):
        if path_kind == "memory":
            path = Path(":memory:")
        elif path_kind == "directory":
            path = tmp_path / "directory"
            path.mkdir()
        elif path_kind == "missing_parent":
            path = tmp_path / "missing" / "outcomes.db"
        elif path_kind == "fifo":
            path = tmp_path / "outcomes.fifo"
            os.mkfifo(path)
        else:
            parent = tmp_path / "read-only"
            parent.mkdir(mode=0o500)
            parent.chmod(0o500)
            path = parent / "outcomes.db"

        with pytest.raises(
            OutcomeValidationError,
            match="invalid outcome database path",
        ):
            SQLiteOutcomeStore(path)


def test_prepare_and_lookup_return_deeply_detached_mappings(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    terminal: dict[str, object] = {
        "id": "terminal-1",
        "type": "result",
        "payload": {"items": [1, {"name": "stable"}]},
    }
    outcome = replace(make_outcome(), terminal_envelope=terminal)
    store = SQLiteOutcomeStore(path)
    try:
        prepared = store.prepare(outcome)
        terminal["id"] = "input-mutated"
        payload = cast(dict[str, object], terminal["payload"])
        cast(list[object], payload["items"]).clear()

        prepared_terminal = cast(dict[str, object], prepared.terminal_envelope)
        prepared_terminal["id"] = "return-mutated"
        prepared_payload = cast(dict[str, object], prepared_terminal["payload"])
        cast(list[object], prepared_payload["items"]).append("changed")

        first_lookup = store.lookup(outcome.key)
        assert first_lookup is not None
        assert first_lookup.terminal_envelope == {
            "id": "terminal-1",
            "type": "result",
            "payload": {"items": [1, {"name": "stable"}]},
        }
        lookup_terminal = cast(dict[str, object], first_lookup.terminal_envelope)
        cast(dict[str, object], lookup_terminal["payload"])["items"] = ["mutated"]

        second_lookup = store.lookup(outcome.key)
        assert second_lookup is not None
        assert second_lookup.terminal_envelope == {
            "id": "terminal-1",
            "type": "result",
            "payload": {"items": [1, {"name": "stable"}]},
        }
    finally:
        store.close()


def test_same_prepare_is_idempotent_and_attempt_is_unique(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    outcome = make_outcome()
    store = SQLiteOutcomeStore(path)
    try:
        assert store.prepare(outcome) == outcome
        assert store.prepare(outcome) == outcome
        assert read_attempts(path, outcome.key) == ["wire-1"]
        assert read_outcome_count(path) == 1
    finally:
        store.close()


def test_immutable_conflicts_do_not_append_or_disclose_payload(
    tmp_path: Path,
) -> None:
    for change in ("sender", "fingerprint", "terminal", "payload_hash", "completed_at"):
        path = tmp_path / f"{change}.db"
        original = make_outcome(terminal_value={"secret_value": "never-disclose"})
        store = SQLiteOutcomeStore(path)
        try:
            store.prepare(original)
            if change == "sender":
                conflicting = replace(
                    original, sender_id="sender-2", request_envelope_id="wire-2"
                )
            elif change == "fingerprint":
                conflicting = replace(
                    original,
                    request_fingerprint="c" * 64,
                    request_envelope_id="wire-2",
                )
            elif change == "terminal":
                conflicting = replace(
                    original,
                    terminal_envelope={
                        "id": "terminal-2",
                        "type": "result",
                        "payload": {"secret_value": "other-secret"},
                    },
                    request_envelope_id="wire-2",
                )
            elif change == "payload_hash":
                conflicting = replace(
                    original,
                    terminal_payload_hash="d" * 64,
                    request_envelope_id="wire-2",
                )
            else:
                conflicting = replace(
                    original,
                    completed_at="2026-07-25T12:00:01Z",
                    request_envelope_id="wire-2",
                )

            with pytest.raises(OutcomeConflict) as caught:
                store.prepare(conflicting)
            message = str(caught.value)
            assert message == "outcome conflicts with durable record"
            assert "secret" not in message
            assert read_attempts(path, original.key) == ["wire-1"]
            assert store.lookup(original.key) == original
        finally:
            store.close()


def test_terminal_conflicts_use_canonical_blob_equality(
    tmp_path: Path,
) -> None:
    cases: tuple[tuple[object, object], ...] = (
        (1, 1.0),
        (True, 1),
        (-0.0, 0.0),
    )
    for index, (first_value, second_value) in enumerate(cases):
        path = tmp_path / f"canonical-{index}.db"
        first = make_outcome(terminal_value=first_value)
        second = replace(
            make_outcome(terminal_value=second_value),
            request_envelope_id="wire-2",
        )
        assert first.terminal_envelope == second.terminal_envelope
        assert canonical_json(first.terminal_envelope) != canonical_json(
            second.terminal_envelope
        )

        store = SQLiteOutcomeStore(path)
        try:
            store.prepare(first)
            with pytest.raises(OutcomeConflict):
                store.prepare(second)
            assert read_attempts(path, first.key) == ["wire-1"]
        finally:
            store.close()


def test_published_reopen_registers_semantic_retries_without_replacement(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    original = make_outcome()
    first_receipt = make_receipt()

    store = SQLiteOutcomeStore(path)
    store.prepare(original)
    published = store.mark_published(original.key, first_receipt)
    store.close()

    reopened = SQLiteOutcomeStore(path)
    cached = reopened.lookup(original.key)
    assert cached == published
    assert cached is not None
    semantic_retry = replace(cached, request_envelope_id="wire-2")
    assert reopened.prepare(semantic_retry) == published
    assert reopened.prepare(semantic_retry) == published

    conflict = replace(
        semantic_retry,
        request_envelope_id="wire-3",
        sender_id="colliding-sender",
    )
    with pytest.raises(OutcomeConflict):
        reopened.prepare(conflict)
    reopened.close()

    final = SQLiteOutcomeStore(path)
    try:
        assert final.lookup(original.key) == published
        assert read_attempts(path, original.key) == ["wire-1", "wire-2"]
    finally:
        final.close()


def test_first_durably_marked_receipt_is_immutable(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    outcome = make_outcome()
    first = make_receipt()
    later = make_receipt(
        transport="core",
        stream=None,
        stream_sequence=None,
        duplicate=None,
        accepted_ns=99,
        application_bytes=129,
        wire_bytes=None,
    )
    store = SQLiteOutcomeStore(path)
    try:
        store.prepare(outcome)
        marked = store.mark_published(outcome.key, first)
        assert store.mark_published(outcome.key, first) == marked
        assert store.mark_published(outcome.key, later) == marked
        assert store.lookup(outcome.key) == marked
    finally:
        store.close()

    reopened = SQLiteOutcomeStore(path)
    try:
        cached = reopened.lookup(outcome.key)
        assert cached is not None
        assert cached.receipt == first
    finally:
        reopened.close()


def test_nullable_core_style_receipt_is_valid(tmp_path: Path) -> None:
    outcome = make_outcome()
    receipt = make_receipt(
        transport="core",
        stream=None,
        stream_sequence=None,
        duplicate=None,
        wire_bytes=None,
    )
    store = SQLiteOutcomeStore(tmp_path / "outcomes.db")
    try:
        store.prepare(outcome)
        published = store.mark_published(outcome.key, receipt)
        assert published.receipt == receipt
    finally:
        store.close()


def test_invalid_receipts_are_typed_atomic_failures(tmp_path: Path) -> None:
    receipts = (
        make_receipt(accepted=False),
        make_receipt(accepted=cast(bool, 1)),
        make_receipt(envelope_id=""),
        make_receipt(transport=""),
        make_receipt(stream=cast(str | None, 7)),
        make_receipt(stream_sequence=0),
        make_receipt(stream_sequence=cast(int | None, True)),
        make_receipt(duplicate=cast(bool | None, 1)),
        make_receipt(accepted_ns=cast(int, True)),
        make_receipt(accepted_ns=-1),
        make_receipt(application_bytes=cast(int, False)),
        make_receipt(application_bytes=-1),
        make_receipt(wire_bytes=cast(int | None, True)),
        make_receipt(wire_bytes=-1),
    )
    for index, receipt in enumerate(receipts):
        outcome = make_outcome()
        store = SQLiteOutcomeStore(tmp_path / f"invalid-receipt-{index}.db")
        try:
            store.prepare(outcome)
            with pytest.raises(
                OutcomeValidationError,
                match="invalid publication receipt",
            ):
                store.mark_published(outcome.key, receipt)
            assert store.lookup(outcome.key) == outcome
        finally:
            store.close()


def test_missing_and_mismatched_marks_are_atomic(tmp_path: Path) -> None:
    outcome = make_outcome()
    store = SQLiteOutcomeStore(tmp_path / "outcomes.db")
    try:
        with pytest.raises(OutcomeNotFound, match="outcome is not prepared"):
            store.mark_published(outcome.key, make_receipt())
        store.prepare(outcome)
        with pytest.raises(
            OutcomeValidationError, match="receipt envelope does not match terminal"
        ):
            store.mark_published(
                outcome.key, make_receipt(envelope_id="other-terminal")
            )
        assert store.lookup(outcome.key) == outcome
    finally:
        store.close()


def test_non_receipt_objects_are_typed_atomic_failures(tmp_path: Path) -> None:
    receipts = (
        cast(PublicationReceipt, None),
        cast(PublicationReceipt, object()),
    )
    for index, receipt in enumerate(receipts):
        outcome = make_outcome()
        store = SQLiteOutcomeStore(tmp_path / f"non-receipt-{index}.db")
        try:
            store.prepare(outcome)
            with pytest.raises(
                OutcomeValidationError,
                match="invalid publication receipt",
            ):
                store.mark_published(outcome.key, receipt)
            assert store.lookup(outcome.key) == outcome
        finally:
            store.close()


def test_invalid_mark_after_publication_does_not_replace_first_receipt(
    tmp_path: Path,
) -> None:
    outcome = make_outcome()
    first = make_receipt()
    malformed = make_receipt(accepted_ns=-1)
    store = SQLiteOutcomeStore(tmp_path / "outcomes.db")
    try:
        store.prepare(outcome)
        published = store.mark_published(outcome.key, first)
        with pytest.raises(OutcomeValidationError, match="invalid publication receipt"):
            store.mark_published(outcome.key, malformed)
        assert store.lookup(outcome.key) == published
    finally:
        store.close()


def test_new_prepare_rejects_malformed_input(
    tmp_path: Path,
) -> None:
    for change in (
        "state",
        "receipt",
        "key",
        "sender",
        "wire",
        "fingerprint",
        "terminal_id",
    ):
        outcome = make_outcome()
        if change == "state":
            invalid = replace(
                outcome,
                publish_state=cast(Literal["prepared", "published"], "unknown"),
            )
        elif change == "receipt":
            invalid = replace(outcome, receipt=make_receipt())
        elif change == "key":
            invalid = replace(outcome, key=OutcomeKey("", TASK_ID))
        elif change == "sender":
            invalid = replace(outcome, sender_id="")
        elif change == "wire":
            invalid = replace(outcome, request_envelope_id="")
        elif change == "fingerprint":
            invalid = replace(outcome, request_fingerprint="not-a-hash")
        else:
            invalid = replace(
                outcome,
                terminal_envelope={"id": "", "type": "result", "payload": {}},
            )

        path = tmp_path / f"invalid-{change}.db"
        store = SQLiteOutcomeStore(path)
        try:
            with pytest.raises(
                OutcomeValidationError, match="invalid prepared outcome"
            ):
                store.prepare(invalid)
            assert read_outcome_count(path) == 0
        finally:
            store.close()


def test_noncanonical_terminal_input_fails_closed(tmp_path: Path) -> None:
    terminals: tuple[Mapping[str, object], ...] = (
        {"id": "terminal-1", "payload": float("nan")},
        {"id": "terminal-1", "payload": "\ud800"},
    )
    for index, terminal in enumerate(terminals):
        outcome = replace(make_outcome(), terminal_envelope=terminal)
        store = SQLiteOutcomeStore(tmp_path / f"noncanonical-{index}.db")
        try:
            with pytest.raises(
                OutcomeValidationError, match="invalid prepared outcome"
            ):
                store.prepare(outcome)
            assert store.lookup(outcome.key) is None
        finally:
            store.close()


def test_prepare_and_mark_use_immediate_transactions(tmp_path: Path) -> None:
    outcome = make_outcome()
    store = SQLiteOutcomeStore(tmp_path / "outcomes.db")
    traces: list[str] = []
    connection_for(store).set_trace_callback(traces.append)
    try:
        store.prepare(outcome)
        prepare_trace = tuple(traces)
        traces.clear()
        store.mark_published(outcome.key, make_receipt())
        mark_trace = tuple(traces)
    finally:
        store.close()

    assert any(statement == "BEGIN IMMEDIATE" for statement in prepare_trace)
    assert any(statement == "COMMIT" for statement in prepare_trace)
    assert any(statement == "BEGIN IMMEDIATE" for statement in mark_trace)
    assert any(statement == "COMMIT" for statement in mark_trace)


def test_first_outcome_and_attempt_are_one_transaction(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    outcome = make_outcome()
    store = SQLiteOutcomeStore(path)
    connection = connection_for(store)
    connection.execute(
        """
        CREATE TRIGGER force_attempt_failure
        BEFORE INSERT ON request_attempts
        BEGIN
            SELECT RAISE(ABORT, 'sensitive-trigger-detail');
        END
        """
    )
    try:
        with pytest.raises(OutcomeStoreError) as caught:
            store.prepare(outcome)
        assert str(caught.value) == "outcome store transaction failed"
        assert "sensitive" not in str(caught.value)
        assert caught.value.__cause__ is None
        assert caught.value.__suppress_context__ is True
        assert read_outcome_count(path) == 0
        assert read_attempts(path, outcome.key) == []

        connection.execute("DROP TRIGGER force_attempt_failure")
        assert store.prepare(outcome) == outcome
    finally:
        store.close()


def test_failed_mark_rolls_back_and_connection_remains_usable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    outcome = make_outcome()
    store = SQLiteOutcomeStore(path)
    connection = connection_for(store)
    store.prepare(outcome)
    connection.execute(
        """
        CREATE TRIGGER force_mark_failure
        BEFORE UPDATE ON outcomes
        BEGIN
            SELECT RAISE(ABORT, 'private-mark-detail');
        END
        """
    )
    try:
        with pytest.raises(OutcomeStoreError) as caught:
            store.mark_published(outcome.key, make_receipt())
        assert str(caught.value) == "outcome store transaction failed"
        assert "private" not in str(caught.value)
        assert caught.value.__cause__ is None
        assert caught.value.__suppress_context__ is True
        assert store.lookup(outcome.key) == outcome

        connection.execute("DROP TRIGGER force_mark_failure")
        assert (
            store.mark_published(outcome.key, make_receipt()).receipt == make_receipt()
        )
    finally:
        store.close()


def test_two_connections_race_identical_outcomes_and_keep_both_attempts(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    first_store = SQLiteOutcomeStore(path)
    second_store = SQLiteOutcomeStore(path)
    first = make_outcome(wire_id="wire-1")
    second = make_outcome(wire_id="wire-2")
    barrier = threading.Barrier(2)
    results: queue.Queue[PreparedOutcome | OutcomeStoreError] = queue.Queue()

    def prepare(store: SQLiteOutcomeStore, outcome: PreparedOutcome) -> None:
        barrier.wait()
        try:
            results.put(store.prepare(outcome))
        except OutcomeStoreError as exc:
            results.put(exc)

    threads = [
        threading.Thread(target=prepare, args=(first_store, first)),
        threading.Thread(target=prepare, args=(second_store, second)),
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            assert not thread.is_alive()
        observed = [results.get_nowait(), results.get_nowait()]
        assert all(isinstance(value, PreparedOutcome) for value in observed)
        prepared = [cast(PreparedOutcome, value) for value in observed]
        assert prepared[0] == prepared[1]
        assert prepared[0].request_envelope_id in {"wire-1", "wire-2"}
        attempts = read_attempts(path, first.key)
        assert attempts[0] == prepared[0].request_envelope_id
        assert set(attempts) == {"wire-1", "wire-2"}
        assert read_outcome_count(path) == 1
    finally:
        first_store.close()
        second_store.close()


def test_two_connections_race_conflicts_with_one_winner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    first_store = SQLiteOutcomeStore(path)
    second_store = SQLiteOutcomeStore(path)
    first = make_outcome(wire_id="wire-1", terminal_id="terminal-1")
    second = make_outcome(wire_id="wire-2", terminal_id="terminal-2")
    barrier = threading.Barrier(2)
    results: queue.Queue[PreparedOutcome | OutcomeStoreError] = queue.Queue()

    def prepare(store: SQLiteOutcomeStore, outcome: PreparedOutcome) -> None:
        barrier.wait()
        try:
            results.put(store.prepare(outcome))
        except OutcomeStoreError as exc:
            results.put(exc)

    threads = [
        threading.Thread(target=prepare, args=(first_store, first)),
        threading.Thread(target=prepare, args=(second_store, second)),
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            assert not thread.is_alive()
        observed = [results.get_nowait(), results.get_nowait()]
        winners = [value for value in observed if isinstance(value, PreparedOutcome)]
        conflicts = [value for value in observed if isinstance(value, OutcomeConflict)]
        assert len(winners) == 1
        assert len(conflicts) == 1
        winner = winners[0]
        assert read_attempts(path, first.key) == [winner.request_envelope_id]
        assert read_outcome_count(path) == 1

        losing_store = (
            second_store if winner.request_envelope_id == "wire-1" else first_store
        )
        assert losing_store.lookup(first.key) == winner
    finally:
        first_store.close()
        second_store.close()


def test_concurrent_initialization_is_serialized(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    barrier = threading.Barrier(2)
    results: queue.Queue[SQLiteOutcomeStore | OutcomeStoreError] = queue.Queue()

    def construct() -> None:
        barrier.wait()
        try:
            results.put(SQLiteOutcomeStore(path))
        except OutcomeStoreError as exc:
            results.put(exc)

    threads = [threading.Thread(target=construct) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()
    observed = [results.get_nowait(), results.get_nowait()]
    try:
        assert all(isinstance(value, SQLiteOutcomeStore) for value in observed)
        assert read_outcome_count(path) == 0
    finally:
        for value in observed:
            if isinstance(value, SQLiteOutcomeStore):
                value.close()


def test_unknown_schema_version_is_rejected_without_modification(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 99")
    before = path.read_bytes()
    before_mode = stat.S_IMODE(path.stat().st_mode)
    with sqlite3.connect(path) as connection:
        before_journal = connection.execute("PRAGMA journal_mode").fetchone()

    with pytest.raises(OutcomeSchemaError, match="unsupported outcome schema"):
        SQLiteOutcomeStore(path)

    assert path.read_bytes() == before
    assert stat.S_IMODE(path.stat().st_mode) == before_mode
    assert not path.with_name(f"{path.name}-wal").exists()
    assert not path.with_name(f"{path.name}-shm").exists()
    assert not path.with_name(f"{path.name}-journal").exists()
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (99,)
        assert connection.execute("PRAGMA journal_mode").fetchone() == before_journal
        assert connection.execute(
            """
                SELECT COUNT(*) FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
        ).fetchone() == (0,)


def test_version_zero_database_with_application_table_is_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE unrelated(secret_payload TEXT)")
        connection.execute(
            "INSERT INTO unrelated(secret_payload) VALUES ('leave-intact')"
        )

    with pytest.raises(OutcomeSchemaError) as caught:
        SQLiteOutcomeStore(path)
    assert str(caught.value) == "unsupported outcome schema"
    assert "secret" not in str(caught.value)

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (0,)
        assert connection.execute("SELECT * FROM unrelated").fetchone() == (
            "leave-intact",
        )


def test_version_one_database_with_missing_schema_is_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 1")

    with pytest.raises(OutcomeSchemaError, match="unsupported outcome schema"):
        SQLiteOutcomeStore(path)


def test_version_one_same_named_tables_without_constraints_are_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            f"""
            CREATE TABLE metadata (key TEXT, value TEXT);
            CREATE TABLE outcomes (
                worker_agent_id TEXT,
                task_id TEXT,
                sender_id TEXT,
                request_envelope_id TEXT,
                request_fingerprint TEXT,
                terminal_envelope BLOB,
                terminal_payload_hash TEXT,
                publish_state TEXT,
                completed_at TEXT,
                receipt BLOB
            );
            CREATE TABLE request_attempts (
                attempt_ordinal INTEGER,
                worker_agent_id TEXT,
                task_id TEXT,
                wire_id TEXT
            );
            INSERT INTO metadata(key, value)
            VALUES
                ('schema_version', '1'),
                ('eviction_policy', 'no_eviction_during_run'),
                ('minimum_retention_formula', '{RETENTION_FORMULA}');
            PRAGMA user_version = 1;
            """
        )

    with pytest.raises(OutcomeSchemaError, match="unsupported outcome schema"):
        SQLiteOutcomeStore(path)


def test_schema_comparison_preserves_quoted_literal_case_without_modification(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.db"
    store = SQLiteOutcomeStore(path)
    store.close()
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'outcomes'"
        ).fetchone()
        assert row is not None
        original_sql = cast(str, row[0])
        altered_sql = original_sql.replace(
            "('prepared', 'published')",
            "('PREPARED', 'PUBLISHED')",
        )
        assert altered_sql != original_sql
        connection.execute("PRAGMA writable_schema = ON")
        connection.execute(
            "UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = 'outcomes'",
            (altered_sql,),
        )
        schema_version = connection.execute("PRAGMA schema_version").fetchone()
        assert schema_version is not None
        connection.execute(
            f"PRAGMA schema_version = {cast(int, schema_version[0]) + 1}"
        )
        connection.execute("PRAGMA writable_schema = OFF")

    before = path.read_bytes()
    with pytest.raises(OutcomeSchemaError, match="unsupported outcome schema"):
        SQLiteOutcomeStore(path)
    assert path.read_bytes() == before
    with sqlite3.connect(path) as connection:
        persisted = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'outcomes'"
        ).fetchone()
    assert persisted is not None and persisted[0] == altered_sql


def test_schema_corruption_is_rejected_on_reopen(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    store = SQLiteOutcomeStore(path)
    store.close()
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE request_attempts")

    with pytest.raises(OutcomeSchemaError, match="unsupported outcome schema"):
        SQLiteOutcomeStore(path)


def test_corrupt_cached_payload_fails_without_error_chain_disclosure(
    tmp_path: Path,
) -> None:
    outcome = make_outcome()
    store = SQLiteOutcomeStore(tmp_path / "outcomes.db")
    try:
        store.prepare(outcome)
        connection_for(store).execute(
            "UPDATE outcomes SET terminal_envelope = ?",
            (sqlite3.Binary(b'{"sensitive_cached_value":'),),
        )
        with pytest.raises(OutcomeSchemaError) as caught:
            store.lookup(outcome.key)
        assert str(caught.value) == "unsupported outcome schema"
        assert "sensitive" not in str(caught.value)
        assert caught.value.__cause__ is None
        assert caught.value.__suppress_context__ is True
    finally:
        store.close()


def test_unexpected_persisted_schema_objects_are_rejected(
    tmp_path: Path,
) -> None:
    statements = (
        "CREATE INDEX extra_index ON outcomes(sender_id)",
        "CREATE VIEW extra_view AS SELECT * FROM outcomes",
        """
        CREATE TRIGGER extra_trigger
        AFTER INSERT ON outcomes
        BEGIN
            DELETE FROM outcomes;
        END
        """,
    )
    for index, statement in enumerate(statements):
        path = tmp_path / f"extra-schema-{index}.db"
        store = SQLiteOutcomeStore(path)
        store.close()
        with sqlite3.connect(path) as connection:
            connection.execute(statement)

        with pytest.raises(OutcomeSchemaError, match="unsupported outcome schema"):
            SQLiteOutcomeStore(path)


def test_close_is_idempotent_and_closed_operations_are_typed(
    tmp_path: Path,
) -> None:
    store = SQLiteOutcomeStore(tmp_path / "outcomes.db")
    outcome = make_outcome()
    store.close()
    store.close()

    with pytest.raises(OutcomeStoreClosed, match="outcome store is closed"):
        store.lookup(outcome.key)
    with pytest.raises(OutcomeStoreClosed, match="outcome store is closed"):
        store.prepare(outcome)
    with pytest.raises(OutcomeStoreClosed, match="outcome store is closed"):
        store.mark_published(outcome.key, make_receipt())


def test_no_outcome_or_attempt_is_evicted_during_run(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.db"
    store = SQLiteOutcomeStore(path)
    try:
        for index in range(64):
            suffix = f"{index:012d}"
            outcome = make_outcome(
                task_id=f"899d8a29-8c6c-4fef-b491-{suffix}",
                wire_id=f"wire-{index}",
                terminal_id=f"terminal-{index}",
            )
            store.prepare(outcome)
        assert read_outcome_count(path) == 64
        with sqlite3.connect(path) as connection:
            attempt_count = connection.execute(
                "SELECT COUNT(*) FROM request_attempts"
            ).fetchone()
        assert attempt_count == (64,)
    finally:
        store.close()
