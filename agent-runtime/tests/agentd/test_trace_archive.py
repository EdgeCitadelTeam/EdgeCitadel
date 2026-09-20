import json
from uuid import uuid4

import pytest

from edgecitadel_agentd import trace_archive
from edgecitadel_agentd.client import AgentdClientError
from edgecitadel_agentd.trace_import import configure_import, import_trace
import test_trace_import_store as imports

store = imports.store


def archive(tmp_path, records):
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    return path


def record():
    params = imports.request()
    return {
        key: value
        for key, value in params.items()
        if key not in {"schema_version", "request_id", "import_source_id"}
    }


class LocalClient:
    def __init__(self, store):
        self.store = store

    def call(self, operation, **params):
        if operation == "trace.import.configure":
            return configure_import(
                self.store, administrator_authenticated=True, params=params
            )
        return import_trace(
            self.store,
            node_id="node-a",
            administrator_authenticated=True,
            params=params,
        )


def test_archive_import_retry_returns_replay_identity_without_creating_tasks(
    store, tmp_path
):
    item = record()
    path = archive(tmp_path, [item, item])
    records = trace_archive.read_archive(path, "saved-session")
    assert len(records) == 1
    result = trace_archive.import_archive(LocalClient(store), records)
    assert result["records"] == 1 and len(result["trace_ids"]) == 1
    assert (
        trace_archive.import_archive(
            LocalClient(store), trace_archive.read_archive(path, "saved-session")
        )
        == result
    )
    db = store._connection
    event = json.loads(db.execute("SELECT event_json FROM trace_journal").fetchone()[0])
    assert event["trace_id"] == result["trace_ids"][0]
    assert event["evidence_kind"] == "historical_import"
    assert db.execute("SELECT count(*) FROM trace_journal").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0


def test_malformed_later_record_is_rejected_before_connecting(tmp_path, monkeypatch):
    path = archive(tmp_path, [record(), {"wrong": True}])

    def unexpected(*args, **kwargs):
        raise AssertionError("must validate the whole archive first")

    monkeypatch.setattr(trace_archive, "AgentdClient", unexpected)
    assert (
        trace_archive.main(
            [str(path), "--source-id", "archive", "--state-dir", str(tmp_path)]
        )
        == 1
    )


def test_conflicting_archive_duplicates_are_rejected(tmp_path):
    item = record()
    changed = {**item, "observation": {**item["observation"], "duration_ms": 456}}
    with pytest.raises(ValueError, match="line 2"):
        trace_archive.read_archive(archive(tmp_path, [item, changed]), "archive")


@pytest.mark.parametrize("code", ["storage_unavailable", "idempotency_conflict"])
def test_service_refusal_reports_progress_instead_of_reading_missing_result(
    tmp_path, code
):
    class RefusingClient:
        def call(self, operation, **params):
            if operation == "trace.import.configure":
                return {}
            return {
                "schema_version": 1,
                "operation": "import",
                "request_id": params["request_id"],
                "status": "error",
                "code": code,
                "retryable": code == "storage_unavailable",
            }

    records = trace_archive.read_archive(archive(tmp_path, [record()]), "archive")
    with pytest.raises(AgentdClientError, match=f"after 0 records: {code}"):
        trace_archive.import_archive(RefusingClient(), records)


def test_partial_import_has_retryable_progress_and_no_duplicate_effects(
    store, tmp_path
):
    item = record()
    other = {**item, "record_id": str(uuid4())}
    records = trace_archive.read_archive(archive(tmp_path, [item, other]), "archive")
    client = LocalClient(store)
    original = client.call
    count = 0

    def disconnect(operation, **params):
        nonlocal count
        if operation == "trace.import":
            count += 1
            if count == 2:
                raise AgentdClientError("connection lost")
        return original(operation, **params)

    client.call = disconnect
    with pytest.raises(AgentdClientError, match="after 1 records"):
        trace_archive.import_archive(client, records)
    trace_archive.import_archive(LocalClient(store), records)
    assert (
        store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[0]
        == 2
    )
