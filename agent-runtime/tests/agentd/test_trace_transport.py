import json
from uuid import uuid4

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_transport import TransportTrace


def test_transport_evidence_is_exported_and_failure_is_optional(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    trace = TransportTrace(store, "owned-node")
    trace.record("connected", kind="infrastructure")
    row = store._connection.execute(
        "SELECT event_json FROM trace_journal_all WHERE json_extract(event_json,'$.kind')='infrastructure'"
    ).fetchone()
    event = json.loads(row[0])
    assert event["trace_id"] is None
    assert event["attributes"]["provenance"] == "nats_client"
    assert (
        store._connection.execute("SELECT count(*) FROM trace_spool").fetchone()[0] >= 1
    )
    trace.record("invalid-phase", attributes={"publication_attempt_id": str(uuid4())})
    assert trace.dropped == 1
    trace.record("disconnected", kind="infrastructure")
    assert trace.dropped == 0
    row = store._connection.execute(
        "SELECT event_json FROM trace_journal_all WHERE json_extract(event_json,'$.phase')='disconnected'"
    ).fetchone()
    assert json.loads(row[0])["attributes"]["dropped_observations"] == 1
    store.close()
