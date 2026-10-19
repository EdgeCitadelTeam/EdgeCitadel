"""Leaf poison delivery never fabricates settlement or blocks later valid events."""

import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

from test_telemetry_leaf import (  # noqa: F401
    publish_when_routed,
    pytestmark,
    topology,
)

from edgecitadel_agentd.trace_contract import canonical_bytes
from edgecitadel_agentd.trace_settlement_pages import (
    SETTLEMENT_PAGE_SUBJECT,
    validate_page_reply,
    validate_page_request,
)
from edgecitadel_plugin_runtime.telemetry_stream import CONSUMER_NAME, STREAM_NAME


async def test_leaf_poison_retry_continuation_and_restart(
    topology,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
    record_property,  # noqa: F811
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_collector import TraceCollectorService

    url, token = topology.endpoints["core"]
    path = tmp_path / "core.db"
    collector = TraceCollectorService(path, url, token)
    epoch, generation = str(uuid4()), str(uuid4())
    sentinel = "owned-private-poison-sentinel"
    base = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]

    def wrapper(sequence):
        event = {
            **base,
            "node_id": "edge-a",
            "source_epoch": epoch,
            "event_id": str(uuid4()),
            "source_seq": sequence,
        }
        return {
            "schema_version": 1,
            "node_id": "edge-a",
            "source_epoch": epoch,
            "export_generation": generation,
            "export_seq": sequence,
            "event_sha256": hashlib.sha256(canonical_bytes(event)).hexdigest(),
            "event": event,
        }

    def rows(sql):
        with sqlite3.connect(path) as db:
            return db.execute(sql).fetchall()

    async def until(predicate):
        async with asyncio.timeout(20):
            while not predicate():
                await asyncio.sleep(0.05)

    async def publish(payload, leaf="a"):
        await publish_when_routed(
            topology[leaf].jetstream(),
            f"edgecitadel.telemetry.v1.edge-{leaf}",
            payload,
            str(uuid4()),
        )

    async def drained():
        async with asyncio.timeout(20):
            while True:
                state = (
                    await topology["core"]
                    .jetstream()
                    .consumer_info(STREAM_NAME, CONSUMER_NAME)
                )
                if state.num_pending == state.num_ack_pending == 0:
                    return
                await asyncio.sleep(0.05)

    async def checkpoint(leaf):
        request = {
            "schema_version": 2,
            "request_id": str(uuid4()),
            "node_id": "edge-a",
            "source_epoch": epoch,
            "export_generation": generation,
            "after_export_seq": 0,
            "collector_epoch": None,
        }
        reply = await topology[leaf].request(
            SETTLEMENT_PAGE_SUBJECT, validate_page_request(request), timeout=5
        )
        result = json.loads(reply.data)
        validate_page_reply(result, request=request)
        return result["page"]

    try:
        collector.start()
        await until(lambda: collector.status()["state"] == "running")
        with sqlite3.connect(path) as db:
            db.execute(
                "CREATE TRIGGER owned_fail_poison BEFORE INSERT ON trace_poison_counts "
                "WHEN NEW.reason='wire' BEGIN SELECT RAISE(ABORT,'owned failure'); END"
            )
        await publish(sentinel.encode())
        await until(lambda: collector.status()["metrics"]["persistence_failures"] > 0)
        assert rows("SELECT * FROM trace_poison_counts") == []
        pending = (
            await topology["core"].jetstream().consumer_info(STREAM_NAME, CONSUMER_NAME)
        )
        assert pending.num_ack_pending >= 1

        unsupported = wrapper(9999)
        unsupported["schema_version"] = 99
        await publish(canonical_bytes(unsupported))
        # Subject says edge-b, while the envelope claims edge-a. No receipt is valid.
        await publish(canonical_bytes(wrapper(10000)), leaf="b")
        rejected = wrapper(1)
        rejected["event"].update(schema_version=99, private=sentinel)
        rejected["event_sha256"] = hashlib.sha256(
            canonical_bytes(rejected["event"])
        ).hexdigest()
        await publish(canonical_bytes(rejected))
        valid = wrapper(2)
        await publish(canonical_bytes(valid))
        await until(lambda: rows("SELECT count(*) FROM trace_raw_events") == [(1,)])
        assert rows("SELECT export_seq FROM trace_ingest_positions") == [(2,)]
        assert rows(
            "SELECT node_id,source_epoch,export_generation,export_seq,event_sha256 "
            "FROM trace_rejected_positions"
        ) == [("edge-a", epoch, generation, 1, rejected["event_sha256"])]
        assert rows(
            "SELECT node_id,source_epoch,event_id,event_sha256 FROM trace_rejected_identities"
        ) == [
            ("edge-a", epoch, rejected["event"]["event_id"], rejected["event_sha256"])
        ]
        page = await checkpoint("a")
        assert page["settled_export_seq"] == 2
        assert page["rejected_ranges"] == [{"first": 1, "last": 1}]
        assert page["lost_ranges"] == []
        assert rows("SELECT * FROM trace_poison_counts WHERE reason='wire'") == []
        pending = (
            await topology["core"].jetstream().consumer_info(STREAM_NAME, CONSUMER_NAME)
        )
        assert pending.num_ack_pending >= 1
        with sqlite3.connect(path) as db:
            db.execute("DROP TRIGGER owned_fail_poison")
        await drained()
        assert dict(rows("SELECT reason,observations FROM trace_poison_counts")) == {
            "wire": 1,
            "wrapper": 1,
            "origin": 1,
        }
        await asyncio.to_thread(collector.stop)
        collector.start()
        await until(lambda: collector.status()["state"] == "running")
        assert await checkpoint("b") == page
        # Broker IDs differ, so this exercises persistent rejection dedupe after restart.
        await publish(canonical_bytes(rejected))
        following = wrapper(3)
        await publish(canonical_bytes(following))
        await drained()
        final = await checkpoint("a")
        assert final["collector_epoch"] == page["collector_epoch"]
        assert final["settled_export_seq"] == 3
        assert final["rejected_ranges"] == [{"first": 1, "last": 1}]
        with sqlite3.connect(path) as db:
            assert set(
                db.execute(
                    "SELECT r.event_id,r.event_sha256,p.event_json FROM trace_raw_events r "
                    "LEFT JOIN trace_payloads p USING(ingest_seq)"
                )
            ) == {
                (
                    item["event"]["event_id"],
                    item["event_sha256"],
                    canonical_bytes(item["event"]).decode(),
                )
                for item in (valid, following)
            }
            assert (
                db.execute("SELECT count(*) FROM trace_rejected_positions").fetchone()[
                    0
                ]
                == 1
            )
            assert (
                db.execute("SELECT count(*) FROM trace_rejected_identities").fetchone()[
                    0
                ]
                == 1
            )
            assert (
                db.execute("SELECT count(*) FROM trace_ingest_conflicts").fetchone()[0]
                == 0
            )
            assert sentinel not in "\n".join(db.iterdump())
            assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        evidence = {
            "failed_poison_commit_left_unacked": True,
            "valid_event_committed_while_poison_retry_pending": True,
            "exact_valid_events": 2,
            "rejected_positions": 1,
            "poison_counts": dict(
                rows("SELECT reason,observations FROM trace_poison_counts")
            ),
            "settled_export_seq": 3,
            "rejected_ranges": final["rejected_ranges"],
            "restart_preserves_epoch_and_rejection": True,
            "sentinel_absent_from_core_sql": True,
            "scope": "Real Core/two-Leaf routing; trusted fleet; direct injected wrappers, no source applier or task workload",
        }
        record_property("poison_evidence", json.dumps(evidence))
        print(json.dumps(evidence), flush=True)
    finally:
        await asyncio.to_thread(collector.stop)
