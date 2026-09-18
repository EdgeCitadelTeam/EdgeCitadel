import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nats.errors import TimeoutError as NatsTimeoutError

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import page_request
from edgecitadel_agentd.trace_settlement_pages import SETTLEMENT_PAGE_SUBJECT
from edgecitadel_agentd.trace_settlement_poll import SettlementPoller


@pytest.fixture
def source(tmp_path):
    store = AgentdStore(tmp_path / "state.db")
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(store._connection)
        epoch, generation = journal.initialize("edge-a")
        journal.record("edge-a", event, selected=True)
    try:
        yield store, ("edge-a", epoch, generation)
    finally:
        store.close()


def success(data, *, through=1, more=False):
    request = json.loads(data)
    page = {
        key: request[key]
        for key in (
            "schema_version",
            "node_id",
            "source_epoch",
            "export_generation",
            "after_export_seq",
        )
    }
    page.update(
        collector_epoch=request["collector_epoch"] or str(uuid4()),
        settled_export_seq=through,
        more=more,
        rejected_ranges=[],
        lost_ranges=[],
    )
    return SimpleNamespace(
        data=json.dumps(
            {
                "schema_version": 2,
                "request_id": request["request_id"],
                "status": "ok",
                "page": page,
            }
        ).encode()
    )


def failure(data, code, retry=500):
    return SimpleNamespace(
        data=json.dumps(
            {
                "schema_version": 2,
                "request_id": json.loads(data)["request_id"],
                "status": "error",
                "code": code,
                "retry_after_ms": retry,
            }
        ).encode()
    )


@pytest.mark.asyncio
async def test_concurrent_triggers_coalesce_and_commit_before_next_cursor(source):
    store, scope = source
    started, release = asyncio.Event(), asyncio.Event()

    async def request(subject, data, *, timeout):
        assert subject == SETTLEMENT_PAGE_SUBJECT and timeout == 5 and len(data) <= 1024
        started.set()
        await release.wait()
        return success(data)

    nc = SimpleNamespace(request=AsyncMock(side_effect=request))
    poller = SettlementPoller(store, nc, scope)
    callers = [asyncio.create_task(poller.poll()) for _ in range(20)]
    await started.wait()
    assert nc.request.call_count == 1
    release.set()
    assert await asyncio.gather(*callers) == ["applied"] * 20
    assert page_request(store, scope)["after_export_seq"] == 1
    assert poller.delay == 30 and poller.replay_required is False
    assert store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_transient_backoff_is_capped_and_success_resets_it(source):
    store, scope = source
    nc = SimpleNamespace(request=AsyncMock(side_effect=NatsTimeoutError()))
    poller = SettlementPoller(store, nc, scope)
    delays = []
    for _ in range(9):
        poller._next_at = 0  # Advance the scheduler in this deterministic policy test.
        assert await poller.poll() == "settlement_unavailable"
        delays.append(poller.delay)
    assert delays == [0.5, 1, 2, 4, 8, 16, 30, 30, 30]
    assert page_request(store, scope)["after_export_seq"] == 0
    nc.request.side_effect = lambda subject, data, **kwargs: success(data)
    poller._next_at = 0
    assert await poller.poll() == "applied"
    nc.request.side_effect = NatsTimeoutError()
    poller._next_at = 0
    await poller.poll()
    assert poller.delay == 0.5


@pytest.mark.asyncio
async def test_unknown_source_honors_retry_hint_and_requires_replay(source):
    store, scope = source
    nc = SimpleNamespace(
        request=AsyncMock(
            side_effect=lambda subject, data, **kwargs: failure(
                data, "unknown_source", 7000
            )
        )
    )
    poller = SettlementPoller(store, nc, scope)
    assert await poller.poll() == "unknown_source"
    assert poller.delay == 7 and poller.replay_required
    assert page_request(store, scope)["after_export_seq"] == 0


@pytest.mark.parametrize(
    "code", ["unsupported_version", "invalid_request", "collector_changed"]
)
@pytest.mark.asyncio
async def test_permanent_or_reset_reply_pauses_without_retirement(source, code):
    store, scope = source
    nc = SimpleNamespace(
        request=AsyncMock(
            side_effect=lambda subject, data, **kwargs: failure(data, code)
        )
    )
    poller = SettlementPoller(store, nc, scope)
    assert await poller.poll() == code
    assert await poller.poll() == "paused"
    assert nc.request.call_count == 1
    assert poller.fault == code
    assert page_request(store, scope)["after_export_seq"] == 0


@pytest.mark.parametrize(
    "kind", ["oversize", "duplicate_key", "invalid_json", "wrong_request"]
)
@pytest.mark.asyncio
async def test_unavailable_evidence_cannot_retire(source, kind):
    store, scope = source

    def request(subject, data, **kwargs):
        if kind == "oversize":
            raw = b"x" * (18 * 1024 + 1)
        elif kind == "duplicate_key":
            raw = b'{"schema_version":2,"schema_version":2}'
        elif kind == "invalid_json":
            raw = b"private-secret-invalid"
        else:
            value = json.loads(success(data).data)
            value["request_id"] = str(uuid4())
            raw = json.dumps(value).encode()
        return SimpleNamespace(data=raw)

    poller = SettlementPoller(
        store, SimpleNamespace(request=AsyncMock(side_effect=request)), scope
    )
    assert await poller.poll() == "invalid_settlement_reply"
    assert poller.delay == 0.5
    assert page_request(store, scope)["after_export_seq"] == 0
    assert "private-secret" not in str(vars(poller))


@pytest.mark.asyncio
async def test_stop_cancels_inflight_request_without_applying(source):
    store, scope = source
    started = asyncio.Event()

    async def request(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    poller = SettlementPoller(
        store, SimpleNamespace(request=AsyncMock(side_effect=request)), scope
    )
    stop = asyncio.Event()
    runner = asyncio.create_task(poller.run(stop))
    await started.wait()
    stop.set()
    await asyncio.wait_for(runner, timeout=1)
    assert poller.state == "stopped" and poller._inflight.done()
    assert page_request(store, scope)["after_export_seq"] == 0


@pytest.mark.asyncio
async def test_continuation_delay_is_enforced_even_for_manual_triggers(source):
    store, scope = source
    nc = SimpleNamespace(
        request=AsyncMock(
            side_effect=lambda subject, data, **kwargs: success(data, more=True)
        )
    )
    poller = SettlementPoller(store, nc, scope)
    assert await poller.poll() == "applied"
    assert poller.delay == 0.5
    # The next successful response has no further positions and is an idle page.
    nc.request.side_effect = lambda subject, data, **kwargs: success(data)
    started = asyncio.get_running_loop().time()
    assert await poller.poll() == "applied"
    assert asyncio.get_running_loop().time() - started >= 0.45
    assert poller.delay == 30


@pytest.mark.asyncio
async def test_empty_known_page_requests_replay_of_unsettled_tail(source):
    store, scope = source
    nc = SimpleNamespace(
        request=AsyncMock(
            side_effect=lambda subject, data, **kwargs: success(data, through=0)
        )
    )
    poller = SettlementPoller(store, nc, scope)
    assert await poller.poll() == "applied"
    assert poller.replay_required and poller.delay == 30
    assert (
        store._connection.execute("SELECT state FROM trace_spool").fetchone()[0]
        == "pending"
    )
