"""The ground-truth services over HTTP, through the worker's own client."""

from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer

import pytest
from det_worker.services import ServicesClient, ServicesUnavailable

from harness.services import RunServices, make_handler


@pytest.fixture
def served(tmp_path):
    services = RunServices(tmp_path, max_spawn=2)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(services, "secret"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    yield url, services, tmp_path
    server.shutdown()
    server.server_close()
    services.close()


def test_ledger_records_each_effect_phase_once(served):
    url, _, directory = served
    client = ServicesClient(url, "secret")
    record = {"effect_id": "E_B", "issuer": "B.t0", "phase": "committed", "labels": []}
    first = client.ledger(record)
    again = client.ledger(record)  # a resumed worker repeating its actuation
    assert again == {"t_ms": first["t_ms"], "repeated": True}
    lines = (directory / "ledger.jsonl").read_text().splitlines()
    assert [json.loads(line)["phase"] for line in lines] == ["committed"]


def test_readiness_counts_distinct_delivery_invocations(served):
    url, _, _ = served
    client = ServicesClient(url, "secret")
    for rt in ("d-1", "d-1", "d-2"):  # d-1 resumed once: one landing, not two
        client.event({"kind": "delivered", "target": "B.t0", "item": "X@v1", "rt": rt})
    client.event({"kind": "delivered", "target": "R.agg", "item": "rA", "rt": "d-3"})
    assert client.ready("B.t0") == {"X@v1": 2}
    assert client.ready("nobody") == {}


def test_spawn_cap_admits_repeats_of_reserved_ids(served):
    url, _, _ = served
    client = ServicesClient(url, "secret")
    assert client.spawn({"id": "nv|a|1"}) and client.spawn({"id": "nv|a|2"})
    assert not client.spawn({"id": "nv|a|3"})
    assert client.spawn({"id": "nv|a|1"})  # the same spawn retried after a pause


def test_requests_need_the_run_token(served):
    url, _, _ = served
    with pytest.raises(ServicesUnavailable):
        ServicesClient(url, "wrong", timeout=1).event({"kind": "start"})


def test_event_feed_is_ordered_and_resumable(served):
    url, services, _ = served
    client = ServicesClient(url, "secret")
    for i in range(5):
        client.event({"kind": "step", "i": i})
    first = services.events_after(0, 3)
    rest = services.events_after(first["next"], 100)
    assert [e["i"] for e in first["events"] + rest["events"]] == list(range(5))
    assert [e["seq"] for e in first["events"]] == [1, 2, 3]
