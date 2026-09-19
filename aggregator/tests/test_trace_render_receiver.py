"""Local HTTP component checks, not a deployed application E2E stack."""

import json
import time
import urllib.error
import urllib.request

import pytest

from e2e.helpers.trace_render_receiver import RenderReceiver


def request(receiver, path, data=None, token=None):
    req = urllib.request.Request(
        receiver.url + path,
        data=data,
        headers={"Authorization": "Bearer " + (token or receiver.token)},
    )
    with urllib.request.urlopen(req, timeout=3) as response:
        return json.load(response)


@pytest.fixture
def receiver():
    instance = RenderReceiver(capacity=2)
    try:
        yield instance
    finally:
        instance.close()
    assert not instance.thread.is_alive()


def test_ack_is_allowlisted_first_receipt_on_monotonic_clock(receiver):
    receiver.expect("event-1", "span:1", "running")
    assert request(receiver, "/next") == {
        "event_id": "event-1",
        "node_id": "span:1",
        "state": "running",
        "trace_id": None,
        "lane": 0,
        "measured": True,
    }
    request(receiver, "/ready", b"")
    assert receiver.ready.is_set()
    before = time.monotonic_ns()
    request(receiver, "/ack", b'{"event_id":"event-1"}')
    after = time.monotonic_ns()
    receiver.wait("event-1", 0.1)
    marker = receiver.report()["acks"]["event-1"]
    assert before <= marker <= after
    with pytest.raises(urllib.error.HTTPError) as error:
        request(receiver, "/ack", b'{"event_id":"event-1"}')
    assert error.value.code == 409
    assert receiver.report()["acks"]["event-1"] == marker
    assert request(receiver, "/next") is None
    assert receiver.report()["valid"]


def test_authorization_does_not_create_an_ack(receiver):
    receiver.expect("event-1", "span:1", "running")
    with pytest.raises(urllib.error.HTTPError) as error:
        request(receiver, "/ack", b'{"event_id":"event-1"}', token="wrong")
    assert error.value.code == 401
    assert receiver.report()["acks"] == {}
    assert receiver.report()["valid"]


@pytest.mark.parametrize("body", [b'{"event_id":"unknown"}', b"[]", b"x" * 257])
def test_invalid_ack_invalidates_measurement(receiver, body):
    with pytest.raises(urllib.error.HTTPError) as error:
        request(receiver, "/ack", body)
    assert error.value.code == 400
    assert receiver.report()["failure"] == "invalid_ack_request"
    assert receiver.report()["acks"] == {}


def test_timeout_preserves_missing_denominator(receiver):
    receiver.expect("missing", "span:1", "finished")
    with pytest.raises(TimeoutError):
        receiver.wait("missing", 0.01)
    assert receiver.report()["expected"] == ["missing"]
    assert receiver.report()["failure"] == "render_timeout"


def test_capacity_is_bounded_and_report_is_not_mutable_state(receiver):
    receiver.expect("first", "span:1", "running")
    receiver.expect("second", "span:1", "finished")
    with pytest.raises(ValueError, match="capacity"):
        receiver.expect("third", "span:2", "running")
    assert receiver.report()["failure"] == "capacity_exceeded"
    snapshot = receiver.report()
    snapshot["expected"].clear()
    snapshot["acks"]["forged"] = 1
    assert receiver.report()["expected"] == ["first", "second"]
    assert receiver.report()["acks"] == {}


def test_warmup_is_required_but_not_in_measured_cohort(receiver):
    receiver.expect(
        "warmup", "span:1", "finished", trace_id="run-1", lane=1, measured=False
    )
    receiver.expect("measured", "span:2", "finished", trace_id="run-2", lane=0)
    pending = request(receiver, "/next")
    assert pending["event_id"] == "warmup" and pending["lane"] == 1
    assert pending["trace_id"] == "run-1" and not pending["measured"]
    request(receiver, "/ack", b'{"event_id":"warmup"}')
    assert request(receiver, "/next")["event_id"] == "measured"
    report = receiver.report()
    assert report["expected"] == ["warmup", "measured"]
    assert report["eligible"] == ["measured"]
    assert set(report["acks"]) == {"warmup"}
