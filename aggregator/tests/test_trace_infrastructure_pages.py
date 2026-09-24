from uuid import uuid4

import pytest
from test_trace_projection_store import core as core_fixture, event
from test_trace_projection_coverage import put
from aggregator.trace_infrastructure_pages import read_infrastructure
from aggregator.trace_event_pages import TraceReadError

core = core_fixture
KEY = b"owned-infrastructure-fixture-key-32"
SCOPE = "a" * 64


def test_infrastructure_pages_are_bounded_scoped_and_exclude_late_arrivals(core):
    def add(seq):
        value = event(seq=seq)
        for key in (
            "trace_id",
            "task_id",
            "context_id",
            "parent_task_id",
            "parent_run_id",
            "execution_attempt_id",
            "span_id",
            "parent_span_id",
        ):
            value[key] = None
        value.update(
            kind="infrastructure",
            phase="connected",
            attributes={"provenance": "nats_client"},
        )
        put(core, value, str(uuid4()), 1)
        return value

    first, second = add(1), add(2)
    from aggregator.trace_projection_store import project_batch

    project_batch(core)
    params = dict(signing_key=KEY, scope_hash=SCOPE, family="infrastructure", limit=1)
    page = read_infrastructure(core, **params)
    assert page["events"] == [first]
    assert page["receipt_times"]
    add(3)
    following = read_infrastructure(core, cursor=page["next_cursor"], **params)
    assert following["events"] == [second]
    assert following["next_cursor"] is None
    with pytest.raises(TraceReadError, match="cursor_scope_mismatch"):
        read_infrastructure(
            core, cursor=page["next_cursor"], **{**params, "family": "broker"}
        )
