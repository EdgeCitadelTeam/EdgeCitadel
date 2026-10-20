"""Root cause v0 on scripted scenarios with known ground truth."""

import pytest

from aggregator.containment_monitor import rootcause
from aggregator.containment_monitor.config import MonitorConfig
from aggregator.containment_monitor.model import inv, parse_external_alert, ver
from containment_support import BASE_MS, Harness, TraceBuilder


def alert(harness, kind, version, at):
    value = parse_external_alert(
        {"kind": kind, "version_id": version, "observed_at_ms": BASE_MS + at}, now_ms=0
    )
    harness.monitor.submit(value)
    return value


def rank(harness, value):
    harness.at(1000)
    return rootcause.rank(
        harness.monitor.graph, harness.config, value, now_ms=harness.now
    )


def team(trace, *, bad_late_read=False):
    """K delegates to T; T delegates to leaves A1 (faulty) and A2; K then reads T's
    result and only afterwards delegates to B1 and B2 (CASE branch)."""
    kipp = trace.start(trace.root("kipp", "n0", 0), 1)
    early = trace.dispatch(kipp, "case", "n2", 5)  # before anything is tainted
    trace.arrive(early, 8)
    early_case = trace.start(early, 9)
    t_task = trace.dispatch(kipp, "tars", "n1", 10)
    trace.arrive(t_task, 12)
    tars = trace.start(t_task, 13)
    a1_task = trace.dispatch(tars, "a1", "n1", 20)
    a2_task = trace.dispatch(tars, "a2", "n1", 21)
    trace.arrive(a1_task, 22)
    trace.arrive(a2_task, 22)
    a1 = trace.start(a1_task, 23)
    a2 = trace.start(a2_task, 23)
    trace.complete(a1, 30)
    trace.complete(a2, 31)
    trace.read_result(tars, a1_task, 32)
    trace.read_result(tars, a2_task, 33)
    trace.complete(tars, 40)
    trace.read_result(kipp, t_task, 45)
    b1_task = trace.dispatch(kipp, "b1", "n2", 50)
    b2_task = trace.dispatch(kipp, "b2", "n2", 51)
    for task_id in (b1_task, b2_task):
        trace.arrive(task_id, 55)
    b1 = trace.start(b1_task, 56)
    b2 = trace.start(b2_task, 56)
    trace.complete(b1, 70)
    trace.complete(b2, 71)
    return dict(
        kipp=kipp,
        early_case=early_case,
        tars=tars,
        a1=a1,
        a2=a2,
        b1=b1,
        b2=b2,
        tasks=dict(
            t=t_task, a1=a1_task, a2=a2_task, b1=b1_task, b2=b2_task, early=early
        ),
    )


@pytest.fixture
def harness(tmp_path):
    value = Harness(tmp_path)
    yield value
    value.close()


def test_single_failure_points_at_the_producer_and_contamination_respects_time(harness):
    trace = TraceBuilder()
    run = team(trace)
    harness.feed(trace.events)
    bad = trace.versions[("result", run["tasks"]["a1"])]
    ranking = rank(harness, alert(harness, "validator_failure", bad, 80))
    top = ranking[0]
    assert (top.node, top.prune_level, top.roots) == (
        inv(run["a1"]),
        "invocation",
        [inv(run["a1"])],
    )
    assert top.path == [inv(run["a1"]), ver(bad)]
    closure = rootcause.forward_closure(harness.monitor.graph, top.roots)
    # T read the bad result, K read T's; B1/B2 were dispatched afterwards.
    for name in ("tars", "kipp", "b1", "b2"):
        assert inv(run[name]) in closure
    # CASE's early task was dispatched before K was tainted: not contaminated.
    assert inv(run["early_case"]) not in closure
    assert inv(run["a2"]) not in closure
    summary = rootcause.contamination(harness.monitor.graph, top.roots, {ver(bad)})
    assert set(summary["silent"]) >= {inv(run["b1"]), inv(run["b2"]), inv(run["kipp"])}
    assert summary["live"]["running"] == [run["kipp"]]  # early CASE runs on, untainted


def test_spectrum_uses_healthy_outputs_as_negative_evidence(harness):
    trace = TraceBuilder()
    run = team(trace)
    harness.feed(trace.events)
    versions = trace.versions
    alert(harness, "validator_pass", versions[("result", run["tasks"]["a2"])], 75)
    alert(harness, "validator_failure", versions[("result", run["tasks"]["t"])], 76)
    last = alert(
        harness, "validator_failure", versions[("result", run["tasks"]["b1"])], 77
    )
    ranking = rank(harness, last)
    by_node = {c.node: c for c in ranking}
    assert ranking[0].node == inv(run["a1"])
    assert (by_node[inv(run["a1"])].failed, by_node[inv(run["a1"])].passed) == (2, 0)
    assert by_node[inv(run["a1"])].ochiai == pytest.approx(1.0)
    # T and K sit upstream of the healthy A2 result as well.
    assert by_node[inv(run["tars"])].ochiai == pytest.approx(2 / (2 * 3) ** 0.5)


def test_untrusted_origin_prior_breaks_ties(tmp_path):
    for untrusted, expected in ((frozenset(), "tars"), (frozenset({"web"}), "web")):
        harness = Harness(
            tmp_path / str(bool(untrusted)), MonitorConfig(untrusted_agents=untrusted)
        )
        trace = TraceBuilder()
        tars = trace.start(trace.root("tars", "n1", 0), 1)
        web_task = trace.dispatch(tars, "web", "n3", 2)
        trace.arrive(web_task, 3)
        web = trace.start(web_task, 4)
        trace.complete(web, 6)
        trace.read_result(tars, web_task, 7)
        t_result = trace.complete(tars, 9)
        harness.feed(trace.events)
        ranking = rank(harness, alert(harness, "validator_failure", t_result, 10))
        assert ranking[0].node == inv({"tars": tars, "web": web}[expected])
        harness.close()


def test_prune_level_version_when_the_producer_also_produced_healthy_outputs(harness):
    trace = TraceBuilder()
    tars = trace.start(trace.root("tars", "n1", 0), 1)
    child = trace.dispatch(tars, "a2", "n1", 2)
    trace.arrive(child, 3)
    a2 = trace.start(child, 4)
    good = trace.complete(a2, 5)
    bad = trace.complete(tars, 9)
    harness.feed(trace.events)
    alert(harness, "validator_pass", good, 10)
    ranking = rank(harness, alert(harness, "validator_failure", bad, 11))
    producer = next(c for c in ranking if c.node == inv(tars))
    assert (producer.passed, producer.prune_level, producer.roots) == (
        1,
        "version",
        [ver(bad)],
    )


def test_prune_level_session_agent_and_subtree(tmp_path):
    context = "6e8f0b6c-6a65-4b8b-9e7f-0e3b9c6a7f10"
    # Session: two failing invocations of one agent in one conversation.
    harness = Harness(tmp_path / "session")
    trace = TraceBuilder()
    first = trace.start(trace.root("hermes", "n1", 0, context=context), 1)
    v1 = trace.complete(first, 2)
    second = trace.start(trace.root("hermes", "n1", 3, context=context), 4)
    v2 = trace.complete(second, 5)
    harness.feed(trace.events)
    alert(harness, "validator_failure", v1, 6)
    ranking = rank(harness, alert(harness, "validator_failure", v2, 7))
    top = ranking[0]
    assert top.prune_level == "session"
    assert set(top.roots) == {inv(first), inv(second)}
    harness.close()

    # Agent: the same agent fails in two unrelated conversations.
    harness = Harness(
        tmp_path / "agent", MonitorConfig(session_isolated_agents=frozenset())
    )
    trace = TraceBuilder()
    first = trace.start(trace.root("hermes", "n1", 0), 1)
    v1 = trace.complete(first, 2)
    second = trace.start(trace.root("hermes", "n1", 3), 4)
    v2 = trace.complete(second, 5)
    harness.feed(trace.events)
    alert(harness, "validator_failure", v1, 6)
    ranking = rank(harness, alert(harness, "validator_failure", v2, 7))
    assert ranking[0].prune_level == "agent"
    assert set(ranking[0].roots) == {inv(first), inv(second)}
    harness.close()

    # Subtree: a team member fails independently of the lead's failing invocation.
    config = MonitorConfig(org_parent={"a1": "tars"})
    harness = Harness(tmp_path / "subtree", config)
    trace = TraceBuilder()
    lead = trace.start(trace.root("tars", "n1", 0), 1)
    lead_result = trace.complete(lead, 3)
    other = trace.start(trace.root("tars", "n1", 4), 5)
    member_task = trace.dispatch(other, "a1", "n1", 6)
    trace.arrive(member_task, 7)
    member = trace.start(member_task, 8)
    member_result = trace.complete(member, 9)
    harness.feed(trace.events)
    alert(harness, "validator_failure", member_result, 10)
    ranking = rank(harness, alert(harness, "validator_failure", lead_result, 11))
    top = ranking[0]
    assert top.node == inv(lead)
    assert top.prune_level == "subtree"
    assert {inv(lead), inv(other), inv(member)} <= set(top.roots)
    harness.close()


def test_admission_denial_slice_follows_authority(harness):
    trace = TraceBuilder()
    kipp = trace.start(trace.root("kipp", "n0", 0), 1)
    task = trace.dispatch(kipp, "leaf", "n1", 2)
    trace.arrive(task, 3)
    leaf = trace.start(task, 4)
    trace.deny(leaf, "kipp", "outside_authority_scope", 6)
    harness.feed(trace.events)
    (stored,) = harness.db.execute(
        "SELECT alert_json FROM containment_alerts WHERE kind='admission_denied'"
    ).fetchall()
    from aggregator.containment_monitor.model import Alert
    import json

    value = Alert.from_json(json.loads(stored[0]))
    ranking = rank(harness, value)
    assert ranking[0].node == inv(leaf)
    assert inv(kipp) in {c.node for c in ranking}  # reached through the grant chain
