import json
from itertools import permutations
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import validate_read_response
from test_trace_projection_store import core as core_fixture, event, ingest

from aggregator import trace_projection_store as projection
from aggregator.trace_graph_projection import local_id, resolve_graph

core = core_fixture
READ = json.loads(
    (
        Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces/read.v1.json"
    ).read_text()
)
TRACE = READ["expected_graph"]["trace_id"]


def graph(core):
    return projection.read_graph(core, trace_id=TRACE)


def assert_wire_shapes(value):
    response = {
        **READ["expected_graph"],
        "nodes": value["nodes"],
        "edges": value["edges"],
        "total_nodes": len(value["nodes"]),
    }
    validate_read_response(response)


@pytest.mark.parametrize("order", list(permutations(range(4))))
def test_frozen_root_children_join_graph_converges(core, order):
    for index in order:
        ingest(core, READ["input_events"][index])
        projection.project_batch(core)
    value = graph(core)
    assert value["nodes"] == READ["expected_graph"]["nodes"]

    def logical(e):
        return e["kind"], e["from"], e["to"], e["status"]

    assert sorted(map(logical, value["edges"])) == sorted(
        map(logical, READ["expected_graph"]["edges"])
    )
    assert not value["unresolved_ancestry"]
    assert_wire_shapes(value)


def test_missing_parent_is_explicit_then_resolves_without_rewriting_claim(core):
    root, child = READ["input_events"][:2]
    ingest(core, child)
    projection.project_batch(core)
    before = graph(core)
    assert before["edges"][0]["status"] == "unresolved"
    placeholder = next(node for node in before["nodes"] if node["kind"] == "unresolved")
    assert placeholder["id"] == "task:" + root["task_id"]
    saved = core.execute("SELECT * FROM trace_relationship_claims").fetchall()
    ingest(core, root)
    projection.project_batch(core)
    after = graph(core)
    assert after["edges"][0]["id"] == before["edges"][0]["id"]
    assert after["edges"][0]["status"] == "resolved"
    assert core.execute("SELECT * FROM trace_relationship_claims").fetchall() == saved
    assert_wire_shapes(before)
    assert_wire_shapes(after)


@pytest.mark.parametrize("reverse", [False, True])
def test_cycles_and_competing_parents_never_become_tree_ancestry(core, reverse):
    a, b, c, d = [str(uuid4()) for _ in range(4)]
    values = [
        event(task_id=a, parent_task_id=b, seq=1),
        event(task_id=b, parent_task_id=c, seq=2),
        event(task_id=c, parent_task_id=a, seq=3),
        event(task_id=d, parent_task_id=a, seq=4),
    ]
    for value in reversed(values) if reverse else values:
        ingest(core, value)
    projection.project_batch(core)
    cycle = graph(core)
    assert {e["to"] for e in cycle["edges"] if e["status"] == "invalid"} == {
        "task:" + x for x in (a, b, c)
    }
    assert (
        next(e for e in cycle["edges"] if e["to"] == "task:" + d)["status"]
        == "resolved"
    )
    assert_wire_shapes(cycle)
    # A new competing parent invalidates both claims, not the last-arriving one.
    correction = event(
        task_id=d, parent_task_id=b, seq=5, supersedes_event_id=values[-1]["event_id"]
    )
    ingest(core, correction)
    projection.project_batch(core)
    ambiguous = graph(core)
    parents = [e for e in ambiguous["edges"] if e["to"] == "task:" + d]
    assert len(parents) == 2 and all(e["status"] == "invalid" for e in parents)
    assert_wire_shapes(ambiguous)


def test_native_root_attempt_spans_and_permission_have_explicit_identities(core):
    attempt = str(uuid4())
    root = event("started", kind="run", execution_attempt_id=attempt)
    model = event(
        "finished", kind="model", seq=2, task_id=None, execution_attempt_id=attempt
    )
    tool = event(
        "finished",
        kind="tool",
        seq=3,
        task_id=None,
        execution_attempt_id=attempt,
        parent_span_id=model["span_id"],
    )
    child = event("completed", seq=4, parent_run_id=TRACE)
    permission = event("denied", kind="permission", seq=5, execution_attempt_id=attempt)
    dispatch = event("denied", kind="dispatch", seq=6, execution_attempt_id=attempt)
    # Terminal tool/model evidence can arrive before its parent operation/root.
    for value in [tool, child, permission, model, dispatch, root]:
        ingest(core, value)
        projection.project_batch(core)
    value = graph(core)
    nodes = {n["id"]: n for n in value["nodes"]}
    assert nodes["run:" + TRACE]["state"] == "running"
    assert nodes[local_id(root, "attempt", attempt)]["state"] == "running"
    assert nodes[local_id(tool, "span", tool["span_id"])]["state"] == "finished"
    assert nodes[local_id(model, "span", model["span_id"])]["state"] == "finished"
    assert all(e["status"] == "resolved" for e in value["edges"])
    assert_wire_shapes(value)
    finished = {**root, "phase": "completed", "source_seq": 7, "event_id": str(uuid4())}
    ingest(core, finished)
    projection.project_batch(core)
    assert (
        next(n for n in graph(core)["nodes"] if n["id"] == "run:" + TRACE)["state"]
        == "completed"
    )


def test_shared_span_uuid_on_different_sources_does_not_merge_operations(core):
    one = event("finished", kind="model")
    two = {**one, "node_id": "other-source", "event_id": str(uuid4())}
    ingest(core, one)
    ingest(core, two)
    projection.project_batch(core)
    models = [n for n in graph(core)["nodes"] if n["kind"] == "model"]
    assert len(models) == 2 and models[0]["id"] != models[1]["id"]


def test_reused_operation_and_request_ids_stay_with_their_execution_attempt(core):
    first_attempt, second_attempt = str(uuid4()), str(uuid4())
    values = []
    for index, attempt in enumerate((first_attempt, second_attempt)):
        values.extend(
            [
                event(
                    "started",
                    kind="model",
                    seq=index * 3 + 1,
                    execution_attempt_id=attempt,
                ),
                event(
                    "started",
                    kind="tool",
                    seq=index * 3 + 2,
                    execution_attempt_id=attempt,
                ),
                event(
                    "denied",
                    kind="dispatch",
                    seq=index * 3 + 3,
                    execution_attempt_id=attempt,
                ),
            ]
        )
    for value in values:
        ingest(core, value)
    projection.project_batch(core)
    actual = graph(core)
    observed = [node for node in actual["nodes"] if node["kind"] != "unresolved"]
    assert len(observed) == 6 and len({node["id"] for node in observed}) == 6
    for value in values:
        family = "dispatch" if value["kind"] == "dispatch" else "span"
        identity = (
            value["attributes"]["dispatch_id"]
            if family == "dispatch"
            else value["span_id"]
        )
        relation = next(
            e for e in actual["edges"] if e["to"] == local_id(value, family, identity)
        )
        assert relation["from"] == local_id(
            value, "attempt", value["execution_attempt_id"]
        )
    assert_wire_shapes(actual)


def test_recorded_graph_changes_reconstruct_each_observed_snapshot(core):
    values = [READ["input_events"][i] for i in (2, 3, 1, 0)]
    model = event("finished", kind="model", seq=30)
    values.extend(
        [
            model,
            event(
                "started",
                kind="run",
                seq=31,
                task_id=model["task_id"],
                execution_attempt_id=model["execution_attempt_id"],
            ),
        ]
    )
    nodes, claims = {}, {}
    after = 0
    for value in values:
        ingest(core, value)
        state = projection.project_batch(core)
        changes = projection.read_changes(
            core, generation=state.generation, after=after
        )["changes"]
        for item in changes:
            change = item["change"]
            if "node" in change:
                nodes[change["node"]["id"]] = change["node"]
            for update in change.get("node_updates", []):
                nodes[update["node"]["id"]] = update["node"]
            for claim in change.get("edge_claims", []):
                claims[claim["id"]] = claim
            after = item["cursor"]
        rebuilt = resolve_graph(list(nodes.values()), list(claims.values()))
        current = graph(core)
        assert rebuilt == {
            key: current[key] for key in ("nodes", "edges", "unresolved_ancestry")
        }


def test_permission_policies_for_one_dispatch_keep_separate_decisions(core):
    denied = event("denied", kind="permission", seq=1)
    allowed = {
        **denied,
        "event_id": str(uuid4()),
        "source_seq": 2,
        "phase": "allowed",
        "attributes": {
            **denied["attributes"],
            "policy": "another-policy",
            "reason": None,
        },
    }
    ingest(core, denied)
    ingest(core, allowed)
    projection.project_batch(core)
    decisions = [node for node in graph(core)["nodes"] if node["kind"] == "permission"]
    assert len(decisions) == 2
    assert {node["state"] for node in decisions} == {"allowed", "denied"}
    assert all(not node["conflict"] for node in decisions)
    assert_wire_shapes(graph(core))


def test_conflicting_operation_outcomes_retain_first_observation_and_evidence(core):
    failed = event("failed", kind="tool", seq=2)
    finished = {
        **failed,
        "event_id": str(uuid4()),
        "phase": "finished",
        "source_seq": 3,
    }
    started = {**failed, "event_id": str(uuid4()), "phase": "started", "source_seq": 1}
    for value in (failed, finished, started):
        ingest(core, value)
        projection.project_batch(core)
    node = next(n for n in graph(core)["nodes"] if n["kind"] == "tool")
    assert node["state"] == "failed" and node["conflict"]
    assert node["outcome_candidate_count"] == 2
    evidence = [
        json.loads(row[0])
        for row in core.execute(
            "SELECT evidence_json FROM trace_entity_observations ORDER BY ingest_seq"
        )
    ]
    assert [item["phase"] for item in evidence] == ["failed", "finished", "started"]
    assert_wire_shapes(graph(core))


def test_entity_type_collision_stays_unresolved(core):
    model = event("started", kind="model", seq=1)
    tool = event("started", kind="tool", seq=2, span_id=model["span_id"])
    ingest(core, model)
    ingest(core, tool)
    projection.project_batch(core)
    identity = local_id(model, "span", model["span_id"])
    node = next(n for n in graph(core)["nodes"] if n["id"] == identity)
    assert node["kind"] == "unresolved" and node["state"] == "unknown"
    assert (
        core.execute(
            "SELECT identity_conflict FROM trace_projected_entities WHERE entity_id=?",
            (identity,),
        ).fetchone()[0]
        == 1
    )


def test_projection_v1_is_not_silently_reinterpreted(core):
    with core:
        core.execute("UPDATE trace_projection_state SET version=1")
    before = list(core.iterdump())
    with pytest.raises(ValueError, match="version_unavailable"):
        projection.initialize(core)
    assert list(core.iterdump()) == before


def test_graph_expansion_refusal_never_discards_persisted_nodes(core):
    for i in range(501):
        ingest(core, event("completed", seq=i + 1, task_id=str(uuid4())))
    while projection.project_batch(core).ingest_cursor < 501:
        pass
    with pytest.raises(ValueError, match="graph_expansion_required"):
        graph(core)
    assert (
        core.execute("SELECT count(*) FROM trace_projected_tasks").fetchone()[0] == 501
    )
    assert core.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 501
    assert not core.in_transaction
