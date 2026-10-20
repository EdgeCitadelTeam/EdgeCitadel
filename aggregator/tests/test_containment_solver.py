"""Grouped minimum cut: exactness against exhaustive search and the theory examples."""

import random

import pytest

from aggregator.containment_monitor import solver
from aggregator.containment_monitor.solver import CutInstance


def instance(edges, costs, sources=("s",), sinks=("t",)):
    return CutInstance(
        tuple((u, v, tuple(a)) for u, v, a in edges),
        dict(costs),
        frozenset(sources),
        frozenset(sinks),
    )


def random_instance(rng: random.Random, *, groups: str):
    """Layered random digraph; groups: 'gated' (bundles share a tail),
    'ungated' (bundles scattered), or 'mixed' (also edges covered twice)."""
    layers = [
        ["s"],
        *[[f"n{i}_{j}" for j in range(rng.randint(1, 3))] for i in range(3)],
        ["t1", "t2"],
    ]
    edges, costs = [], {}
    next_action = 0
    group_pool = [f"g{i}" for i in range(rng.randint(1, 4))]
    for g in group_pool:
        costs[g] = rng.choice([0.5, 1.0, 2.0, 3.0, 7.0])
    for i in range(len(layers) - 1):
        for u in layers[i]:
            targets = rng.sample(layers[i + 1], rng.randint(1, len(layers[i + 1])))
            # gated: one group may take all out-edges of u
            tail_group = (
                rng.choice(group_pool)
                if groups == "gated" and rng.random() < 0.4
                else None
            )
            for v in targets:
                covering = []
                roll = rng.random()
                if tail_group is not None:
                    covering.append(tail_group)
                elif roll < 0.6:
                    action = f"e{next_action}"
                    next_action += 1
                    costs[action] = rng.choice([0.01, 0.5, 1.0, 2.0, 4.0, 9.0])
                    covering.append(action)
                    if groups == "mixed" and rng.random() < 0.3:
                        covering.append(rng.choice(group_pool))
                elif groups in {"ungated", "mixed"} and roll < 0.85:
                    covering.append(rng.choice(group_pool))
                edges.append((u, v, covering))
    used = {a for _, _, cover in edges for a in cover}
    return instance(
        edges, {a: c for a, c in costs.items() if a in used}, sinks=("t1", "t2")
    )


@pytest.mark.parametrize("groups", ["gated", "ungated", "mixed"])
def test_exact_solver_matches_exhaustive_search(groups):
    rng = random.Random(f"solver-{groups}")
    methods = set()
    for _ in range(150):
        problem = random_instance(rng, groups=groups)
        expected = solver.brute_force(problem, max_actions=18)
        if expected is None:
            continue
        got = solver.solve(problem, enum_limit=12)
        methods.add(got.method)
        assert got.unavoidable == expected.unavoidable
        assert got.cost == pytest.approx(expected.cost, abs=1e-9)
        assert sum(problem.costs[a] for a in got.actions) == pytest.approx(
            got.cost, abs=1e-9
        )
        # The cut property: no source reaches an avoidable sink.
        assert solver.separated(problem, got.actions, problem.sinks - got.unavoidable)
        greedy = solver.greedy(problem)
        assert solver.separated(
            problem, greedy.actions, problem.sinks - got.unavoidable
        )
        assert greedy.cost >= got.cost - 1e-9
    assert methods & {"gadget", "enumeration"}


def test_gated_groups_take_one_max_flow_and_ungated_ones_enumerate():
    gated = instance(
        [
            ("s", "a", []),
            ("a", "y1", ["w"]),
            ("a", "y2", ["w"]),
            ("y1", "t", []),
            ("y2", "t", []),
        ],
        {"w": 1.0},
    )
    single = solver.solve(gated)
    assert (single.method, single.fast_path, single.fast_path_reason) == (
        "gadget",
        True,
        None,
    )
    # Two branches no single vertex separates: the group has no gate.
    scattered = instance(
        [
            ("s", "a", []),
            ("s", "b", []),
            ("a", "x", ["w"]),
            ("b", "y", ["w"]),
            ("x", "t", []),
            ("y", "t", []),
        ],
        {"w": 1.0},
    )
    result = solver.solve(scattered)
    assert (result.method, result.exceptions, result.actions) == (
        "enumeration",
        ["w"],
        ["w"],
    )
    assert (result.fast_path, result.fast_path_reason) == (False, "ungated")


def test_theory_example_grouped_optimum_beats_edge_only_and_stop_all():
    """Theory note 6.1: revoke u1, u2 and cut a->y5 for 5, against 13 and 6.
    Witness nesting is expressed with ANY semantics (an edge removable by its
    leaf witness or by the apex w)."""
    edges = [("s", "a", [])]
    for y, leaf in (("y1", "u1"), ("y2", "u1"), ("y3", "u2"), ("y4", "u2")):
        edges.append(("a", y, [f"e_{y}", leaf, "w"]))
    edges.append(("a", "y5", ["e_y5", "w"]))
    edges += [(f"y{i}", "t", []) for i in range(1, 6)]
    costs = {
        **{f"e_y{i}": 3.0 for i in range(1, 5)},
        "e_y5": 1.0,
        "u1": 2.0,
        "u2": 2.0,
        "w": 6.0,
    }
    result = solver.solve(instance(edges, costs))
    assert result.cost == 5.0
    assert result.actions == ["e_y5", "u1", "u2"]
    # Every witness is vertex-local at a's out-port and the family is nested:
    # the chain gadget solves it with one max-flow (theory 4.3).
    assert (result.method, result.fast_path, result.exceptions) == ("gadget", True, [])


def test_theory_example_stop_all_descendants_overpays():
    """Theory note 6.2: optimum revoke w2 + cut a->t = 5; stopping all = 31."""
    edges = [
        ("s", "a", []),
        ("a", "t", ["e_at", "wa"]),
        ("a", "b1", ["wa"]),
        ("a", "b2", ["wa"]),
        ("a", "b3", ["wa"]),
        ("b1", "p1", ["w1", "wa"]),
        ("b3", "p3", ["w3", "wa"]),
        *[("b2", f"q{i}", [f"e_q{i}", "w2", "wa"]) for i in (1, 2, 3)],
        *[(f"q{i}", "t", []) for i in (1, 2, 3)],
    ]
    costs = {
        "e_at": 1.0,
        "wa": 31.0,
        "w1": 10.0,
        "w3": 10.0,
        "w2": 4.0,
        "e_q1": 2.0,
        "e_q2": 2.0,
        "e_q3": 2.0,
    }
    result = solver.solve(instance(edges, costs))
    assert (result.cost, result.actions) == (5.0, ["e_at", "w2"])


def test_shared_gadget_trap_is_solved_exactly():
    """Theory note 4.1: routing a group through one shared edge would cost 100."""
    edges = [
        ("s", "a", []),
        ("s", "a2", ["e_sa2"]),
        ("a", "b", ["e_ab", "w"]),
        ("a2", "b2", ["e_a2b2", "w"]),
        ("b", "t", []),
        ("b2", "t", []),
    ]
    costs = {"e_sa2": 1.0, "e_ab": 1.0, "e_a2b2": 100.0, "w": 100.0}
    result = solver.solve(instance(edges, costs))
    assert (result.cost, result.actions) == (2.0, ["e_ab", "e_sa2"])


def test_unavoidable_sinks_are_reported_and_the_rest_still_protected():
    problem = instance(
        [("s", "t1", []), ("s", "x", ["cut"]), ("x", "t2", [])],
        {"cut": 1.0},
        sinks=("t1", "t2"),
    )
    result = solver.solve(problem)
    assert result.unavoidable == {"t1"}
    assert result.actions == ["cut"]


def test_too_many_ungated_groups_fall_back_without_highs(monkeypatch):
    edges = [("s", f"m{i}", []) for i in range(5)]
    edges += [(f"m{i}", "t", [f"g{i % 3}", f"h{i}"]) for i in range(5)]
    edges += [("s", "t", [f"g{i}" for i in range(3)])]
    costs = {**{f"g{i}": 1.0 for i in range(3)}, **{f"h{i}": 0.6 for i in range(5)}}
    problem = instance(edges, costs)
    monkeypatch.setattr(solver, "ilp_available", lambda: False)
    approximate = solver.solve(problem, enum_limit=1)
    assert approximate.status == "approximate"
    assert solver.separated(problem, approximate.actions)


@pytest.mark.skipif(not solver.ilp_available(), reason="HiGHS (highspy) not installed")
def test_ilp_and_lp_bound_match_exhaustive_search():
    rng = random.Random("ilp")
    for _ in range(60):
        problem = random_instance(rng, groups="mixed")
        expected = solver.brute_force(problem, max_actions=16)
        if expected is None:
            continue
        exact = solver.ilp(problem)
        bound = solver.ilp(problem, relaxed=True)
        assert exact.cost == pytest.approx(expected.cost, abs=1e-6)
        assert bound.lower_bound <= expected.cost + 1e-6


def test_a_group_with_a_dominating_member_is_gated_by_that_edge():
    """Holding a child task on its parent's node pauses the child and parks its
    result: two edges in series, the second entered only through the first."""
    problem = instance(
        [
            ("s", "in", []),
            ("in", "out", ["hold"]),
            ("out", "parent", ["hold"]),
            ("out", "t", []),
            ("parent", "t", []),
        ],
        {"hold": 1.0},
    )
    result = solver.solve(problem)
    assert (result.method, result.exceptions, result.actions) == (
        "gadget",
        [],
        ["hold"],
    )
    # Not dominating: the second member can be reached around the first.
    bypass = instance(
        [
            ("s", "in", []),
            ("s", "out", []),
            ("in", "out", ["hold"]),
            ("out", "p", ["hold"]),
            ("p", "t", []),
        ],
        {"hold": 1.0},
    )
    assert solver.solve(bypass).method == "enumeration"


def test_crossing_gate_sets_at_one_port_are_never_solved_by_the_fast_path():
    """Theory Theorem 3.4: vertex-local but non-laminar bundles at one vertex are
    Set Cover. The check rejects the crossing set and enumerates it exactly."""
    elements = {"S1": ["y1", "y2"], "S2": ["y2", "y3"], "S3": ["y3", "y4"]}
    edges = [("s", "v", [])]
    for y in ("y1", "y2", "y3", "y4"):
        edges.append(("v", y, [name for name, ys in elements.items() if y in ys]))
        edges.append((y, "t", []))
    problem = instance(edges, {"S1": 1.0, "S2": 1.0, "S3": 1.0})
    result = solver.solve(problem)
    assert (result.fast_path, result.fast_path_reason) == (False, "non_laminar")
    assert result.method == "enumeration" and result.exceptions == ["S2"]
    assert result.cost == solver.brute_force(problem).cost == 2.0
    assert solver.separated(problem, result.actions)
    report = result.report()
    assert (report["solver"], report["fast_path"], report["exceptions"]) == (
        "enumeration",
        False,
        1,
    )


def test_a_dominating_member_whose_head_is_a_source_is_no_gate():
    """A path may start at the head of g, so removing g alone is not equivalent."""
    problem = instance(
        [
            ("s", "in", []),
            ("in", "out", ["hold"]),
            ("out", "t", ["hold"]),
        ],
        {"hold": 1.0},
        sources=("s", "out"),
    )
    result = solver.solve(problem)
    assert (result.fast_path, result.exceptions) == (False, ["hold"])
    assert result.actions == ["hold"]
    assert solver.separated(problem, result.actions)


def test_exceptions_above_the_limit_use_the_ilp_never_the_fast_path(monkeypatch):
    edges = [("s", f"m{i}", []) for i in range(5)]
    edges += [(f"m{i}", "t", [f"g{i % 3}", f"h{i}"]) for i in range(5)]
    edges += [("s", "t", [f"g{i}" for i in range(3)])]
    costs = {**{f"g{i}": 1.0 for i in range(3)}, **{f"h{i}": 0.6 for i in range(5)}}
    problem = instance(edges, costs)
    calls = []
    monkeypatch.setattr(solver, "ilp_available", lambda: True)
    monkeypatch.setattr(
        solver,
        "ilp",
        lambda given: calls.append(given)
        or solver.CutSolution(
            ["g0", "g1", "g2"], 3.0, "optimal", "ilp", frozenset(), 0
        ),
    )
    result = solver.solve(problem, enum_limit=1)
    assert calls == [problem]
    # Each g is gated at the sink's in-port, but the three gate sets cross at s->t.
    assert (result.method, result.fast_path, result.fast_path_reason) == (
        "ilp",
        False,
        "non_laminar",
    )
    assert result.exceptions == ["g1", "g2"]


def laminar_instance(rng: random.Random):
    """Layered digraph whose grouped actions are nested families at out-ports
    (delegated sub-scopes of one principal), plus single-edge actions."""
    layers = [
        ["s"],
        *[[f"n{i}_{j}" for j in range(rng.randint(1, 3))] for i in range(3)],
    ]
    layers.append(["t1", "t2"])
    covering: dict[tuple[str, str], list[str]] = {}
    costs: dict[str, float] = {}
    for i in range(len(layers) - 1):
        for u in layers[i]:
            heads = rng.sample(layers[i + 1], rng.randint(1, len(layers[i + 1])))
            for v in heads:
                covering[(u, v)] = []
                if rng.random() < 0.5:
                    name = f"e:{u}>{v}"
                    covering[(u, v)].append(name)
                    costs[name] = rng.choice([0.5, 1.0, 3.0, 9.0])
            # A random chain of nested scopes over u's out-edges.
            members = list(heads)
            rng.shuffle(members)
            size = len(members)
            for depth in range(rng.randint(0, 2)):
                size = rng.randint(1, size)
                name = f"w:{u}:{depth}"
                costs[name] = rng.choice([0.5, 1.0, 2.0, 4.0])
                for v in members[:size]:
                    covering[(u, v)].append(name)
    return instance(
        [(u, v, cover) for (u, v), cover in covering.items()],
        costs,
        sinks=("t1", "t2"),
    )


def test_nested_scopes_at_a_port_stay_on_the_fast_path_and_are_exact():
    rng = random.Random("laminar")
    checked = 0
    for _ in range(200):
        problem = laminar_instance(rng)
        expected = solver.brute_force(problem, max_actions=18)
        if expected is None:
            continue
        got = solver.solve(problem)
        assert got.fast_path, got.exceptions
        assert got.cost == pytest.approx(expected.cost, abs=1e-9)
        assert solver.separated(problem, got.actions, problem.sinks - got.unavoidable)
        checked += 1
    assert checked > 100
