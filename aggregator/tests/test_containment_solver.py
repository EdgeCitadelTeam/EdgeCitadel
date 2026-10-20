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
    assert solver.solve(gated).method == "gadget"
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
    assert (result.method, result.ungated, result.actions) == ("enumeration", 1, ["w"])


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
    assert (result.method, result.ungated, result.actions) == ("gadget", 0, ["hold"])
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
