"""The pre-registered one-sided tests of H10 (b)."""

from __future__ import annotations

import math

from harness.compare import fisher_one_sided, mcnemar_one_sided


def test_mcnemar_one_sided_is_the_binomial_upper_tail():
    assert mcnemar_one_sided(5, 0) == 1 / 32
    assert mcnemar_one_sided(3, 1) == 5 / 16
    assert mcnemar_one_sided(0, 4) == 1.0
    assert mcnemar_one_sided(0, 0) == 1.0


def test_fisher_one_sided_is_the_hypergeometric_upper_tail():
    # Firebreak 10 of 10 clean against 3 of 10: only the observed table is as
    # extreme, C(10,10) C(10,3) / C(20,13).
    assert math.isclose(fisher_one_sided(10, 10, 3, 10), 120 / math.comb(20, 13))
    assert math.isclose(
        fisher_one_sided(5, 10, 5, 10),
        sum(math.comb(10, k) * math.comb(10, 10 - k) for k in range(5, 11))
        / math.comb(20, 10),
    )


def test_each_leak_falls_in_one_class():
    from harness.compare import leak_class

    stopped = [(100, False), (200, True)]  # started, then cancelled
    assert leak_class(250, stopped, "L2.a", {"L1.a"}) == "leak_after_stop"
    assert leak_class(150, stopped, "L2.a", {"L1.a"}) == "leak_polluted_use"
    resumed = [(100, False), (200, True), (220, False)]  # paused, released
    assert leak_class(250, resumed, "L2.a", {"L1.a"}) == "leak_polluted_use"
    assert leak_class(250, [], "L1.a", {"L1.a"}) == "leak_at_source"
    assert leak_class(250, stopped, "L1.a", {"L1.a"}) == "leak_after_stop"


def _row(system, code, rep, clean, kept, **cell):
    base = {
        "instance": "c2",
        "rogue_rate": 0.0,
        "alert": "late",
        "root_mode": "given",
        "latency": "native",
        "time_scale": 2.0,
        "profile": "deployed",
        "phase1_mode": "adaptive",
        "phase1_labels": "frontier",
        "receding_during_waits": 0,
        "makespan_s": 30.0,
        "consumed_at_alert": 1.0,
        "timeout": 0,
    }
    return {
        **base,
        **cell,
        "system": system,
        "strategy_code": code,
        "rep": rep,
        "clean_after_alert": clean,
        "kept_cfrac": kept,
    }


def test_pairs_match_cells_across_systems_and_one_ec_configuration():
    import pandas as pd
    import pytest

    from harness.compare import check_cells, h10_table, select_edgecitadel

    rows = [_row("edgecitadel", "firebreak", r, 1, 0.9) for r in range(3)]
    rows += [
        _row("edgecitadel", "firebreak", r, 0, 0.9, phase1_mode="freeze")
        for r in range(3)
    ]
    rows += [_row("a2a", "a2a_cancel_all", r, 1, 0.0) for r in range(3)]
    runs = pd.DataFrame(rows)
    with pytest.raises(ValueError, match="more than one run"):
        check_cells(runs)
    runs = select_edgecitadel(runs, [(None, "phase1_mode", "adaptive")])
    check_cells(runs)
    row = h10_table(runs).set_index("strategy_code").loc["a2a_cancel_all"]
    assert (row.pairs, row.b, row.c) == (3, 0, 0)
    assert row.kept_diff_median == pytest.approx(0.9)


def test_fisher_uses_firebreak_runs_of_the_same_cells():
    import pandas as pd

    from harness.compare import h10_table

    rows = [
        _row("edgecitadel", "firebreak", r, 1, 1.0, rogue_rate=0.3) for r in range(5)
    ]
    # The main grid's rate-0.3 runs of another workload stay out of the test.
    rows += [
        _row("edgecitadel", "firebreak", r, 0, 1.0, rogue_rate=0.3, instance="n2")
        for r in range(5)
    ]
    rows += [
        _row("a2a", "a2a_block_input", r, 0, 1.0, rogue_rate=0.3) for r in range(5)
    ]
    table = h10_table(pd.DataFrame(rows))
    row = table[table.strategy_code == "a2a_block_input"].iloc[0]
    assert (row.fb_runs, row.fb_clean) == (5, 5)


def test_the_ec_selection_can_differ_per_part():
    import pandas as pd

    from harness.compare import _setting, check_cells, select_edgecitadel

    rows = [
        _row("edgecitadel", "firebreak", 0, 1, 1.0, phase1_mode=mode, rogue_rate=rate)
        for mode in ("freeze", "adaptive")
        for rate in (0.0, 0.3)
    ]
    runs = select_edgecitadel(
        pd.DataFrame(rows),
        [
            _setting("content:phase1_mode=freeze"),
            _setting("rogue:phase1_mode=adaptive"),
        ],
    )
    check_cells(runs)
    assert sorted(zip(runs.rogue_rate, runs.phase1_mode)) == [
        (0.0, "freeze"),
        (0.3, "adaptive"),
    ]
    assert _setting("phase1_mode=adaptive") == (None, "phase1_mode", "adaptive")


def test_makespan_ratio_is_against_the_same_systems_reference():
    import pandas as pd

    from harness.compare import with_makespan_ratio

    runs = pd.DataFrame(
        [
            {
                "system": "langgraph",
                "instance": "c2",
                "time_scale": 2.0,
                "strategy_code": "reference",
                "makespan_s": 40.0,
            },
            {
                "system": "langgraph",
                "instance": "c2",
                "time_scale": 2.0,
                "strategy_code": "lg_time_travel",
                "makespan_s": 60.0,
            },
            {
                "system": "a2a",
                "instance": "c2",
                "time_scale": 2.0,
                "strategy_code": "reference",
                "makespan_s": 25.0,
            },
            {
                "system": "a2a",
                "instance": "n2",
                "time_scale": 2.0,
                "strategy_code": "none",
                "makespan_s": 30.0,
            },
        ]
    )
    ratio = with_makespan_ratio(runs)["makespan_ratio"].tolist()
    assert ratio[:3] == [1.0, 1.5, 1.0]
    assert ratio[3] != ratio[3]  # no A2A reference of n2


def test_kept_work_leaves_out_tasks_redone_after_the_alert():
    import pandas as pd
    import pytest

    from harness.compare import h10_table

    rows = [_row("edgecitadel", "firebreak", r, 1, 0.9) for r in range(3)]
    rows += [_row("langgraph", "lg_time_travel", r, 1, 1.0) for r in range(3)]
    for row in rows:
        row["kept_cfrac_no_redo"] = 0.9 if row["system"] == "edgecitadel" else 0.2
    table = h10_table(pd.DataFrame(rows)).set_index("strategy_code")
    tt = table.loc["lg_time_travel"]
    assert tt.kept_median == pytest.approx(0.2)
    assert tt.kept_scorer_median == pytest.approx(1.0)
    assert tt.kept_diff_median == pytest.approx(0.7)


def test_kept_without_redo_uses_the_scorers_completion_rule(tmp_path, monkeypatch):
    """With no task started again, kept_cfrac_no_redo is the scorer's kept_cfrac:
    both judge completion with the tainted closure (amendment 89a0506), so a
    refused hand-off into it costs the producer nothing in either."""
    from test_report import write_run

    from harness import compare, report

    real = compare.completion_times
    seen: list[set] = []

    def spy(spec, records, tainted=frozenset()):
        seen.append(set(tainted))
        return real(spec, records, tainted)

    monkeypatch.setattr(compare, "completion_times", spy)
    write_run(tmp_path, strategy="firebreak", rep=0, kept=True)
    out = compare.kept_without_redo(
        tmp_path / "runs" / "mini__late__native__r0__firebreak"
    )
    assert seen and seen[0], "called without the tainted closure"
    [row] = report.score(tmp_path)
    assert out["redone_kept"] == 0
    assert out["kept_cfrac_no_redo"] == row["kept_cfrac"]
