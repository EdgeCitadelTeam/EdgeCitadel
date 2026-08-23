"""Fixed profile schedule contracts for research artifact runs."""

from __future__ import annotations

from pathlib import Path

from scripts.research.run_artifact import build_schedule


def test_quick_schedule_has_fixed_warmup_and_measured_inventory() -> None:
    schedule = build_schedule(profile="quick", seed=20260725)

    assert schedule.warmup_count == 4
    assert schedule.measured_count == 18
    assert schedule.inferential is False
    assert len({rep.run_id for rep in schedule.repetitions}) == 22
    assert all(rep.cell.workload == "W1" for rep in schedule.repetitions[:4])
    assert all(not rep.measured for rep in schedule.repetitions[:4])
    assert {rep.cell.mode for rep in schedule.repetitions[:4]} == {
        "central-relay",
        "core-only",
        "edgecitadel",
        "all-durable",
    }


def test_matrix_smoke_executes_each_declared_cell_once_without_measurement() -> None:
    schedule = build_schedule(profile="matrix-smoke", seed=20260725)

    assert len(schedule.repetitions) == 46
    assert schedule.measured_count == 0
    assert all(not rep.measured for rep in schedule.repetitions)
    assert {
        (rep.cell.workload, rep.cell.mode, rep.cell.ablation)
        for rep in schedule.repetitions
    } == {(cell.workload, cell.mode, cell.ablation) for cell in schedule.cells}


def test_paper_schedule_has_reproducible_complete_blocks() -> None:
    config = Path("scripts/research/configs/campaigns/preliminary-x86-lan.yaml")
    first = build_schedule(profile="paper", campaign_config=config)
    second = build_schedule(profile="paper", campaign_config=config)

    assert first.warmup_blocks == 5
    assert first.measured_blocks == 30
    assert len(first.repetitions) == 35 * 46
    assert first == second
    for block in range(35):
        repetitions = [rep for rep in first.repetitions if rep.block == block]
        assert len(repetitions) == 46
        assert {
            (rep.cell.workload, rep.cell.mode, rep.cell.ablation) for rep in repetitions
        } == {(cell.workload, cell.mode, cell.ablation) for cell in first.cells}
        assert all(rep.measured is (block >= 5) for rep in repetitions)
