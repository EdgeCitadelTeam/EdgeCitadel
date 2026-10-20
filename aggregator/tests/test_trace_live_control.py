import pytest

from e2e.helpers.trace_live_control import (
    cpu_summary,
    process_cpu,
    source_display_summary,
)


def cohort():
    return (
        {"samples": 3, "measured_samples": 2},
        {
            "valid": True,
            "failure": None,
            "expected": ["warm", "one", "two"],
            "eligible": ["one", "two"],
            "acks": {"warm": 9_000_000, "one": 5_000_000, "two": 8_000_000},
        },
        {"warm": 1_000_000, "one": 3_000_000, "two": 5_000_000},
    )


def test_source_display_excludes_warmup_but_requires_its_ack():
    declared, render, starts = cohort()
    result = source_display_summary(declared, render, starts)
    assert result["values_ms"] == [2, 3]
    assert result["all_samples"] == 3
    assert result["p95_sample_sufficient"] is False
    del render["acks"]["warm"]
    with pytest.raises(ValueError, match="cohort"):
        source_display_summary(declared, render, starts)


@pytest.mark.parametrize(
    "mutation",
    ["duplicate", "outside", "start_missing", "warm_clock", "bool_clock", "invalid"],
)
def test_source_display_rejects_incomplete_or_invalid_observations(mutation):
    declared, render, starts = cohort()
    if mutation == "duplicate":
        render["eligible"] = ["one", "one"]
    elif mutation == "outside":
        render["eligible"] = ["one", "other"]
    elif mutation == "start_missing":
        del starts["two"]
    elif mutation == "warm_clock":
        starts["warm"] = render["acks"]["warm"] + 1
    elif mutation == "bool_clock":
        starts["one"] = True
    else:
        render["valid"] = False
    with pytest.raises(ValueError):
        source_display_summary(declared, render, starts)


def snapshots():
    before = dict(
        pid=100,
        start_ticks=200,
        cpu_ticks=300,
        ticks_per_second=100,
        monotonic_ns=1_000_000_000,
    )
    return before, dict(before, cpu_ticks=550, monotonic_ns=3_000_000_000)


def test_cpu_summary_uses_process_ticks_and_elapsed_window():
    result = cpu_summary(*snapshots())
    assert result["cpu_seconds"] == 2.5
    assert result["window_seconds"] == 2


@pytest.mark.parametrize(
    "key,value",
    [
        ("pid", 101),
        ("start_ticks", 201),
        ("ticks_per_second", 101),
        ("cpu_ticks", 299),
        ("monotonic_ns", 1_000_000_000),
        ("cpu_ticks", True),
        ("monotonic_ns", float("nan")),
    ],
)
def test_cpu_rejects_restart_counter_regression_and_invalid_types(key, value):
    before, after = snapshots()
    after[key] = value
    with pytest.raises(ValueError):
        cpu_summary(before, after)


def test_cpu_rejects_zero_tick_frequency():
    before, after = snapshots()
    before["ticks_per_second"] = after["ticks_per_second"] = 0
    with pytest.raises(ValueError):
        cpu_summary(before, after)


def test_process_cpu_parses_parentheses_in_comm(tmp_path, monkeypatch):
    directory = tmp_path / "100"
    directory.mkdir()
    fields = ["0"] * 20
    fields[0], fields[11], fields[12], fields[19] = "S", "123", "45", "200"
    (directory / "stat").write_text("100 (core (worker) name)) " + " ".join(fields))
    monkeypatch.setattr("e2e.helpers.trace_live_control.os.sysconf", lambda _: 100)
    snapshot = process_cpu(100, proc_root=tmp_path)
    assert snapshot["pid"] == 100
    assert snapshot["start_ticks"] == 200
    assert snapshot["cpu_ticks"] == 168
    assert snapshot["monotonic_ns"] > 0
