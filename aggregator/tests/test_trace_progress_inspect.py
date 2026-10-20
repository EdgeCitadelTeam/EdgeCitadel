import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from test_trace_ingest import connection, deliver, encode, record  # noqa: F401
from test_trace_settlement import loss

from aggregator import trace_inspect
from aggregator.trace_restore import prepare_restore


def scope_of(value):
    return tuple(value[key] for key in ("node_id", "source_epoch", "export_generation"))


def test_progress_stops_at_hole_and_preserves_rejection_and_loss(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
):
    deliver(connection, encode(record))
    later = deepcopy(record)
    later["export_seq"] = 5
    deliver(connection, encode(later))
    path = tmp_path / "core.db"
    scope = scope_of(record)
    page = trace_inspect.inspect_source_progress(path, scope=scope)["page"]
    assert page["settled_export_seq"] == 1
    assert page["more"] is False
    rejected = deepcopy(record)
    rejected["export_seq"] = 2
    rejected["event"]["event_id"] = str(uuid4())
    rejected["event"]["private_unknown_field"] = "do-not-report"
    deliver(connection, encode(rejected))
    loss(
        connection,
        dict(zip(("node_id", "source_epoch", "export_generation"), scope)),
        [(3, 4)],
        through=4,
    )
    before = list(connection.iterdump())
    report = trace_inspect.inspect_source_progress(path, scope=scope)
    assert report["coverage"] == "committed_interval_only"
    assert report["page"]["settled_export_seq"] == 5
    assert report["page"]["rejected_ranges"] == [{"first": 2, "last": 2}]
    assert report["page"]["lost_ranges"] == [{"first": 3, "last": 4}]
    assert "do-not-report" not in json.dumps(report)
    assert list(connection.iterdump()) == before
    # A page after a caller-provided base makes no claim about that prefix.
    suffix = trace_inspect.inspect_source_progress(
        path, scope=scope, after_export_seq=4, collector_epoch=page["collector_epoch"]
    )
    assert suffix["page"]["after_export_seq"] == 4
    assert suffix["page"]["lost_ranges"] == []


def test_progress_is_paged_and_actual_restore_fences_cursor(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
):
    deliver(connection, encode(record))
    with connection:
        connection.executemany(
            "INSERT INTO trace_ingest_positions SELECT node_id,source_epoch,export_generation,?,event_id,event_sha256,'duplicate',received_at_ms,? FROM trace_ingest_positions WHERE export_seq=1",
            [(i, i) for i in range(2, 1026)],
        )
        connection.execute("UPDATE trace_collector SET ingest_seq=1025")
    path = tmp_path / "core.db"
    scope = scope_of(record)
    first = trace_inspect.inspect_source_progress(path, scope=scope)["page"]
    assert first["settled_export_seq"] == 512 and first["more"]
    second = trace_inspect.inspect_source_progress(
        path,
        scope=scope,
        after_export_seq=512,
        collector_epoch=first["collector_epoch"],
    )["page"]
    assert second["settled_export_seq"] == 1024 and second["more"]
    restored = tmp_path / "restored.db"
    prepare_restore(path, restored)
    with pytest.raises(
        trace_inspect.InspectionError, match="^collector_epoch_changed$"
    ):
        trace_inspect.inspect_source_progress(
            restored,
            scope=scope,
            after_export_seq=512,
            collector_epoch=first["collector_epoch"],
        )


def test_progress_unknown_source_missing_file_and_budget_are_explicit(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "core.db"
    scope = scope_of(record)
    with pytest.raises(trace_inspect.InspectionError, match="^unknown_source$"):
        trace_inspect.inspect_source_progress(path, scope=scope)
    missing = tmp_path / "missing.db"
    with pytest.raises(trace_inspect.InspectionError, match="^inspection_unavailable$"):
        trace_inspect.inspect_source_progress(missing, scope=scope)
    assert not missing.exists()
    before = list(connection.iterdump())
    monkeypatch.setattr(
        trace_inspect, "_budget", lambda db: db.set_progress_handler(lambda: 1, 1)
    )
    with pytest.raises(trace_inspect.InspectionError, match="^inspection_unavailable$"):
        trace_inspect.inspect_source_progress(path, scope=scope)
    assert list(connection.iterdump()) == before


@pytest.mark.parametrize(
    "params",
    [
        {"after_export_seq": 1},
        {"after_export_seq": True},
        {"after_export_seq": -1},
        {"scope": ("x",)},
        {"collector_epoch": "invalid"},
    ],
)
def test_progress_invalid_request_before_open(record, tmp_path, params):  # noqa: F811
    with pytest.raises(
        trace_inspect.InspectionError, match="^invalid_inspection_request$"
    ):
        trace_inspect.inspect_source_progress(
            tmp_path / "missing.db", **{"scope": scope_of(record), **params}
        )


def test_progress_cli_and_uncommitted_writer(connection, record, tmp_path):  # noqa: F811
    deliver(connection, encode(record))
    path = tmp_path / "core.db"
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("BEGIN IMMEDIATE")
    connection.execute("DELETE FROM trace_ingest_positions")
    root = Path(__file__).parents[2]
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "aggregator.trace_inspect",
                str(path),
                "--scope",
                *scope_of(record),
            ],
            cwd=root,
            env={
                **os.environ,
                "PYTHONPATH": os.pathsep.join(
                    (str(root), str(root / "agent-runtime/src"))
                ),
            },
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["page"]["settled_export_seq"] == 1
    finally:
        connection.rollback()


@pytest.mark.parametrize(
    "extra", [["--view", "raw"], ["--after", "1"], ["--limit", "1"]]
)
def test_progress_cli_rejects_mixed_cursor_modes(record, tmp_path, extra):  # noqa: F811
    root = Path(__file__).parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "aggregator.trace_inspect",
            str(tmp_path / "missing.db"),
            "--scope",
            *scope_of(record),
            *extra,
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error": "invalid_inspection_request"}
