"""Read-only baseline completion audit; raw evidence remains on jim-eq."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import sqlite3
import subprocess
from pathlib import Path

if __package__:
    from .trace_live_control import cpu_summary, source_display_summary
    from .trace_latency_workload import (
        baseline_slot,
        baseline_workload,
        summarize_latencies,
    )
else:
    from trace_live_control import cpu_summary, source_display_summary
    from trace_latency_workload import (
        baseline_slot,
        baseline_workload,
        summarize_latencies,
    )


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def audit_source_render(declared, scope, events, render, browser, claimed):
    """Reconstruct eligibility from actual source order, independently of ACK lists."""
    require(
        declared == baseline_workload(preflight=declared.get("profile") == "preflight"),
        "undeclared baseline profile",
    )
    agents = scope["agent_ids"]
    require(len(agents) == len(set(agents)) == 10, "agent scope mismatch")
    require(len(events) == declared["expected_events"], "source cohort incomplete")
    events = sorted(events, key=lambda event: event["source_seq"])
    require(
        len({e["source_seq"] for e in events}) == len(events),
        "duplicate source position",
    )
    require(
        len({e["event_id"] for e in events}) == len(events), "duplicate source identity"
    )
    expected = []
    eligible = []
    runs = {}
    spans = {}
    for index, event in enumerate(events):
        cycle, actor, step = baseline_slot(index)
        require(
            (event["node_id"], event["source_epoch"])
            == (scope["node_id"], scope["source_epoch"]),
            "foreign source",
        )
        require(event["agent_id"] == agents[actor], "emission agent order mismatch")
        key = (cycle, actor)
        if step == 0:
            require(
                (event["kind"], event["phase"]) == ("run", "started"),
                "missing run start",
            )
            runs[key] = event["trace_id"]
        else:
            require(event["trace_id"] == runs[key], "cross-run event")
            if step == 49:
                require(
                    (event["kind"], event["phase"]) == ("run", "completed"),
                    "missing run completion",
                )
            else:
                phase = "started" if step % 2 else "finished"
                require(
                    (event["kind"], event["phase"]) == ("tool", phase),
                    "tool phase mismatch",
                )
                span_key = (*key, (step - 1) // 2)
                if phase == "started":
                    spans[span_key] = event["span_id"]
                else:
                    require(
                        event["span_id"] == spans[span_key],
                        "tool pair identity mismatch",
                    )
                    if actor in declared["sampled_agents"]:
                        expected.append(event["event_id"])
                        if cycle >= declared["warmup_cycles"]:
                            eligible.append(event["event_id"])
    require(
        len(set(runs.values())) == declared["expected_runs"], "run identities reused"
    )
    require(len(set(spans.values())) == len(spans), "span identities reused")
    require(
        len(expected) == declared["samples"]
        and len(eligible) == declared["measured_samples"],
        "declared sample cardinality mismatch",
    )
    require(
        render["valid"] is True and render["failure"] is None, "receiver invalidated"
    )
    require(render["expected"] == expected, "render denominator differs from source")
    require(render["eligible"] == eligible, "warmup or measured cohort substituted")
    require(set(render["acks"]) == set(expected), "missing or foreign acknowledgment")
    require(browser["samples"] == len(expected), "browser sample count mismatch")
    require(
        browser["lanes"] == [{"samples": declared["cycles"] * 24}] * 2,
        "browser lane population mismatch",
    )
    require(
        browser["page_errors"] == [] and browser["execution_writes"] == 0,
        "browser behavior failed",
    )
    emission = claimed["emission"]
    require(
        emission["events"] == len(events)
        and emission["waited_for_render_during_emission"] is False,
        "emission cohort mismatch",
    )
    require(
        math.isfinite(emission["seconds"])
        and emission["seconds"] >= declared["duration_s"],
        "declared duration not reached",
    )
    return dict(
        events=events,
        expected=expected,
        eligible=eligible,
        runs=len(runs),
        tool_pairs=len(spans),
    )


def audit_cohort(declared, scope, events, positions, commits, render, browser, claimed):
    cohort = audit_source_render(declared, scope, events, render, browser, claimed)
    events, expected, eligible = (
        cohort["events"],
        cohort["expected"],
        cohort["eligible"],
    )
    require(
        commits["valid"] is True and commits["failure"] is None,
        "commit observer invalidated",
    )
    records = commits["records"]
    require(len(records) == len(events), "commit denominator differs from source")
    by_id = {record["event_id"]: record for record in records}
    require(len(by_id) == len(records), "duplicate commit identity")
    require(
        set(by_id) == {e["event_id"] for e in events},
        "commit identities differ from source",
    )
    for event in events:
        record = by_id[event["event_id"]]
        require(
            all(
                record[key] == event[key]
                for key in ("node_id", "source_epoch", "trace_id", "agent_id")
            ),
            "commit source attribution mismatch",
        )
        require(
            (record["collector_epoch"], record["ingest_seq"])
            == positions[event["event_id"]],
            "commit position mismatch",
        )
        require(
            type(record["before_ns"]) is int
            and type(record["after_ns"]) is int
            and 0 <= record["before_ns"] <= record["after_ns"],
            "invalid commit bracket",
        )
    values = []
    widths = []
    for identity in expected:
        record, ack = by_id[identity], render["acks"][identity]
        require(type(ack) is int and ack >= record["after_ns"], "ack predates commit")
    for identity in eligible:
        record, ack = by_id[identity], render["acks"][identity]
        values.append((ack - record["before_ns"]) / 1_000_000)
        widths.append((record["after_ns"] - record["before_ns"]) / 1_000_000)
    require(
        claimed["samples"] == len(eligible)
        and claimed["warmup_and_measured_samples"] == len(expected),
        "reported sample count mismatch",
    )
    require(
        len(claimed["latency_upper_bounds_ms"]) == len(values),
        "reported timing count mismatch",
    )
    require(
        all(
            math.isclose(a, b, rel_tol=0, abs_tol=1e-6)
            for a, b in zip(values, claimed["latency_upper_bounds_ms"], strict=True)
        ),
        "reported timings differ from markers",
    )
    summary = summarize_latencies(values, len(eligible))
    for key, value in summary.items():
        require(claimed[key] == value, "reported statistic mismatch")
    require(
        claimed["commit_bracket_max_ms"] == max(widths),
        "reported commit width mismatch",
    )
    return {
        "source_events": len(events),
        "runs": cohort["runs"],
        "tool_pairs": cohort["tool_pairs"],
        "warmup_samples": len(expected) - len(eligible),
        "measured_samples": len(eligible),
        "all_source_commit_and_render_identities_verified": True,
        "mean_emitted_events_per_second": len(events) / claimed["emission"]["seconds"],
        **summary,
        "p95_numerical_target_met": summary["p95_upper_bound_ms"] <= 2000
        if summary["p95_sample_sufficient"]
        else None,
    }


def main():
    require(platform.node().lower() == "jim-eq", "audit runs on jim-eq only")
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--observer-control", action="store_true")
    args = parser.parse_args()
    directory = args.directory
    require(
        directory.is_absolute() and directory.is_dir(),
        "absolute run directory required",
    )

    def load(name):
        return json.loads((directory / name).read_text())

    declared, config = load("workload.json"), load("scope.json")
    claimed = load("control-result.json" if args.observer_control else "result.json")
    fixture = load("fixture.json")
    require(
        config["workload"] == fixture["workload"] == claimed["workload"] == declared,
        "workload records disagree",
    )
    require(
        fixture["source_core_exact"]
        and fixture["all_core_settled"]
        and fixture["owned_connector_revoked"],
        "fixture incomplete",
    )
    scope, agents = config["scope"], config["scope"]["agent_ids"]
    require(len(agents) == 10, "agent scope mismatch")
    placeholders = ",".join("?" for _ in agents)
    db = sqlite3.connect(
        "file:/root/.edgecitadel/agentd/agentd.sqlite3?mode=ro", uri=True
    )
    try:
        db.execute("BEGIN")
        rows = db.execute(
            f"SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json FROM trace_journal WHERE agent_id IN ({placeholders}) ORDER BY source_seq LIMIT ?",
            (*agents, declared["expected_events"] + 1),
        ).fetchall()
        states = db.execute(
            f"SELECT p.state FROM trace_spool p JOIN trace_journal j ON j.node_id=p.node_id AND j.source_epoch=p.source_epoch AND j.event_id=p.journal_event_id WHERE j.agent_id IN ({placeholders})",
            agents,
        ).fetchall()
        revoked = db.execute(
            f"SELECT COUNT(*) FROM connectors WHERE connector_id IN ({placeholders}) AND revoked_at_ms IS NOT NULL",
            agents,
        ).fetchone()[0]
        active = db.execute(
            f"SELECT COUNT(*) FROM sessions WHERE connector_id IN ({placeholders}) AND closed_at_ms IS NULL",
            agents,
        ).fetchone()[0]
    finally:
        db.close()
    require(revoked == 10 and active == 0, "owned connector/session cleanup incomplete")
    require(
        len(rows) == len(states) == declared["expected_events"]
        and all(s == ("core_settled",) for s in states),
        "settlement incomplete",
    )
    events = []
    positions = {}
    db = sqlite3.connect(
        "file:/root/.edgecitadel/core/data/openclaw.db?mode=ro", uri=True
    )
    try:
        db.execute("BEGIN")
        epoch = db.execute(
            "SELECT collector_epoch FROM trace_collector WHERE singleton=1"
        ).fetchone()[0]
        for row in rows:
            event = json.loads(row[-1])
            require(
                hashlib.sha256(row[-1].encode()).hexdigest() == row[-2],
                "source content hash mismatch",
            )
            require(
                tuple(
                    event[key]
                    for key in ("node_id", "source_epoch", "event_id", "source_seq")
                )
                == row[:4],
                "source header/content mismatch",
            )
            central = db.execute(
                "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,COALESCE(p.event_json,NULLIF(r.event_json,'')),r.ingest_seq FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq) WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                row[:3],
            ).fetchone()
            require(
                central is not None and central[:6] == row, "source/Core tuple mismatch"
            )
            events.append(event)
            positions[event["event_id"]] = (epoch, central[6])
    finally:
        db.close()
    render, browser = fixture["render"], load("browser.json")
    common = source_display_summary(declared, render, fixture["source_started_ns"])
    cpu = cpu_summary(fixture["core_cpu_before"], fixture["core_cpu_after"])
    require(claimed["source_to_display"] == common, "source/display report mismatch")
    require(claimed["core_cpu"] == cpu, "Core CPU report mismatch")
    commits = load("commits.json")
    require(
        claimed["commit_observer_enabled"] is (not args.observer_control),
        "observer mode mismatch",
    )
    if args.observer_control:
        require(
            commits == {"enabled": False, "records": []},
            "control retained commit markers",
        )
        require(
            not any(
                key in claimed
                for key in (
                    "latency_upper_bounds_ms",
                    "p95_upper_bound_ms",
                    "commit_bracket_max_ms",
                )
            ),
            "control claims commit timing",
        )
        cohort = audit_source_render(declared, scope, events, render, browser, claimed)
        report = dict(
            source_events=len(events),
            runs=cohort["runs"],
            tool_pairs=cohort["tool_pairs"],
            all_source_and_render_identities_verified=True,
            commit_observer_enabled=False,
        )
    else:
        require(commits["enabled"] is True, "commit observer disabled")
        report = audit_cohort(
            declared, scope, events, positions, commits, render, browser, claimed
        )
    report.update(
        source_to_display={
            key: value for key, value in common.items() if key != "values_ms"
        },
        core_cpu=cpu,
    )
    container = json.loads(
        subprocess.check_output(["docker", "inspect", "edgecitadel-aggregator-1"])
    )[0]
    require(
        claimed["normal_launcher_restored"] is True
        and load("restoration.json")["normal_launcher_restored"] is True,
        "restoration not recorded",
    )
    require(container["Image"] == claimed["core_image"], "Core image changed")
    require(
        container["Config"]["Cmd"]
        == [
            "uvicorn",
            "aggregator.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
            "--ws",
            "websockets",
            "--ws-max-size",
            "65536",
            "--ws-max-queue",
            "4",
            "--ws-per-message-deflate",
            "false",
        ],
        "normal Core command not restored",
    )
    require(
        not any(
            m["Destination"].startswith("/qualification") for m in container["Mounts"]
        ),
        "qualification mounts remain",
    )
    report.update(
        profile=declared["profile"],
        source_core_exact=True,
        all_core_settled=True,
        owned_connectors_revoked=revoked,
        owned_sessions_active=active,
        normal_core_image_command_and_mounts_verified=True,
        scope="Read-only source/Core/observer/cohort audit; not observer-overhead, stress/soak, actual execution or full M4-M7 acceptance.",
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
