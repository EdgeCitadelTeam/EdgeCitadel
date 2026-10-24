"""The scaled testbed: team placement, workloads, stack, metrics, sim export."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from harness import coreq, runner, runstats, scale
from harness import spec as S
from harness import stack as stack_mod

ROOT = Path(__file__).resolve().parents[1]
WORKLOADS = ROOT / "workloads" / "scale"


def load(name: str) -> S.Spec:
    return S.load(WORKLOADS / f"{name}.json")


@pytest.mark.parametrize("sites", scale.SITES)
def test_team_groups_are_connected_balanced_and_rooted_on_core(sites):
    spec = load("rand-n63-s0-S3")
    groups = scale.team_groups(spec, sites)
    assert len(groups) == sites
    parent = {a.id: a.parent for a in spec.agents}
    root = next(a for a, p in parent.items() if p is None)
    assert groups[0][0] == root  # the root's team first (node core)
    seen = [a for group in groups for a in group]
    assert sorted(seen) == sorted(parent) and len(seen) == len(set(seen))
    for group in groups:
        lead, members = group[0], set(group)
        # Connected: every member but the lead has its parent in the group.
        assert all(parent[a] in members for a in group if a != lead)
    sizes = [len(g) for g in groups]
    assert max(sizes) <= 2 * (len(seen) / sites) + 1
    assert scale.team_groups(spec, sites) == groups  # deterministic


def test_every_S_places_the_same_workflow_on_its_own_sites():
    for seed in scale.SEEDS:
        base = load(f"rand-n63-s{seed}-S3")
        assert len(base.agents) == scale.N_AGENTS
        assert scale.alert_gap(base) >= scale.MIN_ALERT_GAP_S
        for sites in scale.SITES:
            spec = load(f"rand-n63-s{seed}-S{sites}")
            assert S.to_dict(spec)["tasks"] == S.to_dict(base)["tasks"]
            assert spec.fault == base.fault and spec.alerts == base.alerts
            nodes = {spec.placement[a.site] for a in spec.agents}
            assert nodes == set(scale.node_names(sites))
            root = next(a for a in spec.agents if a.parent is None)
            assert spec.placement[root.site] == "core"
            # The committed file is the generator's placement.
            assert spec.placement == scale.placed(base, sites).placement


def test_the_lab_stack_keeps_core_on_jim_eq_and_its_own_ports_and_units(
    monkeypatch,
):
    path = ROOT / "configs" / "lab-scale.json"
    document = json.loads(path.read_text())
    assert document == scale.stack_document(32)
    import re

    for name in re.findall(r"\$\{(\w+)\}", path.read_text()):
        monkeypatch.setenv(name, f"/x/{name.lower()}")
    config = stack_mod.StackConfig.load(path)
    assert config.node_hosts["core"] == "jim-eq"
    edges = {n: h for n, h in config.node_hosts.items() if n != "core"}
    assert len(edges) == 31 and set(edges.values()) == {"netsys-endurance"}
    assert config.hosts["netsys-endurance"].unit_prefix == "firebreak-exp-s-"
    assert config.runtime_env == {
        "EDGECITADEL_TRANSPORT_POLL_MS": "1000",
        "EDGECITADEL_CLAIM_POLL_MS": "1000",
    }
    assert config.effect_sink == {"kind": "ledger"}
    assert config.procstat_interval_ms == 1000
    scale.check_ports(config.ports)
    for reserved in (4222, 8000, 8123, 14322, 18322, 18900, 18800, 19500, 19400):
        with pytest.raises(ValueError, match="reserved"):
            scale.check_ports({"nats": reserved})
    dry = stack_mod.StackConfig.load(ROOT / "configs" / "dryrun-scale.json")
    assert len(dry.node_hosts) == 8


def test_the_scale_grid_has_its_planned_runs():
    cells = runner.grid(runner.Experiment.load(ROOT / "experiments" / "scale.json"))
    assert len(cells) == 72
    extra = runner.grid(
        runner.Experiment.load(ROOT / "experiments" / "scale-extra.json")
    )
    assert len(extra) == 27
    assert not {c.run_id for c in cells} & {c.run_id for c in extra}
    by_sites = {}
    for cell in cells:
        sites = int(cell.instance.stem.rsplit("-S", 1)[1])
        by_sites.setdefault(sites, set()).add(cell.strategy)
    assert by_sites[32] == {
        "firebreak",
        "trace_and_quarantine",
        "stop_team",
        "none",
        "reference",
    }
    assert all(by_sites[s] == {"firebreak", "reference"} for s in (3, 8, 16))
    assert {c.rogue_rate for c in cells} == {0.0, 0.3}


def test_sim_export_writes_each_site_assignment_and_names_its_spec(tmp_path):
    path = WORKLOADS / "rand-n63-s0-S8.json"
    stack = scale.stack_document(32)
    manifest = scale.sim_export([path], stack, tmp_path)
    entry = manifest["instances"]["rand-n63-s0-S8"]
    assert entry["sites"] == 8 and entry["agents"] == 63
    again = S.load(tmp_path / entry["spec"])
    assert S.to_dict(again) == S.to_dict(S.load(path))
    sites = json.loads((tmp_path / entry["sites_file"]).read_text())
    agent = next(iter(again.agents))
    assert sites["agents"][agent.id]["sim_id"] == agent.label
    assert sites["agents"][agent.id]["node"] == again.placement[agent.site]
    assert sites["nodes"]["core"]["host"] == "jim-eq"
    assert manifest["control_latency_ms"] == 400


def test_a_distribution_is_nearest_rank():
    assert runstats.distribution([]) == {"n": 0}
    summary = runstats.distribution(range(1, 101))
    assert summary == {"n": 100, "p50": 50.0, "p95": 95.0, "p99": 99.0, "max": 100.0}


def decision(*records):
    return {"type": "decision", "phase1": list(records), "phase2": [], "released": []}


def record(command, sent, confirmed, ingested, node="edge-1"):
    return {
        "action": {"op": "hold", "kind": "task", "object_id": command, "node_id": node},
        "state": "in_force",
        "sites": {
            node: {
                "command_id": command,
                "t_sent": sent,
                "t_confirmed": confirmed,
                "t_ingested": ingested,
            }
        },
    }


def test_runstats_columns_from_decisions_ingest_and_process_samples(tmp_path):
    a = record("a", 1000, 1040, 1100)
    decisions = [
        decision(a, record("b", 1000, 1020, None, node="core")),
        decision(a),  # listed again by a re-solve: counted once
    ]
    assert runstats.fence_lags(decisions) == [60.0]
    (tmp_path / "core_ingest.json").write_text(
        json.dumps(
            {
                "all": {"n": 3, "p50": 20.0, "p95": 90.0, "p99": 90.0, "max": 90.0},
                "by_node": {"core": {"n": 1, "p50": 5.0}},
            }
        )
    )

    def sample(t, cpu_agg, cpu_a, cpu_b):
        return {
            "t_ms": t,
            "groups": {
                "aggregator": {"cpu_s": cpu_agg, "rss_kb": 204800, "n": 1},
                "agentd-core": {"cpu_s": cpu_a, "rss_kb": 51200, "n": 1},
                "agentd-edge-1": {"cpu_s": cpu_b, "rss_kb": 61440, "n": 1},
                "worker-a1": {"cpu_s": 1.0, "rss_kb": 30720, "n": 1},
                "nats": {"cpu_s": 0.5, "rss_kb": 10240, "n": 1},
            },
        }

    (tmp_path / "procstat__jim-eq.jsonl").write_text(
        "".join(
            json.dumps(s) + "\n"
            for s in (
                sample(0, 1.0, 1.0, 2.0),
                sample(5000, 3.0, 2.0, 5.0),
                sample(20000, 4.0, 3.0, 9.0),
            )
        )
    )
    meta = {
        "control_latency_ms": {"core": 400, "edge-1": 400},
        "t0_ms": 0,
        "t_end_ms": 20000,
        "t_alert_ms": 0,
    }
    row = runstats.columns(tmp_path, meta, decisions)
    assert row["n_sites"] == 2 and row["n_confirm"] == 2
    assert (row["confirm_ms_p50"], row["confirm_ms_max"]) == (20.0, 40.0)
    assert json.loads(row["confirm_ms_by_site"])["edge-1"]["max"] == 40.0
    assert row["fence_lag_ms_p50"] == 60.0
    assert row["ingest_lag_ms_p95"] == 90.0
    assert json.loads(row["ingest_lag_ms_by_node"]) == {"core": {"n": 1, "p50": 5.0}}
    assert row["core_cpu_s"] == 3.0 and row["core_rss_peak_mb"] == 200.0
    assert row["agentd_cpu_s"] == 9.0 and row["agentd_cpu_s_max"] == 7.0
    assert row["core_cpu_s_response"] == 2.0  # the first 10 s after the alert
    assert row["agentd_rss_peak_mb"] == 110.0
    assert row["workers_cpu_s"] == 0.0 and row["nats_cpu_s"] == 0.0
    empty = runstats.columns(tmp_path / "missing", {}, [])
    assert empty["n_confirm"] == 0 and empty["core_cpu_s"] != empty["core_cpu_s"]


def test_core_ingest_lag_is_receipt_minus_occurrence_per_node(tmp_path, monkeypatch):
    db = tmp_path / "core.db"
    core = sqlite3.connect(db)
    core.execute(
        "CREATE TABLE trace_raw_events(node_id TEXT, received_at_ms INTEGER, "
        "ingest_seq INTEGER, event_json TEXT)"
    )
    occurred = "2001-09-09T01:46:40.000Z"  # 1_000_000_000_000 ms
    rows = [
        ("core", 1_000_000_000_050, 1, json.dumps({"occurred_at": occurred})),
        ("edge-1", 1_000_000_000_400, 2, json.dumps({"occurred_at": occurred})),
        ("edge-1", 1_000_000_000_100, 3, json.dumps({"occurred_at": "not a time"})),
        # A separated payload: event_json is empty, the payload holds it.
        ("edge-2", 1_000_000_000_070, 4, ""),
        ("edge-2", 1_000_000_000_070, 5, ""),  # an expired payload
    ]
    core.executemany("INSERT INTO trace_raw_events VALUES (?,?,?,?)", rows)
    core.commit()
    core.close()
    payloads = {4: {"event": {"occurred_at": occurred}}}
    monkeypatch.setattr(coreq, "read_payload", lambda db, seq: payloads.get(seq))
    assert coreq.ingest_lags(db) == {"core": [50], "edge-1": [400], "edge-2": [70]}
    # Events that occurred before the run's start are left out.
    assert coreq.ingest_lags(db, since_ms=1_000_000_000_001) == {}


def test_a_hosts_unit_prefix_scopes_its_units_and_their_cleanup(monkeypatch, tmp_path):
    log = "/w/rand-n63-s0-S32__early/logs/agentd-edge-1.log"
    assert stack_mod.unit_name(log, "agentd-edge-1").startswith("firebreak-exp-h-")
    assert stack_mod.unit_name(log, "x", "firebreak-exp-s-").startswith(
        "firebreak-exp-s-rand-n63"
    )
    host = stack_mod.SSHHost(
        stack_mod.HostConfig(
            name="e",
            address="e",
            python="p",
            repo="r",
            runtime_root="r",
            work_root="/w",
            ssh="e",
            units="user",
            unit_prefix="firebreak-exp-s-",
        )
    )
    commands = []

    def ssh(command, **_):
        commands.append(command)
        return (
            "firebreak-exp-s-run-agentd.service\nfirebreak-exp-s-runner.service\n"
            if "list-units" in command
            else ""
        )

    monkeypatch.setattr(host, "_ssh", ssh)
    assert host.stop_leftover_units() == ["firebreak-exp-s-run-agentd.service"]
    assert "'firebreak-exp-s-*'" in commands[0]
    assert "firebreak-exp-h-" not in " ".join(commands)
    bad = {
        "mode": "lab",
        "controller": "a",
        "hosts": {
            "a": {
                "address": "a",
                "python": "p",
                "runtime_root": "r",
                "work_root": "/w",
                "unit_prefix": "other-",
            }
        },
        "node_hosts": {"core": "a"},
    }
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(bad))
    with pytest.raises(stack_mod.StackError, match="unit_prefix"):
        stack_mod.StackConfig.load(path)


def test_the_committed_sim_export_covers_every_lab_workload():
    sim = WORKLOADS / "sim"
    manifest = json.loads((sim / "manifest.json").read_text())
    names = {p.stem for p in WORKLOADS.glob("rand-n63-*.json")}
    assert set(manifest["instances"]) == names
    for name, entry in manifest["instances"].items():
        assert (sim / entry["spec"]).resolve() == (WORKLOADS / f"{name}.json").resolve()
        assert (sim / entry["sites_file"]).exists()


def test_an_alert_waits_a_bounded_time_for_core_to_know_its_root(monkeypatch):
    answers = iter([None, None, "attempt-1"])
    stack = type("Stack", (), {"attempt_of": lambda self, rt: next(answers)})()
    monkeypatch.setattr(runner, "ATTEMPT_LOOKUP_FAST_S", 0.0)
    meta: dict = {}
    assert runner._wait_for_root(stack, "task-1", 5.0, meta) == "attempt-1"
    assert meta["alert_root_wait_ms"] >= 0
    never = type("Stack", (), {"attempt_of": lambda self, rt: None})()
    assert runner._wait_for_root(never, "task-1", 0.05, meta) is None
    loaded = runner.Experiment.load(ROOT / "experiments" / "scale.json")
    assert loaded.root_wait_s == 10.0
    assert all(part.root_wait_s == 10.0 for part in loaded.parts)
