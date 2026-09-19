"""Own one jim-eq pilot and always restore the normal Core launcher."""

import argparse
import json
import os
import platform
import signal
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


from trace_latency_workload import baseline_workload, summarize_latencies, workload
from trace_live_control import cpu_summary, source_display_summary


BASE = [
    "docker",
    "compose",
    "--project-name",
    "edgecitadel",
    "--env-file",
    "/root/.edgecitadel/core/.env",
    "-f",
    "/root/.local/share/uv/tools/edgecitadel/share/edgecitadel/docker-compose.yml",
    "-f",
    "/root/.edgecitadel/core/docker-compose.managed.yml",
]


def run(command):
    return subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=90
    )


def ready():
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                "http://127.0.0.1/api/system/status", timeout=3
            ) as response:
                status = json.load(response)
            if (
                status["nats_connected"]
                and status["telemetry"]["connected"]
                and status["trace_projection"]["state"] == "running"
            ):
                return status
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(0.5)
    raise TimeoutError("Core readiness timeout")


def stop_owned(process):
    if process is not None and process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            raise RuntimeError("owned process required forced termination")


def main():
    if platform.node().lower() != "jim-eq":
        raise RuntimeError("jim-eq only")
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    profile = parser.add_mutually_exclusive_group()
    profile.add_argument("--open-loop-samples", type=int)
    profile.add_argument("--baseline", action="store_true")
    profile.add_argument("--baseline-preflight", action="store_true")
    parser.add_argument("--event-rate", type=float, default=25 / 3)
    parser.add_argument("--observer-control", action="store_true")
    args = parser.parse_args()
    if (args.baseline or args.baseline_preflight) and args.event_rate != 25 / 3:
        parser.error("baseline profiles require the fixed 25/3 event rate")
    if args.observer_control and not (args.baseline or args.baseline_preflight):
        parser.error("observer control requires a baseline profile")
    declared = (
        baseline_workload(preflight=args.baseline_preflight)
        if args.baseline or args.baseline_preflight
        else workload(args.open_loop_samples, args.event_rate)
    )
    out = args.directory
    if not out.is_absolute() or not out.is_dir() or list(out.iterdir()):
        raise ValueError("empty private absolute pilot directory required")
    (out / "workload.json").write_text(json.dumps(declared))
    (out / "observer.json").write_text(
        json.dumps({"enabled": not args.observer_control})
    )
    helpers = Path(__file__).resolve().parent
    inspect = json.loads(run(["docker", "inspect", "edgecitadel-aggregator-1"]).stdout)[
        0
    ]
    expected_files = BASE[7] + "," + BASE[9]
    if (
        inspect["Config"]["Labels"]["com.docker.compose.project.config_files"]
        != expected_files
    ):
        raise RuntimeError("Core has an unexpected active Compose override")
    override = out / "compose.json"
    override.write_text(
        json.dumps(
            {
                "services": {
                    "aggregator": {
                        "command": [
                            "python",
                            "/qualification/helpers/trace-latency-core.py",
                            "/qualification/run",
                        ],
                        "volumes": [
                            f"{helpers}:/qualification/helpers:ro",
                            f"{out}:/qualification/run",
                            "/etc/hostname:/qualification/run/host-name:ro",
                        ],
                    }
                }
            }
        )
    )
    timed = BASE + ["-f", str(override)]
    fixture = browser = None
    changed = False
    report = {
        "host": "jim-eq",
        "core_image": inspect["Image"],
        "normal_launcher_restored": False,
    }

    def terminate(*_):
        raise SystemExit(143)

    for stop_signal in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(stop_signal, terminate)
    try:
        with (out / "fixture.log").open("w") as log:
            fixture = subprocess.Popen(
                [
                    "/var/lib/edgecitadel-core/state/supervisor/bin/python",
                    str(
                        helpers
                        / (
                            "trace-baseline-fixture.py"
                            if declared["mode"] == "baseline"
                            else "trace-latency-fixture.py"
                        )
                    ),
                    str(out),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.monotonic() + 20
        while not (out / "scope.json").exists():
            if fixture.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("fixture did not establish scope")
            time.sleep(0.1)
        changed = True
        run(
            timed
            + ["up", "-d", "--no-build", "--force-recreate", "--no-deps", "aggregator"]
        )
        run(["docker", "exec", "edgecitadel-nginx-1", "nginx", "-s", "reload"])
        ready()
        clock = run(["/usr/bin/python3", str(helpers / "trace-clock-probe.py")])
        (out / "clock.json").write_text(clock.stdout)
        current = json.loads(
            run(["docker", "inspect", "edgecitadel-aggregator-1"]).stdout
        )[0]
        (out / "runtime.json").write_text(
            json.dumps({"core_pid": current["State"]["Pid"]})
        )
        with (out / "browser.log").open("w") as log:
            browser = subprocess.Popen(
                ["/usr/bin/node", str(helpers / "trace-latency-browser.js"), str(out)],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        browser_deadline = time.monotonic() + declared["browser_timeout_s"] + 30
        while browser.poll() is None:
            if fixture.poll() is not None and fixture.returncode != 0:
                raise RuntimeError(
                    "fixture exited before browser; see private fixture.log"
                )
            if time.monotonic() >= browser_deadline:
                raise TimeoutError("browser workload timeout")
            time.sleep(0.25)
        if browser.returncode != 0:
            raise RuntimeError("browser pilot failed; see private browser.log")
        if fixture.wait(timeout=120) != 0:
            raise RuntimeError("fixture pilot failed; see private fixture.log")
    finally:
        try:
            stop_owned(browser)
        finally:
            try:
                stop_owned(fixture)
            finally:
                if changed:
                    try:
                        run(timed + ["stop", "--timeout", "30", "aggregator"])
                    finally:
                        run(
                            BASE
                            + [
                                "up",
                                "-d",
                                "--no-build",
                                "--force-recreate",
                                "--no-deps",
                                "aggregator",
                            ]
                        )
                        run(
                            [
                                "docker",
                                "exec",
                                "edgecitadel-nginx-1",
                                "nginx",
                                "-s",
                                "reload",
                            ]
                        )
                        status = ready()
                        restored = json.loads(
                            run(
                                ["docker", "inspect", "edgecitadel-aggregator-1"]
                            ).stdout
                        )[0]
                        report["normal_launcher_restored"] = (
                            restored["Image"] == inspect["Image"]
                            and restored["Config"]["Cmd"] == inspect["Config"]["Cmd"]
                            and restored["Config"]["Labels"][
                                "com.docker.compose.project.config_files"
                            ]
                            == expected_files
                        )
                        report["collector_connected"] = status["telemetry"]["connected"]
                        (out / "restoration.json").write_text(
                            json.dumps(report, indent=2) + "\n"
                        )
                        assert report["normal_launcher_restored"]
    commits = json.loads((out / "commits.json").read_text())
    fixture_report = json.loads((out / "fixture.json").read_text())
    browser_report = json.loads((out / "browser.json").read_text())
    render = fixture_report["render"]
    assert render["valid"]
    assert fixture_report["source_core_exact"] and fixture_report["all_core_settled"]
    assert fixture_report["owned_connector_revoked"]
    assert (
        len(render["expected"])
        == len(render["acks"])
        == browser_report["samples"]
        == declared["samples"]
    )
    if declared["mode"] == "baseline":
        report.update(
            source_to_display=source_display_summary(
                declared, render, fixture_report["source_started_ns"]
            ),
            core_cpu=cpu_summary(
                fixture_report["core_cpu_before"], fixture_report["core_cpu_after"]
            ),
            commit_observer_enabled=not args.observer_control,
        )
    if args.observer_control:
        assert commits == {"enabled": False, "records": []}
        report.update(
            workload=declared,
            emission=fixture_report["emission"],
            source_core_exact=True,
            all_core_settled=True,
            event_count=fixture_report["event_count"],
            run_count=fixture_report["run_count"],
            owned_connector_revoked=True,
            browser=browser_report,
            scope="Matched commit-observer control; source-append-to-display and Core CPU only. No commit-to-render measurement, browser-observer or actual execution overhead claim.",
        )
        (out / "control-result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report), flush=True)
        return
    assert commits["enabled"] is True and commits["valid"]
    markers = {record["event_id"]: record for record in commits["records"]}
    upper = []
    widths = []
    measured_count = declared.get("measured_samples", declared["samples"])
    assert len(render["eligible"]) == measured_count
    for identity in render["eligible"]:
        marker = markers[identity]
        assert marker["before_ns"] <= marker["after_ns"] <= render["acks"][identity]
        upper.append((render["acks"][identity] - marker["before_ns"]) / 1_000_000)
        widths.append((marker["after_ns"] - marker["before_ns"]) / 1_000_000)
    report.update(
        trace_id=fixture_report["trace_id"],
        samples=measured_count,
        warmup_and_measured_samples=declared["samples"],
        workload=declared,
        emission=fixture_report["emission"],
        source_core_exact=True,
        all_core_settled=True,
        event_count=fixture_report["event_count"],
        run_count=fixture_report.get("run_count", 1),
        owned_connector_revoked=True,
        commit_bracket_max_ms=max(widths),
        observer_callback_max_ms=commits["max_callback_ns"] / 1_000_000,
        latency_upper_bounds_ms=upper,
        **summarize_latencies(upper, measured_count),
        browser=browser_report,
        scope="Declared synthetic workload on one source host; no actual tools or multi-host topology. Bounds include step reveal and render ACK transport; callback metric is not total observer overhead. Warmup is excluded from statistics, never from required correlations; profile declares whether full baseline duration ran.",
    )
    (out / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
