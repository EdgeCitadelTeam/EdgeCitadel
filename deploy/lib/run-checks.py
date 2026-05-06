#!/usr/bin/env python3
"""Run the 41 checks defined in checks.yaml and produce a table + exit code.

Usage:
  run-checks.py [--quiet] [--report-via-nats]

Exit codes:
  0   all green
  1   one or more checks failed
  2   critical failure (yaml unparseable, etc.)
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

try:
    import yaml  # PyYAML; not in stdlib. Install via apt: python3-yaml
except ImportError:
    print("run-checks.py: missing PyYAML — apt install python3-yaml", file=sys.stderr)
    sys.exit(2)

LIB_DIR = Path(__file__).resolve().parent
CHECKS_YAML = LIB_DIR / "checks.yaml"


@dataclass
class CheckResult:
    name: str
    category: str
    status: str        # "pass" | "fail" | "warn"
    elapsed_ms: int
    reason: str = ""
    remediation: str = ""


def run_check(check: dict, category: str) -> CheckResult:
    name = check["name"]
    cmd = check["cmd"]
    level = check.get("level", "error")
    remediation = check.get("remediation", "")
    # Per-check timeout override (default 30s); round-trip smoke needs more.
    timeout = check.get("timeout_sec", 30)

    t0 = time.monotonic()
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        elapsed = int((time.monotonic() - t0) * 1000)
        return CheckResult(name, category, "fail", elapsed,
                           reason=f"timed out ({timeout}s)", remediation=remediation)
    elapsed = int((time.monotonic() - t0) * 1000)

    if result.returncode == 0:
        return CheckResult(name, category, "pass", elapsed)
    status = "warn" if level == "warn" else "fail"
    reason = (result.stderr or result.stdout or "exit " + str(result.returncode)).strip().splitlines()[-1]
    return CheckResult(name, category, status, elapsed, reason=reason, remediation=remediation)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quiet", action="store_true", help="suppress per-row output, print summary only")
    ap.add_argument("--report-via-nats", action="store_true",
                    help="publish summary as system.health.aggregator-host envelope (Phase 5.x)")
    args = ap.parse_args()

    if not CHECKS_YAML.is_file():
        print(f"checks.yaml not found at {CHECKS_YAML}", file=sys.stderr)
        return 2
    with CHECKS_YAML.open() as f:
        try:
            doc = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(f"checks.yaml unparseable: {e}", file=sys.stderr)
            return 2

    results: list[CheckResult] = []
    for cat in doc.get("categories", []):
        for check in cat.get("checks", []):
            results.append(run_check(check, cat["name"]))

    if not args.quiet:
        print()
        print("EdgeCitadel Phase 5 Host Check —", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        print()
        print(f"{'Category':<22} {'Item':<32} Status")
        print("─" * 22, "─" * 32, "─" * 12)
        for r in results:
            sym = {"pass": "✓", "fail": "✗", "warn": "⚠"}[r.status]
            line = f"{r.category:<22} {r.name:<32} {sym} {r.status}"
            if r.status != "pass":
                line += f" — {r.reason[:40]}"
            print(line)
            if r.status != "pass" and r.remediation:
                print(" " * 56, "→", r.remediation)
        print()

    n = len(results)
    n_pass = sum(1 for r in results if r.status == "pass")
    n_fail = sum(1 for r in results if r.status == "fail")
    n_warn = sum(1 for r in results if r.status == "warn")
    print(f"TOTAL: {n} checks  ✓ {n_pass} passed  ✗ {n_fail} failed  ⚠ {n_warn} warnings")

    if args.report_via_nats:
        # TODO Phase 5.x — publish system.health.aggregator-host envelope
        pass

    return 1 if n_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
