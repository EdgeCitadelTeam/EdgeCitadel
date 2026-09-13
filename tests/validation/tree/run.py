#!/usr/bin/env python3
"""Run isolated contracts and write JSON/JUnit evidence; failures and skips are nonzero."""
from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
import time
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from bootstrap import DEFAULT_BINARY, VERSION

ROOT = Path(__file__).resolve().parent


class Result(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []

    def startTest(self, test):
        self.started = time.monotonic()
        super().startTest(test)

    def stopTest(self, test):
        problem = [(kind, detail) for kind, entries in (
            ("failure", self.failures), ("error", self.errors), ("skipped", self.skipped)
        ) for case, detail in entries if case is test]
        self.records.append({"test": test.id(), "seconds": time.monotonic() - self.started,
                             "status": problem[0][0] if problem else "passed",
                             "problems": problem, "evidence": getattr(test, "evidence", {})})
        super().stopTest(test)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nats-server", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    binary = args.nats_server.expanduser().resolve()
    output = (args.output_dir or ROOT / "artifacts" / datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")).resolve()
    # Avoid stale/overwritten evidence. Each invocation owns a fresh directory.
    output.mkdir(parents=True, exist_ok=False)
    report = {"scope": "loopback Core + two Leaves; synthetic broker messages only",
              "time_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "platform": platform.system(), "architecture": platform.machine(),
              "python": platform.python_version(), "all_passed": False,
              "sources_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in sorted(ROOT.glob("*.py"))}}
    try:
        if sys.version_info < (3, 11):
            raise RuntimeError("Python 3.11+ required")
        version = subprocess.check_output([str(binary), "--version"], text=True, timeout=5).strip()
        if version != f"nats-server: v{VERSION}":
            raise RuntimeError(f"Expected NATS {VERSION}; got {version!r}")
        dependency = importlib.metadata.version("nats-py")
        if dependency != "2.15.0":
            raise RuntimeError("Install the exact requirements.txt in an isolated environment")
        report.update(nats_server=version, nats_py=dependency,
                      binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        os.environ["TREE_NATS_BINARY"], os.environ["TREE_OUTPUT"] = str(binary), str(output)
        # Restrict discovery to this directory, independent of root pytest configuration.
        suite = unittest.defaultTestLoader.discover(str(ROOT), pattern="check_*.py")
        unittest.installHandler()
        result = unittest.TextTestRunner(verbosity=2, resultclass=Result).run(suite)
        passed = result.wasSuccessful() and result.testsRun == 9 and not result.skipped
        report.update(tests=result.records, tests_run=result.testsRun,
                      failures=len(result.failures), errors=len(result.errors),
                      skipped=len(result.skipped), all_passed=passed)
        xml = ET.Element("testsuite", name="standalone_tree", tests=str(result.testsRun),
                         failures=str(len(result.failures)), errors=str(len(result.errors)),
                         skipped=str(len(result.skipped)))
        for record in result.records:
            case = ET.SubElement(xml, "testcase", name=record["test"], time=f'{record["seconds"]:.6f}')
            for kind, detail in record["problems"]:
                ET.SubElement(case, kind).text = detail
        ET.ElementTree(xml).write(output / "junit.xml", encoding="utf-8", xml_declaration=True)
        return 0 if passed else 1
    except Exception as error:
        report["setup_error"] = f"{type(error).__name__}: {error}"
        print(report["setup_error"], file=sys.stderr)
        return 1
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"Evidence: {output}")


if __name__ == "__main__":
    raise SystemExit(main())
