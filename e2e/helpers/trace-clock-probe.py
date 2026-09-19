"""Read-only prerequisite for the jim-eq commit/render latency harness."""

from __future__ import annotations

import json
import platform
import statistics
import subprocess
import time
from pathlib import Path


CONTAINER_PROBE = """
import json, os, time
from pathlib import Path
print(json.dumps({
    'monotonic_ns': time.monotonic_ns(),
    'clock': time.get_clock_info('monotonic').implementation,
    'resolution_seconds': time.get_clock_info('monotonic').resolution,
    'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
    'time_namespace': os.readlink('/proc/self/ns/time'),
}))
"""


def main():
    if platform.node().lower() != "jim-eq" or platform.system() != "Linux":
        raise SystemExit("Run this read-only probe on the existing jim-eq server")
    host_boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    host_namespace = str(Path("/proc/self/ns/time").readlink())
    samples = []
    for _ in range(20):
        before = time.monotonic_ns()
        result = subprocess.run(
            [
                "docker",
                "exec",
                "edgecitadel-aggregator-1",
                "python",
                "-c",
                CONTAINER_PROBE,
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        after = time.monotonic_ns()
        observed = json.loads(result.stdout)
        if not (
            before <= observed["monotonic_ns"] <= after
            and observed["boot_id"] == host_boot
            and observed["time_namespace"] == host_namespace
            and observed["clock"] == time.get_clock_info("monotonic").implementation
        ):
            raise RuntimeError(
                "Host/container monotonic clock agreement not established"
            )
        samples.append((after - before) / 1_000_000)
    print(
        json.dumps(
            {
                "host": "jim-eq",
                "container": "edgecitadel-aggregator-1",
                "samples": len(samples),
                "all_container_timestamps_inside_host_brackets": True,
                "same_boot_and_time_namespace": True,
                "clock": observed["clock"],
                "host_resolution_seconds": time.get_clock_info("monotonic").resolution,
                "container_resolution_seconds": observed["resolution_seconds"],
                "docker_exec_roundtrip_ms": {
                    "minimum": min(samples),
                    "median": statistics.median(samples),
                    "maximum": max(samples),
                },
                "scope": (
                    "Clock prerequisite only; docker exec is not a commit or render "
                    "observer. No display latency, observer overhead or p95 claim. "
                    "Repeat after host/container recreation or time-namespace changes."
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
