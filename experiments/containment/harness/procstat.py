"""CPU and memory of one run's processes on one host, sampled from /proc (Linux).

The stack starts every process in its own session, so each process's group id
is the pid the stack recorded. The stack writes ``{name: pgid}`` for this
host's processes to ``--groups`` and starts this sampler; every ``--interval``
it sums, per group, the CPU time (user + system, live processes) and resident
memory, and appends one line to ``--out``:

  {"t_ms": ..., "groups": {"agentd-core": {"cpu_s": 1.23, "rss_kb": 51200, "n": 2}, ...}}

CPU used over a window is the difference of ``cpu_s`` between two samples;
peak memory is the largest ``rss_kb``. Reading /proc costs well under 1% of a
core at 200 ms for a few dozen processes. Off unless the stack file enables it
(``procstat``), so containment runs keep their timing unchanged.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from pathlib import Path

TICKS = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
PAGE_KB = (os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096) // 1024


def _proc_stat(pid: str) -> tuple[int, float, int] | None:
    """(process group, CPU seconds, RSS kB) of one process, or None if gone."""
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # The command name may contain spaces; fields resume after its closing ')'.
    fields = raw[raw.rindex(")") + 2 :].split()
    pgrp = int(fields[2])
    cpu = (int(fields[11]) + int(fields[12])) / TICKS
    rss_kb = int(fields[21]) * PAGE_KB
    return pgrp, cpu, rss_kb


def sample(groups: dict[str, int]) -> dict[str, dict[str, float]]:
    by_pgid = {pgid: name for name, pgid in groups.items()}
    out = {name: {"cpu_s": 0.0, "rss_kb": 0, "n": 0} for name in groups}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        stat = _proc_stat(pid)
        if stat is None or stat[0] not in by_pgid:
            continue
        entry = out[by_pgid[stat[0]]]
        entry["cpu_s"] = round(entry["cpu_s"] + stat[1], 3)
        entry["rss_kb"] += stat[2]
        entry["n"] += 1
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.procstat")
    parser.add_argument("--groups", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--interval", type=float, default=0.2)
    args = parser.parse_args(argv)
    groups = {k: int(v) for k, v in json.loads(args.groups.read_text()).items()}
    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with args.out.open("a") as handle:
        while running:
            started = time.monotonic()
            line = {"t_ms": time.time_ns() // 1_000_000, "groups": sample(groups)}
            handle.write(json.dumps(line, separators=(",", ":")) + "\n")
            handle.flush()
            time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
