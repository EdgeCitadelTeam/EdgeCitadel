from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

from scripts.research.benchmark_core import write_markdown_summary


NATIVE_SLICE = ["E1", "E4", "E5", "E7"]
ALL_WORKLOADS = [f"E{i}" for i in range(1, 13)] + ["SECURITY_TEMPORAL"]


def parse_workloads(raw: str) -> list[str]:
    if raw == "native":
        return NATIVE_SLICE.copy()
    if raw == "all":
        return [f"E{i}" for i in range(1, 13)]
    if raw == "security":
        return ["SECURITY_TEMPORAL"]
    selected = [item.strip().upper() for item in raw.split(",") if item.strip()]
    invalid = [item for item in selected if item not in ALL_WORKLOADS]
    if invalid:
        raise ValueError(f"unknown workloads: {', '.join(invalid)}")
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run EdgeCitadel research benchmark workloads.")
    parser.add_argument("--workloads", default="native")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--api-base", default="http://localhost/api")
    parser.add_argument("--nats-url", default="nats://127.0.0.1:4222")
    parser.add_argument("--nats-token", default=None)
    parser.add_argument("--mqtt-url", default="mqtt://127.0.0.1:1883")
    parser.add_argument("--out-dir", default="data/research/results")
    parser.add_argument("--target-agent", default="shell-1")
    parser.add_argument("--streaming-agent", default="gemma-1")
    parser.add_argument("--render-md", action="store_true")
    return parser


async def main() -> int:
    args = build_parser().parse_args()
    selected = parse_workloads(args.workloads)
    from scripts.research import workloads

    for workload_id in selected:
        run_one = getattr(workloads, f"run_{workload_id.lower()}")
        try:
            run, path = await run_one(args, Path(args.out_dir))
        except RuntimeError as exc:
            print(f"benchmark failed: {exc}", file=sys.stderr)
            return 2
        print(path)
        if args.render_md:
            print(write_markdown_summary(run, path))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
