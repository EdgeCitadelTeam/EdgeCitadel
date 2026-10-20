"""Temporary jim-eq Core launcher; normal product entrypoint remains unchanged."""

import json
import os
import signal
import sys
from contextlib import nullcontext
from pathlib import Path

import uvicorn

from trace_commit_observer import CommitObserver, instrument_collector


def main():
    directory = Path(sys.argv[1])
    if not directory.is_absolute():
        raise ValueError("absolute qualification directory required")
    if (directory / "host-name").read_text().strip().lower() != "jim-eq":
        raise RuntimeError("qualification is restricted to jim-eq")
    config = json.loads((directory / "scope.json").read_text())
    enabled = json.loads((directory / "observer.json").read_text())["enabled"]
    if type(enabled) is not bool:
        raise ValueError("invalid observer mode")
    observer = (
        CommitObserver(
            **config["scope"], capacity=max(4096, config["workload"]["expected_events"])
        )
        if enabled
        else None
    )
    sys.path.insert(0, str(Path.cwd()))
    from aggregator import trace_collector

    # Uvicorn re-raises captured signals after graceful lifespan shutdown.
    # Defer their default termination until this launcher has saved its report.
    signal.signal(signal.SIGTERM, lambda *_: None)
    signal.signal(signal.SIGINT, lambda *_: None)
    try:
        with (
            instrument_collector(trace_collector, observer)
            if enabled
            else nullcontext()
        ):
            uvicorn.run(
                "aggregator.main:app",
                host="0.0.0.0",
                port=8000,
                ws="websockets",
                ws_max_size=65536,
                ws_max_queue=4,
                ws_per_message_deflate=False,
            )
    finally:
        target = directory / "commits.json"
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            report = (
                observer.report() | {"enabled": True}
                if enabled
                else {"enabled": False, "records": []}
            )
            json.dump(report, output, indent=2)
            output.write("\n")


if __name__ == "__main__":
    main()
