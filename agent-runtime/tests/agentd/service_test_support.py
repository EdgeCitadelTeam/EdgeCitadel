"""Explicit ordinary-store injection for owned protocol/lifecycle component tests.

Native admission is tested separately on jim-eq. This helper is never packaged.
"""

import signal
import sys
import threading
from pathlib import Path

from edgecitadel_agentd.service import serve as production_serve
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.writer_lock import WriterActiveError


def serve(state_dir, stop_event=None):
    return production_serve(
        state_dir,
        stop_event,
        open_store=lambda: AgentdStore(state_dir / "agentd.sqlite3"),
    )


if __name__ == "__main__":
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        serve(Path(sys.argv[1]), stop)
    except WriterActiveError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
