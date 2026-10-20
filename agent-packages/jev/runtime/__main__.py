"""JEV managed process entrypoint."""

import asyncio
import os
from pathlib import Path

from edgecitadel_agentd.managed_runtime import run

from .coordinator import Coordinator, Journal


async def main():
    if not os.environ.get("TYPESAFE_API_KEY", "").strip():
        raise SystemExit("JEV requires the server-side TYPESAFE_API_KEY secret")
    journal = Journal(Path(os.environ["EDGECITADEL_PLUGIN_STATE_DIR"]) / "jev.sqlite3")
    coordinator = Coordinator(journal)
    try:
        await run(
            Path(__file__).with_name("config.yaml"),
            coordinator.handle,
            on_start=coordinator.reconcile,
        )
    finally:
        journal.db.close()


if __name__ == "__main__":
    asyncio.run(main())
