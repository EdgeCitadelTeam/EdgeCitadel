"""Explicit offline native storage provisioning and schema/layout upgrade."""

import argparse
import json
import platform
from pathlib import Path

from .storage_layout import StorageLayout
from .storage_migration import migrate_storage
from .storage_macos import setup_macos_storage
from .restore import RESTORE_BARRIER, require_startable
from .writer_lock import exclusive_writer


def setup_storage(state: Path) -> dict:
    state = state.expanduser().resolve()
    with exclusive_writer(state):
        if platform.system() == "Darwin":
            setup_macos_storage(state)
        else:
            from .trace_quota import verify_trace_quota

            verify_trace_quota(state / "trace", state)
    # Migration owns the same locks independently. Concurrent startup cannot
    # admit a legacy source or a barrier; fresh storage can safely start here.
    if (state / "agentd.sqlite3").exists() or (state / RESTORE_BARRIER).exists():
        migrate_storage(state)
    require_startable(state)
    return StorageLayout(state).status()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(setup_storage(args.state_dir), sort_keys=True))


if __name__ == "__main__":
    main()
