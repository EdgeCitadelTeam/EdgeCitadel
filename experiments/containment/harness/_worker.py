"""The det-worker package's deterministic content model, shared with the checkers.

The package is deployed on the runtime nodes on its own; the harness imports
the same module so expected contents are computed by the code that produces them.
"""

import sys
from pathlib import Path

DET_WORKER_ROOT = Path(__file__).resolve().parents[3] / "agent-packages" / "det-worker"
if str(DET_WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(DET_WORKER_ROOT))

from det_worker import content, effects  # noqa: E402

__all__ = ["DET_WORKER_ROOT", "content", "effects"]
