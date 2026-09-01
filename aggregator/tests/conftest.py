import os
import tempfile
from pathlib import Path
import pytest

REPO = Path(__file__).resolve().parents[2]

# Provide a safe default DB_PATH so module-level `app = make_app()` in
# aggregator.main can initialize on macOS dev hosts where `/data` is read-only.
# Per-test fixtures override this with their own tmp_path-based DB.
os.environ.setdefault(
    "DB_PATH",
    str(Path(tempfile.gettempdir()) / "edgecitadel-tests-default.db"),
)


@pytest.fixture(scope="session")
def envelope_schema_path():
    return REPO / "schemas" / "envelope.v1.json"


@pytest.fixture(scope="session")
def card_schema_path():
    return REPO / "schemas" / "agent-card.v1.json"
