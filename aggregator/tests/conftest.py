import json
from pathlib import Path
import pytest

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def envelope_schema_path():
    return REPO / "schemas" / "envelope.v1.json"


@pytest.fixture(scope="session")
def card_schema_path():
    return REPO / "schemas" / "agent-card.v1.json"
