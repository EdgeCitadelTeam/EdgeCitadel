"""Unit tests: hermes config.yaml produces a valid A2A v1 agent card."""

from pathlib import Path

import pytest
import edgecitadel_hermes_plugin

from edgecitadel_plugin_runtime.agent_card import build_card

CONFIG_PATH = Path(edgecitadel_hermes_plugin.__file__).resolve().parent / "config.yaml"


@pytest.fixture(scope="module")
def card():
    return build_card(CONFIG_PATH)


def test_card_name_is_us_mac_hermes(card):
    assert card["name"] == "us-mac-hermes"


def test_card_runtime_kind_bridge(card):
    assert card["metadata"]["runtime.kind"] == "bridge"


def test_card_runtime_upstream_set(card):
    assert card["metadata"]["runtime.upstream"] == "hermes-agent"


def test_card_streaming_capability_true(card):
    assert card["capabilities"]["streaming"] is True


def test_card_external_memory_tag_present(card):
    assert "external-memory" in card["metadata"]["runtime.tags"]


def test_card_has_single_chat_skill(card):
    assert len(card["skills"]) == 1
    assert card["skills"][0]["id"] == "reasoning.chat"


def test_card_url_uses_nats_inbox(card):
    assert card["url"] == "nats://edgecitadel/agents.us-mac-hermes.inbox"


def test_card_validates_against_a2a_schema():
    import json
    import jsonschema

    schema = json.loads(
        (
            Path(__file__).resolve().parents[3] / "schemas" / "agent-card.v1.json"
        ).read_text()
    )
    jsonschema.validate(build_card(CONFIG_PATH), schema)
