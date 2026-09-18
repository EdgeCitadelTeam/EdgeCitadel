from copy import deepcopy

import pytest

from edgecitadel_agentd.trace_capabilities import (
    TELEMETRY_EXTENSION_URI,
    TRACE_FAMILIES,
    negotiate_telemetry,
)
from edgecitadel_plugin_runtime.agent_card import build_card
from edgecitadel_plugin_runtime.validator import default_validator


def advertisement():
    return {
        "uri": TELEMETRY_EXTENSION_URI,
        "required": False,
        "params": {
            "schema_versions": [1],
            "families": {"task": "live", "tool": "historical", "model": "unsupported"},
        },
    }


def card(extension=None):
    return {"capabilities": {"extensions": [extension or advertisement()]}}


def test_new_reader_of_legacy_card_has_unknown_coverage():
    result = negotiate_telemetry({"capabilities": {"streaming": False}})
    assert result.status == "not_advertised"
    assert result.schema_version is None
    assert not result.live_families
    assert not result.unsupported_families  # Missing advertisement is not evidence.


def test_live_historical_and_unsupported_are_distinct():
    result = negotiate_telemetry(card())
    assert result.status == "negotiated"
    assert result.schema_version == 1
    assert result.live_families == frozenset({"task"})
    assert result.historical_families == frozenset({"tool"})
    assert result.unsupported_families == TRACE_FAMILIES - {"task", "tool"}


def test_version_intersection_and_unknown_versions():
    extension = advertisement()
    extension["params"]["schema_versions"] = [2, 1]
    assert negotiate_telemetry(card(extension)).schema_version == 1
    extension["params"]["schema_versions"] = [2]
    result = negotiate_telemetry(card(extension))
    assert result.status == "unsupported_version"
    assert result.schema_version is None
    assert not result.live_families


@pytest.mark.parametrize(
    "versions", [[], [True], [1, 1], [0], [65536], ["1"], [{}], list(range(1, 10))]
)
def test_malformed_versions_do_not_disable_messaging(versions):
    extension = advertisement()
    extension["params"]["schema_versions"] = versions
    assert negotiate_telemetry(card(extension)).status == "invalid_advertisement"


@pytest.mark.parametrize(
    "families", [{"tool": "guessed"}, {"prompt": "live"}, {"tool": []}, []]
)
def test_unknown_modes_or_families_fail_closed(families):
    extension = advertisement()
    extension["params"]["families"] = families
    assert negotiate_telemetry(card(extension)).status == "invalid_advertisement"


def test_duplicate_oversized_or_required_extension_is_not_negotiated():
    value = card()
    value["capabilities"]["extensions"].append(deepcopy(advertisement()))
    assert negotiate_telemetry(value).status == "invalid_advertisement"
    for changes in ({"required": True}, {"description": "x" * 4097}):
        extension = advertisement()
        extension.update(changes)
        assert negotiate_telemetry(card(extension)).status == "invalid_advertisement"


def test_legacy_card_validator_accepts_new_optional_extension(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("""agent_id: trace-fixture
name: trace-fixture
description: Owned compatibility fixture.
version: 0.1.0
runtime:
  kind: native
  roles: [worker]
skills: []
""")
    value = build_card(config)
    default_validator().validate_card(value)
    before = deepcopy(value)
    value["capabilities"].setdefault("extensions", []).append(advertisement())
    default_validator().validate_card(value)
    assert negotiate_telemetry(value).schema_version == 1
    assert value["url"] == before["url"]
    assert value["skills"] == before["skills"]
