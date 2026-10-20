"""Prevent checked-in Compose broker limits drifting from managed-Core rendering."""

from pathlib import Path

import pytest

from scripts import edgecitadel_cli as cli

ROOT = Path(__file__).parents[2]


def test_checked_in_default_matches_authoritative_template():
    assert (ROOT / "nats/nats.conf").read_bytes() == (
        ROOT / "nats/nats.conf.tpl"
    ).read_bytes()


@pytest.mark.parametrize("mqtt", [False, True])
def test_managed_render_preserves_capacity_and_mqtt_choice(tmp_path, monkeypatch, mqtt):
    monkeypatch.setattr(cli, "INSTALL_ROOT", ROOT)
    monkeypatch.setattr(cli, "CORE_RUNTIME_DIR", tmp_path)
    cli._render_nats_config(mqtt_enabled=mqtt)
    rendered = (tmp_path / "nats/nats.conf").read_text()
    assert "max_file: 2GB" in rendered
    assert ("\nmqtt {" in rendered) == mqtt
    assert "$NATS_TOKEN" in rendered and "$NATS_LEAF_PASSWORD" in rendered
