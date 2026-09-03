from __future__ import annotations

import importlib
import os
import sys
import tomllib
from pathlib import Path

import pytest

from edgecitadel import __version__
from scripts.edgecitadel_cli import VERSION as CLI_VERSION


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_pip_metadata_and_runtime_assets_are_declared() -> None:
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())

    assert config["project"]["name"] == "edgecitadel"
    assert config["project"]["version"] == __version__ == CLI_VERSION
    assert config["project"]["scripts"]["edgecitadel"] == "edgecitadel.cli:main"
    wheel = config["tool"]["hatch"]["build"]["targets"]["wheel"]
    forced = wheel["force-include"]
    sources = wheel["sources"]
    data_prefix = (
        f"edgecitadel-{config['project']['version']}.data/data/share/edgecitadel"
    )
    assert forced["docker-compose.yml"] == f"{data_prefix}/docker-compose.yml"
    assert forced["scripts/research/lab_config.py"].endswith(
        "/scripts/research/lab_config.py"
    )
    assert sources["plugin-toolkit/src"] == f"{data_prefix}/plugin-toolkit/src"
    assert sources["schemas"] == f"{data_prefix}/schemas"


def test_pip_entrypoint_configures_source_checkout(monkeypatch) -> None:
    monkeypatch.delenv("EDGECITADEL_DISTRIBUTION", raising=False)
    monkeypatch.delenv("EDGECITADEL_INSTALL_ROOT", raising=False)
    sys.modules.pop("edgecitadel_cli", None)
    entrypoint = importlib.import_module("edgecitadel.cli")

    with pytest.raises(SystemExit, match="0"):
        entrypoint.main(["--version"])
    assert os.environ["EDGECITADEL_DISTRIBUTION"] == "pip"
    assert Path(os.environ["EDGECITADEL_INSTALL_ROOT"]) == REPO_ROOT
    sys.modules.pop("edgecitadel_cli", None)
