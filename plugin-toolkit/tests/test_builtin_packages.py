from pathlib import Path

import pytest

from edgecitadel_supervisor.inventory import build_inventory
from edgecitadel_supervisor.validator import validate_package


PLUGINS = Path(__file__).parents[2] / "plugins"


@pytest.mark.parametrize(
    ("name", "package_id", "module", "skill_count"),
    [
        ("gemma", "edgecitadel.gemma", "edgecitadel_gemma_plugin", 4),
        ("watchdog", "edgecitadel.watchdog", "edgecitadel_watchdog_plugin", 0),
        (
            "homeassistant",
            "edgecitadel.homeassistant",
            "edgecitadel_homeassistant_plugin",
            4,
        ),
        ("hermes", "edgecitadel.hermes", "edgecitadel_hermes_plugin", 1),
        ("shell", "edgecitadel.shell", "edgecitadel_shell_plugin", 1),
    ],
)
def test_builtin_plugin_is_locked_and_has_an_executable_runtime(
    name: str, package_id: str, module: str, skill_count: int
) -> None:
    package = validate_package(PLUGINS / name)
    inventory = build_inventory(package)

    assert package.package_id == package_id
    assert inventory["runtime"]["command"] == ["python", "-m", module]
    assert len(inventory["skills"]) == skill_count
    assert (PLUGINS / name / module / "__main__.py").is_file()
    requirements = inventory["runtime"]["pythonRequirements"]
    assert (PLUGINS / name / requirements).is_file()
