from pathlib import Path

import pytest

from edgecitadel_supervisor.inventory import build_inventory
from edgecitadel_supervisor.validator import validate_package

PLUGINS = Path(__file__).parents[2] / "agent-packages"


@pytest.mark.parametrize(
    ("name", "package_id", "module", "skill_count", "kind", "runtime_kind"),
    [
        (
            "gemma",
            "edgecitadel.gemma",
            "edgecitadel_gemma_plugin",
            1,
            "ManagedAgent",
            "model_agent",
        ),
        (
            "homeassistant",
            "edgecitadel.homeassistant",
            "edgecitadel_homeassistant_plugin",
            4,
            "ManagedAgent",
            "service_adapter",
        ),
        (
            "hermes",
            "edgecitadel.hermes",
            "edgecitadel_hermes_plugin",
            1,
            "ManagedAgent",
            "service_adapter",
        ),
    ],
)
def test_builtin_plugin_is_locked_and_has_an_executable_runtime(
    name: str,
    package_id: str,
    module: str,
    skill_count: int,
    kind: str,
    runtime_kind: str | None,
) -> None:
    package = validate_package(PLUGINS / name)
    inventory = build_inventory(package)

    assert package.package_id == package_id
    assert inventory["package"]["kind"] == kind
    assert inventory["runtime"].get("kind") == runtime_kind
    assert inventory["runtime"]["command"] == ["python", "-m", module]
    assert len(inventory["skills"]) == skill_count
    assert (PLUGINS / name / module / "__main__.py").is_file()
    requirements = inventory["runtime"].get("pythonRequirements")
    if name == "gemma":
        assert requirements is None
    else:
        assert isinstance(requirements, str)
        assert (PLUGINS / name / requirements).is_file()
    adapter = (PLUGINS / name / module / "adapter.py").read_text()
    assert "edgecitadel_agentd.managed_runtime" in adapter
    assert "edgecitadel_plugin_runtime.template" not in adapter
