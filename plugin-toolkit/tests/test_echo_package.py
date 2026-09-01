from pathlib import Path

from edgecitadel_supervisor.inventory import build_inventory
from edgecitadel_supervisor.validator import validate_package


PACKAGE_ROOT = Path(__file__).parents[2] / "plugins" / "examples" / "echo"


def test_repository_echo_package_is_locked_and_executable() -> None:
    package = validate_package(PACKAGE_ROOT)
    inventory = build_inventory(package)

    assert package.package_id == "edgecitadel.echo"
    assert package.agent_skill_names == {"echo-agent": ("echo",)}
    assert inventory["runtime"]["command"] == ["python", "-m", "runtime"]
    assert inventory["permissions"]["messaging"]["outboundAgents"] == ["aggregator"]


def test_echo_runtime_contains_the_join_lifecycle() -> None:
    source = (PACKAGE_ROOT / "runtime" / "__main__.py").read_text()

    assert "agents.{AGENT_ID}.register" in source
    assert "agents.{AGENT_ID}.heartbeat" in source
    assert "agents.{AGENT_ID}.inbox" in source
    assert '"result"' in source
