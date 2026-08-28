import ast
import hashlib
import json
from pathlib import Path
from typing import cast

import pytest
import yaml
from edgecitadel_supervisor.cli import main
from edgecitadel_supervisor.inventory import build_inventory
from edgecitadel_supervisor.validator import validate_package
from jsonschema import Draft202012Validator

PACKAGE_ROOT = Path(__file__).parents[2] / "plugins" / "examples" / "placeholder"
SKILL_ROOT = PACKAGE_ROOT / "skills" / "placeholder"


def test_repository_placeholder_package_is_valid() -> None:
    package = validate_package(PACKAGE_ROOT)

    assert package.package_id == "local.placeholder"
    assert package.agent_skill_names == {"placeholder-agent": ("placeholder",)}
    assert len(package.skills) == 1
    skill = package.skills[0]
    assert (skill.name, skill.skill_id, skill.execution_name) == (
        "placeholder",
        "example.placeholder",
        "placeholder",
    )
    assert package.manifest == {
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentPlugin",
        "metadata": {
            "name": "placeholder",
            "displayName": "Placeholder Plugin",
            "description": "Demonstrates the EdgeCitadel plugin package contract.",
            "version": "0.1.0",
            "publisher": "local",
        },
        "compatibility": {
            "supervisorApi": ">=0.1.0,<0.2.0",
            "protocols": ["edgecitadel.plugin.v1"],
        },
        "runtime": {
            "command": ["python", "-m", "runtime"],
            "healthTimeoutSeconds": 10,
            "restartPolicy": "on-failure",
        },
        "skills": {"directory": "skills"},
        "agents": [
            {
                "id": "placeholder-agent",
                "skillNames": ["placeholder"],
                "listensBroadcast": False,
            }
        ],
        "permissions": {
            "knowledge": [],
            "messaging": {"outboundAgents": []},
            "network": {"outbound": []},
            "devices": [],
        },
        "security": {"sandbox": "restricted", "secrets": []},
        "extensions": {},
    }


def test_placeholder_binding_matches_the_edgecitadel_contract() -> None:
    binding = yaml.safe_load((SKILL_ROOT / "binding.yaml").read_text(encoding="utf-8"))

    assert binding == {
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentSkillBinding",
        "skillId": "example.placeholder",
        "version": "0.1.0",
        "execution": {"kind": "runtime-handler", "name": "placeholder"},
        "schemas": {
            "input": "schemas/input.json",
            "output": "schemas/output.json",
        },
        "requires": {"knowledge": [], "network": [], "devices": []},
        "extensions": {},
    }


def test_placeholder_skill_contains_the_bounded_portable_procedure() -> None:
    assert (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8") == (
        "---\n"
        "name: placeholder\n"
        "description: Validate the EdgeCitadel plugin package path without "
        "performing external work. Use when testing plugin discovery and procedural "
        "packaging.\n"
        "compatibility: Requires the EdgeCitadel plugin runtime v1 protocol.\n"
        "metadata:\n"
        '  version: "0.1.0"\n'
        "---\n\n"
        "# Placeholder validation procedure\n\n"
        "1. Accept an input object containing `body`.\n"
        "2. Do not access the network, filesystem, devices, secrets, or shared "
        "knowledge.\n"
        "3. Return an object whose `message` states that execution is intentionally "
        "unavailable.\n\n"
        "Success means the response matches `schemas/output.json` and produces no "
        "side effects.\n"
    )


@pytest.mark.parametrize(
    ("schema_name", "valid_document", "invalid_documents"),
    [
        (
            "input.json",
            {"body": "validate this package"},
            [{}, {"body": "value", "unknown": True}, {"body": 1}],
        ),
        (
            "output.json",
            {"message": "execution is intentionally unavailable"},
            [{}, {"message": "value", "unknown": True}, {"message": False}],
        ),
    ],
)
def test_placeholder_schemas_define_strict_typed_objects(
    schema_name: str,
    valid_document: dict[str, object],
    invalid_documents: list[dict[str, object]],
) -> None:
    schema = json.loads((SKILL_ROOT / "schemas" / schema_name).read_text())
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)

    assert validator.is_valid(valid_document)
    assert all(not validator.is_valid(document) for document in invalid_documents)


def test_placeholder_lock_is_canonical_complete_and_current() -> None:
    lock_path = PACKAGE_ROOT / "plugin.lock.json"
    lock_text = lock_path.read_text(encoding="utf-8")
    lock = json.loads(lock_text)
    assert lock_text == json.dumps(lock, indent=2, sort_keys=True) + "\n"

    actual_paths = sorted(
        path.relative_to(PACKAGE_ROOT).as_posix()
        for path in PACKAGE_ROOT.rglob("*")
        if path.is_file() and path != lock_path
    )
    locked_paths = [entry["path"] for entry in lock["files"]]
    assert locked_paths == actual_paths
    assert locked_paths == sorted(locked_paths)

    locked_hashes = {entry["path"]: entry["sha256"] for entry in lock["files"]}
    for relative_path in actual_paths:
        content = (PACKAGE_ROOT / relative_path).read_bytes()
        assert locked_hashes[relative_path] == hashlib.sha256(content).hexdigest()

    skill_hash = hashlib.sha256((SKILL_ROOT / "SKILL.md").read_bytes()).hexdigest()
    assert lock["skills"] == [
        {
            "contentSha256": skill_hash,
            "name": "placeholder",
            "skillId": "example.placeholder",
            "version": "0.1.0",
        }
    ]


@pytest.mark.parametrize(
    ("directory", "empty_statement"),
    [
        ("references", "No reference data ships in this example"),
        ("scripts", "No executable helper ships in this example"),
        ("assets", "No binary asset ships in this example"),
    ],
)
def test_optional_resource_directories_are_deliberately_empty(
    directory: str, empty_statement: str
) -> None:
    resource_root = SKILL_ROOT / directory

    assert {path.name for path in resource_root.iterdir()} == {"README.md"}
    readme = (resource_root / "README.md").read_text(encoding="utf-8")
    assert empty_statement in readme
    assert "deliberately empty" in readme


def test_package_readme_documents_validation_only_boundaries() -> None:
    readme = (PACKAGE_ROOT / "README.md").read_text(encoding="utf-8")

    assert "validation-only" in readme
    assert "must not be run for this milestone" in readme
    assert "package identity is `local.placeholder`" in readme
    assert "agent identity is `placeholder-agent`" in readme
    assert "portable skill identity is `example.placeholder`" in readme
    assert "Learned memory is stored externally" in readme
    assert "must not mutate the installed package" in readme
    assert "python3 -m venv .venv" in readme
    assert "python -m pip install -e '.[test]'" in readme
    assert "writes or regenerates `plugin.lock.json`" in readme
    assert "`validate` is read-only" in readme
    assert "edgecitadel_supervisor lock" in readme
    assert "edgecitadel_supervisor validate" in readme


def test_placeholder_runtime_is_a_guarded_nonimplementation() -> None:
    runtime_source = (PACKAGE_ROOT / "runtime" / "__main__.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(runtime_source)

    assert not any(
        isinstance(node, (ast.Import, ast.ImportFrom)) for node in ast.walk(tree)
    )
    main = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    )
    assert len(main.body) == 1
    failure = main.body[0]
    assert isinstance(failure, ast.Raise)
    assert isinstance(failure.exc, ast.Call)
    assert isinstance(failure.exc.func, ast.Name)
    assert failure.exc.func.id == "RuntimeError"
    assert len(failure.exc.args) == 1
    message = failure.exc.args[0]
    assert isinstance(message, ast.Constant)
    assert message.value == (
        "placeholder runtime execution is outside the plugin-infrastructure milestone"
    )

    guard = tree.body[-1]
    assert isinstance(guard, ast.If)
    assert ast.unparse(guard.test) == "__name__ == '__main__'"
    assert ast.unparse(guard.body[0]) == "raise SystemExit(main())"


def test_lock_and_validate_do_not_import_or_launch_runtime(
    valid_package: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import_sentinel = tmp_path / "runtime-imported"
    launch_sentinel = tmp_path / "runtime-launched"
    runtime = valid_package / "runtime"
    runtime.mkdir()
    (runtime / "__init__.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_sentinel)!r}).write_text('imported')\n",
        encoding="utf-8",
    )
    (runtime / "__main__.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(launch_sentinel)!r}).write_text('launched')\n",
        encoding="utf-8",
    )

    assert main(["lock", str(valid_package)]) == 0
    assert not import_sentinel.exists()
    assert not launch_sentinel.exists()

    capsys.readouterr()
    assert main(["validate", str(valid_package)]) == 0
    assert not import_sentinel.exists()
    assert not launch_sentinel.exists()


def test_placeholder_inventory_is_deterministic() -> None:
    first = build_inventory(validate_package(PACKAGE_ROOT))
    second = build_inventory(validate_package(PACKAGE_ROOT))

    assert first == second
    package = cast(dict[str, object], first["package"])
    assert package["id"] == "local.placeholder"


def test_placeholder_package_contains_no_generated_or_secret_files() -> None:
    package_paths = {
        path.relative_to(PACKAGE_ROOT).as_posix()
        for path in PACKAGE_ROOT.rglob("*")
        if path.is_file()
    }

    assert not any("__pycache__" in path for path in package_paths)
    assert not any(path.endswith((".pyc", ".pyo")) for path in package_paths)
    assert not any(
        Path(path).name in {".env", "credentials.json", "secrets.json"}
        for path in package_paths
    )
