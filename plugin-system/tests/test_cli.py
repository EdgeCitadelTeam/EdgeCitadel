from __future__ import annotations

import json
from pathlib import Path

import pytest

from edgecitadel_supervisor import cli
from edgecitadel_supervisor.cli import main
from edgecitadel_supervisor.errors import PluginError
from edgecitadel_supervisor.inventory import write_lock
from edgecitadel_supervisor.validator import validate_package


def test_lock_command_writes_canonical_lock(
    valid_package: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["lock", str(valid_package)]) == 0

    captured = capsys.readouterr()
    payload = {
        "lockfile": str((valid_package / "plugin.lock.json").resolve()),
        "packageId": "local.example",
        "status": "locked",
    }
    assert captured.out == json.dumps(payload, indent=2, sort_keys=True) + "\n"
    assert captured.err == ""
    assert json.loads(captured.out) == payload
    assert (valid_package / "plugin.lock.json").is_file()


def test_validate_command_emits_deterministic_inventory(
    valid_package: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = validate_package(valid_package, verify_integrity=False)
    write_lock(package)

    assert main(["validate", str(valid_package)]) == 0
    first = capsys.readouterr()
    assert main(["validate", str(valid_package)]) == 0
    second = capsys.readouterr()

    payload = json.loads(first.out)
    assert payload["package"]["id"] == "local.example"
    assert payload["skills"][0]["name"] == "placeholder"
    assert first.out == json.dumps(payload, indent=2, sort_keys=True) + "\n"
    assert first.out == second.out
    assert first.err == second.err == ""


def test_cli_reports_domain_error_only_on_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["validate", str(tmp_path / "missing")]) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.count("error:") == 1
    assert "plugin root not found" in captured.err.casefold()


def test_lock_overwrites_invalid_existing_lock(valid_package: Path) -> None:
    lock_path = valid_package / "plugin.lock.json"
    lock_path.write_text("not valid json", encoding="utf-8")

    assert main(["lock", str(valid_package)]) == 0

    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    assert lock["package"]["id"] == "local.example"


def test_validate_does_not_modify_lock(
    valid_package: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = validate_package(valid_package, verify_integrity=False)
    lock_path = write_lock(package)
    original = lock_path.read_bytes()

    assert main(["validate", str(valid_package)]) == 0

    capsys.readouterr()
    assert lock_path.read_bytes() == original


def test_cli_catches_plugin_error_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail_validation(root: str, *, verify_integrity: bool = True) -> None:
        raise PluginError("expected domain failure")

    monkeypatch.setattr(cli, "validate_package", fail_validation)

    assert main(["validate", "plugin"]) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error: expected domain failure\n"


def test_cli_does_not_catch_unexpected_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_validation(root: str, *, verify_integrity: bool = True) -> None:
        raise ValueError("programmer failure")

    monkeypatch.setattr(cli, "validate_package", fail_validation)

    with pytest.raises(ValueError, match="programmer failure"):
        main(["validate", "plugin"])


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["lock"],
        ["validate"],
        ["unknown", "plugin"],
        ["lock", "plugin", "extra"],
        ["validate", "plugin", "extra"],
    ],
)
def test_cli_requires_supported_command_and_plugin_root(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as error:
        main(argv)

    assert error.value.code == 2


@pytest.mark.parametrize("command", ["lock", "validate"])
def test_cli_subcommand_help_uses_argparse(command: str) -> None:
    with pytest.raises(SystemExit) as error:
        main([command, "--help"])

    assert error.value.code == 0
