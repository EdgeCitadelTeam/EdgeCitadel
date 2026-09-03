from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import installation_assets as assets
from scripts import plugin_installation as plugins


def _write_plugin_assets(root: Path, directory: str = "plugins") -> Path:
    plugin_root = root / directory
    (plugin_root / ".agents" / "plugins").mkdir(parents=True)
    (plugin_root / ".agents" / "plugins" / "marketplace.json").write_text("{}")
    (plugin_root / ".claude-plugin").mkdir()
    (plugin_root / ".claude-plugin" / "marketplace.json").write_text("{}")
    (plugin_root / "pi-edgecitadel").mkdir()
    (plugin_root / "pi-edgecitadel" / "package.json").write_text("{}")
    return plugin_root


def test_asset_resolver_prefers_new_layout_and_warns_for_legacy(tmp_path, capsys):
    current = _write_plugin_assets(tmp_path)
    _write_plugin_assets(tmp_path, "native-plugins")

    assert assets.plugins_root(tmp_path) == current.resolve()
    assert capsys.readouterr().err == ""

    for child in sorted(current.rglob("*"), reverse=True):
        child.unlink() if child.is_file() else child.rmdir()
    current.rmdir()
    assert assets.plugins_root(tmp_path) == (tmp_path / "native-plugins").resolve()
    assert "legacy Plugin layout" in capsys.readouterr().err


def test_asset_resolver_keeps_agent_package_and_platform_fallbacks(tmp_path, capsys):
    package = tmp_path / "plugins" / "examples" / "demo"
    package.mkdir(parents=True)
    (package / "plugin.yaml").write_text("kind: ManagedAgent\n")
    platform = tmp_path / "plugin-toolkit"
    (platform / "src" / "edgecitadel_agentd").mkdir(parents=True)
    (platform / "pyproject.toml").write_text("")

    assert assets.agent_packages_root(tmp_path) == (tmp_path / "plugins").resolve()
    assert assets.agent_platform_root(tmp_path) == platform.resolve()
    assert capsys.readouterr().err.count("legacy") == 2


class CodexRunner:
    def __init__(
        self, source: Path, *, installed: bool = False, malformed: bool = False
    ):
        self.source = source
        self.market = installed
        self.installed = installed
        self.malformed = malformed
        self.mutations: list[list[str]] = []

    def __call__(self, command, cwd):
        argv = list(command)
        if argv[-1] == "--version":
            return plugins.CommandEvidence(argv, 0, "codex-cli 0.153.0", "")
        if argv[-4:] == ["plugin", "marketplace", "list", "--json"]:
            if self.malformed:
                return plugins.CommandEvidence(argv, 0, "not-json", "")
            value = {
                "marketplaces": (
                    [{"name": "edgecitadel", "root": str(self.source)}]
                    if self.market
                    else []
                )
            }
            return plugins.CommandEvidence(argv, 0, json.dumps(value), "")
        if argv[-6:] == [
            "plugin",
            "list",
            "--marketplace",
            "edgecitadel",
            "--available",
            "--json",
        ]:
            value = {
                "installed": (
                    [
                        {
                            "pluginId": plugins.PLUGIN_ID,
                            "installed": True,
                            "version": "0.1.0",
                        }
                    ]
                    if self.installed
                    else []
                )
            }
            return plugins.CommandEvidence(argv, 0, json.dumps(value), "")
        self.mutations.append(argv)
        if argv[1:4] == ["plugin", "marketplace", "remove"]:
            self.market = False
            self.installed = False
        if argv[1:4] == ["plugin", "marketplace", "add"]:
            self.market = True
            self.source = Path(argv[-2])
        if argv[1:3] == ["plugin", "add"]:
            self.installed = True
        if argv[1:3] == ["plugin", "remove"]:
            self.installed = False
        return plugins.CommandEvidence(argv, 0, "{}", "")


def test_codex_install_is_status_first_and_second_run_is_noop(tmp_path):
    source = _write_plugin_assets(tmp_path)
    runner = CodexRunner(source)
    driver = plugins.CodexDriver(tmp_path, runner=runner, executable="/opt/bin/codex")

    first = driver.apply("install", "user")
    second = driver.apply("install", "user")

    assert first.state == "installed" and first.changed is True
    assert second.state == "unchanged" and second.changed is False
    assert len(runner.mutations) == 2
    assert all(command[0] == "/opt/bin/codex" for command in runner.mutations)


def test_codex_unknown_json_blocks_mutation(tmp_path):
    source = _write_plugin_assets(tmp_path)
    runner = CodexRunner(source, malformed=True)
    driver = plugins.CodexDriver(tmp_path, runner=runner, executable="/opt/bin/codex")

    result = driver.apply("install", "user")

    assert result.state == "failed"
    assert result.status.state == "unknown"
    assert runner.mutations == []


def test_codex_timeout_reconciles_once_and_stops_before_next_operation(tmp_path):
    source = _write_plugin_assets(tmp_path)
    runner = CodexRunner(source)
    original = runner.__call__

    def timed_runner(command, cwd):
        argv = list(command)
        if argv[1:4] == ["plugin", "marketplace", "add"]:
            runner.mutations.append(argv)
            return plugins.CommandEvidence(argv, 124, "", "timed out", True)
        return original(command, cwd)

    driver = plugins.CodexDriver(
        tmp_path, runner=timed_runner, executable="/opt/bin/codex"
    )

    result = driver.apply("install", "user")

    assert result.state == "failed"
    assert len(result.evidence) == 1
    assert result.evidence[0].timed_out is True
    assert not any(command[1:3] == ["plugin", "add"] for command in runner.mutations)


def test_codex_interrupt_reconciles_and_does_not_continue(tmp_path):
    source = _write_plugin_assets(tmp_path)
    runner = CodexRunner(source)
    original = runner.__call__

    def interrupting_runner(command, cwd):
        argv = list(command)
        if argv[1:4] == ["plugin", "marketplace", "add"]:
            runner.mutations.append(argv)
            raise KeyboardInterrupt
        return original(command, cwd)

    driver = plugins.CodexDriver(
        tmp_path, runner=interrupting_runner, executable="/opt/bin/codex"
    )

    result = driver.apply("install", "user")

    assert result.state == "failed"
    assert result.exit_code == 130
    assert result.status.state == "absent"
    assert len(runner.mutations) == 1


def test_codex_rejects_project_scope_before_planning(tmp_path):
    source = _write_plugin_assets(tmp_path)
    driver = plugins.CodexDriver(
        tmp_path, runner=CodexRunner(source), executable="/opt/bin/codex"
    )

    with pytest.raises(ValueError, match="does not support scope"):
        driver.plan("install", "project")


def test_codex_repair_replaces_a_stale_marketplace_source(tmp_path):
    current = _write_plugin_assets(tmp_path)
    old = tmp_path / "old-plugins"
    runner = CodexRunner(old, installed=True)
    driver = plugins.CodexDriver(tmp_path, runner=runner, executable="/opt/bin/codex")

    assert driver.status().state == "stale"
    result = driver.apply("repair", "user")

    assert result.state == "installed"
    assert result.changed is True
    assert runner.source == current
    assert runner.mutations[0][1:4] == ["plugin", "marketplace", "remove"]


def test_claude_project_plan_names_exact_settings_target(tmp_path):
    _write_plugin_assets(tmp_path)

    def runner(command, cwd):
        argv = list(command)
        if argv[-1] == "--version":
            return plugins.CommandEvidence(argv, 0, "2.1.150 (Claude Code)", "")
        return plugins.CommandEvidence(argv, 0, "[]", "")

    project = tmp_path / "project"
    project.mkdir()
    driver = plugins.ClaudeCodeDriver(
        tmp_path,
        project_root=project,
        runner=runner,
        executable="/opt/bin/claude",
    )

    plan = driver.plan("install", "project")

    assert plan.target_file == str(project / ".claude" / "settings.json")
    assert plan.operations[-1][-2:] == ["--scope", "project"]


def test_project_scope_rejects_symlinked_settings_directory(tmp_path):
    _write_plugin_assets(tmp_path)
    project = tmp_path / "project"
    outside = tmp_path / "outside"
    project.mkdir()
    outside.mkdir()
    (project / ".claude").symlink_to(outside, target_is_directory=True)

    def runner(command, cwd):
        argv = list(command)
        if argv[-1] == "--version":
            return plugins.CommandEvidence(argv, 0, "2.1.150 (Claude Code)", "")
        return plugins.CommandEvidence(argv, 0, "[]", "")

    driver = plugins.ClaudeCodeDriver(
        tmp_path,
        project_root=project,
        runner=runner,
        executable="/opt/bin/claude",
    )

    with pytest.raises(ValueError, match="escapes the project root"):
        driver.plan("install", "project")


def test_claude_status_uses_marketplace_path_not_source_kind(tmp_path):
    source = _write_plugin_assets(tmp_path)

    def runner(command, cwd):
        argv = list(command)
        if argv[-1] == "--version":
            return plugins.CommandEvidence(argv, 0, "2.1.150 (Claude Code)", "")
        if argv[-4:] == ["plugin", "marketplace", "list", "--json"]:
            value = [
                {
                    "name": "edgecitadel",
                    "source": "directory",
                    "path": str(source),
                    "installLocation": str(source),
                }
            ]
        else:
            value = [
                {
                    "id": plugins.PLUGIN_ID,
                    "scope": "user",
                    "version": "0.1.0",
                }
            ]
        return plugins.CommandEvidence(argv, 0, json.dumps(value), "")

    driver = plugins.ClaudeCodeDriver(
        tmp_path, runner=runner, executable="/opt/bin/claude"
    )

    assert driver.status().state == "installed"


def test_claude_install_and_remove_contract_is_idempotent(tmp_path):
    source = _write_plugin_assets(tmp_path)
    state = {"source": None, "installed": False}
    mutations = []

    def runner(command, cwd):
        argv = list(command)
        if argv[-1] == "--version":
            return plugins.CommandEvidence(argv, 0, "2.1.150 (Claude Code)", "")
        if argv[-4:] == ["plugin", "marketplace", "list", "--json"]:
            value = (
                [
                    {
                        "name": "edgecitadel",
                        "source": "directory",
                        "path": state["source"],
                    }
                ]
                if state["source"]
                else []
            )
            return plugins.CommandEvidence(argv, 0, json.dumps(value), "")
        if argv[-3:] == ["plugin", "list", "--json"]:
            value = (
                [{"id": plugins.PLUGIN_ID, "scope": "user", "version": "0.1.0"}]
                if state["installed"]
                else []
            )
            return plugins.CommandEvidence(argv, 0, json.dumps(value), "")
        mutations.append(argv)
        if argv[1:4] == ["plugin", "marketplace", "add"]:
            state["source"] = argv[4]
        elif argv[1:4] == ["plugin", "marketplace", "remove"]:
            state.update(source=None, installed=False)
        elif argv[1:3] == ["plugin", "install"]:
            state["installed"] = True
        elif argv[1:3] == ["plugin", "uninstall"]:
            state["installed"] = False
        return plugins.CommandEvidence(argv, 0, "ok", "")

    driver = plugins.ClaudeCodeDriver(
        tmp_path, runner=runner, executable="/opt/bin/claude"
    )

    assert driver.apply("install", "user").state == "installed"
    assert driver.apply("install", "user").state == "unchanged"
    assert driver.apply("remove", "user").state == "succeeded"
    assert driver.status().state == "absent"
    assert len(mutations) == 3
    assert state["source"] == str(source)


def test_pi_malformed_settings_is_unknown_and_never_mutates(tmp_path, monkeypatch):
    _write_plugin_assets(tmp_path)
    home = tmp_path / "home"
    settings = home / ".pi" / "agent" / "settings.json"
    settings.parent.mkdir(parents=True)
    settings.write_text("[")
    monkeypatch.setattr(plugins.Path, "home", lambda: home)

    def runner(command, cwd):
        argv = list(command)
        return plugins.CommandEvidence(argv, 0, "pi 0.84.4", "")

    driver = plugins.PiDriver(tmp_path, runner=runner, executable="/opt/bin/pi")

    assert driver.status().state == "unknown"
    assert driver.apply("install", "user").evidence == []


def test_pi_reads_string_package_entries_and_removes_the_local_source(
    tmp_path, monkeypatch
):
    source = _write_plugin_assets(tmp_path) / "pi-edgecitadel"
    home = tmp_path / "home"
    settings = home / ".pi" / "agent" / "settings.json"
    settings.parent.mkdir(parents=True)
    settings.write_text(json.dumps({"packages": [str(source)]}))
    monkeypatch.setattr(plugins.Path, "home", lambda: home)

    def runner(command, cwd):
        argv = list(command)
        output = "pi 0.84.4" if argv[-1] == "--version" else str(source)
        return plugins.CommandEvidence(argv, 0, output, "")

    driver = plugins.PiDriver(tmp_path, runner=runner, executable="/opt/bin/pi")

    assert driver.status().state == "installed"
    assert driver.plan("remove", "user").operations == [
        ["/opt/bin/pi", "remove", str(source)]
    ]


def test_pi_resolves_project_package_paths_relative_to_settings(tmp_path):
    source = _write_plugin_assets(tmp_path) / "pi-edgecitadel"
    project = tmp_path / "project"
    settings = project / ".pi" / "settings.json"
    settings.parent.mkdir(parents=True)
    relative = Path("../..") / source.relative_to(tmp_path)
    settings.write_text(json.dumps({"packages": [str(relative)]}))

    def runner(command, cwd):
        argv = list(command)
        output = "pi 0.84.4" if argv[-1] == "--version" else str(source)
        return plugins.CommandEvidence(argv, 0, output, "")

    driver = plugins.PiDriver(
        tmp_path,
        project_root=project,
        runner=runner,
        executable="/opt/bin/pi",
    )

    assert driver.status("project").state == "installed"


def test_pi_repair_removes_stale_local_source_before_install(tmp_path, monkeypatch):
    source = _write_plugin_assets(tmp_path) / "pi-edgecitadel"
    old_source = tmp_path / "old" / "pi-edgecitadel"
    home = tmp_path / "home"
    settings = home / ".pi" / "agent" / "settings.json"
    settings.parent.mkdir(parents=True)
    settings.write_text(json.dumps({"packages": [str(old_source)]}))
    monkeypatch.setattr(plugins.Path, "home", lambda: home)

    def runner(command, cwd):
        argv = list(command)
        output = "pi 0.84.4" if argv[-1] == "--version" else str(old_source)
        return plugins.CommandEvidence(argv, 0, output, "")

    driver = plugins.PiDriver(tmp_path, runner=runner, executable="/opt/bin/pi")

    assert driver.status().state == "stale"
    assert driver.plan("repair", "user").operations == [
        ["/opt/bin/pi", "remove", str(old_source)],
        ["/opt/bin/pi", "install", str(source)],
    ]


def test_pi_install_and_remove_contract_is_idempotent(tmp_path, monkeypatch):
    source = _write_plugin_assets(tmp_path) / "pi-edgecitadel"
    home = tmp_path / "home"
    settings = home / ".pi" / "agent" / "settings.json"
    settings.parent.mkdir(parents=True)
    monkeypatch.setattr(plugins.Path, "home", lambda: home)
    mutations = []

    def runner(command, cwd):
        argv = list(command)
        if argv[-1] == "--version":
            return plugins.CommandEvidence(argv, 0, "pi 0.84.4", "")
        if argv[1:] == ["list"]:
            installed = json.loads(settings.read_text()).get("packages", [])
            return plugins.CommandEvidence(argv, 0, "\n".join(installed), "")
        mutations.append(argv)
        if argv[1] == "install":
            settings.write_text(json.dumps({"packages": [argv[-1]]}))
        elif argv[1] == "remove":
            settings.write_text(json.dumps({"packages": []}))
        return plugins.CommandEvidence(argv, 0, "ok", "")

    driver = plugins.PiDriver(tmp_path, runner=runner, executable="/opt/bin/pi")

    assert driver.apply("install", "user").state == "installed"
    assert driver.apply("install", "user").state == "unchanged"
    assert driver.apply("remove", "user").state == "succeeded"
    assert driver.status().state == "absent"
    assert mutations == [
        ["/opt/bin/pi", "install", str(source)],
        ["/opt/bin/pi", "remove", str(source)],
    ]


def test_native_output_is_bounded_and_secret_shaped_values_are_redacted():
    raw = "token=super-secret " + "x" * (plugins.OUTPUT_LIMIT + 100)

    value = plugins._bounded_redacted(raw)

    assert "super-secret" not in value
    assert len(value.encode()) <= plugins.OUTPUT_LIMIT + len("[output truncated]\n")
