"""Native package-manager drivers for EdgeCitadel host Plugins."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Sequence

try:
    from .installation_assets import AssetResolutionError, plugin_source
except ImportError:  # Installed scripts are imported as top-level modules.
    from installation_assets import AssetResolutionError, plugin_source


HOSTS = ("codex", "claude-code", "pi")
OUTPUT_LIMIT = 64 * 1024
COMMAND_TIMEOUT = 120
PLUGIN_ID = "edgecitadel@edgecitadel"
PI_PACKAGE_ID = "@edgecitadel/pi-plugin"
_SECRET = re.compile(r"(?i)(token|password|secret|api[_-]?key)(\s*[=:]\s*)([^\s,;]+)")


@dataclass(frozen=True)
class CommandEvidence:
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False


@dataclass(frozen=True)
class PluginStatus:
    host: str
    state: str
    available: bool
    version: str | None = None
    scope: str | None = None
    source: str | None = None
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PluginPlan:
    host: str
    action: str
    scope: str
    source: str
    executable: str
    operations: list[list[str]]
    target_file: str | None = None
    capabilities: tuple[str, ...] = ("MCP", "skills", "hooks")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PluginResult:
    host: str
    state: str
    changed: bool
    status: PluginStatus
    evidence: list[CommandEvidence] = field(default_factory=list)
    recovery_command: str | None = None
    exit_code: int = 0

    @property
    def ok(self) -> bool:
        return self.state in {"installed", "unchanged", "succeeded", "planned"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "state": self.state,
            "changed": self.changed,
            "status": self.status.to_dict(),
            "evidence": [asdict(item) for item in self.evidence],
            "recovery_command": self.recovery_command,
        }


Runner = Callable[[Sequence[str], Path | None], CommandEvidence]


def _bounded_redacted(value: str) -> str:
    redacted = _SECRET.sub(
        lambda match: match.group(1) + match.group(2) + "[REDACTED]", value
    )
    encoded = redacted.encode(errors="replace")
    if len(encoded) <= OUTPUT_LIMIT:
        return redacted
    return "[output truncated]\n" + encoded[-OUTPUT_LIMIT:].decode(errors="replace")


def run_native(command: Sequence[str], cwd: Path | None = None) -> CommandEvidence:
    """Execute one bounded native operation without a shell or retries."""
    argv = [str(item) for item in command]
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT,
            shell=False,
        )
        return CommandEvidence(
            argv=argv,
            returncode=result.returncode,
            stdout=_bounded_redacted(result.stdout),
            stderr=_bounded_redacted(result.stderr),
        )
    except subprocess.TimeoutExpired as error:
        stdout = (
            error.stdout.decode(errors="replace")
            if isinstance(error.stdout, bytes)
            else error.stdout or ""
        )
        stderr = (
            error.stderr.decode(errors="replace")
            if isinstance(error.stderr, bytes)
            else error.stderr or ""
        )
        return CommandEvidence(
            argv=argv,
            returncode=124,
            stdout=_bounded_redacted(stdout),
            stderr=_bounded_redacted(stderr),
            timed_out=True,
        )
    except OSError as error:
        return CommandEvidence(
            argv=argv,
            returncode=126,
            stdout="",
            stderr=_bounded_redacted(str(error)),
        )


def _json(evidence: CommandEvidence) -> Any | None:
    if evidence.returncode != 0 or not evidence.stdout.strip():
        return None
    try:
        return json.loads(evidence.stdout)
    except json.JSONDecodeError:
        return None


def _version_tuple(value: str) -> tuple[int, ...] | None:
    match = re.search(r"(?<!\d)(\d+)\.(\d+)\.(\d+)(?!\d)", value)
    return tuple(int(part) for part in match.groups()) if match else None


def _same_path(left: object, right: Path) -> bool:
    if not isinstance(left, str):
        return False
    try:
        return Path(left).expanduser().resolve() == right.resolve()
    except OSError:
        return False


def _failed_codex_marketplace_source(evidence: CommandEvidence) -> str | None:
    combined = f"{evidence.stdout}\n{evidence.stderr}"
    match = re.search(
        r"(?m)^-\s+`edgecitadel`\s+at\s+(.+?):\s+marketplace root\b",
        combined,
    )
    return match.group(1).strip() if match else None


def _same_settings_path(value: str, source: Path, settings: Path) -> bool:
    try:
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = settings.parent / candidate
        return candidate.resolve() == source.resolve()
    except OSError:
        return False


def _resolved_settings_path(value: str, settings: Path) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = settings.parent / candidate
    return candidate.resolve()


def _strings(value: Any):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child)


class PluginDriver:
    """Base contract shared by native host package managers."""

    host = ""
    executable_name = ""
    supported_scopes: tuple[str, ...] = ()
    minimum_version = (0, 0, 0)

    def __init__(
        self,
        install_root: Path,
        *,
        project_root: Path | None = None,
        runner: Runner = run_native,
        executable: str | None = None,
    ) -> None:
        self.install_root = install_root.resolve()
        project = project_root or Path.cwd()
        self.project_root_was_symlink = project.is_symlink()
        self.project_root = project.resolve()
        self.runner = runner
        discovered = executable or shutil.which(self.executable_name)
        self.executable = str(Path(discovered).resolve()) if discovered else None

    @property
    def source(self) -> Path:
        return plugin_source(self.install_root, self.host)

    def _run(self, *arguments: str) -> CommandEvidence:
        if self.executable is None:
            return CommandEvidence(
                [self.executable_name, *arguments], 127, "", "executable not found"
            )
        return self.runner([self.executable, *arguments], self.project_root)

    def detect(self) -> PluginStatus:
        if self.executable is None:
            return PluginStatus(
                self.host,
                "absent",
                False,
                detail=f"{self.executable_name} executable not found",
            )
        evidence = self._run("--version")
        raw = f"{evidence.stdout}\n{evidence.stderr}".strip()
        parsed = _version_tuple(raw)
        if evidence.returncode != 0 or parsed is None:
            return PluginStatus(
                self.host,
                "unknown",
                True,
                detail="host version could not be determined",
            )
        version = ".".join(str(part) for part in parsed)
        if parsed < self.minimum_version:
            floor = ".".join(str(part) for part in self.minimum_version)
            return PluginStatus(
                self.host,
                "unsupported",
                True,
                version=version,
                detail=f"requires {self.executable_name} >= {floor}",
            )
        return PluginStatus(
            self.host,
            "available",
            True,
            version=version,
            detail="supported host executable",
        )

    def status(self, scope: str = "user") -> PluginStatus:
        raise NotImplementedError

    def _operations(self, action: str, scope: str) -> list[list[str]]:
        raise NotImplementedError

    def plan(self, action: str, scope: str) -> PluginPlan:
        if scope not in self.supported_scopes:
            supported = ", ".join(self.supported_scopes)
            raise ValueError(
                f"{self.host} does not support scope {scope!r}; use {supported}"
            )
        detected = self.detect()
        if detected.state == "absent":
            raise ValueError(
                f"{self.host} is not installed; install the host application first"
            )
        if detected.state != "available":
            raise ValueError(detected.detail)
        if scope == "project":
            target = self._target_file(scope)
            if self.project_root_was_symlink or target is None:
                raise ValueError("project scope requires a non-symlinked project root")
            resolved_parent = target.parent.resolve()
            if not resolved_parent.is_relative_to(self.project_root):
                raise ValueError(
                    f"project settings target escapes the project root: {target}"
                )
            if target.is_symlink() and not target.resolve().is_relative_to(
                self.project_root
            ):
                raise ValueError(
                    f"project settings target escapes the project root: {target}"
                )
        source = self.source
        target = self._target_file(scope)
        return PluginPlan(
            host=self.host,
            action=action,
            scope=scope,
            source=str(source),
            executable=str(self.executable),
            operations=self._operations(action, scope),
            target_file=str(target) if target else None,
        )

    def _target_file(self, scope: str) -> Path | None:
        return None

    def apply(self, action: str, scope: str) -> PluginResult:
        before = self.status(scope)
        if before.state in {"unknown", "unsupported"}:
            return PluginResult(
                self.host,
                "failed",
                False,
                before,
                recovery_command=f"edgecitadel plugin status {self.host} --scope {scope}",
            )
        if action == "install" and before.state == "installed":
            return PluginResult(self.host, "unchanged", False, before)
        if action == "install" and before.state == "stale":
            return PluginResult(
                self.host,
                "failed",
                False,
                before,
                recovery_command=f"edgecitadel plugin repair {self.host} --scope {scope}",
            )
        if action == "remove" and before.state == "absent":
            return PluginResult(self.host, "unchanged", False, before)

        planned_action = (
            "install" if action == "repair" and before.state == "absent" else action
        )
        plan = self.plan(planned_action, scope)
        evidence: list[CommandEvidence] = []
        try:
            for operation in plan.operations:
                result = self.runner(operation, self.project_root)
                evidence.append(result)
                if result.returncode != 0:
                    break
        except KeyboardInterrupt:
            after = self.status(scope)
            return PluginResult(
                self.host,
                "failed",
                after.state != before.state,
                after,
                evidence,
                f"edgecitadel plugin repair {self.host} --scope {scope}",
                130,
            )

        after = self.status(scope)
        success_state = "absent" if action == "remove" else "installed"
        if after.state == success_state:
            visible = "succeeded" if action == "remove" else "installed"
            return PluginResult(
                self.host,
                visible,
                after.state != before.state or action == "repair",
                after,
                evidence,
            )
        return PluginResult(
            self.host,
            "failed",
            after.state != before.state,
            after,
            evidence,
            f"edgecitadel plugin {'install' if action == 'remove' else 'repair'} {self.host} --scope {scope}",
        )


class CodexDriver(PluginDriver):
    host = "codex"
    executable_name = "codex"
    supported_scopes = ("user",)
    minimum_version = (0, 151, 0)

    def status(self, scope: str = "user") -> PluginStatus:
        if scope not in self.supported_scopes:
            return PluginStatus(
                self.host,
                "unsupported",
                self.executable is not None,
                scope=scope,
                detail=f"{self.host} does not support scope {scope!r}",
            )
        detected = self.detect()
        if detected.state != "available":
            return detected
        marketplace_evidence = self._run("plugin", "marketplace", "list", "--json")
        marketplaces = _json(marketplace_evidence)
        if not isinstance(marketplaces, dict):
            failed_source = _failed_codex_marketplace_source(marketplace_evidence)
            if failed_source is not None and not _same_path(failed_source, self.source):
                return PluginStatus(
                    self.host,
                    "stale",
                    True,
                    detected.version,
                    scope,
                    failed_source,
                    "configured Codex marketplace path is stale",
                )
            return PluginStatus(
                self.host,
                "unknown",
                True,
                detected.version,
                scope,
                detail="Codex returned invalid Plugin JSON",
            )
        market = next(
            (
                item
                for item in marketplaces.get("marketplaces", [])
                if isinstance(item, dict) and item.get("name") == "edgecitadel"
            ),
            None,
        )
        if market is None:
            return PluginStatus(
                self.host,
                "absent",
                True,
                detected.version,
                scope,
                detail="Plugin is not installed",
            )
        plugins = _json(
            self._run(
                "plugin",
                "list",
                "--marketplace",
                "edgecitadel",
                "--available",
                "--json",
            )
        )
        if not isinstance(plugins, dict):
            return PluginStatus(
                self.host,
                "unknown",
                True,
                detected.version,
                scope,
                detail="Codex returned invalid Plugin JSON",
            )
        installed = next(
            (
                item
                for item in plugins.get("installed", [])
                if isinstance(item, dict)
                and item.get("pluginId") == PLUGIN_ID
                and item.get("installed") is True
            ),
            None,
        )
        if installed is None:
            return PluginStatus(
                self.host,
                "absent",
                True,
                detected.version,
                scope,
                detail="Plugin is not installed",
            )
        source = market.get("root")
        if source is None:
            source = (
                market.get("marketplaceSource", {}).get("source")
                if isinstance(market.get("marketplaceSource"), dict)
                else None
            )
        state = "installed" if _same_path(source, self.source) else "stale"
        return PluginStatus(
            self.host,
            state,
            True,
            str(installed.get("version") or detected.version),
            scope,
            str(source) if source else None,
            "native Codex Plugin state",
        )

    def _operations(self, action: str, scope: str) -> list[list[str]]:
        assert self.executable is not None
        if action == "remove":
            return [[self.executable, "plugin", "remove", PLUGIN_ID, "--json"]]
        operations = [
            [
                self.executable,
                "plugin",
                "marketplace",
                "add",
                str(self.source),
                "--json",
            ],
            [self.executable, "plugin", "add", PLUGIN_ID, "--json"],
        ]
        if action == "repair":
            operations.insert(
                0,
                [
                    self.executable,
                    "plugin",
                    "marketplace",
                    "remove",
                    "edgecitadel",
                    "--json",
                ],
            )
        return operations


class ClaudeCodeDriver(PluginDriver):
    host = "claude-code"
    executable_name = "claude"
    supported_scopes = ("user", "project")
    minimum_version = (2, 1, 150)

    def _target_file(self, scope: str) -> Path | None:
        return (
            self.project_root / ".claude" / "settings.json"
            if scope == "project"
            else None
        )

    def status(self, scope: str = "user") -> PluginStatus:
        if scope not in self.supported_scopes:
            return PluginStatus(
                self.host,
                "unsupported",
                self.executable is not None,
                scope=scope,
                detail=f"{self.host} does not support scope {scope!r}",
            )
        detected = self.detect()
        if detected.state != "available":
            return detected
        marketplaces = _json(self._run("plugin", "marketplace", "list", "--json"))
        plugins = _json(self._run("plugin", "list", "--json"))
        if not isinstance(marketplaces, list) or not isinstance(plugins, list):
            return PluginStatus(
                self.host,
                "unknown",
                True,
                detected.version,
                scope,
                detail="Claude Code returned invalid Plugin JSON",
            )
        installed = next(
            (
                item
                for item in plugins
                if isinstance(item, dict)
                and item.get("id") == PLUGIN_ID
                and item.get("scope", "user") == scope
            ),
            None,
        )
        market = next(
            (
                item
                for item in marketplaces
                if isinstance(item, dict) and item.get("name") == "edgecitadel"
            ),
            None,
        )
        if installed is None:
            return PluginStatus(
                self.host,
                "absent",
                True,
                detected.version,
                scope,
                detail="Plugin is not installed",
            )
        source = None
        if isinstance(market, dict):
            source = next(
                (
                    market.get(key)
                    for key in ("path", "installLocation", "source")
                    if isinstance(market.get(key), str)
                ),
                None,
            )
        source_matches = source is None or _same_path(source, self.source)
        state = "installed" if source_matches else "stale"
        return PluginStatus(
            self.host,
            state,
            True,
            str(installed.get("version") or detected.version),
            scope,
            source,
            "native Claude Code Plugin state",
        )

    def _operations(self, action: str, scope: str) -> list[list[str]]:
        assert self.executable is not None
        if action == "remove":
            return [
                [self.executable, "plugin", "uninstall", PLUGIN_ID, "--scope", scope]
            ]
        operations = [
            [
                self.executable,
                "plugin",
                "marketplace",
                "add",
                str(self.source),
                "--scope",
                scope,
            ],
            [self.executable, "plugin", "install", PLUGIN_ID, "--scope", scope],
        ]
        if action == "repair":
            operations.insert(
                0,
                [
                    self.executable,
                    "plugin",
                    "marketplace",
                    "remove",
                    "edgecitadel",
                ],
            )
        return operations


class PiDriver(PluginDriver):
    host = "pi"
    executable_name = "pi"
    supported_scopes = ("user", "project")
    minimum_version = (0, 84, 4)

    def _target_file(self, scope: str) -> Path:
        return (
            self.project_root / ".pi" / "settings.json"
            if scope == "project"
            else Path.home() / ".pi" / "agent" / "settings.json"
        )

    def status(self, scope: str = "user") -> PluginStatus:
        if scope not in self.supported_scopes:
            return PluginStatus(
                self.host,
                "unsupported",
                self.executable is not None,
                scope=scope,
                detail=f"{self.host} does not support scope {scope!r}",
            )
        detected = self.detect()
        if detected.state != "available":
            return detected
        settings = self._target_file(scope)
        if not settings.exists():
            return PluginStatus(
                self.host,
                "absent",
                True,
                detected.version,
                scope,
                detail="Plugin is not installed",
            )
        try:
            document = json.loads(settings.read_text())
        except (OSError, json.JSONDecodeError):
            return PluginStatus(
                self.host,
                "unknown",
                True,
                detected.version,
                scope,
                detail=f"cannot parse {settings}",
            )
        strings = list(_strings(document))
        exact = next(
            (
                value
                for value in strings
                if _same_settings_path(value, self.source, settings)
            ),
            None,
        )
        named = next(
            (
                value
                for value in strings
                if PI_PACKAGE_ID in value or "pi-edgecitadel" in value
            ),
            None,
        )
        if exact is None and named is None:
            return PluginStatus(
                self.host,
                "absent",
                True,
                detected.version,
                scope,
                detail="Plugin is not installed",
            )
        listed = self._run("list")
        if listed.returncode != 0:
            return PluginStatus(
                self.host,
                "unknown",
                True,
                detected.version,
                scope,
                exact or named,
                "Pi settings exist but native list failed",
            )
        combined = listed.stdout + listed.stderr
        if (
            PI_PACKAGE_ID not in combined
            and "pi-edgecitadel" not in combined
            and str(self.source) not in combined
        ):
            return PluginStatus(
                self.host,
                "unknown",
                True,
                detected.version,
                scope,
                exact or named,
                "Pi settings and native list disagree",
            )
        state = "installed" if exact is not None else "stale"
        return PluginStatus(
            self.host,
            state,
            True,
            detected.version,
            scope,
            exact or named,
            "native Pi package state",
        )

    def _operations(self, action: str, scope: str) -> list[list[str]]:
        assert self.executable is not None
        local = ["-l"] if scope == "project" else []
        if action == "remove":
            return [[self.executable, "remove", *local, str(self.source)]]
        operations: list[list[str]] = []
        if action == "repair":
            observed = self.status(scope)
            if observed.state == "stale" and observed.source:
                old_source = _resolved_settings_path(
                    observed.source, self._target_file(scope)
                )
                operations.append([self.executable, "remove", *local, str(old_source)])
        operations.append([self.executable, "install", *local, str(self.source)])
        return operations


def driver_for(
    host: str,
    install_root: Path,
    *,
    project_root: Path | None = None,
    runner: Runner = run_native,
    executable: str | None = None,
) -> PluginDriver:
    drivers = {"codex": CodexDriver, "claude-code": ClaudeCodeDriver, "pi": PiDriver}
    try:
        driver_type = drivers[host]
    except KeyError as error:
        raise AssetResolutionError(f"unsupported Plugin host: {host}") from error
    return driver_type(
        install_root, project_root=project_root, runner=runner, executable=executable
    )
