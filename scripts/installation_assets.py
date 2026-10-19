"""Resolve EdgeCitadel's packaged assets across the layout migration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable


class AssetResolutionError(RuntimeError):
    """Raised when a required distribution asset cannot be located safely."""


def _resolve(
    install_root: Path,
    *,
    label: str,
    current: str,
    legacy: str,
    valid: Callable[[Path], bool],
) -> Path:
    root = install_root.resolve()
    for relative, is_legacy in ((current, False), (legacy, True)):
        candidate = (root / relative).resolve()
        if not candidate.is_relative_to(root) or not valid(candidate):
            continue
        if is_legacy:
            print(
                f"warning: using legacy {label} layout at {candidate}; "
                "reinstall EdgeCitadel to migrate packaged assets",
                file=sys.stderr,
            )
        return candidate
    raise AssetResolutionError(
        f"{label} assets are missing from {root}; reinstall EdgeCitadel"
    )


def agent_packages_root(install_root: Path) -> Path:
    """Return the public Agent Package root, with one-release fallback."""
    return _resolve(
        install_root,
        label="Agent Package",
        current="agent-packages",
        legacy="plugins",
        valid=lambda path: path.is_dir() and any(path.glob("**/plugin.yaml")),
    )


def plugins_root(install_root: Path) -> Path:
    """Return the native host Plugin marketplace root."""
    return _resolve(
        install_root,
        label="Plugin",
        current="plugins",
        legacy="native-plugins",
        valid=lambda path: path.is_dir()
        and (path / ".agents" / "plugins" / "marketplace.json").is_file()
        and (path / ".claude-plugin" / "marketplace.json").is_file()
        and (path / "pi-edgecitadel" / "package.json").is_file(),
    )


def agent_runtime_root(install_root: Path) -> Path:
    """Return agentd/runtime sources while preserving Python import names."""
    return _resolve(
        install_root,
        label="Agent Runtime",
        current="agent-runtime",
        legacy="plugin-toolkit",
        valid=lambda path: path.is_dir()
        and (path / "pyproject.toml").is_file()
        and (path / "src" / "edgecitadel_agentd").is_dir(),
    )


def plugin_source(install_root: Path, host: str) -> Path:
    """Resolve and validate the bundled source a native host may install."""
    root = plugins_root(install_root)
    if host == "codex":
        source = root
        marker = source / ".agents" / "plugins" / "marketplace.json"
    elif host == "claude-code":
        source = root
        marker = source / ".claude-plugin" / "marketplace.json"
    elif host == "pi":
        source = root / "pi-edgecitadel"
        marker = source / "package.json"
    else:
        raise AssetResolutionError(f"unsupported Plugin host: {host}")
    resolved = source.resolve()
    if not resolved.is_relative_to(root.resolve()) or not marker.is_file():
        raise AssetResolutionError(f"bundled {host} Plugin is incomplete: {source}")
    return resolved
