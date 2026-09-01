"""Console entry point for the pip distribution."""

from __future__ import annotations

import importlib
import os
import sys
import sysconfig
from collections.abc import Callable
from pathlib import Path
from typing import Sequence, cast


def _install_root() -> Path:
    """Return shared wheel assets, falling back to the editable checkout."""
    shared = Path(sysconfig.get_path("data")) / "share" / "edgecitadel"
    if (shared / "scripts" / "edgecitadel_cli.py").is_file():
        return shared
    checkout = Path(__file__).resolve().parents[1]
    if (checkout / "scripts" / "edgecitadel_cli.py").is_file():
        return checkout
    raise RuntimeError(
        "EdgeCitadel installation assets are missing; reinstall the package"
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Configure the pip layout before loading the shared CLI implementation."""
    install_root = _install_root()
    os.environ.setdefault("EDGECITADEL_DISTRIBUTION", "pip")
    os.environ.setdefault("EDGECITADEL_INSTALL_ROOT", str(install_root))
    scripts = str(install_root / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)

    module = importlib.import_module("edgecitadel_cli")
    cli_main = cast(Callable[[Sequence[str] | None], int], module.main)

    return cli_main(argv)
