#!/usr/bin/env python3
"""Atomically reconcile required deployment secrets without exposing values."""

from __future__ import annotations

import argparse
import grp
import os
import secrets
import tempfile
from pathlib import Path


REQUIRED = {
    "NATS_TOKEN": {"", "change-me", "changeme"},
    "NATS_LEAF_USERNAME": {"", "change-me-leaf-user", "changeme"},
    "NATS_LEAF_PASSWORD": {"", "change-me-leaf-password", "changeme"},
    "EDGECITADEL_ADMIN_TOKEN": {"", "change-me-admin", "changeme"},
}


class ReconcileError(RuntimeError):
    """Deployment environment cannot be safely reconciled."""


def _assignment(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        return None
    key, value = line.split("=", 1)
    return key.strip(), value.strip().strip('"').strip("'")


def missing_keys(lines: list[str]) -> list[str]:
    """Return required keys whose effective value is absent or a placeholder."""
    values: dict[str, str] = {}
    for line in lines:
        assignment = _assignment(line)
        if assignment and assignment[0] in REQUIRED:
            values[assignment[0]] = assignment[1]
    return [
        key
        for key, placeholders in REQUIRED.items()
        if values.get(key, "") in placeholders
    ]


def reconcile(path: Path, *, group: str) -> list[str]:
    """Generate missing values and atomically replace the deployment env file."""
    if path.is_symlink():
        raise ReconcileError(f"refusing symbolic-link environment file: {path}")
    if not path.is_file():
        raise ReconcileError(f"deployment environment file is missing: {path}")
    try:
        target_group = grp.getgrnam(group).gr_gid
    except KeyError as error:
        raise ReconcileError(f"deployment group does not exist: {group}") from error

    original = path.read_text().splitlines()
    generated = missing_keys(original)
    if not generated:
        os.chown(path, path.stat().st_uid, target_group)
        path.chmod(0o640)
        return []

    replacements = {key: secrets.token_urlsafe(32) for key in generated}
    rendered: list[str] = []
    replaced: set[str] = set()
    for line in original:
        assignment = _assignment(line)
        if assignment and assignment[0] in replacements:
            key = assignment[0]
            if key not in replaced:
                rendered.append(f"{key}={replacements[key]}")
                replaced.add(key)
            continue
        rendered.append(line)
    for key in generated:
        if key not in replaced:
            rendered.append(f"{key}={replacements[key]}")

    stat = path.stat()
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write("\n".join(rendered) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chown(temporary, stat.st_uid, target_group)
        temporary.chmod(0o640)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
    return generated


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path("/etc/edgecitadel/env"))
    parser.add_argument("--group", default="edgecitadel")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        if args.env_file.is_symlink() or not args.env_file.is_file():
            raise ReconcileError(
                f"deployment environment file is missing or unsafe: {args.env_file}"
            )
        missing = missing_keys(args.env_file.read_text().splitlines())
        if args.check:
            if missing:
                print("Missing deployment secrets: " + ", ".join(missing))
                return 1
            print("Deployment secrets are reconciled.")
            return 0
        generated = reconcile(args.env_file, group=args.group)
    except (OSError, UnicodeError, ReconcileError) as error:
        print(f"Cannot reconcile deployment secrets: {error}")
        return 1
    if generated:
        print("Generated deployment secrets: " + ", ".join(generated))
    else:
        print("Deployment secrets already reconciled.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
