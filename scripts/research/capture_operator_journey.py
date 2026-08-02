"""Capture a portable, sealed operator-journey evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, Mapping, cast

from scripts.research.evidence import (
    SourceProvenance,
    capture_source_provenance,
    finalize_bundle,
    verify_source_provenance,
    write_json,
)
from scripts.research.check_artifact import check_bundle

PROJECTS = ("desktop", "mobile")
SCHEMA_PATH = Path("schemas/research-manifest.v1.json")


def tool_version(argv: list[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        argv, cwd=cwd, check=True, capture_output=True, text=True
    )
    lines = (completed.stdout or completed.stderr).splitlines()
    if not lines:
        raise RuntimeError(f"empty version output from {argv[0]}")
    return lines[0].strip()


def source_provenance(source_root: Path) -> SourceProvenance:
    source = capture_source_provenance(source_root)
    if source.git_dirty:
        raise RuntimeError("operator capture source must be clean")
    if ".dockerignore" not in source.paths:
        raise RuntimeError("operator source must include .dockerignore")
    return source


def iter_report_tests(suites: Iterable[object]) -> Iterable[Mapping[str, object]]:
    for raw_suite in suites:
        if not isinstance(raw_suite, Mapping):
            continue
        nested = raw_suite.get("suites", [])
        if isinstance(nested, list):
            yield from iter_report_tests(nested)
        specs = raw_suite.get("specs", [])
        if not isinstance(specs, list):
            continue
        for raw_spec in specs:
            if not isinstance(raw_spec, Mapping):
                continue
            tests = raw_spec.get("tests", [])
            if isinstance(tests, list):
                yield from (
                    test for test in tests if isinstance(test, Mapping)
                )


def passed_project_results(
    report: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    suites = report.get("suites", [])
    if not isinstance(suites, list):
        raise RuntimeError("Playwright report suites are invalid")
    selected: dict[str, Mapping[str, object]] = {}
    for test_case in iter_report_tests(suites):
        project = test_case.get("projectName")
        if project not in PROJECTS:
            continue
        results = test_case.get("results", [])
        if (
            not isinstance(results, list)
            or len(results) != 1
            or not isinstance(results[0], Mapping)
            or results[0].get("status") != "passed"
            or results[0].get("retry", 0) != 0
            or project in selected
        ):
            raise RuntimeError(f"expected one passed result for {project}")
        selected[project] = results[0]
    if set(selected) != set(PROJECTS):
        raise RuntimeError("desktop and mobile results are required")
    return selected


def _attachment(
    result: Mapping[str, object], project: str, name: str
) -> Mapping[str, object]:
    attachments = result.get("attachments", [])
    if not isinstance(attachments, list):
        raise RuntimeError(f"{project} attachments are invalid")
    matches = [
        item
        for item in attachments
        if isinstance(item, Mapping) and item.get("name") == name and item.get("path")
    ]
    if len(matches) != 1:
        raise RuntimeError(f"{project} requires one {name} attachment")
    return matches[0]


def _attachment_path(source_root: Path, attachment: Mapping[str, object]) -> Path:
    raw_path = attachment.get("path")
    if not isinstance(raw_path, str):
        raise RuntimeError("attachment path is invalid")
    path = Path(raw_path)
    return path if path.is_absolute() else source_root / "e2e" / path


def copy_media(
    source_root: Path,
    bundle: Path,
    results: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    """Copy Playwright's temporary media and describe only portable paths."""
    portable: dict[str, object] = {}
    for project in PROJECTS:
        attachments = {
            name: _attachment(results[project], project, name)
            for name in ("chat", "tasks", "operator-metadata", "video", "trace")
        }
        project_root = bundle / "raw" / "playwright" / project
        for name, destination in (
            ("chat", "chat.png"),
            ("tasks", "tasks.png"),
            ("operator-metadata", "operator-metadata.json"),
            ("video", "video.webm"),
            ("trace", "trace.zip"),
        ):
            source = _attachment_path(source_root, attachments[name])
            if not source.is_file():
                raise RuntimeError(f"{project} {name} attachment is absent")
            target = project_root / destination
            target.parent.mkdir(parents=True, exist_ok=True)
            if source.resolve() != target.resolve():
                shutil.copyfile(source, target)
        portable[project] = {
            "project": project,
            "title": "operator observes one deterministic task lifecycle",
            "status": "passed",
            "retry": results[project].get("retry", 0),
            "duration_ms": results[project].get("duration"),
            "attachments": [
                {
                    "name": name,
                    "path": (Path("raw/playwright") / project / destination).as_posix(),
                    "content_type": content_type,
                }
                for name, destination, content_type in (
                    ("chat", "chat.png", "image/png"),
                    ("tasks", "tasks.png", "image/png"),
                    ("operator-metadata", "operator-metadata.json", "application/json"),
                    ("video", "video.webm", "video/webm"),
                    ("trace", "trace.zip", "application/zip"),
                )
            ],
        }
    return {"schema_version": "playwright-operator-results.v1", "projects": portable}


def require_exact_artifacts(bundle: Path) -> None:
    expected = {
        "png": 4,
        "webm": 2,
        "trace.zip": 2,
        "api-json": 8,
        "metadata-json": 2,
        "playwright-json": 1,
        "runtime-json": 2,
    }
    actual = {
        "png": len(list((bundle / "raw/playwright").glob("*/*.png"))),
        "webm": len(list((bundle / "raw/playwright").glob("*/*.webm"))),
        "trace.zip": len(list((bundle / "raw/playwright").glob("*/trace.zip"))),
        "api-json": len(list((bundle / "raw/api").glob("*/*.json"))),
        "metadata-json": len(
            list((bundle / "raw/playwright").glob("*/operator-metadata.json"))
        ),
        "playwright-json": int((bundle / "playwright-results.json").is_file()),
        "runtime-json": len(list((bundle / "raw/runtime").glob("*.json"))),
    }
    if actual != expected:
        raise RuntimeError(f"unexpected evidence counts: {actual}")


def require_cleanup(runtime: Mapping[str, object]) -> None:
    cleanup = runtime.get("cleanup")
    if not isinstance(cleanup, Mapping) or cleanup.get("valid") is not True:
        raise RuntimeError("launcher cleanup is invalid")
    resources = cleanup.get("resources")
    if not isinstance(resources, Mapping):
        raise RuntimeError("launcher cleanup resources are invalid")
    for name in ("containers", "networks", "volumes", "owned_build_images"):
        if resources.get(name) != []:
            raise RuntimeError(f"launcher left {name}")
    if runtime.get("run_directory") != "<run-owned-path>":
        raise RuntimeError("runtime directory was not normalized")
    if runtime.get("scratch_removed") is not True:
        raise RuntimeError("credential scratch directory was not removed")


def _write_replacement_json(path: Path, value: object) -> None:
    path.unlink()
    write_json(path, value)


def capture_operator_journey(
    output_root: Path,
    source_root: Path,
    *,
    runner: object = subprocess.run,
) -> Path:
    output_root = output_root.resolve()
    source_root = source_root.resolve()
    before = source_provenance(source_root)
    bundle = output_root / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{before.commit[:12]}"
    bundle.mkdir(parents=True, exist_ok=False)
    runtime_dir = bundle / "raw" / "runtime"
    launcher = [
        "node",
        str(source_root / "e2e/run-isolated.js"),
        "--config",
        str(source_root / "e2e/playwright.evidence.config.js"),
        "--evidence-runtime-dir",
        str(runtime_dir),
    ]
    environment = dict(os.environ)
    environment["EVIDENCE_DIR"] = str(bundle)
    try:
        completed = cast(object, runner)(
            launcher,
            cwd=source_root / "e2e",
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        if getattr(completed, "returncode", 1) != 0:
            raise RuntimeError(f"evidence launcher failed: {getattr(completed, 'stderr', '')}")
        report_path = bundle / "playwright-results.json"
        report = json.loads(report_path.read_text())
        if not isinstance(report, Mapping):
            raise RuntimeError("Playwright report is invalid")
        _write_replacement_json(
            report_path, copy_media(source_root, bundle, passed_project_results(report))
        )
        metadata = {
            project: json.loads(
                (bundle / "raw/playwright" / project / "operator-metadata.json").read_text()
            )
            for project in PROJECTS
        }
        if len({metadata[project]["task_id"] for project in PROJECTS}) != 2:
            raise RuntimeError("desktop and mobile task IDs must be distinct")
        if len({metadata[project]["browser_version"] for project in PROJECTS}) != 1:
            raise RuntimeError("desktop and mobile Chromium versions must match")
        runtime = json.loads((runtime_dir / "launcher-summary.json").read_text())
        cleanup = json.loads((runtime_dir / "cleanup.json").read_text())
        if runtime.get("cleanup") != cleanup:
            raise RuntimeError("runtime cleanup copies disagree")
        require_cleanup(runtime)
        require_exact_artifacts(bundle)
        if not verify_source_provenance(source_root, before):
            raise RuntimeError("source changed during evidence capture")
        compose_text = str(runtime["compose_config"]).encode()
        manifest = {
            "schema_version": "research-manifest.v1",
            "evidence_kind": "operator",
            "status": "PASS",
            "run_id": runtime["run_id"],
            "source": before.to_dict(),
            "command": [
                "node", "$SOURCE_ROOT/e2e/run-isolated.js", "--config",
                "$SOURCE_ROOT/e2e/playwright.evidence.config.js",
                "--evidence-runtime-dir", "$EVIDENCE_DIR/raw/runtime",
            ],
            "timing": {"started_at": runtime["started_at"], "ended_at": runtime["completed_at"]},
            "host": {"os": platform.system(), "architecture": platform.machine()},
            "dependencies": {
                "python": f"Python {platform.python_version()}",
                "node": tool_version(["node", "--version"]),
                "npm": tool_version(["npm", "--version"]),
                "git": tool_version(["git", "--version"]),
                "docker_client": tool_version(["docker", "version", "--format", "{{.Client.Version}}"]),
                "docker_server": tool_version(["docker", "version", "--format", "{{.Server.Version}}"]),
                "docker_compose": tool_version(["docker", "compose", "version", "--short"]),
                "playwright": tool_version(["npx", "--no-install", "playwright", "--version"], cwd=source_root / "e2e"),
                "chromium": metadata["desktop"]["browser_version"],
                "ffmpeg": tool_version(["ffmpeg", "-version"]),
                "ffprobe": tool_version(["ffprobe", "-version"]),
            },
            "images": runtime["images"],
            "compose_config_sha256": hashlib.sha256(compose_text).hexdigest(),
            "schemas": {"manifest": SCHEMA_PATH.as_posix()},
            "cleanup": {"completed": True, **cleanup},
            "task": {"projects": list(PROJECTS)},
            "media": {"report": "playwright-results.json"},
            "projects": {project: metadata[project] for project in PROJECTS},
            "artifacts": {},
        }
        status = finalize_bundle(bundle, manifest, source_root / SCHEMA_PATH)
        if status != "PASS":
            raise RuntimeError(f"finalization returned {status}")
        check_bundle(bundle, expected_kind="operator", source_root=source_root).require_valid()
    except BaseException:
        shutil.rmtree(bundle, ignore_errors=True)
        raise
    print(bundle)
    print("PASS")
    return bundle


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    arguments = parser.parse_args()
    if not arguments.output_root.is_absolute() or not arguments.source_root.is_absolute():
        parser.error("--output-root and --source-root must be absolute")
    capture_operator_journey(arguments.output_root, arguments.source_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
