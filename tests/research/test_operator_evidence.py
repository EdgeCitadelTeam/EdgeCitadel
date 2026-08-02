"""Portable evidence contracts for the deterministic operator journey."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.research.capture_operator_journey import (
    PROJECTS,
    copy_media,
    passed_project_results,
)


def _report(result_root: Path, bundle: Path | None = None) -> dict[str, object]:
    suites: list[dict[str, object]] = []
    for project in PROJECTS:
        attachments = []
        for name, filename, content_type in (
            ("chat", "chat.png", "image/png"),
            ("tasks", "tasks.png", "image/png"),
            ("operator-metadata", "operator-metadata.json", "application/json"),
            ("video", "video.webm", "video/webm"),
            ("trace", "trace.zip", "application/zip"),
        ):
            artifact = (
                bundle / "raw" / "playwright" / project / filename
                if bundle is not None and name in {"chat", "tasks", "operator-metadata"}
                else result_root / project / filename
            )
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.write_bytes(f"{project}:{name}".encode())
            attachments.append(
                {"name": name, "path": str(artifact), "contentType": content_type}
            )
        suites.append(
            {
                "specs": [
                    {
                        "tests": [
                            {
                                "projectName": project,
                                "results": [
                                    {
                                        "status": "passed",
                                        "retry": 0,
                                        "duration": 12,
                                        "attachments": attachments,
                                    }
                                ],
                            }
                        ]
                    }
                ]
            }
        )
    return {"suites": suites}


def test_copy_media_builds_a_portable_two_project_report(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    report = _report(tmp_path / "playwright-results", bundle)

    portable = copy_media(source_root, bundle, passed_project_results(report))

    assert portable["schema_version"] == "playwright-operator-results.v1"
    for project in PROJECTS:
        attachments = portable["projects"][project]["attachments"]
        assert [item["path"] for item in attachments] == [
            f"raw/playwright/{project}/chat.png",
            f"raw/playwright/{project}/tasks.png",
            f"raw/playwright/{project}/operator-metadata.json",
            f"raw/playwright/{project}/video.webm",
            f"raw/playwright/{project}/trace.zip",
        ]
        assert (bundle / f"raw/playwright/{project}/video.webm").read_bytes() == (
            f"{project}:video".encode()
        )
        assert (bundle / f"raw/playwright/{project}/trace.zip").read_bytes() == (
            f"{project}:trace".encode()
        )
    json.dumps(portable)


def test_passed_project_results_rejects_retries_and_missing_projects(tmp_path: Path) -> None:
    report = _report(tmp_path / "playwright-results")
    desktop = report["suites"][0]["specs"][0]["tests"][0]["results"][0]
    desktop["retry"] = 1

    try:
        passed_project_results(report)
    except RuntimeError as error:
        assert "desktop" in str(error)
    else:
        raise AssertionError("retried project must be rejected")
