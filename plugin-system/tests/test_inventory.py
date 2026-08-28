from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
import yaml

from edgecitadel_supervisor.errors import LockIntegrityError, UnsafePackagePathError
from edgecitadel_supervisor.inventory import (
    LOCK_FILENAME,
    build_inventory,
    build_lock,
    package_files,
    sha256_file,
    verify_lock,
    write_lock,
)
from edgecitadel_supervisor.validator import PackageRecord, validate_package


def _package(valid_package: Path) -> PackageRecord:
    return validate_package(valid_package, verify_integrity=False)


def _write_lock_document(root: Path, document: dict[str, object]) -> None:
    (root / LOCK_FILENAME).write_text(json.dumps(document), encoding="utf-8")


def _add_skill(root: Path, name: str, skill_id: str) -> None:
    source = root / "skills" / "placeholder"
    destination = root / "skills" / name
    shutil.copytree(source, destination)
    skill_file = destination / "SKILL.md"
    skill_file.write_text(
        skill_file.read_text(encoding="utf-8").replace(
            "name: placeholder", f"name: {name}"
        ),
        encoding="utf-8",
    )
    binding_file = destination / "binding.yaml"
    binding = yaml.safe_load(binding_file.read_text(encoding="utf-8"))
    assert isinstance(binding, dict)
    binding["skillId"] = skill_id
    binding_file.write_text(yaml.safe_dump(binding, sort_keys=False), encoding="utf-8")


def test_sha256_file_hashes_bytes(tmp_path: Path) -> None:
    path = tmp_path / "payload.bin"
    payload = b"edgecitadel\x00plugin\n"
    path.write_bytes(payload)

    assert sha256_file(path) == hashlib.sha256(payload).hexdigest()


def test_sha256_file_wraps_read_errors_without_absolute_paths(
    tmp_path: Path,
) -> None:
    path = tmp_path / "missing.bin"

    with pytest.raises(LockIntegrityError, match="missing.bin") as error:
        sha256_file(path)

    assert str(tmp_path) not in str(error.value)


def test_package_files_are_posix_sorted_and_exclude_only_root_lock(
    valid_package: Path,
) -> None:
    (valid_package / LOCK_FILENAME).write_text("ignored", encoding="utf-8")
    nested = valid_package / "z-directory"
    nested.mkdir()
    (nested / LOCK_FILENAME).write_text("included", encoding="utf-8")
    (valid_package / "a-file").write_text("included", encoding="utf-8")

    relative_paths = tuple(
        path.relative_to(valid_package).as_posix()
        for path in package_files(valid_package)
    )

    assert relative_paths == tuple(sorted(relative_paths))
    assert LOCK_FILENAME not in relative_paths
    assert f"z-directory/{LOCK_FILENAME}" in relative_paths
    assert "z-directory" not in relative_paths


def test_package_files_reject_symlinks_without_absolute_paths(
    valid_package: Path, tmp_path: Path
) -> None:
    outside = tmp_path / "outside"
    outside.write_text("do not follow", encoding="utf-8")
    (valid_package / "nested-link").symlink_to(outside)

    with pytest.raises(UnsafePackagePathError, match="nested-link") as error:
        package_files(valid_package)

    assert str(valid_package) not in str(error.value)
    assert str(outside) not in str(error.value)


def test_build_lock_is_deterministic_canonical_and_uses_typed_identity(
    valid_package: Path,
) -> None:
    package = _package(valid_package)
    metadata = package.manifest["metadata"]
    assert isinstance(metadata, dict)
    metadata["publisher"] = "mutated"
    metadata["name"] = "mutated"
    metadata["version"] = "9.9.9"

    first = build_lock(package)
    second = build_lock(package)

    assert first == second
    assert "generatedAt" not in first
    assert first["lockVersion"] == 1
    assert first["package"] == {
        "id": "local.example",
        "version": "0.1.0",
        "protocol": "edgecitadel.plugin.v1",
    }
    files = first["files"]
    assert isinstance(files, list)
    assert [entry["path"] for entry in files] == sorted(
        entry["path"] for entry in files
    )
    assert LOCK_FILENAME not in {entry["path"] for entry in files}


def test_build_lock_sorts_skills_and_hashes_skill_markdown(
    valid_package: Path,
) -> None:
    _add_skill(valid_package, "alpha", "example.alpha")
    package = _package(valid_package)

    lock = build_lock(package)

    skills = lock["skills"]
    assert isinstance(skills, list)
    assert skills == [
        {
            "name": skill.name,
            "skillId": skill.skill_id,
            "version": skill.version,
            "contentSha256": hashlib.sha256(skill.skill_file.read_bytes()).hexdigest(),
        }
        for skill in sorted(package.skills, key=lambda record: record.name)
    ]


def test_write_lock_uses_exact_repeatable_canonical_bytes(valid_package: Path) -> None:
    package = _package(valid_package)
    expected = (
        json.dumps(build_lock(package), indent=2, sort_keys=True) + "\n"
    ).encode()

    lock_path = write_lock(package)
    first = lock_path.read_bytes()
    second_path = write_lock(package)

    assert lock_path == valid_package / LOCK_FILENAME
    assert second_path == lock_path
    assert first == expected
    assert lock_path.read_bytes() == expected


def test_write_lock_wraps_write_errors_with_relative_path(valid_package: Path) -> None:
    package = _package(valid_package)
    (valid_package / LOCK_FILENAME).mkdir()

    with pytest.raises(LockIntegrityError, match=LOCK_FILENAME) as error:
        write_lock(package)

    assert str(valid_package) not in str(error.value)


def test_verify_lock_accepts_unchanged_package(valid_package: Path) -> None:
    package = _package(valid_package)
    write_lock(package)

    verify_lock(package)


def test_build_lock_wraps_hash_errors_with_package_relative_path(
    valid_package: Path,
) -> None:
    package = _package(valid_package)
    package.skills[0].skill_file.unlink()

    with pytest.raises(
        LockIntegrityError, match="skills/placeholder/SKILL.md"
    ) as error:
        build_lock(package)

    assert str(valid_package) not in str(error.value)


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        (None, "Unable to load plugin lock"),
        ("[", "Unable to load plugin lock"),
        ("[]", "Unable to load plugin lock"),
        (json.dumps({"lockVersion": 1}), "failed schema validation"),
    ],
)
def test_verify_lock_translates_load_and_schema_errors(
    valid_package: Path, contents: str | None, message: str
) -> None:
    package = _package(valid_package)
    if contents is not None:
        (valid_package / LOCK_FILENAME).write_text(contents, encoding="utf-8")

    with pytest.raises(LockIntegrityError, match=message) as error:
        verify_lock(package)

    assert LOCK_FILENAME in str(error.value)
    assert str(valid_package) not in str(error.value)
    if contents:
        assert contents not in str(error.value)


def test_verify_lock_reports_duplicate_paths_even_with_different_hashes(
    valid_package: Path,
) -> None:
    package = _package(valid_package)
    lock = build_lock(package)
    files = lock["files"]
    assert isinstance(files, list)
    duplicate = dict(files[0])
    duplicate["sha256"] = "0" * 64
    files.append(duplicate)
    _write_lock_document(valid_package, lock)

    with pytest.raises(
        LockIntegrityError, match="Duplicate locked file paths"
    ) as error:
        verify_lock(package)

    assert files[0]["path"] in str(error.value)
    assert str(valid_package) not in str(error.value)


def test_verify_lock_does_not_report_unsafe_duplicate_path_values(
    valid_package: Path,
) -> None:
    package = _package(valid_package)
    lock = build_lock(package)
    files = lock["files"]
    assert isinstance(files, list)
    files.extend(
        [
            {"path": "C:/do-not-leak", "sha256": "0" * 64},
            {"path": "C:/do-not-leak", "sha256": "1" * 64},
        ]
    )
    _write_lock_document(valid_package, lock)

    with pytest.raises(LockIntegrityError, match="failed schema validation") as error:
        verify_lock(package)

    assert "do-not-leak" not in str(error.value)


@pytest.mark.parametrize(
    ("mutation", "message", "expected"),
    [
        ("missing", "Missing locked files", "plugin.yaml"),
        ("modified", "Modified files", "plugin.yaml"),
        ("unlisted", "Unlisted package files", "extra.txt"),
    ],
)
def test_verify_lock_reports_file_drift_deterministically(
    valid_package: Path, mutation: str, message: str, expected: str
) -> None:
    package = _package(valid_package)
    lock = build_lock(package)
    if mutation == "missing":
        (valid_package / expected).unlink()
    elif mutation == "modified":
        (valid_package / expected).write_text("modified", encoding="utf-8")
    else:
        (valid_package / expected).write_text("extra", encoding="utf-8")
    _write_lock_document(valid_package, lock)

    with pytest.raises(LockIntegrityError, match=message) as error:
        verify_lock(package)

    assert expected in str(error.value)
    assert str(valid_package) not in str(error.value)


def test_verify_lock_reports_mismatched_package_metadata(valid_package: Path) -> None:
    package = _package(valid_package)
    lock = build_lock(package)
    locked_package = lock["package"]
    assert isinstance(locked_package, dict)
    locked_package["version"] = "9.9.9"
    _write_lock_document(valid_package, lock)

    with pytest.raises(LockIntegrityError, match="package metadata.*version") as error:
        verify_lock(package)

    assert "9.9.9" not in str(error.value)


def test_verify_lock_reports_missing_duplicate_and_mismatched_skills(
    valid_package: Path,
) -> None:
    _add_skill(valid_package, "alpha", "example.alpha")
    package = _package(valid_package)
    lock = build_lock(package)
    skills = lock["skills"]
    assert isinstance(skills, list)
    placeholder = next(skill for skill in skills if skill["name"] == "placeholder")
    placeholder["skillId"] = "example.wrong"
    placeholder["contentSha256"] = "0" * 64
    alpha = next(skill for skill in skills if skill["name"] == "alpha")
    skills.remove(alpha)
    duplicate = dict(placeholder)
    duplicate["version"] = "0.2.0"
    skills.append(duplicate)
    _write_lock_document(valid_package, lock)

    with pytest.raises(LockIntegrityError) as error:
        verify_lock(package)

    message = str(error.value)
    assert "Duplicate locked skill names: placeholder" in message
    assert "Missing locked skills: alpha" in message
    assert "Mismatched locked skills: placeholder" in message
    assert "0" * 64 not in message
    assert str(valid_package) not in message


def test_build_inventory_is_deterministic_json_compatible_and_path_free(
    valid_package: Path,
) -> None:
    _add_skill(valid_package, "alpha", "example.alpha")
    package = _package(valid_package)
    metadata = package.manifest["metadata"]
    assert isinstance(metadata, dict)
    metadata["publisher"] = "mutated"
    metadata["name"] = "mutated"
    metadata["version"] = "9.9.9"

    first = build_inventory(package)
    second = build_inventory(package)

    assert first == second
    assert json.loads(json.dumps(first)) == first
    assert first["package"] == {
        "id": "local.example",
        "version": "0.1.0",
        "protocol": "edgecitadel.plugin.v1",
    }
    assert first["compatibility"] == package.manifest["compatibility"]
    assert first["runtime"] == package.manifest["runtime"]
    assert first["permissions"] == package.manifest["permissions"]
    assert first["security"] == package.manifest["security"]
    assert first["agents"] == [{"id": "example-agent", "skillNames": ["placeholder"]}]
    skills = first["skills"]
    assert isinstance(skills, list)
    assert [skill["name"] for skill in skills] == ["alpha", "placeholder"]
    assert all("contentSha256" in skill for skill in skills)
    assert str(valid_package) not in json.dumps(first)
