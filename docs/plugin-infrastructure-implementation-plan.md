# EdgeCitadel Plugin Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a framework-neutral plugin package contract, portable procedural skills, deterministic package integrity, placeholder SDK protocols, and a runnable no-op supervisor that locks and validates packages without executing them.

**Architecture:** Host-side code lives in `plugin-system/`; installable packages live in `plugins/`. The supervisor safely loads YAML/JSON, validates strict schemas and Agent Skills metadata, checks compatibility and containment, generates or verifies a canonical SHA-256 lockfile, and emits deterministic inventory JSON. Existing adapters, messaging, persistence, and deployment remain untouched.

**Tech Stack:** Python 3.12, PyYAML, jsonschema Draft 2020-12, packaging specifiers, pytest, JSON Schema, Agent Skills `SKILL.md` conventions.

---

## File map

Host-side files:

- `plugin-system/pyproject.toml`: isolated package metadata, runtime dependencies, and pytest configuration.
- `plugin-system/schemas/*.json`: strict plugin, binding, and lock contracts.
- `plugin-system/src/edgecitadel_supervisor/errors.py`: stable domain errors.
- `plugin-system/src/edgecitadel_supervisor/loader.py`: safe file parsing, frontmatter parsing, and contained-path resolution.
- `plugin-system/src/edgecitadel_supervisor/validator.py`: schema, compatibility, skill, mapping, and package validation.
- `plugin-system/src/edgecitadel_supervisor/inventory.py`: canonical inventory, SHA-256 lock generation, and lock verification.
- `plugin-system/src/edgecitadel_supervisor/cli.py`: `lock` and `validate` commands.
- `plugin-system/src/edgecitadel_plugin_sdk/*.py`: protocol-only extension seams.
- `plugin-system/tests/`: hermetic behavioral tests using temporary packages.

Installable package files:

- `plugins/examples/placeholder/plugin.yaml`: example package declaration.
- `plugins/examples/placeholder/plugin.lock.json`: generated canonical integrity record.
- `plugins/examples/placeholder/skills/placeholder/SKILL.md`: portable procedure and activation metadata.
- `plugins/examples/placeholder/skills/placeholder/binding.yaml`: A2A/runtime binding.
- `plugins/examples/placeholder/skills/placeholder/schemas/*.json`: typed skill boundary.
- `plugins/examples/placeholder/runtime/`: deliberately nonfunctional runtime package.

Repository documentation:

- `plugin-system/README.md` and `plugins/README.md`: host and package authoring boundaries.
- `AGENTS.md`: repository map entries and verification command.

### Task 1: Establish the Python package and strict schemas

**Files:**

- Create: `plugin-system/pyproject.toml`
- Create: `plugin-system/schemas/agent-plugin.v1alpha1.schema.json`
- Create: `plugin-system/schemas/agent-skill-binding.v1alpha1.schema.json`
- Create: `plugin-system/schemas/plugin-lock.v1.schema.json`
- Create: `plugin-system/tests/test_schemas.py`

- [ ] **Step 1: Write schema tests first**

Create `test_schemas.py` with one test per schema and explicit valid/invalid samples:

```python
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError


SCHEMAS = Path(__file__).parents[1] / "schemas"


def validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMAS / name).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def test_plugin_schema_accepts_separate_package_and_agent_identity():
    document = {
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentPlugin",
        "metadata": {
            "name": "example",
            "displayName": "Example",
            "description": "Example package.",
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
        "agents": [{
            "id": "example-agent",
            "skillNames": ["example"],
            "listensBroadcast": False,
        }],
        "permissions": {
            "knowledge": [],
            "messaging": {"outboundAgents": []},
            "network": {"outbound": []},
            "devices": [],
        },
        "security": {"sandbox": "restricted", "secrets": []},
        "extensions": {},
    }
    validator("agent-plugin.v1alpha1.schema.json").validate(document)


def test_plugin_schema_rejects_unknown_core_field():
    with pytest.raises(ValidationError):
        validator("agent-plugin.v1alpha1.schema.json").validate({"unexpected": True})


def test_binding_schema_requires_runtime_execution_name():
    document = {
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
    validator("agent-skill-binding.v1alpha1.schema.json").validate(document)


def test_lock_schema_requires_sorted_file_records_shape():
    document = {
        "lockVersion": 1,
        "package": {
            "id": "local.example",
            "version": "0.1.0",
            "protocol": "edgecitadel.plugin.v1",
        },
        "files": [{"path": "plugin.yaml", "sha256": "0" * 64}],
        "skills": [{
            "name": "placeholder",
            "skillId": "example.placeholder",
            "version": "0.1.0",
            "contentSha256": "1" * 64,
        }],
    }
    validator("plugin-lock.v1.schema.json").validate(document)
```

- [ ] **Step 2: Run the schema tests and verify RED**

Run:

```bash
cd /Users/yefanzhang/workplace/edge-research/plugin-system
python -m pytest tests/test_schemas.py -q
```

Expected: failure because `pyproject.toml` and the schema files do not exist.

- [ ] **Step 3: Add package metadata and schemas**

Create `pyproject.toml` with these exact constraints and source layout:

```toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "edgecitadel-plugin-system"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "jsonschema>=4.23,<5",
  "packaging>=24,<26",
  "PyYAML>=6,<7",
]

[project.optional-dependencies]
test = ["pytest>=8,<9"]

[project.scripts]
edgecitadel-supervisor = "edgecitadel_supervisor.cli:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

Implement all three Draft 2020-12 schemas with `additionalProperties: false` on
core objects. Reuse `$defs` for normalized names, semantic versions, relative
paths, extension maps, SHA-256 strings, and agent IDs. Require every field shown
in the tests. `extensions` keys must match either reverse-domain form or an
absolute URI.

- [ ] **Step 4: Install the isolated package and test dependencies**

Run:

```bash
python -m pip install -e '.[test]'
```

Expected: the editable `edgecitadel-plugin-system` package and its constrained
dependencies install successfully.

- [ ] **Step 5: Run the schema tests and verify GREEN**

Run `python -m pytest tests/test_schemas.py -q` from `plugin-system/`.

Expected: all schema tests pass.

- [ ] **Step 6: Commit the schema foundation**

```bash
git add plugin-system/pyproject.toml plugin-system/schemas plugin-system/tests/test_schemas.py
git commit -m "feat(infra): define plugin package schemas"
```

### Task 2: Build safe loaders and stable errors

**Files:**

- Create: `plugin-system/src/edgecitadel_supervisor/__init__.py`
- Create: `plugin-system/src/edgecitadel_supervisor/errors.py`
- Create: `plugin-system/src/edgecitadel_supervisor/loader.py`
- Create: `plugin-system/tests/test_loader.py`

- [ ] **Step 1: Write failing loader tests**

Cover missing roots, malformed YAML, malformed frontmatter, absolute paths,
parent traversal, and symlink rejection:

```python
def test_load_yaml_wraps_parser_failure(tmp_path):
    path = tmp_path / "plugin.yaml"
    path.write_text("metadata: [")
    with pytest.raises(ManifestLoadError, match="plugin.yaml"):
        load_yaml(path)


def test_resolve_package_path_rejects_parent_traversal(tmp_path):
    with pytest.raises(UnsafePackagePathError, match="outside package"):
        resolve_package_path(tmp_path, "../secret")


def test_reject_symlinks_finds_nested_link(tmp_path):
    target = tmp_path / "target"
    target.write_text("data")
    nested = tmp_path / "skills"
    nested.mkdir()
    (nested / "escape").symlink_to(target)
    with pytest.raises(UnsafePackagePathError, match="symbolic link"):
        reject_symlinks(tmp_path)


def test_load_skill_markdown_returns_frontmatter_and_body(tmp_path):
    path = tmp_path / "SKILL.md"
    path.write_text("---\nname: example\ndescription: Use for examples.\n---\n# Procedure\n")
    metadata, body = load_skill_markdown(path)
    assert metadata["name"] == "example"
    assert body == "# Procedure\n"
```

- [ ] **Step 2: Run loader tests and verify RED**

Run `python -m pytest tests/test_loader.py -q`.

Expected: import failure because supervisor modules do not exist.

- [ ] **Step 3: Implement errors and loader helpers**

Define the exact error hierarchy:

```python
class PluginError(RuntimeError):
    """Base class for plugin package failures."""


class PluginNotFoundError(PluginError): ...
class ManifestLoadError(PluginError): ...
class ManifestValidationError(PluginError): ...
class CompatibilityError(PluginError): ...
class UnsafePackagePathError(PluginError): ...
class SkillDiscoveryError(PluginError): ...
class DuplicateSkillError(PluginError): ...
class LockIntegrityError(PluginError): ...
```

Implement these loader signatures:

```python
def require_plugin_root(path: str | Path) -> Path: ...
def load_yaml(path: Path) -> dict[str, object]: ...
def load_json(path: Path) -> dict[str, object]: ...
def load_skill_markdown(path: Path) -> tuple[dict[str, object], str]: ...
def resolve_package_path(root: Path, relative: str, *, base: Path | None = None) -> Path: ...
def reject_symlinks(root: Path) -> None: ...
```

Use `yaml.safe_load`, require mapping roots, split `SKILL.md` only when it begins
with `---\n`, and resolve paths with `Path.resolve(strict=False)`. Use
`candidate.is_relative_to(root.resolve())` for containment and reject absolute
input before resolution. Walk with `root.rglob("*")` and reject `is_symlink()`.

- [ ] **Step 4: Run loader tests and verify GREEN**

Run `python -m pytest tests/test_loader.py -q`.

Expected: all loader tests pass.

- [ ] **Step 5: Commit the safe loader**

```bash
git add plugin-system/src/edgecitadel_supervisor plugin-system/tests/test_loader.py
git commit -m "feat(infra): add safe plugin package loader"
```

### Task 3: Validate portable skills and EdgeCitadel bindings

**Files:**

- Create: `plugin-system/src/edgecitadel_supervisor/validator.py`
- Create: `plugin-system/tests/conftest.py`
- Create: `plugin-system/tests/test_validator.py`

- [ ] **Step 1: Create a temporary-package fixture and failing skill tests**

The fixture must write a minimal valid package excluding its lockfile and return
the root path. Tests then mutate one concern at a time:

```python
@pytest.fixture
def valid_package(tmp_path: Path) -> Path:
    root = tmp_path / "example"
    skill = root / "skills" / "placeholder"
    schemas = skill / "schemas"
    schemas.mkdir(parents=True)
    (root / "plugin.yaml").write_text(yaml.safe_dump({
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentPlugin",
        "metadata": {
            "name": "example", "displayName": "Example",
            "description": "Example package.", "version": "0.1.0",
            "publisher": "local",
        },
        "compatibility": {
            "supervisorApi": ">=0.1.0,<0.2.0",
            "protocols": ["edgecitadel.plugin.v1"],
        },
        "runtime": {
            "command": ["python", "-m", "runtime"],
            "healthTimeoutSeconds": 10, "restartPolicy": "on-failure",
        },
        "skills": {"directory": "skills"},
        "agents": [{
            "id": "example-agent", "skillNames": ["placeholder"],
            "listensBroadcast": False,
        }],
        "permissions": {
            "knowledge": [], "messaging": {"outboundAgents": []},
            "network": {"outbound": []}, "devices": [],
        },
        "security": {"sandbox": "restricted", "secrets": []},
        "extensions": {},
    }, sort_keys=False))
    (skill / "SKILL.md").write_text(
        "---\nname: placeholder\n"
        "description: Use when validating an example package.\n"
        "compatibility: Requires EdgeCitadel plugin protocol v1.\n"
        "metadata:\n  version: '0.1.0'\n---\n# Procedure\nReturn a message.\n"
    )
    (skill / "binding.yaml").write_text(yaml.safe_dump({
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentSkillBinding",
        "skillId": "example.placeholder",
        "version": "0.1.0",
        "execution": {"kind": "runtime-handler", "name": "placeholder"},
        "schemas": {
            "input": "schemas/input.json", "output": "schemas/output.json",
        },
        "requires": {"knowledge": [], "network": [], "devices": []},
        "extensions": {},
    }, sort_keys=False))
    object_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object", "additionalProperties": False,
    }
    (schemas / "input.json").write_text(json.dumps(object_schema))
    (schemas / "output.json").write_text(json.dumps(object_schema))
    return root


def test_discover_skills_combines_portable_and_binding_metadata(valid_package):
    skills = discover_skills(valid_package, "skills")
    assert [(s.name, s.skill_id, s.execution_name) for s in skills] == [
        ("placeholder", "example.placeholder", "placeholder")
    ]


def test_skill_directory_must_match_frontmatter_name(valid_package):
    skill = valid_package / "skills" / "placeholder" / "SKILL.md"
    skill.write_text(skill.read_text().replace("name: placeholder", "name: renamed"))
    with pytest.raises(ManifestValidationError, match="directory name"):
        discover_skills(valid_package, "skills")


def test_duplicate_a2a_skill_ids_are_rejected(valid_package):
    shutil.copytree(
        valid_package / "skills" / "placeholder",
        valid_package / "skills" / "second",
    )
    second = valid_package / "skills" / "second" / "SKILL.md"
    second.write_text(second.read_text().replace("name: placeholder", "name: second"))
    with pytest.raises(DuplicateSkillError, match="example.placeholder"):
        discover_skills(valid_package, "skills")


def test_schema_reference_cannot_escape_skill_directory(valid_package):
    binding = valid_package / "skills" / "placeholder" / "binding.yaml"
    binding.write_text(binding.read_text().replace("schemas/input.json", "../../plugin.yaml"))
    with pytest.raises(UnsafePackagePathError):
        discover_skills(valid_package, "skills")
```

- [ ] **Step 2: Run validator tests and verify RED**

Run `python -m pytest tests/test_validator.py -q`.

Expected: import failure because `validator.py` does not exist.

- [ ] **Step 3: Implement schema validation and skill discovery**

Define an immutable skill record:

```python
@dataclass(frozen=True)
class SkillRecord:
    name: str
    description: str
    skill_id: str
    version: str
    execution_name: str
    directory: Path
    skill_file: Path
    binding_file: Path
    input_schema: Path
    output_schema: Path
```

Implement:

```python
def validate_schema(document: object, schema_name: str) -> None: ...
def validate_skill_metadata(metadata: dict[str, object], directory_name: str) -> None: ...
def discover_skills(root: Path, skills_directory: str) -> tuple[SkillRecord, ...]: ...
```

Validate Agent Skills name and description limits, require a non-empty Markdown
body, validate each binding against its JSON Schema, resolve both schema paths
relative to the skill directory, require valid JSON Schema documents, and return
records sorted by portable name. Track portable names and A2A IDs separately so
either duplicate produces `DuplicateSkillError`.

- [ ] **Step 4: Run validator tests and verify GREEN**

Run `python -m pytest tests/test_validator.py -q`.

Expected: all skill validation tests pass.

- [ ] **Step 5: Commit skill validation**

```bash
git add plugin-system/src/edgecitadel_supervisor/validator.py plugin-system/tests
git commit -m "feat(infra): validate packaged agent skills"
```

### Task 4: Validate plugin compatibility and agent mappings

**Files:**

- Modify: `plugin-system/src/edgecitadel_supervisor/validator.py`
- Modify: `plugin-system/tests/test_validator.py`

- [ ] **Step 1: Write failing package validation tests**

```python
def replace_manifest(root: Path, value: object, field: str) -> None:
    path = root / "plugin.yaml"
    document = yaml.safe_load(path.read_text())
    document["compatibility"][field] = value
    path.write_text(yaml.safe_dump(document, sort_keys=False))


def replace_protocols(root: Path, protocols: list[str]) -> None:
    replace_manifest(root, protocols, "protocols")


def replace_agent_skills(root: Path, skill_names: list[str]) -> None:
    path = root / "plugin.yaml"
    document = yaml.safe_load(path.read_text())
    document["agents"][0]["skillNames"] = skill_names
    path.write_text(yaml.safe_dump(document, sort_keys=False))


def test_validate_package_returns_package_and_skill_records(valid_package):
    package = validate_package(valid_package, verify_integrity=False)
    assert package.package_id == "local.example"
    assert package.agent_skill_names == {"example-agent": ("placeholder",)}


def test_rejects_unsupported_supervisor_api(valid_package):
    replace_manifest(valid_package, ">=9.0.0", "supervisorApi")
    with pytest.raises(CompatibilityError, match="supervisor API"):
        validate_package(valid_package, verify_integrity=False)


def test_rejects_unsupported_process_protocol(valid_package):
    replace_protocols(valid_package, ["edgecitadel.plugin.v9"])
    with pytest.raises(CompatibilityError, match="process protocol"):
        validate_package(valid_package, verify_integrity=False)


def test_rejects_unknown_agent_skill_name(valid_package):
    replace_agent_skills(valid_package, ["missing"])
    with pytest.raises(ManifestValidationError, match="unknown skill"):
        validate_package(valid_package, verify_integrity=False)
```

- [ ] **Step 2: Run targeted tests and verify RED**

Run `python -m pytest tests/test_validator.py -q`.

Expected: failures because `validate_package` and `PackageRecord` do not exist.

- [ ] **Step 3: Implement package validation**

Add constants and records:

```python
SUPERVISOR_API_VERSION = Version("0.1.0")
SUPPORTED_PROTOCOLS = frozenset({"edgecitadel.plugin.v1"})


@dataclass(frozen=True)
class PackageRecord:
    root: Path
    manifest: dict[str, object]
    package_id: str
    package_version: str
    protocol: str
    skills: tuple[SkillRecord, ...]
    agent_skill_names: dict[str, tuple[str, ...]]
```

Implement `validate_package(root, *, verify_integrity=True)`. Require the
supervisor version to satisfy `SpecifierSet(supervisorApi)`, select exactly one
supported declared protocol, reject duplicate agent IDs, verify every skill name
reference, and call `reject_symlinks` before reading package content.

- [ ] **Step 4: Run package validation tests and verify GREEN**

Run `python -m pytest tests/test_validator.py -q`.

Expected: all package and skill tests pass.

- [ ] **Step 5: Commit compatibility and mapping validation**

```bash
git add plugin-system/src/edgecitadel_supervisor/validator.py plugin-system/tests/test_validator.py plugin-system/tests/conftest.py
git commit -m "feat(infra): validate plugin compatibility and agents"
```

### Task 5: Generate and verify canonical lockfiles

**Files:**

- Create: `plugin-system/src/edgecitadel_supervisor/inventory.py`
- Create: `plugin-system/tests/test_inventory.py`
- Modify: `plugin-system/src/edgecitadel_supervisor/validator.py`

- [ ] **Step 1: Write failing inventory and integrity tests**

```python
def test_build_lock_is_deterministic_and_excludes_itself(valid_package):
    package = validate_package(valid_package, verify_integrity=False)
    first = build_lock(package)
    second = build_lock(package)
    assert first == second
    assert "generatedAt" not in first
    assert [item["path"] for item in first["files"]] == sorted(
        item["path"] for item in first["files"]
    )
    assert "plugin.lock.json" not in {item["path"] for item in first["files"]}


def test_verify_lock_rejects_modified_procedure(valid_package):
    package = validate_package(valid_package, verify_integrity=False)
    write_lock(package)
    skill = valid_package / "skills" / "placeholder" / "SKILL.md"
    skill.write_text(skill.read_text() + "\nChanged.\n")
    with pytest.raises(LockIntegrityError, match="modified"):
        verify_lock(package)


def test_verify_lock_rejects_unlisted_file(valid_package):
    package = validate_package(valid_package, verify_integrity=False)
    write_lock(package)
    (valid_package / "extra.txt").write_text("extra")
    with pytest.raises(LockIntegrityError, match="unlisted"):
        verify_lock(package)
```

- [ ] **Step 2: Run inventory tests and verify RED**

Run `python -m pytest tests/test_inventory.py -q`.

Expected: import failure because `inventory.py` does not exist.

- [ ] **Step 3: Implement canonical inventory and lock operations**

Implement these functions:

```python
LOCK_FILENAME = "plugin.lock.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_files(root: Path) -> tuple[Path, ...]: ...
def build_lock(package: PackageRecord) -> dict[str, object]: ...
def write_lock(package: PackageRecord) -> Path: ...
def verify_lock(package: PackageRecord) -> None: ...
def build_inventory(package: PackageRecord) -> dict[str, object]: ...
```

`package_files` includes every regular file below the package root except
`plugin.lock.json`, sorts by POSIX relative path, and rejects symlinks. `write_lock`
uses `json.dumps(lock, indent=2, sort_keys=True) + "\n"`. `verify_lock` validates
the loaded lock schema before comparing path sets and hashes, and reports missing,
modified, duplicated, and unlisted files via `LockIntegrityError`.

Update `validate_package(..., verify_integrity=True)` to import and call
`verify_lock` only after structural validation, avoiding an import cycle by doing
the import inside the function.

- [ ] **Step 4: Run inventory tests and the full suite**

Run:

```bash
python -m pytest tests/test_inventory.py -q
python -m pytest -q
```

Expected: both commands pass.

- [ ] **Step 5: Commit lockfile integrity**

```bash
git add plugin-system/src/edgecitadel_supervisor plugin-system/tests/test_inventory.py
git commit -m "feat(infra): add deterministic plugin integrity locks"
```

### Task 6: Add the no-op supervisor CLI

**Files:**

- Create: `plugin-system/src/edgecitadel_supervisor/cli.py`
- Create: `plugin-system/src/edgecitadel_supervisor/__main__.py`
- Create: `plugin-system/tests/test_cli.py`

- [ ] **Step 1: Write failing CLI tests**

```python
def test_lock_command_writes_canonical_lock(valid_package, capsys):
    assert main(["lock", str(valid_package)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "lockfile": str(valid_package / "plugin.lock.json"),
        "packageId": "local.example",
        "status": "locked",
    }


def test_validate_command_emits_deterministic_inventory(valid_package, capsys):
    package = validate_package(valid_package, verify_integrity=False)
    write_lock(package)
    assert main(["validate", str(valid_package)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["package"]["id"] == "local.example"
    assert payload["skills"][0]["name"] == "placeholder"


def test_cli_reports_domain_error_only_on_stderr(tmp_path, capsys):
    assert main(["validate", str(tmp_path / "missing")]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "plugin root not found" in captured.err
```

- [ ] **Step 2: Run CLI tests and verify RED**

Run `python -m pytest tests/test_cli.py -q`.

Expected: import failure because CLI modules do not exist.

- [ ] **Step 3: Implement `lock` and `validate` commands**

Use `argparse` with required subcommands. `main(argv: Sequence[str] | None = None)
-> int` catches only `PluginError`, writes `error: <message>\n` to stderr, and
returns 2. Successful commands print canonical JSON with `sort_keys=True` and
return 0. `__main__.py` contains only:

```python
from .cli import main

raise SystemExit(main())
```

The CLI must never import a module from inside the plugin package or call
`subprocess`.

- [ ] **Step 4: Run CLI tests and a static no-execution assertion**

Run:

```bash
python -m pytest tests/test_cli.py -q
if rg -n "importlib|subprocess|exec\(|eval\(" src/edgecitadel_supervisor; then exit 1; fi
```

Expected: CLI tests pass; `rg` returns no matches.

- [ ] **Step 5: Commit the CLI**

```bash
git add plugin-system/src/edgecitadel_supervisor plugin-system/tests/test_cli.py
git commit -m "feat(infra): add plugin validation supervisor cli"
```

### Task 7: Add protocol-only SDK seams

**Files:**

- Create: `plugin-system/src/edgecitadel_plugin_sdk/__init__.py`
- Create: `plugin-system/src/edgecitadel_plugin_sdk/lifecycle.py`
- Create: `plugin-system/src/edgecitadel_plugin_sdk/runtime.py`
- Create: `plugin-system/src/edgecitadel_plugin_sdk/skills.py`
- Create: `plugin-system/src/edgecitadel_plugin_sdk/knowledge.py`
- Create: `plugin-system/src/edgecitadel_plugin_sdk/transport.py`
- Create: `plugin-system/tests/test_sdk_contracts.py`

- [ ] **Step 1: Write failing protocol contract tests**

```python
def test_lifecycle_states_reserve_full_supervisor_vocabulary():
    assert [state.value for state in LifecycleState] == [
        "discovered", "validated", "installed", "starting", "ready",
        "draining", "stopped", "failed",
    ]


def test_protocols_are_runtime_checkable():
    assert AgentRuntime._is_protocol is True
    assert SkillProvider._is_protocol is True
    assert KnowledgeStore._is_protocol is True
    assert Transport._is_protocol is True
    assert LifecycleHooks._is_protocol is True


def test_knowledge_record_preserves_provenance_fields():
    record = KnowledgeRecord(
        plugin_id="local.example",
        skill_id="example.placeholder",
        skill_version="0.1.0",
        namespace="procedures/example",
        revision=1,
        content_hash="0" * 64,
        provenance=("task:123",),
    )
    assert record.revision == 1
```

- [ ] **Step 2: Run SDK tests and verify RED**

Run `python -m pytest tests/test_sdk_contracts.py -q`.

Expected: import failure because SDK modules do not exist.

- [ ] **Step 3: Implement minimal protocols and immutable value types**

Use `@runtime_checkable Protocol`, `@dataclass(frozen=True)`, `Enum`, `Mapping`,
and `AsyncIterator`. Keep NATS, YAML, filesystem paths, and framework classes out
of public signatures. The runtime surface is:

```python
@runtime_checkable
class AgentRuntime(Protocol):
    async def initialize(self, context: RuntimeContext) -> None: ...
    async def handle(self, message: Mapping[str, object]) -> Mapping[str, object]: ...
    async def drain(self) -> None: ...
    async def shutdown(self) -> None: ...
```

The remaining protocols expose only enumeration/resolution of skill descriptors,
read/propose for knowledge, register/receive/publish/drain for transport, and
before/after lifecycle callbacks. Re-export the public types from `__init__.py`.

- [ ] **Step 4: Run SDK and full tests**

Run:

```bash
python -m pytest tests/test_sdk_contracts.py -q
python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit SDK seams**

```bash
git add plugin-system/src/edgecitadel_plugin_sdk plugin-system/tests/test_sdk_contracts.py
git commit -m "feat(infra): define plugin sdk protocol seams"
```

### Task 8: Create and lock the placeholder plugin

**Files:**

- Create: `plugins/examples/placeholder/plugin.yaml`
- Create: `plugins/examples/placeholder/runtime/__init__.py`
- Create: `plugins/examples/placeholder/runtime/__main__.py`
- Create: `plugins/examples/placeholder/skills/placeholder/SKILL.md`
- Create: `plugins/examples/placeholder/skills/placeholder/binding.yaml`
- Create: `plugins/examples/placeholder/skills/placeholder/schemas/input.json`
- Create: `plugins/examples/placeholder/skills/placeholder/schemas/output.json`
- Create: `plugins/examples/placeholder/skills/placeholder/references/README.md`
- Create: `plugins/examples/placeholder/skills/placeholder/scripts/README.md`
- Create: `plugins/examples/placeholder/skills/placeholder/assets/README.md`
- Create: `plugins/examples/placeholder/README.md`
- Generate: `plugins/examples/placeholder/plugin.lock.json`
- Create: `plugin-system/tests/test_example_package.py`

- [ ] **Step 1: Write the failing repository-example test**

```python
def test_repository_placeholder_package_is_valid():
    root = Path(__file__).parents[2] / "plugins" / "examples" / "placeholder"
    package = validate_package(root)
    assert package.package_id == "local.placeholder"
    assert package.agent_skill_names == {
        "placeholder-agent": ("placeholder",)
    }
```

- [ ] **Step 2: Run the example test and verify RED**

Run `python -m pytest tests/test_example_package.py -q`.

Expected: `PluginNotFoundError` because the example package does not exist.

- [ ] **Step 3: Create the example package content**

Use the manifest and binding shown in the design spec, changing the binding
`skillId` to `example.placeholder` and package identity to `local.placeholder`.
The input schema accepts a required string field `body` and no unknown fields.
The output schema accepts a required string field `message` and no unknown fields.

`SKILL.md` must include a real, bounded procedure:

```markdown
---
name: placeholder
description: Validate the EdgeCitadel plugin package path without performing external work. Use when testing plugin discovery and procedural packaging.
compatibility: Requires the EdgeCitadel plugin runtime v1 protocol.
metadata:
  version: "0.1.0"
---

# Placeholder validation procedure

1. Accept an input object containing `body`.
2. Do not access the network, filesystem, devices, secrets, or shared knowledge.
3. Return an object whose `message` states that execution is intentionally unavailable.

Success means the response matches `schemas/output.json` and produces no side effects.
```

The runtime `__main__.py` must make nonimplementation explicit and safe:

```python
def main() -> int:
    raise RuntimeError(
        "placeholder runtime execution is outside the plugin-infrastructure milestone"
    )


if __name__ == "__main__":
    raise SystemExit(main())
```

The three resource-directory README files explain their intended contents and
state that the example deliberately ships no executable helper, reference data,
or binary asset.

- [ ] **Step 4: Generate the lock and verify GREEN**

Run:

```bash
cd /Users/yefanzhang/workplace/edge-research/plugin-system
python -m edgecitadel_supervisor lock ../plugins/examples/placeholder
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
python -m pytest tests/test_example_package.py -q
```

Expected: `lock` and `validate` emit JSON with `packageId: local.placeholder`;
the example test passes. Do not run the placeholder runtime.

- [ ] **Step 5: Commit the placeholder package**

```bash
git add plugins plugin-system/tests/test_example_package.py
git commit -m "feat(infra): add placeholder agent plugin package"
```

### Task 9: Document boundaries and run the repository gate

**Files:**

- Create: `plugin-system/README.md`
- Create: `plugins/README.md`
- Modify: `AGENTS.md`
- Modify: `docs/plugin-infrastructure-design.md` only if implementation reveals a factual mismatch.

- [ ] **Step 1: Write host and package documentation**

`plugin-system/README.md` documents installation in a virtual environment,
`lock`/`validate` commands, static safety guarantees, and explicit non-goals.
`plugins/README.md` documents package identity, `SKILL.md` versus `binding.yaml`,
lock regeneration, and the prohibition on package mutation for learned memory.

- [ ] **Step 2: Update the repository map**

Add these entries to `AGENTS.md` under “Repo map”:

```markdown
- `plugin-system/` - Plugin schemas, SDK protocols, no-op supervisor, and hermetic tests
- `plugins/` - Installable EdgeCitadel plugin packages and examples
```

Add this command under “Commands”:

```markdown
- Plugin checks: `cd plugin-system && python -m pytest -q && python -m edgecitadel_supervisor validate ../plugins/examples/placeholder`
```

- [ ] **Step 3: Run focused quality checks**

Run:

```bash
cd /Users/yefanzhang/workplace/edge-research/plugin-system
python -m pytest -q
python -m compileall -q src tests
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
```

Expected: pytest passes, compileall is silent with exit 0, and validation emits
deterministic inventory JSON.

- [ ] **Step 4: Prove existing contracts were untouched**

Run:

```bash
cd /Users/yefanzhang/workplace/edge-research
git diff main...HEAD --name-only
git diff --check
```

Expected: changes are limited to `AGENTS.md`, `docs/plugin-infrastructure-*`,
`plugin-system/`, and `plugins/`. No file under `aggregator/`, `adapters/`,
`schemas/`, `nats/`, `frontend/`, or `docker-compose.yml` appears.

- [ ] **Step 5: Run the repository `verify-infra` gate**

Run:

```bash
cd /Users/yefanzhang/workplace/edge-research
docker compose down
docker compose up --build -d
docker compose ps
curl --fail http://localhost:8222/healthz
curl --fail http://localhost/api/system/status
cd e2e
npm test -- tests/phase1-smoke.spec.js
```

Expected: every Compose service is running without restart loops, both curl
commands return 2xx, and the Phase 1 Playwright smoke passes. If Docker is
unavailable, record the exact skipped steps and do not claim end-to-end infra
verification.

- [ ] **Step 6: Run the commit-check skill and commit documentation**

After the commit-check passes:

```bash
git add AGENTS.md plugin-system/README.md plugins/README.md docs/plugin-infrastructure-design.md
git commit -m "docs(infra): document plugin package workflow"
```

- [ ] **Step 7: Run verification-before-completion**

Re-run the full plugin test suite and both CLI commands from a clean shell. Record
the command output, final commit list, and `git status --short` for the handoff.
