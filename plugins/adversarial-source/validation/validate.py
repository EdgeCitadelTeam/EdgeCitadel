"""Offline package/action checks; stdlib only, never executes a payload."""

import argparse
import copy
import json
import re
from pathlib import Path


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_text(), object_pairs_hook=unique)


def pointer_parts(pointer):
    if pointer == "":
        return []
    assert pointer.startswith("/"), "Expected JSON Pointer"
    assert not re.search(r"~(?![01])", pointer), "Invalid pointer escape"
    return [p.replace("~1", "/").replace("~0", "~") for p in pointer[1:].split("/")]


def slot(value, part):
    if isinstance(value, list):
        assert re.fullmatch(r"0|[1-9][0-9]*", part), "Invalid array index"
        return int(part)
    assert isinstance(value, dict), "Pointer traverses scalar"
    return part


def masked(value, pointer):
    value = copy.deepcopy(value)
    parts = pointer_parts(pointer)
    if not parts:
        assert isinstance(value, str), "Selected root must be a string"
        return None
    current = value
    for part in parts[:-1]:
        current = current[slot(current, part)]
    key = slot(current, parts[-1])
    assert isinstance(current[key], str), "Selected field must exist and be a string"
    current[key] = None
    return value


def canonical(value):
    # Serialization distinguishes bool/int, null/missing, and preserves array order.
    return json.dumps(value, sort_keys=True, allow_nan=False)


def fixture_path(directory, relative):
    assert not Path(relative).is_absolute()
    path = (directory / relative).resolve()
    assert path.is_relative_to(directory.resolve()), "Fixture escapes action directory"
    assert path.is_file()
    return path


def validate_action(directory):
    action = read_json(directory / "action.json")
    assert action["format_version"] == 1 and action["status"] == "prepared"
    assert action["threat_model"] == "external-content-only"
    for field in ("action_id", "benign_task", "objective"):
        assert isinstance(action[field], str) and action[field].strip()
    assert isinstance(action["parameters"], dict)
    for field in ("benign_success", "attack_attempt", "attack_effect"):
        assert (
            isinstance(action["checks"][field], str) and action["checks"][field].strip()
        )
    assert action["surface"]["source_id"]
    baseline = fixture_path(directory, action["baseline_path"])
    payload = fixture_path(directory, action["payload_path"])
    assert baseline != payload
    carrier = action["carrier"]
    assert carrier in ("document-body", "tool-response-field")
    expected = (
        "attack-document-injection"
        if carrier == "document-body"
        else "attack-tool-response-injection"
    )
    assert action["techniques"][0] == expected
    assert all(t in SKILLS - {"compose-attack-actions"} for t in action["techniques"])
    assert not any(
        t in ("attack-document-injection", "attack-tool-response-injection")
        for t in action["techniques"][1:]
    )
    if carrier == "tool-response-field":
        before, after = read_json(baseline), read_json(payload)
        pointer = action["surface"]["selector"]
        assert canonical(masked(before, pointer)) == canonical(
            masked(after, pointer)
        ), "Change outside selected field"
    else:
        assert action["surface"]["selector"] == "body"
        baseline.read_text(encoding="utf-8")
        payload.read_text(encoding="utf-8")
    return action


SKILLS = {
    "attack-document-injection",
    "attack-tool-response-injection",
    "attack-authority-spoofing",
    "attack-workflow-hijack",
    "attack-context-poisoning",
    "attack-canary-exfiltration",
    "compose-attack-actions",
}


def validate_package(root):
    package = read_json(root / "package.json")
    codex = read_json(root / ".codex-plugin/plugin.json")
    read_json(root / ".claude-plugin/plugin.json")
    assert package["pi"] == {"skills": ["./skills"]}
    assert codex["skills"] == "./skills/" and codex["name"] == root.name
    assert package["version"] == codex["version"]
    assert not any(k in package for k in ("dependencies", "scripts", "bin"))
    assert not any(k in codex for k in ("hooks", "mcpServers", "apps"))
    assert not any(
        (root / p).exists() for p in ("extensions", "hooks", ".mcp.json", ".app.json")
    )
    assert {p.parent.name for p in (root / "skills").glob("*/SKILL.md")} == SKILLS
    for path in root.rglob("*.json"):
        read_json(path)
    for path in root.rglob("*.md"):
        for target in re.findall(r"\]\(([^)]+)\)", path.read_text()):
            if "://" in target or target.startswith("#"):
                continue
            assert (path.parent / target.split("#")[0]).is_file(), (path, target)
    actions = [
        validate_action(p.parent) for p in (root / "examples").glob("*/action.json")
    ]
    assert len(actions) >= 3
    parent = root / "examples/tool-marker"
    child = root / "examples/tool-marker-composed"
    validate_composition(parent, child)


def validate_composition(parent, child):
    a, b = validate_action(parent), validate_action(child)
    assert a["action_id"] != b["action_id"] and b["parent_action_id"] == a["action_id"]
    for field in ("benign_task", "carrier", "surface", "objective"):
        assert a[field] == b[field], field
    assert b["techniques"][: len(a["techniques"])] == a["techniques"]
    assert (parent / a["baseline_path"]).read_bytes() == (
        child / b["baseline_path"]
    ).read_bytes()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--action",
        type=Path,
        help="Validate a generated action directory instead of package",
    )
    parser.add_argument(
        "--parent", type=Path, help="Also compare this parent action with --action"
    )
    args = parser.parse_args()
    if args.action:
        validate_action(args.action)
        if args.parent:
            validate_composition(args.parent, args.action)
    else:
        assert args.parent is None, "--parent requires --action"
        validate_package(Path(__file__).resolve().parents[1])
    print("PASS: structural checks (no victim execution or attack-effect claim)")
