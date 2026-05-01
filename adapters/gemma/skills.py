"""Skill loading + dispatch + structured output validation."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional

import yaml
from jsonschema import Draft202012Validator, ValidationError as SchemaError


class SkillError(Exception):
    """Raised when skill dispatch or structured output validation fails.

    Caller maps to task_state='rejected' (unknown_skill) or
    'failed' (structured_output_validation_failed) per the spec.
    """


def load_skills(config_path: str | Path) -> dict[str, dict]:
    """Read config.yaml; return {skill_id → skill_dict}.

    Skills are dicts as written in YAML: id, name, description, tags,
    system_prompt, defaults, output_shape (string 'free_text' or JSON dict).
    """
    cfg = yaml.safe_load(Path(config_path).read_text())
    skills_list = cfg.get("skills", [])
    return {s["id"]: s for s in skills_list}


def dispatch_skill(skills: dict[str, dict], skill_id: Optional[str]) -> dict:
    """Resolve skill_id → skill dict.

    Falls back to 'reasoning.chat' if skill_id is None/empty (back-compat
    with Phase 2 callers). Raises SkillError on unknown skill_id."""
    sid = skill_id or "reasoning.chat"
    skill = skills.get(sid)
    if skill is None:
        raise SkillError(f"unknown_skill: {sid}")
    return skill


def validate_structured_output(raw: str, schema: dict) -> dict:
    """Parse + validate an LLM JSON response against the skill's schema.

    Raises SkillError with 'validation' in the message on parse or
    schema violation. Returns the parsed object on success."""
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SkillError(
            f"structured_output_validation_failed: invalid JSON ({e})"
        ) from e
    try:
        Draft202012Validator(schema).validate(parsed)
    except SchemaError as e:
        raise SkillError(
            f"structured_output_validation_failed: {e.message}"
        ) from e
    return parsed
