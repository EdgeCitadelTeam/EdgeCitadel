"""Unit tests for skill loading + dispatch + structured output validation."""

import pytest
import edgecitadel_gemma_plugin

from edgecitadel_gemma_plugin.skills import (
    load_skills,
    dispatch_skill,
    validate_structured_output,
    SkillError,
)


@pytest.fixture(scope="module")
def skills():
    from pathlib import Path

    return load_skills(
        Path(edgecitadel_gemma_plugin.__file__).resolve().parent / "config.yaml"
    )


def test_load_skills_returns_four_known_skills(skills):
    assert set(skills) == {
        "reasoning.chat",
        "text.summarize",
        "text.classify",
        "code.explain",
    }


def test_loaded_skill_has_system_prompt(skills):
    assert skills["reasoning.chat"]["system_prompt"].startswith("You are Gemma")


def test_classify_skill_has_json_output_shape(skills):
    out = skills["text.classify"]["output_shape"]
    assert out["type"] == "json"
    assert out["schema"]["required"] == ["label", "confidence"]


def test_chat_skill_defaults_to_free_text(skills):
    out = skills["reasoning.chat"]["output_shape"]
    assert out == "free_text"


def test_dispatch_skill_returns_known(skills):
    s = dispatch_skill(skills, "text.summarize")
    assert s["id"] == "text.summarize"


def test_dispatch_skill_default_to_chat(skills):
    s = dispatch_skill(skills, None)
    assert s["id"] == "reasoning.chat"


def test_dispatch_skill_unknown_raises(skills):
    with pytest.raises(SkillError) as excinfo:
        dispatch_skill(skills, "text.unknown")
    assert "unknown_skill" in str(excinfo.value)


def test_validate_structured_output_passes(skills):
    out = skills["text.classify"]["output_shape"]
    parsed = validate_structured_output(
        '{"label":"spam","confidence":0.9}', out["schema"]
    )
    assert parsed["label"] == "spam"


def test_validate_structured_output_invalid_json(skills):
    out = skills["text.classify"]["output_shape"]
    with pytest.raises(SkillError) as excinfo:
        validate_structured_output("not json", out["schema"])
    assert "validation" in str(excinfo.value).lower()


def test_validate_structured_output_schema_violation(skills):
    out = skills["text.classify"]["output_shape"]
    with pytest.raises(SkillError):
        validate_structured_output('{"label":"spam"}', out["schema"])
