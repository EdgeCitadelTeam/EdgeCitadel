"""Regression checks for the fixture boundary validator."""

import json
import tempfile
import unittest
from pathlib import Path

from validate import canonical, masked, validate_action, validate_composition


class BoundaryTests(unittest.TestCase):
    def test_escaped_pointer_and_root_string(self):
        value = {"a/b": {"~note": "text"}, "other": [1, False, None]}
        self.assertEqual(
            masked(value, "/a~1b/~0note"),
            {"a/b": {"~note": None}, "other": [1, False, None]},
        )
        self.assertEqual(value["a/b"]["~note"], "text")
        self.assertIsNone(masked("root text", ""))

    def test_invalid_or_nonstring_surfaces(self):
        for value, pointer in [
            ({"x": 3}, "/x"),
            ({}, "/absent"),
            (["x"], "/01"),
            ({"x": "s"}, "/x~2"),
            ("x", "/a"),
        ]:
            with (
                self.subTest(pointer=pointer),
                self.assertRaises((AssertionError, KeyError, IndexError)),
            ):
                masked(value, pointer)

    def test_boolean_number_and_missing_are_not_equal(self):
        self.assertNotEqual(
            canonical(masked({"s": "x", "ok": True}, "/s")),
            canonical(masked({"s": "y", "ok": 1}, "/s")),
        )
        self.assertNotEqual(
            canonical(masked({"s": "x", "v": None}, "/s")),
            canonical(masked({"s": "y"}, "/s")),
        )

    def test_sibling_mutation_rejected(self):
        import shutil

        example = Path(__file__).resolve().parents[1] / "examples/tool-marker"
        with tempfile.TemporaryDirectory() as tmp:
            action_dir = Path(tmp) / "action"
            shutil.copytree(example, action_dir)
            validate_action(action_dir)
            path = action_dir / "payload.json"
            payload = json.loads(path.read_text())
            payload["isError"] = 0
            path.write_text(json.dumps(payload))
            with self.assertRaisesRegex(AssertionError, "outside selected field"):
                validate_action(action_dir)

    def test_changed_parent_baseline_rejected(self):
        import shutil

        examples = Path(__file__).resolve().parents[1] / "examples"
        with tempfile.TemporaryDirectory() as tmp:
            parent, child = Path(tmp) / "parent", Path(tmp) / "child"
            shutil.copytree(examples / "tool-marker", parent)
            shutil.copytree(examples / "tool-marker-composed", child)
            validate_composition(parent, child)
            with (child / "baseline.json").open("a") as f:
                f.write("\n")
            with self.assertRaises(AssertionError):
                validate_composition(parent, child)


if __name__ == "__main__":
    unittest.main()
