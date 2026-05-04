"""Tests for parse-manifest.py.

Run from repo root: python3 -m pytest deploy/tests/test_parse_manifest.py -v
(or: python3 deploy/tests/test_parse_manifest.py if pytest unavailable)
"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PARSER = REPO_ROOT / "deploy" / "lib" / "parse-manifest.py"


class TestParseManifest(unittest.TestCase):
    def _run(self, args, manifest_text=None):
        if manifest_text is None:
            manifest_path = REPO_ROOT / "deploy" / "manifest.toml"
        else:
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".toml", delete=False
            )
            tmp.write(manifest_text)
            tmp.close()
            manifest_path = Path(tmp.name)
        result = subprocess.run(
            [sys.executable, str(PARSER), "--manifest", str(manifest_path), *args],
            capture_output=True, text=True,
        )
        return result

    def test_get_python_version(self):
        r = self._run(["get", "python.version"])
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), "3.12")

    def test_get_ollama_models_as_json(self):
        r = self._run(["get", "ollama.models", "--format", "json"])
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout), ["gemma3:4b"])

    def test_get_apt_packages_as_lines(self):
        r = self._run(["get", "apt_packages.common", "--format", "lines"])
        self.assertEqual(r.returncode, 0, r.stderr)
        lines = [ln for ln in r.stdout.splitlines() if ln]
        # apt_packages.common contains the non-python deps:
        self.assertIn("jq", lines)
        self.assertIn("sqlite3", lines)
        self.assertIn("cron", lines)

    def test_get_adapters_enabled(self):
        r = self._run(["get", "adapters.enabled", "--format", "lines"])
        self.assertEqual(r.returncode, 0, r.stderr)
        lines = [ln for ln in r.stdout.splitlines() if ln]
        self.assertEqual(set(lines), {"gemma", "watchdog"})

    def test_missing_key_exits_nonzero(self):
        r = self._run(["get", "nonexistent.key"])
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("not found", r.stderr.lower())

    def test_malformed_toml_exits_nonzero(self):
        r = self._run(
            ["get", "python.version"],
            manifest_text="[python\nversion = no quotes",
        )
        self.assertNotEqual(r.returncode, 0)


if __name__ == "__main__":
    unittest.main()
