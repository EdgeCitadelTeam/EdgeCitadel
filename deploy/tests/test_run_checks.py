"""Tests for run-checks.py — uses synthetic checks.yaml in tempdir."""
import subprocess
import sys
import textwrap
import tempfile
import unittest
from pathlib import Path
import shutil

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "deploy" / "lib" / "run-checks.py"


class TestRunChecks(unittest.TestCase):
    def _run_with_checks_yaml(self, yaml_text):
        # Copy run-checks.py to a tempdir and pair it with a synthetic checks.yaml.
        tmp = Path(tempfile.mkdtemp())
        try:
            (tmp / "checks.yaml").write_text(yaml_text)
            shutil.copy(RUNNER, tmp / "run-checks.py")
            return subprocess.run(
                [sys.executable, str(tmp / "run-checks.py"), "--quiet"],
                capture_output=True, text=True
            )
        finally:
            shutil.rmtree(tmp)

    def test_all_pass(self):
        r = self._run_with_checks_yaml(textwrap.dedent("""
        categories:
          - name: TestCat
            checks:
              - name: trivial-true
                cmd: "true"
                remediation: "should never run"
        """))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("✓ 1 passed", r.stdout)

    def test_one_fail(self):
        r = self._run_with_checks_yaml(textwrap.dedent("""
        categories:
          - name: TestCat
            checks:
              - name: trivial-true
                cmd: "true"
              - name: trivial-false
                cmd: "false"
                remediation: "fix it"
        """))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("✗ 1 failed", r.stdout)

    def test_warn_does_not_fail_exit(self):
        r = self._run_with_checks_yaml(textwrap.dedent("""
        categories:
          - name: TestCat
            checks:
              - name: warning-only
                cmd: "false"
                level: warn
                remediation: "warn"
        """))
        self.assertEqual(r.returncode, 0, "warn-only should exit 0")
        self.assertIn("⚠ 1 warnings", r.stdout)


if __name__ == "__main__":
    unittest.main()
