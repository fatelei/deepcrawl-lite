import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CLITests(unittest.TestCase):
    def test_status_and_export_without_key(self):
        with tempfile.TemporaryDirectory() as folder:
            db = str(Path(folder) / "data.db")
            output = str(Path(folder) / "data.jsonl")
            env = {k: v for k, v in os.environ.items() if k != "DEEPSEEK_API_KEY"}
            for args in (["status", "--db", db], ["export", "--db", db, "--output", output]):
                result = subprocess.run([sys.executable, "-m", "deepcrawl", *args],
                                        env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIsInstance(json.loads(result.stdout), dict)
            self.assertEqual(Path(output).read_text(), "")

    def test_missing_key_is_actionable(self):
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run(
                [sys.executable, "-m", "deepcrawl", "crawl", "https://example.com", "--db", str(Path(folder) / "data.db")],
                env={k: v for k, v in os.environ.items() if k != "DEEPSEEK_API_KEY"},
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("DEEPSEEK_API_KEY", result.stderr)
            self.assertNotIn("Traceback", result.stderr)
