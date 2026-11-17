"""Tests for scripts/install.py dry-run safety, idempotent apply, backups, and uninstall."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest

from scripts.install import run_installer


class InstallerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_home = tempfile.mkdtemp(prefix="dbell-inst-", dir="/tmp")
        self.repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Seed existing configs that must be preserved untouched around deskbell entries.
        gemini_cfg = os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json")
        os.makedirs(os.path.dirname(gemini_cfg), exist_ok=True)
        with open(gemini_cfg, "w", encoding="utf-8") as handle:
            json.dump({"mcpServers": {"keep-me": {"url": "http://127.0.0.1:9999/mcp"}}}, handle)

        codex_cfg = os.path.join(self.tmp_home, ".codex", "config.toml")
        os.makedirs(os.path.dirname(codex_cfg), exist_ok=True)
        with open(codex_cfg, "w", encoding="utf-8") as handle:
            handle.write('model = "gpt-5"\n')

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def test_dry_run_does_not_mutate_files(self) -> None:
        report = run_installer(
            mode="dry-run",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
        )
        self.assertEqual(report["mode"], "dry-run")
        self.assertEqual(report["backup_root"], "")
        self.assertFalse(os.path.exists(os.path.join(self.tmp_home, ".claude.json")))
        self.assertFalse(
            os.path.exists(
                os.path.join(self.tmp_home, "Library", "LaunchAgents", "com.up.deskbell.plist")
            )
        )

    def test_apply_idempotency_backup_and_uninstall(self) -> None:
        first = run_installer(
            mode="apply",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
        )
        self.assertTrue(os.path.isdir(first["backup_root"]))

        gemini_cfg = os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json")
        with open(gemini_cfg, "r", encoding="utf-8") as handle:
            gemini_data = json.load(handle)
        self.assertIn("keep-me", gemini_data["mcpServers"])
        self.assertIn("deskbell", gemini_data["mcpServers"])

        codex_cfg = os.path.join(self.tmp_home, ".codex", "config.toml")
        with open(codex_cfg, "r", encoding="utf-8") as handle:
            toml_text = handle.read()
        self.assertIn('model = "gpt-5"', toml_text)
        self.assertIn("[mcp_servers.deskbell]", toml_text)

        # Second apply must be completely idempotent.
        second = run_installer(
            mode="apply",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
        )
        self.assertTrue(all(op["action"] == "unchanged" for op in second["operations"]))

        # Uninstall must remove deskbell while preserving user configs.
        run_installer(
            mode="uninstall",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
        )
        with open(gemini_cfg, "r", encoding="utf-8") as handle:
            gemini_after = json.load(handle)
        self.assertIn("keep-me", gemini_after["mcpServers"])
        self.assertNotIn("deskbell", gemini_after["mcpServers"])

        with open(codex_cfg, "r", encoding="utf-8") as handle:
            toml_after = handle.read()
        self.assertIn('model = "gpt-5"', toml_after)
        self.assertNotIn("[mcp_servers.deskbell]", toml_after)
        self.assertFalse(
            os.path.exists(
                os.path.join(self.tmp_home, "Library", "LaunchAgents", "com.up.deskbell.plist")
            )
        )


if __name__ == "__main__":
    unittest.main()
