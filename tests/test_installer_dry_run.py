"""Tests for scripts/install.py dry-run, claude CLI sync, duplicate TOML guard, and safe uninstall."""

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
        os.environ["CONSILIUM_RENDEZVOUS_DIR"] = tempfile.mkdtemp(
            prefix="dbell-irv-", dir="/tmp"
        )

        gemini_cfg = os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json")
        os.makedirs(os.path.dirname(gemini_cfg), exist_ok=True)
        with open(gemini_cfg, "w", encoding="utf-8") as handle:
            json.dump({"mcpServers": {"keep-me": {"url": "http://127.0.0.1:9999/mcp"}}}, handle)

        codex_cfg = os.path.join(self.tmp_home, ".codex", "config.toml")
        os.makedirs(os.path.dirname(codex_cfg), exist_ok=True)
        with open(codex_cfg, "w", encoding="utf-8") as handle:
            handle.write('model = "gpt-5"\n')

    def tearDown(self) -> None:
        rv = os.environ.pop("CONSILIUM_RENDEZVOUS_DIR", None)
        if rv:
            shutil.rmtree(rv, ignore_errors=True)
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def test_dry_run_does_not_mutate_files(self) -> None:
        report = run_installer(
            mode="dry-run",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
            rendezvous_dir=os.environ["CONSILIUM_RENDEZVOUS_DIR"],
            claude_running_check=lambda: False,
        )
        self.assertEqual(report["mode"], "dry-run")
        self.assertEqual(report["backup_root"], "")
        self.assertFalse(os.path.exists(os.path.join(self.tmp_home, ".claude.json")))
        self.assertFalse(
            os.path.exists(
                os.path.join(self.tmp_home, "Library", "Application Support", "consilium", "auth.token")
            )
        )
        self.assertFalse(
            os.path.exists(
                os.path.join(self.tmp_home, "Library", "LaunchAgents", "com.up.consilium.plist")
            )
        )

    def test_apply_idempotency_token_paths_and_safe_uninstall(self) -> None:
        first = run_installer(
            mode="apply",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
            rendezvous_dir=os.environ["CONSILIUM_RENDEZVOUS_DIR"],
            claude_running_check=lambda: False,
        )
        self.assertTrue(os.path.isdir(first["backup_root"]))
        token_path = os.path.join(
            self.tmp_home, "Library", "Application Support", "consilium", "auth.token"
        )
        self.assertTrue(os.path.exists(token_path))
        with open(token_path, "r", encoding="utf-8") as handle:
            token = handle.read().strip()
        self.assertTrue(os.path.isdir(os.path.join(self.tmp_home, "Library", "Logs", "consilium")))

        gemini_cfg = os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json")
        with open(gemini_cfg, "r", encoding="utf-8") as handle:
            gemini_raw = handle.read()
            gemini_data = json.loads(gemini_raw)
        self.assertNotIn("?token=", gemini_raw)
        self.assertIn("keep-me", gemini_data["mcpServers"])
        self.assertEqual(
            gemini_data["mcpServers"]["consilium"],
            {
                "url": "http://127.0.0.1:18920/mcp",
                "headers": {"Authorization": f"Bearer {token}"},
            },
        )

        # Claude Code config written via `claude mcp add-json -s user`
        claude_json = os.path.join(self.tmp_home, ".claude.json")
        self.assertTrue(os.path.exists(claude_json))
        with open(claude_json, "r", encoding="utf-8") as handle:
            claude_data = json.load(handle)
        self.assertEqual(
            claude_data["mcpServers"]["consilium"],
            {
                "type": "http",
                "url": "http://127.0.0.1:18920/mcp",
                "headers": {"Authorization": f"Bearer {token}"},
            },
        )

        # Codex TOML includes tool_timeout_sec and http_headers
        codex_cfg = os.path.join(self.tmp_home, ".codex", "config.toml")
        with open(codex_cfg, "r", encoding="utf-8") as handle:
            toml_text = handle.read()
        self.assertIn('model = "gpt-5"', toml_text)
        self.assertIn("[mcp_servers.consilium]", toml_text)
        self.assertIn("tool_timeout_sec = 330", toml_text)
        self.assertIn(f'http_headers = {{ Authorization = "Bearer {token}" }}', toml_text)

        # Claude agent templates: opus only, no sonnet/haiku
        agents_dir = os.path.join(self.tmp_home, ".claude", "agents")
        agent_files = sorted(os.listdir(agents_dir))
        self.assertEqual(
            agent_files,
            ["opus-architect.md", "opus-builder.md", "opus-scout.md"],
        )

        # launchd plist points logs to ~/Library/Logs/consilium and passes --require-token
        plist_path = os.path.join(
            self.tmp_home, "Library", "LaunchAgents", "com.up.consilium.plist"
        )
        with open(plist_path, "r", encoding="utf-8") as handle:
            plist_text = handle.read()
        self.assertIn("--require-token", plist_text)
        self.assertIn("<key>ThrottleInterval</key>", plist_text)
        self.assertIn("Library/Logs/consilium/consilium.out.log", plist_text)

        # Second apply must be completely idempotent.
        second = run_installer(
            mode="apply",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
            rendezvous_dir=os.environ["CONSILIUM_RENDEZVOUS_DIR"],
            claude_running_check=lambda: False,
        )
        self.assertTrue(all(op["action"] == "unchanged" for op in second["operations"]))

        # Modify claude skill file to verify --uninstall skips user-modified files.
        claude_skill = os.path.join(
            self.tmp_home, ".claude", "skills", "consilium-collab", "SKILL.md"
        )
        with open(claude_skill, "a", encoding="utf-8") as handle:
            handle.write("\n# User customization\n")

        uninstall_report = run_installer(
            mode="uninstall",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
            rendezvous_dir=os.environ["CONSILIUM_RENDEZVOUS_DIR"],
            claude_running_check=lambda: False,
        )
        ops_by_target = {op["target"]: op for op in uninstall_report["operations"]}
        self.assertEqual(ops_by_target["claude_skill"]["action"], "skipped_modified")
        self.assertTrue(os.path.exists(claude_skill))

        with open(gemini_cfg, "r", encoding="utf-8") as handle:
            gemini_after = json.load(handle)
        self.assertIn("keep-me", gemini_after["mcpServers"])
        self.assertNotIn("consilium", gemini_after["mcpServers"])

        with open(claude_json, "r", encoding="utf-8") as handle:
            claude_after = json.load(handle)
        self.assertNotIn("consilium", claude_after.get("mcpServers", {}))

        with open(codex_cfg, "r", encoding="utf-8") as handle:
            toml_after = handle.read()
        self.assertIn('model = "gpt-5"', toml_after)
        self.assertNotIn("[mcp_servers.consilium]", toml_after)
        self.assertFalse(os.path.exists(plist_path))

    def test_duplicate_codex_toml_table_outside_block_rejected(self) -> None:
        codex_cfg = os.path.join(self.tmp_home, ".codex", "config.toml")
        with open(codex_cfg, "a", encoding="utf-8") as handle:
            handle.write('\n[mcp_servers.consilium]\nurl = "http://127.0.0.1:9999/mcp"\n')
        with self.assertRaises(ValueError):
            run_installer(
                mode="dry-run",
                target_home=self.tmp_home,
                repo_dir=self.repo_dir,
                port=18920,
                rendezvous_dir=os.environ["CONSILIUM_RENDEZVOUS_DIR"],
                claude_running_check=lambda: False,
            )


if __name__ == "__main__":
    unittest.main()
