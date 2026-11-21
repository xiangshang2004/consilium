from __future__ import annotations

import os
import re
import shutil
import unittest

from consilium.dashboard import render_dashboard_html

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class R016RegressionsTests(unittest.TestCase):
    """Claude takeover round: brand mark, 会诊 wording, narrow-screen menu, app deep-link allowlist."""

    def setUp(self) -> None:
        self.html = render_dashboard_html("0.1.0")

    def test_brand_mark_and_favicon_are_inline_and_offline(self) -> None:
        self.assertIn('<span class="brand-mark"><svg', self.html)
        self.assertNotIn('<span class="brand-mark">C</span>', self.html)
        self.assertIn('<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,', self.html)
        self.assertNotIn("http://", self.html)
        self.assertNotIn("https://", self.html)

    def test_user_facing_wording_uses_huizhen_not_mission(self) -> None:
        self.assertNotIn("关闭 Mission", self.html)
        self.assertIn("关闭会诊", self.html)

    def test_narrow_screen_more_menu_wraps_theme_tabs_and_logout(self) -> None:
        self.assertIn('id="more-btn"', self.html)
        start = self.html.index('<div id="topbar-more" class="topbar-more">')
        end = self.html.index("</header>", start)
        block = self.html[start:end]
        for marker in ('data-set-theme="light"', 'id="top-drawer-tabs"', 'id="logout-btn"'):
            self.assertIn(marker, block)
        # R017: the ⋯ menu is the only top-bar menu at every width (desktop-only product).
        self.assertRegex(self.html, r"\.topbar-more\.open\s*\{\s*display:\s*flex;")
        self.assertNotIn('class="icon-btn show-narrow"', self.html)

    def test_app_only_opens_verified_deep_links(self) -> None:
        path = os.path.join(REPO_ROOT, "app", "macos", "Sources", "MainWindowController.swift")
        with open(path, "r", encoding="utf-8") as handle:
            src = handle.read()
        self.assertIn('scheme == "claude" && host == "code" && path == "/new"', src)
        self.assertIn('scheme == "codex" && host == "new"', src)
        self.assertTrue(re.search(r"if allowed \{\s*NSWorkspace\.shared\.open\(url\)", src))

    def test_app_is_a_regular_dock_app_that_opens_its_window(self) -> None:
        with open(os.path.join(REPO_ROOT, "app", "macos", "Info.plist"), "r", encoding="utf-8") as handle:
            self.assertNotIn("LSUIElement", handle.read())
        src_dir = os.path.join(REPO_ROOT, "app", "macos", "Sources")
        with open(os.path.join(src_dir, "AppDelegate.swift"), "r", encoding="utf-8") as handle:
            delegate = handle.read()
        with open(os.path.join(src_dir, "Config.swift"), "r", encoding="utf-8") as handle:
            config = handle.read()
        self.assertIn("NSApp.setActivationPolicy(.regular)", delegate)
        self.assertNotIn(".accessory", delegate)
        self.assertIn("applicationShouldHandleReopen", delegate)
        self.assertIn("NSApp.mainMenu = mainMenu", delegate)
        self.assertIn("var openOnLaunch = true", config)

    def test_installer_detects_claude_app_when_pgrep_misses_it(self) -> None:
        import subprocess
        import sys

        sys.path.insert(0, REPO_ROOT)
        from scripts import install as inst

        def fake_runner(cmd, **_kwargs):
            if cmd[0] == "pgrep":
                return subprocess.CompletedProcess(cmd, 1, "", "")
            return subprocess.CompletedProcess(
                cmd, 0, "/usr/sbin/cfprefsd\n" + inst.CLAUDE_DESKTOP_EXECUTABLE + "\n", ""
            )

        self.assertTrue(inst._is_claude_desktop_running(fake_runner))

        def none_runner(cmd, **_kwargs):
            if cmd[0] == "pgrep":
                return subprocess.CompletedProcess(cmd, 1, "", "")
            return subprocess.CompletedProcess(cmd, 0, "/usr/sbin/cfprefsd\n", "")

        self.assertFalse(inst._is_claude_desktop_running(none_runner))

    def test_subagent_templates_are_a_separate_install_target(self) -> None:
        import json
        import subprocess
        import sys
        import tempfile

        def plan(*targets: str) -> list:
            home = tempfile.mkdtemp(prefix="consilium-r016-")
            self.addCleanup(shutil.rmtree, home, True)
            env = dict(os.environ)
            env.pop("CLAUDE_CONFIG_DIR", None)
            out = subprocess.run(
                [sys.executable, "-B", os.path.join(REPO_ROOT, "scripts", "install.py"), "--dry-run",
                 "--target-home", home, "--rendezvous-dir", os.path.join(home, "rv"), "--targets", *targets],
                capture_output=True, text=True, check=True, env=env,
            ).stdout
            return [op["target"] for op in json.loads(out)["operations"]]

        base = plan("claude_code", "codex")
        self.assertIn("claude_code_mcp", base)
        self.assertIn("codex_mcp", base)
        self.assertFalse([t for t in base if t.startswith(("claude_agent:", "codex_agent:"))])
        agents = plan("claude_agents", "codex_agents")
        self.assertTrue([t for t in agents if t.startswith("claude_agent:")])
        self.assertTrue([t for t in agents if t.startswith("codex_agent:")])

    def test_codex_apply_then_uninstall_restores_file_and_removes_skill_dir(self) -> None:
        import subprocess
        import sys
        import tempfile

        for original in ('model = "x"\n', 'model = "x"\n\n', 'model = "x"'):
            home = tempfile.mkdtemp(prefix="consilium-r016-rt-")
            self.addCleanup(shutil.rmtree, home, True)
            os.makedirs(os.path.join(home, ".codex"))
            cfg = os.path.join(home, ".codex", "config.toml")
            with open(cfg, "w", encoding="utf-8") as handle:
                handle.write(original)
            env = dict(os.environ)
            env.pop("CLAUDE_CONFIG_DIR", None)
            base = [sys.executable, "-B", os.path.join(REPO_ROOT, "scripts", "install.py"),
                    "--target-home", home, "--rendezvous-dir", os.path.join(home, "rv"), "--targets", "codex"]
            subprocess.run(base + ["--apply"], capture_output=True, text=True, check=True, env=env)
            with open(cfg, "r", encoding="utf-8") as handle:
                self.assertIn("# BEGIN CONSILIUM MCP", handle.read())
            subprocess.run(base + ["--uninstall"], capture_output=True, text=True, check=True, env=env)
            # Byte-exact, except a file that lacked a final newline keeps the one added on install.
            expected = original if original.endswith("\n") else original + "\n"
            with open(cfg, "r", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), expected)
            self.assertFalse(os.path.exists(os.path.join(home, ".codex", "skills", "consilium-collab")))


if __name__ == "__main__":
    unittest.main()
