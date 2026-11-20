from __future__ import annotations

import os
import re
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
        narrow = self.html[self.html.index("@media (max-width: 860px)"):]
        self.assertRegex(narrow, r"\.topbar-more\.open\s*\{\s*display:\s*flex;")
        self.assertRegex(narrow, r"\.peer-pills-bar\s*\{[^}]*flex-wrap:\s*nowrap")

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


if __name__ == "__main__":
    unittest.main()
