"""Tests for public skills.sh Agent Skill packaging and internal skill hiding."""

from __future__ import annotations

import os
import re
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read_frontmatter(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    if not match:
        raise AssertionError(f"Missing YAML frontmatter in {path}")
    return match.group(1)


class SkillPackagingTests(unittest.TestCase):
    def test_public_consilium_skill_structure_and_frontmatter(self) -> None:
        skill_dir = os.path.join(REPO_ROOT, "skills", "consilium")
        skill_md = os.path.join(skill_dir, "SKILL.md")
        openai_yaml = os.path.join(skill_dir, "agents", "openai.yaml")
        setup_ref = os.path.join(skill_dir, "references", "setup-and-install.md")
        trouble_ref = os.path.join(skill_dir, "references", "troubleshooting.md")

        for path in (skill_md, openai_yaml, setup_ref, trouble_ref):
            self.assertTrue(os.path.isfile(path), f"Missing expected skill file: {path}")
            self.assertGreater(os.path.getsize(path), 0, f"Empty skill file: {path}")

        fm = _read_frontmatter(skill_md)
        self.assertRegex(fm, r"(?m)^name:\s*consilium\s*$")
        self.assertRegex(fm, r"(?m)^description:\s*\S+")
        self.assertRegex(fm, r"(?m)^license:\s*CC-BY-NC-SA-4\.0\s*$")
        self.assertRegex(fm, r"(?m)^\s*author:\s*xiangshang2004\s*$")

        with open(skill_md, "r", encoding="utf-8") as handle:
            body = handle.read()
        self.assertIn("references/setup-and-install.md", body)
        self.assertIn("references/troubleshooting.md", body)
        self.assertNotIn("press Return", body)
        self.assertNotIn("按 Return", body)

    def test_internal_openspec_skills_marked_internal(self) -> None:
        agents_skills_dir = os.path.join(REPO_ROOT, ".agents", "skills")
        entries = sorted(
            d
            for d in os.listdir(agents_skills_dir)
            if os.path.isdir(os.path.join(agents_skills_dir, d))
        )
        self.assertGreaterEqual(len(entries), 1)
        for name in entries:
            skill_md = os.path.join(agents_skills_dir, name, "SKILL.md")
            self.assertTrue(os.path.isfile(skill_md), f"Missing {skill_md}")
            fm = _read_frontmatter(skill_md)
            self.assertRegex(
                fm,
                r"(?m)^\s*internal:\s*true\s*$",
                f"Expected metadata.internal: true in {skill_md}",
            )

    def test_legacy_installer_collab_skill_preserved(self) -> None:
        legacy = os.path.join(
            REPO_ROOT, "adapters", "skill", "consilium-collab", "SKILL.md"
        )
        self.assertTrue(os.path.isfile(legacy))
