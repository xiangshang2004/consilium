"""Safe, idempotent installer for deskbell desktop MCP configs, skills, agents, and launchd."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple

BEGIN_MARKER = "# BEGIN DESKBELL MCP"
END_MARKER = "# END DESKBELL MCP"


def _timestamp_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _backup_file(target_path: str, home_dir: str, backup_root: str) -> Optional[str]:
    if not os.path.exists(target_path):
        return None
    rel = os.path.relpath(target_path, home_dir)
    dest = os.path.join(backup_root, rel)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(target_path, dest)
    return dest


def _load_json_dict(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        raw = handle.read().strip()
    if not raw:
        return {}
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}, found {type(data).__name__}")
    return data


def _update_json_mcp(
    path: str,
    entry: Dict[str, Any],
    *,
    remove: bool,
    apply_changes: bool,
    home_dir: str,
    backup_root: str,
) -> Tuple[str, str]:
    exists = os.path.exists(path)
    if remove and not exists:
        return "unchanged", "not present"

    data = _load_json_dict(path)
    servers = data.get("mcpServers")
    if servers is None:
        servers = {}
    elif not isinstance(servers, dict):
        raise ValueError(f"Expected 'mcpServers' object in {path}")

    current = servers.get("deskbell")
    if remove:
        if "deskbell" not in servers:
            return "unchanged", "deskbell key absent"
        del servers["deskbell"]
        if servers:
            data["mcpServers"] = servers
        elif "mcpServers" in data:
            del data["mcpServers"]
        action = "remove"
    else:
        if current == entry:
            return "unchanged", "already configured"
        new_servers = dict(servers)
        new_servers["deskbell"] = entry
        data["mcpServers"] = new_servers
        action = "update" if exists else "create"

    if apply_changes:
        _backup_file(path, home_dir, backup_root)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
    return action, "deskbell entry " + ("removed" if remove else "configured")


def _render_codex_block(port: int) -> str:
    return (
        f"{BEGIN_MARKER}\n"
        f"# Verify exact Codex config.toml MCP table key against your Codex Desktop build.\n"
        f"[mcp_servers.deskbell]\n"
        f'url = "http://127.0.0.1:{port}/mcp"\n'
        f"{END_MARKER}\n"
    )


def _update_codex_toml(
    path: str,
    port: int,
    *,
    remove: bool,
    apply_changes: bool,
    home_dir: str,
    backup_root: str,
) -> Tuple[str, str]:
    exists = os.path.exists(path)
    if remove and not exists:
        return "unchanged", "not present"

    original = ""
    if exists:
        with open(path, "r", encoding="utf-8") as handle:
            original = handle.read()

    pattern = re.compile(
        re.escape(BEGIN_MARKER) + r".*?" + re.escape(END_MARKER) + r"\n?",
        re.DOTALL,
    )
    if remove:
        updated = pattern.sub("", original)
        if updated == original:
            return "unchanged", "managed block absent"
        action = "remove"
    else:
        block = _render_codex_block(port)
        if pattern.search(original):
            updated = pattern.sub(block, original)
        else:
            sep = "" if (not original or original.endswith("\n")) else "\n"
            gap = "\n" if (original and not original.endswith("\n\n")) else ""
            updated = original + sep + gap + block
        if updated == original:
            return "unchanged", "already configured"
        action = "update" if exists else "create"

    if apply_changes:
        _backup_file(path, home_dir, backup_root)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(updated)
    return action, "managed TOML block " + ("removed" if remove else "configured")


def _sync_text_file(
    dest_path: str,
    content: str,
    *,
    remove: bool,
    apply_changes: bool,
    home_dir: str,
    backup_root: str,
) -> Tuple[str, str]:
    exists = os.path.exists(dest_path)
    if remove:
        if not exists:
            return "unchanged", "not present"
        if apply_changes:
            _backup_file(dest_path, home_dir, backup_root)
            os.remove(dest_path)
        return "remove", "file removed"

    if exists:
        with open(dest_path, "r", encoding="utf-8") as handle:
            if handle.read() == content:
                return "unchanged", "up to date"
        action = "update"
    else:
        action = "create"

    if apply_changes:
        _backup_file(dest_path, home_dir, backup_root)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "w", encoding="utf-8") as handle:
            handle.write(content)
    return action, "synced"


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def run_installer(
    *,
    mode: str,
    target_home: str,
    repo_dir: str,
    port: int,
) -> Dict[str, Any]:
    """Plan, check, apply, or uninstall deskbell configs under target_home."""
    home_dir = os.path.abspath(os.path.expanduser(target_home))
    resolved_repo = os.path.abspath(repo_dir)
    data_dir = f"/tmp/deskbell-{os.getuid()}"
    backup_root = os.path.join(home_dir, ".deskbell", "backups", _timestamp_tag())

    apply_changes = mode in ("apply", "uninstall")
    remove = mode == "uninstall"

    operations: List[Dict[str, str]] = []

    def _record(label: str, path: str, outcome: Tuple[str, str]) -> None:
        operations.append(
            {"target": label, "path": path, "action": outcome[0], "detail": outcome[1]}
        )

    def _safe_op(label: str, path: str, fn: Any) -> None:
        try:
            _record(label, path, fn())
        except PermissionError as exc:
            if apply_changes:
                raise
            _record(label, path, ("permission_denied", f"unreadable in current sandbox: {exc}"))

    http_entry = {"url": f"http://127.0.0.1:{port}/mcp"}
    claude_code_http_entry = {"type": "http", "url": f"http://127.0.0.1:{port}/mcp"}
    claude_desktop_stdio_entry = {
        "command": "/usr/bin/python3",
        "args": ["-m", "deskbell.cli", "mcp-stdio", "--peer", "claude", "--data-dir", data_dir],
        "env": {"PYTHONPATH": resolved_repo},
    }

    _safe_op(
        "antigravity_mcp",
        os.path.join(home_dir, ".gemini", "config", "mcp_config.json"),
        lambda: _update_json_mcp(
            os.path.join(home_dir, ".gemini", "config", "mcp_config.json"),
            http_entry,
            remove=remove,
            apply_changes=apply_changes,
            home_dir=home_dir,
            backup_root=backup_root,
        ),
    )
    _safe_op(
        "claude_desktop_mcp",
        os.path.join(home_dir, "Library", "Application Support", "Claude", "claude_desktop_config.json"),
        lambda: _update_json_mcp(
            os.path.join(home_dir, "Library", "Application Support", "Claude", "claude_desktop_config.json"),
            claude_desktop_stdio_entry,
            remove=remove,
            apply_changes=apply_changes,
            home_dir=home_dir,
            backup_root=backup_root,
        ),
    )
    _safe_op(
        "claude_code_mcp",
        os.path.join(home_dir, ".claude.json"),
        lambda: _update_json_mcp(
            os.path.join(home_dir, ".claude.json"),
            claude_code_http_entry,
            remove=remove,
            apply_changes=apply_changes,
            home_dir=home_dir,
            backup_root=backup_root,
        ),
    )
    _safe_op(
        "codex_mcp",
        os.path.join(home_dir, ".codex", "config.toml"),
        lambda: _update_codex_toml(
            os.path.join(home_dir, ".codex", "config.toml"),
            port,
            remove=remove,
            apply_changes=apply_changes,
            home_dir=home_dir,
            backup_root=backup_root,
        ),
    )

    skill_src = os.path.join(resolved_repo, "adapters", "skill", "deskbell-collab", "SKILL.md")
    skill_content = _read_text(skill_src)
    for label, skill_dest in (
        ("antigravity_skill", os.path.join(home_dir, ".gemini", "config", "skills", "deskbell-collab", "SKILL.md")),
        ("claude_skill", os.path.join(home_dir, ".claude", "skills", "deskbell-collab", "SKILL.md")),
        ("codex_skill", os.path.join(home_dir, ".codex", "skills", "deskbell-collab", "SKILL.md")),
    ):
        _safe_op(
            label,
            skill_dest,
            lambda d=skill_dest: _sync_text_file(
                d,
                skill_content,
                remove=remove,
                apply_changes=apply_changes,
                home_dir=home_dir,
                backup_root=backup_root,
            ),
        )

    claude_agents_dir = os.path.join(resolved_repo, "adapters", "claude", "agents")
    for fname in sorted(os.listdir(claude_agents_dir)):
        if fname.endswith(".md"):
            dest = os.path.join(home_dir, ".claude", "agents", fname)
            content = _read_text(os.path.join(claude_agents_dir, fname))
            _safe_op(
                f"claude_agent:{fname}",
                dest,
                lambda d=dest, c=content: _sync_text_file(
                    d,
                    c,
                    remove=remove,
                    apply_changes=apply_changes,
                    home_dir=home_dir,
                    backup_root=backup_root,
                ),
            )

    codex_agents_dir = os.path.join(resolved_repo, "adapters", "codex", "agents")
    for fname in sorted(os.listdir(codex_agents_dir)):
        if fname.endswith(".toml"):
            dest = os.path.join(home_dir, ".codex", "agents", fname)
            content = _read_text(os.path.join(codex_agents_dir, fname))
            _safe_op(
                f"codex_agent:{fname}",
                dest,
                lambda d=dest, c=content: _sync_text_file(
                    d,
                    c,
                    remove=remove,
                    apply_changes=apply_changes,
                    home_dir=home_dir,
                    backup_root=backup_root,
                ),
            )

    plist_tmpl = _read_text(
        os.path.join(resolved_repo, "adapters", "launchd", "com.up.deskbell.plist.tmpl")
    )
    plist_rendered = (
        plist_tmpl.replace("__PORT__", str(port))
        .replace("__DATA_DIR__", data_dir)
        .replace("__REPO_DIR__", resolved_repo)
    )
    plist_dest = os.path.join(home_dir, "Library", "LaunchAgents", "com.up.deskbell.plist")
    _safe_op(
        "launchd_plist",
        plist_dest,
        lambda: _sync_text_file(
            plist_dest,
            plist_rendered,
            remove=remove,
            apply_changes=apply_changes,
            home_dir=home_dir,
            backup_root=backup_root,
        ),
    )

    return {
        "mode": mode,
        "target_home": home_dir,
        "repo_dir": resolved_repo,
        "port": port,
        "backup_root": backup_root if (apply_changes and os.path.exists(backup_root)) else "",
        "operations": operations,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Install or inspect deskbell configurations (defaults to --dry-run)."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", help="Show planned changes without writing (default).")
    group.add_argument("--check", action="store_true", help="Inspect current configuration status without writing.")
    group.add_argument("--apply", action="store_true", help="Write configs/templates with timestamped backups.")
    group.add_argument("--uninstall", action="store_true", help="Remove deskbell config entries and installed files.")
    parser.add_argument("--target-home", default=os.path.expanduser("~"))
    parser.add_argument(
        "--repo-dir",
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    parser.add_argument("--port", type=int, default=18920)
    args = parser.parse_args(argv)

    if args.apply:
        mode = "apply"
    elif args.uninstall:
        mode = "uninstall"
    elif args.check:
        mode = "check"
    else:
        mode = "dry-run"

    try:
        report = run_installer(
            mode=mode,
            target_home=args.target_home,
            repo_dir=args.repo_dir,
            port=args.port,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
