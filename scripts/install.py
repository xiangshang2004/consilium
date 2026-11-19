"""Safe, idempotent, two-phase atomic installer & token rotator for consilium."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Callable, Dict, List, Optional, Tuple

REPO_DIR_DEFAULT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR_DEFAULT not in sys.path:
    sys.path.insert(0, REPO_DIR_DEFAULT)

from consilium.models import (
    ensure_auth_token,
    generate_auth_token,
    read_auth_token,
    write_auth_token,
)

BEGIN_MARKER = "# BEGIN CONSILIUM MCP"
END_MARKER = "# END CONSILIUM MCP"

CODEX_BLOCK_RE = re.compile(
    re.escape(BEGIN_MARKER) + r".*?" + re.escape(END_MARKER) + r"\n?",
    re.DOTALL,
)
CODEX_DUP_TABLE_RE = re.compile(
    r"""(?m)^\s*\[\s*mcp_servers\.(?:"consilium"|'consilium'|consilium)(?:\s*\.[^\]]+)?\s*\]\s*(?:#.*)?$"""
)
CODEX_DUP_DOTTED_RE = re.compile(
    r"""(?m)^\s*mcp_servers\.(?:"consilium"|'consilium'|consilium)\s*\.[^=\n]+="""
)


def _timestamp_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _atomic_write_text(path: str, content: str, mode: int = 0o600) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".consilium-tmp-", dir=parent)
    try:
        os.chmod(tmp_path, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


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
    servers = data.get("mcpServers")
    if servers is not None and not isinstance(servers, dict):
        raise ValueError(f"Expected 'mcpServers' object in {path}")
    return data


def _find_claude_cli() -> str:
    return shutil.which("claude") or "/opt/homebrew/bin/claude"


def _claude_subprocess_env(home_dir: str) -> Dict[str, str]:
    """Build an isolated env for `claude` CLI with CLAUDE_CONFIG_DIR stripped."""
    env = dict(os.environ)
    env.pop("CLAUDE_CONFIG_DIR", None)
    env["HOME"] = home_dir
    return env


def _is_claude_desktop_running() -> Optional[bool]:
    try:
        res = subprocess.run(
            ["pgrep", "-x", "Claude"], capture_output=True, text=True, check=False
        )
        return res.returncode == 0
    except OSError:
        return None


def _plan_json_mcp(
    path: str,
    entry: Dict[str, Any],
    *,
    remove: bool,
) -> Tuple[str, str, Optional[str]]:
    exists = os.path.exists(path)
    if remove and not exists:
        return "unchanged", "not present", None

    data = _load_json_dict(path)
    servers = dict(data.get("mcpServers") or {})
    current = servers.get("consilium")

    if remove:
        if "consilium" not in servers:
            return "unchanged", "consilium key absent", None
        del servers["consilium"]
        if servers:
            data["mcpServers"] = servers
        elif "mcpServers" in data:
            del data["mcpServers"]
        action = "remove"
    else:
        if current == entry:
            return "unchanged", "already configured", None
        servers["consilium"] = entry
        data["mcpServers"] = servers
        action = "update" if exists else "create"

    serialized = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    return action, "consilium entry " + ("removed" if remove else "configured"), serialized


def _render_codex_block(port: int, token: str) -> str:
    return (
        f"{BEGIN_MARKER}\n"
        f"[mcp_servers.consilium]\n"
        f'url = "http://127.0.0.1:{port}/mcp"\n'
        f"tool_timeout_sec = 330\n"
        f'http_headers = {{ Authorization = "Bearer {token}" }}\n'
        f"{END_MARKER}\n"
    )


def _check_codex_duplicates(outside_text: str, path: str) -> None:
    if CODEX_DUP_TABLE_RE.search(outside_text) or CODEX_DUP_DOTTED_RE.search(outside_text):
        raise ValueError(
            f"Duplicate consilium MCP table or dotted key exists outside managed block in {path}"
        )


def _plan_codex_toml(
    path: str,
    port: int,
    token: str,
    *,
    remove: bool,
) -> Tuple[str, str, Optional[str]]:
    exists = os.path.exists(path)
    if remove and not exists:
        return "unchanged", "not present", None

    original = ""
    if exists:
        with open(path, "r", encoding="utf-8") as handle:
            original = handle.read()

    outside = CODEX_BLOCK_RE.sub("", original)
    if not remove:
        _check_codex_duplicates(outside, path)

    if remove:
        updated = outside
        if updated == original:
            return "unchanged", "managed block absent", None
        action = "remove"
    else:
        block = _render_codex_block(port, token)
        if CODEX_BLOCK_RE.search(original):
            updated = CODEX_BLOCK_RE.sub(block, original)
        else:
            sep = "" if (not original or original.endswith("\n")) else "\n"
            gap = "\n" if (original and not original.endswith("\n\n")) else ""
            updated = original + sep + gap + block
        if updated == original:
            return "unchanged", "already configured", None
        action = "update" if exists else "create"

    return action, "managed TOML block " + ("removed" if remove else "configured"), updated


def _plan_claude_code_mcp(
    path: str,
    port: int,
    token: str,
    *,
    remove: bool,
) -> Tuple[str, str, Dict[str, Any], Optional[Dict[str, Any]]]:
    exists = os.path.exists(path)
    desired = {
        "type": "http",
        "url": f"http://127.0.0.1:{port}/mcp",
        "headers": {"Authorization": f"Bearer {token}"},
    }
    if remove and not exists:
        return "unchanged", "not present", desired, None

    data = _load_json_dict(path)
    servers = data.get("mcpServers") or {}
    current = servers.get("consilium")

    if remove:
        if "consilium" not in servers:
            return "unchanged", "consilium key absent", desired, None
        return "remove", "removed via claude mcp remove", desired, current

    if current == desired:
        return "unchanged", "already configured", desired, current
    action = "update" if exists else "create"
    return action, "configured via claude mcp add-json -s user", desired, current


def _apply_claude_code_mcp(
    path: str,
    desired: Dict[str, Any],
    previous_entry: Optional[Dict[str, Any]],
    *,
    remove: bool,
    home_dir: str,
    backup_root: str,
    claude_bin: str,
    runner: Any = subprocess.run,
) -> None:
    """Apply Claude Code MCP change via CLI with rollback if add fails after remove."""
    _backup_file(path, home_dir, backup_root)
    env = _claude_subprocess_env(home_dir)

    # Probe via `claude mcp get consilium` before removal (R005 3.7 item 3).
    if previous_entry is not None or remove:
        runner(
            [claude_bin, "mcp", "get", "consilium"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    removed_old = False
    if remove or previous_entry is not None:
        rm_res = runner(
            [claude_bin, "mcp", "remove", "-s", "user", "consilium"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if remove and rm_res.returncode != 0:
            raise RuntimeError(
                f"claude mcp remove failed ({rm_res.returncode}): {rm_res.stderr or rm_res.stdout}"
            )
        removed_old = rm_res.returncode == 0

    if not remove:
        payload_json = json.dumps(desired, ensure_ascii=False)
        add_res = runner(
            [claude_bin, "mcp", "add-json", "-s", "user", "consilium", payload_json],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if add_res.returncode != 0:
            # Roll back previous entry if we removed it before add failed.
            if removed_old and previous_entry is not None:
                runner(
                    [
                        claude_bin,
                        "mcp",
                        "add-json",
                        "-s",
                        "user",
                        "consilium",
                        json.dumps(previous_entry, ensure_ascii=False),
                    ],
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
            raise RuntimeError(
                f"claude mcp add-json failed ({add_res.returncode}): {add_res.stderr or add_res.stdout}"
            )


def _plan_text_file(
    dest_path: str,
    content: str,
    *,
    remove: bool,
) -> Tuple[str, str]:
    exists = os.path.exists(dest_path)
    if remove:
        if not exists:
            return "unchanged", "not present"
        with open(dest_path, "r", encoding="utf-8") as handle:
            existing = handle.read()
        if existing != content:
            return "skipped_modified", "file modified by user; skipped removal"
        return "remove", "file removed"

    if exists:
        with open(dest_path, "r", encoding="utf-8") as handle:
            if handle.read() == content:
                return "unchanged", "up to date"
        return "update", "synced"
    return "create", "synced"


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _peek_token(state_dir: str) -> str:
    token_path = os.path.join(state_dir, "auth.token")
    if os.path.exists(token_path):
        with open(token_path, "r", encoding="utf-8") as handle:
            existing = handle.read().strip()
        if existing:
            return existing
    return "<generated-on-apply>"


def run_installer(
    *,
    mode: str,
    target_home: str,
    repo_dir: str,
    port: int,
    rendezvous_dir: Optional[str] = None,
    claude_running_check: Optional[Callable[[], Optional[bool]]] = None,
    claude_runner: Any = subprocess.run,
) -> Dict[str, Any]:
    """Two-phase atomic installer: validate ALL targets before writing a single byte."""
    home_dir = os.path.abspath(os.path.expanduser(target_home))
    resolved_repo = os.path.abspath(repo_dir)
    state_dir = os.path.join(home_dir, "Library", "Application Support", "consilium")
    resolved_rv = (
        os.path.abspath(os.path.expanduser(rendezvous_dir))
        if rendezvous_dir
        else (os.environ.get("CONSILIUM_RENDEZVOUS_DIR") or f"/tmp/consilium-{os.getuid()}")
    )
    log_dir = os.path.join(home_dir, "Library", "Logs", "consilium")
    backup_root = os.path.join(state_dir, "backups", _timestamp_tag())

    apply_changes = mode in ("apply", "uninstall")
    remove = mode == "uninstall"

    # ---- PHASE 1: READ & VALIDATE EVERYTHING (ZERO WRITES!) ----
    running_fn = claude_running_check or _is_claude_desktop_running
    claude_running = running_fn()
    warnings: List[str] = []
    if claude_running:
        msg = (
            "Claude.app is currently running; quit Claude.app before applying "
            "claude_desktop_config.json changes."
        )
        if mode == "apply":
            raise RuntimeError(msg)
        warnings.append(msg)

    # Read template sources up front so missing files fail before any write.
    skill_content = _read_text(
        os.path.join(resolved_repo, "adapters", "skill", "consilium-collab", "SKILL.md")
    )
    claude_agents_dir = os.path.join(resolved_repo, "adapters", "claude", "agents")
    claude_agent_files = [
        (fname, _read_text(os.path.join(claude_agents_dir, fname)))
        for fname in sorted(os.listdir(claude_agents_dir))
        if fname.endswith(".md")
    ]
    codex_agents_dir = os.path.join(resolved_repo, "adapters", "codex", "agents")
    codex_agent_files = [
        (fname, _read_text(os.path.join(codex_agents_dir, fname)))
        for fname in sorted(os.listdir(codex_agents_dir))
        if fname.endswith(".toml")
    ]
    plist_tmpl = _read_text(
        os.path.join(resolved_repo, "adapters", "launchd", "com.up.consilium.plist.tmpl")
    )
    plist_rendered = (
        plist_tmpl.replace("__PORT__", str(port))
        .replace("__STATE_DIR__", state_dir)
        .replace("__RENDEZVOUS_DIR__", resolved_rv)
        .replace("__LOG_DIR__", log_dir)
        .replace("__REPO_DIR__", resolved_repo)
    )

    # Decide token value WITHOUT writing auth.token during Phase 1!
    existing_token = _peek_token(state_dir)
    if mode == "apply" and existing_token == "<generated-on-apply>":
        pending_new_token: Optional[str] = generate_auth_token()
        effective_token = pending_new_token
    else:
        pending_new_token = None
        effective_token = existing_token

    antigravity_path = os.path.join(home_dir, ".gemini", "config", "mcp_config.json")
    claude_desktop_path = os.path.join(
        home_dir, "Library", "Application Support", "Claude", "claude_desktop_config.json"
    )
    claude_code_path = os.path.join(home_dir, ".claude.json")
    codex_path = os.path.join(home_dir, ".codex", "config.toml")

    antigravity_http_entry = {
        "url": f"http://127.0.0.1:{port}/mcp",
        "headers": {"Authorization": f"Bearer {effective_token}"},
    }
    claude_desktop_stdio_entry = {
        "command": "/usr/bin/python3",
        "args": [
            "-m",
            "consilium.cli",
            "mcp-stdio",
            "--peer",
            "claude",
            "--data-dir",
            state_dir,
            "--rendezvous-dir",
            resolved_rv,
        ],
        "env": {"PYTHONPATH": resolved_repo},
    }

    operations: List[Dict[str, str]] = []
    write_tasks: List[Callable[[], None]] = []

    def _record(label: str, path: str, action: str, detail: str) -> None:
        operations.append({"target": label, "path": path, "action": action, "detail": detail})

    def _plan_safe(label: str, path: str, fn: Callable[[], None]) -> None:
        try:
            fn()
        except PermissionError as exc:
            if apply_changes:
                raise
            _record(label, path, "permission_denied", f"unreadable in current sandbox: {exc}")

    # 1. Antigravity MCP (headers auth, no ?token=!)
    def _plan_ag() -> None:
        action, detail, content = _plan_json_mcp(
            antigravity_path, antigravity_http_entry, remove=remove
        )
        _record("antigravity_mcp", antigravity_path, action, detail)
        if apply_changes and content is not None:
            def _do_write(p: str = antigravity_path, c: str = content) -> None:
                _backup_file(p, home_dir, backup_root)
                _atomic_write_text(p, c, mode=0o600)
            write_tasks.append(_do_write)

    _plan_safe("antigravity_mcp", antigravity_path, _plan_ag)

    # 2. Claude Desktop MCP
    def _plan_cd() -> None:
        action, detail, content = _plan_json_mcp(
            claude_desktop_path, claude_desktop_stdio_entry, remove=remove
        )
        _record("claude_desktop_mcp", claude_desktop_path, action, detail)
        if apply_changes and content is not None:
            def _do_write(p: str = claude_desktop_path, c: str = content) -> None:
                _backup_file(p, home_dir, backup_root)
                _atomic_write_text(p, c, mode=0o600)
            write_tasks.append(_do_write)

    _plan_safe("claude_desktop_mcp", claude_desktop_path, _plan_cd)

    # 3. Claude Code MCP (check CLI executability in Phase 1!)
    def _plan_cc() -> None:
        action, detail, desired, prev_entry = _plan_claude_code_mcp(
            claude_code_path, port, effective_token, remove=remove
        )
        _record("claude_code_mcp", claude_code_path, action, detail)
        if apply_changes and action != "unchanged":
            claude_bin = _find_claude_cli()
            if not (os.path.isfile(claude_bin) and os.access(claude_bin, os.X_OK)):
                raise FileNotFoundError(f"Executable claude CLI not found at {claude_bin}")
            def _do_cc(
                p: str = claude_code_path,
                d: Dict[str, Any] = desired,
                prev: Optional[Dict[str, Any]] = prev_entry,
                cb: str = claude_bin,
            ) -> None:
                _apply_claude_code_mcp(
                    p,
                    d,
                    prev,
                    remove=remove,
                    home_dir=home_dir,
                    backup_root=backup_root,
                    claude_bin=cb,
                    runner=claude_runner,
                )
            write_tasks.append(_do_cc)

    _plan_safe("claude_code_mcp", claude_code_path, _plan_cc)

    # 4. Codex TOML (validates duplicate tables & dotted keys in Phase 1!)
    def _plan_cx() -> None:
        action, detail, content = _plan_codex_toml(
            codex_path, port, effective_token, remove=remove
        )
        _record("codex_mcp", codex_path, action, detail)
        if apply_changes and content is not None:
            def _do_write(p: str = codex_path, c: str = content) -> None:
                _backup_file(p, home_dir, backup_root)
                _atomic_write_text(p, c, mode=0o600)
            write_tasks.append(_do_write)

    _plan_safe("codex_mcp", codex_path, _plan_cx)

    # 5. Skills, agent templates, launchd plist
    text_targets: List[Tuple[str, str, str, int]] = [
        (
            "antigravity_skill",
            os.path.join(home_dir, ".gemini", "config", "skills", "consilium-collab", "SKILL.md"),
            skill_content,
            0o644,
        ),
        (
            "claude_skill",
            os.path.join(home_dir, ".claude", "skills", "consilium-collab", "SKILL.md"),
            skill_content,
            0o644,
        ),
        (
            "codex_skill",
            os.path.join(home_dir, ".codex", "skills", "consilium-collab", "SKILL.md"),
            skill_content,
            0o644,
        ),
    ]
    for fname, content in claude_agent_files:
        text_targets.append(
            (f"claude_agent:{fname}", os.path.join(home_dir, ".claude", "agents", fname), content, 0o644)
        )
    for fname, content in codex_agent_files:
        text_targets.append(
            (f"codex_agent:{fname}", os.path.join(home_dir, ".codex", "agents", fname), content, 0o644)
        )
    text_targets.append(
        (
            "launchd_plist",
            os.path.join(home_dir, "Library", "LaunchAgents", "com.up.consilium.plist"),
            plist_rendered,
            0o644,
        )
    )

    for label, dest, content, file_mode in text_targets:
        def _plan_txt(lbl: str = label, dst: str = dest, cnt: str = content, fm: int = file_mode) -> None:
            action, detail = _plan_text_file(dst, cnt, remove=remove)
            _record(lbl, dst, action, detail)
            if apply_changes:
                if action == "remove":
                    def _do_rm(p: str = dst) -> None:
                        _backup_file(p, home_dir, backup_root)
                        os.remove(p)
                    write_tasks.append(_do_rm)
                elif action in ("create", "update"):
                    def _do_wr(p: str = dst, c: str = cnt, m: int = fm) -> None:
                        _backup_file(p, home_dir, backup_root)
                        _atomic_write_text(p, c, mode=m)
                    write_tasks.append(_do_wr)

        _plan_safe(label, dest, _plan_txt)

    # ---- PHASE 2: EXECUTE WRITES ONLY AFTER ALL VALIDATIONS PASSED ----
    if apply_changes:
        if mode == "apply":
            os.makedirs(log_dir, mode=0o700, exist_ok=True)
            if pending_new_token is not None:
                write_auth_token(state_dir, pending_new_token)
            else:
                ensure_auth_token(state_dir)
        for task_fn in write_tasks:
            task_fn()

    return {
        "mode": mode,
        "target_home": home_dir,
        "state_dir": state_dir,
        "rendezvous_dir": resolved_rv,
        "log_dir": log_dir,
        "repo_dir": resolved_repo,
        "port": port,
        "claude_desktop_running": claude_running,
        "warnings": warnings,
        "backup_root": backup_root if (apply_changes and os.path.exists(backup_root)) else "",
        "operations": operations,
    }


def rotate_token(
    *,
    target_home: str,
    port: int = 18920,
    state_dir: Optional[str] = None,
    claude_runner: Any = subprocess.run,
) -> Dict[str, Any]:
    """Rotate consilium Bearer token: update all configured clients first, swap auth.token last."""
    home_dir = os.path.abspath(os.path.expanduser(target_home))
    resolved_state = (
        os.path.abspath(os.path.expanduser(state_dir))
        if state_dir
        else os.path.join(home_dir, "Library", "Application Support", "consilium")
    )
    backup_root = os.path.join(resolved_state, "backups", f"rotate-{_timestamp_tag()}")
    new_token = generate_auth_token()

    antigravity_path = os.path.join(home_dir, ".gemini", "config", "mcp_config.json")
    claude_code_path = os.path.join(home_dir, ".claude.json")
    codex_path = os.path.join(home_dir, ".codex", "config.toml")

    # Phase 1: validate existing configs before mutating anything.
    ag_exists = os.path.exists(antigravity_path)
    ag_content: Optional[str] = None
    if ag_exists:
        ag_data = _load_json_dict(antigravity_path)
        if "consilium" in (ag_data.get("mcpServers") or {}):
            _, _, ag_content = _plan_json_mcp(
                antigravity_path,
                {
                    "url": f"http://127.0.0.1:{port}/mcp",
                    "headers": {"Authorization": f"Bearer {new_token}"},
                },
                remove=False,
            )

    cc_prev: Optional[Dict[str, Any]] = None
    cc_desired = {
        "type": "http",
        "url": f"http://127.0.0.1:{port}/mcp",
        "headers": {"Authorization": f"Bearer {new_token}"},
    }
    cc_needs_update = False
    claude_bin = _find_claude_cli()
    if os.path.exists(claude_code_path):
        cc_data = _load_json_dict(claude_code_path)
        if "consilium" in (cc_data.get("mcpServers") or {}):
            cc_prev = cc_data["mcpServers"]["consilium"]
            cc_needs_update = True
            if not (os.path.isfile(claude_bin) and os.access(claude_bin, os.X_OK)):
                raise FileNotFoundError(f"Executable claude CLI not found at {claude_bin}")

    cx_content: Optional[str] = None
    if os.path.exists(codex_path):
        with open(codex_path, "r", encoding="utf-8") as handle:
            raw_toml = handle.read()
        if CODEX_BLOCK_RE.search(raw_toml):
            _, _, cx_content = _plan_codex_toml(codex_path, port, new_token, remove=False)

    # Phase 2: rewrite client configs FIRST, swap server auth.token LAST!
    updated_targets: List[str] = []
    if ag_content is not None:
        _backup_file(antigravity_path, home_dir, backup_root)
        _atomic_write_text(antigravity_path, ag_content, mode=0o600)
        updated_targets.append("antigravity_mcp")

    if cc_needs_update:
        _apply_claude_code_mcp(
            claude_code_path,
            cc_desired,
            cc_prev,
            remove=False,
            home_dir=home_dir,
            backup_root=backup_root,
            claude_bin=claude_bin,
            runner=claude_runner,
        )
        updated_targets.append("claude_code_mcp")

    if cx_content is not None:
        _backup_file(codex_path, home_dir, backup_root)
        _atomic_write_text(codex_path, cx_content, mode=0o600)
        updated_targets.append("codex_mcp")

    previous_token_existed = bool(read_auth_token(resolved_state))
    write_auth_token(resolved_state, new_token)
    updated_targets.append("server_auth_token")

    return {
        "rotated": True,
        "state_dir": resolved_state,
        "port": port,
        "previous_token_existed": previous_token_existed,
        "updated_targets": updated_targets,
        "backup_root": backup_root if os.path.exists(backup_root) else "",
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Install or inspect consilium configurations (defaults to --dry-run)."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", help="Show planned changes without writing (default).")
    group.add_argument("--check", action="store_true", help="Inspect current configuration status without writing.")
    group.add_argument("--apply", action="store_true", help="Write configs/templates with timestamped backups.")
    group.add_argument("--uninstall", action="store_true", help="Remove unmodified consilium config entries and files.")
    parser.add_argument("--target-home", default=os.path.expanduser("~"))
    parser.add_argument("--repo-dir", default=REPO_DIR_DEFAULT)
    parser.add_argument("--rendezvous-dir", default=None)
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
            rendezvous_dir=args.rendezvous_dir,
        )
    except (ValueError, json.JSONDecodeError, RuntimeError, FileNotFoundError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
