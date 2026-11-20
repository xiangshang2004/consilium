"""Safe, idempotent, two-phase atomic installer & token rotator for consilium."""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

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

VALID_INSTALL_TARGETS: Tuple[str, ...] = (
    "claude_desktop",
    "claude_code",
    "codex",
    "antigravity",
    "launchd",
    "claude_agents",
    "codex_agents",
)

TARGET_ALIASES: Dict[str, str] = {
    "claude_desktop": "claude_desktop",
    "claude-desktop": "claude_desktop",
    "desktop": "claude_desktop",
    "claude_app": "claude_desktop",
    "claude-app": "claude_desktop",
    "claude_code": "claude_code",
    "claude-code": "claude_code",
    "code": "claude_code",
    "codex": "codex",
    "antigravity": "antigravity",
    "ag": "antigravity",
    "gemini": "antigravity",
    "launchd": "launchd",
    "plist": "launchd",
    "claude_agents": "claude_agents",
    "claude-agents": "claude_agents",
    "codex_agents": "codex_agents",
    "codex-agents": "codex_agents",
}


class TokenRotationError(RuntimeError):
    """Raised when rotate_token fails midway, carrying partial progress and rollback steps."""

    def __init__(self, message: str, *, report: Dict[str, Any]) -> None:
        super().__init__(message)
        self.report = report


class InstallApplyError(RuntimeError):
    """Raised when run_installer Phase 2 fails mid-way, reporting exact per-target state."""

    def __init__(self, message: str, *, report: Dict[str, Any]) -> None:
        super().__init__(message)
        self.report = report


def _normalize_install_targets(
    targets: Optional[Iterable[str]] = None,
    *,
    skip_claude_desktop: bool = False,
) -> Tuple[str, ...]:
    if targets is None:
        selected = list(VALID_INSTALL_TARGETS)
    else:
        raw_items: List[str] = []
        if isinstance(targets, str):
            raw_items.extend(part.strip() for part in targets.split(",") if part.strip())
        else:
            for item in targets:
                raw_items.extend(part.strip() for part in str(item).split(",") if part.strip())
        if not raw_items:
            raise ValueError(
                f"targets must not be empty; valid targets: {', '.join(VALID_INSTALL_TARGETS)}"
            )
        selected = []
        for item in raw_items:
            lowered = item.lower()
            if lowered == "all":
                for canon in VALID_INSTALL_TARGETS:
                    if canon not in selected:
                        selected.append(canon)
                continue
            canon = TARGET_ALIASES.get(lowered)
            if canon is None:
                raise ValueError(
                    f"Unknown install target {item!r}; valid targets: {', '.join(VALID_INSTALL_TARGETS)}"
                )
            if canon not in selected:
                selected.append(canon)
    if skip_claude_desktop:
        selected = [t for t in selected if t != "claude_desktop"]
    if not selected:
        raise ValueError("No install targets remain after filtering.")
    return tuple(selected)


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


_BEARER_RE = re.compile(r"(Bearer\s+)[^\s\"'`]+", re.IGNORECASE)
_AUTH_JSON_RE = re.compile(r'("Authorization"\s*:\s*")[^"]+(")', re.IGNORECASE)
_AUTH_TOML_RE = re.compile(r'(Authorization\s*=\s*")[^"]+(")', re.IGNORECASE)


def _redact_secrets(text: str, secrets: Iterable[str] = ()) -> str:
    """Scrub Bearer tokens and explicit secret strings from diagnostic/rollback text."""
    out = str(text or "")
    if not out:
        return out
    out = _BEARER_RE.sub(r"\1[REDACTED]", out)
    out = _AUTH_JSON_RE.sub(r"\1[REDACTED]\2", out)
    out = _AUTH_TOML_RE.sub(r"\1[REDACTED]\2", out)
    for secret in sorted({str(s or "").strip() for s in secrets if s}, key=len, reverse=True):
        if len(secret) >= 6:
            out = out.replace(secret, "[REDACTED]")
    return out


def _backup_file(target_path: str, home_dir: str, backup_root: str) -> Optional[str]:
    if not os.path.exists(target_path):
        return None
    os.makedirs(backup_root, mode=0o700, exist_ok=True)
    try:
        os.chmod(backup_root, 0o700)
    except OSError:
        pass
    rel = os.path.relpath(target_path, home_dir)
    dest = os.path.join(backup_root, rel)
    os.makedirs(os.path.dirname(dest), mode=0o700, exist_ok=True)
    shutil.copy2(target_path, dest)
    try:
        os.chmod(dest, 0o600)
    except OSError:
        pass
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


CLAUDE_DESKTOP_EXECUTABLE = "/Applications/Claude.app/Contents/MacOS/Claude"


def _is_claude_desktop_running(runner: Any = subprocess.run) -> Optional[bool]:
    """Detect Claude.app; `pgrep -x` can miss it in sandboxed shells, so fall back to `ps`."""
    pgrep_ok = False
    try:
        res = runner(["pgrep", "-x", "Claude"], capture_output=True, text=True, check=False)
        if res.returncode == 0:
            return True
        pgrep_ok = res.returncode == 1
    except OSError:
        pass
    try:
        res = runner(["ps", "-axo", "comm="], capture_output=True, text=True, check=False)
        if res.returncode == 0:
            for line in (res.stdout or "").splitlines():
                if line.strip() == CLAUDE_DESKTOP_EXECUTABLE:
                    return True
            return False
    except OSError:
        pass
    return False if pgrep_ok else None


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
    secrets_to_redact: Iterable[str] = (),
) -> Optional[str]:
    """Apply Claude Code MCP change via CLI with saved entry and verified rollback."""
    saved_previous: Optional[Dict[str, Any]] = copy.deepcopy(previous_entry)
    if saved_previous is None and os.path.exists(path):
        pre_data = _load_json_dict(path)
        existing = (pre_data.get("mcpServers") or {}).get("consilium")
        if existing is not None:
            saved_previous = copy.deepcopy(existing)

    backup_path = _backup_file(path, home_dir, backup_root)
    env = _claude_subprocess_env(home_dir)

    def _restore_and_verify(failure_summary: str) -> str:
        if not (removed_old and saved_previous is not None):
            return "no previous entry to restore"
        try:
            rb_res = runner(
                [
                    claude_bin,
                    "mcp",
                    "add-json",
                    "-s",
                    "user",
                    "consilium",
                    json.dumps(saved_previous, ensure_ascii=False),
                ],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception as rb_exc:
            raise RuntimeError(
                _redact_secrets(
                    f"{failure_summary}; rollback invocation failed ({type(rb_exc).__name__}: {rb_exc}); "
                    f"restore backup from {backup_root}",
                    secrets_to_redact,
                )
            ) from rb_exc
        if rb_res.returncode != 0:
            raise RuntimeError(
                _redact_secrets(
                    f"{failure_summary}; rollback add-json also failed "
                    f"({rb_res.returncode}): {rb_res.stderr or rb_res.stdout}; "
                    f"restore backup from {backup_root}",
                    secrets_to_redact,
                )
            )
        verify_res = runner(
            [claude_bin, "mcp", "get", "consilium"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if verify_res.returncode != 0:
            raise RuntimeError(
                _redact_secrets(
                    f"{failure_summary}; rollback verification via "
                    f"`claude mcp get consilium` failed ({verify_res.returncode}): "
                    f"{verify_res.stderr or verify_res.stdout}; restore backup from {backup_root}",
                    secrets_to_redact,
                )
            )
        if os.path.exists(path):
            restored_data = _load_json_dict(path)
            restored_entry = (restored_data.get("mcpServers") or {}).get("consilium")
            if restored_entry != saved_previous:
                raise RuntimeError(
                    _redact_secrets(
                        f"{failure_summary}; rollback verification "
                        f"failed: {path} consilium entry does not match saved previous entry; "
                        f"restore backup from {backup_root}",
                        secrets_to_redact,
                    )
                )
        elif saved_previous is not None:
            raise RuntimeError(
                _redact_secrets(
                    f"{failure_summary}; rollback verification "
                    f"failed: {path} missing after rollback; restore backup from {backup_root}",
                    secrets_to_redact,
                )
            )
        return "previous entry restored and verified"

    # Probe via `claude mcp get consilium` before removal so prior state is confirmed.
    if saved_previous is not None or remove:
        runner(
            [claude_bin, "mcp", "get", "consilium"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    removed_old = False
    if remove or saved_previous is not None:
        rm_res = runner(
            [claude_bin, "mcp", "remove", "-s", "user", "consilium"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if remove and rm_res.returncode != 0:
            raise RuntimeError(
                _redact_secrets(
                    f"claude mcp remove failed ({rm_res.returncode}): {rm_res.stderr or rm_res.stdout}",
                    secrets_to_redact,
                )
            )
        removed_old = rm_res.returncode == 0

    if not remove:
        payload_json = json.dumps(desired, ensure_ascii=False)
        try:
            add_res = runner(
                [claude_bin, "mcp", "add-json", "-s", "user", "consilium", payload_json],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception as add_exc:
            summary = f"claude mcp add-json raised {type(add_exc).__name__}: {add_exc}"
            rollback_note = _restore_and_verify(summary)
            raise RuntimeError(
                _redact_secrets(f"{summary} ({rollback_note})", secrets_to_redact)
            ) from add_exc
        if add_res.returncode != 0:
            summary = f"claude mcp add-json failed ({add_res.returncode}): {add_res.stderr or add_res.stdout}"
            rollback_note = _restore_and_verify(summary)
            raise RuntimeError(
                _redact_secrets(f"{summary} ({rollback_note})", secrets_to_redact)
            )
    return backup_path


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
    targets: Optional[Iterable[str]] = None,
    skip_claude_desktop: bool = False,
    claude_running_check: Optional[Callable[[], Optional[bool]]] = None,
    claude_runner: Any = subprocess.run,
) -> Dict[str, Any]:
    """Two-phase atomic installer: validate all selected targets before writing a single byte."""
    selected_targets = _normalize_install_targets(
        targets, skip_claude_desktop=skip_claude_desktop
    )
    selected_set = set(selected_targets)

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

    # ---- PHASE 1: READ & VALIDATE SELECTED TARGETS (ZERO WRITES!) ----
    running_fn = claude_running_check or _is_claude_desktop_running
    claude_running = running_fn()
    warnings: List[str] = []
    if claude_running:
        if "claude_desktop" in selected_set:
            msg = (
                "Claude.app is currently running; quit Claude.app before applying "
                "claude_desktop_config.json changes (or pass --skip-claude-desktop / "
                "--targets claude_code,codex,antigravity)."
            )
            if mode == "apply":
                raise RuntimeError(msg)
            warnings.append(msg)
        else:
            warnings.append(
                "Claude.app is currently running; proceeding because claude_desktop target is excluded."
            )

    # Read template sources up front for selected targets so missing files fail before any write.
    skill_content = ""
    if selected_set & {"antigravity", "claude_code", "codex"}:
        skill_content = _read_text(
            os.path.join(resolved_repo, "adapters", "skill", "consilium-collab", "SKILL.md")
        )
    claude_agent_files: List[Tuple[str, str]] = []
    if "claude_agents" in selected_set:
        claude_agents_dir = os.path.join(resolved_repo, "adapters", "claude", "agents")
        claude_agent_files = [
            (fname, _read_text(os.path.join(claude_agents_dir, fname)))
            for fname in sorted(os.listdir(claude_agents_dir))
            if fname.endswith(".md")
        ]
    codex_agent_files: List[Tuple[str, str]] = []
    if "codex_agents" in selected_set:
        codex_agents_dir = os.path.join(resolved_repo, "adapters", "codex", "agents")
        codex_agent_files = [
            (fname, _read_text(os.path.join(codex_agents_dir, fname)))
            for fname in sorted(os.listdir(codex_agents_dir))
            if fname.endswith(".toml")
        ]
    plist_rendered = ""
    if "launchd" in selected_set:
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

    secrets_to_redact: List[str] = [
        s for s in (pending_new_token, effective_token) if s and s != "<generated-on-apply>"
    ]
    operations: List[Dict[str, str]] = []
    write_tasks: List[Tuple[str, str, str, Callable[[], Optional[str]]]] = []

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
    if "antigravity" in selected_set:
        def _plan_ag() -> None:
            action, detail, content = _plan_json_mcp(
                antigravity_path, antigravity_http_entry, remove=remove
            )
            _record("antigravity_mcp", antigravity_path, action, detail)
            if apply_changes and content is not None:
                def _do_write(p: str = antigravity_path, c: str = content) -> Optional[str]:
                    bak = _backup_file(p, home_dir, backup_root)
                    _atomic_write_text(p, c, mode=0o600)
                    return bak
                write_tasks.append(("antigravity_mcp", antigravity_path, action, _do_write))

        _plan_safe("antigravity_mcp", antigravity_path, _plan_ag)

    # 2. Claude Desktop MCP
    if "claude_desktop" in selected_set:
        def _plan_cd() -> None:
            action, detail, content = _plan_json_mcp(
                claude_desktop_path, claude_desktop_stdio_entry, remove=remove
            )
            _record("claude_desktop_mcp", claude_desktop_path, action, detail)
            if apply_changes and content is not None:
                def _do_write(p: str = claude_desktop_path, c: str = content) -> Optional[str]:
                    bak = _backup_file(p, home_dir, backup_root)
                    _atomic_write_text(p, c, mode=0o600)
                    return bak
                write_tasks.append(("claude_desktop_mcp", claude_desktop_path, action, _do_write))

        _plan_safe("claude_desktop_mcp", claude_desktop_path, _plan_cd)

    # 3. Claude Code MCP (check CLI executability in Phase 1!)
    if "claude_code" in selected_set:
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
                ) -> Optional[str]:
                    return _apply_claude_code_mcp(
                        p,
                        d,
                        prev,
                        remove=remove,
                        home_dir=home_dir,
                        backup_root=backup_root,
                        claude_bin=cb,
                        runner=claude_runner,
                        secrets_to_redact=secrets_to_redact,
                    )
                write_tasks.append(("claude_code_mcp", claude_code_path, action, _do_cc))

        _plan_safe("claude_code_mcp", claude_code_path, _plan_cc)

    # 4. Codex TOML (validates duplicate tables & dotted keys in Phase 1!)
    if "codex" in selected_set:
        def _plan_cx() -> None:
            action, detail, content = _plan_codex_toml(
                codex_path, port, effective_token, remove=remove
            )
            _record("codex_mcp", codex_path, action, detail)
            if apply_changes and content is not None:
                def _do_write(p: str = codex_path, c: str = content) -> Optional[str]:
                    bak = _backup_file(p, home_dir, backup_root)
                    _atomic_write_text(p, c, mode=0o600)
                    return bak
                write_tasks.append(("codex_mcp", codex_path, action, _do_write))

        _plan_safe("codex_mcp", codex_path, _plan_cx)

    # 5. Skills, agent templates, launchd plist filtered by selected_set
    text_targets: List[Tuple[str, str, str, int]] = []
    if "antigravity" in selected_set:
        text_targets.append(
            (
                "antigravity_skill",
                os.path.join(home_dir, ".gemini", "config", "skills", "consilium-collab", "SKILL.md"),
                skill_content,
                0o644,
            )
        )
    if "claude_code" in selected_set:
        text_targets.append(
            (
                "claude_skill",
                os.path.join(home_dir, ".claude", "skills", "consilium-collab", "SKILL.md"),
                skill_content,
                0o644,
            )
        )
    for fname, content in claude_agent_files:
        text_targets.append(
            (f"claude_agent:{fname}", os.path.join(home_dir, ".claude", "agents", fname), content, 0o644)
        )
    if "codex" in selected_set:
        text_targets.append(
            (
                "codex_skill",
                os.path.join(home_dir, ".codex", "skills", "consilium-collab", "SKILL.md"),
                skill_content,
                0o644,
            )
        )
    for fname, content in codex_agent_files:
        text_targets.append(
            (f"codex_agent:{fname}", os.path.join(home_dir, ".codex", "agents", fname), content, 0o644)
        )
    if "launchd" in selected_set:
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
                    def _do_rm(p: str = dst) -> Optional[str]:
                        bak = _backup_file(p, home_dir, backup_root)
                        os.remove(p)
                        return bak
                    write_tasks.append((lbl, dst, action, _do_rm))
                elif action in ("create", "update"):
                    def _do_wr(p: str = dst, c: str = cnt, m: int = fm) -> Optional[str]:
                        bak = _backup_file(p, home_dir, backup_root)
                        _atomic_write_text(p, c, mode=m)
                        return bak
                    write_tasks.append((lbl, dst, action, _do_wr))

        _plan_safe(label, dest, _plan_txt)

    # ---- PHASE 2: EXECUTE WRITES ONLY AFTER ALL VALIDATIONS PASSED ----
    target_results: List[Dict[str, Any]] = []
    if not apply_changes:
        for op in operations:
            target_results.append(
                {
                    "target": op["target"],
                    "path": op["path"],
                    "action": op["action"],
                    "status": "planned",
                }
            )
    else:
        if mode == "apply":
            os.makedirs(log_dir, mode=0o700, exist_ok=True)
            if pending_new_token is not None:
                write_auth_token(state_dir, pending_new_token)
            else:
                ensure_auth_token(state_dir)

        queued_keys = {(lbl, pth) for lbl, pth, _, _ in write_tasks}
        applied_targets: List[str] = []
        for op in operations:
            if (op["target"], op["path"]) not in queued_keys:
                target_results.append(
                    {
                        "target": op["target"],
                        "path": op["path"],
                        "action": op["action"],
                        "status": op["action"],
                    }
                )

        for idx, (lbl, dst, act, task_fn) in enumerate(write_tasks):
            try:
                bak_path = task_fn()
                applied_targets.append(lbl)
                target_results.append(
                    {
                        "target": lbl,
                        "path": dst,
                        "action": act,
                        "status": "applied",
                        "backup_path": bak_path or "",
                    }
                )
            except Exception as exc:
                safe_error = _redact_secrets(str(exc), secrets_to_redact)
                actual_backup = backup_root if os.path.exists(backup_root) else ""
                candidate_bak = ""
                if actual_backup:
                    rel_dst = os.path.relpath(dst, home_dir)
                    probe_bak = (
                        os.path.join(actual_backup, rel_dst)
                        if not rel_dst.startswith("..")
                        else os.path.join(actual_backup, os.path.basename(dst))
                    )
                    if os.path.exists(probe_bak):
                        candidate_bak = probe_bak
                target_results.append(
                    {
                        "target": lbl,
                        "path": dst,
                        "action": act,
                        "status": "failed",
                        "may_be_modified": True,
                        "backup_path": candidate_bak,
                        "error": safe_error,
                    }
                )
                not_attempted_targets: List[str] = []
                for rem_lbl, rem_dst, rem_act, _ in write_tasks[idx + 1 :]:
                    not_attempted_targets.append(rem_lbl)
                    target_results.append(
                        {
                            "target": rem_lbl,
                            "path": rem_dst,
                            "action": rem_act,
                            "status": "not_attempted",
                            "may_be_modified": False,
                        }
                    )
                partial_report = {
                    "ok": False,
                    "mode": mode,
                    "selected_targets": list(selected_targets),
                    "target_home": home_dir,
                    "state_dir": state_dir,
                    "rendezvous_dir": resolved_rv,
                    "port": port,
                    "failed_target": lbl,
                    "failed_path": dst,
                    "failed_target_may_be_modified": True,
                    "error": safe_error,
                    "applied_targets": applied_targets,
                    "not_attempted_targets": not_attempted_targets,
                    "backup_root": actual_backup,
                    "target_results": target_results,
                }
                summary = _redact_secrets(
                    (
                        f"Installer {mode} failed at {lbl} ({dst}): {safe_error}. "
                        f"applied={applied_targets}; not_attempted={not_attempted_targets}; "
                        f"backup_root={actual_backup!r}"
                    ),
                    secrets_to_redact,
                )
                raise InstallApplyError(summary, report=partial_report) from exc

    return {
        "mode": mode,
        "selected_targets": list(selected_targets),
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
        "target_results": target_results,
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
    existing_token = read_auth_token(resolved_state) or ""
    new_token = generate_auth_token()
    secrets_to_redact: List[str] = [s for s in (existing_token, new_token) if s]

    antigravity_path = os.path.join(home_dir, ".gemini", "config", "mcp_config.json")
    claude_code_path = os.path.join(home_dir, ".claude.json")
    codex_path = os.path.join(home_dir, ".codex", "config.toml")

    # Phase 1: validate existing configs before mutating anything.
    ag_exists = os.path.exists(antigravity_path)
    ag_content: Optional[str] = None
    if ag_exists:
        ag_data = _load_json_dict(antigravity_path)
        ag_entry = (ag_data.get("mcpServers") or {}).get("consilium")
        if isinstance(ag_entry, dict):
            hdrs = ag_entry.get("headers") or {}
            if isinstance(hdrs, dict) and hdrs.get("Authorization"):
                auth_val = str(hdrs["Authorization"]).strip()
                secrets_to_redact.append(auth_val)
                if auth_val.startswith("Bearer "):
                    secrets_to_redact.append(auth_val[7:].strip())
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
            if isinstance(cc_prev, dict):
                hdrs = cc_prev.get("headers") or {}
                if isinstance(hdrs, dict) and hdrs.get("Authorization"):
                    auth_val = str(hdrs["Authorization"]).strip()
                    secrets_to_redact.append(auth_val)
                    if auth_val.startswith("Bearer "):
                        secrets_to_redact.append(auth_val[7:].strip())
            cc_needs_update = True
            if not (os.path.isfile(claude_bin) and os.access(claude_bin, os.X_OK)):
                raise FileNotFoundError(f"Executable claude CLI not found at {claude_bin}")

    cx_content: Optional[str] = None
    if os.path.exists(codex_path):
        with open(codex_path, "r", encoding="utf-8") as handle:
            raw_toml = handle.read()
        if CODEX_BLOCK_RE.search(raw_toml):
            _, _, cx_content = _plan_codex_toml(codex_path, port, new_token, remove=False)

    steps: List[Tuple[str, Optional[str], Callable[[], None]]] = []
    if ag_content is not None:
        def _rot_ag() -> None:
            _backup_file(antigravity_path, home_dir, backup_root)
            _atomic_write_text(antigravity_path, ag_content, mode=0o600)

        steps.append(("antigravity_mcp", antigravity_path, _rot_ag))

    if cc_needs_update:
        def _rot_cc() -> None:
            _apply_claude_code_mcp(
                claude_code_path,
                cc_desired,
                cc_prev,
                remove=False,
                home_dir=home_dir,
                backup_root=backup_root,
                claude_bin=claude_bin,
                runner=claude_runner,
                secrets_to_redact=secrets_to_redact,
            )

        steps.append(("claude_code_mcp", claude_code_path, _rot_cc))

    if cx_content is not None:
        def _rot_cx() -> None:
            _backup_file(codex_path, home_dir, backup_root)
            _atomic_write_text(codex_path, cx_content, mode=0o600)

        steps.append(("codex_mcp", codex_path, _rot_cx))

    previous_token_existed = bool(existing_token)
    token_path = os.path.join(resolved_state, "auth.token")
    if previous_token_existed:
        _backup_file(token_path, resolved_state, backup_root)

    def _rot_token() -> None:
        write_auth_token(resolved_state, new_token)

    steps.append(("server_auth_token", token_path, _rot_token))

    # Phase 2: rewrite client configs FIRST, swap server auth.token LAST!
    updated_targets: List[str] = []
    updated_paths: Dict[str, str] = {}
    planned_targets = [name for name, _, _ in steps]

    for target_name, dest_path, fn in steps:
        try:
            fn()
            updated_targets.append(target_name)
            if dest_path:
                updated_paths[target_name] = dest_path
        except Exception as exc:
            remaining_targets = [t for t in planned_targets if t not in updated_targets]
            server_rotated = "server_auth_token" in updated_targets
            actual_backup = backup_root if os.path.exists(backup_root) else ""
            rollback_instructions: List[str] = []
            if not server_rotated:
                rollback_instructions.append(
                    "Server auth.token was NOT rotated yet (existing token remains active)."
                )
            else:
                rollback_instructions.append(
                    f"Restore previous server token from protected backup into {token_path} and restart consilium serve."
                )
            for done_target in updated_targets:
                if done_target == "antigravity_mcp":
                    bak = os.path.join(actual_backup, os.path.relpath(antigravity_path, home_dir)) if actual_backup else ""
                    rollback_instructions.append(
                        f"Restore Antigravity config from protected backup: cp -f {bak!r} {antigravity_path!r}"
                        if bak
                        else f"Inspect {antigravity_path!r} and restore previous consilium entry."
                    )
                elif done_target == "claude_code_mcp":
                    bak = os.path.join(actual_backup, os.path.relpath(claude_code_path, home_dir)) if actual_backup else ""
                    rollback_instructions.append(
                        (
                            f"Restore Claude Code config from 0o600 protected backup {bak!r} "
                            f"(e.g., restore `mcpServers.consilium` from backup into {claude_code_path!r} or re-register via "
                            f"`{claude_bin} mcp add-json` using the backed-up entry) and verify with `{claude_bin} mcp get consilium`."
                        )
                        if bak
                        else f"Restore Claude Code `mcpServers.consilium` in {claude_code_path!r} and verify with `{claude_bin} mcp get consilium`."
                    )
                elif done_target == "codex_mcp":
                    bak = os.path.join(actual_backup, os.path.relpath(codex_path, home_dir)) if actual_backup else ""
                    rollback_instructions.append(
                        f"Restore Codex managed block from protected backup: cp -f {bak!r} {codex_path!r}"
                        if bak
                        else f"Inspect managed block in {codex_path!r} and restore previous token."
                    )
            if not updated_targets:
                rollback_instructions.append("No client configs completed before the failure.")

            if actual_backup and os.path.exists(dest_path or ""):
                rollback_instructions.append(
                    f"Failed step {target_name!r} may have partially mutated {dest_path!r}; verify or restore from {actual_backup!r}."
                )

            safe_error = _redact_secrets(str(exc), secrets_to_redact)
            safe_instructions = [_redact_secrets(inst, secrets_to_redact) for inst in rollback_instructions]
            report = {
                "rotated": False,
                "state_dir": resolved_state,
                "port": port,
                "failed_target": target_name,
                "failed_target_path": dest_path or "",
                "failed_target_may_be_modified": True,
                "codex_scope": "managed_block_only",
                "error": safe_error,
                "updated_targets": updated_targets,
                "updated_paths": updated_paths,
                "remaining_targets": remaining_targets,
                "server_token_rotated": server_rotated,
                "backup_root": actual_backup,
                "rollback_instructions": safe_instructions,
            }
            summary = _redact_secrets(
                (
                    f"rotate-token failed at {target_name}: {safe_error}. "
                    f"Updated targets={updated_targets}; remaining={remaining_targets}; "
                    f"server_token_rotated={server_rotated}; backup_root={actual_backup!r}. "
                    f"Rollback: {' | '.join(safe_instructions)}"
                ),
                secrets_to_redact,
            )
            raise TokenRotationError(summary, report=report) from exc

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
    parser.add_argument(
        "--targets",
        nargs="+",
        default=None,
        help=(
            "Target components to plan/apply (comma or space separated): "
            "claude_desktop, claude_code (code), codex, antigravity, launchd, claude_agents, codex_agents, or all."
        ),
    )
    parser.add_argument(
        "--skip-claude-desktop",
        action="store_true",
        help="Skip claude_desktop_config.json so other targets can install while Claude.app runs.",
    )
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
            targets=args.targets,
            skip_claude_desktop=args.skip_claude_desktop,
        )
    except InstallApplyError as exc:
        print(json.dumps(exc.report, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    except (ValueError, json.JSONDecodeError, RuntimeError, FileNotFoundError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
