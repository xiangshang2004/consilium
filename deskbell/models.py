"""Shared constants, validation helpers, and home/token paths for deskbell."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import secrets
import stat
from typing import Optional

VALID_PEERS = ("chatgpt", "claude", "antigravity")
VALID_ISOLATION = ("worktree", "read_only", "shared")
VALID_MODEL_TIERS = ("pro_reasoning", "balanced", "fast", "custom")
VALID_TASK_STATUSES = ("queued", "claimed", "running", "done", "failed", "blocked")
TERMINAL_TASK_STATUSES = ("done", "failed", "blocked")

DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 18920


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp with second precision."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_peer(peer_id: str) -> str:
    """Validate and normalize a desktop peer identifier."""
    normalized = (peer_id or "").strip().lower()
    if normalized not in VALID_PEERS:
        raise ValueError(
            f"Unsupported peer {peer_id!r}; expected one of {VALID_PEERS}"
        )
    return normalized


def validate_choice(value: str, allowed: tuple, label: str) -> str:
    """Validate an enumerated string choice against an allowed tuple."""
    normalized = (value or "").strip().lower()
    if normalized not in allowed:
        raise ValueError(f"Unsupported {label} {value!r}; expected one of {allowed}")
    return normalized


def resolve_home_dir(override: Optional[str] = None) -> str:
    """Resolve and privately create the deskbell state directory (mode 0700).

    Defaults to /tmp/deskbell-<uid> so both host MCP servers and macOS
    Seatbelt-sandboxed desktop commands can share SQLite WAL and kqueue signals.
    """
    default_dir = f"/tmp/deskbell-{os.getuid()}"
    raw = override or os.environ.get("DESKBELL_HOME") or default_dir
    home_dir = os.path.abspath(os.path.expanduser(raw))
    os.makedirs(home_dir, mode=0o700, exist_ok=True)
    st = os.lstat(home_dir)
    if stat.S_ISLNK(st.st_mode):
        raise PermissionError(f"Refusing symlinked deskbell home directory: {home_dir}")
    if st.st_uid != os.getuid():
        raise PermissionError(
            f"Refusing deskbell home directory owned by uid {st.st_uid}: {home_dir}"
        )
    try:
        os.chmod(home_dir, 0o700)
    except OSError:
        pass
    return home_dir


def ensure_auth_token(home_dir: str) -> str:
    """Return the local Bearer token stored in $DESKBELL_HOME/auth.token (0600)."""
    token_path = os.path.join(home_dir, "auth.token")
    if os.path.exists(token_path):
        with open(token_path, "r", encoding="utf-8") as handle:
            existing = handle.read().strip()
        if existing:
            return existing

    token = secrets.token_hex(32)
    fd = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(token + "\n")
    try:
        os.chmod(token_path, 0o600)
    except OSError:
        pass
    return token
