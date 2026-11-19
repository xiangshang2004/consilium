"""Shared constants, validation helpers, and split state/rendezvous paths for consilium."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import secrets
import stat
import tempfile
from typing import Optional

VALID_PEERS = ("chatgpt", "claude", "antigravity")
VALID_ISOLATION = ("worktree", "read_only", "shared")
VALID_MODEL_TIERS = ("pro_reasoning", "balanced", "fast", "custom")
VALID_TASK_STATUSES = (
    "queued",
    "claimed",
    "running",
    "done",
    "failed",
    "blocked",
    "cancelled",
)
NON_TERMINAL_TASK_STATUSES = ("queued", "claimed", "running")
PROGRESS_TASK_STATUSES = ("claimed", "running")
TERMINAL_TASK_STATUSES = ("done", "failed", "blocked", "cancelled")
REQUEUEABLE_TASK_STATUSES = ("claimed", "running", "failed", "blocked")
VALID_MISSION_CLOSE_STATUSES = ("completed", "cancelled", "failed")


class ConflictError(ValueError):
    """Raised when a client_request_id collides with a different operation, caller, or payload."""

DEFAULT_CLAIM_TIMEOUT_SEC = 900
DEFAULT_RUNNING_TIMEOUT_SEC = 7200
DEFAULT_REAPER_INTERVAL_SEC = 60.0
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


def _ensure_private_dir(raw_path: str, label: str) -> str:
    """Create a private 0700 directory and reject symlinks or foreign UID ownership."""
    resolved = os.path.abspath(os.path.expanduser(raw_path))
    os.makedirs(resolved, mode=0o700, exist_ok=True)
    st = os.lstat(resolved)
    if stat.S_ISLNK(st.st_mode):
        raise PermissionError(f"Refusing symlinked consilium {label} directory: {resolved}")
    if st.st_uid != os.getuid():
        raise PermissionError(
            f"Refusing consilium {label} directory owned by uid {st.st_uid}: {resolved}"
        )
    try:
        os.chmod(resolved, 0o700)
    except OSError:
        pass
    return resolved


def resolve_state_dir(override: Optional[str] = None) -> str:
    """Resolve the persistent state directory (defaults to ~/Library/Application Support/consilium)."""
    default_dir = os.path.join("~", "Library", "Application Support", "consilium")
    raw = (
        override
        or os.environ.get("CONSILIUM_STATE_DIR")
        or os.environ.get("CONSILIUM_HOME")
        or default_dir
    )
    return _ensure_private_dir(raw, "state")


def resolve_home_dir(override: Optional[str] = None) -> str:
    """Backward-compatible alias to resolve_state_dir."""
    return resolve_state_dir(override)


def resolve_rendezvous_dir(
    override: Optional[str] = None,
    fallback_dir: Optional[str] = None,
) -> str:
    """Resolve the ephemeral /tmp rendezvous directory for kqueue signals, leases, and wake.sock.

    Precedence: explicit override -> CONSILIUM_RENDEZVOUS_DIR env -> optional fallback_dir -> /tmp/consilium-<uid>.
    """
    default_dir = f"/tmp/consilium-{os.getuid()}"
    raw = (
        override
        or os.environ.get("CONSILIUM_RENDEZVOUS_DIR")
        or fallback_dir
        or default_dir
    )
    return _ensure_private_dir(raw, "rendezvous")


def read_auth_token(state_dir: str) -> str:
    """Read existing Bearer token from <state_dir>/auth.token, or return empty string."""
    resolved_dir = resolve_state_dir(state_dir)
    token_path = os.path.join(resolved_dir, "auth.token")
    if not os.path.exists(token_path):
        return ""
    with open(token_path, "r", encoding="utf-8") as handle:
        return handle.read().strip()


def write_auth_token(state_dir: str, token: str) -> str:
    """Atomically write a Bearer token into <state_dir>/auth.token (0600)."""
    clean = (token or "").strip()
    if not clean:
        raise ValueError("Auth token must not be empty")
    resolved_dir = resolve_state_dir(state_dir)
    token_path = os.path.join(resolved_dir, "auth.token")
    fd, tmp_path = tempfile.mkstemp(prefix=".auth.token.", dir=resolved_dir)
    try:
        os.chmod(tmp_path, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(clean + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, token_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    try:
        os.chmod(token_path, 0o600)
    except OSError:
        pass
    return clean


def generate_auth_token() -> str:
    """Generate a new 256-bit hex Bearer token."""
    return secrets.token_hex(32)


def ensure_auth_token(state_dir: str) -> str:
    """Return the local Bearer token stored in <state_dir>/auth.token (0600)."""
    existing = read_auth_token(state_dir)
    if existing:
        return existing
    return write_auth_token(state_dir, generate_auth_token())
