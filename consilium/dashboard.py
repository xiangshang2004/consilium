"""Offline Consilium dashboard UI (/ui), snapshot builder (/api/state), and SSE EventHub (/api/stream)."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import queue
import secrets
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from consilium import __version__
from consilium.models import utc_now_iso
from consilium.quota import sync_local_quotas
from consilium.store import BusStore
from consilium.wakeup import query_active_waiters

SESSION_COOKIE_NAME = "consilium_session"
SESSION_TTL_SEC = 12 * 3600.0
SSE_MAX_SUBSCRIBERS = 16


@dataclass
class SessionRecord:
    session_id: str
    csrf_token: str
    token_version: int
    created_at: float
    expires_at: float


class SessionStore:
    """In-memory random server-side session registry with token-version binding and 12h TTL."""

    def __init__(self, ttl_sec: float = SESSION_TTL_SEC, max_sessions: int = 64) -> None:
        self._lock = threading.Lock()
        self._sessions: Dict[str, SessionRecord] = {}
        self._ttl_sec = max(60.0, float(ttl_sec))
        self._max_sessions = max(4, int(max_sessions))

    def _prune_locked(self, now: float) -> None:
        expired = [sid for sid, rec in self._sessions.items() if rec.expires_at <= now]
        for sid in expired:
            self._sessions.pop(sid, None)

    def create(self, token_version: int) -> SessionRecord:
        now = time.time()
        with self._lock:
            self._prune_locked(now)
            if len(self._sessions) >= self._max_sessions:
                oldest = min(self._sessions.items(), key=lambda item: item[1].created_at)[0]
                self._sessions.pop(oldest, None)
            rec = SessionRecord(
                session_id=secrets.token_urlsafe(32),
                csrf_token=secrets.token_urlsafe(24),
                token_version=int(token_version),
                created_at=now,
                expires_at=now + self._ttl_sec,
            )
            self._sessions[rec.session_id] = rec
            return rec

    def get(self, session_id: str, current_token_version: int) -> Optional[SessionRecord]:
        if not session_id:
            return None
        now = time.time()
        with self._lock:
            self._prune_locked(now)
            rec = self._sessions.get(session_id)
            if rec is None:
                return None
            if rec.expires_at <= now or rec.token_version != int(current_token_version):
                self._sessions.pop(session_id, None)
                return None
            return rec

    def revoke(self, session_id: str) -> bool:
        if not session_id:
            return False
        with self._lock:
            return self._sessions.pop(session_id, None) is not None

    def revoke_all(self) -> None:
        with self._lock:
            self._sessions.clear()


def parse_cookie_header(cookie_header: str) -> Dict[str, str]:
    """Parse an HTTP Cookie header into a key/value dictionary."""
    cookies: Dict[str, str] = {}
    if not cookie_header:
        return cookies
    for part in cookie_header.split(";"):
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        clean_name = name.strip()
        if clean_name:
            cookies[clean_name] = value.strip()
    return cookies


class LoginTicketStore:
    """Short-lived, single-use local login tickets bound to the current token version."""

    def __init__(self, max_pending: int = 32) -> None:
        self._lock = threading.Lock()
        self._tickets: Dict[str, Tuple[float, int]] = {}
        self._max_pending = max(4, int(max_pending))

    def _prune_locked(self, now: float) -> None:
        expired = [k for k, (exp, _) in self._tickets.items() if exp <= now]
        for k in expired:
            self._tickets.pop(k, None)

    def issue(self, token_version: int = 1, ttl_sec: float = 30.0) -> str:
        now = time.monotonic()
        ttl = max(5.0, min(120.0, float(ttl_sec)))
        ticket = secrets.token_urlsafe(24)
        with self._lock:
            self._prune_locked(now)
            if len(self._tickets) >= self._max_pending:
                oldest = min(self._tickets.items(), key=lambda item: item[1][0])[0]
                self._tickets.pop(oldest, None)
            self._tickets[ticket] = (now + ttl, int(token_version))
        return ticket

    def consume(self, ticket: str, current_token_version: int = 1) -> bool:
        if not ticket:
            return False
        now = time.monotonic()
        with self._lock:
            self._prune_locked(now)
            item = self._tickets.pop(ticket, None)
        if item is None:
            return False
        exp, tv = item
        return exp >= now and tv == int(current_token_version)

    def revoke_all(self) -> None:
        with self._lock:
            self._tickets.clear()


@dataclass
class _SSESubscriber:
    queue: "queue.Queue[Tuple[int, str, Dict[str, Any]]]"
    session_id: str
    token_version: int
    closed_event: threading.Event


class EventHub:
    """Thread-safe pub/sub hub with single monotonic version, bounded subscribers, and active close."""

    def __init__(self, max_subscribers: int = SSE_MAX_SUBSCRIBERS) -> None:
        self._lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._version = 0
        self._subscribers: List[_SSESubscriber] = []
        self._max_subscribers = max(1, int(max_subscribers))

    def subscribe(
        self,
        *,
        session_id: str = "",
        token_version: int = 0,
    ) -> Optional[_SSESubscriber]:
        with self._lock:
            if len(self._subscribers) >= self._max_subscribers:
                return None
            sub = _SSESubscriber(
                queue=queue.Queue(maxsize=128),
                session_id=session_id,
                token_version=int(token_version),
                closed_event=threading.Event(),
            )
            self._subscribers.append(sub)
            return sub

    def unsubscribe(self, sub: Optional[_SSESubscriber]) -> None:
        if sub is None:
            return
        sub.closed_event.set()
        with self._lock:
            if sub in self._subscribers:
                self._subscribers.remove(sub)

    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    def next_version(self) -> int:
        with self._lock:
            self._version += 1
            return self._version

    def current_version(self) -> int:
        with self._lock:
            return self._version

    @staticmethod
    def _enqueue(sub: _SSESubscriber, item: Tuple[int, str, Dict[str, Any]]) -> None:
        try:
            sub.queue.put_nowait(item)
        except queue.Full:
            try:
                sub.queue.get_nowait()
            except queue.Empty:
                pass
            try:
                sub.queue.put_nowait(item)
            except queue.Full:
                pass

    def publish_state(
        self,
        event_name: str,
        reason: str,
        state_builder: Callable[[], Dict[str, Any]],
    ) -> int:
        """Build snapshot, assign monotonic version, and enqueue to subscribers in strict order."""
        with self._publish_lock:
            snapshot = state_builder()
            with self._lock:
                self._version += 1
                ver = self._version
                targets = [s for s in self._subscribers if not s.closed_event.is_set()]
            snapshot["state_version"] = ver
            payload = {"reason": reason, "version": ver, "state": snapshot}
            for sub in targets:
                self._enqueue(sub, (ver, event_name, payload))
            return ver

    def snapshot_with_version(
        self,
        state_builder: Callable[[], Dict[str, Any]],
    ) -> Tuple[int, Dict[str, Any]]:
        """Build a snapshot linearized against publish_state and assign a monotonic state_version."""
        with self._publish_lock:
            snapshot = state_builder()
            with self._lock:
                self._version += 1
                ver = self._version
            snapshot["state_version"] = ver
            return ver, snapshot

    def close_all(self) -> None:
        """Actively terminate all active SSE streams (e.g., on token rotation or daemon stop)."""
        with self._lock:
            targets = list(self._subscribers)
        for sub in targets:
            sub.closed_event.set()
            self._enqueue(sub, (0, "__close__", {}))

    def close_for_session(self, session_id: str) -> None:
        """Terminate any active SSE streams belonging to a revoked session_id."""
        if not session_id:
            return
        with self._lock:
            targets = [s for s in self._subscribers if s.session_id == session_id]
        for sub in targets:
            sub.closed_event.set()
            self._enqueue(sub, (0, "__close__", {}))


def _strip_volatile_fields(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_strip_volatile_fields(x) for x in obj]
    if isinstance(obj, dict):
        return {
            k: _strip_volatile_fields(v)
            for k, v in obj.items()
            if k not in ("age_seconds", "claim_age_seconds")
        }
    return obj


def compute_state_fingerprint(state: Dict[str, Any]) -> str:
    """Compute a stable SHA-256 fingerprint over all visible dashboard state (ignoring clock ticks)."""
    relevant = {
        "peers": _strip_volatile_fields(state.get("peers") or []),
        "active_waiters": state.get("active_waiters") or {},
        "quotas": state.get("quotas") or [],
        "missions": _strip_volatile_fields(state.get("missions") or []),
        "latest_event_id": int(state.get("latest_event_id", 0)),
    }
    encoded = json.dumps(relevant, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_dashboard_state(
    store: BusStore,
    rendezvous_dir: str,
    *,
    started_at: str,
    started_monotonic: float,
    host: str,
    port: int,
    sse_clients: int = 0,
    sync_quotas: bool = True,
) -> Dict[str, Any]:
    """Build the complete JSON snapshot returned by GET /api/state and SSE updates."""
    if sync_quotas:
        try:
            sync_local_quotas(store)
        except Exception:
            pass
    missions, latest_event_id = store.get_dashboard_missions(limit=20, events_per_mission=100)
    peers = store.list_peers()
    active_waiters = query_active_waiters(rendezvous_dir)
    quotas_by_peer = {
        q["peer_id"]: q for q in store.list_quota_windows()["peers"]
    }
    return {
        "daemon": {
            "version": __version__,
            "started_at": started_at,
            "uptime_seconds": round(max(0.0, time.monotonic() - started_monotonic), 1),
            "host": host,
            "port": port,
            "sse_clients": sse_clients,
        },
        "peers": peers,
        "active_waiters": active_waiters,
        "quotas": [quotas_by_peer[p["peer_id"]] for p in peers if p["peer_id"] in quotas_by_peer],
        "missions": missions,
        "latest_event_id": latest_event_id,
        "server_time": utc_now_iso(),
    }


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN" data-theme="system">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Consilium · 三端会诊看板</title>
<style>
:root {
  --bg-canvas: #f4f6f9;
  --bg-surface: #ffffff;
  --bg-subtle: #f8fafc;
  --bg-elevated: #ffffff;
  --bg-hover: #f1f5f9;
  --border: rgba(15, 23, 42, 0.09);
  --border-strong: rgba(15, 23, 42, 0.16);
  --text-primary: #0f172a;
  --text-secondary: #475569;
  --text-muted: #64748b;
  --shadow-sm: 0 1px 2px rgba(15, 23, 42, 0.04), 0 1px 1px rgba(15, 23, 42, 0.03);
  --shadow-md: 0 8px 24px rgba(15, 23, 42, 0.07), 0 2px 6px rgba(15, 23, 42, 0.04);
  --shadow-modal: 0 24px 60px rgba(15, 23, 42, 0.22);

  --peer-claude: #c8643c;
  --peer-claude-soft: rgba(200, 100, 60, 0.11);
  --peer-chatgpt: #0d9470;
  --peer-chatgpt-soft: rgba(13, 148, 112, 0.11);
  --peer-antigravity: #4763e4;
  --peer-antigravity-soft: rgba(71, 99, 228, 0.11);

  --status-standby: #16a34a;
  --status-standby-soft: rgba(22, 163, 74, 0.12);
  --status-working: #2563eb;
  --status-working-soft: rgba(37, 99, 235, 0.12);
  --status-offline: #94a3b8;
  --status-offline-soft: rgba(148, 163, 184, 0.16);
  --status-stale: #ea580c;
  --status-stale-soft: rgba(234, 88, 12, 0.13);
  --status-danger: #dc2626;
  --status-danger-soft: rgba(220, 38, 38, 0.11);

  --bar-track: #e2e8f0;
  --radius-sm: 8px;
  --radius-md: 12px;
  --radius-lg: 16px;
  --font-sans: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "PingFang SC", "Helvetica Neue", sans-serif;
  --font-mono: "SF Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  --t: 150ms cubic-bezier(0.2, 0.8, 0.2, 1);
}

@media (prefers-color-scheme: dark) {
  :root[data-theme="system"] {
    --bg-canvas: #0b0e14;
    --bg-surface: #131822;
    --bg-subtle: #181e2b;
    --bg-elevated: #1c2333;
    --bg-hover: #212a3d;
    --border: rgba(248, 250, 252, 0.08);
    --border-strong: rgba(248, 250, 252, 0.16);
    --text-primary: #f1f5f9;
    --text-secondary: #cbd5e1;
    --text-muted: #94a3b8;
    --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.35);
    --shadow-md: 0 10px 28px rgba(0, 0, 0, 0.42);
    --shadow-modal: 0 24px 64px rgba(0, 0, 0, 0.65);

    --peer-claude: #eb8a63;
    --peer-claude-soft: rgba(235, 138, 99, 0.16);
    --peer-chatgpt: #34d3a3;
    --peer-chatgpt-soft: rgba(52, 211, 163, 0.15);
    --peer-antigravity: #7c96ff;
    --peer-antigravity-soft: rgba(124, 150, 255, 0.16);

    --status-standby: #22c55e;
    --status-standby-soft: rgba(34, 197, 94, 0.16);
    --status-working: #60a5fa;
    --status-working-soft: rgba(96, 165, 250, 0.16);
    --status-offline: #64748b;
    --status-offline-soft: rgba(100, 116, 139, 0.22);
    --status-stale: #fb923c;
    --status-stale-soft: rgba(251, 146, 60, 0.18);
    --status-danger: #f87171;
    --status-danger-soft: rgba(248, 113, 113, 0.16);

    --bar-track: rgba(255, 255, 255, 0.09);
  }
}

:root[data-theme="dark"] {
  --bg-canvas: #0b0e14;
  --bg-surface: #131822;
  --bg-subtle: #181e2b;
  --bg-elevated: #1c2333;
  --bg-hover: #212a3d;
  --border: rgba(248, 250, 252, 0.08);
  --border-strong: rgba(248, 250, 252, 0.16);
  --text-primary: #f1f5f9;
  --text-secondary: #cbd5e1;
  --text-muted: #94a3b8;
  --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.35);
  --shadow-md: 0 10px 28px rgba(0, 0, 0, 0.42);
  --shadow-modal: 0 24px 64px rgba(0, 0, 0, 0.65);

  --peer-claude: #eb8a63;
  --peer-claude-soft: rgba(235, 138, 99, 0.16);
  --peer-chatgpt: #34d3a3;
  --peer-chatgpt-soft: rgba(52, 211, 163, 0.15);
  --peer-antigravity: #7c96ff;
  --peer-antigravity-soft: rgba(124, 150, 255, 0.16);

  --status-standby: #22c55e;
  --status-standby-soft: rgba(34, 197, 94, 0.16);
  --status-working: #60a5fa;
  --status-working-soft: rgba(96, 165, 250, 0.16);
  --status-offline: #64748b;
  --status-offline-soft: rgba(100, 116, 139, 0.22);
  --status-stale: #fb923c;
  --status-stale-soft: rgba(251, 146, 60, 0.18);
  --status-danger: #f87171;
  --status-danger-soft: rgba(248, 113, 113, 0.16);

  --bar-track: rgba(255, 255, 255, 0.09);
}

*, *::before, *::after { box-sizing: border-box; }
html, body {
  margin: 0;
  padding: 0;
  background: var(--bg-canvas);
  color: var(--text-primary);
  font-family: var(--font-sans);
  font-size: 14px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
  transition: background-color var(--t), color var(--t);
}

.nums { font-variant-numeric: tabular-nums; }
.mono { font-family: var(--font-mono); }

.app-shell {
  max-width: 1320px;
  margin: 0 auto;
  padding: 20px 24px 48px;
}

/* Top Bar */
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
  padding: 14px 20px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-sm);
  margin-bottom: 20px;
  transition: background-color var(--t), border-color var(--t);
}
.brand {
  display: flex;
  align-items: center;
  gap: 12px;
}
.brand-mark {
  width: 34px;
  height: 34px;
  border-radius: 10px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  display: grid;
  place-items: center;
  flex-shrink: 0;
}
.brand-title {
  font-size: 16px;
  font-weight: 650;
  letter-spacing: -0.02em;
  display: flex;
  align-items: baseline;
  gap: 8px;
}
.brand-sub {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-muted);
}
.topbar-meta {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
.meta-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 10px;
  border-radius: 999px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  font-size: 12px;
  color: var(--text-secondary);
  transition: background-color var(--t), border-color var(--t);
}
.meta-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--status-offline);
  transition: background-color var(--t);
}
.meta-dot.live { background: var(--status-standby); }
.meta-dot.poll { background: var(--status-stale); }

.theme-seg {
  display: inline-flex;
  padding: 3px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: 999px;
  gap: 2px;
}
.theme-btn {
  border: none;
  background: transparent;
  color: var(--text-muted);
  font-family: inherit;
  font-size: 12px;
  font-weight: 500;
  padding: 4px 10px;
  border-radius: 999px;
  cursor: pointer;
  transition: background-color var(--t), color var(--t);
}
.theme-btn:hover { color: var(--text-primary); }
.theme-btn.active {
  background: var(--bg-surface);
  color: var(--text-primary);
  box-shadow: var(--shadow-sm);
}

.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  border: 1px solid var(--border-strong);
  background: var(--bg-surface);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 12.5px;
  font-weight: 550;
  padding: 6px 12px;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: background-color var(--t), border-color var(--t), opacity var(--t);
}
.btn:hover:not(:disabled) { background: var(--bg-hover); }
.btn:disabled { opacity: 0.45; cursor: not-allowed; }
.btn-primary {
  background: var(--text-primary);
  color: var(--bg-surface);
  border-color: transparent;
}
.btn-primary:hover:not(:disabled) { opacity: 0.88; background: var(--text-primary); }
.btn-danger {
  color: var(--status-danger);
  border-color: rgba(220, 38, 38, 0.28);
  background: var(--status-danger-soft);
}
.btn-danger:hover:not(:disabled) { border-color: var(--status-danger); }
.btn-subtle-danger:hover:not(:disabled) {
  color: var(--status-danger);
  border-color: rgba(220, 38, 38, 0.35);
  background: var(--status-danger-soft);
}

/* Section Header */
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 0 2px 12px;
}
.section-title {
  font-size: 13px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--text-muted);
}
.section-count {
  font-size: 12px;
  color: var(--text-muted);
}

/* Three Peer Cards */
.peers-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}
.peer-card {
  position: relative;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 18px 20px;
  box-shadow: var(--shadow-sm);
  overflow: hidden;
  transition: background-color var(--t), border-color var(--t), transform var(--t);
}
.peer-card::before {
  content: "";
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 3px;
  background: var(--peer-accent, var(--border));
}
.peer-card[data-peer="claude"] { --peer-accent: var(--peer-claude); --peer-soft: var(--peer-claude-soft); }
.peer-card[data-peer="chatgpt"] { --peer-accent: var(--peer-chatgpt); --peer-soft: var(--peer-chatgpt-soft); }
.peer-card[data-peer="antigravity"] { --peer-accent: var(--peer-antigravity); --peer-soft: var(--peer-antigravity-soft); }

.peer-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 12px;
}
.peer-id-wrap {
  display: flex;
  align-items: center;
  gap: 10px;
}
.peer-badge {
  width: 30px;
  height: 30px;
  border-radius: 9px;
  background: var(--peer-soft);
  color: var(--peer-accent);
  font-weight: 700;
  font-size: 13px;
  display: grid;
  place-items: center;
}
.peer-name {
  font-size: 15px;
  font-weight: 650;
  letter-spacing: -0.01em;
}
.peer-branch-prefix {
  font-size: 11px;
  color: var(--text-muted);
}

.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 550;
  transition: background-color var(--t), color var(--t);
}
.status-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
}
.status-standby { background: var(--status-standby-soft); color: var(--status-standby); }
.status-standby .status-dot { background: var(--status-standby); }
.status-working { background: var(--status-working-soft); color: var(--status-working); }
.status-working .status-dot { background: var(--status-working); }
.status-online { background: var(--bg-subtle); color: var(--text-secondary); border: 1px solid var(--border-strong); }
.status-online .status-dot { background: var(--text-secondary); }
.status-offline { background: var(--status-offline-soft); color: var(--text-muted); opacity: 0.85; }
.status-offline .status-dot { background: var(--status-offline); }
.status-stale { background: var(--status-stale-soft); color: var(--status-stale); }
.status-stale .status-dot { background: var(--status-stale); }

.peer-session {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  font-size: 12px;
  color: var(--text-muted);
  margin-bottom: 14px;
}
.peer-session-label {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.quota-box {
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 10px 12px;
  margin-bottom: 14px;
}
.quota-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  font-size: 12px;
  margin-bottom: 6px;
}
.quota-label { color: var(--text-muted); font-weight: 500; display: inline-flex; align-items: center; gap: 6px; }
.demo-tag {
  font-size: 10px;
  font-weight: 600;
  padding: 1px 6px;
  border-radius: 999px;
  background: var(--status-stale-soft);
  color: var(--status-stale);
}
.quota-value { font-weight: 650; color: var(--text-primary); }
.quota-value.unknown { color: var(--text-muted); font-weight: 500; }
.bar-track {
  width: 100%;
  height: 6px;
  border-radius: 999px;
  background: var(--bar-track);
  overflow: hidden;
}
.bar-fill {
  height: 100%;
  border-radius: 999px;
  background: var(--peer-accent, var(--status-working));
  transition: width var(--t), background-color var(--t);
}
.bar-fill.low { background: var(--status-stale); }
.bar-fill.empty { background: var(--status-danger); }
.bar-fill.unknown { width: 0%; }
.quota-reset {
  margin-top: 5px;
  font-size: 11px;
  color: var(--text-muted);
  display: flex;
  justify-content: space-between;
}

.peer-counts {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 8px;
}
.count-chip {
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 7px 8px;
  text-align: center;
}
.count-num {
  display: block;
  font-size: 16px;
  font-weight: 700;
  line-height: 1.2;
}
.count-lbl {
  font-size: 11px;
  color: var(--text-muted);
}

/* Mission List */
.missions-stack {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.mission-card {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-sm);
  overflow: hidden;
  transition: border-color var(--t), background-color var(--t);
}
.mission-card.expanded {
  border-color: var(--border-strong);
  box-shadow: var(--shadow-md);
}
.mission-row {
  display: grid;
  grid-template-columns: minmax(0, 1.8fr) auto minmax(140px, 0.8fr) auto auto;
  align-items: center;
  gap: 16px;
  padding: 15px 20px;
  cursor: pointer;
  transition: background-color var(--t);
}
.mission-row:hover { background: var(--bg-subtle); }
.mission-title-col { min-width: 0; }
.mission-title-line {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.mission-title {
  font-size: 14.5px;
  font-weight: 650;
  letter-spacing: -0.01em;
}
.mission-id {
  font-size: 11.5px;
  color: var(--text-muted);
}
.mission-goal {
  font-size: 12.5px;
  color: var(--text-muted);
  margin-top: 2px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.peer-tag {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 9px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
}
.peer-tag.claude { background: var(--peer-claude-soft); color: var(--peer-claude); }
.peer-tag.chatgpt { background: var(--peer-chatgpt-soft); color: var(--peer-chatgpt); }
.peer-tag.antigravity { background: var(--peer-antigravity-soft); color: var(--peer-antigravity); }

.progress-wrap { min-width: 130px; }
.progress-top {
  display: flex;
  justify-content: space-between;
  font-size: 11.5px;
  color: var(--text-muted);
  margin-bottom: 5px;
}

.rounds-wrap {
  display: flex;
  align-items: center;
  gap: 7px;
  font-size: 12px;
  color: var(--text-secondary);
}
.round-dots {
  display: inline-flex;
  gap: 4px;
}
.round-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--bar-track);
  transition: background-color var(--t);
}
.round-dot.filled { background: var(--status-working); }
.round-dot.capped { background: var(--status-stale); }

.mission-status-badge {
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  white-space: nowrap;
}
.ms-active { background: var(--status-working-soft); color: var(--status-working); }
.ms-ready_to_report { background: var(--status-stale-soft); color: var(--status-stale); }
.ms-completed { background: var(--status-standby-soft); color: var(--status-standby); }
.ms-cancelled, .ms-failed { background: var(--status-danger-soft); color: var(--status-danger); }

.chevron {
  width: 18px;
  height: 18px;
  color: var(--text-muted);
  transition: transform var(--t);
}
.mission-card.expanded .chevron { transform: rotate(180deg); }

/* Mission Detail */
.mission-detail {
  border-top: 1px solid var(--border);
  padding: 18px 20px 22px;
  background: var(--bg-subtle);
}
.detail-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}
.detail-meta {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  font-size: 12px;
  color: var(--text-muted);
}
.detail-chip {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  padding: 3px 9px;
  border-radius: 6px;
}

.detail-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 16px;
}

.panel {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 16px;
}
.panel-title {
  font-size: 12.5px;
  font-weight: 650;
  color: var(--text-secondary);
  margin: 0 0 12px;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

/* Task Columns */
.kanban {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}
.kanban.cols-5 {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}
.kanban-col {
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 10px;
  min-height: 96px;
  min-width: 0;
}
.kanban-col.empty-col {
  background: transparent;
  border-style: dashed;
  opacity: 0.78;
}
.kanban-head {
  display: flex;
  justify-content: space-between;
  font-size: 11.5px;
  font-weight: 650;
  color: var(--text-muted);
  margin-bottom: 8px;
  text-transform: uppercase;
  letter-spacing: 0.03em;
}
.task-card {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px;
  margin-bottom: 8px;
  box-shadow: var(--shadow-sm);
  transition: border-color var(--t);
}
.task-card:last-child { margin-bottom: 0; }
.task-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  margin-bottom: 6px;
  flex-wrap: wrap;
}
.tier-pill {
  font-size: 10.5px;
  font-weight: 600;
  padding: 2px 6px;
  border-radius: 5px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  color: var(--text-secondary);
}
.stale-pill {
  background: var(--status-stale-soft);
  border-color: transparent;
  color: var(--status-stale);
}
.attempt-pill {
  font-size: 10.5px;
  font-weight: 550;
  padding: 2px 6px;
  border-radius: 5px;
  background: var(--bg-subtle);
  color: var(--text-muted);
}
.task-branch {
  font-size: 11px;
  color: var(--text-secondary);
  background: var(--bg-subtle);
  padding: 2px 6px;
  border-radius: 5px;
  display: inline-block;
  margin-bottom: 6px;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.task-note {
  font-size: 12.5px;
  color: var(--text-secondary);
  word-break: break-word;
  line-height: 1.55;
}
.task-note code, .tl-desc code {
  font-family: var(--font-mono);
  font-size: 11.5px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  padding: 1px 4px;
  border-radius: 4px;
}
.task-foot {
  margin-top: 8px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
}
.task-id { font-size: 10.5px; color: var(--text-muted); white-space: nowrap; }
.btn-xs { padding: 3px 8px; font-size: 11px; white-space: nowrap; flex-shrink: 0; }

/* Final Summary Markdown */
.summary-box {
  margin-top: 14px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 16px;
}
.md-body h1, .md-body h2, .md-body h3 { margin: 10px 0 6px; font-weight: 650; }
.md-body h1 { font-size: 16px; }
.md-body h2 { font-size: 14.5px; }
.md-body h3 { font-size: 13.5px; }
.md-body p { margin: 6px 0; color: var(--text-secondary); }
.md-body ul, .md-body ol { margin: 6px 0; padding-left: 20px; color: var(--text-secondary); }
.md-body li { margin: 3px 0; }
.md-body code {
  font-family: var(--font-mono);
  font-size: 12px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  padding: 1px 5px;
  border-radius: 4px;
}
.md-body pre {
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 12px;
  overflow-x: auto;
}
.md-body pre code { border: none; padding: 0; background: transparent; }

/* Event Timeline */
.timeline-wrap {
  position: relative;
}
.timeline {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.timeline-fade {
  position: relative;
  margin-top: -28px;
  padding-top: 34px;
  display: flex;
  justify-content: center;
  background: linear-gradient(to bottom, rgba(255, 255, 255, 0), var(--bg-surface) 62%);
}
:root[data-theme="dark"] .timeline-fade {
  background: linear-gradient(to bottom, rgba(19, 24, 34, 0), var(--bg-surface) 62%);
}
@media (prefers-color-scheme: dark) {
  :root[data-theme="system"] .timeline-fade {
    background: linear-gradient(to bottom, rgba(19, 24, 34, 0), var(--bg-surface) 62%);
  }
}
.timeline-more-row {
  margin-top: 10px;
  display: flex;
  justify-content: center;
}
.tl-item {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr);
  gap: 10px;
  align-items: start;
}
.tl-icon {
  width: 28px;
  height: 28px;
  border-radius: 8px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  display: grid;
  place-items: center;
  color: var(--text-secondary);
  flex-shrink: 0;
}
.tl-icon.guard { color: var(--status-stale); background: var(--status-stale-soft); border-color: transparent; }
.tl-icon.done { color: var(--status-standby); background: var(--status-standby-soft); border-color: transparent; }
.tl-body {
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: 9px;
  padding: 9px 11px;
}
.tl-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 4px;
}
.tl-kind { font-size: 12px; font-weight: 650; }
.tl-time { font-size: 11px; color: var(--text-muted); }
.tl-badges {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  margin-bottom: 4px;
}
.delivery-pill {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 11px;
  font-weight: 600;
  padding: 2px 7px;
  border-radius: 999px;
}
.delivery-hot { background: var(--status-standby-soft); color: var(--status-standby); }
.delivery-cold { background: var(--status-stale-soft); color: var(--status-stale); }
.delivery-err { background: var(--status-danger-soft); color: var(--status-danger); }
.delivery-claim { background: var(--status-working-soft); color: var(--status-working); }
.delivery-muted { background: var(--status-offline-soft); color: var(--text-muted); }
.tl-desc {
  font-size: 12px;
  color: var(--text-secondary);
  word-break: break-word;
}

/* Auth Overlay & Modal */
.overlay {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  backdrop-filter: blur(6px);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
  z-index: 100;
}
.overlay.hidden { display: none; }
.modal {
  width: 100%;
  max-width: 440px;
  background: var(--bg-surface);
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-lg);
  padding: 24px;
  box-shadow: var(--shadow-modal);
}
.modal h2 { margin: 0 0 8px; font-size: 17px; font-weight: 650; }
.modal p { margin: 0 0 14px; font-size: 13px; color: var(--text-secondary); }
.input, .textarea, .select {
  width: 100%;
  font-family: inherit;
  font-size: 13.5px;
  color: var(--text-primary);
  background: var(--bg-subtle);
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-sm);
  padding: 9px 12px;
  margin-bottom: 12px;
  outline: none;
  transition: border-color var(--t);
}
.input:focus, .textarea:focus, .select:focus { border-color: var(--peer-antigravity); }
.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 6px;
}
.err-text {
  color: var(--status-danger);
  font-size: 12px;
  margin: -4px 0 10px;
  min-height: 16px;
}
.empty-state {
  background: var(--bg-surface);
  border: 1px dashed var(--border-strong);
  border-radius: var(--radius-lg);
  padding: 36px 20px;
  text-align: center;
  color: var(--text-muted);
}

@media (max-width: 1120px) {
  .kanban, .kanban.cols-5 { grid-template-columns: repeat(2, minmax(0, 1fr)) !important; }
}
@media (max-width: 960px) {
  .detail-grid { grid-template-columns: 1fr; }
  .mission-row { grid-template-columns: 1fr auto; }
  .progress-wrap, .rounds-wrap { grid-column: 1 / -1; }
}
@media (max-width: 820px) {
  .peers-grid { grid-template-columns: 1fr; }
  .app-shell { padding: 14px 14px 36px; }
}
@media (max-width: 640px) {
  .kanban, .kanban.cols-5 { grid-template-columns: 1fr !important; }
}
</style>
</head>
<body>
<div class="app-shell">
  <header class="topbar">
    <div class="brand">
      <div class="brand-mark" aria-hidden="true">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
          <circle cx="12" cy="5.5" r="3" fill="var(--peer-claude)"/>
          <circle cx="6" cy="17" r="3" fill="var(--peer-chatgpt)"/>
          <circle cx="18" cy="17" r="3" fill="var(--peer-antigravity)"/>
          <path d="M10.4 8.1L7.5 14.2M13.6 8.1L16.5 14.2M9.2 17H14.8" stroke="var(--text-muted)" stroke-width="1.5" stroke-linecap="round"/>
        </svg>
      </div>
      <div>
        <div class="brand-title">
          Consilium
          <span class="brand-sub">三端会诊看板</span>
        </div>
      </div>
    </div>
    <div class="topbar-meta">
      <span class="meta-pill" id="conn-pill"><span class="meta-dot" id="conn-dot"></span><span id="conn-label">连接中</span></span>
      <span class="meta-pill nums">运行 <strong id="uptime-val" class="mono">00:00:00</strong></span>
      <span class="meta-pill mono nums" id="version-pill">v__VERSION__</span>
      <div class="theme-seg" role="group" aria-label="主题切换">
        <button class="theme-btn" data-set-theme="light">浅色</button>
        <button class="theme-btn" data-set-theme="dark">深色</button>
        <button class="theme-btn" data-set-theme="system">跟随系统</button>
      </div>
      <button class="btn" id="logout-btn" title="退出当前令牌会话">退出</button>
    </div>
  </header>

  <section aria-label="三端状态">
    <div class="section-head">
      <span class="section-title">三端状态 · Peers</span>
      <span class="section-count nums" id="peers-summary"></span>
    </div>
    <div class="peers-grid" id="peers-grid"></div>
  </section>

  <section aria-label="Mission 列表">
    <div class="section-head">
      <span class="section-title">会诊任务 · Missions</span>
      <span class="section-count nums" id="missions-summary"></span>
    </div>
    <div class="missions-stack" id="missions-stack"></div>
  </section>
</div>

<!-- Token Login Overlay -->
<div class="overlay hidden" id="login-overlay" role="dialog" aria-modal="true" aria-labelledby="login-title">
  <div class="modal">
    <h2 id="login-title">登录 Consilium 看板</h2>
    <p>推荐使用一条命令免密直达（自动签发一次性票据并记住当前浏览器会话，之后不用再输）：</p>
    <p><code class="mono">python3 -m consilium.cli ui</code></p>
    <p>若手动输入，可先复制本机令牌：<code class="mono">pbcopy &lt; ~/Library/Application\ Support/consilium/auth.token</code></p>
    <input type="password" id="token-input" class="input mono" placeholder="粘贴本机 auth.token 令牌…" autocomplete="off">
    <div class="err-text" id="login-err"></div>
    <div class="modal-actions">
      <button class="btn btn-primary" id="login-submit">进入看板</button>
    </div>
  </div>
</div>

<!-- Double-Confirmation Action Modal -->
<div class="overlay hidden" id="confirm-overlay" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
  <div class="modal">
    <h2 id="confirm-title">确认操作</h2>
    <p id="confirm-desc"></p>
    <div id="confirm-extra"></div>
    <div class="err-text" id="confirm-err"></div>
    <div class="modal-actions">
      <button class="btn" id="confirm-cancel">取消</button>
      <button class="btn btn-danger" id="confirm-ok">二次确认并执行</button>
    </div>
  </div>
</div>

<script>
(function () {
  const PEER_META = {
    claude: { name: "Claude", subtitle: "Claude.app · Code", prefix: "claude/", monogram: "C" },
    chatgpt: { name: "ChatGPT / Codex", subtitle: "ChatGPT.app · Codex", prefix: "codex/", monogram: "G" },
    antigravity: { name: "Antigravity", subtitle: "Antigravity.app", prefix: "gemini/", monogram: "A" }
  };
  const STATUS_TEXT = {
    standby: "待命",
    working: "工作中",
    online: "在线",
    active: "在线",
    offline: "离线",
    stale: "陈旧"
  };
  const MISSION_STATUS_TEXT = {
    active: "进行中",
    ready_to_report: "待汇总",
    completed: "已完成",
    cancelled: "已取消",
    failed: "已失败"
  };
  const EVENT_LABELS = {
    task_assigned: "派发任务",
    progress: "进度回报",
    task_done: "任务完成",
    review_request: "交叉审核",
    loop_guard_reached: "触发轮次上限",
    task_requeued: "任务重新入队",
    task_stale: "执行超时（陈旧）",
    mission_closed: "关闭 Mission"
  };

  let appState = null;
  let csrfToken = "";
  let lastStateVersion = 0;
  let expandedMissionIds = new Set();
  let expandedTimelines = new Set();
  let autoExpandedOnce = false;
  let eventSource = null;
  let pollTimer = null;
  let sseReconnectTimer = null;
  let uptimeTimer = null;
  let baseUptime = 0;
  let baseUptimeStamp = Date.now();
  let pendingConfirmFn = null;

  function esc(str) {
    return String(str == null ? "" : str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatDuration(sec) {
    const total = Math.max(0, Math.floor(sec || 0));
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    return [h, m, s].map(v => String(v).padStart(2, "0")).join(":");
  }

  function formatRelTime(isoStr) {
    if (!isoStr) return "从未活跃";
    const dt = new Date(isoStr);
    if (isNaN(dt.getTime())) return isoStr;
    const diff = Math.max(0, Math.floor((Date.now() - dt.getTime()) / 1000));
    if (diff < 5) return "刚刚";
    if (diff < 60) return diff + " 秒前";
    if (diff < 3600) return Math.floor(diff / 60) + " 分钟前";
    if (diff < 86400) return Math.floor(diff / 3600) + " 小时前";
    return Math.floor(diff / 86400) + " 天前";
  }

  function formatClock(isoStr) {
    if (!isoStr) return "";
    const dt = new Date(isoStr);
    if (isNaN(dt.getTime())) return isoStr;
    return dt.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }

  function renderInline(text) {
    let s = esc(text);
    s = s.replace(/`([^`]+)`/g, "<code>$1</code>");
    s = s.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    return s;
  }

  function renderMarkdown(raw) {
    if (!raw) return "";
    const lines = String(raw).replace(/\r\n/g, "\n").split("\n");
    let html = "";
    let inList = null;
    let inCode = false;
    function closeList() {
      if (inList) { html += "</" + inList + ">"; inList = null; }
    }
    for (const line of lines) {
      if (line.trim().startsWith("```")) {
        closeList();
        if (!inCode) { html += "<pre><code>"; inCode = true; }
        else { html += "</code></pre>"; inCode = false; }
        continue;
      }
      if (inCode) { html += esc(line) + "\n"; continue; }
      const h3 = line.match(/^###\s+(.+)/);
      const h2 = line.match(/^##\s+(.+)/);
      const h1 = line.match(/^#\s+(.+)/);
      const ul = line.match(/^\s*[-*]\s+(.+)/);
      const ol = line.match(/^\s*\d+\.\s+(.+)/);
      if (h3) { closeList(); html += "<h3>" + renderInline(h3[1]) + "</h3>"; }
      else if (h2) { closeList(); html += "<h2>" + renderInline(h2[1]) + "</h2>"; }
      else if (h1) { closeList(); html += "<h1>" + renderInline(h1[1]) + "</h1>"; }
      else if (ul) {
        if (inList !== "ul") { closeList(); html += "<ul>"; inList = "ul"; }
        html += "<li>" + renderInline(ul[1]) + "</li>";
      } else if (ol) {
        if (inList !== "ol") { closeList(); html += "<ol>"; inList = "ol"; }
        html += "<li>" + renderInline(ol[1]) + "</li>";
      } else if (line.trim() === "") {
        closeList();
      } else {
        closeList();
        html += "<p>" + renderInline(line) + "</p>";
      }
    }
    closeList();
    if (inCode) html += "</code></pre>";
    return html;
  }

  function eventIconSvg(kind) {
    const icons = {
      task_assigned: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M13 5l7 7-7 7"/></svg>',
      progress: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>',
      task_done: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M5 13l4 4L19 7"/></svg>',
      review_request: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M7 10l5-5 5 5M17 14l-5 5-5-5"/></svg>',
      loop_guard_reached: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 9v4m0 4h.01M10.3 3.6L1.8 18a2 2 0 001.7 3h17a2 2 0 001.7-3L13.7 3.6a2 2 0 00-3.4 0z"/></svg>',
      task_stale: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 9v4m0 4h.01M10.3 3.6L1.8 18a2 2 0 001.7 3h17a2 2 0 001.7-3L13.7 3.6a2 2 0 00-3.4 0z"/></svg>',
      task_requeued: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 12a9 9 0 109-9 9.75 9.75 0 00-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>',
      mission_closed: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="3"/><path d="M9 12l2 2 4-4"/></svg>'
    };
    return icons[kind] || icons.progress;
  }

  function applyTheme(mode) {
    document.documentElement.setAttribute("data-theme", mode);
    try { localStorage.setItem("consilium-theme", mode); } catch (_) {}
    document.querySelectorAll(".theme-btn").forEach(btn => {
      btn.classList.toggle("active", btn.getAttribute("data-set-theme") === mode);
    });
  }

  function setConn(mode, label) {
    const dot = document.getElementById("conn-dot");
    const lbl = document.getElementById("conn-label");
    dot.className = "meta-dot" + (mode ? " " + mode : "");
    lbl.textContent = label;
  }

  function renderPeers(state) {
    const grid = document.getElementById("peers-grid");
    const quotasByPeer = {};
    (state.quotas || []).forEach(q => { quotasByPeer[q.peer_id] = q; });
    const orderedPeers = ["claude", "chatgpt", "antigravity"];
    const peerById = {};
    (state.peers || []).forEach(p => { peerById[p.peer_id] = p; });

    let onlineCount = 0;
    const cards = orderedPeers.map(pid => {
      const p = peerById[pid] || { peer_id: pid, status: "offline", task_counts: { queued: 0, claimed: 0, running: 0 } };
      const q = quotasByPeer[pid] || { min_remaining_percent: null, earliest_reset_in_human: "", windows: [] };
      const meta = PEER_META[pid] || { name: pid, subtitle: pid, prefix: "", monogram: pid[0].toUpperCase() };
      const st = p.status || "offline";
      if (st === "standby" || st === "working" || st === "online" || st === "active") onlineCount += 1;

      const hasQuota = q.min_remaining_percent !== null && q.min_remaining_percent !== undefined;
      const pct = hasQuota ? Math.max(0, Math.min(100, Number(q.min_remaining_percent))) : 0;
      const barCls = !hasQuota ? "unknown" : (pct <= 0 ? "empty" : (pct < 20 ? "low" : ""));
      const quotaText = hasQuota ? pct.toFixed(pct % 1 === 0 ? 0 : 1) + "%" : "未知";
      const resetText = hasQuota && q.earliest_reset_in_human ? ("重置 " + q.earliest_reset_in_human) : (hasQuota ? "无重置倒计时" : "暂无本地配额数据");
      const isDemoQuota = (q.windows || []).some(w => w.source === "demo" || w.account_id === "demo");
      const tc = p.task_counts || { queued: 0, claimed: 0, running: 0 };

      return `
        <article class="peer-card" data-peer="${esc(pid)}">
          <div class="peer-top">
            <div class="peer-id-wrap">
              <div class="peer-badge">${esc(meta.monogram)}</div>
              <div>
                <div class="peer-name">${esc(meta.name)}</div>
                <div class="peer-branch-prefix" title="${esc(meta.prefix ? ("分支前缀：" + meta.prefix) : "")}">${esc(meta.subtitle || meta.prefix)}</div>
              </div>
            </div>
            <span class="status-pill status-${esc(st)}">
              <span class="status-dot"></span>${esc(STATUS_TEXT[st] || st)}
            </span>
          </div>
          <div class="peer-session">
            <span class="peer-session-label mono" title="${esc(p.session_label || '')}">${esc(p.session_label || "未绑定会话")}</span>
            <span class="nums">${esc(formatRelTime(p.last_seen_at))}</span>
          </div>
          <div class="quota-box">
            <div class="quota-head">
              <span class="quota-label">剩余配额${isDemoQuota ? `<span class="demo-tag">演示数据</span>` : ""}</span>
              <span class="quota-value nums ${hasQuota ? "" : "unknown"}">${esc(quotaText)}</span>
            </div>
            <div class="bar-track"><div class="bar-fill ${barCls}" style="width:${hasQuota ? pct : 0}%"></div></div>
            <div class="quota-reset nums">
              <span>${esc(resetText)}</span>
              <span>${q.windows && q.windows.length ? (q.windows.length + " 个窗口") : ""}</span>
            </div>
          </div>
          <div class="peer-counts nums">
            <div class="count-chip"><span class="count-num">${tc.queued || 0}</span><span class="count-lbl">排队</span></div>
            <div class="count-chip"><span class="count-num">${tc.claimed || 0}</span><span class="count-lbl">已认领</span></div>
            <div class="count-chip"><span class="count-num">${tc.running || 0}</span><span class="count-lbl">执行中</span></div>
          </div>
        </article>`;
    });
    grid.innerHTML = cards.join("");
    document.getElementById("peers-summary").textContent = onlineCount + " / 3 端活跃";
  }

  function renderTaskBoard(mission) {
    const tasks = mission.tasks || [];
    const closed = ["completed", "cancelled", "failed"].includes(mission.status);
    const cols = [
      { key: "queued", label: "排队", filter: s => s === "queued" },
      { key: "claimed", label: "已认领", filter: s => s === "claimed" },
      { key: "running", label: "执行中", filter: s => s === "running" },
      { key: "done", label: "已完成", filter: s => s === "done" },
      { key: "other", label: "阻塞 / 其他", filter: s => ["failed", "blocked", "cancelled"].includes(s) }
    ];
    const visibleCols = cols.filter(c => c.key !== "other" || tasks.some(t => c.filter(t.status)));
    const cls = visibleCols.length > 4 ? "kanban cols-5" : "kanban";
    const anyNonEmpty = visibleCols.some(c => tasks.some(t => c.filter(t.status)));
    const colTrack = anyNonEmpty
      ? visibleCols.map(c => tasks.some(t => c.filter(t.status)) ? "minmax(220px, 1fr)" : "minmax(84px, 0.34fr)").join(" ")
      : `repeat(${visibleCols.length}, minmax(0, 1fr))`;

    return `<div class="${cls}" style="grid-template-columns:${colTrack}">` + visibleCols.map(col => {
      const items = tasks.filter(t => col.filter(t.status));
      const isEmpty = items.length === 0;
      const cardsHtml = !isEmpty ? items.map(t => {
        const canRequeue = !closed && ["claimed", "running", "failed", "blocked"].includes(t.status);
        const noteRaw = t.result_summary || t.progress_note || t.prompt || "";
        const noteClip = noteRaw.length > 180 ? (noteRaw.slice(0, 180) + "…") : noteRaw;
        const stalePill = (t.running_stale || t.claim_stale) ? `<span class="tier-pill stale-pill">陈旧</span>` : "";
        const histCnt = Array.isArray(t.attempt_history) ? t.attempt_history.length : 0;
        const attemptPill = histCnt > 0 ? `<span class="attempt-pill nums" title="已归档历史尝试 ${histCnt} 次">第 ${Number(t.attempt_seq || (histCnt + 1))} 次</span>` : "";
        return `
          <div class="task-card">
            <div class="task-top">
              <span class="peer-tag ${esc(t.assignee_peer)}">${esc(t.assignee_peer)}</span>
              <span style="display:inline-flex;gap:4px;align-items:center;flex-wrap:wrap;">
                ${stalePill}${attemptPill}
                <span class="tier-pill mono">${esc(t.model_tier)}</span>
              </span>
            </div>
            ${t.worktree_branch ? `<div class="task-branch mono" title="${esc(t.worktree_branch)}">${esc(t.worktree_branch)}</div>` : ""}
            <div class="task-note">${renderInline(noteClip)}</div>
            <div class="task-foot">
              <span class="task-id mono">${esc(t.task_id)}</span>
              ${canRequeue ? `<button class="btn btn-xs" data-requeue-task="${esc(t.task_id)}">重新入队</button>` : ""}
            </div>
          </div>`;
      }).join("") : `<div style="font-size:11.5px;color:var(--text-muted);padding:6px 0;">空</div>`;
      return `<div class="kanban-col${isEmpty ? " empty-col" : ""}"><div class="kanban-head"><span>${col.label}</span><span class="nums">${items.length}</span></div>${cardsHtml}</div>`;
    }).join("") + `</div>`;
  }

  function renderTimeline(events, missionId) {
    if (!events || !events.length) {
      return `<div style="font-size:12.5px;color:var(--text-muted);">暂无事件</div>`;
    }
    const LIMIT = 6;
    const isExpanded = expandedTimelines.has(missionId);
    const hasMore = events.length > LIMIT;
    const visibleEvents = (hasMore && !isExpanded) ? events.slice(0, LIMIT) : events;

    const itemsHtml = visibleEvents.map(ev => {
      const p = ev.payload || {};
      const mode = p.delivery_mode || "";
      const lat = p.wakeup_latency_ms;
      const claimLat = p.claim_latency_ms;
      let badges = [];
      if (claimLat !== undefined && claimLat !== null) {
        badges.push(`<span class="delivery-pill delivery-claim nums">⏱ 认领 ${Number(claimLat).toFixed(2)} ms</span>`);
      }
      if (mode === "hot_wakeup") {
        const latStr = (lat !== undefined && lat !== null) ? (" · " + Number(lat).toFixed(2) + " ms") : "";
        badges.push(`<span class="delivery-pill delivery-hot nums">⚡ 热唤醒${esc(latStr)}</span>`);
      } else if (mode === "cold_notification") {
        let cmdLabel = "🔔 冷通知";
        if (p.dry_run) cmdLabel = "🔔 冷通知 · dry-run";
        else if (p.command_ok || p.notified) cmdLabel = "🔔 已尝试 · 系统已投递";
        else if (p.delivery_attempted) cmdLabel = "🔔 已尝试 · 系统命令失败";
        badges.push(`<span class="delivery-pill delivery-cold">${esc(cmdLabel)}</span>`);
        const acked = Boolean(ev.acked_at || p.peer_acked);
        badges.push(`<span class="delivery-pill ${acked ? "delivery-hot" : "delivery-muted"}">${acked ? "✓ 对方已确认" : "待对方确认"}</span>`);
      } else if (mode === "error") {
        badges.push(`<span class="delivery-pill delivery-err">⚠ 投递异常</span>`);
      }
      if (p.round) {
        badges.push(`<span class="tier-pill mono nums">R${p.round}/${p.max_rounds || ""}</span>`);
      }
      const iconCls = (ev.kind === "loop_guard_reached" || ev.kind === "task_stale") ? "guard" : (ev.kind === "task_done" || ev.kind === "mission_closed" ? "done" : "");
      const descRaw = p.result_summary || p.progress_note || p.question || p.summary || p.reason || p.prompt_preview || p.final_summary || "";
      const descClip = String(descRaw).length > 180 ? (String(descRaw).slice(0, 180) + "…") : String(descRaw);
      return `
        <div class="tl-item">
          <div class="tl-icon ${iconCls}">${eventIconSvg(ev.kind)}</div>
          <div class="tl-body">
            <div class="tl-head">
              <span class="tl-kind">${esc(EVENT_LABELS[ev.kind] || ev.kind)} <span class="mono" style="color:var(--text-muted);font-weight:500;">${esc(ev.from_peer)} → ${esc(ev.to_peer)}</span></span>
              <span class="tl-time mono nums">#${ev.event_id} · ${esc(formatClock(ev.created_at))}</span>
            </div>
            ${badges.length ? `<div class="tl-badges">${badges.join("")}</div>` : ""}
            ${descClip ? `<div class="tl-desc">${renderInline(descClip)}</div>` : ""}
          </div>
        </div>`;
    }).join("");

    let toggleHtml = "";
    if (hasMore && !isExpanded) {
      toggleHtml = `<div class="timeline-fade"><button class="btn btn-xs" data-toggle-timeline="${esc(missionId)}">显示更多（余 ${events.length - LIMIT} 条）</button></div>`;
    } else if (hasMore && isExpanded) {
      toggleHtml = `<div class="timeline-more-row"><button class="btn btn-xs" data-toggle-timeline="${esc(missionId)}">收起时间线</button></div>`;
    }
    return `<div class="timeline-wrap"><div class="timeline">${itemsHtml}</div>${toggleHtml}</div>`;
  }

  function renderMissions(state) {
    const stack = document.getElementById("missions-stack");
    const missions = state.missions || [];
    document.getElementById("missions-summary").textContent = missions.length + " 条记录";
    if (!missions.length) {
      stack.innerHTML = `<div class="empty-state">暂无 Mission。可在任意已接入 consilium 的桌面端发起会诊，或运行 <code class="mono">run_demo_a.py</code> 驱动实时演示。</div>`;
      return;
    }
    if (!autoExpandedOnce && missions.length) {
      expandedMissionIds.add(missions[0].mission_id);
      autoExpandedOnce = true;
    }

    stack.innerHTML = missions.map(m => {
      const expanded = expandedMissionIds.has(m.mission_id);
      const tasks = m.tasks || [];
      const doneOnlyCnt = tasks.filter(t => t.status === "done").length;
      const settledCnt = tasks.filter(t => ["done", "failed", "blocked", "cancelled"].includes(t.status)).length;
      const totalCnt = tasks.length;
      const pct = totalCnt ? Math.round((settledCnt / totalCnt) * 100) : 0;
      const hasFailed = tasks.some(t => t.status === "failed");
      const hasBlocked = tasks.some(t => ["blocked", "cancelled"].includes(t.status));
      const progColor = (totalCnt > 0 && doneOnlyCnt === totalCnt)
        ? "var(--status-standby)"
        : (hasFailed ? "var(--status-danger)" : (hasBlocked ? "var(--status-stale)" : "var(--status-working)"));
      const maxR = Math.max(1, Number(m.max_rounds || 3));
      const curR = Number(m.current_round || 0);
      let dotsHtml = "";
      for (let i = 1; i <= maxR; i++) {
        const cls = i <= curR ? (curR >= maxR ? "round-dot capped" : "round-dot filled") : "round-dot";
        dotsHtml += `<span class="${cls}"></span>`;
      }
      const isClosed = ["completed", "cancelled", "failed"].includes(m.status);

      let detailHtml = "";
      if (expanded) {
        detailHtml = `
          <div class="mission-detail">
            <div class="detail-toolbar">
              <div class="detail-meta">
                ${m.workspace_path ? `<span class="detail-chip mono">${esc(m.workspace_path)}</span>` : ""}
                <span class="detail-chip">隔离策略：<strong class="mono">${esc(m.isolation_policy)}</strong></span>
                <span class="detail-chip nums">更新：${esc(formatRelTime(m.updated_at))}</span>
              </div>
              <div>
                <button class="btn btn-subtle-danger" data-close-mission="${esc(m.mission_id)}" ${isClosed ? "disabled" : ""}>关闭 Mission</button>
              </div>
            </div>
            <div class="detail-grid">
              <div class="panel">
                <div class="panel-title"><span>任务看板</span><span class="nums">${settledCnt}/${totalCnt} 完成</span></div>
                ${renderTaskBoard(m)}
              </div>
              ${m.final_summary ? `<div class="summary-box" style="margin-top:0;"><div class="panel-title"><span>最终汇总 · Final Summary</span></div><div class="md-body">${renderMarkdown(m.final_summary)}</div></div>` : ""}
              <div class="panel">
                <div class="panel-title"><span>事件时间线（倒序）</span><span class="nums">${(m.events || []).length} 条</span></div>
                ${renderTimeline(m.events || [], m.mission_id)}
              </div>
            </div>
          </div>`;
      }

      return `
        <article class="mission-card ${expanded ? "expanded" : ""}" data-mission="${esc(m.mission_id)}">
          <div class="mission-row" data-toggle-mission="${esc(m.mission_id)}">
            <div class="mission-title-col">
              <div class="mission-title-line">
                <span class="mission-title">${esc(m.title)}</span>
                <span class="mission-id mono">${esc(m.mission_id)}</span>
              </div>
              <div class="mission-goal">${esc(m.goal)}</div>
            </div>
            <div><span class="peer-tag ${esc(m.coordinator_peer)}">协调：${esc(m.coordinator_peer)}</span></div>
            <div class="progress-wrap">
              <div class="progress-top nums"><span>进度</span><span>${settledCnt}/${totalCnt}</span></div>
              <div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${progColor}"></div></div>
            </div>
            <div class="rounds-wrap nums" title="交叉审核轮次">
              <span class="round-dots">${dotsHtml}</span>
              <span class="mono">${curR}/${maxR}</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
              <span class="mission-status-badge ms-${esc(m.status)}">${esc(MISSION_STATUS_TEXT[m.status] || m.status)}</span>
              <svg class="chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 9l6 6 6-6"/></svg>
            </div>
          </div>
          ${detailHtml}
        </article>`;
    }).join("");
  }

  function applyStateData(data) {
    if (!data || typeof data !== "object") return;
    if (typeof data.csrf_token === "string" && data.csrf_token) {
      csrfToken = data.csrf_token;
    }
    const ver = Number(data.state_version || 0);
    if (ver > 0 && lastStateVersion > 0 && ver < lastStateVersion) {
      return;
    }
    if (ver > lastStateVersion) {
      lastStateVersion = ver;
    }
    showLogin(false);
    renderAll(data);
  }

  function renderAll(state) {
    appState = state;
    if (state.daemon) {
      baseUptime = Number(state.daemon.uptime_seconds || 0);
      baseUptimeStamp = Date.now();
      document.getElementById("uptime-val").textContent = formatDuration(baseUptime);
      document.getElementById("version-pill").textContent = "v" + (state.daemon.version || "__VERSION__");
    }
    renderPeers(state);
    renderMissions(state);
  }

  function showLogin(show) {
    document.getElementById("login-overlay").classList.toggle("hidden", !show);
    if (show) {
      document.getElementById("token-input").focus();
    }
  }

  function openConfirm(title, desc, extraHtml, onConfirm) {
    document.getElementById("confirm-title").textContent = title;
    document.getElementById("confirm-desc").textContent = desc;
    document.getElementById("confirm-extra").innerHTML = extraHtml || "";
    document.getElementById("confirm-err").textContent = "";
    pendingConfirmFn = onConfirm;
    document.getElementById("confirm-overlay").classList.remove("hidden");
  }

  function closeConfirm() {
    pendingConfirmFn = null;
    document.getElementById("confirm-overlay").classList.add("hidden");
  }

  async function fetchState() {
    const res = await fetch("/api/state", { credentials: "same-origin", cache: "no-store" });
    if (res.status === 401) {
      stopStreams();
      csrfToken = "";
      lastStateVersion = 0;
      setConn("", "待登录");
      showLogin(true);
      return false;
    }
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    applyStateData(data);
    return true;
  }

  function startPollingFallback() {
    if (pollTimer) return;
    setConn("poll", "轮询 3s");
    pollTimer = setInterval(() => {
      fetchState().catch(() => setConn("", "连接中断"));
    }, 3000);
  }

  function scheduleSSEReconnect() {
    if (sseReconnectTimer) return;
    sseReconnectTimer = setInterval(() => {
      if (eventSource) return;
      connectSSE();
    }, 6000);
  }

  function stopStreams() {
    if (eventSource) { eventSource.close(); eventSource = null; }
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    if (sseReconnectTimer) { clearInterval(sseReconnectTimer); sseReconnectTimer = null; }
  }

  function connectSSE() {
    if (eventSource) { eventSource.close(); eventSource = null; }
    if (typeof EventSource === "undefined") {
      startPollingFallback();
      return;
    }
    if (!pollTimer) setConn("", "连接中");
    const es = new EventSource("/api/stream");
    eventSource = es;

    es.onopen = () => {
      if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
      if (sseReconnectTimer) { clearInterval(sseReconnectTimer); sseReconnectTimer = null; }
      setConn("live", "SSE 实时");
    };
    es.addEventListener("state", ev => {
      try {
        applyStateData(JSON.parse(ev.data));
      } catch (_) {}
    });
    es.addEventListener("update", ev => {
      try {
        const payload = JSON.parse(ev.data);
        if (payload.state) applyStateData(payload.state);
      } catch (_) {}
    });
    es.onerror = () => {
      if (eventSource === es) {
        es.close();
        eventSource = null;
      } else {
        try { es.close(); } catch (_) {}
      }
      fetchState()
        .then(ok => {
          if (ok) {
            startPollingFallback();
            scheduleSSEReconnect();
          }
        })
        .catch(() => {
          startPollingFallback();
          scheduleSSEReconnect();
        });
    };
  }

  function writeHeaders() {
    const hdrs = { "Content-Type": "application/json" };
    if (csrfToken) hdrs["X-Consilium-CSRF"] = csrfToken;
    return hdrs;
  }

  // Event delegation for expand, timeline toggle, close mission, requeue task
  document.getElementById("missions-stack").addEventListener("click", ev => {
    const tlBtn = ev.target.closest("[data-toggle-timeline]");
    if (tlBtn) {
      ev.stopPropagation();
      const mid = tlBtn.getAttribute("data-toggle-timeline");
      if (expandedTimelines.has(mid)) expandedTimelines.delete(mid);
      else expandedTimelines.add(mid);
      if (appState) renderMissions(appState);
      return;
    }

    const closeBtn = ev.target.closest("[data-close-mission]");
    if (closeBtn) {
      ev.stopPropagation();
      const mid = closeBtn.getAttribute("data-close-mission");
      openConfirm(
        "关闭 Mission " + mid,
        "此操作将把该 Mission 标记为终态。若仍有未完成任务，可勾选强制取消剩余任务。",
        `<textarea id="close-summary-input" class="textarea" rows="3" placeholder="输入最终汇总说明…">通过 Consilium 看板人工确认关闭。</textarea>
         <select id="close-status-select" class="select">
           <option value="completed">completed（正常完成）</option>
           <option value="cancelled">cancelled（取消）</option>
           <option value="failed">failed（失败）</option>
         </select>
         <label style="display:flex;align-items:center;gap:6px;font-size:12.5px;margin-bottom:8px;">
           <input type="checkbox" id="close-force-check"> 强制取消未完成任务（force=true）
         </label>`,
        async () => {
          const summary = (document.getElementById("close-summary-input").value || "").trim() || "通过看板关闭。";
          const status = document.getElementById("close-status-select").value;
          const force = document.getElementById("close-force-check").checked;
          const r = await fetch("/api/missions/" + encodeURIComponent(mid) + "/close", {
            method: "POST",
            credentials: "same-origin",
            headers: writeHeaders(),
            body: JSON.stringify({ final_summary: summary, status, force })
          });
          if (r.status === 401) {
            closeConfirm();
            stopStreams();
            csrfToken = "";
            lastStateVersion = 0;
            setConn("", "待登录");
            showLogin(true);
            return;
          }
          const body = await r.json();
          if (!r.ok) throw new Error(body.error || ("HTTP " + r.status));
          await fetchState();
        }
      );
      return;
    }

    const reqBtn = ev.target.closest("[data-requeue-task]");
    if (reqBtn) {
      ev.stopPropagation();
      const tid = reqBtn.getAttribute("data-requeue-task");
      openConfirm(
        "重新入队任务 " + tid,
        "此操作将把任务状态重置为 queued，并向对应 peer 发送 task_requeued 通知。",
        `<input id="requeue-reason-input" class="input" value="通过 Consilium 看板人工重新入队">`,
        async () => {
          const reason = (document.getElementById("requeue-reason-input").value || "").trim() || "manual_requeue";
          const r = await fetch("/api/tasks/" + encodeURIComponent(tid) + "/requeue", {
            method: "POST",
            credentials: "same-origin",
            headers: writeHeaders(),
            body: JSON.stringify({ reason })
          });
          if (r.status === 401) {
            closeConfirm();
            stopStreams();
            csrfToken = "";
            lastStateVersion = 0;
            setConn("", "待登录");
            showLogin(true);
            return;
          }
          const body = await r.json();
          if (!r.ok) throw new Error(body.error || ("HTTP " + r.status));
          await fetchState();
        }
      );
      return;
    }

    const row = ev.target.closest("[data-toggle-mission]");
    if (row) {
      const mid = row.getAttribute("data-toggle-mission");
      if (expandedMissionIds.has(mid)) expandedMissionIds.delete(mid);
      else expandedMissionIds.add(mid);
      if (appState) renderMissions(appState);
    }
  });

  document.querySelectorAll("[data-set-theme]").forEach(btn => {
    btn.addEventListener("click", () => applyTheme(btn.getAttribute("data-set-theme")));
  });

  document.getElementById("login-submit").addEventListener("click", async () => {
    const token = (document.getElementById("token-input").value || "").trim();
    const errEl = document.getElementById("login-err");
    errEl.textContent = "";
    if (!token) { errEl.textContent = "请输入令牌"; return; }
    try {
      const r = await fetch("/api/login", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token })
      });
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        errEl.textContent = body.error || "令牌无效";
        return;
      }
      const loginBody = await r.json().catch(() => ({}));
      if (loginBody.csrf_token) csrfToken = loginBody.csrf_token;
      lastStateVersion = 0;
      document.getElementById("token-input").value = "";
      showLogin(false);
      await fetchState();
      connectSSE();
    } catch (e) {
      errEl.textContent = String(e);
    }
  });
  document.getElementById("token-input").addEventListener("keydown", e => {
    if (e.key === "Enter") document.getElementById("login-submit").click();
  });

  document.getElementById("logout-btn").addEventListener("click", async () => {
    stopStreams();
    await fetch("/api/logout", {
      method: "POST",
      credentials: "same-origin",
      headers: writeHeaders(),
      body: "{}"
    }).catch(() => {});
    csrfToken = "";
    lastStateVersion = 0;
    setConn("", "已退出");
    showLogin(true);
  });

  document.getElementById("confirm-cancel").addEventListener("click", closeConfirm);
  document.getElementById("confirm-ok").addEventListener("click", async () => {
    if (!pendingConfirmFn) return;
    const errEl = document.getElementById("confirm-err");
    errEl.textContent = "";
    try {
      await pendingConfirmFn();
      closeConfirm();
    } catch (e) {
      errEl.textContent = e.message || String(e);
    }
  });

  let savedTheme = "system";
  try { savedTheme = localStorage.getItem("consilium-theme") || "system"; } catch (_) {}
  applyTheme(savedTheme);

  uptimeTimer = setInterval(() => {
    if (!appState) return;
    const elapsed = baseUptime + (Date.now() - baseUptimeStamp) / 1000;
    document.getElementById("uptime-val").textContent = formatDuration(elapsed);
  }, 1000);

  fetchState().then(ok => {
    if (ok) connectSSE();
  }).catch(() => {
    setConn("", "待登录");
    showLogin(true);
  });
})();
</script>
</body>
</html>
"""


def render_dashboard_html(version: Optional[str] = None) -> str:
    """Return the self-contained Consilium dashboard HTML with daemon version stamped."""
    return DASHBOARD_HTML.replace("__VERSION__", version or __version__)
