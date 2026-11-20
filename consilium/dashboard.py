"""Offline Consilium dashboard UI (/ui), snapshot builder (/api/state), and SSE EventHub (/api/stream)."""

from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import os
import json
import queue
import secrets
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from consilium import __version__
from consilium.antigravity_models import models_for_tasks
from consilium.models import MISSION_SOURCE_UI, utc_now_iso
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


_VOLATILE_TICK_KEYS = frozenset({
    "age_seconds",
    "claim_age_seconds",
    "sample_age_seconds",
    "reset_in_seconds",
    "reset_in_human",
    "earliest_reset_in_human",
    "updated_at",
})


def _strip_volatile_fields(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_strip_volatile_fields(x) for x in obj]
    if isinstance(obj, dict):
        return {
            k: _strip_volatile_fields(v)
            for k, v in obj.items()
            if k not in _VOLATILE_TICK_KEYS
        }
    return obj


def compute_state_fingerprint(state: Dict[str, Any]) -> str:
    """Compute a stable SHA-256 fingerprint over all visible dashboard state (ignoring clock ticks)."""
    relevant = {
        "peers": _strip_volatile_fields(state.get("peers") or []),
        "active_waiters": state.get("active_waiters") or {},
        "quotas": _strip_volatile_fields(state.get("quotas") or []),
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
    missions, latest_event_id = store.get_dashboard_missions(
        limit=20, events_per_mission=100, source=MISSION_SOURCE_UI
    )
    tasks = [task for mission in missions for task in mission.get("tasks", [])]
    runtime_models = models_for_tasks(tasks)
    for task in tasks:
        model = runtime_models.get((task["task_id"], task.get("attempt_id")))
        if model:
            task["runtime_model_used"] = model
    peers = store.list_peers()
    active_waiters = query_active_waiters(rendezvous_dir)
    quotas_by_peer = {
        q["peer_id"]: q for q in store.list_quota_windows(reveal_names=True)["peers"]
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
        "preferences": store.list_preferences(),
        "peer_models": store.latest_model_by_peer(),
        "peer_efforts": store.latest_effort_by_peer(),
        "latest_event_id": latest_event_id,
        "server_time": utc_now_iso(),
    }


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN" data-theme="system">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Consilium</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns=%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22 viewBox=%220 0 128 128%22%3E%3Cpath d='M95.09 89.17A40 40 0 0 1 47.94 100.64' stroke='%234763E4' stroke-width='15.2' stroke-linecap='round' fill='none'/%3E%3Cpath d='M32.20 88.26A40 40 0 0 1 32.20 39.74' stroke='%230D9470' stroke-width='15.2' stroke-linecap='round' fill='none'/%3E%3Cpath d='M47.94 27.36A40 40 0 0 1 95.09 38.83' stroke='%23C8643C' stroke-width='15.2' stroke-linecap='round' fill='none'/%3E%3Ccircle cx='67.2' cy='64' r='9.4' fill='%23111827'/%3E%3C/svg%3E">
<style>
:root {
  color-scheme: light;
  --bg-canvas: #f8fafc;
  --bg-surface: #ffffff;
  --bg-subtle: #f1f5f9;
  --bg-elevated: #ffffff;
  --bg-hover: #e9eef5;
  --border: rgba(15, 23, 42, 0.09);
  --border-strong: rgba(15, 23, 42, 0.16);
  --text-primary: #0f172a;
  --text-secondary: #475569;
  --text-muted: #64748b;
  --accent: #2563eb;
  --accent-fg: #ffffff;
  --shadow-sm: 0 1px 2px rgba(15, 23, 42, 0.04);
  --shadow-md: 0 10px 28px rgba(15, 23, 42, 0.09), 0 2px 6px rgba(15, 23, 42, 0.04);
  --shadow-modal: 0 24px 60px rgba(15, 23, 42, 0.22);

  --peer-claude: #c8643c;
  --peer-claude-soft: rgba(200, 100, 60, 0.12);
  --peer-chatgpt: #0d9470;
  --peer-chatgpt-soft: rgba(13, 148, 112, 0.12);
  --peer-antigravity: #4763e4;
  --peer-antigravity-soft: rgba(71, 99, 228, 0.12);

  --status-standby: #16a34a;
  --status-standby-soft: rgba(22, 163, 74, 0.12);
  --status-working: #2563eb;
  --status-working-soft: rgba(37, 99, 235, 0.12);
  --status-online: #0ea5e9;
  --status-online-soft: rgba(14, 165, 233, 0.12);
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
    color-scheme: dark;
    --bg-canvas: #0b0e14;
    --bg-surface: #121722;
    --bg-subtle: #181e2b;
    --bg-elevated: #1c2333;
    --bg-hover: #222b3e;
    --border: rgba(248, 250, 252, 0.09);
    --border-strong: rgba(248, 250, 252, 0.18);
    --text-primary: #f1f5f9;
    --text-secondary: #cbd5e1;
    --text-muted: #94a3b8;
    --accent: #3b82f6;
    --accent-fg: #ffffff;
    --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.35);
    --shadow-md: 0 12px 32px rgba(0, 0, 0, 0.45);
    --shadow-modal: 0 24px 64px rgba(0, 0, 0, 0.68);

    --peer-claude: #eb8a63;
    --peer-claude-soft: rgba(235, 138, 99, 0.16);
    --peer-chatgpt: #34d3a3;
    --peer-chatgpt-soft: rgba(52, 211, 163, 0.16);
    --peer-antigravity: #7c96ff;
    --peer-antigravity-soft: rgba(124, 150, 255, 0.16);

    --status-standby: #22c55e;
    --status-standby-soft: rgba(34, 197, 94, 0.16);
    --status-working: #60a5fa;
    --status-working-soft: rgba(96, 165, 250, 0.16);
    --status-online: #38bdf8;
    --status-online-soft: rgba(56, 189, 248, 0.15);
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
  color-scheme: dark;
  --bg-canvas: #0b0e14;
  --bg-surface: #121722;
  --bg-subtle: #181e2b;
  --bg-elevated: #1c2333;
  --bg-hover: #222b3e;
  --border: rgba(248, 250, 252, 0.09);
  --border-strong: rgba(248, 250, 252, 0.18);
  --text-primary: #f1f5f9;
  --text-secondary: #cbd5e1;
  --text-muted: #94a3b8;
  --accent: #3b82f6;
  --accent-fg: #ffffff;
  --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.35);
  --shadow-md: 0 12px 32px rgba(0, 0, 0, 0.45);
  --shadow-modal: 0 24px 64px rgba(0, 0, 0, 0.68);

  --peer-claude: #eb8a63;
  --peer-claude-soft: rgba(235, 138, 99, 0.16);
  --peer-chatgpt: #34d3a3;
  --peer-chatgpt-soft: rgba(52, 211, 163, 0.16);
  --peer-antigravity: #7c96ff;
  --peer-antigravity-soft: rgba(124, 150, 255, 0.16);

  --status-standby: #22c55e;
  --status-standby-soft: rgba(34, 197, 94, 0.16);
  --status-working: #60a5fa;
  --status-working-soft: rgba(96, 165, 250, 0.16);
  --status-online: #38bdf8;
  --status-online-soft: rgba(56, 189, 248, 0.15);
  --status-offline: #64748b;
  --status-offline-soft: rgba(100, 116, 139, 0.22);
  --status-stale: #fb923c;
  --status-stale-soft: rgba(251, 146, 60, 0.18);
  --status-danger: #f87171;
  --status-danger-soft: rgba(248, 113, 113, 0.16);

  --bar-track: rgba(255, 255, 255, 0.09);
}

*, *::before, *::after { box-sizing: border-box; }
:root.no-theme-transition *, :root.no-theme-transition *::before, :root.no-theme-transition *::after { transition: none !important; }
html, body {
  margin: 0;
  padding: 0;
  height: 100%;
  overflow: hidden;
  background: var(--bg-canvas);
  color: var(--text-primary);
  font-family: var(--font-sans);
  font-size: 14px;
  line-height: 1.55;
  -webkit-font-smoothing: antialiased;
}

.tabular-nums { font-variant-numeric: tabular-nums; }
.hidden { display: none !important; }
button, input, textarea, select { font-family: inherit; color: inherit; }
button { -webkit-appearance: none; appearance: none; }

code, .mono {
  font-family: var(--font-mono);
  font-size: 12px;
}
code.inline {
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 1px 5px;
}

/* App Shell */
.app-shell {
  display: flex;
  width: 100%;
  height: 100vh;
  overflow: hidden;
  position: relative;
}

/* Left Sidebar */
.sidebar {
  width: 272px;
  flex-shrink: 0;
  background: var(--bg-surface);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  min-height: 0;
}
.sidebar.collapsed { display: none; }
.sidebar-head {
  height: 52px;
  padding: 0 14px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}
.brand-row {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}
.brand-mark {
  width: 24px;
  height: 24px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
.brand-name { font-size: 14px; font-weight: 650; letter-spacing: -0.01em; }
.sidebar-scroll {
  flex: 1;
  overflow-y: auto;
  padding: 10px;
}
.side-label {
  padding: 4px 8px 6px;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-muted);
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.mission-item {
  width: 100%;
  text-align: left;
  border: none;
  background: transparent;
  padding: 9px 10px;
  border-radius: 10px;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  gap: 3px;
  transition: background var(--t);
  margin-bottom: 3px;
}
.mission-item:hover { background: var(--bg-subtle); }
.mission-item.active { background: var(--bg-hover); }
.mission-item-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
}
.mission-item-title {
  font-size: 12.5px;
  font-weight: 550;
  color: var(--text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.mission-item-sub {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 12px;
  color: var(--text-muted);
}
.standby-banner { display: flex; align-items: center; justify-content: space-between; gap: 10px; font-size: 12.5px; color: var(--text-secondary); background: var(--bg-subtle); border-radius: 10px; padding: 8px 12px; margin-bottom: 8px; }
.standby-banner.hidden { display: none; }

/* Main Workspace */
.main-col {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  background: var(--bg-canvas);
  position: relative;
}

/* Compact Single-Row Top Bar */
.topbar {
  min-height: 52px;
  padding: 6px 12px;
  background: var(--bg-surface);
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  position: relative;
  z-index: 25;
  flex-shrink: 0;
}
.topbar-left {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  flex: 0 1 auto;
}
.icon-btn {
  width: 30px;
  height: 30px;
  border-radius: 8px;
  border: 1px solid transparent;
  background: transparent;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  color: var(--text-secondary);
  flex-shrink: 0;
  transition: background var(--t), color var(--t);
}
.icon-btn:hover { background: var(--bg-subtle); color: var(--text-primary); }
.session-title-pill {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 320px;
}

/* Dedicated Compact Peer Sub-Bar (Always Unclipped Across 1440px & 390px) */
/* Claude-like simplicity: peer status lives in the composer chips; the capsule strip is hidden. */
.peer-subbar { display: none !important; }
.btn-link, .btn-link-danger { border: none; background: transparent; padding: 2px 4px; font-size: 12.5px; cursor: pointer; color: var(--text-muted); }
.btn-link:hover { color: var(--text-primary); text-decoration: underline; }
.btn-link-danger:hover { color: #dc2626; }
.chat-user { display: flex; flex-direction: column; align-items: flex-end; gap: 4px; margin: 10px 0 18px; }
.chat-user-bubble { max-width: min(78%, 640px); background: var(--bg-subtle); border-radius: 16px; padding: 10px 14px; font-size: 14px; line-height: 1.6; overflow-wrap: anywhere; }
.chat-user-meta { font-size: 11.5px; color: var(--text-muted); }
.chat-agent { display: flex; gap: 12px; margin: 6px 0 20px; }
.chat-agent .avatar { flex-shrink: 0; }
.chat-agent-body { min-width: 0; flex: 1; }
.chat-agent-head { display: flex; align-items: baseline; gap: 8px; margin-bottom: 4px; }
.chat-agent-text { font-size: 14px; line-height: 1.65; overflow-wrap: anywhere; }
.chat-tag { font-size: 11.5px; padding: 1px 7px; border-radius: 999px; background: var(--bg-subtle); color: var(--text-muted); }
.chat-tag.bad { background: rgba(220, 38, 38, 0.1); color: #dc2626; }
.chat-note { font-size: 12.5px; color: var(--text-muted); margin: 4px 0 14px 44px; }
.chat-pending { display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--text-muted); margin: 4px 0 12px 44px; }
.chat-pending .dot { animation: pulseDot 1.4s ease-in-out infinite; }
@keyframes pulseDot { 0%, 100% { opacity: 1; } 50% { opacity: 0.35; } }
.target-chip { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--border); background: transparent; border-radius: 999px; padding: 3px 10px; font-size: 12.5px; cursor: pointer; color: var(--text-secondary); }
.target-chip.active { background: var(--text-primary); color: var(--bg-surface); border-color: var(--text-primary); }
.compose-opts { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 6px; position: relative; }
.compose-opts:empty { display: none; }
.folder-btn { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--border); background: var(--bg-surface); border-radius: 8px; padding: 3px 10px; font-size: 12.5px; cursor: pointer; color: var(--text-primary); max-width: 320px; }
.folder-btn .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.folder-btn.missing { color: #b45309; border-color: rgba(180, 83, 9, 0.4); }
.folder-menu { position: absolute; bottom: calc(100% + 6px); left: 0; z-index: 80; min-width: 300px; max-width: 460px; background: var(--bg-elevated); border: 1px solid var(--border); border-radius: 12px; box-shadow: var(--shadow-md); padding: 6px; display: flex; flex-direction: column; }
.folder-menu-label { font-size: 11px; color: var(--text-muted); padding: 4px 8px; }
.folder-item { display: flex; flex-direction: column; align-items: flex-start; gap: 1px; border: none; background: transparent; text-align: left; padding: 6px 8px; border-radius: 8px; cursor: pointer; font-size: 13px; color: var(--text-primary); }
.folder-item:hover, .folder-item.current { background: var(--bg-subtle); }
.folder-item .path { font-size: 11px; color: var(--text-muted); max-width: 420px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.folder-sep { height: 1px; background: var(--border); margin: 4px 2px; }
.folder-group { margin-top: 6px; }
.folder-group-head { display: flex; align-items: center; gap: 6px; font-size: 11.5px; color: var(--text-muted); padding: 6px 8px 2px; }
.folder-group-head .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.archived-toggle { border: none; background: transparent; font-size: 11.5px; color: var(--text-muted); padding: 8px; cursor: pointer; text-align: left; }
.title-input { font: inherit; font-size: 15px; font-weight: 650; padding: 2px 6px; border: 1px solid var(--border-strong); border-radius: 6px; background: var(--bg-surface); color: var(--text-primary); min-width: 0; flex: 1; }
.opt-chip { display: inline-flex; align-items: center; gap: 4px; border: 1px dashed var(--border-strong); background: transparent; border-radius: 999px; padding: 2px 10px; font-size: 12px; cursor: pointer; color: var(--text-secondary); max-width: 360px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.opt-chip.on { border-style: solid; color: var(--text-primary); background: var(--bg-subtle); }
.ws-compose-input { font: inherit; font-size: 12px; padding: 3px 8px; border: 1px solid var(--border-strong); border-radius: 999px; background: var(--bg-surface); color: var(--text-primary); width: 360px; }
.send-btn { width: 32px; height: 32px; border-radius: 999px; border: none; background: var(--text-primary); color: var(--bg-surface); font-size: 16px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; }
.send-btn:disabled { opacity: 0.35; cursor: default; }
.new-chat-btn { border: 1px solid var(--border); background: var(--bg-surface); border-radius: 8px; padding: 4px 10px; font-size: 12.5px; cursor: pointer; }
.new-chat-btn:hover { background: var(--bg-subtle); }
.pref-row { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; }
.peer-subbar-legacy {
  position: relative;
  z-index: 24;
  padding: 6px 14px;
  background: var(--bg-canvas);
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
.peer-pills-bar {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-wrap: wrap;
  gap: 6px;
  max-width: 100%;
  padding: 2px 0;
}
.peer-pill {
  position: relative;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px 6px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: var(--bg-surface);
  font-size: 12px;
  cursor: pointer;
  white-space: nowrap;
  flex-shrink: 0;
  transition: background var(--t), border-color var(--t);
}
.peer-pill:hover, .peer-pill.open {
  background: var(--bg-subtle);
  border-color: var(--border-strong);
}
.dot {
  width: 7px;
  height: 7px;
  border-radius: 999px;
  flex-shrink: 0;
}
.dot.standby { background: var(--status-standby); }
.dot.working { background: var(--status-working); animation: softPulse 1.6s ease-in-out infinite; }
.dot.online { background: var(--status-online); }
.dot.offline { background: var(--status-offline); }
.dot.stale { background: var(--status-stale); }
@keyframes softPulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.4; }
}
.pill-name { font-weight: 600; color: var(--text-primary); }
.pill-state { color: var(--text-muted); }
.pill-quota { font-weight: 550; color: var(--text-secondary); }
.pill-quota.low { color: var(--status-stale); }
.pill-quota.empty { color: var(--status-danger); }
.pill-quota.unknown { color: var(--text-muted); }
.pill-badge {
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 0 5px;
  font-size: 12px;
  color: var(--text-muted);
}
.pill-track {
  position: absolute;
  left: 12px;
  right: 12px;
  bottom: 1px;
  height: 2px;
  border-radius: 999px;
  background: var(--bar-track);
  overflow: hidden;
}
.pill-fill { height: 100%; border-radius: 999px; }
.pill-fill.claude { background: var(--peer-claude); }
.pill-fill.chatgpt { background: var(--peer-chatgpt); }
.pill-fill.antigravity { background: var(--peer-antigravity); }
.pill-fill.low { background: var(--status-stale) !important; }
.pill-fill.empty { background: var(--status-danger) !important; }
.pill-fill.unknown { background: transparent !important; width: 0 !important; }

/* Right Controls */
.topbar-right {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-shrink: 0;
}
.topbar-right { position: relative; }
.topbar-more {
  display: none;
  position: absolute;
  top: calc(100% + 6px);
  right: 0;
  z-index: 70;
  flex-direction: column;
  align-items: stretch;
  gap: 8px;
  padding: 10px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  box-shadow: var(--shadow-md);
  min-width: 220px;
}
.topbar-more.open { display: flex; }
.topbar-more .tab-group { flex-wrap: wrap; max-width: 280px; }
.conn-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 8px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: var(--bg-subtle);
  font-size: 12px;
  color: var(--text-muted);
  white-space: nowrap;
}
.conn-dot { width: 6px; height: 6px; border-radius: 999px; background: var(--status-offline); }
.conn-badge.live .conn-dot { background: var(--status-standby); }
.conn-badge.poll .conn-dot { background: var(--status-stale); }

.seg {
  display: inline-flex;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--bg-subtle);
  padding: 2px;
  gap: 1px;
}
.seg-btn {
  border: none;
  background: transparent;
  padding: 3px 7px;
  font-size: 12px;
  border-radius: 6px;
  cursor: pointer;
  color: var(--text-muted);
  white-space: nowrap;
  transition: background var(--t), color var(--t);
}
.seg-btn:hover { color: var(--text-primary); }
.seg-btn.active {
  background: var(--bg-surface);
  color: var(--text-primary);
  font-weight: 600;
  box-shadow: var(--shadow-sm);
}

.tab-group {
  display: inline-flex;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--bg-surface);
  padding: 2px;
  gap: 1px;
}
.tab-chip {
  border: none;
  background: transparent;
  padding: 3px 7px;
  font-size: 12px;
  border-radius: 6px;
  cursor: pointer;
  color: var(--text-secondary);
  white-space: nowrap;
  transition: background var(--t), color var(--t);
}
.tab-chip:hover { background: var(--bg-subtle); color: var(--text-primary); }
.tab-chip.active { background: var(--text-primary); color: var(--bg-surface); font-weight: 600; }

.btn-quiet {
  border: 1px solid var(--border);
  background: var(--bg-surface);
  border-radius: 8px;
  padding: 4px 9px;
  font-size: 12px;
  cursor: pointer;
  color: var(--text-secondary);
  white-space: nowrap;
}
.btn-quiet:hover { background: var(--bg-subtle); color: var(--text-primary); }

/* Peer Popover (Opaque Elevated Surface) */
.peer-popover {
  position: absolute;
  top: calc(100% + 6px);
  left: 50%;
  transform: translateX(-50%);
  width: 344px;
  max-width: calc(100vw - 24px);
  background: var(--bg-elevated);
  border: 1px solid var(--border-strong);
  border-radius: 14px;
  box-shadow: var(--shadow-md);
  padding: 14px;
  z-index: 45;
  font-size: 12.5px;
}
.pop-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 8px;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--border);
}
.pop-body { padding: 10px 0; display: flex; flex-direction: column; gap: 8px; }
.pop-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.pop-actions {
  padding-top: 10px;
  border-top: 1px solid var(--border);
  display: flex;
  gap: 8px;
}

/* Workspace Body */
.workspace-body {
  flex: 1;
  min-height: 0;
  display: flex;
  overflow: hidden;
  position: relative;
}

/* Conversation Area */
.chat-column {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  position: relative;
}
.chat-scroll {
  flex: 1;
  overflow-y: auto;
  padding: 20px 18px 28px;
}
.chat-container {
  max-width: 740px;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  gap: 18px;
}

/* Opening Mission Goal Header */
.mission-banner {
  border: 1px solid var(--border);
  background: var(--bg-surface);
  border-radius: 14px;
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.mission-banner-top {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
}
.mission-banner-title { font-size: 15px; font-weight: 650; margin: 0; }
.mission-banner-goal { font-size: 13px; color: var(--text-secondary); margin: 2px 0 0; }
.meta-chips {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  font-size: 12px;
  color: var(--text-muted);
}
.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 999px;
  background: var(--bg-subtle);
  border: 1px solid var(--border);
  font-size: 12px;
  color: var(--text-secondary);
  white-space: nowrap;
  flex-shrink: 0;
}
.meta-chips .chip.mono {
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
}
.chip-btn {
  border: 1px solid var(--border);
  background: var(--bg-surface);
  border-radius: 7px;
  padding: 2px 8px;
  font-size: 12px;
  color: var(--text-secondary);
  cursor: pointer;
}
.chip-btn:hover { background: var(--bg-subtle); color: var(--text-primary); }

.progress-row { display: flex; align-items: center; gap: 10px; }
.progress-track {
  flex: 1;
  height: 5px;
  border-radius: 999px;
  background: var(--bar-track);
  overflow: hidden;
}
.progress-fill { height: 100%; border-radius: 999px; background: var(--status-working); transition: width var(--t); }
.progress-fill.complete { background: var(--status-standby); }
.progress-fill.warn { background: var(--status-stale); }
.progress-fill.err { background: var(--status-danger); }

/* Editorial Conversation Messages */
.msg-turn {
  display: flex;
  align-items: flex-start;
  gap: 11px;
}
.avatar {
  width: 28px;
  height: 28px;
  border-radius: 999px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 700;
  flex-shrink: 0;
  margin-top: 2px;
}
.avatar.claude { background: var(--peer-claude-soft); color: var(--peer-claude); }
.avatar.chatgpt { background: var(--peer-chatgpt-soft); color: var(--peer-chatgpt); }
.avatar.antigravity { background: var(--peer-antigravity-soft); color: var(--peer-antigravity); }
.avatar.system { background: var(--bg-subtle); color: var(--text-secondary); }
/* Real app icons replace the letter avatars when the PNGs are bundled (see _peer_icon_css). */
.peer-icon { width: 16px; height: 16px; border-radius: 4px; background-size: cover; background-position: center; flex-shrink: 0; display: none; }
__PEER_ICON_CSS__

.msg-body { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 5px; }
.msg-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  flex-wrap: wrap;
}
.msg-author { font-size: 13.5px; font-weight: 600; color: var(--text-primary); }
.msg-sub { font-size: 12px; color: var(--text-muted); }
.msg-text { font-size: 14px; color: var(--text-primary); word-break: break-word; }

.trace-row { display: flex; flex-wrap: wrap; gap: 6px; }
.trace-pill {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 12px;
  padding: 2px 8px;
  border-radius: 6px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  color: var(--text-secondary);
}
.trace-pill.hot { color: var(--status-standby); }
.trace-pill.cold { color: var(--status-stale); }
.trace-pill.lat { color: var(--status-working); }

/* Folded Earlier Events Fade */
.fold-wrap { position: relative; }
.timeline-fade {
  position: relative;
  max-height: 82px;
  overflow: hidden;
  border: 1px dashed var(--border);
  border-radius: 10px;
  padding: 8px 12px;
  background: var(--bg-surface);
}
.timeline-fade::after {
  content: "";
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  height: 44px;
  background: linear-gradient(to bottom, transparent, var(--bg-surface));
  pointer-events: none;
}

/* Review Accordion & Final Summary */
.review-box {
  margin-left: 39px;
  border: 1px solid var(--border);
  background: var(--bg-surface);
  border-radius: 12px;
  padding: 9px 12px;
}
.summary-card {
  margin-left: 39px;
  border: 1px solid var(--border-strong);
  background: var(--bg-surface);
  border-radius: 14px;
  padding: 14px 16px;
  box-shadow: var(--shadow-sm);
}

/* Bottom Pure-Frontend Composer / Handoff Bar */
.composer-wrap {
  flex-shrink: 0;
  padding: 8px 18px 14px;
  border-top: 1px solid var(--border);
  background: var(--bg-surface);
  position: relative;
}
.composer-inner { max-width: 740px; margin: 0 auto; position: relative; }
.slash-palette {
  position: absolute;
  left: 0;
  bottom: calc(100% + 8px);
  width: 100%;
  max-width: 440px;
  background: var(--bg-elevated);
  border: 1px solid var(--border-strong);
  border-radius: 12px;
  box-shadow: var(--shadow-md);
  padding: 6px;
  z-index: 60;
}
.slash-item {
  width: 100%;
  text-align: left;
  border: none;
  background: transparent;
  padding: 7px 10px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  font-size: 12.5px;
  cursor: pointer;
}
.slash-item:hover { background: var(--bg-subtle); }
.slash-cmd { font-family: var(--font-mono); font-weight: 600; color: var(--accent); flex-shrink: 0; }

.composer-box {
  border: 1px solid var(--border-strong);
  border-radius: 14px;
  background: var(--bg-subtle);
  padding: 8px 10px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.composer-input {
  width: 100%;
  border: none;
  background: transparent;
  resize: none;
  font-size: 13.5px;
  color: var(--text-primary);
  outline: none;
  min-height: 22px;
  max-height: 96px;
}
.composer-foot {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  flex-wrap: wrap;
}

/* Collapsible Right Tabbed Inspector */
.right-drawer {
  width: 348px;
  max-width: 90vw;
  flex-shrink: 0;
  background: var(--bg-surface);
  border-left: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  min-height: 0;
}
.right-drawer.closed { display: none; }
.drawer-head {
  padding: 10px 12px;
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
}
.drawer-body {
  flex: 1;
  overflow-y: auto;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  font-size: 12.5px;
}
.panel-card {
  border: 1px solid var(--border);
  background: var(--bg-subtle);
  border-radius: 10px;
  padding: 10px 11px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

/* Shared Buttons */
.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 6px 12px;
  border-radius: 8px;
  border: 1px solid var(--border-strong);
  background: var(--bg-surface);
  color: var(--text-primary);
  font-size: 12.5px;
  font-weight: 550;
  cursor: pointer;
  transition: background var(--t);
}
.btn:hover { background: var(--bg-hover); }
.btn-primary { background: var(--accent); border-color: var(--accent); color: var(--accent-fg); }
.btn-primary:hover { opacity: 0.92; background: var(--accent); }
.btn-subtle-danger {
  background: transparent;
  border-color: rgba(220, 38, 38, 0.35);
  color: var(--status-danger);
}
.btn-subtle-danger:hover { background: var(--status-danger-soft); }
.btn-danger { background: var(--status-danger); border-color: var(--status-danger); color: #ffffff; }
.btn-sm { padding: 4px 9px; font-size: 12px; }

/* Login & Confirm Modals */
.overlay {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.56);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
  z-index: 80;
}
.modal {
  width: 100%;
  max-width: 420px;
  background: var(--bg-elevated);
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-modal);
  padding: 20px;
}
.modal h2 { margin: 0 0 6px; font-size: 16px; }
.modal p { margin: 0 0 12px; font-size: 13px; color: var(--text-secondary); }
.input, .textarea, .select {
  width: 100%;
  padding: 8px 10px;
  border-radius: 8px;
  border: 1px solid var(--border-strong);
  background: var(--bg-surface);
  font-size: 13px;
  margin-bottom: 10px;
}
.err-text { color: var(--status-danger); font-size: 12px; min-height: 18px; margin-bottom: 8px; }
.modal-actions { display: flex; justify-content: flex-end; gap: 8px; }

.toast {
  position: fixed;
  right: 16px;
  bottom: 16px;
  max-width: 340px;
  background: var(--bg-elevated);
  border: 1px solid var(--border-strong);
  border-radius: 10px;
  box-shadow: var(--shadow-md);
  padding: 10px 12px;
  font-size: 12.5px;
  z-index: 90;
}

/* Mobile / 390px Responsive */
@media (max-width: 860px) {
  .sidebar {
    position: fixed;
    left: 0;
    top: 0;
    bottom: 0;
    z-index: 55;
    box-shadow: var(--shadow-modal);
  }
  .right-drawer {
    position: fixed;
    right: 0;
    top: 0;
    bottom: 0;
    z-index: 55;
    box-shadow: var(--shadow-modal);
  }
  .topbar { flex-wrap: nowrap; padding: 6px 10px; position: relative; }
  .topbar-left { min-width: 0; flex: 1; }
  .topbar-right { flex-wrap: nowrap; gap: 6px; }
  .tab-chip { padding: 3px 6px; }
  .peer-subbar { padding: 6px 10px; justify-content: flex-start; }
  .peer-pills-bar { justify-content: flex-start; flex-wrap: nowrap; overflow-x: auto; scrollbar-width: none; }
  .peer-pills-bar::-webkit-scrollbar { display: none; }
  .peer-pill { padding: 4px 8px 6px; gap: 5px; }
  .session-title-pill { max-width: none; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
  .review-box, .summary-card { margin-left: 0; }
  .hide-narrow { display: none !important; }
  .chat-scroll { padding: 14px 12px 22px; }
  .composer-wrap { padding: 8px 12px 12px; }
}

/* ===== R019 "paper & ink" redesign: warm paper, serif titles, colour only for the three peers ===== */
:root {
  --bg-canvas: #F6F3EB; --bg-side: #EEEADF; --bg-surface: #FFFFFF; --bg-subtle: #EFEBE1; --bg-elevated: #FFFFFF;
  --bg-hover: #E7E2D6; --bubble: #E9E4D7;
  --border: rgba(30, 29, 26, 0.10); --border-strong: rgba(30, 29, 26, 0.17);
  --text-primary: #1E1D1A; --text-secondary: #55524A; --text-muted: #8C887D;
  --accent: #1E1D1A; --accent-fg: #FFFFFF; --bar-track: rgba(30, 29, 26, 0.10);
  --status-standby: #2E9E5B; --status-working: #4763E4; --status-online: #2E9E5B; --status-offline: #B0AB9F;
  --shadow-md: 0 1px 2px rgba(30, 29, 26, 0.05), 0 8px 28px rgba(30, 29, 26, 0.08);
  --font-serif: ui-serif, "New York", "Songti SC", "STSong", Georgia, serif;
}
@media (prefers-color-scheme: dark) {
  :root[data-theme="system"] {
    --bg-canvas: #1F1E1B; --bg-side: #191816; --bg-surface: #2A2925; --bg-subtle: #2F2D29; --bg-elevated: #2F2E2A;
    --bg-hover: #36342F; --bubble: #34322D;
    --border: rgba(236, 232, 222, 0.09); --border-strong: rgba(236, 232, 222, 0.16);
    --text-primary: #ECE8DE; --text-secondary: #B9B4A7; --text-muted: #8A8579;
    --accent: #ECE8DE; --accent-fg: #1F1E1B; --bar-track: rgba(236, 232, 222, 0.12);
    --peer-claude: #E08A62; --peer-chatgpt: #3CCB9E; --peer-antigravity: #8399FF;
    --status-standby: #4CC47F; --status-working: #8399FF; --status-online: #4CC47F; --status-offline: #6F6B62;
    --shadow-md: 0 1px 2px rgba(0, 0, 0, 0.3), 0 10px 30px rgba(0, 0, 0, 0.35);
  }
}
:root[data-theme="dark"] {
  --bg-canvas: #1F1E1B; --bg-side: #191816; --bg-surface: #2A2925; --bg-subtle: #2F2D29; --bg-elevated: #2F2E2A;
  --bg-hover: #36342F; --bubble: #34322D;
  --border: rgba(236, 232, 222, 0.09); --border-strong: rgba(236, 232, 222, 0.16);
  --text-primary: #ECE8DE; --text-secondary: #B9B4A7; --text-muted: #8A8579;
  --accent: #ECE8DE; --accent-fg: #1F1E1B; --bar-track: rgba(236, 232, 222, 0.12);
  --peer-claude: #E08A62; --peer-chatgpt: #3CCB9E; --peer-antigravity: #8399FF;
  --status-standby: #4CC47F; --status-working: #8399FF; --status-online: #4CC47F; --status-offline: #6F6B62;
  --shadow-md: 0 1px 2px rgba(0, 0, 0, 0.3), 0 10px 30px rgba(0, 0, 0, 0.35);
}
body { font-size: 14px; line-height: 1.6; -webkit-font-smoothing: antialiased; }

/* sidebar */
.sidebar { width: 264px; background: var(--bg-side); border-right: 1px solid var(--border); }
.sidebar-head { height: auto; padding: 16px 16px 8px; border-bottom: none; }
.brand-row { gap: 9px; }
.brand-mark { width: 22px; height: 22px; }
.brand-mark svg { width: 22px; height: 22px; }
.brand-name { font-family: var(--font-serif); font-size: 17px; font-weight: 600; letter-spacing: 0.01em; }
.new-row { margin: 2px 10px 4px; display: flex; align-items: center; gap: 8px; padding: 7px 10px; border-radius: 9px; border: none; background: transparent; cursor: pointer; font-size: 13.5px; font-weight: 500; color: var(--text-primary); flex-shrink: 0; }
.new-row:hover { background: var(--bg-hover); }
.new-row .plus { width: 20px; height: 20px; border-radius: 999px; background: var(--text-primary); color: var(--bg-side); display: inline-flex; align-items: center; justify-content: center; font-size: 14px; line-height: 1; }
.new-row kbd { margin-left: auto; font-family: var(--font-sans); font-size: 11px; color: var(--text-muted); }
.sidebar-scroll { padding: 2px 10px 10px; }
.folder-group { margin-top: 0; }
.folder-group-head { font-size: 11.5px; color: var(--text-muted); padding: 12px 10px 4px; letter-spacing: 0.02em; }
.mission-item { flex-direction: row; align-items: center; gap: 8px; padding: 6px 10px; border-radius: 8px; font-size: 13.5px; color: var(--text-secondary); }
.mission-item:hover { background: var(--bg-hover); }
.mission-item.active { background: var(--bg-hover); color: var(--text-primary); box-shadow: none; }
.mi-title { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.mi-live { width: 6px; height: 6px; border-radius: 999px; background: var(--status-working); animation: pulseDot 1.6s ease-in-out infinite; flex-shrink: 0; }
.mi-who { display: inline-flex; flex-shrink: 0; }
.mission-item { cursor: pointer; user-select: none; -webkit-user-select: none; }
.mission-item:focus-visible { outline: 2px solid var(--accent, #c96442); outline-offset: -2px; }
.mi-archive { display: none; flex-shrink: 0; width: 22px; height: 22px; margin: -2px -4px -2px 0; padding: 0; border: none; border-radius: 6px; background: transparent; color: var(--text-muted); cursor: pointer; align-items: center; justify-content: center; }
.mi-archive:hover { background: var(--bg-subtle); color: var(--text-primary); }
.mission-item:hover .mi-archive, .mission-item:focus-within .mi-archive { display: inline-flex; }
.mission-item:hover .mi-who, .mission-item:focus-within .mi-who { display: none; }
.mi-who .peer-icon { width: 14px; height: 14px; border-radius: 4px; margin-left: -3px; box-shadow: 0 0 0 1.5px var(--bg-side); }
.archived-toggle { font-size: 12px; padding: 10px; }

/* three-peer dock */
.dock-wrap { position: relative; flex-shrink: 0; margin: 0 10px 10px; }
.peer-dock { padding: 8px 8px 8px; background: var(--bg-surface); border: 1px solid var(--border); border-radius: 14px; }
.dock-row { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; column-gap: 8px; }
.dock-row + .dock-row { margin-top: 2px; }
.dock-main { display: grid; grid-template-columns: 26px minmax(0, 1fr); align-items: center; column-gap: 9px; border: none; background: transparent; padding: 5px 4px; border-radius: 9px; cursor: pointer; text-align: left; color: inherit; min-width: 0; }
.dock-main:hover { background: var(--bg-hover); }
.dock-ic { position: relative; width: 26px; height: 26px; }
.dock-ic .peer-icon { width: 26px; height: 26px; border-radius: 7px; display: block; }
.dock-ic i { position: absolute; right: -2px; bottom: -2px; width: 9px; height: 9px; border-radius: 999px; border: 2px solid var(--bg-surface); background: var(--status-offline); }
.dock-ic i.on { background: var(--status-standby); }
.dock-ic i.busy { background: var(--status-working); }
.dock-name { font-size: 13px; font-weight: 550; line-height: 1.25; display: block; }
.dock-sub { font-size: 11.5px; color: var(--text-muted); line-height: 1.35; display: block; white-space: normal; overflow-wrap: break-word; }
/* Model and effort get their own full-width line so a long name is never hidden behind the ring or button. */
.dock-model { grid-column: 1 / -1; padding: 0 6px 4px 39px; margin-top: -3px; font-size: 11.5px; line-height: 1.35; color: var(--text-muted); overflow-wrap: break-word; cursor: pointer; }
.dock-ring.stale { opacity: 0.5; }

/* ===== R022 right panel, after Claude Code's side panes: icon buttons in the top bar,
   a resizable panel beside the chat with one tab per kind of content ===== */
.panel-tools { display: flex; align-items: center; gap: 2px; margin-right: 2px; }
.panel-tools:empty { display: none; }
.panel-tool { position: relative; width: 30px; height: 30px; border-radius: 8px; border: none; background: transparent; color: var(--text-secondary); display: inline-flex; align-items: center; justify-content: center; cursor: pointer; }
.panel-tool:hover { background: var(--bg-hover); color: var(--text-primary); }
.panel-tool.on { background: var(--bg-hover); color: var(--text-primary); }
.panel-tool .n { position: absolute; top: 1px; right: 0; min-width: 13px; height: 13px; padding: 0 3px; border-radius: 999px; background: var(--status-working); color: #fff; font-size: 9px; line-height: 13px; text-align: center; font-weight: 600; }
.panel-sep { width: 1px; height: 16px; background: var(--border-strong); margin: 0 6px; }
.right-drawer { width: 372px; min-width: 280px; max-width: 50vw; background: var(--bg-canvas); border-left: 1px solid var(--border); position: relative; }
.drawer-grip { position: absolute; left: -3px; top: 0; bottom: 0; width: 7px; cursor: col-resize; z-index: 2; }
.drawer-grip::after { content: ""; position: absolute; left: 2px; top: 50%; width: 2px; height: 34px; margin-top: -17px; border-radius: 2px; background: var(--border-strong); opacity: 0; transition: opacity .15s; }
.drawer-grip:hover::after, .right-drawer.resizing .drawer-grip::after { opacity: 1; }
.right-drawer.resizing, .right-drawer.resizing * { user-select: none; -webkit-user-select: none; }
.drawer-head { padding: 9px 10px 0 16px; gap: 2px; justify-content: flex-start; align-items: center; }
.panel-tabs { display: flex; align-items: center; gap: 2px; min-width: 0; }
.panel-tab { border: none; background: transparent; padding: 7px 10px 9px; font-size: 13px; color: var(--text-muted); border-bottom: 2px solid transparent; margin-bottom: -1px; display: inline-flex; align-items: center; gap: 6px; cursor: pointer; white-space: nowrap; }
.panel-tab:hover { color: var(--text-primary); }
.panel-tab.on { color: var(--text-primary); border-bottom-color: var(--text-primary); font-weight: 550; }
.panel-tab small { font-size: 11px; color: var(--text-muted); font-weight: 400; }
.panel-x { margin-left: auto; width: 26px; height: 26px; border: none; background: transparent; color: var(--text-muted); border-radius: 7px; display: inline-flex; align-items: center; justify-content: center; margin-bottom: 5px; cursor: pointer; flex-shrink: 0; }
.panel-x:hover { background: var(--bg-hover); color: var(--text-primary); }
.drawer-body { padding: 14px 14px 18px 16px; gap: 0; display: block; font-size: 13px; }
.panel-empty { color: var(--text-muted); font-size: 12.5px; padding: 6px 2px; }
.pt-sum { display: flex; align-items: center; gap: 10px; font-size: 12px; color: var(--text-muted); margin: 0 2px 12px; }
.pt-bar { flex: 1; height: 4px; border-radius: 4px; background: var(--border-strong); display: flex; overflow: hidden; gap: 2px; }
.pt-bar i { display: block; height: 100%; flex: 1; }
.pt-bar i.wait { background: transparent; }
.pt-bar i.run { opacity: 0.45; }
.pt-row { display: grid; grid-template-columns: 18px minmax(0, 1fr); column-gap: 10px; padding: 10px 8px; border-radius: 10px; }
.pt-row.open { background: var(--bg-surface); box-shadow: 0 0 0 1px var(--border); }
.pt-row + .pt-row { margin-top: 2px; }
.pt-st { margin-top: 1px; display: inline-flex; }
.pt-st.done { color: var(--status-done, #2f7d57); }
.pt-st.run { color: var(--status-working); }
.pt-st.wait { color: var(--text-muted); }
.pt-st.bad { color: var(--status-failed, #c2410c); }
.pt-st.run svg { animation: ptSpin 1.1s linear infinite; }
@keyframes ptSpin { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .pt-st.run svg { animation: none; } }
.pt-title { width: 100%; border: none; background: transparent; padding: 0; text-align: left; color: var(--text-primary); font-size: 13.5px; line-height: 1.4; display: flex; gap: 6px; align-items: flex-start; cursor: pointer; }
.pt-title .peer-icon { width: 15px; height: 15px; border-radius: 4px; flex-shrink: 0; margin-top: 2px; }
.pt-title .t { flex: 1; min-width: 0; overflow-wrap: anywhere; }
.pt-title .c { color: var(--text-muted); margin-top: 3px; display: inline-flex; transition: transform .15s; }
.pt-row.open .pt-title .c { transform: rotate(90deg); }
.pt-meta { font-size: 11.5px; color: var(--text-muted); margin-top: 3px; }
.pt-note { margin-top: 8px; font-size: 12.5px; line-height: 1.55; color: var(--text-secondary); overflow-wrap: anywhere; }
.pt-note > :first-child { margin-top: 0; }
.pt-act { margin-top: 9px; display: flex; gap: 6px; flex-wrap: wrap; }
.pt-btn { border: 1px solid var(--border-strong); background: transparent; border-radius: 999px; padding: 2px 10px; font-size: 11.5px; color: var(--text-secondary); cursor: pointer; white-space: nowrap; }
.pt-btn:hover { background: var(--bg-hover); color: var(--text-primary); }
.pf-group { font-size: 11.5px; color: var(--text-muted); margin: 14px 2px 4px; letter-spacing: .02em; }
.pf-group:first-child { margin-top: 0; }
.pf-row { display: grid; grid-template-columns: 18px minmax(0, 1fr) auto; column-gap: 10px; align-items: center; padding: 9px 8px; border-radius: 10px; }
.pf-row:hover, .pf-row:focus-within { background: var(--bg-surface); box-shadow: 0 0 0 1px var(--border); }
.pf-row .ic { color: var(--text-muted); display: inline-flex; }
.pf-row .nm { font-family: var(--font-mono); font-size: 12.5px; color: var(--text-primary); overflow-wrap: anywhere; }
.pf-row .dir { font-size: 11.5px; color: var(--text-muted); margin-top: 1px; overflow-wrap: anywhere; }
.pf-row .peer-icon { width: 15px; height: 15px; border-radius: 4px; }
.pf-row .pf-act { display: none; gap: 6px; }
.pf-row:hover .pf-act, .pf-row:focus-within .pf-act { display: inline-flex; }
.pf-row:hover .pf-who, .pf-row:focus-within .pf-who { display: none; }
.settings-sheet { width: min(720px, 94vw); height: min(560px, 86vh); background: var(--bg-canvas); border: 1px solid var(--border); border-radius: 16px; box-shadow: 0 24px 60px rgba(0, 0, 0, 0.25); display: grid; grid-template-columns: 168px minmax(0, 1fr); overflow: hidden; }
.settings-nav { background: var(--bg-side); border-right: 1px solid var(--border); padding: 16px 10px; display: flex; flex-direction: column; gap: 2px; }
.settings-nav button { border: none; background: transparent; text-align: left; padding: 7px 10px; border-radius: 8px; font-size: 13.5px; color: var(--text-secondary); cursor: pointer; }
.settings-nav button:hover { background: var(--bg-hover); }
.settings-nav button.on { background: var(--bg-hover); color: var(--text-primary); font-weight: 550; }
.settings-main { display: flex; flex-direction: column; min-height: 0; }
.settings-head { display: flex; align-items: center; padding: 14px 14px 6px 20px; }
.settings-title { font-family: ui-serif, "New York", "Songti SC", serif; font-size: 18px; }
.settings-body { flex: 1; overflow-y: auto; padding: 8px 20px 20px; display: flex; flex-direction: column; gap: 10px; font-size: 12.5px; }
.dock-side { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-secondary); font-variant-numeric: tabular-nums; padding-right: 4px; }
.dock-ring { width: 22px; height: 22px; transform: rotate(-90deg); }
.dock-ring circle { fill: none; stroke-width: 3.2; }
.dock-ring .bg { stroke: var(--border-strong); }
.wake-btn { border: 1px solid var(--border-strong); background: transparent; border-radius: 999px; padding: 1px 9px; font-size: 11.5px; cursor: pointer; color: var(--text-secondary); }
.wake-btn:hover { background: var(--bg-hover); }
.dock-wrap .peer-popover { top: auto; bottom: calc(100% + 8px); left: 0; right: auto; transform: none; width: 320px; max-height: 60vh; overflow: auto; }

/* top bar */
.topbar { background: transparent; border-bottom: none; padding: 12px 18px 6px; min-height: 0; }
.topbar-left { flex: 1 1 auto; min-width: 0; gap: 8px; }
.top-title { font-family: var(--font-serif); font-size: 17px; font-weight: 600; letter-spacing: 0.01em; min-width: 0; flex: 0 1 auto; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 46vw; cursor: text; }
.top-title.plain { cursor: default; color: var(--text-muted); font-weight: 500; }
.top-rename-btn { display: inline-flex; align-items: center; gap: 4px; height: 26px; padding: 0 9px; border-radius: 999px; border: 1px solid var(--border); background: transparent; color: var(--text-secondary); font-size: 12px; font-weight: 500; line-height: 1; white-space: nowrap; flex-shrink: 0; cursor: pointer; transition: background var(--t), color var(--t), border-color var(--t); }
.top-rename-btn:hover { background: var(--bg-hover); color: var(--text-primary); border-color: var(--border-strong); }
.top-rename-btn:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.top-rename-btn svg { flex-shrink: 0; }
.top-folder { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; color: var(--text-secondary); border: 1px solid var(--border); border-radius: 999px; padding: 1px 9px; white-space: nowrap; max-width: 180px; overflow: hidden; text-overflow: ellipsis; flex-shrink: 1; min-width: 0; }
.title-input { font-family: var(--font-serif); font-size: 17px; font-weight: 600; flex: 0 1 420px; }
.conn-badge.live { display: none; }
.conv-actions { display: flex; flex-direction: column; gap: 2px; }
.conv-actions:empty { display: none; }
.menu-item { border: none; background: transparent; text-align: left; padding: 6px 8px; border-radius: 8px; font-size: 13px; cursor: pointer; color: var(--text-primary); }
.menu-item:hover { background: var(--bg-hover); }
.menu-item.muted { color: var(--text-muted); cursor: default; }
.menu-sep { height: 1px; background: var(--border); margin: 4px 2px; }

/* conversation */
.chat-scroll { padding: 10px 24px 30px; }
.chat-container { max-width: 720px; gap: 0; }
.chat-user { margin: 18px 0 14px; gap: 5px; }
.chat-user-bubble { background: var(--bubble); border-radius: 18px 18px 6px 18px; padding: 9px 14px; font-size: 14.5px; }
.chat-agent { display: grid; grid-template-columns: 26px minmax(0, 1fr); column-gap: 12px; margin: 14px 0 20px; }
.chat-agent > .peer-icon { width: 26px; height: 26px; border-radius: 7px; margin-top: 2px; }
.chat-agent-head { gap: 8px; margin-bottom: 2px; }
.chat-agent-head .msg-author { font-size: 13.5px; font-weight: 600; }
.chat-agent-head .msg-sub { font-size: 11.5px; color: var(--text-muted); }
.chat-agent-text { font-size: 14.5px; line-height: 1.7; }
.chat-note { display: flex; align-items: center; gap: 10px; margin: 6px 0 6px 38px; font-size: 12px; }
.chat-note::before { content: ""; width: 14px; height: 1px; background: var(--border-strong); flex-shrink: 0; }
.chat-pending { margin: 10px 0 0; gap: 12px; font-size: 13px; color: var(--text-secondary); }
.chat-pending > .peer-icon { width: 26px; height: 26px; border-radius: 7px; }
.task-receipt { align-items:flex-start; padding:9px 11px; border:1px solid var(--border); border-radius:8px; background:var(--bg-subtle); }
.task-receipt > div { min-width:0; }
.task-receipt > .peer-icon { flex-shrink:0; }
.task-receipt strong { color:var(--text-primary); }
.receipt-detail { margin-top:3px; font-size:12px; color:var(--text-secondary); overflow-wrap:anywhere; }
.chat-pending.quota-exhausted { border-inline-start:3px solid var(--status-stale); }
.pend-dots { display: inline-flex; gap: 3px; }
.pend-dots i { width: 4px; height: 4px; border-radius: 999px; background: var(--text-muted); animation: pulseDot 1.2s ease-in-out infinite; }
.pend-dots i:nth-child(2) { animation-delay: 0.2s; }
.pend-dots i:nth-child(3) { animation-delay: 0.4s; }
.chat-hero { display: flex; flex-direction: column; align-items: center; padding: 0 0 26px; }
.chat-hero svg { width: 44px; height: 44px; margin-bottom: 18px; }
.chat-hero h2 { margin: 0; font-family: var(--font-serif); font-weight: 500; font-size: 32px; letter-spacing: 0.01em; }

/* composer */
.composer-wrap { border-top: none; background: transparent; padding: 0 24px 16px; }
.composer-inner { max-width: 720px; }
.composer-box { background: var(--bg-surface); border: 1px solid var(--border-strong); border-radius: 20px; box-shadow: var(--shadow-md); padding: 12px 12px 10px 16px; gap: 8px; }
.composer-input { font-size: 14.5px; min-height: 26px; max-height: 220px; }
.composer-foot { display: flex; align-items: center; gap: 6px; position: relative; }
.compose-opts { margin: 0; gap: 6px; position: static; }
.compose-opts:not(:empty)::after { content: ""; width: 1px; height: 16px; background: var(--border-strong); margin: 0 4px; }
.folder-btn, .tier-btn, .target-chip { height: 28px; padding: 0 10px; border-radius: 999px; border: 1px solid var(--border); background: transparent; font-size: 12.5px; color: var(--text-secondary); display: inline-flex; align-items: center; gap: 6px; cursor: pointer; }
.folder-btn:hover, .tier-btn:hover, .target-chip:hover { background: var(--bg-hover); }
.target-chip.active { background: var(--text-primary); color: var(--bg-surface); border-color: var(--text-primary); }
.target-chip .peer-icon { width: 15px; height: 15px; border-radius: 4px; }
.caret { color: var(--text-muted); font-size: 10px; }
.folder-menu, .tier-menu { position: absolute; left: 0; bottom: auto; top: calc(100% + 8px); z-index: 80; background: var(--bg-elevated); border: 1px solid var(--border-strong); border-radius: 14px; box-shadow: var(--shadow-md); padding: 6px; display: flex; flex-direction: column; }
.folder-menu { width: 320px; max-width: none; min-width: 0; }
.tier-menu { width: 230px; left: 120px; }
.tier-menu.solo { left: 0; }
.tier-k { color: var(--text-muted); margin-right: 5px; }
.folder-item { border-radius: 9px; }
.folder-item.current { background: var(--bg-hover); }
.send-btn { margin-left: auto; background: var(--text-primary); color: var(--bg-surface); flex-shrink: 0; }
.standby-banner { background: transparent; justify-content: center; gap: 6px; font-size: 11.5px; color: var(--text-muted); padding: 8px 0 0; margin: 0; }
.standby-banner .btn-link { font-size: 11.5px; text-decoration: underline; text-underline-offset: 2px; padding: 0 2px; }
.quota-warning { margin-bottom: 8px; padding: 7px 10px; border-inline-start: 3px solid var(--status-stale); border-radius: 7px; background: var(--status-stale-soft); color: var(--text-primary); font-size: 12px; white-space: pre-line; overflow-wrap: anywhere; }
.pt-quota-warning { margin-top: 3px; color: var(--text-primary); font-size: 11.5px; font-weight: 600; }
.composer-media { display:flex; gap:4px; flex-wrap:wrap; }
.composer-media button { border:1px solid var(--border); border-radius:7px; background:transparent; color:var(--text-secondary); padding:4px 8px; white-space:nowrap; }
.composer-media button:hover, .composer-media button[aria-expanded=true] { background:var(--bg-hover); color:var(--text-primary); }
.composer-box.dragging-image { outline:2px dashed var(--accent); }
.attachment-list, .sent-images { display:flex; gap:6px; flex-wrap:wrap; padding:4px 0; }
.attachment-list:empty { display:none; }
.attachment-chip { display:flex; gap:6px; align-items:center; max-width:100%; min-width:0; padding:3px 6px; border:1px solid var(--border); border-radius:7px; font-size:12px; }
.attachment-chip img { width:28px; height:28px; object-fit:cover; border-radius:4px; }
.attachment-chip span { overflow-wrap:anywhere; }
.attachment-chip .remove-image { flex-shrink:0; }
.sent-images { justify-content:flex-end; }
.sent-image { padding:0; background:transparent; border:1px solid var(--border); border-radius:7px; overflow:hidden; }
.sent-image img { width:112px; height:84px; object-fit:cover; display:block; }
#image-preview { max-width:94vw; max-height:92vh; border:1px solid var(--border); border-radius:12px; padding:12px; background:var(--bg-surface); color:var(--text-primary); }
#image-preview img { display:block; max-width:85vw; max-height:74vh; object-fit:contain; margin-top:8px; }
#image-preview::backdrop { background:rgba(0,0,0,.55); }
.slash-item[aria-selected=true] { background:var(--bg-hover); }
.account-notice { margin-bottom:8px; padding:7px 10px; border:1px solid var(--border); border-radius:7px; color:var(--text-primary); font-size:12px; overflow-wrap:anywhere; }
.account-notice button { margin-inline-start:6px; }


/* new conversation: greeting and composer centred */
.chat-column.is-new { justify-content: center; padding-bottom: 20vh; }
.chat-column.is-new .chat-scroll { flex: 0 0 auto; overflow: visible; padding-bottom: 0; }
.chat-column.is-new .composer-inner { max-width: 680px; }
.chat-column:not(.is-new) .folder-menu, .chat-column:not(.is-new) .tier-menu { top: auto; bottom: calc(100% + 8px); }
@media (max-width: 860px) {
  .topbar { padding: 8px 10px 6px; }
  .topbar-left { gap: 6px; }
  .top-title { max-width: none; font-size: 15.5px; }
  .top-rename-btn { height: 24px; padding: 0 7px; font-size: 11.5px; }
  .top-folder { display: none !important; }
  .title-input { flex: 1 1 auto; min-width: 0; max-width: none; font-size: 15.5px; }
}
</style>
</head>
<body>
<div class="app-shell">
  <!-- Left Retractable Sidebar: 会诊列表 -->
  <aside id="sidebar" class="sidebar">
    <div class="sidebar-head">
      <div class="brand-row">
        <span class="brand-mark"><svg viewBox="0 0 128 128" width="24" height="24" aria-hidden="true"><path d="M95.09 89.17A40 40 0 0 1 47.94 100.64" stroke="var(--peer-antigravity)" stroke-width="15.2" stroke-linecap="round" fill="none"/><path d="M32.20 88.26A40 40 0 0 1 32.20 39.74" stroke="var(--peer-chatgpt)" stroke-width="15.2" stroke-linecap="round" fill="none"/><path d="M47.94 27.36A40 40 0 0 1 95.09 38.83" stroke="var(--peer-claude)" stroke-width="15.2" stroke-linecap="round" fill="none"/><circle cx="67.2" cy="64" r="9.4" fill="var(--text-primary)"/></svg></span>
        <span class="brand-name">Consilium</span>
      </div>
    </div>
    <button id="new-chat-btn" type="button" class="new-row" title="新会诊"><span class="plus">+</span>新会诊<kbd>⌘N</kbd></button>
    <div class="sidebar-scroll">
      <div id="mission-list"></div>
    </div>
    <div class="dock-wrap">
      <div id="peer-popover" class="peer-popover hidden" role="dialog" aria-label="原生端账号与接手详情"></div>
      <div id="peer-dock" class="peer-dock" aria-label="三端状态、模型与当前账号余量"></div>
    </div>
    <span hidden><span id="mission-count-pill">0</span><span id="uptime-val">00:00:00</span><span id="version-pill">v__VERSION__</span></span>
  </aside>

  <!-- Main Workspace -->
  <div class="main-col">
    <!-- Compact Top Bar -->
    <header class="topbar">
      <div class="topbar-left">
        <button id="sidebar-toggle-btn" type="button" class="icon-btn" title="展开/收起会诊列表" aria-label="展开或收起侧栏">☰</button>
        <span id="top-title-wrap" style="display:contents;"><span id="active-mission-title" class="top-title plain">新会诊</span></span>
        <span id="top-folder" class="top-folder" style="display:none;"></span>
        <span id="active-mission-badge" class="chip" style="display:none;">空闲</span>
      </div>

      <!-- Right: Connection + Theme + Collapsible Right Drawer Tabs + Logout -->
      <div class="topbar-right">
        <div id="conn-pill" class="conn-badge hide-narrow">
          <span class="conn-dot"></span>
          <span id="conn-text">连接中</span>
        </div>
        <div id="panel-tools" class="panel-tools" role="group" aria-label="右侧面板"></div>
        <button id="more-btn" type="button" class="icon-btn" title="更多" aria-label="更多" aria-haspopup="true" aria-expanded="false">⋯</button>
        <div id="topbar-more" class="topbar-more">
          <div id="conv-actions" class="conv-actions"></div>
          <div class="seg" role="group" aria-label="主题切换">
            <button type="button" class="seg-btn" data-set-theme="light">浅色</button>
            <button type="button" class="seg-btn" data-set-theme="dark">深色</button>
            <button type="button" class="seg-btn" data-set-theme="system">系统</button>
          </div>
          <div id="top-drawer-tabs" class="conv-actions" role="group" aria-label="设置"></div>
          <button id="logout-btn" type="button" class="btn-quiet">退出</button>
        </div>
      </div>
    </header>

    <!-- Dedicated Compact 3-Peer Status & Quota Strip -->
    <div class="peer-subbar">
      <div id="top-peer-pills" class="peer-pills-bar" aria-label="三端原生状态与当前账号配额"></div>
    </div>

    <div class="workspace-body">
      <!-- Centered Minimalist Conversation Stream -->
      <section class="chat-column">
        <main id="chat-scroll" class="chat-scroll">
          <div id="chat-container" class="chat-container"></div>
        </main>

        <!-- Bottom Pure-Frontend Bar -->
        <footer class="composer-wrap">
          <div class="composer-inner">
            <div id="slash-palette" class="slash-palette hidden" role="listbox" aria-label="斜杠指令"></div>
            <div id="sync-warning" class="account-notice hidden" role="status"></div>
            <div id="quota-warning" class="quota-warning hidden" role="status"></div>
            <div id="account-notice" class="account-notice hidden" role="status"><span></span><button type="button" class="btn-link" id="dismiss-account-notice">知道了</button></div>
            <div class="composer-box" id="composer-box">
              <textarea id="composer-input" class="composer-input" rows="1" placeholder="说说要做的事…" aria-label="消息"></textarea>
              <div id="composer-attachments" class="attachment-list" aria-label="图片草稿"></div>
              <div id="attachment-error" class="hidden" role="status"></div>
              <div class="composer-foot">
                <div class="composer-media">
                  <input id="composer-image" type="file" accept="image/png,image/jpeg,image/webp,image/gif" multiple hidden>
                  <button type="button" id="btn-attach-image">＋ 图片</button>
                  <button type="button" id="btn-slash-toggle" aria-controls="slash-palette" aria-expanded="false">/ 指令</button>
                </div>
                <div id="compose-opts" class="compose-opts"></div>
                <div id="handoff-targets" style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;"></div>
                <button id="btn-send" type="button" class="send-btn" title="发送（Enter）" aria-label="发送" disabled>↑</button>
              </div>
            </div>
            <div id="standby-banner" class="standby-banner hidden"></div>
          </div>
        </footer>
      </section>

      <dialog id="image-preview" aria-label="图片预览">
        <form method="dialog"><button type="submit" class="btn-link">关闭预览</button></form>
        <img alt="">
      </dialog>

      <!-- Collapsible Right Tabbed Inspector (Closed by Default) -->
      <aside id="right-drawer" class="right-drawer closed" aria-label="这个会诊的任务、文件和分支">
        <div id="drawer-grip" class="drawer-grip" title="拖动调整宽度"></div>
        <div class="drawer-head">
          <div id="drawer-tabs-inner" class="panel-tabs" role="tablist"></div>
          <button id="btn-close-drawer" type="button" class="panel-x" title="关闭（⌘\）" aria-label="关闭右侧面板"></button>
        </div>
        <div id="drawer-body" class="drawer-body"></div>
      </aside>
    </div>
  </div>
</div>

<!-- Settings: things that are not about one conversation -->
<div id="settings-overlay" class="overlay hidden" role="dialog" aria-modal="true" aria-label="设置">
  <div class="settings-sheet">
    <nav id="settings-nav" class="settings-nav"></nav>
    <div class="settings-main">
      <div class="settings-head">
        <span id="settings-title" class="settings-title"></span>
        <button id="btn-close-settings" type="button" class="panel-x" title="关闭" aria-label="关闭设置"></button>
      </div>
      <div id="settings-body" class="settings-body"></div>
    </div>
  </div>
</div>

<!-- Login Overlay -->
<div id="login-overlay" class="overlay hidden">
  <div class="modal" role="dialog" aria-labelledby="login-title">
    <h2 id="login-title">连接本地 Consilium</h2>
    <p>请输入本地 <code class="inline">auth.token</code>；或在终端运行 <code class="inline">python3 -m consilium.cli ui</code> 免密打开。<br>提示：请使用 <code class="inline">127.0.0.1:&lt;port&gt;/ui</code> 访问，勿混用 <code class="inline">localhost</code>（同主机异源会拦截 Cookie 写请求）。</p>
    <input id="token-input" class="input mono" type="password" autocomplete="current-password" placeholder="粘贴 64 位 hex auth.token">
    <div id="login-err" class="err-text"></div>
    <div class="modal-actions">
      <button id="login-submit" type="button" class="btn btn-primary">登录看板</button>
    </div>
  </div>
</div>

<!-- Confirm Overlay -->
<div id="confirm-overlay" class="overlay hidden">
  <div class="modal" role="dialog" aria-labelledby="confirm-title">
    <h2 id="confirm-title">确认操作</h2>
    <p id="confirm-desc"></p>
    <div id="confirm-extra"></div>
    <div id="confirm-err" class="err-text"></div>
    <div class="modal-actions">
      <button id="confirm-cancel" type="button" class="btn">取消</button>
      <button class="btn btn-danger" id="confirm-ok" type="button">确认</button>
    </div>
  </div>
</div>

<div id="toast" class="toast hidden"></div>

<script>
(() => {
  "use strict";

  const PEER_META = {
    claude: { id: "claude", label: "Claude", appName: "Claude.app · Code", branchPrefix: "claude/", short: "C", cls: "claude" },
    chatgpt: { id: "chatgpt", label: "Codex", appName: "ChatGPT.app · Codex", branchPrefix: "codex/", short: "G", cls: "chatgpt" },
    antigravity: { id: "antigravity", label: "Antigravity", appName: "Antigravity.app", branchPrefix: "gemini/", short: "A", cls: "antigravity" }
  };
  const STATUS_TEXT = { standby: "待命", working: "工作中", online: "在线", offline: "离线", stale: "陈旧" };
  const TASK_STATUS_TEXT = { queued: "排队", claimed: "已认领", running: "执行中", blocked: "阻塞", failed: "失败", done: "已完成", cancelled: "已取消" };
  const EVENT_LABELS = {
    task_assigned: "派发任务",
    task_done: "完成交付",
    progress: "进度上报",
    review_request: "交叉复核",
    loop_guard_reached: "触达轮次上限",
    mission_closed: "会诊结束",
    task_requeued: "重新入队",
    task_stale: "进度超时陈旧",
    task_handoff: "任务交接"
  };
  // The right panel holds what belongs to one conversation; everything else lives in settings.
  const PANEL_TABS = [
    { key: "tasks", label: "任务" },
    { key: "files", label: "文件" },
    { key: "branches", label: "分支" }
  ];
  const SETTINGS_TABS = [
    { key: "accounts", label: "模型与账号" },
    { key: "prefs", label: "偏好" }
  ];
  const PANEL_ALIASES = { artifacts: "files", worktrees: "branches" };
  const ICONS = {
    tasks: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M2.5 4.2l1.2 1.2 2-2.2M2.5 9.2l1.2 1.2 2-2.2M8.2 4.5h5.3M8.2 9.5h5.3M8.2 13h3"/></svg>',
    files: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 2H4.5a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h7a1 1 0 0 0 1-1V5.5L9 2z"/><path d="M9 2v3.5h3.5M6 9h4M6 11.5h2.5"/></svg>',
    branches: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="4.5" cy="3.5" r="1.5"/><circle cx="4.5" cy="12.5" r="1.5"/><circle cx="11.5" cy="5.5" r="1.5"/><path d="M4.5 5v6M11.5 7c0 2.5-3 2.2-5.2 3.2"/></svg>',
    panel: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round" aria-hidden="true"><rect x="2" y="3" width="12" height="10" rx="1.6"/><path d="M10 3v10"/></svg>',
    close: '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8"/></svg>',
    done: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="8" cy="8" r="6" fill="currentColor" stroke="none" opacity=".14"/><path d="M5.2 8.2l1.9 1.9 3.7-4"/></svg>',
    bad: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" aria-hidden="true"><circle cx="8" cy="8" r="6" fill="currentColor" stroke="none" opacity=".14"/><path d="M8 4.8v3.8M8 11v.2"/></svg>',
    wait: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-dasharray="2 2.2" aria-hidden="true"><circle cx="8" cy="8" r="5.6"/></svg>',
    run: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" aria-hidden="true"><circle cx="8" cy="8" r="5.6" opacity=".22"/><path d="M8 2.4a5.6 5.6 0 0 1 5.6 5.6"/></svg>',
    chev: '<svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 4l4 4-4 4"/></svg>',
    edit: '<svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.35" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M11.2 2.4a1.4 1.4 0 0 1 2 2L5.6 12l-2.8.8.8-2.8 7.6-7.6z"/><path d="M9.6 4l2 2"/></svg>'
  };
  const SLASH_COMMANDS = [
    { cmd: "/mission", desc: "打开或定位左侧会诊列表面板", action: "focus_mission" },
    { cmd: "/files", desc: "打开右侧“文件”：各端改动或产出的文件", action: "tab:files" },
    { cmd: "/export", desc: "合成当前会诊 Markdown 并复制到剪贴板", action: "export_md" },
    { cmd: "/tasks", desc: "打开右侧“任务”：这个会诊拆出的每项任务", action: "tab:tasks" },
    { cmd: "/branches", desc: "打开右侧“分支”：各端用的分支", action: "tab:branches" },
    { cmd: "/accounts", desc: "设置：各端的账号和余量", action: "tab:accounts" },
    { cmd: "/prefs", desc: "设置：查看和删除三端学到的使用偏好", action: "tab:prefs" },
    { cmd: "/new", desc: "开始一个新会诊", action: "new_chat" }
  ];

  let appState = null;
  let csrfToken = "";
  let lastStateVersion = 0;
  let lastStateInstance = "";
  const retiredStateInstances = new Set();
  let lastSnapshotAt = "";
  let currentConnectionLabel = "";
  let fetchSequence = 0;
  let nativeReauthPending = false;
  let nativeReauthAttempted = false;
  let selectedMissionId = null;
  let rightTab = null; // Closed by default per minimalist chat spec
  let settingsTab = null;
  let taskRowOpen = {}; // task_id -> true/false once the user has toggled it
  function storedPref(key, fallback) {
    try { const v = window.localStorage.getItem(key); return v === null ? fallback : v; } catch (e) { return fallback; }
  }
  function storePref(key, value) {
    try { window.localStorage.setItem(key, String(value)); } catch (e) { /* private window: just not remembered */ }
  }
  let panelWidth = Math.max(280, Math.min(640, Number(storedPref("consilium.panelWidth", 372)) || 372));
  let openPopoverPeer = null;
  let handoffTarget = "claude";
  let composingNew = false;
  let composeWorkspace = "";
  let folderMenuOpen = false;
  let tierMenuOpen = false;
  let composeTier = "auto";
  let tierSyncedFor = null; // which conversation composeTier was last read from
  const TIERS = [
    ["auto", "自动", "按难度自己定，不用你选"],
    ["fast", "快", "轻量问题，回得快"],
    ["balanced", "均衡", "日常使用"],
    ["deep", "深度", "难题和架构，想得久"]
  ];
  const TIER_LABELS = { fast: "快", balanced: "均衡", pro_reasoning: "深度", deep: "深度" };
  function tierFromMission(m) {
    const t = (m && m.tier) || "";
    return t === "pro_reasoning" ? "deep" : (t === "fast" || t === "balanced" ? t : "auto");
  }
  // "深度" plus why: shown in front of the model on each reply and task.
  function tierNote(task) {
    if (!task || !task.tier_used) return { label: "", tip: "" };
    const label = TIER_LABELS[task.tier_used] || "";
    const how = task.model_tier === "auto" ? "自动定的档位" : "你指定的档位";
    return { label: label, tip: how + "：" + label + (task.tier_reason ? "。" + task.tier_reason : "") };
  }
  let renamingMission = null;
  let showArchived = false;
  const LOCAL_FOLDERS_KEY = "consilium.recentFolders";

  function loadLocalFolders() {
    try { const v = JSON.parse(localStorage.getItem(LOCAL_FOLDERS_KEY) || "[]"); return Array.isArray(v) ? v.filter(x => typeof x === "string") : []; }
    catch (e) { return []; }
  }
  function rememberFolder(path) {
    if (!path) return;
    try {
      const list = [path].concat(loadLocalFolders().filter(x => x !== path)).slice(0, 12);
      localStorage.setItem(LOCAL_FOLDERS_KEY, JSON.stringify(list));
    } catch (e) { /* storage unavailable: the in-memory choice still works */ }
  }
  function recentFolders(state) {
    const out = [];
    loadLocalFolders().forEach(f => { if (out.indexOf(f) < 0) out.push(f); });
    ((state && state.missions) || []).forEach(mm => { if (mm.workspace_path && out.indexOf(mm.workspace_path) < 0) out.push(mm.workspace_path); });
    return out.slice(0, 12);
  }
  function folderName(path) { return path ? (path.split("/").filter(Boolean).pop() || path) : ""; }
  function canPickNativeFolder() {
    return !!(window.webkit && window.webkit.messageHandlers && window.webkit.messageHandlers.consilium);
  }
  window.__consiliumFolderPicked = function (path) {
    if (path) { composeWorkspace = path; rememberFolder(path); }
    folderMenuOpen = false;
    renderComposerBar(appState);
    document.getElementById("composer-input").focus();
  };
  const AG_STANDBY_PROMPT = "请作为 Consilium 的 Antigravity 待命端（peer_id=antigravity）：反复调用 consilium_wait(peer_id=\"antigravity\", timeout_sec=300, session_label=\"antigravity-standby\", scope=\"consilium\")；返回 woke=true 时调用 consilium_pickup_inbox(peer_id=\"antigravity\", session_label=\"antigravity-standby\", scope=\"consilium\") 取任务，按 consilium-collab 技能处理并用 consilium_complete_task 回复（带上 model_used、effort_used、tier_used 和 tier_reason；任务的 model_tier 是 auto 时由你按难度自己定档位并真的按它做，不要让使用者选；不要自己查询或汇报余量，余量由 Consilium 从 Cockpit 同步）；超时没消息就继续等。每轮少说话，不要停下来问我。";
  const CODEX_STANDBY_PROMPT = "请作为 Consilium 的 Codex 待命端（peer_id=chatgpt）：反复调用 consilium_wait(peer_id=\"chatgpt\", timeout_sec=300, session_label=\"chatgpt-standby\", scope=\"consilium\")；返回 woke=true 时调用 consilium_pickup_inbox(peer_id=\"chatgpt\", session_label=\"chatgpt-standby\", scope=\"consilium\") 取任务，按 consilium-collab 技能处理并用 consilium_complete_task 回复（带上 model_used、effort_used、tier_used 和 tier_reason；任务的 model_tier 是 auto 时由你按难度自己定档位并真的按它做，不要让使用者选；不要自己查询或汇报余量，余量由 Consilium 从 Cockpit 同步）；超时没消息就继续等。每轮少说话，不要停下来问我。";
  const STANDBY_PROMPT = "请作为 Consilium 的 Claude 常驻待命端：先读 ~/.claude/skills/consilium-collab/SKILL.md 第 2、3 节；用 run_in_background 运行 `PYTHONPATH=/Applications/Consilium.app/Contents/Resources/python /usr/bin/python3 -B -m consilium.cli wait --peer claude --session-label claude-standby --scope consilium --timeout 86400`，被唤醒后用 consilium_pickup_inbox(peer_id=\"claude\", session_label=\"claude-standby\", scope=\"consilium\") 取任务（只取 Consilium 窗口里发起的，取到空就直接重新挂上）、处理、用 consilium_complete_task 回复（带上 model_used、effort_used、tier_used 和 tier_reason；任务的 model_tier 是 auto 时由你按难度自己定档位并真的按它做，不要让使用者选；不要自己查询或汇报余量，余量由 Consilium 从 Cockpit 同步），然后重新挂上等待。不要轮询，每轮少说话。";
  let sendTargets = [];
  let currentDraftKey = null;
  const messageDrafts = new Map();
  let slashIndex = 0;
  let slashDismissed = null;
  const observedAccounts = (() => {
    try { return JSON.parse(storedPref("consilium.observedAccounts", "{}")) || {}; } catch (_) { return {}; }
  })();
  const accountNotices = new Map();
  const dismissedAccountNotices = new Map();
  let sending = false;
  let slashOpen = false;
  let sidebarDesktopOpen = true;
  let sidebarMobileOpen = false;
  function isNarrowViewport() { return window.innerWidth <= 860; }
  function isSidebarOpen() { return isNarrowViewport() ? sidebarMobileOpen : sidebarDesktopOpen; }
  let eventSource = null;
  let pollTimer = null;
  let sseReconnectTimer = null;
  let baseUptime = 0;
  let baseUptimeStamp = Date.now();
  let pendingConfirmFn = null;
  const expandedFolds = new Set();
  const expandedReviews = new Set();

  function esc(str) {
    return String(str == null ? "" : str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function renderInline(str) {
    return esc(str)
      .replace(/`([^`\n]+)`/g, '<code class="inline">$1</code>')
      .replace(/\*\*([^*\n]+)\*\*/g, "<strong>$1</strong>");
  }

  function renderMarkdownBlock(mdText) {
    if (!mdText) return "";
    return String(mdText).split("\n").map(line => {
      if (line.startsWith("### ")) return "<div style='font-weight:600;margin:6px 0 2px;'>" + renderInline(line.slice(4)) + "</div>";
      if (line.startsWith("## ")) return "<div style='font-weight:650;font-size:14.5px;margin:6px 0 3px;'>" + renderInline(line.slice(3)) + "</div>";
      if (line.startsWith("- ")) return "<div style='padding-left:10px;'>• " + renderInline(line.slice(2)) + "</div>";
      if (!line.trim()) return "<div style='height:4px;'></div>";
      return "<div>" + renderInline(line) + "</div>";
    }).join("");
  }

  function formatDuration(sec) {
    const s = Math.max(0, Math.floor(sec || 0));
    const h = String(Math.floor(s / 3600)).padStart(2, "0");
    const m = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
    const r = String(s % 60).padStart(2, "0");
    return h + ":" + m + ":" + r;
  }

  function relativeAge(isoStr) {
    if (!isoStr) return "暂无上报";
    const ms = Date.parse(isoStr);
    if (Number.isNaN(ms)) return "未知";
    const diff = Math.max(0, Math.floor((Date.now() - ms) / 1000));
    if (diff < 60) return diff + " 秒前";
    if (diff < 3600) return Math.floor(diff / 60) + " 分钟前";
    return Math.floor(diff / 3600) + " 小时前";
  }

  function formatClock(isoStr) {
    if (!isoStr) return "";
    const d = new Date(isoStr);
    if (Number.isNaN(d.getTime())) return isoStr;
    return [d.getHours(), d.getMinutes(), d.getSeconds()].map(n => String(n).padStart(2, "0")).join(":");
  }

  let toastTimer = null;
  function showToast(msg) {
    const el = document.getElementById("toast");
    el.textContent = String(msg || "");
    el.classList.remove("hidden");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => el.classList.add("hidden"), 3200);
  }

  async function copyTextSafe(text, okMsg) {
    const content = String(text || "");
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
        await navigator.clipboard.writeText(content);
        showToast(okMsg);
        return true;
      }
    } catch (_) {}
    try {
      const ta = document.createElement("textarea");
      ta.value = content;
      ta.setAttribute("readonly", "");
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      const copied = typeof document.execCommand === "function" && document.execCommand("copy");
      document.body.removeChild(ta);
      if (copied) {
        showToast(okMsg);
        return true;
      }
    } catch (_) {}
    showToast("剪贴板写入失败，请手动复制");
    return false;
  }

  function setConn(kind, text) {
    currentConnectionLabel = text;
    const pill = document.getElementById("conn-pill");
    pill.className = "conn-badge hide-narrow" + (kind ? " " + kind : "") + (text === "SSE 实时" ? " live" : "");
    document.getElementById("conn-text").textContent = text;
    renderSyncNotice();
  }

  function renderSyncNotice() {
    const box = document.getElementById("sync-warning");
    let text = "";
    if (lastSnapshotAt && currentConnectionLabel !== "SSE 实时") {
      const label = currentConnectionLabel.startsWith("轮询") ? "轮询更新" :
        (["待登录", "已退出"].includes(currentConnectionLabel) ? currentConnectionLabel : "正在重连，显示上次状态");
      text = label + " · 上次同步 " + formatClock(lastSnapshotAt);
    }
    box.classList.toggle("hidden", !text);
    if (box.textContent !== text) box.textContent = text;
  }

  function applyTheme(theme) {
    document.documentElement.classList.add("no-theme-transition");
    document.documentElement.setAttribute("data-theme", theme);
    try { localStorage.setItem("consilium-theme", theme); } catch (_) {}
    document.querySelectorAll("[data-set-theme]").forEach(btn => {
      btn.classList.toggle("active", btn.getAttribute("data-set-theme") === theme);
    });
    void document.documentElement.offsetHeight;
    requestAnimationFrame(() => document.documentElement.classList.remove("no-theme-transition"));
  }

  function buildHandoffDeeplink(peerId, promptText, workspace) {
    const clean = (promptText || "请检查 Consilium 收件箱并调用 consilium_pickup_inbox 认领任务").replace(/^\/+/, "");
    if (peerId === "claude") {
      return "claude://code/new?q=" + encodeURIComponent(clean) + (workspace ? "&folder=" + encodeURIComponent(workspace) : "");
    }
    if (peerId === "chatgpt") {
      return "codex://new?prompt=" + encodeURIComponent(clean) + (workspace ? "&path=" + encodeURIComponent(workspace) : "");
    }
    return "";
  }

  function getActiveMission(state) {
    if (composingNew) return null;
    const missions = (state && state.missions) || [];
    if (!missions.length) return null;
    if (selectedMissionId) {
      const found = missions.find(m => m.mission_id === selectedMissionId);
      if (found) return found;
    }
    const active = missions.find(m => m.status === "active" || m.status === "ready_to_report");
    return active || missions[0];
  }

  function renderSidebar(state) {
    const sidebar = document.getElementById("sidebar");
    sidebar.classList.toggle("collapsed", !isSidebarOpen());
    renderPeerDock(state);
    const missions = (state && state.missions) || [];
    document.getElementById("mission-count-pill").textContent = String(missions.length);
    const cur = getActiveMission(state);
    const listEl = document.getElementById("mission-list");
    if (!missions.length) {
      listEl.innerHTML = "";
      return;
    }
    const itemHtml = m => {
      const tasks = m.tasks || [];
      const closed = m.status === "completed" || m.status === "cancelled" || m.status === "failed";
      const busy = !closed && tasks.some(t => t.status === "queued" || t.status === "claimed" || t.status === "running");
      const activeCls = cur && cur.mission_id === m.mission_id ? " active" : "";
      const who = ["claude", "chatgpt", "antigravity"].filter(pid => tasks.some(t => t.assignee_peer === pid));
      return '<div role="button" tabindex="0" class="mission-item' + activeCls + '" data-select-mission="' + esc(m.mission_id) + '" title="' + esc(m.title + " · " + missionStatusText(m) + " · " + relativeAge(m.updated_at)) + '">' +
        '<span class="mi-title">' + esc(m.title) + '</span>' +
        (busy ? '<span class="mi-live"></span>' : "") +
        '<span class="mi-who">' + who.map(pid => '<span class="peer-icon ' + pid + '" aria-hidden="true"></span>').join("") + '</span>' +
        '<button type="button" class="mi-archive" data-archive-mission="' + esc(m.mission_id) + '"' +
          (m.archived ? ' data-archive-undo="1" title="取消归档" aria-label="取消归档">' : ' title="归档（只在 Consilium 里收起，不动各 App 里的会话）" aria-label="归档">') +
          '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="2" y="3" width="12" height="3.2" rx="0.8"/><path d="M3.2 6.2v5.6a1 1 0 0 0 1 1h7.6a1 1 0 0 0 1-1V6.2M6.5 9h3"/></svg>' +
        '</button>' +
      '</div>';
    };
    const live = missions.filter(m => !m.archived);
    const archived = missions.filter(m => m.archived);
    const groups = [];
    live.forEach(m => {
      const key = m.workspace_path || "";
      let g = groups.find(x => x.key === key);
      if (!g) { g = { key: key, items: [] }; groups.push(g); }
      g.items.push(m);
    });
    listEl.innerHTML = groups.map(g =>
      '<div class="folder-group">' +
        '<div class="folder-group-head" title="' + esc(g.key || "没有选文件夹的旧会诊") + '">' + esc(g.key ? folderName(g.key) : "其他") + '</div>' +
        g.items.map(itemHtml).join("") +
      '</div>'
    ).join("") +
    (archived.length
      ? '<button type="button" class="archived-toggle" data-toggle-archived="1">已归档 ' + archived.length + '</button>' +
        (showArchived ? archived.map(itemHtml).join("") : "")
      : "");
  }

  function renderTopBar(state) {
    const cur = getActiveMission(state);
    const titleWrap = document.getElementById("top-title-wrap");
    const titleEl = document.getElementById("active-mission-title");
    const editingTitle = cur && renamingMission === cur.mission_id;
    const showingSameTitle = cur && titleEl &&
      titleEl.dataset.renameMission === cur.mission_id && titleEl.textContent === cur.title;
    if (editingTitle ? !document.getElementById("title-input") : !showingSameTitle) {
      titleWrap.innerHTML = !cur
        ? '<span id="active-mission-title" class="top-title plain"></span>'
        : (editingTitle
            ? '<input id="title-input" class="title-input" maxlength="40" aria-label="修改会诊标题（回车保存，Esc 取消）" value="' + esc(cur.title) + '">'
            : '<span id="active-mission-title" class="top-title" data-rename-mission="' + esc(cur.mission_id) + '" title="' + esc(cur.title) + '（点击改名）">' + esc(cur.title) + '</span>' +
              '<button id="rename-title-btn" type="button" class="top-rename-btn" data-rename-mission="' + esc(cur.mission_id) + '" title="修改当前会诊标题">' + ICONS.edit + '<span>改名</span></button>');
    }
    const folderEl = document.getElementById("top-folder");
    if (cur && cur.workspace_path) {
      folderEl.style.display = "";
      folderEl.title = cur.workspace_path;
      folderEl.textContent = "📁 " + folderName(cur.workspace_path);
    } else {
      folderEl.style.display = "none";
    }
    document.getElementById("active-mission-badge").textContent = missionStatusText(cur);
    const actEl = document.getElementById("conv-actions");
    if (cur) {
      const closedCur = cur.status === "completed" || cur.status === "cancelled" || cur.status === "failed";
      actEl.innerHTML =
        '<button type="button" class="menu-item" data-rename-mission="' + esc(cur.mission_id) + '">改名</button>' +
        (closedCur ? "" : ((cur.native_sessions || {}).claude
          ? '<span class="menu-item muted">Claude 专属会话已绑定</span>'
          : '<button type="button" class="menu-item" data-dedicated-session="' + esc(cur.mission_id) + '" title="在这个文件夹里新开一个同名 Claude Code 会话，之后这个会诊的消息都交给它">在 Claude 里开专属会话</button>')) +
        '<div class="menu-sep"></div>';
    } else {
      actEl.innerHTML = "";
    }

    const peers = (state && state.peers) || [];
    const quotas = {};
    ((state && state.quotas) || []).forEach(q => { quotas[q.peer_id] = q; });

    document.getElementById("top-peer-pills").innerHTML = ["claude", "chatgpt", "antigravity"].map(pid => {
      const meta = PEER_META[pid];
      const peer = peers.find(x => x.peer_id === pid) || { peer_id: pid, status: "offline" };
      const q = quotas[pid] || { min_remaining_percent: null, windows: [], pool: [] };
      const stKey = STATUS_TEXT[peer.status] ? peer.status : "offline";
      const hasQuota = q.min_remaining_percent !== null && q.min_remaining_percent !== undefined;
      const pct = hasQuota ? Math.max(0, Math.min(100, Number(q.min_remaining_percent))) : null;
      const isDemo = (q.windows || []).some(w => w.source === "demo");
      const poolLen = (q.pool || []).length;
      const qCls = !hasQuota ? "unknown" : (pct <= 0 ? "empty" : (pct < 20 ? "low" : ""));
      const barCls = !hasQuota ? "unknown" : (pct <= 0 ? "empty" : (pct < 20 ? "low" : ""));
      const quotaLabel = q.account_switch_suspected ? "可能已切换" : (q.current_account_unknown ? "账号未知" : (hasQuota ? Math.round(pct) + "%" : "账号未知"));
      const activeCls = openPopoverPeer === pid ? " open" : "";
      const pillTitle = meta.appName + " · 分支前缀：" + meta.branchPrefix + (q.account_switch_suspected ? " · 账号可能已切换" : "");

      return '<button type="button" class="peer-pill' + activeCls + '" data-peer-pill="' + pid + '" title="' + esc(pillTitle) + '">' +
        '<span class="dot ' + stKey + '" style="box-shadow:0 0 0 2px var(--bg-surface);"></span>' +
        '<span class="peer-icon ' + meta.cls + '" aria-hidden="true"></span>' +
        '<span class="pill-name">' + esc(meta.label) + '</span>' +
        '<span class="pill-state hide-narrow">' + esc(STATUS_TEXT[stKey]) + '</span>' +
        '<span class="pill-quota tabular-nums ' + qCls + '">' + esc(quotaLabel) + '</span>' +
        (isDemo ? '<span class="pill-badge hide-narrow">演示数据</span>' : "") +
        (poolLen > 1 ? '<span class="pill-badge tabular-nums hide-narrow">' + poolLen + '号</span>' : "") +
        '<span class="pill-track"><span class="pill-fill ' + meta.cls + ' ' + barCls + '" style="width:' + (hasQuota ? pct : 0) + '%;"></span></span>' +
      '</button>';
    }).join("");

    document.getElementById("top-drawer-tabs").innerHTML = SETTINGS_TABS.map(tb => (
      '<button type="button" class="menu-item" data-open-settings="' + tb.key + '">' + esc(tb.label) + '</button>'
    )).join("") + '<div class="menu-sep"></div>';
    renderPanelTools(state);

    renderPeerPopover(state);
  }

  function renderPeerPopover(state) {
    const pop = document.getElementById("peer-popover");
    if (!openPopoverPeer || !state) {
      pop.classList.add("hidden");
      return;
    }
    const pid = openPopoverPeer;
    const meta = PEER_META[pid];
    const peer = (state.peers || []).find(x => x.peer_id === pid) || { peer_id: pid, status: "offline", task_counts: {} };
    const q = (state.quotas || []).find(x => x.peer_id === pid) || { min_remaining_percent: null, windows: [], pool: [], historical_accounts: [] };
    const hasQuota = q.min_remaining_percent !== null && q.min_remaining_percent !== undefined;
    const pct = hasQuota ? Math.round(Number(q.min_remaining_percent)) : null;
    const isDemo = (q.windows || []).some(w => w.source === "demo");
    const stKey = STATUS_TEXT[peer.status] ? peer.status : "offline";
    const curM = getActiveMission(state);
    const ws = curM ? curM.workspace_path : "";
    const deepUrl = buildHandoffDeeplink(pid, "检查 Consilium 收件箱并调用 consilium_pickup_inbox 认领任务", ws);

    const poolRows = (q.pool || []).concat(q.historical_accounts || []);
    const poolHtml = poolRows.map(acct => {
      const aPct = acct.remaining_percent == null ? "未知" : Math.round(acct.remaining_percent) + "%";
      const badgeHtml = acct.is_current ? ' <span class="pill-badge">当前</span>' : (acct.is_historical ? ' <span class="pill-badge">历史账号</span>' : "");
      return '<div class="pop-row" style="padding:3px 6px;border-radius:6px;' + (acct.is_current ? "background:var(--bg-subtle);font-weight:600;" : "") + '">' +
        '<span style="display:inline-flex;align-items:center;gap:4px;">' + esc(acct.alias || "账号") + badgeHtml + '</span>' +
        '<span class="tabular-nums" style="color:var(--text-muted);">' + esc(aPct) + (acct.reset_in_human ? " · " + esc(acct.reset_in_human) : "") + '</span>' +
      '</div>';
    }).join("");

    const actionBtn = deepUrl
      ? '<a class="btn btn-primary btn-sm" style="text-decoration:none;flex:1;" href="' + esc(deepUrl) + '">在 ' + esc(meta.label) + ' 新开会话接手 ↗</a>'
      : '<button type="button" class="btn btn-primary btn-sm" style="flex:1;" data-copy-pickup="' + pid + '">复制接手提示词</button>';

    const activeAcctText = q.account_switch_suspected
      ? "账号可能已切换"
      : (q.active_account_alias || (q.current_account_unknown || !hasQuota ? "账号未知" : "默认账号"));

    pop.innerHTML =
      '<div class="pop-head">' +
        '<div>' +
          '<div style="font-weight:650;" title="' + esc("分支前缀：" + meta.branchPrefix) + '">' + esc(meta.appName) + '</div>' +
          '<div style="font-size:12px;color:var(--text-muted);">' + esc(STATUS_TEXT[stKey]) + ' · 最后上报于 ' + esc(relativeAge(peer.last_seen_at)) + '</div>' +
        '</div>' +
        '<button type="button" class="icon-btn" data-close-popover="1">✕</button>' +
      '</div>' +
      '<div class="pop-body">' +
        '<div class="pop-row">' +
          '<span style="color:var(--text-muted);">当前激活账号</span>' +
          '<span style="font-weight:550;">' + esc(activeAcctText) + '</span>' +
        '</div>' +
        '<div class="pop-row">' +
          '<span style="color:var(--text-muted);display:inline-flex;align-items:center;gap:6px;">当前号余量' + (isDemo ? '<span class="pill-badge">演示数据</span>' : "") + '</span>' +
          '<span class="tabular-nums" style="font-weight:600;">' + (hasQuota ? pct + "%" + (q.earliest_reset_in_human ? " · 重置 " + esc(q.earliest_reset_in_human) : "") : "未知") + '</span>' +
        '</div>' +
        (q.account_switch_suspected ? '<div style="padding:5px 8px;border-radius:7px;background:var(--status-stale-soft);color:var(--status-stale);font-size:12px;">账号可能已切换（最近配额样本已超过 30 分钟），配额显示为未知</div>' : "") +
        (q.switch_hint ? '<div style="padding:5px 8px;border-radius:7px;background:var(--status-standby-soft);color:var(--status-standby);font-size:12px;">' + esc(q.switch_hint) + '</div>' : "") +
        (poolHtml ? '<div style="padding-top:4px;border-top:1px solid var(--border);"><div style="font-size:12px;color:var(--text-muted);margin-bottom:4px;">只读账号池</div>' + poolHtml + '</div>' : "") +
      '</div>' +
      '<div class="pop-actions">' + actionBtn + '<button type="button" class="btn btn-sm" data-focus-handoff="' + pid + '">指定交接</button></div>';
    pop.classList.remove("hidden");
  }

  function renderTraceBadges(ev) {
    const p = ev.payload || {};
    const badges = [];
    if (p.claim_latency_ms !== undefined && p.claim_latency_ms !== null) {
      badges.push('<span class="trace-pill lat tabular-nums">⏱ 认领 ' + esc(p.claim_latency_ms) + ' ms</span>');
    }
    if (p.delivery_mode === "hot_wakeup") {
      const lat = p.wakeup_latency_ms !== undefined && p.wakeup_latency_ms !== null ? " · " + p.wakeup_latency_ms + " ms" : "";
      badges.push('<span class="trace-pill hot tabular-nums">⚡ 热唤醒' + esc(lat) + '</span>');
    } else if (p.delivery_mode === "cold_notification") {
      let cmdTxt = "冷通知";
      if (p.delivery_dry_run || p.command_status === "dry_run") cmdTxt = "冷通知 · dry-run";
      else if (p.command_ok === true || p.command_status === "ok") cmdTxt = "冷通知 · 命令已发";
      else if (p.command_status === "failed") cmdTxt = "冷通知 · 命令失败";
      badges.push('<span class="trace-pill cold">🔔 ' + esc(cmdTxt) + '</span>');
      badges.push('<span class="trace-pill">' + ((ev.acked_at || p.peer_acked) ? "✓ 对方已确认" : "待对方确认") + '</span>');
    }
    return badges.length ? '<div class="trace-row">' + badges.join("") + '</div>' : "";
  }

  function renderSingleEventMessage(ev, mission) {
    const p = ev.payload || {};
    const fromId = ev.from_peer || "";
    const meta = PEER_META[fromId] || { label: fromId || "系统", appName: "Consilium", short: "S", cls: "system", branchPrefix: "" };
    const isAssign = ev.kind === "task_assigned";
    const evLabel = EVENT_LABELS[ev.kind] || ev.kind;
    const linkedTask = ((mission && mission.tasks) || []).find(t => t.task_id === p.task_id) || null;
    let noteClip = p.progress_note || p.result_summary || p.question || p.summary || p.reason || p.prompt || "";
    if (!noteClip && isAssign && linkedTask) {
      noteClip = linkedTask.prompt || linkedTask.title || "";
    }
    const branch = p.worktree_branch || (linkedTask && linkedTask.worktree_branch) || "";
    const artifacts = Array.isArray(p.artifacts) ? p.artifacts : [];

    const relTime = ev.created_at ? "最后上报于 " + relativeAge(ev.created_at) : "最后上报时间未知";
    const clockHint = ev.created_at ? formatClock(ev.created_at) : "";
    let titleText = meta.label;
    let subText = evLabel + " · " + relTime;
    if (isAssign) {
      const targetMeta = PEER_META[ev.to_peer] || { label: ev.to_peer };
      titleText = meta.label + " → @" + targetMeta.label;
      subText = "派发任务" + (p.model_tier ? " · 建议档位 " + p.model_tier : "") + " · " + relTime;
    }

    const extraChips = [];
    if (branch) extraChips.push('<button type="button" class="chip-btn mono" data-open-tab="worktrees">⎇ ' + esc(branch) + '</button>');
    artifacts.slice(0, 4).forEach(a => {
      extraChips.push('<button type="button" class="chip-btn mono" data-open-tab="artifacts">📄 ' + esc(String(a).split("/").pop()) + '</button>');
    });

    return '<article class="msg-turn">' +
      '<div class="avatar ' + meta.cls + '" title="' + esc("分支前缀：" + meta.branchPrefix) + '">' + esc(meta.short) + '</div>' +
      '<div class="msg-body">' +
        '<div class="msg-head">' +
          '<span class="msg-author">' + esc(titleText) + '</span>' +
          '<span class="msg-sub tabular-nums" title="' + esc(clockHint) + '">#' + esc(ev.event_id) + ' · ' + esc(subText) + '</span>' +
        '</div>' +
        renderTraceBadges(ev) +
        (noteClip ? '<div class="msg-text">' + renderInline(noteClip) + '</div>' : "") +
        (extraChips.length ? '<div style="display:flex;flex-wrap:wrap;gap:6px;padding-top:2px;">' + extraChips.join("") + '</div>' : "") +
      '</div>' +
    '</article>';
  }

  function missionStatusText(m) {
    if (!m) return "空闲";
    if (m.status === "ready_to_report") {
      return (m.coordinator_peer === "user" || (m.user_messages || []).length) ? "已回复" : "待汇总";
    }
    return ({ active: "进行中", completed: "已完成", cancelled: "已结束", failed: "已结束" })[m.status] || m.status;
  }

  function renderPeerDock(state) {
    const el = document.getElementById("peer-dock");
    if (!el) return;
    const PIDS = ["claude", "chatgpt", "antigravity"];
    const waiters = (state && state.active_waiters) || {};
    const peers = (state && state.peers) || [];
    const models = (state && state.peer_models) || {};
    const efforts = (state && state.peer_efforts) || {};
    const quotas = {};
    ((state && state.quotas) || []).forEach(q => { quotas[q.peer_id] = q; });
    const busyPeers = {};
    ((state && state.missions) || []).forEach(m => (m.tasks || []).forEach(t => {
      if (t.status === "claimed" || t.status === "running") busyPeers[t.assignee_peer] = true;
    }));
    const rows = PIDS.map(pid => {
      const standby = Number(waiters[pid] || 0) > 0;
      const busy = !!busyPeers[pid];
      const q = quotas[pid] || {};
      const known = !(q.current_account_unknown || q.account_switch_suspected) && q.min_remaining_percent !== null && q.min_remaining_percent !== undefined;
      const pct = known ? Math.max(0, Math.min(100, Number(q.min_remaining_percent))) : 0;
      const stTxt = busy ? "处理中" : (standby ? "待命中" : "未待命");
      const outTxt = known && q.exhausted ? ("额度用完" + (q.earliest_reset_in_human ? "，" + q.earliest_reset_in_human + " 后恢复" : "")) : "";
      const modelTxt = [models[pid] || "", efforts[pid] || ""].filter(Boolean).join(" · ");
      const sub = [stTxt].concat(outTxt ? [outTxt] : []).concat(q.account_switch_suspected ? ["账号可能已切换"] : []).join(" · ");
      const ageTxt = (known && q.quota_stale && q.sample_age_seconds) ? agoText(q.sample_age_seconds) : "";
      const color = pid === "claude" ? "var(--peer-claude)" : (pid === "chatgpt" ? "var(--peer-chatgpt)" : "var(--peer-antigravity)");
      const ring = known
        ? '<svg class="dock-ring' + (q.quota_stale ? " stale" : "") + '" viewBox="0 0 24 24" aria-hidden="true"><circle class="bg" cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="9" stroke="' + color + '" stroke-linecap="round" stroke-dasharray="' + (pct * 0.5655).toFixed(1) + ' 56.6"/></svg>' + Math.round(pct) + '%'
        : "";
      const wake = (!standby && !busy) ? '<button type="button" class="wake-btn" data-start-standby="' + pid + '" title="让它开始待命，之后发给它的消息会自动处理">叫醒</button>' : "";
      const acctTxt = (q.active_account_alias || "") + (q.active_plan_type ? "（" + q.active_plan_type + "）" : "");
      const tip = PEER_META[pid].appName + (acctTxt ? " · " + acctTxt : "") +
        (modelTxt ? " · " + modelTxt : "") +
        (known ? " · 余量 " + Math.round(pct) + "%" + (ageTxt ? "（Cockpit " + ageTxt + "的记录）" : "") + (q.earliest_reset_in_human ? "，" + q.earliest_reset_in_human + " 后重置" : "")
               : (q.account_switch_suspected ? " · 账号可能已切换" : (pid === "claude" ? " · Cockpit 没有 Claude 的余量记录" : " · 余量未知"))) +
        (q.switch_hint ? " · " + q.switch_hint : "");
      return '<div class="dock-row">' +
        '<button type="button" class="dock-main" data-peer-pill="' + pid + '" title="' + esc(tip) + '">' +
          '<span class="dock-ic"><span class="peer-icon ' + pid + '" aria-hidden="true"></span><i class="' + (busy ? "busy" : (standby ? "on" : "")) + '"></i></span>' +
          '<span style="min-width:0;"><span class="dock-name">' + esc(PEER_META[pid].label) + '</span><span class="dock-sub">' + esc(sub) + '</span></span>' +
        '</button>' +
        '<span class="dock-side">' + ring + wake + '</span>' +
        (modelTxt ? '<div class="dock-model" data-peer-pill="' + pid + '" title="' + esc(tip) + '">' + esc(modelTxt) + '</div>' : "") +
      '</div>';
    }).join("");
    el.innerHTML = rows;
  }

  function agoText(seconds) {
    const s = Math.max(0, Number(seconds) || 0);
    if (s < 3600) return Math.max(1, Math.round(s / 60)) + " 分钟前";
    if (s < 86400) return Math.round(s / 3600) + " 小时前";
    return Math.round(s / 86400) + " 天前";
  }

  function peerLabel(pid) {
    if (pid === "user") return "你";
    return (PEER_META[pid] || { label: pid || "系统" }).label;
  }

  function renderNote(text, hint) {
    return '<div class="chat-note"' + (hint ? ' title="' + esc(hint) + '"' : "") + '>' + esc(text) + '</div>';
  }

  function renderUserMsg(u) {
    const targets = (u.targets || []).map(peerLabel).join("、");
    const meta = u.mode === "auto" ? "自动 · " + (u.reason || ("交给 " + targets)) : "交给 " + targets;
    const images = (u.attachments || []).map(image => {
      const url = "/api/attachments/" + encodeURIComponent(image.id);
      return '<button type="button" class="sent-image" data-open-image="' + esc(url) + '" data-image-label="' + esc(image.name) +
        '" aria-label="' + esc("查看 " + image.name) + '"><img src="' + esc(url) + '" alt="' + esc(image.name) + '"></button>';
    }).join("");
    return '<div class="chat-user">' +
      (u.text ? '<div class="chat-user-bubble">' + renderMarkdownBlock(u.text) + '</div>' : "") +
      (images ? '<div class="sent-images">' + images + '</div>' : "") +
      '<div class="chat-user-meta" title="' + esc(formatClock(u.created_at)) + '">' + esc(meta) + '</div>' +
    '</div>';
  }

  function reportedTaskModel(peer, task) {
    const label = (task && (task.runtime_model_used || task.model_used) || "").trim();
    if (peer === "antigravity" &&
        (!label || /^(?:antigravity(?:\.app)?(?: orchestrator)?|orchestrator(?: \(.*\))?)$/i.test(label))) {
      return "具体型号未上报";
    }
    return label;
  }

  function renderAgentMsg(ev, task) {
    const p = ev.payload || {};
    const pid = ev.from_peer || "";
    const meta = PEER_META[pid] || { label: peerLabel(pid), short: "S", cls: "system" };
    const st = p.status || (task && task.status) || "done";
    const tag = st === "failed" ? '<span class="chat-tag bad">失败</span>' : (st === "blocked" ? '<span class="chat-tag bad">受阻</span>' : "");
    const artifacts = Array.isArray(p.artifacts) ? p.artifacts : [];
    const chips = artifacts.slice(0, 6).map(a =>
      '<button type="button" class="chip-btn mono" data-open-tab="artifacts">📄 ' + esc(String(a).split("/").pop()) + '</button>'
    ).join("");
    const tn = tierNote(task);
    const sub = [tn.label, reportedTaskModel(pid, task), task && task.effort_used ? task.effort_used : "", relativeAge(ev.created_at)].filter(Boolean).join(" · ");
    return '<article class="chat-agent">' +
      (PEER_META[pid] ? '<span class="peer-icon ' + pid + '" aria-hidden="true"></span>' : '<div class="avatar ' + meta.cls + '">' + esc(meta.short) + '</div>') +
      '<div class="chat-agent-body">' +
        '<div class="chat-agent-head"><span class="msg-author">' + esc(meta.label) + '</span>' + tag +
          '<span class="msg-sub" title="' + esc(formatClock(ev.created_at) + (tn.tip ? " · " + tn.tip : "") +
           (task && task.runtime_model_used && task.model_used ? " · 原始回报 " + task.model_used : "")) + '">' + esc(sub) + '</span></div>' +
        '<div class="chat-agent-text">' + renderMarkdownBlock(p.result_summary || "") + '</div>' +
        (chips ? '<div style="display:flex;flex-wrap:wrap;gap:6px;padding-top:6px;">' + chips + '</div>' : "") +
      '</div>' +
    '</article>';
  }

  function exhaustedQuota(state, peer) {
    const quota = ((state && state.quotas) || []).find(q => q.peer_id === peer);
    return quota && quota.exhausted && !quota.current_account_unknown && !quota.account_switch_suspected
      ? quota
      : null;
  }

  function exhaustedLabel(peer) {
    return peerLabel(peer) + " 当前账号额度已用完";
  }

  function deliveryFacts(mission, task) {
    const events = (mission.events || []).filter(event =>
      event.task_id === task.task_id &&
      event.to_peer === task.assignee_peer &&
      ["task_assigned", "task_requeued", "task_handoff"].includes(event.kind)
    );
    return events.reduce((latest, event) => (!latest || event.event_id > latest.event_id) ? event : latest, null);
  }

  function taskPresentation(task, mission) {
    const event = deliveryFacts(mission, task);
    const payload = (event && event.payload) || {};
    const receiving = task.assignee_peer;
    let label = TASK_STATUS_TEXT[task.status] || task.status;
    let detail = task.progress_note || "";
    let since = "";
    if (task.status === "queued") {
      label = "待认领";
      since = (event && event.created_at) || task.created_at;
      if (event && event.acked_at) detail = "已确认收到通知，等领取任务";
      else if (payload.delivery_mode === "hot_wakeup") detail = "已唤醒监听，等领取任务";
      else if (payload.delivery_mode === "cold_notification" && payload.notified) detail = "系统通知已发，等领取任务";
      else if (payload.delivery_mode === "error" || payload.command_ok === false) detail = "投递失败，可到 " + peerLabel(receiving) + " 取件";
      else detail = "任务已排队";
    } else if (task.status === "claimed") {
      label = "已认领 · 等开始";
      since = task.claimed_at;
    } else if (task.status === "running") {
      label = task.stale_at ? "进度过期" : "已上报进度";
      since = task.last_progress_at || (payload.last_progress_at || "") || (task.stale_at ? "" : task.updated_at);
      if (!detail) detail = "等下一次进度或完成回执";
    }
    return {label, detail, since, event};
  }

  function deliveryTimeline(task, mission) {
    const presentation = taskPresentation(task, mission);
    const event = presentation.event;
    const payload = (event && event.payload) || {};
    const facts = [];
    const add = (time, label) => { if (time) facts.push({time,label}); };
    if (event) {
      add(event.created_at, "任务已排队");
      const attempts = payload.delivery_attempts || payload.delivery_history || [];
      for (const attempt of attempts) {
        const mode = attempt.mode || attempt.delivery;
        const at = attempt.at || attempt.sent_at || attempt.created_at ||
          (attempt.ring_sent_at_ms ? new Date(attempt.ring_sent_at_ms).toISOString() : "");
        add(at, mode === "hot_wakeup" ? "唤醒监听" :
          mode === "cold_notification" ? (attempt.notified ? "系统通知已发" : "补发系统通知") : "尝试投递");
      }
      if (!attempts.length && payload.delivery_mode) {
        const at = payload.ring_sent_at_ms ? new Date(payload.ring_sent_at_ms).toISOString() : event.created_at;
        add(at, payload.delivery_mode === "hot_wakeup" ? "唤醒监听" :
          payload.notified ? "系统通知已发" : "尝试投递");
      }
      add(event.acked_at, "确认收到通知");
    }
    add(task.claimed_at, "已认领");
    if (task.status === "running" && presentation.since) add(presentation.since, "最后上报进度");
    if (task.stale_at) add(task.stale_at, "租约进度过期");
    if (["done", "blocked", "failed", "cancelled"].includes(task.status)) {
      add(task.updated_at, TASK_STATUS_TEXT[task.status] || task.status);
    }
    facts.sort((a,b) => String(a.time).localeCompare(String(b.time)));
    return facts.map(fact => '<div>' + esc(formatClock(fact.time)) + ' · ' + esc(fact.label) + '</div>').join("");
  }

  function renderPending(t, state, mission) {
    const pid = t.assignee_peer;
    const label = peerLabel(pid);
    const facts = taskPresentation(t, mission);
    const exhausted = exhaustedQuota(state, pid);
    const detail = [facts.since ? formatClock(facts.since) : "", facts.detail,
                    t.status === "running" && (t.runtime_model_used || t.model_used) ? reportedTaskModel(pid, t) : ""]
      .filter(Boolean).join(" · ");
    const standby = Number(((state && state.active_waiters) || {})[pid] || 0) > 0;
    const openBtn = (t.status === "queued" && !standby)
      ? '<button type="button" class="btn-link" data-open-native="' + esc(pid) + '">在 ' + esc(label) + ' 里取件</button>'
      : "";
    return '<div class="chat-pending task-receipt' + (exhausted ? ' quota-exhausted' : '') + '" data-task-receipt="' + esc(t.task_id) + '">' +
      '<span class="peer-icon ' + esc(pid) + '" aria-hidden="true"></span>' +
      '<div><strong>' + esc(label + " · " + facts.label) + '</strong>' +
      (detail ? '<div class="receipt-detail">' + esc(detail) + '</div>' : '') +
      (exhausted ? '<div class="receipt-detail">' + esc(exhaustedLabel(pid)) + '</div>' : '') +
      openBtn + '</div></div>';
  }

  // Keep the newest message in view: always after you send or open a conversation,
  // and for incoming replies as long as you have not scrolled up to read earlier ones.
  let scrollToLatest = false;
  let lastShownMission = null;
  let lastThreadHTML = "";
  function renderConversation(state) {
    const container = document.getElementById("chat-container");
    const scroller = document.getElementById("chat-scroll");
    const m = getActiveMission(state);
    document.querySelector(".chat-column").classList.toggle("is-new", !m);
    if (!m) {
      lastShownMission = null;
      lastThreadHTML = "";
      const markEl = document.querySelector(".brand-mark");
      container.innerHTML = '<div class="chat-hero">' + (markEl ? markEl.innerHTML : "") + '<h2>今天会诊什么？</h2></div>';
      return;
    }
    const tasks = m.tasks || [];
    const taskById = {};
    tasks.forEach(t => { taskById[t.task_id] = t; });
    const items = [];
    const closed = ["completed","cancelled","failed"].includes(m.status);
    const pending = closed ? [] : tasks.filter(t => ["queued","claimed","running"].includes(t.status));
    const userMsgs = m.user_messages || [];
    const attached = new Set();
    userMsgs.forEach(u => {
      const linked = pending.filter(task => task.user_message_id === u.message_id);
      linked.forEach(task => attached.add(task.task_id));
      items.push({ts:u.created_at || "", order:0, id:Number(u.message_id),
        html:renderUserMsg(u) + linked.map(task => renderPending(task,state,m)).join("")});
    });
    if (!userMsgs.length) {
      items.push({ ts: m.created_at || "", order: -1, id: 0, html: renderNote(peerLabel(m.coordinator_peer) + " 发起了会诊：" + (m.goal || m.title)) });
    }
    const evs = (m.events || []).slice().sort((a, b) => Number(a.event_id) - Number(b.event_id));
    evs.forEach(ev => {
      const p = ev.payload || {};
      let html = "";
      if (ev.kind === "task_done") html = renderAgentMsg(ev, taskById[p.task_id]);
      else if (ev.kind === "task_assigned" && p.origin !== "user") html = renderNote((ev.from_peer === "user" ? "有一部分工作交给了 " : peerLabel(ev.from_peer) + " 把一部分工作交给了 ") + peerLabel(ev.to_peer), p.prompt_preview || "");
      else if (ev.kind === "task_handoff") html = renderNote(peerLabel(ev.from_peer) + " 把任务交给了 " + peerLabel(ev.to_peer) + (p.note ? "：" + p.note : ""));
      else if (ev.kind === "task_stale") html = renderNote(peerLabel(p.assignee_peer || ev.from_peer) + " 很久没有新进展了");
      else if (ev.kind === "task_requeued") html = renderNote("任务已重新排队" + (p.reason ? "（" + p.reason + "）" : ""));
      else if (ev.kind === "task_superseded") html = renderNote("重复任务已收尾", p.reason || "");
      else if (ev.kind === "review_request") html = renderNote(peerLabel(ev.from_peer) + " 请 " + peerLabel(ev.to_peer) + " 复核", p.question || p.summary || p.reason || "");
      else if (ev.kind === "loop_guard_reached") html = renderNote("复核到了轮次上限，交回协调者处理");
      if (html) items.push({ ts: ev.created_at || "", order: 1, id: Number(ev.event_id), html: html });
    });
    if (m.final_summary && m.coordinator_peer !== "user") {
      items.push({ ts: m.updated_at || "", order: 2, id: 1e12, html: renderAgentMsg(
        { from_peer: m.coordinator_peer, created_at: m.updated_at, payload: { result_summary: m.final_summary, status: "done" } }, null
      ).replace('<span class="msg-sub"', '<span class="chat-tag">汇总</span><button type="button" class="btn-link" data-export-md="1">复制</button><span class="msg-sub"') });
    }
    items.sort((a, b) => (a.ts < b.ts ? -1 : (a.ts > b.ts ? 1 : ((a.order - b.order) || (a.id - b.id)))));
    const wasAtBottom = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 120;
    const switched = lastShownMission !== m.mission_id;
    const markup = items.map(item => item.html).join("") +
      pending.filter(task => !attached.has(task.task_id)).map(task => renderPending(task,state,m)).join("");
    if (switched || lastThreadHTML !== markup) {
      container.innerHTML = markup;
      lastThreadHTML = markup;
      if (scrollToLatest || switched || wasAtBottom) {
        scroller.scrollTop = scroller.scrollHeight;
        requestAnimationFrame(() => { scroller.scrollTop = scroller.scrollHeight; });
        scrollToLatest = false;
      }
    }
    lastShownMission = m.mission_id;
  }

  function durText(seconds) {
    const s = Math.max(0, Math.round(Number(seconds) || 0));
    if (s < 60) return s + " 秒";
    if (s < 3600) return Math.floor(s / 60) + " 分" + (s % 60 ? " " + (s % 60) + " 秒" : "");
    return Math.floor(s / 3600) + " 小时" + (Math.floor((s % 3600) / 60) ? " " + Math.floor((s % 3600) / 60) + " 分" : "");
  }

  // What a task is about, in one line: the user's own words without the routing boilerplate.
  function taskTitle(t) {
    let text = String(t.prompt || "");
    const cut = text.indexOf("————");
    if (cut > 0) text = text.slice(0, cut);
    text = text.replace(/^【[^】]*】\s*/, "").trim();
    const line = (text.split("\n").find(l => l.trim()) || "").trim();
    return line.length > 64 ? line.slice(0, 63) + "…" : (line || "（没有说明）");
  }

  function taskKind(t) {
    if (t.status === "done") return "done";
    if (t.status === "failed" || t.status === "blocked" || t.status === "cancelled") return "bad";
    if (t.status === "claimed" || t.status === "running") return "run";
    return "wait";
  }

  function panelContent(m) {
    // Finished work first, then what is running, then what is still waiting to be picked up.
    const rank = { done: 0, bad: 1, run: 2, wait: 3 };
    const tasks = (m ? (m.tasks || []) : []).map((t, i) => ({ t: t, i: i }))
      .sort((a, b) => (rank[taskKind(a.t)] - rank[taskKind(b.t)]) || (a.i - b.i)).map(x => x.t);
    const files = [];
    const seen = {};
    tasks.forEach(t => (t.artifacts || []).forEach(a => {
      const path = String(a || "").trim();
      const key = t.assignee_peer + "\n" + path;
      if (!path || seen[key]) return;
      seen[key] = true;
      files.push({ peer: t.assignee_peer, path: path });
    }));
    const branches = [];
    tasks.forEach(t => {
      const br = String(t.worktree_branch || "").trim();
      if (!br) return;
      let row = branches.find(x => x.branch === br && x.peer === t.assignee_peer);
      if (!row) { row = { branch: br, peer: t.assignee_peer, files: 0 }; branches.push(row); }
      row.files += (t.artifacts || []).length;
    });
    return { tasks: tasks, files: files, branches: branches };
  }

  function availablePanelTabs(content) {
    return PANEL_TABS.filter(tb => content[tb.key].length > 0);
  }

  function openPanel(key, toggle) {
    const k = PANEL_ALIASES[key] || key;
    if (SETTINGS_TABS.some(tb => tb.key === k)) {
      settingsTab = k;
    } else if (PANEL_TABS.some(tb => tb.key === k)) {
      rightTab = (toggle && rightTab === k) ? null : k;
      if (rightTab) storePref("consilium.panelTab", rightTab);
    }
    if (appState) { renderTopBar(appState); renderRightDrawer(appState); renderSettings(appState); }
  }

  function renderPanelTools(state) {
    const el = document.getElementById("panel-tools");
    if (!el) return;
    const m = getActiveMission(state);
    const content = panelContent(m);
    const avail = availablePanelTabs(content);
    if (!m || !avail.length) { el.innerHTML = ""; return; }
    const active = content.tasks.filter(t => taskKind(t) === "run" || taskKind(t) === "wait").length;
    el.innerHTML = avail.map(tb =>
      '<button type="button" class="panel-tool' + (rightTab === tb.key ? " on" : "") + '" data-open-tab="' + tb.key + '" title="' + esc(tb.label) + '" aria-label="' + esc(tb.label) + '" aria-pressed="' + (rightTab === tb.key ? "true" : "false") + '">' +
        ICONS[tb.key] + (tb.key === "tasks" && active ? '<span class="n">' + active + '</span>' : "") +
      '</button>'
    ).join("") +
    '<span class="panel-sep"></span>' +
    '<button type="button" class="panel-tool" data-toggle-panel="1" title="' + (rightTab ? "隐藏侧栏" : "显示侧栏") + '" aria-label="' + (rightTab ? "隐藏侧栏" : "显示侧栏") + '">' + ICONS.panel + '</button>';
  }

  function renderTasksTab(m, content, state) {
    const waiters = (state && state.active_waiters) || {};
    const closed = m.status === "completed" || m.status === "cancelled" || m.status === "failed";
    const doneCount = content.tasks.filter(t => t.status === "done").length;
    const bar = content.tasks.map(t => {
      const kind = taskKind(t);
      const color = t.assignee_peer === "claude" ? "var(--peer-claude)" : (t.assignee_peer === "chatgpt" ? "var(--peer-chatgpt)" : "var(--peer-antigravity)");
      return '<i class="' + kind + '"' + (kind === "wait" ? "" : ' style="background:' + color + '"') + '></i>';
    }).join("");
    const rows = content.tasks.map(t => {
      const kind = taskKind(t);
      const pid = t.assignee_peer;
      const label = peerLabel(pid);
      const standby = Number(waiters[pid] || 0) > 0;
      const startMs = Date.parse(t.claimed_at || t.created_at || "");
      const endMs = Date.parse(t.updated_at || "");
      let timeTxt = "";
      if (kind === "run" && !Number.isNaN(startMs)) timeTxt = "已进行 " + durText((Date.now() - startMs) / 1000);
      else if (kind === "done" && !Number.isNaN(startMs) && !Number.isNaN(endMs)) timeTxt = "用时 " + durText((endMs - startMs) / 1000);
      else if (kind === "bad") timeTxt = TASK_STATUS_TEXT[t.status] || t.status;
      const exhausted = !closed && (kind === "wait" || kind === "run") && exhaustedQuota(state, pid);
      const presentation = taskPresentation(t, m);
      const phase = (kind === "wait" || kind === "run") ? presentation.label : "";
      const lastFact = presentation.since ? formatClock(presentation.since) : "";
      const tn = tierNote(t);
      const meta = [phase, lastFact, tn.label, kind === "wait" ? "" : reportedTaskModel(pid, t), t.effort_used || "", timeTxt].filter(Boolean).join(" · ");
      const timeline = isNaN(startMs) && !presentation.event ? "" : deliveryTimeline(t, m);
      const isOpen = Object.prototype.hasOwnProperty.call(taskRowOpen, t.task_id) ? taskRowOpen[t.task_id] : (kind === "run" || !!exhausted);
      const note = kind === "done" || kind === "bad" ? (t.result_summary || t.progress_note || "") : (t.progress_note || "");
      const canRequeue = !closed && (t.status === "claimed" || t.status === "running" || t.status === "failed" || t.status === "blocked");
      const canOpen = !closed && kind === "wait" && !standby;
      const canSettle = !closed && kind === "run" && t.attempt_id && content.tasks.some(task => task.status === "done");
      const acts = (canOpen ? '<button type="button" class="pt-btn" data-open-native="' + esc(pid) + '">在 ' + esc(label) + ' 里取件</button>' : "") +
        (!closed && kind === "wait" ? '<button type="button" class="pt-btn" data-notify-task="' + esc(t.task_id) + '">重新通知</button>' : "") +
        (canRequeue ? '<button type="button" class="pt-btn" data-requeue-task="' + esc(t.task_id) + '" title="收回这次处理，让它重新排队">重新入队</button>' : "") +
        (canSettle ? '<button type="button" class="pt-btn" data-settle-task="' + esc(t.task_id) + '">收尾重复任务</button>' : "");
      return '<div class="pt-row' + (isOpen ? " open" : "") + '">' +
        '<span class="pt-st ' + kind + '">' + ICONS[kind] + '</span>' +
        '<div>' +
          '<button type="button" class="pt-title" data-toggle-task="' + esc(t.task_id) + '" aria-expanded="' + (isOpen ? "true" : "false") + '" title="' + esc(t.task_id) + '">' +
            '<span class="peer-icon ' + esc(pid) + '" aria-hidden="true"></span><span class="t">' + esc(taskTitle(t)) + '</span><span class="c">' + ICONS.chev + '</span>' +
          '</button>' +
          (exhausted ? '<div class="pt-quota-warning">' + esc(exhaustedLabel(pid)) + '</div>' : "") +
          (meta ? '<div class="pt-meta"' + (tn.tip ? ' title="' + esc(tn.tip) + '"' : "") + '>' + esc(meta) + '</div>' : "") +
          (isOpen ? (note ? '<div class="pt-note">' + renderMarkdownBlock(note) + '</div>' : "") +
            (timeline ? '<div class="pt-meta">' + timeline + '</div>' : "") +
            (acts ? '<div class="pt-act">' + acts + '</div>' : "") : "") +
        '</div>' +
      '</div>';
    }).join("");
    return '<div class="pt-sum"><span>' + content.tasks.length + ' 项任务 · ' + doneCount + ' 项完成</span><span class="pt-bar">' + bar + '</span></div>' + rows;
  }

  function renderFilesTab(m, content) {
    const canReveal = canPickNativeFolder();
    return ["claude", "chatgpt", "antigravity"].map(pid => {
      const mine = content.files.filter(f => f.peer === pid);
      if (!mine.length) return "";
      return '<div class="pf-group">' + esc(peerLabel(pid)) + ' 改动或产出的</div>' + mine.map(f => {
        const full = f.path.startsWith("/") || !m.workspace_path ? f.path : m.workspace_path.replace(/\/+$/, "") + "/" + f.path;
        const cut = f.path.lastIndexOf("/");
        const name = cut >= 0 ? f.path.slice(cut + 1) : f.path;
        const dir = cut >= 0 ? f.path.slice(0, cut + 1) : "";
        return '<div class="pf-row" title="' + esc(full) + '">' +
          '<span class="ic">' + ICONS.files + '</span>' +
          '<div><div class="nm">' + esc(name || f.path) + '</div>' + (dir ? '<div class="dir">' + esc(dir) + '</div>' : "") + '</div>' +
          '<span class="pf-who"><span class="peer-icon ' + esc(pid) + '" aria-hidden="true"></span></span>' +
          '<span class="pf-act"><button type="button" class="pt-btn" data-copy-text="' + esc(full) + '">复制路径</button>' +
            (canReveal ? '<button type="button" class="pt-btn" data-reveal-path="' + esc(full) + '">在访达中显示</button>' : "") + '</span>' +
        '</div>';
      }).join("");
    }).join("");
  }

  function renderBranchesTab(content) {
    return content.branches.map(b =>
      '<div class="pf-row">' +
        '<span class="ic">' + ICONS.branches + '</span>' +
        '<div><div class="nm">' + esc(b.branch) + '</div><div class="dir">' + esc(peerLabel(b.peer) + (b.files ? " · " + b.files + " 个文件" : "")) + '</div></div>' +
        '<span class="pf-who"><span class="peer-icon ' + esc(b.peer) + '" aria-hidden="true"></span></span>' +
        '<span class="pf-act"><button type="button" class="pt-btn" data-copy-text="' + esc(b.branch) + '">复制</button></span>' +
      '</div>'
    ).join("");
  }

  function renderRightDrawer(state) {
    const drawer = document.getElementById("right-drawer");
    const m = getActiveMission(state);
    const content = panelContent(m);
    const avail = availablePanelTabs(content);
    // Like Claude Code's panes, a tab exists only while it has something to show.
    if (rightTab && !avail.some(tb => tb.key === rightTab)) rightTab = avail.length ? avail[0].key : null;
    if (!rightTab || !state || !m) {
      drawer.classList.add("closed");
      return;
    }
    drawer.classList.remove("closed");
    drawer.style.width = panelWidth + "px";
    document.getElementById("btn-close-drawer").innerHTML = ICONS.close;
    document.getElementById("drawer-tabs-inner").innerHTML = avail.map(tb => (
      '<button type="button" role="tab" class="panel-tab' + (rightTab === tb.key ? " on" : "") + '" aria-selected="' + (rightTab === tb.key ? "true" : "false") + '" data-open-tab="' + tb.key + '" data-keep-open="1">' + esc(tb.label) + ' <small>' + content[tb.key].length + '</small></button>'
    )).join("");
    const body = document.getElementById("drawer-body");
    const keepScroll = body.getAttribute("data-tab") === rightTab + ":" + m.mission_id ? body.scrollTop : 0;
    body.innerHTML = rightTab === "tasks" ? renderTasksTab(m, content, state)
      : (rightTab === "files" ? renderFilesTab(m, content) : renderBranchesTab(content));
    body.setAttribute("data-tab", rightTab + ":" + m.mission_id);
    body.scrollTop = keepScroll;
  }

  function renderSettings(state) {
    const overlay = document.getElementById("settings-overlay");
    if (!settingsTab || !state) { overlay.classList.add("hidden"); return; }
    overlay.classList.remove("hidden");
    document.getElementById("btn-close-settings").innerHTML = ICONS.close;
    document.getElementById("settings-nav").innerHTML = SETTINGS_TABS.map(tb =>
      '<button type="button" class="' + (settingsTab === tb.key ? "on" : "") + '" data-open-settings="' + tb.key + '">' + esc(tb.label) + '</button>'
    ).join("");
    document.getElementById("settings-title").textContent = (SETTINGS_TABS.find(tb => tb.key === settingsTab) || {}).label || "";
    const body = document.getElementById("settings-body");
    if (settingsTab === "accounts") {
      const quotas = state.quotas || [];
      const peersMap = {};
      (state.peers || []).forEach(pr => { peersMap[pr.peer_id] = pr; });
      body.innerHTML =
        quotas.map(q => {
          const meta = PEER_META[q.peer_id] || { appName: q.peer_id };
          const peer = peersMap[q.peer_id] || {};
          const models = Array.isArray(peer.supported_models) ? peer.supported_models : [];
          const modelStr = models.join(", ");
          const hasQ = q.min_remaining_percent !== null && q.min_remaining_percent !== undefined;
          const isDemo = (q.windows || []).some(w => w.source === "demo");
          const statusTxt = q.account_switch_suspected ? "账号可能已切换" : (q.current_account_unknown || !hasQ ? "账号未知" : Math.round(q.min_remaining_percent) + "%");
          const allAcctRows = (q.pool || []).concat(q.historical_accounts || []);
          return '<div class="panel-card">' +
            '<div style="display:flex;align-items:center;justify-content:space-between;">' +
              '<span style="font-weight:600;">' + esc(meta.appName) + '</span>' +
              '<span class="tabular-nums">' + esc(statusTxt) + (isDemo ? " · 演示数据" : "") + '</span>' +
            '</div>' +
            (modelStr ? '<div style="font-size:12px;color:var(--text-muted);">可用模型：<span class="mono">' + esc(modelStr) + '</span></div>' : "") +
            (q.account_switch_suspected ? '<div style="color:var(--status-stale);font-size:12px;">最近样本超过 30 分钟，账号可能已在客户端内切换</div>' : "") +
            (q.switch_hint ? '<div style="color:var(--status-standby);">' + esc(q.switch_hint) + '</div>' : "") +
            (allAcctRows.map(a => '<div style="display:flex;justify-content:space-between;color:var(--text-muted);"><span>' + esc(a.alias) + (a.is_current ? " [当前]" : (a.is_historical ? " [历史账号]" : "")) + '</span><span class="tabular-nums">' + (a.remaining_percent == null ? "未知" : Math.round(a.remaining_percent) + "%") + '</span></div>').join("") || '<div style="color:var(--text-muted);">暂无本地账号样本</div>') +
          '</div>';
        }).join("");
      return;
    }
    if (settingsTab === "prefs") {
      const prefs = (state && state.preferences) || [];
      body.innerHTML = '<div style="font-size:12.5px;color:var(--text-muted);margin-bottom:8px;">三端从你的使用中学到的习惯。自动分配时会交给协调者参考；不对的直接删掉。</div>' +
        (prefs.length ? prefs.map(pf =>
          '<div class="panel-card"><div class="pref-row">' +
            '<span style="font-size:13px;">' + renderInline(pf.text) + '</span>' +
            '<button type="button" class="btn-link-danger" data-del-pref="' + esc(pf.pref_id) + '">删除</button>' +
          '</div><div style="font-size:11.5px;color:var(--text-muted);">' + esc(peerLabel(pf.source) + " 记录 · " + relativeAge(pf.created_at)) + '</div></div>'
        ).join("") : '<div class="panel-card" style="font-size:12.5px;color:var(--text-muted);">还没有学到偏好。</div>');
    }
  }

  function activeDraft() {
    if (!messageDrafts.has(currentDraftKey)) messageDrafts.set(currentDraftKey, {text:"", images:[], receipt:null});
    return messageDrafts.get(currentDraftKey);
  }

  function selectComposerDraft(mission) {
    const next = mission ? mission.mission_id : "new";
    if (currentDraftKey === next) return;
    const input = document.getElementById("composer-input");
    if (currentDraftKey !== null) activeDraft().text = input.value;
    currentDraftKey = next;
    input.value = activeDraft().text;
    slashOpen = false;
    slashDismissed = null;
  }

  function newClientID() {
    return Array.from(crypto.getRandomValues(new Uint8Array(16)), n => n.toString(16).padStart(2,"0")).join("");
  }

  function renderDraftAttachments() {
    const items = activeDraft().images;
    const el = document.getElementById("composer-attachments");
    const markup = items.map(image => '<div class="attachment-chip"><img src="' + esc(image.preview) + '" alt="">' +
      '<span>' + esc(image.file.name || "剪贴板图片") + esc(image.error ? " · " + image.error : "") + '</span>' +
      '<button type="button" class="btn-link remove-image" data-remove-image="' + image.id + '"' +
      (sending ? ' disabled' : '') + ' aria-label="' +
      esc("移除 " + (image.file.name || "图片")) + '">×</button></div>').join("");
    if (el.dataset.rendered !== markup) {
      el.dataset.rendered = markup;
      el.innerHTML = markup;
    }
  }

  function addComposerImages(files) {
    const supported = ["image/png", "image/jpeg", "image/webp", "image/gif"];
    const incoming = Array.from(files || []);
    const valid = incoming.filter(file => supported.includes(file.type));
    for (const file of valid) {
      activeDraft().images.push({id:newClientID(),file,preview:URL.createObjectURL(file),uploaded:false,error:""});
    }
    const notice = document.getElementById("attachment-error");
    notice.textContent = incoming.length === valid.length ? "" : "部分文件无法上传。请选择 PNG、JPEG、WebP 或 GIF。";
    notice.classList.toggle("hidden", !notice.textContent);
    renderComposerBar(appState);
  }

  function renderAccountNotices(state) {
    let changed = false;
    for (const quota of (state && state.quotas) || []) {
      const peer = quota.peer_id;
      const previous = observedAccounts[peer];
      const nextKnown = quota.active_account_id && !quota.current_account_unknown && quota.min_remaining_percent != null;
      if (quota.account_switch_suspected || (previous && quota.active_account_id &&
          previous !== quota.active_account_id && !nextKnown)) {
        accountNotices.set(peer, peerLabel(peer) + " 正在等新账号额度同步。");
        continue;
      }
      if (!nextKnown) continue;
      if (previous && previous !== quota.active_account_id) {
        accountNotices.set(peer, peerLabel(peer) + " 已换号 · 额度 " + Math.round(quota.min_remaining_percent) + "%。");
      }
      if (previous !== quota.active_account_id) {
        observedAccounts[peer] = quota.active_account_id;
        changed = true;
      } else if ((accountNotices.get(peer) || "").includes("正在等新账号")) {
        accountNotices.delete(peer);
      }
    }
    if (changed) storePref("consilium.observedAccounts", JSON.stringify(observedAccounts));
    const box = document.getElementById("account-notice");
    const message = Array.from(accountNotices.entries())
      .filter(([peer, text]) => dismissedAccountNotices.get(peer) !== text)
      .map(([, text]) => text).join(" ");
    box.classList.toggle("hidden", !message);
    const label = box.querySelector("span");
    if (label.textContent !== message) label.textContent = message;
  }

  function renderComposerBar(state) {
    const waiters = (state && state.active_waiters) || {};
    const curMission = getActiveMission(state);
    selectComposerDraft(curMission);
    renderDraftAttachments();
    renderAccountNotices(state);
    const compact = !curMission;
    const chips = ['<button type="button" class="target-chip' + (sendTargets.length === 0 ? " active" : "") + '" data-send-target="auto" title="不点名：交给待命中、额度充足的一端，由它决定怎么分">自动</button>'];
    ["claude", "chatgpt", "antigravity"].forEach(pid => {
      const standby = Number(waiters[pid] || 0) > 0;
      chips.push('<button type="button" class="target-chip' + (sendTargets.indexOf(pid) >= 0 ? " active" : "") + '" data-send-target="' + pid + '" title="' + esc("交给 " + PEER_META[pid].label + (standby ? "（待命中）" : "（没在待命）")) + '">' +
        '<span class="peer-icon ' + pid + '" aria-hidden="true"></span>' + (compact ? "" : esc(PEER_META[pid].label)) + '</button>');
    });
    document.getElementById("handoff-targets").innerHTML = chips.join("");
    document.getElementById("composer-input").placeholder = compact ? "说说要做的事…" : "继续说…";

    const optsEl = document.getElementById("compose-opts");
    if (optsEl) {
      const tierScope = curMission ? curMission.mission_id : "new";
      if (tierSyncedFor !== tierScope) {
        composeTier = curMission ? tierFromMission(curMission) : "auto";
        tierSyncedFor = tierScope;
      }
      const tierLabel = (TIERS.find(t => t[0] === composeTier) || TIERS[0])[1];
      const tierBtnHtml = '<button type="button" class="tier-btn" data-tier-menu="1" title="档位：决定用多强的模型、想多久。自动＝由处理的一端按难度自己定">' +
        '<span class="tier-k">档位</span>' + esc(tierLabel) + ' <span class="caret">▾</span></button>';
      const tierMenuHtml = tierMenuOpen
        ? '<div class="tier-menu' + (curMission ? " solo" : "") + '" role="menu">' + TIERS.map(t =>
            '<button type="button" class="folder-item' + (t[0] === composeTier ? " current" : "") + '" data-tier="' + t[0] + '"><span>' + t[1] + '</span><span class="path">' + t[2] + '</span></button>'
          ).join("") + '</div>'
        : "";
      const closedMission = curMission && (curMission.status === "completed" || curMission.status === "cancelled" || curMission.status === "failed");
      if (curMission) {
        optsEl.innerHTML = closedMission ? "" : tierBtnHtml + tierMenuHtml;
      } else {
        const recents = recentFolders(state);
        if (!composeWorkspace && recents.length) composeWorkspace = recents[0];
        optsEl.innerHTML =
          '<button type="button" class="folder-btn' + (composeWorkspace ? "" : " missing") + '" data-folder-menu="1" title="' + esc(composeWorkspace || "这个会诊在哪个文件夹里工作") + '">📁 <span class="name">' + esc(composeWorkspace ? folderName(composeWorkspace) : "选择文件夹") + '</span> <span class="caret">▾</span></button>' +
          tierBtnHtml +
          (folderMenuOpen
            ? '<div class="folder-menu" role="menu">' +
                (recents.length ? '<div class="folder-menu-label">最近用过</div>' + recents.map(f =>
                  '<button type="button" class="folder-item' + (f === composeWorkspace ? " current" : "") + '" data-pick-folder="' + esc(f) + '"><span>' + esc(folderName(f)) + '</span><span class="path">' + esc(f) + '</span></button>'
                ).join("") + '<div class="folder-sep"></div>' : "") +
                '<button type="button" class="folder-item" data-open-folder-picker="1"><span>选择文件夹…</span></button>' +
              '</div>'
            : "") +
          tierMenuHtml;
      }
    }
    const quotaPeers = new Set(sendTargets);
    if (curMission && !["completed", "cancelled", "failed"].includes(curMission.status)) {
      if (!sendTargets.length && PEER_META[curMission.coordinator_peer]) {
        quotaPeers.add(curMission.coordinator_peer);
      }
      for (const task of curMission.tasks || []) {
        if (["queued", "claimed", "running"].includes(task.status)) quotaPeers.add(task.assignee_peer);
      }
    }
    const warnings = [];
    for (const peer of quotaPeers) {
      const quota = exhaustedQuota(state, peer);
      if (quota) {
        warnings.push(exhaustedLabel(peer) + "。" +
          (quota.switch_hint || "可切换账号，或发送时改选接单端。"));
      }
    }
    const quotaWarning = document.getElementById("quota-warning");
    const warning = warnings.join("\n");
    quotaWarning.classList.toggle("hidden", !warning);
    if (quotaWarning.textContent !== warning) quotaWarning.textContent = warning;

    const anyStandby = ["claude", "chatgpt", "antigravity"].some(pid => Number(waiters[pid] || 0) > 0);
    const banner = document.getElementById("standby-banner");
    if (state && !anyStandby) {
      banner.innerHTML = '<span>现在没有一端在待命，消息要等它们上线才会处理。让它们待命：</span>' +
        '<button type="button" class="btn-link" data-start-standby="claude">Claude</button>' +
        '<button type="button" class="btn-link" data-start-standby="chatgpt">Codex</button>' +
        '<button type="button" class="btn-link" data-start-standby="antigravity">Antigravity</button>';
      banner.classList.remove("hidden");
    } else {
      banner.classList.add("hidden");
    }
    const val = document.getElementById("composer-input").value || "";
    document.getElementById("btn-send").disabled = sending || (!val.trim() && !activeDraft().images.length);

    const pal = document.getElementById("slash-palette");
    const q = /^\/[a-z]*$/i.test(val) ? val.toLowerCase() : null;
    const showPal = slashOpen || (q !== null && val !== slashDismissed);
    const matches = showPal ? SLASH_COMMANDS.filter(c => !q || q === "/" || c.cmd.startsWith(q)) : [];
    slashIndex = Math.min(slashIndex, Math.max(0, matches.length - 1));
    const commands = matches.map((c, index) =>
      '<button type="button" class="slash-item" id="slash-command-' + index + '" data-slash-act="' + c.action +
      '" aria-selected="' + String(index === slashIndex) + '" role="option">' +
        '<span class="slash-cmd">' + esc(c.cmd) + '</span>' +
        '<span class="muted">' + esc(c.desc) + '</span>' +
      '</button>'
    ).join("");
    if (pal.dataset.rendered !== commands) {
      pal.dataset.rendered = commands;
      pal.innerHTML = commands;
    }
    pal.classList.toggle("hidden", !matches.length);
    const message = document.getElementById("composer-input");
    if (matches.length) message.setAttribute("aria-activedescendant", "slash-command-" + slashIndex);
    else message.removeAttribute("aria-activedescendant");
    document.getElementById("btn-slash-toggle").setAttribute("aria-expanded", String(!!matches.length));
  }

  function renderAll(state) {
    appState = state;
    if (state.daemon) {
      baseUptime = Number(state.daemon.uptime_seconds || 0);
      baseUptimeStamp = Date.now();
      document.getElementById("uptime-val").textContent = formatDuration(baseUptime);
      document.getElementById("version-pill").textContent = "v" + (state.daemon.version || "__VERSION__");
    }
    renderSidebar(state);
    renderTopBar(state);
    renderConversation(state);
    renderRightDrawer(state);
    renderSettings(state);
    renderComposerBar(state);
  }

  function applyStateData(data) {
    if (!data || typeof data !== "object") return false;
    const service = data.daemon || {};
    const instance = data.state_instance_id || data.instance_id || service.instance_id || service.started_at || "";
    if (instance && retiredStateInstances.has(instance)) return false;
    if (instance && instance !== lastStateInstance) {
      if (lastStateInstance) retiredStateInstances.add(lastStateInstance);
      lastStateInstance = instance;
      lastStateVersion = 0;
    }
    if (typeof data.csrf_token === "string" && data.csrf_token) csrfToken = data.csrf_token;
    const ver = Number(data.state_version || 0);
    if (ver < lastStateVersion) return false;
    lastStateVersion = ver;
    lastSnapshotAt = data.server_time || new Date().toISOString();
    nativeReauthAttempted = false;
    showLogin(false);
    renderAll(data);
    renderSyncNotice();
    return true;
  }

  function showLogin(show) {
    document.getElementById("login-overlay").classList.toggle("hidden", !show);
    if (show) document.getElementById("token-input").focus();
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

  function writeHeaders() {
    const hdrs = { "Content-Type": "application/json" };
    if (csrfToken) hdrs["X-Consilium-CSRF"] = csrfToken;
    return hdrs;
  }

  async function fetchState() {
    const request = ++fetchSequence;
    const res = await fetch("/api/state", { credentials: "same-origin", cache: "no-store" });
    if (request !== fetchSequence) return true;
    if (res.status === 401) {
      stopStreams();
      csrfToken = "";
      if (canPickNativeFolder() && !nativeReauthAttempted && !nativeReauthPending) {
        nativeReauthAttempted = true;
        nativeReauthPending = true;
        setConn("", "连接中断");
        window.webkit.messageHandlers.consilium.postMessage({action:"reauthenticate"});
      } else if (!nativeReauthPending) {
        setConn("", "待登录");
        showLogin(true);
      }
      return false;
    }
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    if (request !== fetchSequence) return true;
    applyStateData(data);
    return true;
  }

  window.__consiliumReauthenticated = function (ok) {
    nativeReauthPending = false;
    if (!ok) { showLogin(true); return; }
    fetchState().then(ready => { if (ready) connectSSE(); }).catch(() => showLogin(true));
  };

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
      if (eventSource !== es) return;
      if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
      if (sseReconnectTimer) { clearInterval(sseReconnectTimer); sseReconnectTimer = null; }
      setConn("", "连接中");
    };
    es.addEventListener("state", ev => {
      if (eventSource !== es) return;
      try { if (applyStateData(JSON.parse(ev.data))) setConn("live", "SSE 实时"); } catch (_) {}
    });
    es.addEventListener("update", ev => {
      if (eventSource !== es) return;
      try {
        const payload = JSON.parse(ev.data);
        if (payload.state && applyStateData(payload.state)) setConn("live", "SSE 实时");
      } catch (_) {}
    });
    es.onerror = () => {
      if (eventSource === es) {
        es.close();
        eventSource = null;
      } else {
        try { es.close(); } catch (_) {}
        return;
      }
      setConn("", "连接中断");
      fetchState().then(ok => {
        if (ok) {
          startPollingFallback();
          scheduleSSEReconnect();
        }
      }).catch(() => {
        startPollingFallback();
        scheduleSSEReconnect();
      });
    };
  }

  function exportMissionMarkdown() {
    const m = getActiveMission(appState);
    if (!m) { showToast("暂无可导出的会诊"); return; }
    const lines = ["# " + m.title + " (" + m.mission_id + ")", "", "目标：" + (m.goal || ""), ""];
    if (m.final_summary) {
      lines.push("## 最终汇总", "", m.final_summary, "");
    }
    lines.push("## 任务明细");
    (m.tasks || []).forEach(t => lines.push("- [" + t.status + "] " + t.assignee_peer + " (" + t.task_id + "): " + (t.progress_note || t.result_summary || t.prompt || "")));
    copyTextSafe(lines.join("\n"), "已复制当前会诊 Markdown 到剪贴板");
  }

  // Event Listeners
  document.getElementById("sidebar-toggle-btn").addEventListener("click", () => {
    if (isNarrowViewport()) sidebarMobileOpen = !sidebarMobileOpen;
    else sidebarDesktopOpen = !sidebarDesktopOpen;
    renderSidebar(appState);
  });

  window.addEventListener("resize", () => {
    if (isNarrowViewport()) sidebarMobileOpen = false;
    if (appState) renderSidebar(appState);
  });

  document.getElementById("btn-close-drawer").addEventListener("click", () => {
    rightTab = null;
    if (appState) { renderTopBar(appState); renderRightDrawer(appState); }
  });
  // ⌘\ closes the panel, Escape closes settings.
  document.addEventListener("keydown", ev => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key === "\\" && rightTab) {
      ev.preventDefault();
      rightTab = null;
      if (appState) { renderTopBar(appState); renderRightDrawer(appState); }
    } else if (ev.key === "Escape" && settingsTab) {
      settingsTab = null;
      if (appState) renderSettings(appState);
    }
  });
  // Drag the panel's left edge to resize it; the width is remembered.
  (function () {
    const grip = document.getElementById("drawer-grip");
    const drawer = document.getElementById("right-drawer");
    let dragging = false;
    grip.addEventListener("mousedown", ev => { dragging = true; drawer.classList.add("resizing"); ev.preventDefault(); });
    document.addEventListener("mousemove", ev => {
      if (!dragging) return;
      panelWidth = Math.round(Math.max(280, Math.min(Math.min(640, window.innerWidth * 0.5), window.innerWidth - ev.clientX)));
      drawer.style.width = panelWidth + "px";
    });
    document.addEventListener("mouseup", () => {
      if (!dragging) return;
      dragging = false;
      drawer.classList.remove("resizing");
      storePref("consilium.panelWidth", panelWidth);
    });
  })();
  window.__consiliumRevealResult = function (ok) { if (!ok) showToast("找不到这个文件，可能已被移动或还没生成"); };

  document.getElementById("composer-input").addEventListener("input", () => {
    activeDraft().text = document.getElementById("composer-input").value;
    slashDismissed = null;
    slashIndex = 0;
    renderComposerBar(appState);
  });
  document.addEventListener("keydown", ev => {
    if ((ev.key === "Enter" || ev.key === " ") && ev.target && ev.target.classList && ev.target.classList.contains("mission-item")) {
      ev.preventDefault();
      ev.target.click();
    }
  });
  document.addEventListener("keydown", ev => {
    if (ev.key === "Escape") {
      if (slashOpen || !document.getElementById("slash-palette").classList.contains("hidden")) {
        slashOpen = false;
        slashDismissed = document.getElementById("composer-input").value;
        renderComposerBar(appState);
      }
      if (openPopoverPeer) {
        openPopoverPeer = null;
        if (appState) renderTopBar(appState);
      }
    }
  });

  async function uploadDraftImage(image) {
    if (image.uploaded) return;
    image.error = "";
    const headers = writeHeaders();
    headers["Content-Type"] = "application/octet-stream";
    try {
      const response = await fetch("/api/attachments/" + image.id, {
        method:"POST", credentials:"same-origin", headers, body:image.file
      });
      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.error || ("HTTP " + response.status));
      }
      image.uploaded = true;
    } catch (error) {
      image.error = "上传失败";
      throw error;
    }
  }

  async function sendMessage() {
    const inp = document.getElementById("composer-input");
    const text = (inp.value || "").trim();
    const draft = activeDraft();
    const draftKey = currentDraftKey;
    const photos = draft.images.slice();
    if ((!text && !photos.length) || sending) return;
    const cur = getActiveMission(appState);
    if (!cur && !composeWorkspace) {
      folderMenuOpen = true;
      renderComposerBar(appState);
      showToast("先选择这个会诊要在哪个文件夹里工作");
      return;
    }
    const open = cur && !(cur.status === "completed" || cur.status === "cancelled" || cur.status === "failed");
    const path = open ? "/api/missions/" + encodeURIComponent(cur.mission_id) + "/message" : "/api/conversations";
    const recipients = sendTargets.slice();
    const folder = cur ? (cur.workspace_path || "") : composeWorkspace;
    const payload = {
      text, targets: recipients, tier: composeTier,
      attachments:photos.map(image => ({id:image.id, name:image.file.name || "剪贴板图片"}))
    };
    if (!open) payload.workspace = folder || composeWorkspace;
    const wire = JSON.stringify([path,payload]);
    if (!draft.receipt || draft.receipt.wire !== wire) draft.receipt = {wire, id:newClientID()};
    payload.client_request_id = draft.receipt.id;

    sending = true;
    let accepted = false;
    draft.text = "";
    inp.value = "";
    inp.style.height = "";
    slashOpen = false;
    const restoreDraft = () => {
      const current = currentDraftKey === draftKey ? inp.value : draft.text;
      if (!current) {
        draft.text = text;
        if (currentDraftKey === draftKey) {
          inp.value = text;
          inp.dispatchEvent(new Event("input"));
        }
      }
    };
    renderComposerBar(appState);
    try {
      for (const image of photos) await uploadDraftImage(image);
      const res = await fetch(path, {
        method: "POST", credentials:"same-origin", headers:writeHeaders(), body:JSON.stringify(payload)
      });
      if (res.status === 401) { restoreDraft(); showLogin(true); return; }
      const data = await res.json().catch(() => ({}));
      if (!res.ok) { restoreDraft(); showToast(data.error || ("发送失败（HTTP " + res.status + "）")); return; }
      accepted = true;
      draft.receipt = null;
      const sentIDs = new Set(photos.map(image => image.id));
      draft.images = draft.images.filter(image => !sentIDs.has(image.id));
      photos.forEach(image => URL.revokeObjectURL(image.preview));
      if (currentDraftKey === draftKey) {
        scrollToLatest = true;
        composingNew = false;
        selectedMissionId = data.result.mission_id;
      }
      if (!cur) rememberFolder(folder);
      const codexWaiting = Number(((appState && appState.active_waiters) || {}).chatgpt || 0) > 0;
      const codexBound = cur && cur.native_sessions && cur.native_sessions.chatgpt;
      if (recipients.includes("chatgpt") && !codexWaiting && !codexBound) {
        const codexUrl = buildHandoffDeeplink("chatgpt", "调用 consilium_pickup_inbox(peer_id=\"chatgpt\") 认领 Consilium 里的新消息，按 consilium-collab 技能处理，并用 consilium_bind_session 记下这个会话。", folder);
        if (codexUrl) window.location.href = codexUrl;
      }
      await fetchState();
    } catch (err) {
      if (!accepted) restoreDraft();
      showToast(accepted ? "已发送，但刷新界面时出错：" + err : "发送失败：" + err);
    } finally {
      sending = false;
      renderComposerBar(appState);
    }
  }

  document.getElementById("btn-send").addEventListener("click", sendMessage);
  document.getElementById("dismiss-account-notice").addEventListener("click", () => {
    for (const [peer, text] of accountNotices) dismissedAccountNotices.set(peer, text);
    renderAccountNotices(appState);
  });
  const imageInput = document.getElementById("composer-image");
  const messageInput = document.getElementById("composer-input");
  const composerBox = document.getElementById("composer-box");
  document.getElementById("btn-attach-image").addEventListener("click", () => imageInput.click());
  imageInput.addEventListener("change", () => {
    if (imageInput.files.length) addComposerImages(imageInput.files);
    imageInput.value = "";
    messageInput.focus();
  });
  messageInput.addEventListener("paste", ev => {
    if (ev.clipboardData && ev.clipboardData.files.length) {
      ev.preventDefault();
      addComposerImages(ev.clipboardData.files);
    }
  });
  composerBox.addEventListener("dragover", ev => {
    if (ev.dataTransfer && ev.dataTransfer.types.includes("Files")) {
      ev.preventDefault();
      composerBox.classList.add("dragging-image");
    }
  });
  composerBox.addEventListener("dragleave", ev => {
    if (!composerBox.contains(ev.relatedTarget)) composerBox.classList.remove("dragging-image");
  });
  composerBox.addEventListener("drop", ev => {
    composerBox.classList.remove("dragging-image");
    if (ev.dataTransfer && ev.dataTransfer.files.length) {
      ev.preventDefault();
      addComposerImages(ev.dataTransfer.files);
      messageInput.focus();
    }
  });
  document.getElementById("btn-slash-toggle").addEventListener("click", () => {
    if (!document.getElementById("slash-palette").classList.contains("hidden")) {
      slashOpen = false;
      slashDismissed = messageInput.value;
    } else {
      slashOpen = true;
      slashIndex = 0;
    }
    renderComposerBar(appState);
    messageInput.focus();
  });
  // Input methods (pinyin etc.) use Enter to confirm a candidate. WebKit reports that
  // Enter with isComposing=false right after compositionend, so also check keyCode 229
  // and the time since the composition ended; otherwise confirming a word sends the message.
  let imeComposing = false;
  let imeEndedAt = 0;
  function isImeEnter(ev) {
    return ev.isComposing || ev.keyCode === 229 || imeComposing || (Date.now() - imeEndedAt) < 150;
  }
  document.getElementById("composer-input").addEventListener("compositionstart", () => { imeComposing = true; });
  document.getElementById("composer-input").addEventListener("compositionend", () => { imeComposing = false; imeEndedAt = Date.now(); });
  document.getElementById("composer-input").addEventListener("keydown", ev => {
    if (isImeEnter(ev)) return;
    const pal = document.getElementById("slash-palette");
    if (!pal.classList.contains("hidden") && (ev.key === "ArrowDown" || ev.key === "ArrowUp")) {
      ev.preventDefault();
      const count = pal.childElementCount;
      slashIndex = (slashIndex + count + (ev.key === "ArrowDown" ? 1 : -1)) % count;
      renderComposerBar(appState);
      pal.children[slashIndex].scrollIntoView({block:"nearest"});
      return;
    }
    if (ev.key === "Enter" && !ev.shiftKey) {
      ev.preventDefault();
      if (!pal.classList.contains("hidden")) pal.children[slashIndex].click();
      else sendMessage();
    }
  });
  document.getElementById("composer-input").addEventListener("input", ev => {
    const el = ev.target;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 220) + "px";
  });
  document.addEventListener("keydown", async ev => {
    if (!ev.target || ev.target.id !== "title-input") return;
    if (ev.key === "Escape") {
      renamingMission = null;
      if (appState) renderTopBar(appState);
      const rb = document.getElementById("rename-title-btn");
      if (rb) rb.focus();
      return;
    }
    if (ev.key !== "Enter") return;
    ev.preventDefault();
    const mid = renamingMission;
    const title = (ev.target.value || "").trim();
    if (!title) return;
    try {
      const res = await fetch("/api/missions/" + encodeURIComponent(mid) + "/title", {
        method: "POST", credentials: "same-origin", headers: writeHeaders(), body: JSON.stringify({ title: title })
      });
      if (res.status === 401) { showLogin(true); return; }
      const data = await res.json().catch(() => ({}));
      if (!res.ok) { showToast(data.error || "改名失败"); return; }
      renamingMission = null;
      await fetchState();
      const rb = document.getElementById("rename-title-btn");
      if (rb) rb.focus();
    } catch (err) {
      showToast("改名失败：" + err);
    }
  });

  document.addEventListener("keydown", ev => {
    if ((ev.metaKey || ev.ctrlKey) && !ev.shiftKey && !ev.altKey && ev.key.toLowerCase() === "n") {
      ev.preventDefault();
      document.getElementById("new-chat-btn").click();
    }
  });
  document.getElementById("new-chat-btn").addEventListener("click", () => {
    composingNew = true;
    selectedMissionId = null;
    if (appState) renderAll(appState);
    document.getElementById("composer-input").focus();
  });

  document.addEventListener("click", ev => {
    if (!ev.target.closest("#slash-palette") && !ev.target.closest("#btn-slash-toggle") && slashOpen) {
      slashOpen = false;
      renderComposerBar(appState);
    }

    const themeBtn = ev.target.closest("[data-set-theme]");
    if (themeBtn) { applyTheme(themeBtn.getAttribute("data-set-theme")); return; }

    const archiveBtn = ev.target.closest("[data-archive-mission]");
    if (archiveBtn) {
      const amid = archiveBtn.getAttribute("data-archive-mission");
      const undo = archiveBtn.getAttribute("data-archive-undo") === "1";
      fetch("/api/missions/" + encodeURIComponent(amid) + (undo ? "/unarchive" : "/archive"), {
        method: "POST", credentials: "same-origin", headers: writeHeaders(), body: "{}"
      }).then(r => { if (r.status === 401) showLogin(true); if (!undo && selectedMissionId === amid) { composingNew = true; selectedMissionId = null; } return fetchState(); })
        .then(() => showToast(undo ? "已取消归档" : "已归档，只是在 Consilium 里收起"))
        .catch(() => showToast("操作失败"));
      return;
    }

    const missionBtn = ev.target.closest("[data-select-mission]");
    if (missionBtn) {
      selectedMissionId = missionBtn.getAttribute("data-select-mission");
      composingNew = false;
      if (isNarrowViewport()) sidebarMobileOpen = false;
      if (appState) renderAll(appState);
      return;
    }

    const pillBtn = ev.target.closest("[data-peer-pill]");
    if (pillBtn) {
      ev.stopPropagation();
      const pid = pillBtn.getAttribute("data-peer-pill");
      openPopoverPeer = openPopoverPeer === pid ? null : pid;
      if (appState) renderTopBar(appState);
      return;
    }

    if (ev.target.closest("[data-close-popover]")) {
      openPopoverPeer = null;
      if (appState) renderTopBar(appState);
      return;
    }

    const openTabBtn = ev.target.closest("[data-open-tab]");
    if (openTabBtn) {
      // Top-bar icons toggle; the panel's own tabs and chips in the chat just switch to that tab.
      openPanel(openTabBtn.getAttribute("data-open-tab"), openTabBtn.classList.contains("panel-tool"));
      return;
    }
    if (ev.target.closest("[data-toggle-panel]")) {
      if (rightTab) { rightTab = null; if (appState) { renderTopBar(appState); renderRightDrawer(appState); } }
      else openPanel(storedPref("consilium.panelTab", "tasks"), false);
      return;
    }
    const settingsBtn = ev.target.closest("[data-open-settings]");
    if (settingsBtn) { openPanel(settingsBtn.getAttribute("data-open-settings"), false); return; }
    if (ev.target.closest("#btn-close-settings") || ev.target.id === "settings-overlay") {
      settingsTab = null;
      if (appState) renderSettings(appState);
      return;
    }
    const taskToggle = ev.target.closest("[data-toggle-task]");
    if (taskToggle) {
      const tid = taskToggle.getAttribute("data-toggle-task");
      taskRowOpen[tid] = taskToggle.getAttribute("aria-expanded") !== "true";
      if (appState) renderRightDrawer(appState);
      return;
    }
    const copyBtn = ev.target.closest("[data-copy-text]");
    if (copyBtn) { copyTextSafe(copyBtn.getAttribute("data-copy-text"), "已复制"); return; }
    const revealBtn = ev.target.closest("[data-reveal-path]");
    if (revealBtn) {
      if (canPickNativeFolder()) window.webkit.messageHandlers.consilium.postMessage({ action: "revealPath", path: revealBtn.getAttribute("data-reveal-path") });
      return;
    }

    const removeImage = ev.target.closest("[data-remove-image]");
    if (removeImage) {
      const draft = activeDraft();
      const id = removeImage.getAttribute("data-remove-image");
      const image = draft.images.find(item => item.id === id);
      if (image) {
        URL.revokeObjectURL(image.preview);
        draft.images = draft.images.filter(item => item.id !== id);
        if (image.uploaded) {
          fetch("/api/attachments/" + encodeURIComponent(id) + "/discard", {
            method:"POST", credentials:"same-origin", headers:writeHeaders(), body:"{}"
          }).catch(() => {});
        }
        renderComposerBar(appState);
        messageInput.focus();
      }
      return;
    }
    const openImage = ev.target.closest("[data-open-image]");
    if (openImage) {
      const dialog = document.getElementById("image-preview");
      const image = dialog.querySelector("img");
      image.src = openImage.getAttribute("data-open-image");
      image.alt = openImage.getAttribute("data-image-label");
      dialog.showModal();
      return;
    }
    const slashActBtn = ev.target.closest("[data-slash-act]");
    if (slashActBtn) {
      const act = slashActBtn.getAttribute("data-slash-act");
      slashOpen = false;
      const inp = document.getElementById("composer-input");
      if (/^\/[a-z]*$/i.test(inp.value)) inp.value = "";
      slashDismissed = inp.value;
      if (act === "focus_mission") {
        if (isNarrowViewport()) sidebarMobileOpen = true;
        else sidebarDesktopOpen = true;
        if (appState) renderSidebar(appState);
        document.getElementById("mission-list").scrollIntoView({ block: "nearest" });
        showToast("已定位会诊列表");
      } else if (act.startsWith("tab:")) {
        openPanel(act.slice(4), false);
      } else if (act === "export_md") {
        exportMissionMarkdown();
      } else if (act === "new_chat") {
        composingNew = true;
        selectedMissionId = null;
        if (appState) renderAll(appState);
      }
      renderComposerBar(appState);
      if (act === "new_chat" || act === "focus_mission" || act === "export") inp.focus();
      return;
    }

    const sendTargetBtn = ev.target.closest("[data-send-target]");
    if (sendTargetBtn) {
      const tgt = sendTargetBtn.getAttribute("data-send-target");
      if (tgt === "auto") sendTargets = [];
      else if (sendTargets.indexOf(tgt) >= 0) sendTargets = sendTargets.filter(x => x !== tgt);
      else sendTargets = sendTargets.concat([tgt]);
      renderComposerBar(appState);
      return;
    }

    const openNativeBtn = ev.target.closest("[data-open-native]");
    if (openNativeBtn) {
      const pid = openNativeBtn.getAttribute("data-open-native");
      const curM = getActiveMission(appState);
      const txt = "调用 consilium_pickup_inbox(peer_id=\"" + pid + "\") 认领 Consilium 里的新消息，按 consilium-collab 技能处理。";
      const url = buildHandoffDeeplink(pid, txt, curM ? curM.workspace_path : "");
      if (!url) { copyTextSafe(txt, "已复制，请到 " + peerLabel(pid) + " 里粘贴"); return; }
      window.location.href = url;
      return;
    }

    if (ev.target.closest("[data-folder-menu]")) {
      folderMenuOpen = !folderMenuOpen;
      tierMenuOpen = false;
      renderComposerBar(appState);
      return;
    }
    if (ev.target.closest("[data-tier-menu]")) {
      tierMenuOpen = !tierMenuOpen;
      folderMenuOpen = false;
      renderComposerBar(appState);
      return;
    }
    const pickFolderBtn = ev.target.closest("[data-pick-folder]");
    if (pickFolderBtn) {
      composeWorkspace = pickFolderBtn.getAttribute("data-pick-folder");
      folderMenuOpen = false;
      renderComposerBar(appState);
      return;
    }
    if (ev.target.closest("[data-open-folder-picker]")) {
      folderMenuOpen = false;
      renderComposerBar(appState);
      if (canPickNativeFolder()) window.webkit.messageHandlers.consilium.postMessage({ action: "pickFolder" });
      else showToast("请在 Consilium App 里选择文件夹（浏览器里只能从最近用过的里选）");
      return;
    }
    const tierBtn = ev.target.closest("[data-tier]");
    if (tierBtn) {
      composeTier = tierBtn.getAttribute("data-tier");
      tierMenuOpen = false;
      renderComposerBar(appState);
      return;
    }
    if ((folderMenuOpen && !ev.target.closest(".folder-menu")) || (tierMenuOpen && !ev.target.closest(".tier-menu"))) {
      folderMenuOpen = false;
      tierMenuOpen = false;
      renderComposerBar(appState);
    }
    const renameBtn = ev.target.closest("[data-rename-mission]");
    if (renameBtn) {
      renamingMission = renameBtn.getAttribute("data-rename-mission");
      if (appState) renderTopBar(appState);
      const ti = document.getElementById("title-input");
      if (ti) { ti.focus(); ti.select(); }
      return;
    }
    const dedicatedBtn = ev.target.closest("[data-dedicated-session]");
    if (dedicatedBtn) {
      const dmid = dedicatedBtn.getAttribute("data-dedicated-session");
      const dm = ((appState && appState.missions) || []).find(x => x.mission_id === dmid) || {};
      const prompt = "接管 Consilium 会诊「" + (dm.title || dmid) + "」（mission_id=" + dmid + "）：" +
        "先用 get_session(\"self\") 拿到你自己的会话 id，再调用 consilium_bind_session(mission_id=\"" + dmid + "\", peer_id=\"claude\", session_ref=<你的会话 id>)；" +
        "然后用 consilium_status(mission_id=\"" + dmid + "\") 读一下会诊内容。之后待命会话会把这个会诊的新消息转给你，按 consilium-collab 技能第 3 节处理，用 consilium_complete_task 回复。";
      const url = buildHandoffDeeplink("claude", prompt, dm.workspace_path || "");
      if (url) { window.location.href = url; showToast("已打开 Claude 新会话，按一次发送它就会接管这个会诊"); }
      return;
    }

    if (ev.target.closest("[data-toggle-archived]")) {
      showArchived = !showArchived;
      if (appState) renderSidebar(appState);
      return;
    }

    const standbyBtn = ev.target.closest("[data-start-standby]");
    if (standbyBtn) {
      const sbPeer = standbyBtn.getAttribute("data-start-standby") || "claude";
      const sbPrompt = sbPeer === "chatgpt" ? CODEX_STANDBY_PROMPT : (sbPeer === "antigravity" ? AG_STANDBY_PROMPT : STANDBY_PROMPT);
      const sbUrl = buildHandoffDeeplink(sbPeer, sbPrompt, "");
      if (!sbUrl) {
        copyTextSafe(sbPrompt, "已复制待命指令：到 " + peerLabel(sbPeer) + " 新开一个对话，粘贴并发送");
        return;
      }
      window.location.href = sbUrl;
      showToast("已打开 " + peerLabel(sbPeer) + " 新会话，按一次发送它就会进入待命");
      return;
    }

    const delPrefBtn = ev.target.closest("[data-del-pref]");
    if (delPrefBtn) {
      const pidPref = delPrefBtn.getAttribute("data-del-pref");
      fetch("/api/preferences/" + encodeURIComponent(pidPref) + "/delete", {
        method: "POST", credentials: "same-origin", headers: writeHeaders(), body: "{}"
      }).then(r => { if (r.status === 401) showLogin(true); return fetchState(); }).catch(() => showToast("删除失败"));
      return;
    }

    const handoffPeerBtn = ev.target.closest("[data-handoff-peer]");
    if (handoffPeerBtn) {
      handoffTarget = handoffPeerBtn.getAttribute("data-handoff-peer");
      renderComposerBar(appState);
      return;
    }

    const focusHandoff = ev.target.closest("[data-focus-handoff]");
    if (focusHandoff) {
      handoffTarget = focusHandoff.getAttribute("data-focus-handoff");
      openPopoverPeer = null;
      if (appState) renderAll(appState);
      document.getElementById("composer-input").focus();
      return;
    }

    const copyPick = ev.target.closest("[data-copy-pickup]");
    if (copyPick) {
      const pid = copyPick.getAttribute("data-copy-pickup");
      const txt = "consilium_pickup_inbox(peer_id=" + JSON.stringify(pid) + ")";
      copyTextSafe(txt, "已复制 " + txt);
      return;
    }

    const foldBtn = ev.target.closest("[data-toggle-fold]");
    if (foldBtn) {
      const mid = foldBtn.getAttribute("data-toggle-fold");
      if (expandedFolds.has(mid)) expandedFolds.delete(mid);
      else expandedFolds.add(mid);
      if (appState) renderConversation(appState);
      return;
    }

    const revBtn = ev.target.closest("[data-toggle-review]");
    if (revBtn) {
      const mid = revBtn.getAttribute("data-toggle-review");
      if (expandedReviews.has(mid)) expandedReviews.delete(mid);
      else expandedReviews.add(mid);
      if (appState) renderConversation(appState);
      return;
    }

    if (ev.target.closest("[data-export-md]")) {
      exportMissionMarkdown();
      return;
    }

    const closeBtn = ev.target.closest("[data-close-mission]");
    if (closeBtn) {
      const mid = closeBtn.getAttribute("data-close-mission");
      openConfirm(
        "关闭会诊 " + mid,
        "此操作将把该会诊标记为终态。若仍有未完成任务，可勾选强制取消剩余任务。",
        '<textarea id="close-summary-input" class="textarea" rows="3">通过 Consilium 会诊界面人工确认关闭。</textarea>' +
        '<select id="close-status-select" class="select">' +
          '<option value="completed">completed（正常完成）</option>' +
          '<option value="cancelled">cancelled（取消）</option>' +
          '<option value="failed">failed（失败）</option>' +
        '</select>' +
        '<label style="display:flex;align-items:center;gap:6px;font-size:12.5px;margin-bottom:8px;">' +
          '<input type="checkbox" id="close-force-check"> 强制取消未完成任务（force=true）' +
        '</label>',
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
            closeConfirm(); stopStreams(); csrfToken = ""; lastStateVersion = 0; setConn("", "待登录"); showLogin(true); return;
          }
          const body = await r.json();
          if (!r.ok) throw new Error(body.error || ("HTTP " + r.status));
          await fetchState();
        }
      );
      return;
    }

    const notifyButton = ev.target.closest("[data-notify-task]");
    if (notifyButton) {
      notifyButton.disabled = true;
      const id = notifyButton.getAttribute("data-notify-task");
      fetch("/api/tasks/" + encodeURIComponent(id) + "/notify", {
        method:"POST", credentials:"same-origin", headers:writeHeaders(), body:"{}"
      }).then(async response => {
        const data = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(data.error || "通知失败");
        const delivery = data.delivery || {};
        const message = delivery.delivery === "hot_wakeup" ? "已唤醒监听，等领取任务" :
          delivery.notified ? "系统通知已发，等领取任务" : (delivery.error || "通知未送达，可到目标应用取件");
        showToast(message);
        await fetchState();
      }).catch(error => showToast(error.message)).finally(() => { notifyButton.disabled = false; });
      return;
    }

    const settleButton = ev.target.closest("[data-settle-task]");
    if (settleButton) {
      const mission = getActiveMission(appState);
      const task = (mission.tasks || []).find(t => t.task_id === settleButton.getAttribute("data-settle-task"));
      const completed = (mission.tasks || []).filter(t => t.status === "done");
      const options = completed.map(t => '<option value="' + esc(t.task_id) + '">' +
        esc(peerLabel(t.assignee_peer) + " · " + taskTitle(t) + "（" + t.task_id + "）") + '</option>').join("");
      openConfirm("收尾重复任务", "选择已经完成同一事项的交付。当前任务的原记录会保留。",
        '<label for="settle-result">关联交付</label><select id="settle-result"><option value="">请选择</option>' + options + '</select>',
        async () => {
          const chosen = document.getElementById("settle-result").value;
          if (!chosen) throw new Error("请选择已完成的交付");
          const response = await fetch("/api/tasks/" + encodeURIComponent(task.task_id) + "/supersede", {
            method:"POST", credentials:"same-origin", headers:writeHeaders(),
            body:JSON.stringify({attempt_id:task.attempt_id, delivered_task_id:chosen})
          });
          const result = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(result.error || "收尾失败");
          await fetchState();
        });
      return;
    }

    const reqBtn = ev.target.closest("[data-requeue-task]");
    if (reqBtn) {
      const tid = reqBtn.getAttribute("data-requeue-task");
      openConfirm(
        "重新入队任务 " + tid,
        "此操作将把任务状态重置为 queued，并向对应端发送 task_requeued 通知。",
        '<input id="requeue-reason-input" class="input" value="通过 Consilium 界面人工重新入队">',
        async () => {
          const reason = (document.getElementById("requeue-reason-input").value || "").trim() || "manual_requeue";
          const r = await fetch("/api/tasks/" + encodeURIComponent(tid) + "/requeue", {
            method: "POST",
            credentials: "same-origin",
            headers: writeHeaders(),
            body: JSON.stringify({ reason })
          });
          if (r.status === 401) {
            closeConfirm(); stopStreams(); csrfToken = ""; lastStateVersion = 0; setConn("", "待登录"); showLogin(true); return;
          }
          const body = await r.json();
          if (!r.ok) throw new Error(body.error || ("HTTP " + r.status));
          await fetchState();
        }
      );
      return;
    }

    if (openPopoverPeer && !ev.target.closest("#peer-popover")) {
      openPopoverPeer = null;
      if (appState) renderTopBar(appState);
    }
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

  (function () {
    const moreBtn = document.getElementById("more-btn");
    const moreMenu = document.getElementById("topbar-more");
    function setMore(open) {
      moreMenu.classList.toggle("open", open);
      moreBtn.setAttribute("aria-expanded", open ? "true" : "false");
    }
    moreBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      setMore(!moreMenu.classList.contains("open"));
    });
    moreMenu.addEventListener("click", (e) => {
      if (e.target.closest("button") && !e.target.closest(".seg")) setMore(false);
    });
    document.addEventListener("click", (e) => {
      if (!e.target.closest("#topbar-more") && !e.target.closest("#more-btn")) setMore(false);
    });
    document.addEventListener("keydown", (e) => { if (e.key === "Escape") setMore(false); });
  })();
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

  setInterval(() => {
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


_PEER_ICON_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "peer_icons")
_PEER_ICON_CSS_CACHE: Optional[str] = None


def _peer_icon_css() -> str:
    """CSS that swaps the letter avatars for each desktop app's real icon (inlined as data URIs).

    Icons live in consilium/assets/peer_icons/<peer_id>.png. A missing file just keeps the letter avatar.
    """
    global _PEER_ICON_CSS_CACHE
    if _PEER_ICON_CSS_CACHE is not None:
        return _PEER_ICON_CSS_CACHE
    rules: List[str] = []
    for peer_id in ("claude", "chatgpt", "antigravity"):
        path = os.path.join(_PEER_ICON_DIR, peer_id + ".png")
        try:
            with open(path, "rb") as fh:
                data = base64.b64encode(fh.read()).decode("ascii")
        except OSError:
            continue
        url = "url(data:image/png;base64," + data + ")"
        rules.append(
            ".avatar." + peer_id + " { background-color: transparent; background-image: " + url
            + "; background-size: cover; background-position: center; border-radius: 7px; color: transparent; font-size: 0; }"
        )
        rules.append(".peer-icon." + peer_id + " { display: inline-block; background-image: " + url + "; }")
    _PEER_ICON_CSS_CACHE = "\n".join(rules)
    return _PEER_ICON_CSS_CACHE


def render_dashboard_html(version: Optional[str] = None) -> str:
    """Return the self-contained Consilium dashboard HTML with daemon version stamped."""
    return DASHBOARD_HTML.replace("__VERSION__", version or __version__).replace("__PEER_ICON_CSS__", _peer_icon_css())
