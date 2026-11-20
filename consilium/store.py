"""SQLite WAL truth-layer store for consilium missions, tasks, peers, and events."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
import sqlite3
import time
from typing import Any, Dict, Iterator, List, Optional
import uuid

from consilium.models import (
    ConflictError,
    DEFAULT_CLAIM_TIMEOUT_SEC,
    DEFAULT_RUNNING_TIMEOUT_SEC,
    MAX_ACTIVE_PREFERENCES,
    NON_TERMINAL_TASK_STATUSES,
    PROGRESS_TASK_STATUSES,
    REQUEUEABLE_TASK_STATUSES,
    TERMINAL_TASK_STATUSES,
    USER_PEER,
    VALID_ISOLATION,
    VALID_MISSION_CLOSE_STATUSES,
    VALID_MODEL_TIERS,
    VALID_PEERS,
    resolve_rendezvous_dir,
    resolve_state_dir,
    utc_now_iso,
    validate_choice,
    validate_peer,
)
from consilium.wakeup import query_active_waiters, write_peer_seq

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS peers (
    peer_id TEXT PRIMARY KEY,
    session_label TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'standby',
    supported_models_json TEXT NOT NULL DEFAULT '[]',
    waiter_active INTEGER NOT NULL DEFAULT 0,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS missions (
    mission_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    goal TEXT NOT NULL,
    coordinator_peer TEXT NOT NULL,
    workspace_path TEXT NOT NULL DEFAULT '',
    isolation_policy TEXT NOT NULL DEFAULT 'worktree',
    max_rounds INTEGER NOT NULL DEFAULT 3,
    current_round INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    final_summary TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    mission_id TEXT NOT NULL,
    assignee_peer TEXT NOT NULL,
    model_tier TEXT NOT NULL DEFAULT 'balanced',
    model_hint TEXT NOT NULL DEFAULT '',
    parallel_fanout INTEGER NOT NULL DEFAULT 1,
    worktree_branch TEXT NOT NULL DEFAULT '',
    prompt TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    attempt_id TEXT NOT NULL DEFAULT '',
    attempt_seq INTEGER NOT NULL DEFAULT 0,
    claimed_by_session TEXT NOT NULL DEFAULT '',
    claimed_at TEXT NOT NULL DEFAULT '',
    stale_at TEXT NOT NULL DEFAULT '',
    dispatched_at_ms REAL NOT NULL DEFAULT 0.0,
    claimed_at_ms REAL NOT NULL DEFAULT 0.0,
    claim_latency_ms REAL,
    progress_note TEXT NOT NULL DEFAULT '',
    result_summary TEXT NOT NULL DEFAULT '',
    artifacts_json TEXT NOT NULL DEFAULT '[]',
    attempt_history_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (mission_id) REFERENCES missions(mission_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mission_id TEXT NOT NULL,
    task_id TEXT NOT NULL DEFAULT '',
    from_peer TEXT NOT NULL,
    to_peer TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    acked_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS quota_windows (
    peer_id TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT 'default',
    window_key TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    remaining_percent REAL NOT NULL,
    reset_at TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'manual',
    observed_at TEXT NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 0,
    is_historical INTEGER NOT NULL DEFAULT 0,
    account_alias TEXT NOT NULL DEFAULT '',
    plan_type TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (peer_id, account_id, window_key)
);

CREATE TABLE IF NOT EXISTS quota_accounts (
    peer_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    account_alias TEXT NOT NULL DEFAULT '',
    plan_type TEXT NOT NULL DEFAULT '',
    is_current INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (peer_id, account_id)
);

CREATE TABLE IF NOT EXISTS user_messages (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mission_id TEXT NOT NULL,
    text TEXT NOT NULL,
    targets_json TEXT NOT NULL DEFAULT '[]',
    mode TEXT NOT NULL DEFAULT 'direct',
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    created_ms REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS preferences (
    pref_id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    client_request_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    caller_scope TEXT NOT NULL DEFAULT '',
    request_hash TEXT NOT NULL DEFAULT '',
    response_json TEXT NOT NULL,
    delivery_json TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_assignee_status
    ON tasks (assignee_peer, status);
CREATE INDEX IF NOT EXISTS idx_tasks_mission
    ON tasks (mission_id);
CREATE INDEX IF NOT EXISTS idx_events_to_acked
    ON events (to_peer, acked_at, event_id);
CREATE INDEX IF NOT EXISTS idx_quota_peer
    ON quota_windows (peer_id, remaining_percent);
"""


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _parse_iso(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _request_hash(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _format_countdown(seconds: Optional[int]) -> str:
    """Format remaining seconds until quota reset as a compact human string."""
    if seconds is None:
        return ""
    if seconds <= 0:
        return "resetting now"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


class BusStore:
    """Transactional SQLite WAL store for consilium coordination state."""

    def __init__(
        self,
        home_dir: Optional[str] = None,
        rendezvous_dir: Optional[str] = None,
    ) -> None:
        self.home_dir = resolve_state_dir(home_dir)
        self.rendezvous_dir = resolve_rendezvous_dir(rendezvous_dir, fallback_dir=home_dir)
        self.db_path = os.path.join(self.home_dir, "bus.db")
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self.db_path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _tx(self, conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        conn.execute("BEGIN IMMEDIATE;")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK;")
            raise
        else:
            conn.execute("COMMIT;")

    def _init_schema(self) -> None:
        with self._connect() as conn:
            # executescript manages its own implicit transaction in Python sqlite3.
            conn.executescript(SCHEMA_SQL)
            migrations = (
                "ALTER TABLE tasks ADD COLUMN claimed_at TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE tasks ADD COLUMN attempt_id TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE tasks ADD COLUMN attempt_seq INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE tasks ADD COLUMN stale_at TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE tasks ADD COLUMN dispatched_at_ms REAL NOT NULL DEFAULT 0.0",
                "ALTER TABLE tasks ADD COLUMN claimed_at_ms REAL NOT NULL DEFAULT 0.0",
                "ALTER TABLE tasks ADD COLUMN claim_latency_ms REAL",
                "ALTER TABLE tasks ADD COLUMN attempt_history_json TEXT NOT NULL DEFAULT '[]'",
                "ALTER TABLE idempotency_keys ADD COLUMN caller_scope TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE idempotency_keys ADD COLUMN request_hash TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE idempotency_keys ADD COLUMN delivery_json TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE tasks ADD COLUMN origin TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE tasks ADD COLUMN model_used TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE user_messages ADD COLUMN reason TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE tasks ADD COLUMN new_session INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE missions ADD COLUMN native_sessions_json TEXT NOT NULL DEFAULT '{}'",
                "ALTER TABLE missions ADD COLUMN title_locked INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE missions ADD COLUMN archived INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE missions ADD COLUMN tier TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE user_messages ADD COLUMN created_ms REAL NOT NULL DEFAULT 0",
                "ALTER TABLE quota_windows ADD COLUMN is_current INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE quota_windows ADD COLUMN is_historical INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE quota_windows ADD COLUMN account_alias TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quota_windows ADD COLUMN plan_type TEXT NOT NULL DEFAULT ''",
            )
            for stmt in migrations:
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass
            with self._tx(conn):
                # Legacy DB backfill (R012 P2): give pre-attempt_id active tasks a deterministic fencing token
                conn.execute(
                    """
                    UPDATE tasks
                    SET attempt_seq = CASE WHEN attempt_seq <= 0 THEN 1 ELSE attempt_seq END,
                        attempt_id = 'A-1-legacy-' || lower(replace(task_id, '-', '')),
                        claimed_at = CASE WHEN claimed_at = '' THEN updated_at ELSE claimed_at END
                    WHERE status IN ('claimed', 'running') AND attempt_id = ''
                    """
                )
                # Scrub any pre-Q4 raw email account_ids inside quota tables
                raw_win_accts = conn.execute(
                    "SELECT DISTINCT account_id FROM quota_windows WHERE account_id LIKE '%@%'"
                ).fetchall()
                for row in raw_win_accts:
                    raw_aid = str(row["account_id"] or "")
                    hashed = f"acct-{hashlib.sha256(raw_aid.strip().lower().encode('utf-8')).hexdigest()[:10]}"
                    try:
                        conn.execute(
                            "UPDATE quota_windows SET account_id = ? WHERE account_id = ?",
                            (hashed, raw_aid),
                        )
                    except sqlite3.IntegrityError:
                        conn.execute(
                            "DELETE FROM quota_windows WHERE account_id = ?",
                            (raw_aid,),
                        )
                raw_reg_accts = conn.execute(
                    "SELECT DISTINCT account_id FROM quota_accounts WHERE account_id LIKE '%@%'"
                ).fetchall()
                for row in raw_reg_accts:
                    raw_aid = str(row["account_id"] or "")
                    hashed = f"acct-{hashlib.sha256(raw_aid.strip().lower().encode('utf-8')).hexdigest()[:10]}"
                    try:
                        conn.execute(
                            "UPDATE quota_accounts SET account_id = ? WHERE account_id = ?",
                            (hashed, raw_aid),
                        )
                    except sqlite3.IntegrityError:
                        conn.execute(
                            "DELETE FROM quota_accounts WHERE account_id = ?",
                            (raw_aid,),
                        )
        try:
            os.chmod(self.db_path, 0o600)
        except OSError:
            pass

    def query_peer_seq_counts(self, peer_id: str) -> tuple:
        """Read authoritative (max_event_id, unacked_events, queued_tasks) from one SQLite snapshot."""
        peer = validate_peer(peer_id)
        with self._connect() as conn:
            conn.execute("BEGIN;")
            try:
                evt_row = conn.execute(
                    """
                    SELECT
                        COALESCE(MAX(event_id), 0) AS max_id,
                        SUM(CASE WHEN acked_at = '' THEN 1 ELSE 0 END) AS unacked_cnt
                    FROM events
                    WHERE to_peer = ?
                    """,
                    (peer,),
                ).fetchone()
                task_row = conn.execute(
                    """
                    SELECT COUNT(*) AS queued_cnt
                    FROM tasks
                    WHERE assignee_peer = ? AND status = 'queued'
                    """,
                    (peer,),
                ).fetchone()
                conn.execute("COMMIT;")
            except BaseException:
                conn.execute("ROLLBACK;")
                raise
        max_id = int(evt_row["max_id"] or 0) if evt_row else 0
        unacked = int(evt_row["unacked_cnt"] or 0) if evt_row else 0
        queued = int(task_row["queued_cnt"] or 0) if task_row else 0
        return max_id, unacked, queued

    def sync_peer_seq(
        self, peer_id: str, *, pulse: bool = False, pulse_if_backlog: bool = False
    ) -> Dict[str, Any]:
        """Re-query SQLite inside .<peer>.seq.lock and atomically write signals/<peer>.seq."""
        peer = validate_peer(peer_id)
        return write_peer_seq(
            self.rendezvous_dir,
            peer,
            query_fn=lambda: self.query_peer_seq_counts(peer),
            pulse=pulse,
            pulse_if_backlog=pulse_if_backlog,
        )

    def _safe_sync_peers_seq(
        self,
        peers: Any,
        *,
        pulse_peers: Any = None,
    ) -> List[str]:
        """Best-effort post-commit .seq synchronization; returns warnings instead of raising."""
        warnings: List[str] = []
        pulse_set = set(pulse_peers or ())
        for peer in sorted(set(p for p in (peers or ()) if p in VALID_PEERS)):
            try:
                self.sync_peer_seq(peer, pulse=(peer in pulse_set))
            except Exception as exc:
                warnings.append(f"seq_sync({peer}): {type(exc).__name__}: {exc}")
        return warnings

    def _sync_peer_seq(self, conn: sqlite3.Connection, peer_id: str) -> None:
        del conn
        self.sync_peer_seq(peer_id, pulse=False)

    def _sync_peers_seq(self, conn: sqlite3.Connection, peers: Any) -> None:
        del conn
        for peer in sorted(set(peers)):
            if peer in VALID_PEERS:
                self.sync_peer_seq(peer, pulse=False)

    def _touch_peer_locked(
        self,
        conn: sqlite3.Connection,
        peer_id: str,
        *,
        session_label: str = "",
        status: str = "active",
        now: Optional[str] = None,
    ) -> None:
        if peer_id == USER_PEER:
            return
        peer = validate_peer(peer_id)
        ts = now or utc_now_iso()
        conn.execute(
            """
            INSERT INTO peers (
                peer_id, session_label, status, supported_models_json,
                waiter_active, last_seen_at
            ) VALUES (?, ?, ?, '[]', 0, ?)
            ON CONFLICT(peer_id) DO UPDATE SET
                session_label = CASE
                    WHEN excluded.session_label != '' THEN excluded.session_label
                    ELSE peers.session_label
                END,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at
            """,
            (peer, session_label, status, ts),
        )

    def _check_idempotency_locked(
        self,
        conn: sqlite3.Connection,
        client_request_id: Optional[str],
        operation: str,
        *,
        caller_scope: str = "",
        request_hash: str = "",
    ) -> Optional[Dict[str, Any]]:
        req_id = (client_request_id or "").strip()
        if not req_id:
            return None
        row = conn.execute(
            """
            SELECT operation, caller_scope, request_hash, response_json, delivery_json
            FROM idempotency_keys
            WHERE client_request_id = ?
            """,
            (req_id,),
        ).fetchone()
        if row is None:
            return None
        if row["operation"] != operation:
            raise ConflictError(
                f"client_request_id {req_id!r} already used for operation {row['operation']!r}, not {operation!r}"
            )
        stored_scope = row["caller_scope"] or ""
        if stored_scope != (caller_scope or ""):
            raise ConflictError(
                f"client_request_id {req_id!r} belongs to caller scope {stored_scope!r}, not {caller_scope!r}"
            )
        stored_hash = row["request_hash"] or ""
        if stored_hash and request_hash and stored_hash != request_hash:
            raise ConflictError(
                f"client_request_id {req_id!r} was already committed with different parameters"
            )
        cached = json.loads(row["response_json"])
        if isinstance(cached, dict):
            cached["idempotent_replay"] = True
            raw_delivery = row["delivery_json"] if "delivery_json" in row.keys() else ""
            if raw_delivery:
                try:
                    cached["_cached_delivery"] = json.loads(raw_delivery)
                except ValueError:
                    pass
        return cached

    def save_idempotency_delivery(
        self,
        client_request_id: Optional[str],
        delivery: Dict[str, Any],
    ) -> None:
        """Persist post-commit delivery receipt (including warnings) for faithful idempotent replay."""
        req_id = (client_request_id or "").strip()
        if not req_id:
            return
        encoded = json.dumps(delivery, ensure_ascii=False)
        with self._connect() as conn:
            with self._tx(conn):
                conn.execute(
                    "UPDATE idempotency_keys SET delivery_json = ? WHERE client_request_id = ?",
                    (encoded, req_id),
                )

    def _save_idempotency_locked(
        self,
        conn: sqlite3.Connection,
        client_request_id: Optional[str],
        operation: str,
        payload: Dict[str, Any],
        now: str,
        *,
        caller_scope: str = "",
        request_hash: str = "",
    ) -> None:
        req_id = (client_request_id or "").strip()
        if not req_id:
            return
        conn.execute(
            """
            INSERT INTO idempotency_keys (
                client_request_id, operation, caller_scope, request_hash, response_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                req_id,
                operation,
                caller_scope or "",
                request_hash or "",
                json.dumps(payload, ensure_ascii=False),
                now,
            ),
        )

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        *,
        mission_id: str,
        task_id: str,
        from_peer: str,
        to_peer: str,
        kind: str,
        payload: Dict[str, Any],
        created_at: str,
    ) -> Dict[str, Any]:
        enriched_payload = dict(payload)
        enriched_payload.setdefault("ring_sent_at_ms", round(time.time() * 1000.0, 2))
        acked_at = created_at if to_peer == USER_PEER else ""
        if acked_at:
            enriched_payload["peer_acked"] = True
        payload_str = json.dumps(enriched_payload, ensure_ascii=False)
        cur = conn.execute(
            """
            INSERT INTO events (
                mission_id, task_id, from_peer, to_peer, kind, payload_json, created_at, acked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (mission_id, task_id, from_peer, to_peer, kind, payload_str, created_at, acked_at),
        )
        return {
            "event_id": cur.lastrowid,
            "mission_id": mission_id,
            "task_id": task_id,
            "from_peer": from_peer,
            "to_peer": to_peer,
            "kind": kind,
            "payload": enriched_payload,
            "created_at": created_at,
            "acked_at": acked_at,
        }

    def _decode_task(
        self, row: sqlite3.Row, now_dt: Optional[datetime] = None
    ) -> Dict[str, Any]:
        data = _row_to_dict(row)
        raw_artifacts = data.pop("artifacts_json", "[]")
        try:
            data["artifacts"] = json.loads(raw_artifacts)
        except ValueError:
            data["artifacts"] = []
        raw_history = data.pop("attempt_history_json", "[]")
        try:
            data["attempt_history"] = json.loads(raw_history)
            if not isinstance(data["attempt_history"], list):
                data["attempt_history"] = []
        except ValueError:
            data["attempt_history"] = []
        claim_age: Optional[int] = None
        claim_stale = False
        if data.get("status") in ("claimed", "running") and data.get("claimed_at"):
            cdt = _parse_iso(data["claimed_at"])
            if cdt is not None:
                ref = now_dt or datetime.now(timezone.utc)
                claim_age = max(0, int((ref - cdt).total_seconds()))
                claim_stale = claim_age > DEFAULT_CLAIM_TIMEOUT_SEC
        data["claim_age_seconds"] = claim_age
        data["claim_stale"] = claim_stale
        data["running_stale"] = bool(data.get("stale_at"))
        return data

    def _decode_event(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = _row_to_dict(row)
        raw_payload = data.pop("payload_json", "{}")
        try:
            data["payload"] = json.loads(raw_payload)
        except ValueError:
            data["payload"] = {}
        return data

    def upsert_peer(
        self,
        peer_id: str,
        session_label: str = "",
        status: str = "standby",
        supported_models: Optional[List[str]] = None,
        waiter_active: bool = False,
    ) -> Dict[str, Any]:
        """Register or refresh a peer heartbeat and waiter status."""
        peer = validate_peer(peer_id)
        now = utc_now_iso()
        models_json = json.dumps(supported_models or [], ensure_ascii=False)
        with self._connect() as conn:
            with self._tx(conn):
                conn.execute(
                    """
                    INSERT INTO peers (
                        peer_id, session_label, status, supported_models_json,
                        waiter_active, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(peer_id) DO UPDATE SET
                        session_label = CASE
                            WHEN excluded.session_label != '' THEN excluded.session_label
                            ELSE peers.session_label
                        END,
                        status = excluded.status,
                        supported_models_json = CASE
                            WHEN excluded.supported_models_json != '[]'
                            THEN excluded.supported_models_json
                            ELSE peers.supported_models_json
                        END,
                        waiter_active = excluded.waiter_active,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (peer, session_label, status, models_json, int(waiter_active), now),
                )
                row = conn.execute(
                    "SELECT * FROM peers WHERE peer_id = ?", (peer,)
                ).fetchone()
        return self._decode_peer(row, stale_seconds=600)

    def _decode_peer(self, row: sqlite3.Row, stale_seconds: int) -> Dict[str, Any]:
        data = _row_to_dict(row)
        raw_models = data.pop("supported_models_json", "[]")
        try:
            data["supported_models"] = json.loads(raw_models)
        except ValueError:
            data["supported_models"] = []
        data["waiter_active"] = bool(data.get("waiter_active", 0))
        last_dt = _parse_iso(data.get("last_seen_at", ""))
        age_sec = None
        if last_dt is not None:
            age_sec = int((datetime.now(timezone.utc) - last_dt).total_seconds())
        data["age_seconds"] = age_sec
        data["stale"] = age_sec is None or age_sec > stale_seconds
        return data

    def list_peers(self, stale_seconds: int = 600) -> List[Dict[str, Any]]:
        """List all known peers overlaid with live flock waiter leases, active tasks, and 5-state lights."""
        now_dt = datetime.now(timezone.utc)
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM peers ORDER BY peer_id").fetchall()
            task_rows = conn.execute(
                """
                SELECT assignee_peer, status, stale_at, claimed_at
                FROM tasks
                WHERE status IN ('queued', 'claimed', 'running')
                """
            ).fetchall()
        by_id = {
            row["peer_id"]: self._decode_peer(row, stale_seconds=stale_seconds)
            for row in rows
        }
        task_counts_by_peer: Dict[str, Dict[str, int]] = {
            p: {"queued": 0, "claimed": 0, "running": 0} for p in VALID_PEERS
        }
        stale_task_by_peer: Dict[str, bool] = {p: False for p in VALID_PEERS}
        for tr in task_rows:
            p = tr["assignee_peer"]
            st = tr["status"]
            if p in task_counts_by_peer:
                task_counts_by_peer[p][st] = task_counts_by_peer[p].get(st, 0) + 1
                if tr["stale_at"]:
                    stale_task_by_peer[p] = True
                elif st == "claimed" and tr["claimed_at"]:
                    cdt = _parse_iso(tr["claimed_at"])
                    if cdt is not None and (now_dt - cdt).total_seconds() > DEFAULT_CLAIM_TIMEOUT_SEC:
                        stale_task_by_peer[p] = True

        live_waiters = query_active_waiters(self.rendezvous_dir)
        result: List[Dict[str, Any]] = []
        for peer in VALID_PEERS:
            entry = dict(
                by_id.get(peer)
                or {
                    "peer_id": peer,
                    "session_label": "",
                    "status": "offline",
                    "supported_models": [],
                    "waiter_active": False,
                    "last_seen_at": "",
                    "age_seconds": None,
                    "stale": True,
                }
            )
            counts = task_counts_by_peer[peer]
            entry["task_counts"] = counts
            has_stale_task = stale_task_by_peer[peer]
            if live_waiters.get(peer, 0) > 0:
                entry["waiter_active"] = True
                entry["status"] = "standby"
                entry["stale"] = False
            elif counts["claimed"] + counts["running"] > 0:
                entry["waiter_active"] = False
                entry["status"] = "stale" if (has_stale_task or entry.get("stale")) else "working"
            elif entry.get("last_seen_at"):
                entry["waiter_active"] = False
                entry["status"] = "offline" if entry.get("stale") else "online"
            else:
                entry["waiter_active"] = False
                entry["status"] = "offline"
            result.append(entry)
        return result

    def create_mission(
        self,
        title: str,
        goal: str,
        coordinator_peer: str,
        workspace_path: str = "",
        isolation_policy: str = "worktree",
        max_rounds: int = 3,
        tasks: Optional[List[Dict[str, Any]]] = None,
        client_request_id: Optional[str] = None,
        allow_user_coordinator: bool = False,
        user_message: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a mission and optionally dispatch initial tasks atomically.

        `allow_user_coordinator` is only set by the dashboard: the user owns the
        conversation and agents' results come back to the UI instead of a peer.
        `user_message` ({text, targets, mode}) records what the user typed.
        """
        if allow_user_coordinator and (coordinator_peer or "").strip().lower() == USER_PEER:
            coordinator = USER_PEER
        else:
            coordinator = validate_peer(coordinator_peer)
        isolation = validate_choice(isolation_policy, VALID_ISOLATION, "isolation_policy")
        if max_rounds < 1:
            raise ValueError("max_rounds must be >= 1")

        caller_scope = f"peer:{coordinator}"
        req_hash = _request_hash(
            {
                "title": title,
                "goal": goal,
                "coordinator": coordinator,
                "workspace_path": workspace_path,
                "isolation": isolation,
                "max_rounds": int(max_rounds),
                "tasks": tasks or [],
            }
        )
        mission_id = f"M-{uuid.uuid4().hex[:8].upper()}"
        now = utc_now_iso()
        now_ms = round(time.time() * 1000.0, 2)
        created_tasks: List[Dict[str, Any]] = []
        created_events: List[Dict[str, Any]] = []
        touched_peers: List[str] = [coordinator]

        with self._connect() as conn:
            with self._tx(conn):
                cached = self._check_idempotency_locked(
                    conn,
                    client_request_id,
                    "create_mission",
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
                if cached is not None:
                    return cached

                self._touch_peer_locked(conn, coordinator, status="active", now=now)
                conn.execute(
                    """
                    INSERT INTO missions (
                        mission_id, title, goal, coordinator_peer, workspace_path,
                        isolation_policy, max_rounds, current_round, status,
                        final_summary, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'active', '', ?, ?)
                    """,
                    (
                        mission_id,
                        title,
                        goal,
                        coordinator,
                        workspace_path,
                        isolation,
                        int(max_rounds),
                        now,
                        now,
                    ),
                )

                for item in tasks or []:
                    task_obj, event_obj = self._insert_task_locked(
                        conn,
                        mission_id=mission_id,
                        from_peer=coordinator,
                        assignee_peer=item["assignee_peer"],
                        prompt=item["prompt"],
                        model_tier=item.get("model_tier", "balanced"),
                        model_hint=item.get("model_hint", ""),
                        parallel_fanout=int(item.get("parallel_fanout", 1)),
                        worktree_branch=item.get("worktree_branch", ""),
                        now=now,
                        now_ms=now_ms,
                        origin=str(item.get("origin", "")),
                        new_session=bool(item.get("new_session")),
                    )
                    created_tasks.append(task_obj)
                    created_events.append(event_obj)
                    touched_peers.append(task_obj["assignee_peer"])

                if user_message:
                    self._insert_user_message_locked(conn, mission_id, user_message, now)
                mission_row = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                out = {
                    "mission": _row_to_dict(mission_row),
                    "tasks": created_tasks,
                    "events": created_events,
                }
                self._save_idempotency_locked(
                    conn,
                    client_request_id,
                    "create_mission",
                    out,
                    now,
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
            warnings = self._safe_sync_peers_seq(touched_peers)
            out["post_commit_warnings"] = warnings

        return out

    def _insert_task_locked(
        self,
        conn: sqlite3.Connection,
        *,
        mission_id: str,
        from_peer: str,
        assignee_peer: str,
        prompt: str,
        model_tier: str,
        model_hint: str,
        parallel_fanout: int,
        worktree_branch: str,
        now: str,
        now_ms: Optional[float] = None,
        origin: str = "",
        new_session: bool = False,
    ) -> tuple:
        assignee = validate_peer(assignee_peer)
        tier = validate_choice(model_tier, VALID_MODEL_TIERS, "model_tier")
        fanout = max(1, int(parallel_fanout))
        task_id = f"T-{uuid.uuid4().hex[:8].upper()}"
        dispatched_ms = round(time.time() * 1000.0, 2) if now_ms is None else float(now_ms)

        conn.execute(
            """
            INSERT INTO tasks (
                task_id, mission_id, assignee_peer, model_tier, model_hint,
                parallel_fanout, worktree_branch, prompt, status,
                dispatched_at_ms, created_at, updated_at, origin, new_session
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                mission_id,
                assignee,
                tier,
                model_hint,
                fanout,
                worktree_branch,
                prompt,
                dispatched_ms,
                now,
                now,
                origin,
                1 if new_session else 0,
            ),
        )
        task_row = conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        event_obj = self._insert_event(
            conn,
            mission_id=mission_id,
            task_id=task_id,
            from_peer=from_peer,
            to_peer=assignee,
            kind="task_assigned",
            payload={
                "task_id": task_id,
                "model_tier": tier,
                "model_hint": model_hint,
                "parallel_fanout": fanout,
                "worktree_branch": worktree_branch,
                "prompt_preview": prompt[:200],
                "dispatched_at_ms": dispatched_ms,
                "origin": origin,
            },
            created_at=now,
        )
        return self._decode_task(task_row), event_obj

    def dispatch_task(
        self,
        mission_id: str,
        assignee_peer: str,
        prompt: str,
        model_tier: str = "balanced",
        model_hint: str = "",
        parallel_fanout: int = 1,
        worktree_branch: str = "",
        client_request_id: Optional[str] = None,
        caller_scope: Optional[str] = None,
        origin: str = "",
        user_message: Optional[Dict[str, Any]] = None,
        new_session: bool = False,
    ) -> Dict[str, Any]:
        """Dispatch an additional task on an existing open mission."""
        assignee = validate_peer(assignee_peer)
        tier = validate_choice(model_tier, VALID_MODEL_TIERS, "model_tier")
        now = utc_now_iso()
        now_ms = round(time.time() * 1000.0, 2)
        scope = caller_scope if caller_scope is not None else "coordinator"
        req_hash = _request_hash(
            {
                "mission_id": mission_id,
                "assignee_peer": assignee,
                "prompt": prompt,
                "model_tier": tier,
                "model_hint": model_hint,
                "parallel_fanout": int(parallel_fanout),
                "worktree_branch": worktree_branch,
            }
        )
        with self._connect() as conn:
            with self._tx(conn):
                cached = self._check_idempotency_locked(
                    conn,
                    client_request_id,
                    "dispatch_task",
                    caller_scope=scope,
                    request_hash=req_hash,
                )
                if cached is not None:
                    return cached

                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                if mission["status"] in VALID_MISSION_CLOSE_STATUSES:
                    raise ValueError(
                        f"Cannot dispatch task on closed mission {mission_id} (status={mission['status']})"
                    )
                self._touch_peer_locked(
                    conn, mission["coordinator_peer"], status="active", now=now
                )
                task_obj, event_obj = self._insert_task_locked(
                    conn,
                    mission_id=mission_id,
                    from_peer=mission["coordinator_peer"],
                    assignee_peer=assignee_peer,
                    prompt=prompt,
                    model_tier=model_tier,
                    model_hint=model_hint,
                    parallel_fanout=parallel_fanout,
                    worktree_branch=worktree_branch,
                    now=now,
                    now_ms=now_ms,
                    origin=origin,
                    new_session=new_session,
                )
                if user_message:
                    self._insert_user_message_locked(conn, mission_id, user_message, now)
                next_status = (
                    "active" if mission["status"] == "ready_to_report" else mission["status"]
                )
                conn.execute(
                    "UPDATE missions SET status = ?, updated_at = ? WHERE mission_id = ?",
                    (next_status, now, mission_id),
                )
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                out = {"task": task_obj, "event": event_obj, "mission": _row_to_dict(mission)}
                self._save_idempotency_locked(
                    conn,
                    client_request_id,
                    "dispatch_task",
                    out,
                    now,
                    caller_scope=scope,
                    request_hash=req_hash,
                )
            warnings = self._safe_sync_peers_seq(
                [task_obj["assignee_peer"], mission["coordinator_peer"]]
            )
            out["post_commit_warnings"] = warnings
        return out

    def count_unacked_events(self, peer_id: str) -> int:
        """Count unacknowledged inbox events for a peer."""
        peer = validate_peer(peer_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM events WHERE to_peer = ? AND acked_at = ''",
                (peer,),
            ).fetchone()
        return int(row["cnt"]) if row else 0

    def count_queued_tasks(self, peer_id: str) -> int:
        """Count queued (unclaimed) tasks assigned to a peer."""
        peer = validate_peer(peer_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM tasks WHERE assignee_peer = ? AND status = 'queued'",
                (peer,),
            ).fetchone()
        return int(row["cnt"]) if row else 0

    def record_event_delivery(
        self,
        event_ids: List[int],
        delivery: Dict[str, Any],
        *,
        ring_sent_at_ms: Optional[float] = None,
        wakeup_latency_ms: Optional[float] = None,
    ) -> None:
        """Attach delivery metadata (attempt, OS command result, hot/cold/error) onto event payloads."""
        if not event_ids:
            return
        with self._connect() as conn:
            with self._tx(conn):
                for eid in event_ids:
                    row = conn.execute(
                        "SELECT payload_json, acked_at FROM events WHERE event_id = ?",
                        (int(eid),),
                    ).fetchone()
                    if row is None:
                        continue
                    try:
                        payload = json.loads(row["payload_json"])
                        if not isinstance(payload, dict):
                            payload = {}
                    except ValueError:
                        payload = {}
                    mode = str(delivery.get("delivery", ""))
                    payload["delivery_mode"] = mode
                    if "attempted" in delivery:
                        payload["delivery_attempted"] = bool(delivery["attempted"])
                    elif mode in ("hot_wakeup", "cold_notification", "error"):
                        payload["delivery_attempted"] = True
                    if "command_status" in delivery:
                        payload["command_status"] = str(delivery["command_status"])
                    if "command_ok" in delivery:
                        payload["command_ok"] = bool(delivery["command_ok"])
                    if "notified" in delivery:
                        payload["notified"] = bool(delivery["notified"])
                    if "opened" in delivery:
                        payload["opened"] = bool(delivery["opened"])
                    if "dry_run" in delivery:
                        payload["dry_run"] = bool(delivery["dry_run"])
                    if delivery.get("error"):
                        payload["delivery_error"] = str(delivery["error"])
                    if ring_sent_at_ms is not None:
                        payload["ring_sent_at_ms"] = round(float(ring_sent_at_ms), 2)
                    if wakeup_latency_ms is not None:
                        payload["wakeup_latency_ms"] = round(float(wakeup_latency_ms), 2)
                    payload["peer_acked"] = bool(payload.get("peer_acked") or row["acked_at"])
                    conn.execute(
                        "UPDATE events SET payload_json = ? WHERE event_id = ?",
                        (json.dumps(payload, ensure_ascii=False), int(eid)),
                    )

    def record_peer_wakeup_latency(
        self,
        peer_id: str,
        woke_at_ms: float,
        *,
        max_event_id: Optional[int] = None,
    ) -> Optional[float]:
        """Stamp measured kqueue wakeup latency (ms) onto the single triggering unacked event for peer_id."""
        peer = validate_peer(peer_id)
        recorded: Optional[float] = None
        woke_rounded = round(float(woke_at_ms), 2)
        with self._connect() as conn:
            with self._tx(conn):
                if max_event_id is not None and int(max_event_id) > 0:
                    rows = conn.execute(
                        """
                        SELECT event_id, payload_json
                        FROM events
                        WHERE to_peer = ? AND acked_at = '' AND event_id <= ?
                        ORDER BY event_id DESC
                        LIMIT 10
                        """,
                        (peer, int(max_event_id)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT event_id, payload_json
                        FROM events
                        WHERE to_peer = ? AND acked_at = ''
                        ORDER BY event_id DESC
                        LIMIT 10
                        """,
                        (peer,),
                    ).fetchall()
                for row in rows:
                    try:
                        payload = json.loads(row["payload_json"])
                    except ValueError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    if payload.get("delivery_mode") == "cold_notification":
                        continue
                    if "wakeup_latency_ms" in payload:
                        continue
                    sent_ms = payload.get("ring_sent_at_ms")
                    if sent_ms is None:
                        continue
                    latency = max(0.0, round(float(woke_at_ms) - float(sent_ms), 2))
                    if latency > 60000.0:
                        continue
                    payload["wakeup_latency_ms"] = latency
                    payload["woke_at_ms"] = woke_rounded
                    payload.setdefault("delivery_mode", "hot_wakeup")
                    conn.execute(
                        "UPDATE events SET payload_json = ? WHERE event_id = ?",
                        (json.dumps(payload, ensure_ascii=False), int(row["event_id"])),
                    )
                    recorded = latency
                    break
        return recorded

    def is_event_acked(self, event_id: int) -> bool:
        """Return True when an event has been acknowledged or no longer exists."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT acked_at FROM events WHERE event_id = ?", (int(event_id),)
            ).fetchone()
        if row is None:
            return True
        return bool(row["acked_at"])

    def pickup_inbox(
        self,
        peer_id: str,
        session_label: str = "",
        claim_tasks: bool = True,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """Atomically fetch/ack unread events, stamp claim latency, and claim queued tasks with attempt_id."""
        peer = validate_peer(peer_id)
        now = utc_now_iso()
        now_ms = round(time.time() * 1000.0, 2)
        capped_limit = max(1, int(limit))

        with self._connect() as conn:
            with self._tx(conn):
                event_rows = conn.execute(
                    """
                    SELECT * FROM events
                    WHERE to_peer = ? AND acked_at = ''
                    ORDER BY event_id ASC
                    LIMIT ?
                    """,
                    (peer, capped_limit),
                ).fetchall()
                decoded_events: List[Dict[str, Any]] = []
                for erow in event_rows:
                    ev = self._decode_event(erow)
                    epayload = dict(ev.get("payload") or {})
                    epayload["peer_acked"] = True
                    epayload["acked_at_ms"] = now_ms
                    sent_ms = epayload.get("ring_sent_at_ms")
                    if isinstance(sent_ms, (int, float)):
                        epayload["ack_latency_ms"] = max(0.0, round(now_ms - float(sent_ms), 2))
                    conn.execute(
                        "UPDATE events SET acked_at = ?, payload_json = ? WHERE event_id = ?",
                        (now, json.dumps(epayload, ensure_ascii=False), int(erow["event_id"])),
                    )
                    ev["acked_at"] = now
                    ev["payload"] = epayload
                    decoded_events.append(ev)

                claimed_tasks: List[Dict[str, Any]] = []
                if claim_tasks:
                    queued_rows = conn.execute(
                        """
                        SELECT * FROM tasks
                        WHERE assignee_peer = ? AND status = 'queued'
                        ORDER BY created_at ASC, task_id ASC
                        LIMIT ?
                        """,
                        (peer, capped_limit),
                    ).fetchall()
                    events_by_id = {int(e["event_id"]): e for e in decoded_events}
                    for qrow in queued_rows:
                        tid = qrow["task_id"]
                        next_seq = int(qrow["attempt_seq"] or 0) + 1
                        attempt_id = f"A-{next_seq}-{uuid.uuid4().hex[:8].upper()}"
                        disp_ms = float(qrow["dispatched_at_ms"] or 0.0)
                        claim_lat = (
                            max(0.0, round(now_ms - disp_ms, 2))
                            if disp_ms > 0.0
                            else None
                        )
                        cur = conn.execute(
                            """
                            UPDATE tasks
                            SET status = 'claimed',
                                attempt_id = ?,
                                attempt_seq = ?,
                                claimed_by_session = ?,
                                claimed_at = ?,
                                claimed_at_ms = ?,
                                claim_latency_ms = ?,
                                stale_at = '',
                                updated_at = ?
                            WHERE task_id = ? AND status = 'queued'
                            """,
                            (
                                attempt_id,
                                next_seq,
                                session_label,
                                now,
                                now_ms,
                                claim_lat,
                                now,
                                tid,
                            ),
                        )
                        if cur.rowcount == 0:
                            continue
                        refreshed = conn.execute(
                            "SELECT * FROM tasks WHERE task_id = ?", (tid,)
                        ).fetchone()
                        if refreshed is not None:
                            claimed = self._decode_task(refreshed)
                            mrow = conn.execute(
                                "SELECT title, workspace_path, coordinator_peer, native_sessions_json, tier FROM missions WHERE mission_id = ?",
                                (claimed["mission_id"],),
                            ).fetchone()
                            if mrow is not None:
                                claimed["mission_title"] = mrow["title"]
                                claimed["workspace_path"] = mrow["workspace_path"]
                                claimed["mission_coordinator"] = mrow["coordinator_peer"]
                                claimed["mission_tier"] = mrow["tier"]
                                try:
                                    bindings = json.loads(mrow["native_sessions_json"] or "{}")
                                except ValueError:
                                    bindings = {}
                                claimed["native_session"] = (bindings.get(peer) or {}).get("ref", "")
                            claimed["conversation"] = self._conversation_context_locked(conn, claimed["mission_id"], claimed["task_id"])
                            claimed_tasks.append(claimed)
                        # Annotate the triggering assignment/requeue event with claim_latency_ms + attempt_id.
                        src_ev = conn.execute(
                            """
                            SELECT event_id, payload_json FROM events
                            WHERE task_id = ? AND to_peer = ? AND kind IN ('task_assigned', 'task_requeued')
                            ORDER BY event_id DESC
                            LIMIT 1
                            """,
                            (tid, peer),
                        ).fetchone()
                        if src_ev is not None:
                            try:
                                sp = json.loads(src_ev["payload_json"])
                                if not isinstance(sp, dict):
                                    sp = {}
                            except ValueError:
                                sp = {}
                            sp["claimed_by_attempt_id"] = attempt_id
                            sp["attempt_seq"] = next_seq
                            if claim_lat is not None:
                                sp["claim_latency_ms"] = claim_lat
                            sp["peer_acked"] = True
                            conn.execute(
                                "UPDATE events SET payload_json = ? WHERE event_id = ?",
                                (json.dumps(sp, ensure_ascii=False), int(src_ev["event_id"])),
                            )
                            if int(src_ev["event_id"]) in events_by_id:
                                events_by_id[int(src_ev["event_id"])]["payload"] = sp

                self._touch_peer_locked(
                    conn,
                    peer,
                    session_label=session_label,
                    status="working" if claimed_tasks else "online",
                    now=now,
                )
            warnings = self._safe_sync_peers_seq([peer])

        return {
            "peer_id": peer,
            "events": decoded_events,
            "claimed_tasks": claimed_tasks,
            "picked_up_at": now,
            "post_commit_warnings": warnings,
        }

    def release_stale_claims(
        self,
        timeout_sec: Optional[int] = None,
        *,
        claimed_timeout_sec: Optional[int] = None,
        running_timeout_sec: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Re-queue stale claimed tasks and flag stale running tasks without losing active attempt_id."""
        claim_limit = (
            int(timeout_sec)
            if timeout_sec is not None
            else (
                int(claimed_timeout_sec)
                if claimed_timeout_sec is not None
                else int(
                    os.environ.get("CONSILIUM_CLAIM_TIMEOUT_SEC", str(DEFAULT_CLAIM_TIMEOUT_SEC))
                )
            )
        )
        run_limit = (
            int(running_timeout_sec)
            if running_timeout_sec is not None
            else int(
                os.environ.get("CONSILIUM_RUNNING_TIMEOUT_SEC", str(DEFAULT_RUNNING_TIMEOUT_SEC))
            )
        )
        now_dt = datetime.now(timezone.utc)
        now = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        now_ms = round(time.time() * 1000.0, 2)
        released: List[Dict[str, Any]] = []
        touched_peers: List[str] = []
        pulse_peers: List[str] = []

        with self._connect() as conn:
            with self._tx(conn):
                rows = conn.execute(
                    "SELECT * FROM tasks WHERE status IN ('claimed', 'running')"
                ).fetchall()
                for row in rows:
                    status = row["status"]
                    if status == "claimed":
                        ref_ts = row["claimed_at"] or row["updated_at"]
                        limit = claim_limit
                    else:
                        ref_ts = row["updated_at"] or row["claimed_at"]
                        limit = run_limit
                    ref_dt = _parse_iso(ref_ts)
                    if ref_dt is None:
                        continue
                    age = (now_dt - ref_dt).total_seconds()
                    if age <= limit:
                        continue

                    mission = conn.execute(
                        "SELECT * FROM missions WHERE mission_id = ?", (row["mission_id"],)
                    ).fetchone()
                    coord = (
                        mission["coordinator_peer"]
                        if mission is not None
                        else row["assignee_peer"]
                    )

                    if status == "claimed":
                        history = []
                        try:
                            parsed_hist = json.loads(row["attempt_history_json"] or "[]")
                            if isinstance(parsed_hist, list):
                                history = parsed_hist
                        except ValueError:
                            history = []
                        if row["attempt_id"] or row["attempt_seq"]:
                            history.append(
                                {
                                    "attempt_seq": int(row["attempt_seq"] or 0),
                                    "attempt_id": row["attempt_id"],
                                    "status": "claimed",
                                    "claimed_by_session": row["claimed_by_session"],
                                    "claimed_at": row["claimed_at"],
                                    "worktree_branch": row["worktree_branch"],
                                    "progress_note": row["progress_note"],
                                    "result_summary": "",
                                    "artifacts": [],
                                    "archived_at": now,
                                    "reason": f"claimed timeout exceeded ({int(age)}s > {limit}s)",
                                }
                            )
                        cur = conn.execute(
                            """
                            UPDATE tasks
                            SET status = 'queued',
                                attempt_id = '',
                                claimed_by_session = '',
                                claimed_at = '',
                                claimed_at_ms = 0.0,
                                claim_latency_ms = NULL,
                                stale_at = '',
                                progress_note = '',
                                result_summary = '',
                                artifacts_json = '[]',
                                attempt_history_json = ?,
                                dispatched_at_ms = ?,
                                updated_at = ?
                            WHERE task_id = ? AND status = 'claimed'
                            """,
                            (
                                json.dumps(history, ensure_ascii=False),
                                now_ms,
                                now,
                                row["task_id"],
                            ),
                        )
                        if cur.rowcount == 0:
                            continue
                        if mission is not None and mission["status"] == "ready_to_report":
                            conn.execute(
                                "UPDATE missions SET status = 'active', updated_at = ? WHERE mission_id = ?",
                                (now, row["mission_id"]),
                            )
                        evt = self._insert_event(
                            conn,
                            mission_id=row["mission_id"],
                            task_id=row["task_id"],
                            from_peer=row["assignee_peer"],
                            to_peer=coord,
                            kind="task_requeued",
                            payload={
                                "task_id": row["task_id"],
                                "previous_status": "claimed",
                                "previous_attempt_id": row["attempt_id"],
                                "assignee_peer": row["assignee_peer"],
                                "dispatched_at_ms": now_ms,
                                "reason": f"claimed timeout exceeded ({int(age)}s > {limit}s)",
                            },
                            created_at=now,
                        )
                        updated = conn.execute(
                            "SELECT * FROM tasks WHERE task_id = ?", (row["task_id"],)
                        ).fetchone()
                        if updated is not None:
                            decoded = self._decode_task(updated, now_dt=now_dt)
                            decoded["reaper_action"] = "requeued"
                            decoded["coordinator_peer"] = coord
                            decoded["requeue_event"] = evt
                            decoded["events"] = [evt]
                            released.append(decoded)
                            touched_peers.extend([row["assignee_peer"], coord])
                            pulse_peers.extend([row["assignee_peer"], coord])
                    else:
                        # Running task exceeded heartbeat limit: mark stale (do NOT requeue automatically!).
                        if row["stale_at"]:
                            continue
                        cur = conn.execute(
                            "UPDATE tasks SET stale_at = ? WHERE task_id = ? AND status = 'running' AND stale_at = ''",
                            (now, row["task_id"]),
                        )
                        if cur.rowcount == 0:
                            continue
                        stale_events: List[Dict[str, Any]] = []
                        stale_payload = {
                            "task_id": row["task_id"],
                            "attempt_id": row["attempt_id"],
                            "assignee_peer": row["assignee_peer"],
                            "age_seconds": int(age),
                            "limit_seconds": int(limit),
                            "reason": f"running task stale: no heartbeat for {int(age)}s > {limit}s; awaiting coordinator requeue",
                        }
                        evt_coord = self._insert_event(
                            conn,
                            mission_id=row["mission_id"],
                            task_id=row["task_id"],
                            from_peer=row["assignee_peer"],
                            to_peer=coord,
                            kind="task_stale",
                            payload=stale_payload,
                            created_at=now,
                        )
                        stale_events.append(evt_coord)
                        if row["assignee_peer"] != coord:
                            evt_assign = self._insert_event(
                                conn,
                                mission_id=row["mission_id"],
                                task_id=row["task_id"],
                                from_peer=coord,
                                to_peer=row["assignee_peer"],
                                kind="task_stale",
                                payload=stale_payload,
                                created_at=now,
                            )
                            stale_events.append(evt_assign)
                        updated = conn.execute(
                            "SELECT * FROM tasks WHERE task_id = ?", (row["task_id"],)
                        ).fetchone()
                        if updated is not None:
                            decoded = self._decode_task(updated, now_dt=now_dt)
                            decoded["reaper_action"] = "marked_stale"
                            decoded["coordinator_peer"] = coord
                            decoded["requeue_event"] = None
                            decoded["events"] = stale_events
                            released.append(decoded)
                            touched_peers.extend([row["assignee_peer"], coord])
                            pulse_peers.extend([row["assignee_peer"], coord])
            sync_warnings = self._safe_sync_peers_seq(touched_peers, pulse_peers=pulse_peers)
            if sync_warnings:
                for item in released:
                    item["post_commit_warnings"] = list(sync_warnings)
        return released

    def requeue_task(
        self,
        task_id: str,
        *,
        reason: str = "manual_requeue",
        actor_peer: Optional[str] = None,
        client_request_id: Optional[str] = None,
        caller_scope: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Manually return a claimed/running/failed/blocked task back to queued, archiving previous attempt."""
        now = utc_now_iso()
        now_ms = round(time.time() * 1000.0, 2)
        scope = (
            caller_scope
            if caller_scope is not None
            else (f"peer:{validate_peer(actor_peer)}" if actor_peer else "dashboard")
        )
        req_hash = _request_hash(
            {"task_id": task_id, "reason": reason, "actor_peer": actor_peer or ""}
        )
        with self._connect() as conn:
            with self._tx(conn):
                cached = self._check_idempotency_locked(
                    conn,
                    client_request_id,
                    "requeue_task",
                    caller_scope=scope,
                    request_hash=req_hash,
                )
                if cached is not None:
                    return cached

                task = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if task is None:
                    raise KeyError(f"Unknown task_id: {task_id}")
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (task["mission_id"],)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Missing mission for task {task_id}")
                if mission["status"] in VALID_MISSION_CLOSE_STATUSES:
                    raise ValueError(
                        f"Cannot requeue task {task_id} on closed mission {mission['mission_id']}"
                    )
                if actor_peer is not None:
                    actor = validate_peer(actor_peer)
                    if actor != mission["coordinator_peer"]:
                        raise ValueError(
                            f"Only coordinator {mission['coordinator_peer']!r} can requeue task {task_id}, got {actor!r}"
                        )
                prev_status = task["status"]
                if prev_status not in REQUEUEABLE_TASK_STATUSES:
                    raise ValueError(
                        f"Task {task_id} in status {prev_status!r} cannot be requeued; "
                        f"allowed statuses: {REQUEUEABLE_TASK_STATUSES}"
                    )

                coord = mission["coordinator_peer"]
                sender = validate_peer(actor_peer) if actor_peer else coord

                history = []
                try:
                    parsed_hist = json.loads(task["attempt_history_json"] or "[]")
                    if isinstance(parsed_hist, list):
                        history = parsed_hist
                except ValueError:
                    history = []
                try:
                    prev_artifacts = json.loads(task["artifacts_json"] or "[]")
                except ValueError:
                    prev_artifacts = []
                history.append(
                    {
                        "attempt_seq": int(task["attempt_seq"] or 0),
                        "attempt_id": task["attempt_id"],
                        "status": prev_status,
                        "claimed_by_session": task["claimed_by_session"],
                        "claimed_at": task["claimed_at"],
                        "worktree_branch": task["worktree_branch"],
                        "progress_note": task["progress_note"],
                        "result_summary": task["result_summary"],
                        "artifacts": prev_artifacts,
                        "archived_at": now,
                        "reason": reason,
                    }
                )

                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'queued',
                        attempt_id = '',
                        claimed_by_session = '',
                        claimed_at = '',
                        claimed_at_ms = 0.0,
                        claim_latency_ms = NULL,
                        stale_at = '',
                        progress_note = '',
                        result_summary = '',
                        artifacts_json = '[]',
                        attempt_history_json = ?,
                        dispatched_at_ms = ?,
                        updated_at = ?
                    WHERE task_id = ?
                    """,
                    (json.dumps(history, ensure_ascii=False), now_ms, now, task_id),
                )
                if mission["status"] == "ready_to_report":
                    conn.execute(
                        "UPDATE missions SET status = 'active', updated_at = ? WHERE mission_id = ?",
                        (now, task["mission_id"]),
                    )
                evt = self._insert_event(
                    conn,
                    mission_id=task["mission_id"],
                    task_id=task_id,
                    from_peer=sender,
                    to_peer=task["assignee_peer"],
                    kind="task_requeued",
                    payload={
                        "task_id": task_id,
                        "previous_status": prev_status,
                        "previous_attempt_id": task["attempt_id"],
                        "archived_attempt_seq": int(task["attempt_seq"] or 0),
                        "assignee_peer": task["assignee_peer"],
                        "dispatched_at_ms": now_ms,
                        "reason": reason,
                    },
                    created_at=now,
                )
                updated = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                out = {
                    "task": self._decode_task(updated),
                    "event": evt,
                    "coordinator_peer": coord,
                    "assignee_peer": task["assignee_peer"],
                }
                self._save_idempotency_locked(
                    conn,
                    client_request_id,
                    "requeue_task",
                    out,
                    now,
                    caller_scope=scope,
                    request_hash=req_hash,
                )
            warnings = self._safe_sync_peers_seq(
                [task["assignee_peer"], coord], pulse_peers=[task["assignee_peer"]]
            )
            out["post_commit_warnings"] = warnings
        return out

    def report_progress(
        self,
        task_id: str,
        peer_id: str,
        progress_note: str,
        status: str = "running",
        worktree_branch: Optional[str] = None,
        notify_coordinator: bool = False,
        attempt_id: Optional[str] = None,
        client_request_id: Optional[str] = None,
        model_used: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update claimed/running task progress while enforcing attempt_id ownership."""
        model_label = " ".join(str(model_used or "").split())[:80]
        peer = validate_peer(peer_id)
        normalized_status = validate_choice(
            status, PROGRESS_TASK_STATUSES, "progress status"
        )
        provided_attempt = (attempt_id or "").strip()
        caller_scope = f"peer:{peer}"
        req_hash = _request_hash(
            {
                "task_id": task_id,
                "peer_id": peer,
                "status": normalized_status,
                "progress_note": progress_note,
                "worktree_branch": worktree_branch,
                "notify_coordinator": bool(notify_coordinator),
                "attempt_id": provided_attempt,
            }
        )
        now = utc_now_iso()
        emitted_event: Optional[Dict[str, Any]] = None
        touched_peers = [peer]

        with self._connect() as conn:
            with self._tx(conn):
                cached = self._check_idempotency_locked(
                    conn,
                    client_request_id,
                    "report_progress",
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
                if cached is not None:
                    return cached

                task = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if task is None:
                    raise KeyError(f"Unknown task_id: {task_id}")
                if task["assignee_peer"] != peer:
                    raise ValueError(
                        f"Peer {peer!r} cannot update task {task_id} assigned to {task['assignee_peer']!r}"
                    )
                if task["status"] in TERMINAL_TASK_STATUSES:
                    raise ValueError(
                        f"Task {task_id} is already terminal (status={task['status']})"
                    )
                if task["status"] not in ("claimed", "running") or not task["attempt_id"]:
                    raise ValueError(
                        f"Task {task_id} has no active claim attempt (status={task['status']}); "
                        "call consilium_pickup_inbox first."
                    )
                if provided_attempt != task["attempt_id"]:
                    raise ValueError(
                        f"Stale or missing attempt_id for task {task_id}: "
                        f"got {attempt_id!r}, current attempt is {task['attempt_id']!r}"
                    )

                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (task["mission_id"],)
                ).fetchone()

                branch_value = (
                    worktree_branch
                    if worktree_branch is not None
                    else task["worktree_branch"]
                )
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, progress_note = ?, worktree_branch = ?, stale_at = '', updated_at = ?
                    WHERE task_id = ? AND attempt_id = ?
                    """,
                    (normalized_status, progress_note, branch_value, now, task_id, provided_attempt),
                )
                if model_label:
                    conn.execute("UPDATE tasks SET model_used = ? WHERE task_id = ?", (model_label, task_id))
                conn.execute(
                    "UPDATE missions SET updated_at = ? WHERE mission_id = ?",
                    (now, task["mission_id"]),
                )
                self._touch_peer_locked(conn, peer, status="working", now=now)
                if notify_coordinator and mission is not None:
                    touched_peers.append(mission["coordinator_peer"])
                    emitted_event = self._insert_event(
                        conn,
                        mission_id=task["mission_id"],
                        task_id=task_id,
                        from_peer=peer,
                        to_peer=mission["coordinator_peer"],
                        kind="progress",
                        payload={
                            "task_id": task_id,
                            "attempt_id": provided_attempt,
                            "status": normalized_status,
                            "progress_note": progress_note,
                            "worktree_branch": branch_value,
                        },
                        created_at=now,
                    )
                updated_row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                out = {
                    "task": self._decode_task(updated_row),
                    "event": emitted_event,
                    "coordinator_peer": mission["coordinator_peer"] if mission else "",
                }
                self._save_idempotency_locked(
                    conn,
                    client_request_id,
                    "report_progress",
                    out,
                    now,
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
            warnings = self._safe_sync_peers_seq(touched_peers)
            out["post_commit_warnings"] = warnings

        return out

    def complete_task(
        self,
        task_id: str,
        peer_id: str,
        result_summary: str,
        artifacts: Optional[List[str]] = None,
        status: str = "done",
        attempt_id: Optional[str] = None,
        client_request_id: Optional[str] = None,
        model_used: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Mark a claimed/running task terminal after verifying attempt_id, check mission completion, and notify coordinator."""
        model_label = " ".join(str(model_used or "").split())[:80]
        peer = validate_peer(peer_id)
        terminal_status = validate_choice(status, TERMINAL_TASK_STATUSES, "terminal status")
        provided_attempt = (attempt_id or "").strip()
        now = utc_now_iso()
        artifacts_list = list(artifacts or [])
        artifacts_json = json.dumps(artifacts_list, ensure_ascii=False)
        caller_scope = f"peer:{peer}"
        req_hash = _request_hash(
            {
                "task_id": task_id,
                "peer_id": peer,
                "result_summary": result_summary,
                "artifacts": artifacts_list,
                "status": terminal_status,
                "attempt_id": provided_attempt,
            }
        )

        with self._connect() as conn:
            with self._tx(conn):
                cached = self._check_idempotency_locked(
                    conn,
                    client_request_id,
                    "complete_task",
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
                if cached is not None:
                    return cached

                task = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if task is None:
                    raise KeyError(f"Unknown task_id: {task_id}")
                if task["assignee_peer"] != peer:
                    raise ValueError(
                        f"Peer {peer!r} cannot complete task {task_id} assigned to {task['assignee_peer']!r}"
                    )
                if task["status"] in TERMINAL_TASK_STATUSES:
                    raise ValueError(
                        f"Task {task_id} is already terminal (status={task['status']})"
                    )
                if task["status"] not in ("claimed", "running") or not task["attempt_id"]:
                    raise ValueError(
                        f"Task {task_id} has no active claim attempt (status={task['status']}); "
                        "call consilium_pickup_inbox first."
                    )
                if provided_attempt != task["attempt_id"]:
                    raise ValueError(
                        f"Stale or missing attempt_id for task {task_id}: "
                        f"got {attempt_id!r}, current attempt is {task['attempt_id']!r}"
                    )

                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (task["mission_id"],)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Missing mission for task {task_id}")

                conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, result_summary = ?, artifacts_json = ?, stale_at = '', updated_at = ?
                    WHERE task_id = ? AND attempt_id = ?
                    """,
                    (terminal_status, result_summary, artifacts_json, now, task_id, provided_attempt),
                )
                if model_label:
                    conn.execute("UPDATE tasks SET model_used = ? WHERE task_id = ?", (model_label, task_id))
                self._touch_peer_locked(conn, peer, status="active", now=now)
                placeholders = ",".join("?" for _ in TERMINAL_TASK_STATUSES)
                remaining_row = conn.execute(
                    f"""
                    SELECT COUNT(*) AS cnt FROM tasks
                    WHERE mission_id = ? AND status NOT IN ({placeholders})
                    """,
                    (task["mission_id"],) + TERMINAL_TASK_STATUSES,
                ).fetchone()
                remaining = int(remaining_row["cnt"]) if remaining_row else 0
                all_finished = remaining == 0

                if all_finished and mission["status"] == "active":
                    conn.execute(
                        "UPDATE missions SET status = 'ready_to_report', updated_at = ? WHERE mission_id = ?",
                        (now, task["mission_id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE missions SET updated_at = ? WHERE mission_id = ?",
                        (now, task["mission_id"]),
                    )

                emitted_event = self._insert_event(
                    conn,
                    mission_id=task["mission_id"],
                    task_id=task_id,
                    from_peer=peer,
                    to_peer=mission["coordinator_peer"],
                    kind="task_done",
                    payload={
                        "task_id": task_id,
                        "attempt_id": provided_attempt,
                        "status": terminal_status,
                        "result_summary": result_summary,
                        "artifacts": artifacts_list,
                        "all_tasks_finished": all_finished,
                    },
                    created_at=now,
                )
                updated_row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                out = {
                    "task": self._decode_task(updated_row),
                    "event": emitted_event,
                    "coordinator_peer": mission["coordinator_peer"],
                    "all_tasks_finished": all_finished,
                }
                self._save_idempotency_locked(
                    conn,
                    client_request_id,
                    "complete_task",
                    out,
                    now,
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
            warnings = self._safe_sync_peers_seq([mission["coordinator_peer"], peer])
            out["post_commit_warnings"] = warnings

        return out

    def request_review(
        self,
        mission_id: str,
        from_peer: str,
        to_peer: str,
        summary: str,
        question: str,
        task_id: Optional[str] = None,
        client_request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Request a cross-peer review while atomically enforcing max_rounds loop guard."""
        sender = validate_peer(from_peer)
        recipient = validate_peer(to_peer)
        now = utc_now_iso()
        caller_scope = f"peer:{sender}"
        req_hash = _request_hash(
            {
                "mission_id": mission_id,
                "from_peer": sender,
                "to_peer": recipient,
                "summary": summary,
                "question": question,
                "task_id": task_id or "",
            }
        )

        with self._connect() as conn:
            with self._tx(conn):
                cached = self._check_idempotency_locked(
                    conn,
                    client_request_id,
                    "request_review",
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
                if cached is not None:
                    return cached

                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                if mission["status"] in VALID_MISSION_CLOSE_STATUSES:
                    raise ValueError(
                        f"Cannot request review on closed mission {mission_id} (status={mission['status']})"
                    )
                if task_id:
                    trow = conn.execute(
                        "SELECT mission_id FROM tasks WHERE task_id = ?", (task_id,)
                    ).fetchone()
                    if trow is None or trow["mission_id"] != mission_id:
                        raise ValueError(
                            f"task_id {task_id!r} does not belong to mission {mission_id!r}"
                        )

                self._touch_peer_locked(conn, sender, status="active", now=now)
                current_round = int(mission["current_round"])
                max_rounds = int(mission["max_rounds"])

                if current_round >= max_rounds:
                    guard_event = self._insert_event(
                        conn,
                        mission_id=mission_id,
                        task_id=task_id or "",
                        from_peer=sender,
                        to_peer=mission["coordinator_peer"],
                        kind="loop_guard_reached",
                        payload={
                            "attempted_from": sender,
                            "attempted_to": recipient,
                            "current_round": current_round,
                            "max_rounds": max_rounds,
                            "summary": summary,
                            "question": question,
                        },
                        created_at=now,
                    )
                    conn.execute(
                        "UPDATE missions SET updated_at = ? WHERE mission_id = ?",
                        (now, mission_id),
                    )
                    sync_target = mission["coordinator_peer"]
                    res_payload = {
                        "allowed": False,
                        "round": current_round,
                        "max_rounds": max_rounds,
                        "coordinator_peer": mission["coordinator_peer"],
                        "event": guard_event,
                        "reason": (
                            f"Loop guard reached ({current_round}/{max_rounds} rounds); "
                            "escalating back to coordinator/human."
                        ),
                    }
                else:
                    new_round = current_round + 1
                    conn.execute(
                        "UPDATE missions SET current_round = ?, updated_at = ? WHERE mission_id = ?",
                        (new_round, now, mission_id),
                    )
                    review_event = self._insert_event(
                        conn,
                        mission_id=mission_id,
                        task_id=task_id or "",
                        from_peer=sender,
                        to_peer=recipient,
                        kind="review_request",
                        payload={
                            "round": new_round,
                            "max_rounds": max_rounds,
                            "summary": summary,
                            "question": question,
                        },
                        created_at=now,
                    )
                    sync_target = recipient
                    res_payload = {
                        "allowed": True,
                        "round": new_round,
                        "max_rounds": max_rounds,
                        "to_peer": recipient,
                        "event": review_event,
                    }
                self._save_idempotency_locked(
                    conn,
                    client_request_id,
                    "request_review",
                    res_payload,
                    now,
                    caller_scope=caller_scope,
                    request_hash=req_hash,
                )
            warnings = self._safe_sync_peers_seq([sync_target, sender])
            res_payload["post_commit_warnings"] = warnings

        return res_payload

    def close_mission(
        self,
        mission_id: str,
        final_summary: str,
        peer_id: Optional[str] = None,
        status: str = "completed",
        force: bool = False,
        client_request_id: Optional[str] = None,
        caller_scope: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record final summary and close a mission with coordinator/state validation."""
        close_status = validate_choice(
            status, VALID_MISSION_CLOSE_STATUSES, "mission close status"
        )
        scope = (
            caller_scope
            if caller_scope is not None
            else (f"peer:{validate_peer(peer_id)}" if peer_id is not None else "dashboard")
        )
        req_hash = _request_hash(
            {
                "mission_id": mission_id,
                "final_summary": final_summary,
                "peer_id": peer_id or "",
                "status": close_status,
                "force": bool(force),
            }
        )
        now = utc_now_iso()
        cancelled_task_ids: List[str] = []
        emitted_events: List[Dict[str, Any]] = []
        touched_peers: List[str] = []

        with self._connect() as conn:
            with self._tx(conn):
                cached = self._check_idempotency_locked(
                    conn,
                    client_request_id,
                    "close_mission",
                    caller_scope=scope,
                    request_hash=req_hash,
                )
                if cached is not None:
                    return cached

                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                if peer_id is not None:
                    caller = validate_peer(peer_id)
                    if caller != mission["coordinator_peer"]:
                        raise ValueError(
                            f"Only coordinator {mission['coordinator_peer']!r} can close mission {mission_id}, got {caller!r}"
                        )
                if mission["status"] in VALID_MISSION_CLOSE_STATUSES:
                    raise ValueError(
                        f"Mission {mission_id} is already closed (status={mission['status']})"
                    )

                self._touch_peer_locked(
                    conn, mission["coordinator_peer"], status="active", now=now
                )
                touched_peers.append(mission["coordinator_peer"])
                placeholders = ",".join("?" for _ in NON_TERMINAL_TASK_STATUSES)
                open_rows = conn.execute(
                    f"SELECT * FROM tasks WHERE mission_id = ? AND status IN ({placeholders})",
                    (mission_id,) + NON_TERMINAL_TASK_STATUSES,
                ).fetchall()
                if open_rows and not force:
                    raise ValueError(
                        f"Mission {mission_id} has {len(open_rows)} non-terminal task(s); pass force=True to cancel them"
                    )
                if open_rows and force:
                    for row in open_rows:
                        conn.execute(
                            "UPDATE tasks SET status = 'cancelled', stale_at = '', updated_at = ? WHERE task_id = ?",
                            (now, row["task_id"]),
                        )
                        cancelled_task_ids.append(row["task_id"])
                        evt = self._insert_event(
                            conn,
                            mission_id=mission_id,
                            task_id=row["task_id"],
                            from_peer=mission["coordinator_peer"],
                            to_peer=row["assignee_peer"],
                            kind="mission_closed",
                            payload={
                                "mission_id": mission_id,
                                "task_id": row["task_id"],
                                "close_status": close_status,
                                "final_summary": final_summary[:200],
                            },
                            created_at=now,
                        )
                        emitted_events.append(evt)
                        touched_peers.append(row["assignee_peer"])

                conn.execute(
                    """
                    UPDATE missions
                    SET final_summary = ?, status = ?, updated_at = ?
                    WHERE mission_id = ?
                    """,
                    (final_summary, close_status, now, mission_id),
                )
                updated = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                out = _row_to_dict(updated)
                out["cancelled_task_ids"] = cancelled_task_ids
                out["events"] = emitted_events
                self._save_idempotency_locked(
                    conn,
                    client_request_id,
                    "close_mission",
                    out,
                    now,
                    caller_scope=scope,
                    request_hash=req_hash,
                )
            warnings = self._safe_sync_peers_seq(touched_peers)
            out["post_commit_warnings"] = warnings

        return out

    def get_mission_snapshot(self, mission_id: Optional[str] = None) -> Dict[str, Any]:
        """Return full mission details (with tasks and recent events) or mission list."""
        with self._connect() as conn:
            if mission_id:
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                tasks = conn.execute(
                    "SELECT * FROM tasks WHERE mission_id = ? ORDER BY created_at ASC, task_id ASC",
                    (mission_id,),
                ).fetchall()
                events = conn.execute(
                    "SELECT * FROM events WHERE mission_id = ? ORDER BY event_id DESC LIMIT 50",
                    (mission_id,),
                ).fetchall()
                return {
                    "mission": _row_to_dict(mission),
                    "tasks": [self._decode_task(t) for t in tasks],
                    "events": [self._decode_event(e) for e in events],
                }

            missions = conn.execute(
                "SELECT * FROM missions ORDER BY updated_at DESC LIMIT 20"
            ).fetchall()
            summaries: List[Dict[str, Any]] = []
            for m in missions:
                counts = conn.execute(
                    """
                    SELECT status, COUNT(*) AS cnt
                    FROM tasks WHERE mission_id = ?
                    GROUP BY status
                    """,
                    (m["mission_id"],),
                ).fetchall()
                item = _row_to_dict(m)
                item["task_counts"] = {c["status"]: c["cnt"] for c in counts}
                summaries.append(item)
        return {"missions": summaries}

    # ---- User conversations, preferences and quota-aware handoff ----

    def _insert_user_message_locked(
        self, conn: sqlite3.Connection, mission_id: str, message: Dict[str, Any], now: str
    ) -> None:
        text = str(message.get("text") or "").strip()
        if not text:
            return
        targets = [str(t) for t in (message.get("targets") or [])]
        conn.execute(
            "INSERT INTO user_messages (mission_id, text, targets_json, mode, reason, created_at, created_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (mission_id, text[:20000], json.dumps(targets, ensure_ascii=False), str(message.get("mode") or "direct"),
             str(message.get("reason") or "")[:300], now, round(time.time() * 1000.0, 2)),
        )

    def set_mission_title(self, mission_id: str, title: str, actor: str) -> Dict[str, Any]:
        """Give a conversation a short name (shown in the sidebar and used to name native sessions)."""
        clean = " ".join(str(title or "").split())
        if not clean:
            raise ValueError("title must not be empty")
        if len(clean) > 40:
            raise ValueError("title must be at most 40 characters")
        who = USER_PEER if actor == USER_PEER else validate_peer(actor)
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                if who not in (USER_PEER, mission["coordinator_peer"]):
                    involved = conn.execute(
                        "SELECT 1 FROM tasks WHERE mission_id = ? AND assignee_peer = ? LIMIT 1",
                        (mission_id, who),
                    ).fetchone()
                    if involved is None:
                        raise ValueError(f"Peer {who!r} is not part of mission {mission_id}")
                if who != USER_PEER and int(mission["title_locked"] or 0):
                    return {"mission_id": mission_id, "title": mission["title"], "set_by": USER_PEER, "locked": True}
                conn.execute(
                    "UPDATE missions SET title = ?, title_locked = ?, updated_at = ? WHERE mission_id = ?",
                    (clean, 1 if who == USER_PEER else int(mission["title_locked"] or 0), now, mission_id),
                )
        return {"mission_id": mission_id, "title": clean, "set_by": who}

    def _conversation_context_locked(
        self, conn: sqlite3.Connection, mission_id: str, current_task_id: str, limit: int = 12
    ) -> List[Dict[str, Any]]:
        """Earlier turns of this conversation (user messages and peers' replies), oldest first.

        Standby sessions answer many conversations, so each task carries its own history instead of
        relying on one native session per conversation (Claude Desktop does not let an automatically
        woken turn start new sessions).
        """
        items: List[Dict[str, Any]] = []
        for r in conn.execute(
            "SELECT text, created_at, created_ms, message_id FROM user_messages WHERE mission_id = ? ORDER BY message_id ASC",
            (mission_id,),
        ).fetchall():
            items.append({"at": r["created_at"], "_ms": float(r["created_ms"] or 0), "_seq": int(r["message_id"]),
                          "from": USER_PEER, "text": r["text"][:1500]})
        for r in conn.execute(
            "SELECT event_id, from_peer, task_id, payload_json, created_at FROM events "
            "WHERE mission_id = ? AND kind = 'task_done' AND task_id != ? ORDER BY event_id ASC",
            (mission_id, current_task_id),
        ).fetchall():
            try:
                payload = json.loads(r["payload_json"] or "{}")
            except ValueError:
                payload = {}
            text = str(payload.get("result_summary") or "")
            if text:
                items.append({"at": r["created_at"], "_ms": float(payload.get("ring_sent_at_ms") or 0),
                              "_seq": int(r["event_id"]), "from": r["from_peer"], "text": text[:1500]})
        items.sort(key=lambda x: (x["_ms"] or 0, x["at"], x["_seq"]))
        for it in items:
            it.pop("_ms", None)
            it.pop("_seq", None)
        return items[-max(1, int(limit)):]

    def bind_native_session(self, mission_id: str, peer_id: str, session_ref: str) -> Dict[str, Any]:
        """Remember which native session (e.g. a Claude Code session id) handles this conversation for a peer."""
        peer = validate_peer(peer_id)
        ref = str(session_ref or "").strip()
        if not ref or len(ref) > 200:
            raise ValueError("session_ref must be 1-200 characters")
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                mission = conn.execute(
                    "SELECT native_sessions_json FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                try:
                    bindings = json.loads(mission["native_sessions_json"] or "{}")
                    if not isinstance(bindings, dict):
                        bindings = {}
                except ValueError:
                    bindings = {}
                bindings[peer] = {"ref": ref, "bound_at": now}
                conn.execute(
                    "UPDATE missions SET native_sessions_json = ?, updated_at = ? WHERE mission_id = ?",
                    (json.dumps(bindings, ensure_ascii=False), now, mission_id),
                )
        return {"mission_id": mission_id, "peer_id": peer, "session_ref": ref}

    def mission_bindings(self, mission_id: str) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT native_sessions_json FROM missions WHERE mission_id = ?", (mission_id,)
            ).fetchone()
        if row is None:
            return {}
        try:
            data = json.loads(row["native_sessions_json"] or "{}")
            return data if isinstance(data, dict) else {}
        except ValueError:
            return {}

    def set_mission_archived(self, mission_id: str, archived: bool) -> Dict[str, Any]:
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                cur = conn.execute(
                    "UPDATE missions SET archived = ?, updated_at = ? WHERE mission_id = ?",
                    (1 if archived else 0, now, mission_id),
                )
                if cur.rowcount == 0:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
        return {"mission_id": mission_id, "archived": bool(archived)}

    def set_mission_tier(self, mission_id: str, tier: str) -> None:
        with self._connect() as conn:
            with self._tx(conn):
                conn.execute("UPDATE missions SET tier = ? WHERE mission_id = ?", (tier, mission_id))

    def set_mission_workspace(self, mission_id: str, path: str, actor: str) -> Dict[str, Any]:
        """Record the folder a conversation works in. Must be an existing absolute directory."""
        raw = str(path or "").strip()
        if not raw.startswith("/") and not raw.startswith("~"):
            raise ValueError("workspace must be an absolute path")
        resolved = os.path.realpath(os.path.expanduser(raw))
        if not os.path.isdir(resolved):
            raise ValueError(f"workspace folder does not exist: {raw}")
        who = USER_PEER if actor == USER_PEER else validate_peer(actor)
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                if mission["status"] in VALID_MISSION_CLOSE_STATUSES:
                    raise ValueError(f"Mission {mission_id} is closed")
                if who not in (USER_PEER, mission["coordinator_peer"]):
                    involved = conn.execute(
                        "SELECT 1 FROM tasks WHERE mission_id = ? AND assignee_peer = ? LIMIT 1",
                        (mission_id, who),
                    ).fetchone()
                    if involved is None:
                        raise ValueError(f"Peer {who!r} is not part of mission {mission_id}")
                conn.execute(
                    "UPDATE missions SET workspace_path = ?, updated_at = ? WHERE mission_id = ?",
                    (resolved, now, mission_id),
                )
        return {"mission_id": mission_id, "workspace_path": resolved, "set_by": who}

    def latest_model_by_peer(self) -> Dict[str, str]:
        """The model each peer most recently reported via model_used (for the sidebar)."""
        out: Dict[str, str] = {}
        with self._connect() as conn:
            for r in conn.execute(
                "SELECT assignee_peer, model_used FROM tasks WHERE model_used != '' ORDER BY updated_at ASC"
            ).fetchall():
                out[r["assignee_peer"]] = r["model_used"]
        return out

    def list_preferences(self, include_inactive: bool = False) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM preferences "
                + ("" if include_inactive else "WHERE active = 1 ")
                + "ORDER BY pref_id ASC"
            ).fetchall()
        return [
            {
                "pref_id": int(r["pref_id"]),
                "text": r["text"],
                "source": r["source"],
                "active": bool(r["active"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def add_preference(self, text: str, source: str) -> Dict[str, Any]:
        clean = " ".join(str(text or "").split())
        if not clean:
            raise ValueError("preference text must not be empty")
        if len(clean) > 300:
            raise ValueError("preference text must be at most 300 characters")
        src = USER_PEER if source == USER_PEER else validate_peer(source)
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                dup = conn.execute(
                    "SELECT pref_id FROM preferences WHERE active = 1 AND text = ?", (clean,)
                ).fetchone()
                if dup is not None:
                    return {"pref_id": int(dup["pref_id"]), "text": clean, "duplicate": True}
                active_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM preferences WHERE active = 1"
                ).fetchone()["c"]
                if int(active_count) >= MAX_ACTIVE_PREFERENCES:
                    raise ValueError(
                        f"At most {MAX_ACTIVE_PREFERENCES} active preferences; ask the user to remove one first"
                    )
                cur = conn.execute(
                    "INSERT INTO preferences (text, source, active, created_at) VALUES (?, ?, 1, ?)",
                    (clean, src, now),
                )
        return {"pref_id": int(cur.lastrowid), "text": clean, "source": src, "created_at": now}

    def deactivate_preference(self, pref_id: int) -> bool:
        with self._connect() as conn:
            with self._tx(conn):
                cur = conn.execute(
                    "UPDATE preferences SET active = 0 WHERE pref_id = ? AND active = 1", (int(pref_id),)
                )
        return cur.rowcount > 0

    def recent_user_routing(self, limit: int = 15) -> List[Dict[str, Any]]:
        """Recent explicit (@-mentioned) sends, so a coordinator can infer the user's habits."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT text, targets_json, created_at FROM user_messages WHERE mode = 'direct' "
                "ORDER BY message_id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        return [
            {"text": r["text"][:160], "targets": json.loads(r["targets_json"] or "[]"), "created_at": r["created_at"]}
            for r in rows
        ]

    def handoff_task(
        self,
        task_id: str,
        peer_id: str,
        attempt_id: str,
        to_peer: str,
        note: str,
    ) -> Dict[str, Any]:
        """Hand an active task to another peer (e.g. low quota), archiving the current attempt."""
        peer = validate_peer(peer_id)
        target = validate_peer(to_peer)
        if target == peer:
            raise ValueError("Cannot hand a task off to the same peer")
        handoff_note = str(note or "").strip()
        if not handoff_note:
            raise ValueError("handoff note must describe progress so far and why")
        now = utc_now_iso()
        now_ms = round(time.time() * 1000.0, 2)
        with self._connect() as conn:
            with self._tx(conn):
                task = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
                if task is None:
                    raise KeyError(f"Unknown task_id: {task_id}")
                if task["assignee_peer"] != peer:
                    raise ValueError(f"Peer {peer!r} does not own task {task_id}")
                if task["status"] not in ("claimed", "running") or task["attempt_id"] != (attempt_id or "").strip():
                    raise ValueError(f"Stale or missing attempt_id for task {task_id}")
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (task["mission_id"],)
                ).fetchone()
                if mission is None or mission["status"] in VALID_MISSION_CLOSE_STATUSES:
                    raise ValueError(f"Mission for task {task_id} is closed")
                try:
                    history = json.loads(task["attempt_history_json"] or "[]")
                    if not isinstance(history, list):
                        history = []
                except ValueError:
                    history = []
                history.append(
                    {
                        "attempt_seq": int(task["attempt_seq"] or 0),
                        "attempt_id": task["attempt_id"],
                        "status": task["status"],
                        "assignee_peer": peer,
                        "progress_note": task["progress_note"],
                        "archived_at": now,
                        "reason": f"handoff to {target}: {handoff_note}",
                    }
                )
                new_prompt = (
                    task["prompt"]
                    + f"\n\n【交接说明，来自 {peer}】{handoff_note}"
                )
                conn.execute(
                    """
                    UPDATE tasks
                    SET assignee_peer = ?, status = 'queued', attempt_id = '', claimed_by_session = '',
                        claimed_at = '', claimed_at_ms = 0.0, claim_latency_ms = NULL, stale_at = '',
                        progress_note = '', prompt = ?, attempt_history_json = ?, dispatched_at_ms = ?,
                        updated_at = ?
                    WHERE task_id = ?
                    """,
                    (target, new_prompt, json.dumps(history, ensure_ascii=False), now_ms, now, task_id),
                )
                self._touch_peer_locked(conn, peer, status="online", now=now)
                evt = self._insert_event(
                    conn,
                    mission_id=task["mission_id"],
                    task_id=task_id,
                    from_peer=peer,
                    to_peer=target,
                    kind="task_handoff",
                    payload={
                        "task_id": task_id,
                        "from_peer": peer,
                        "to_peer": target,
                        "note": handoff_note,
                        "previous_attempt_id": task["attempt_id"],
                        "dispatched_at_ms": now_ms,
                    },
                    created_at=now,
                )
                updated = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
                out = {
                    "task": self._decode_task(updated),
                    "event": evt,
                    "coordinator_peer": mission["coordinator_peer"],
                }
            out["post_commit_warnings"] = self._safe_sync_peers_seq(
                [peer, target], pulse_peers=[target]
            )
        return out

    def get_dashboard_missions(
        self, limit: int = 20, events_per_mission: int = 100
    ) -> tuple:
        """Return enriched missions (with tasks and reverse-chronological events) and latest_event_id."""
        with self._connect() as conn:
            latest_row = conn.execute(
                "SELECT COALESCE(MAX(event_id), 0) AS max_id FROM events"
            ).fetchone()
            latest_event_id = int(latest_row["max_id"] or 0) if latest_row else 0
            mission_rows = conn.execute(
                "SELECT * FROM missions ORDER BY updated_at DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
            missions_out: List[Dict[str, Any]] = []
            for m in mission_rows:
                mid = m["mission_id"]
                task_rows = conn.execute(
                    "SELECT * FROM tasks WHERE mission_id = ? ORDER BY created_at ASC, task_id ASC",
                    (mid,),
                ).fetchall()
                event_rows = conn.execute(
                    "SELECT * FROM events WHERE mission_id = ? ORDER BY event_id DESC LIMIT ?",
                    (mid, max(1, int(events_per_mission))),
                ).fetchall()
                tasks_dec = [self._decode_task(t) for t in task_rows]
                counts: Dict[str, int] = {}
                for t in tasks_dec:
                    st = t["status"]
                    counts[st] = counts.get(st, 0) + 1
                item = _row_to_dict(m)
                item["task_counts"] = counts
                try:
                    item["native_sessions"] = json.loads(item.pop("native_sessions_json", "{}") or "{}")
                except ValueError:
                    item["native_sessions"] = {}
                item["tasks"] = tasks_dec
                item["events"] = [self._decode_event(e) for e in event_rows]
                item["user_messages"] = [
                    {
                        "message_id": int(r["message_id"]),
                        "text": r["text"],
                        "targets": json.loads(r["targets_json"] or "[]"),
                        "mode": r["mode"],
                        "reason": r["reason"] if "reason" in r.keys() else "",
                        "created_at": r["created_at"],
                    }
                    for r in conn.execute(
                        "SELECT * FROM user_messages WHERE mission_id = ? ORDER BY message_id ASC",
                        (mid,),
                    ).fetchall()
                ]
                missions_out.append(item)
        return missions_out, latest_event_id

    def sync_peer_accounts(
        self,
        peer_id: str,
        accounts: List[Dict[str, Any]],
    ) -> None:
        """Replace authoritative known-account registry for a peer inside one transaction."""
        peer = validate_peer(peer_id)
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                conn.execute("DELETE FROM quota_accounts WHERE peer_id = ?", (peer,))
                for item in accounts:
                    acct_id = str(item.get("account_id") or "").strip()
                    if not acct_id:
                        continue
                    conn.execute(
                        """
                        INSERT INTO quota_accounts (
                            peer_id, account_id, account_alias, plan_type, is_current, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            peer,
                            acct_id,
                            str(item.get("account_alias") or "").strip(),
                            str(item.get("plan_type") or "").strip(),
                            1 if item.get("is_current") else 0,
                            now,
                        ),
                    )
                known_ids = [
                    str(item.get("account_id") or "").strip()
                    for item in accounts
                    if str(item.get("account_id") or "").strip()
                ]
                conn.execute(
                    "UPDATE quota_windows SET is_current = 0 WHERE peer_id = ?",
                    (peer,),
                )
                for item in accounts:
                    acct_id = str(item.get("account_id") or "").strip()
                    if not acct_id:
                        continue
                    alias = str(item.get("account_alias") or "").strip()
                    plan = str(item.get("plan_type") or "").strip()
                    conn.execute(
                        """
                        UPDATE quota_windows
                        SET is_current = ?,
                            is_historical = 0,
                            account_alias = CASE WHEN ? != '' THEN ? ELSE account_alias END,
                            plan_type = CASE WHEN ? != '' THEN ? ELSE plan_type END
                        WHERE peer_id = ? AND account_id = ?
                        """,
                        (
                            1 if item.get("is_current") else 0,
                            alias,
                            alias,
                            plan,
                            plan,
                            peer,
                            acct_id,
                        ),
                    )
                if known_ids:
                    placeholders = ",".join("?" for _ in known_ids)
                    conn.execute(
                        f"UPDATE quota_windows SET is_historical = 1 WHERE peer_id = ? AND account_id NOT IN ({placeholders})",
                        (peer, *known_ids),
                    )

    def clear_peer_cockpit_current(self, peer_id: str) -> None:
        """Clear stale Cockpit current-account markers when authoritative registry is absent."""
        peer = validate_peer(peer_id)
        with self._connect() as conn:
            with self._tx(conn):
                conn.execute("DELETE FROM quota_accounts WHERE peer_id = ?", (peer,))
                conn.execute(
                    "UPDATE quota_windows SET is_current = 0 WHERE peer_id = ? AND source LIKE 'cockpit%'",
                    (peer,),
                )

    def _decode_quota_row(
        self, row: sqlite3.Row, now_dt: Optional[datetime] = None
    ) -> Dict[str, Any]:
        data = _row_to_dict(row)
        now = now_dt or datetime.now(timezone.utc)
        remaining = round(max(0.0, min(100.0, float(data["remaining_percent"]))), 2)
        data["remaining_percent"] = remaining
        reset_dt = _parse_iso(data.get("reset_at", ""))
        reset_sec: Optional[int] = None
        if reset_dt is not None:
            reset_sec = int((reset_dt - now).total_seconds())
        data["reset_in_seconds"] = reset_sec
        data["reset_in_human"] = _format_countdown(reset_sec)
        obs_dt = _parse_iso(data.get("observed_at", ""))
        obs_age: Optional[int] = None
        if obs_dt is not None:
            obs_age = max(0, int((now - obs_dt).total_seconds()))
        data["sample_age_seconds"] = obs_age
        data["expired_reset"] = reset_sec is not None and reset_sec <= 0
        data["exhausted"] = (remaining <= 0.0) and not data["expired_reset"]
        data["low_quota"] = (remaining <= 15.0) and not data["expired_reset"]
        data["is_current"] = bool(data.get("is_current"))
        data["is_historical"] = bool(data.get("is_historical"))
        return data

    def upsert_quota_windows(
        self, windows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Insert or update quota window snapshots inside an immediate transaction."""
        if not windows:
            return []
        now = utc_now_iso()
        keys: List[tuple] = []
        peers_with_current = {
            validate_peer(str(item["peer_id"]))
            for item in windows
            if item.get("is_current")
        }
        with self._connect() as conn:
            with self._tx(conn):
                for cp in peers_with_current:
                    conn.execute(
                        "UPDATE quota_windows SET is_current = 0 WHERE peer_id = ?",
                        (cp,),
                    )
                for item in windows:
                    peer = validate_peer(str(item["peer_id"]))
                    account_id = str(item.get("account_id") or "default").strip() or "default"
                    if "@" in account_id:
                        digest = hashlib.sha256(account_id.lower().encode("utf-8")).hexdigest()[:10]
                        account_id = f"acct-{digest}"
                    window_key = str(item["window_key"]).strip()
                    if not window_key:
                        raise ValueError("window_key must not be empty")
                    label = str(item.get("label") or window_key).strip()
                    remaining = max(0.0, min(100.0, float(item["remaining_percent"])))
                    reset_at = str(item.get("reset_at") or "").strip()
                    source = str(item.get("source") or "manual").strip()
                    observed_at = str(item.get("observed_at") or now).strip()
                    is_current = 1 if item.get("is_current") else 0
                    is_historical = 1 if item.get("is_historical") else 0
                    account_alias = str(item.get("account_alias") or "").strip()
                    plan_type = str(item.get("plan_type") or "").strip()
                    conn.execute(
                        """
                        INSERT INTO quota_windows (
                            peer_id, account_id, window_key, label,
                            remaining_percent, reset_at, source, observed_at,
                            is_current, is_historical, account_alias, plan_type
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(peer_id, account_id, window_key) DO UPDATE SET
                            label = CASE
                                WHEN excluded.observed_at >= quota_windows.observed_at
                                  OR excluded.source = 'manual_report'
                                THEN excluded.label
                                ELSE quota_windows.label
                            END,
                            remaining_percent = CASE
                                WHEN excluded.observed_at >= quota_windows.observed_at
                                  OR excluded.source = 'manual_report'
                                THEN excluded.remaining_percent
                                ELSE quota_windows.remaining_percent
                            END,
                            reset_at = CASE
                                WHEN (excluded.observed_at >= quota_windows.observed_at OR excluded.source = 'manual_report')
                                 AND excluded.reset_at != ''
                                THEN excluded.reset_at
                                ELSE quota_windows.reset_at
                            END,
                            source = CASE
                                WHEN excluded.observed_at >= quota_windows.observed_at
                                  OR excluded.source = 'manual_report'
                                THEN excluded.source
                                ELSE quota_windows.source
                            END,
                            observed_at = CASE
                                WHEN excluded.observed_at >= quota_windows.observed_at
                                  OR excluded.source = 'manual_report'
                                THEN excluded.observed_at
                                ELSE quota_windows.observed_at
                            END,
                            is_current = excluded.is_current,
                            is_historical = excluded.is_historical,
                            account_alias = CASE
                                WHEN excluded.account_alias != '' THEN excluded.account_alias
                                ELSE quota_windows.account_alias
                            END,
                            plan_type = CASE
                                WHEN excluded.plan_type != '' THEN excluded.plan_type
                                ELSE quota_windows.plan_type
                            END
                        """,
                        (
                            peer,
                            account_id,
                            window_key,
                            label,
                            remaining,
                            reset_at,
                            source,
                            observed_at,
                            is_current,
                            is_historical,
                            account_alias,
                            plan_type,
                        ),
                    )
                    keys.append((peer, account_id, window_key))

                now_dt = datetime.now(timezone.utc)
                result: List[Dict[str, Any]] = []
                for peer, account_id, window_key in keys:
                    row = conn.execute(
                        """
                        SELECT * FROM quota_windows
                        WHERE peer_id = ? AND account_id = ? AND window_key = ?
                        """,
                        (peer, account_id, window_key),
                    ).fetchone()
                    if row is not None:
                        result.append(self._decode_quota_row(row, now_dt=now_dt))
        return result

    def list_quota_windows(self, peer_id: Optional[str] = None) -> Dict[str, Any]:
        """Return active-account primary quota posture plus multi-account backup pool (R011 Q4)."""
        target_peers = [validate_peer(peer_id)] if peer_id else list(VALID_PEERS)
        now_dt = datetime.now(timezone.utc)
        with self._connect() as conn:
            if peer_id:
                rows = conn.execute(
                    """
                    SELECT * FROM quota_windows
                    WHERE peer_id = ?
                    ORDER BY account_id ASC, window_key ASC
                    """,
                    (target_peers[0],),
                ).fetchall()
                acct_rows = conn.execute(
                    """
                    SELECT * FROM quota_accounts
                    WHERE peer_id = ?
                    ORDER BY account_alias ASC, account_id ASC
                    """,
                    (target_peers[0],),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM quota_windows ORDER BY peer_id ASC, account_id ASC, window_key ASC"
                ).fetchall()
                acct_rows = conn.execute(
                    "SELECT * FROM quota_accounts ORDER BY peer_id ASC, account_alias ASC, account_id ASC"
                ).fetchall()

        grouped: Dict[str, List[Dict[str, Any]]] = {p: [] for p in target_peers}
        for row in rows:
            decoded = self._decode_quota_row(row, now_dt=now_dt)
            if decoded["peer_id"] in grouped:
                grouped[decoded["peer_id"]].append(decoded)

        reg_accounts: Dict[str, Dict[str, Dict[str, Any]]] = {p: {} for p in target_peers}
        for arow in acct_rows:
            p = arow["peer_id"]
            if p in reg_accounts:
                reg_accounts[p][arow["account_id"]] = _row_to_dict(arow)

        peers_out: List[Dict[str, Any]] = []
        for p in target_peers:
            all_rows = grouped[p]
            reg_map = reg_accounts[p]

            by_acct_all: Dict[str, List[Dict[str, Any]]] = {}
            by_acct_hist: Dict[str, List[Dict[str, Any]]] = {}
            for w in all_rows:
                if w.get("is_historical"):
                    by_acct_hist.setdefault(w["account_id"], []).append(w)
                else:
                    by_acct_all.setdefault(w["account_id"], []).append(w)

            pool_ids: List[str] = list(reg_map.keys())
            for aid in sorted(by_acct_all.keys()):
                if aid not in reg_map:
                    pool_ids.append(aid)

            def _non_expired(wins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                return [
                    w
                    for w in wins
                    if not (w.get("expired_reset") and w["remaining_percent"] <= 0.0)
                ]

            def _is_acct_current(aid: str) -> bool:
                if aid in reg_map:
                    return bool(reg_map[aid].get("is_current"))
                return any(w.get("is_current") for w in by_acct_all.get(aid, []))

            def _acct_alias(aid: str, idx: int) -> str:
                if aid in reg_map and reg_map[aid].get("account_alias"):
                    return str(reg_map[aid]["account_alias"])
                for w in by_acct_all.get(aid, []):
                    if w.get("account_alias"):
                        return str(w["account_alias"])
                if aid == "default":
                    return "默认账号"
                letter = chr(65 + idx) if idx < 26 else str(idx + 1)
                return f"账号 {letter}"

            def _summarize_bucket(
                aid: str, wins: List[Dict[str, Any]], *, alias: str, is_cur: bool, is_hist: bool
            ) -> Dict[str, Any]:
                nw = _non_expired(wins)
                rem_pct = min((w["remaining_percent"] for w in nw), default=None)
                acct_ex = any(w["exhausted"] for w in nw) if nw else False
                acct_low = any(w["low_quota"] for w in nw) if nw else False
                resets = [w["reset_at"] for w in nw if w["reset_at"] and not w.get("expired_reset")]
                earliest_r = min(resets) if resets else ""
                earliest_r_dt = _parse_iso(earliest_r)
                earliest_r_h = (
                    _format_countdown(int((earliest_r_dt - now_dt).total_seconds()))
                    if earliest_r_dt is not None
                    else ""
                )
                ages = [w["sample_age_seconds"] for w in wins if w.get("sample_age_seconds") is not None]
                sample_age = min(ages) if ages else None
                src = (wins or [{}])[0].get("source", "")
                plan = (
                    (reg_map.get(aid) or {}).get("plan_type")
                    or (wins or [{}])[0].get("plan_type", "")
                )
                return {
                    "account_id": aid,
                    "alias": alias,
                    "is_current": is_cur,
                    "is_historical": is_hist,
                    "remaining_percent": rem_pct,
                    "reset_at": earliest_r,
                    "reset_in_human": earliest_r_h,
                    "sample_age_seconds": sample_age,
                    "exhausted": acct_ex,
                    "low_quota": acct_low,
                    "source": src,
                    "plan_type": plan,
                }

            pool_entries: List[Dict[str, Any]] = []
            current_ids: List[str] = []
            for idx, aid in enumerate(pool_ids):
                is_cur = _is_acct_current(aid)
                if is_cur:
                    current_ids.append(aid)
                pool_entries.append(
                    _summarize_bucket(
                        aid,
                        by_acct_all.get(aid, []),
                        alias=_acct_alias(aid, idx),
                        is_cur=is_cur,
                        is_hist=False,
                    )
                )

            hist_ids_clean = [aid for aid in sorted(by_acct_hist.keys()) if aid not in pool_ids]
            historical_entries = [
                _summarize_bucket(
                    h_aid,
                    by_acct_hist[h_aid],
                    alias=f"历史账号 {h_idx + 1}",
                    is_cur=False,
                    is_hist=True,
                )
                for h_idx, h_aid in enumerate(hist_ids_clean)
            ]

            active_acct_id: Optional[str] = None
            current_account_unknown = False
            account_switch_suspected = False

            if current_ids:
                active_acct_id = current_ids[0]
            elif len(pool_ids) == 1:
                only_id = pool_ids[0]
                only_wins = by_acct_all.get(only_id, [])
                has_cockpit = any(str(w.get("source") or "").startswith("cockpit") for w in only_wins)
                if only_wins and (only_id not in reg_map) and not has_cockpit:
                    active_acct_id = only_id
                    if pool_entries:
                        pool_entries[0]["is_current"] = True
                else:
                    current_account_unknown = True
            elif len(pool_ids) > 1:
                current_account_unknown = True

            active_windows: List[Dict[str, Any]] = []
            active_alias = ""
            if active_acct_id is not None:
                active_windows = _non_expired(by_acct_all.get(active_acct_id, []))
                match_pool = [e for e in pool_entries if e["account_id"] == active_acct_id]
                if match_pool:
                    active_alias = match_pool[0]["alias"]
                has_cockpit_active = any(
                    str(w.get("source") or "").startswith("cockpit") for w in active_windows
                )
                if has_cockpit_active:
                    ages = [
                        w["sample_age_seconds"]
                        for w in active_windows
                        if w.get("sample_age_seconds") is not None
                    ]
                    if ages and min(ages) > 1800:
                        account_switch_suspected = True

            if current_account_unknown or account_switch_suspected or not active_windows:
                min_rem: Optional[float] = None
                exhausted = False
                low_quota = False
                earliest_reset = ""
                earliest_human = ""
                dispatch_recommended: Optional[bool] = None
            else:
                min_rem = min(w["remaining_percent"] for w in active_windows)
                exhausted = any(w["exhausted"] for w in active_windows)
                low_quota = any(w["low_quota"] for w in active_windows)
                reset_candidates = [
                    w["reset_at"]
                    for w in active_windows
                    if w["reset_at"] and not w.get("expired_reset")
                ]
                earliest_reset = min(reset_candidates) if reset_candidates else ""
                earliest_dt = _parse_iso(earliest_reset)
                earliest_human = (
                    _format_countdown(int((earliest_dt - now_dt).total_seconds()))
                    if earliest_dt is not None
                    else ""
                )
                dispatch_recommended = (not exhausted) and (min_rem > 5.0)

            backups_with_cap = [
                e
                for e in pool_entries
                if (not e["is_current"])
                and (not e["is_historical"])
                and e["remaining_percent"] is not None
                and e["remaining_percent"] >= 20.0
                and (
                    not str(e.get("source") or "").startswith("cockpit")
                    or (e["sample_age_seconds"] is not None and e["sample_age_seconds"] <= 1800)
                )
            ]
            pool_has_capacity = len(backups_with_cap) > 0
            best_backup = (
                max(backups_with_cap, key=lambda item: float(item["remaining_percent"] or 0.0))
                if backups_with_cap
                else None
            )
            switch_hint = ""
            if best_backup is not None and (
                exhausted
                or low_quota
                or (min_rem is not None and min_rem < 20.0)
                or account_switch_suspected
            ):
                switch_hint = (
                    f"可在 Cockpit 切到 {best_backup['alias']}（{best_backup['remaining_percent']:.0f}%）"
                )

            peers_out.append(
                {
                    "peer_id": p,
                    "active_account_id": active_acct_id or "",
                    "active_account_alias": active_alias,
                    "current_account_unknown": current_account_unknown,
                    "account_switch_suspected": account_switch_suspected,
                    "min_remaining_percent": min_rem,
                    "exhausted": exhausted,
                    "low_quota": low_quota,
                    "dispatch_recommended": dispatch_recommended,
                    "earliest_reset_at": earliest_reset,
                    "earliest_reset_in_human": earliest_human,
                    "windows": active_windows,
                    "pool": pool_entries,
                    "historical_accounts": historical_entries,
                    "pool_has_capacity": pool_has_capacity,
                    "best_backup": best_backup,
                    "switch_hint": switch_hint,
                    "historical_accounts_count": len(historical_entries),
                }
            )
        return {"peers": peers_out, "updated_at": utc_now_iso()}
