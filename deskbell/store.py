"""SQLite WAL truth-layer store for deskbell missions, tasks, peers, and events."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
import sqlite3
from typing import Any, Dict, Iterator, List, Optional
import uuid

from deskbell.models import (
    TERMINAL_TASK_STATUSES,
    VALID_ISOLATION,
    VALID_MODEL_TIERS,
    VALID_PEERS,
    VALID_TASK_STATUSES,
    resolve_home_dir,
    utc_now_iso,
    validate_choice,
    validate_peer,
)

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
    claimed_by_session TEXT NOT NULL DEFAULT '',
    progress_note TEXT NOT NULL DEFAULT '',
    result_summary TEXT NOT NULL DEFAULT '',
    artifacts_json TEXT NOT NULL DEFAULT '[]',
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
    PRIMARY KEY (peer_id, account_id, window_key)
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
    """Transactional SQLite WAL store for deskbell coordination state."""

    def __init__(self, home_dir: Optional[str] = None) -> None:
        self.home_dir = resolve_home_dir(home_dir)
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
        try:
            os.chmod(self.db_path, 0o600)
        except OSError:
            pass

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
        payload_str = json.dumps(payload, ensure_ascii=False)
        cur = conn.execute(
            """
            INSERT INTO events (
                mission_id, task_id, from_peer, to_peer, kind, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (mission_id, task_id, from_peer, to_peer, kind, payload_str, created_at),
        )
        return {
            "event_id": cur.lastrowid,
            "mission_id": mission_id,
            "task_id": task_id,
            "from_peer": from_peer,
            "to_peer": to_peer,
            "kind": kind,
            "payload": payload,
            "created_at": created_at,
            "acked_at": "",
        }

    def _decode_task(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = _row_to_dict(row)
        raw_artifacts = data.pop("artifacts_json", "[]")
        try:
            data["artifacts"] = json.loads(raw_artifacts)
        except ValueError:
            data["artifacts"] = []
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
        """List all known peers plus placeholder entries for uninitialized peers."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM peers ORDER BY peer_id").fetchall()
        by_id = {
            row["peer_id"]: self._decode_peer(row, stale_seconds=stale_seconds)
            for row in rows
        }
        result: List[Dict[str, Any]] = []
        for peer in VALID_PEERS:
            if peer in by_id:
                result.append(by_id[peer])
            else:
                result.append(
                    {
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
    ) -> Dict[str, Any]:
        """Create a mission and optionally dispatch initial tasks atomically."""
        coordinator = validate_peer(coordinator_peer)
        isolation = validate_choice(isolation_policy, VALID_ISOLATION, "isolation_policy")
        if max_rounds < 1:
            raise ValueError("max_rounds must be >= 1")

        mission_id = f"M-{uuid.uuid4().hex[:8].upper()}"
        now = utc_now_iso()
        created_tasks: List[Dict[str, Any]] = []
        created_events: List[Dict[str, Any]] = []

        with self._connect() as conn:
            with self._tx(conn):
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
                    )
                    created_tasks.append(task_obj)
                    created_events.append(event_obj)

                mission_row = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()

        return {
            "mission": _row_to_dict(mission_row),
            "tasks": created_tasks,
            "events": created_events,
        }

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
    ) -> tuple:
        assignee = validate_peer(assignee_peer)
        tier = validate_choice(model_tier, VALID_MODEL_TIERS, "model_tier")
        fanout = max(1, int(parallel_fanout))
        task_id = f"T-{uuid.uuid4().hex[:8].upper()}"

        conn.execute(
            """
            INSERT INTO tasks (
                task_id, mission_id, assignee_peer, model_tier, model_hint,
                parallel_fanout, worktree_branch, prompt, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
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
                now,
                now,
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
    ) -> Dict[str, Any]:
        """Dispatch an additional task on an existing mission."""
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
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
                )
                conn.execute(
                    "UPDATE missions SET updated_at = ? WHERE mission_id = ?",
                    (now, mission_id),
                )
        return {"task": task_obj, "event": event_obj, "mission": _row_to_dict(mission)}

    def count_unacked_events(self, peer_id: str) -> int:
        """Count unacknowledged inbox events for a peer."""
        peer = validate_peer(peer_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM events WHERE to_peer = ? AND acked_at = ''",
                (peer,),
            ).fetchone()
        return int(row["cnt"]) if row else 0

    def pickup_inbox(
        self,
        peer_id: str,
        session_label: str = "",
        claim_tasks: bool = True,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """Atomically fetch/ack unread events and claim queued tasks for peer_id."""
        peer = validate_peer(peer_id)
        now = utc_now_iso()
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
                event_ids = [row["event_id"] for row in event_rows]
                if event_ids:
                    placeholders = ",".join("?" for _ in event_ids)
                    conn.execute(
                        f"UPDATE events SET acked_at = ? WHERE event_id IN ({placeholders})",
                        [now] + event_ids,
                    )

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
                    task_ids = [row["task_id"] for row in queued_rows]
                    if task_ids:
                        placeholders = ",".join("?" for _ in task_ids)
                        conn.execute(
                            f"""
                            UPDATE tasks
                            SET status = 'claimed',
                                claimed_by_session = ?,
                                updated_at = ?
                            WHERE task_id IN ({placeholders})
                              AND status = 'queued'
                            """,
                            [session_label, now] + task_ids,
                        )
                        refreshed = conn.execute(
                            f"SELECT * FROM tasks WHERE task_id IN ({placeholders}) ORDER BY created_at ASC",
                            task_ids,
                        ).fetchall()
                        claimed_tasks = [self._decode_task(r) for r in refreshed]

        return {
            "peer_id": peer,
            "events": [self._decode_event(r) for r in event_rows],
            "claimed_tasks": claimed_tasks,
            "picked_up_at": now,
        }

    def report_progress(
        self,
        task_id: str,
        peer_id: str,
        progress_note: str,
        status: str = "running",
        worktree_branch: Optional[str] = None,
        notify_coordinator: bool = False,
    ) -> Dict[str, Any]:
        """Update task progress and optionally notify the mission coordinator."""
        peer = validate_peer(peer_id)
        normalized_status = validate_choice(status, VALID_TASK_STATUSES, "status")
        now = utc_now_iso()
        emitted_event: Optional[Dict[str, Any]] = None

        with self._connect() as conn:
            with self._tx(conn):
                task = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if task is None:
                    raise KeyError(f"Unknown task_id: {task_id}")
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
                    SET status = ?, progress_note = ?, worktree_branch = ?, updated_at = ?
                    WHERE task_id = ?
                    """,
                    (normalized_status, progress_note, branch_value, now, task_id),
                )
                conn.execute(
                    "UPDATE missions SET updated_at = ? WHERE mission_id = ?",
                    (now, task["mission_id"]),
                )
                if notify_coordinator and mission is not None:
                    emitted_event = self._insert_event(
                        conn,
                        mission_id=task["mission_id"],
                        task_id=task_id,
                        from_peer=peer,
                        to_peer=mission["coordinator_peer"],
                        kind="progress",
                        payload={
                            "task_id": task_id,
                            "status": normalized_status,
                            "progress_note": progress_note,
                            "worktree_branch": branch_value,
                        },
                        created_at=now,
                    )
                updated_row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()

        return {
            "task": self._decode_task(updated_row),
            "event": emitted_event,
            "coordinator_peer": mission["coordinator_peer"] if mission else "",
        }

    def complete_task(
        self,
        task_id: str,
        peer_id: str,
        result_summary: str,
        artifacts: Optional[List[str]] = None,
        status: str = "done",
    ) -> Dict[str, Any]:
        """Mark a task terminal, check mission completion, and notify coordinator."""
        peer = validate_peer(peer_id)
        terminal_status = validate_choice(status, TERMINAL_TASK_STATUSES, "terminal status")
        now = utc_now_iso()
        artifacts_json = json.dumps(artifacts or [], ensure_ascii=False)

        with self._connect() as conn:
            with self._tx(conn):
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

                conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, result_summary = ?, artifacts_json = ?, updated_at = ?
                    WHERE task_id = ?
                    """,
                    (terminal_status, result_summary, artifacts_json, now, task_id),
                )
                remaining_row = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM tasks
                    WHERE mission_id = ? AND status NOT IN ('done', 'failed', 'blocked')
                    """,
                    (task["mission_id"],),
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
                        "status": terminal_status,
                        "result_summary": result_summary,
                        "artifacts": artifacts or [],
                        "all_tasks_finished": all_finished,
                    },
                    created_at=now,
                )
                updated_row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()

        return {
            "task": self._decode_task(updated_row),
            "event": emitted_event,
            "coordinator_peer": mission["coordinator_peer"],
            "all_tasks_finished": all_finished,
        }

    def request_review(
        self,
        mission_id: str,
        from_peer: str,
        to_peer: str,
        summary: str,
        question: str,
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Request a cross-peer review while atomically enforcing max_rounds loop guard."""
        sender = validate_peer(from_peer)
        recipient = validate_peer(to_peer)
        now = utc_now_iso()

        with self._connect() as conn:
            with self._tx(conn):
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")

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
                    return {
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

        return {
            "allowed": True,
            "round": new_round,
            "max_rounds": max_rounds,
            "to_peer": recipient,
            "event": review_event,
        }

    def close_mission(
        self,
        mission_id: str,
        final_summary: str,
        status: str = "completed",
    ) -> Dict[str, Any]:
        """Record the coordinator's synthesized final summary and close a mission."""
        now = utc_now_iso()
        with self._connect() as conn:
            with self._tx(conn):
                mission = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
                if mission is None:
                    raise KeyError(f"Unknown mission_id: {mission_id}")
                conn.execute(
                    """
                    UPDATE missions
                    SET final_summary = ?, status = ?, updated_at = ?
                    WHERE mission_id = ?
                    """,
                    (final_summary, status, now, mission_id),
                )
                updated = conn.execute(
                    "SELECT * FROM missions WHERE mission_id = ?", (mission_id,)
                ).fetchone()
        return _row_to_dict(updated)

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
        data["exhausted"] = remaining <= 0.0
        data["low_quota"] = remaining <= 15.0
        return data

    def upsert_quota_windows(
        self, windows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Insert or update quota window snapshots inside an immediate transaction."""
        if not windows:
            return []
        now = utc_now_iso()
        keys: List[tuple] = []
        with self._connect() as conn:
            with self._tx(conn):
                for item in windows:
                    peer = validate_peer(str(item["peer_id"]))
                    account_id = str(item.get("account_id") or "default").strip() or "default"
                    window_key = str(item["window_key"]).strip()
                    if not window_key:
                        raise ValueError("window_key must not be empty")
                    label = str(item.get("label") or window_key).strip()
                    remaining = max(0.0, min(100.0, float(item["remaining_percent"])))
                    reset_at = str(item.get("reset_at") or "").strip()
                    source = str(item.get("source") or "manual").strip()
                    observed_at = str(item.get("observed_at") or now).strip()
                    conn.execute(
                        """
                        INSERT INTO quota_windows (
                            peer_id, account_id, window_key, label,
                            remaining_percent, reset_at, source, observed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(peer_id, account_id, window_key) DO UPDATE SET
                            label = excluded.label,
                            remaining_percent = excluded.remaining_percent,
                            reset_at = CASE
                                WHEN excluded.reset_at != '' THEN excluded.reset_at
                                ELSE quota_windows.reset_at
                            END,
                            source = excluded.source,
                            observed_at = excluded.observed_at
                        WHERE excluded.observed_at >= quota_windows.observed_at
                           OR excluded.source = 'manual_report'
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
        """Return per-peer quota posture and individual reset windows."""
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
            else:
                rows = conn.execute(
                    "SELECT * FROM quota_windows ORDER BY peer_id ASC, account_id ASC, window_key ASC"
                ).fetchall()

        grouped: Dict[str, List[Dict[str, Any]]] = {p: [] for p in target_peers}
        for row in rows:
            decoded = self._decode_quota_row(row, now_dt=now_dt)
            if decoded["peer_id"] in grouped:
                grouped[decoded["peer_id"]].append(decoded)

        peers_out: List[Dict[str, Any]] = []
        for p in target_peers:
            windows = grouped[p]
            if windows:
                min_rem: Optional[float] = min(w["remaining_percent"] for w in windows)
                exhausted = any(w["exhausted"] for w in windows)
                low_quota = any(w["low_quota"] for w in windows)
                reset_candidates = [w["reset_at"] for w in windows if w["reset_at"]]
                earliest_reset = min(reset_candidates) if reset_candidates else ""
                earliest_dt = _parse_iso(earliest_reset)
                earliest_human = (
                    _format_countdown(int((earliest_dt - now_dt).total_seconds()))
                    if earliest_dt is not None
                    else ""
                )
            else:
                min_rem = None
                exhausted = False
                low_quota = False
                earliest_reset = ""
                earliest_human = ""

            dispatch_recommended = (not exhausted) and (min_rem is None or min_rem > 5.0)
            peers_out.append(
                {
                    "peer_id": p,
                    "min_remaining_percent": min_rem,
                    "exhausted": exhausted,
                    "low_quota": low_quota,
                    "dispatch_recommended": dispatch_recommended,
                    "earliest_reset_at": earliest_reset,
                    "earliest_reset_in_human": earliest_human,
                    "windows": windows,
                }
            )
        return {"peers": peers_out, "updated_at": utc_now_iso()}
