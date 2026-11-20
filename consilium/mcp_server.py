"""Dual-transport MCP server (stdio + hardened 127.0.0.1 Streamable HTTP)."""

from __future__ import annotations

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import queue
import socket
import sys
import threading
import time
import urllib.parse
from typing import Any, Callable, Dict, List, Optional, Tuple

from consilium import __version__
from consilium.dashboard import (
    SSE_MAX_SUBSCRIBERS,
    SESSION_COOKIE_NAME,
    EventHub,
    LoginTicketStore,
    SessionRecord,
    SessionStore,
    build_dashboard_state,
    compute_state_fingerprint,
    parse_cookie_header,
    render_dashboard_html,
)
from consilium.models import (
    ConflictError,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_REAPER_INTERVAL_SEC,
    VALID_ISOLATION,
    VALID_MISSION_CLOSE_STATUSES,
    VALID_MODEL_TIERS,
    VALID_PEERS,
    PICKUP_SCOPES,
    USER_PEER,
    ensure_auth_token,
    read_auth_token,
    resolve_rendezvous_dir,
    resolve_state_dir,
    utc_now_iso,
)
from consilium.quota import sync_local_quotas
from consilium.store import BusStore
from consilium.wakeup import (
    WakeBrokerServer,
    query_active_waiters,
    rebuild_all_peer_seqs,
    ring_peer,
    wait_for_doorbell,
)

SUPPORTED_PROTOCOL_VERSION = "2025-03-26"
SUPPORTED_PROTOCOL_VERSIONS = ("2025-03-26", "2024-11-05")
LOCAL_HOSTS = ("127.0.0.1", "localhost", "::1")
HTTP_READ_TIMEOUT_SEC = 30.0


def _parse_bool(args: Dict[str, Any], key: str, default: bool) -> bool:
    """Strictly parse a boolean tool argument; reject strings like 'false'."""
    if key not in args or args[key] is None:
        return default
    val = args[key]
    if not isinstance(val, bool):
        raise ValueError(f"Parameter {key!r} must be a boolean, got {type(val).__name__}")
    return val


def _safe_ring_peer(**kwargs: Any) -> Dict[str, Any]:
    """Invoke ring_peer without letting notification errors fail an already-committed DB write."""
    peer_id = str(kwargs.get("peer_id", ""))
    if peer_id == USER_PEER:
        return {"delivery": "ui", "peer_id": USER_PEER}
    store = kwargs.get("store")
    event_ids = kwargs.get("event_ids")
    if store is not None and event_ids and kwargs.get("on_escalate") is None:
        tracked_ids = list(event_ids)
        kwargs["on_escalate"] = lambda cold_res: _safe_record_event_delivery(
            store, tracked_ids, cold_res
        )
    try:
        return ring_peer(**kwargs)
    except Exception as exc:
        return {
            "delivery": "error",
            "peer_id": peer_id,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _safe_save_idempotency_delivery(
    store: BusStore,
    client_request_id: Optional[str],
    delivery: Dict[str, Any],
) -> Optional[str]:
    """Best-effort persistence of post-commit delivery receipt onto idempotency row."""
    req_id = (str(client_request_id) if client_request_id is not None else "").strip()
    if not req_id:
        return None
    try:
        store.save_idempotency_delivery(req_id, delivery)
        return None
    except Exception as exc:
        return f"save_idempotency_delivery({req_id}): {type(exc).__name__}: {exc}"


def _replay_delivery_payload(
    res: Dict[str, Any], *, multi: bool = False
) -> Dict[str, Any]:
    """Return the originally persisted delivery receipt on idempotent replay when present."""
    cached = res.pop("_cached_delivery", None)
    if isinstance(cached, dict):
        out = dict(cached)
        out["idempotent_replay"] = True
        return out
    out = {"delivery": "replayed", "idempotent_replay": True}
    if multi:
        out["deliveries"] = []
    return out


def _safe_record_event_delivery(
    store: BusStore,
    event_ids: List[int],
    delivery: Dict[str, Any],
    **kwargs: Any,
) -> Optional[str]:
    """Best-effort DB attachment of delivery receipt; returns warning string instead of raising."""
    if not event_ids:
        return None
    try:
        store.record_event_delivery(event_ids, delivery, **kwargs)
        return None
    except Exception as exc:
        return f"record_event_delivery({event_ids}): {type(exc).__name__}: {exc}"


def _collect_post_commit_warnings(
    store_warnings: Optional[List[str]],
    delivery_obj: Any,
    extra_warnings: Optional[List[str]] = None,
) -> List[str]:
    warnings: List[str] = []
    seen = set()

    def _add(item: Any) -> None:
        text = str(item or "").strip()
        if text and text not in seen:
            seen.add(text)
            warnings.append(text)

    for w in (store_warnings or []):
        _add(w)
    if isinstance(delivery_obj, dict):
        sub_errors = [
            sub["error"]
            for sub in (delivery_obj.get("deliveries") or [])
            if isinstance(sub, dict) and sub.get("error")
        ]
        if sub_errors:
            for err in sub_errors:
                _add(err)
        elif delivery_obj.get("error"):
            _add(delivery_obj["error"])
    for w in (extra_warnings or []):
        _add(w)
    return warnings


def _deliver_reaper_notifications(
    store: BusStore,
    rendezvous_dir: str,
    released: List[Dict[str, Any]],
) -> List[str]:
    """Notify coordinators and assignees for tasks handled by release_stale_claims."""
    warnings: List[str] = []
    for item in released:
        action = item.get("reaper_action") or "requeued"
        coord = str(item.get("coordinator_peer") or "")
        assignee = str(item.get("assignee_peer") or "")
        task_id = str(item.get("task_id") or "")
        events = list(item.get("events") or [])
        if item.get("post_commit_warnings"):
            warnings.extend(str(w) for w in item["post_commit_warnings"] if w)

        if action == "requeued":
            for evt in events:
                eid = int(evt["event_id"])
                ring_ms = time.time() * 1000.0
                receipt = _safe_ring_peer(
                    store=store,
                    home_dir=rendezvous_dir,
                    peer_id=evt["to_peer"],
                    title="Consilium Task Requeued",
                    body=f"Task {task_id} claim timed out and returned to queued",
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={evt['to_peer']!r}) "
                        f"to handle requeued task {task_id}."
                    ),
                    event_ids=[eid],
                )
                w = _safe_record_event_delivery(store, [eid], receipt, ring_sent_at_ms=ring_ms)
                if w:
                    warnings.append(w)
            if assignee and assignee != coord:
                # Also wake the assignee whose .seq queued_tasks increased on requeue.
                _safe_ring_peer(
                    store=store,
                    home_dir=rendezvous_dir,
                    peer_id=assignee,
                    title="Consilium Task Available",
                    body=f"Task {task_id} returned to queued",
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={assignee!r}) "
                        f"to re-claim task {task_id}."
                    ),
                )
        elif action == "marked_stale":
            for evt in events:
                eid = int(evt["event_id"])
                target = str(evt.get("to_peer") or "")
                if not target:
                    continue
                ring_ms = time.time() * 1000.0
                receipt = _safe_ring_peer(
                    store=store,
                    home_dir=rendezvous_dir,
                    peer_id=target,
                    title="Consilium Running Task Stale",
                    body=f"Task {task_id} exceeded heartbeat threshold; awaiting coordinator requeue",
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={target!r}) "
                        f"to check stale running task {task_id}."
                    ),
                    event_ids=[eid],
                )
                w = _safe_record_event_delivery(store, [eid], receipt, ring_sent_at_ms=ring_ms)
                if w:
                    warnings.append(w)
    return warnings

PEER_LABELS = {"claude": "Claude", "chatgpt": "Codex", "antigravity": "Antigravity", USER_PEER: "使用者"}


_PATH_IN_TEXT_RE = re.compile(r"(?:~|/(?:Users|Volumes|private|opt|tmp))/[^\s，。、；：！？'\"`（）()\[\]<>]+")


def detect_workspace(text: str, explicit: str = "") -> str:
    """Pick a conversation folder: an explicit choice, else the first existing absolute path in the message."""
    candidates = [explicit] if explicit else []
    candidates += _PATH_IN_TEXT_RE.findall(text or "")
    for raw in candidates:
        cand = os.path.realpath(os.path.expanduser(str(raw).strip().rstrip(".,;:")))
        if os.path.isdir(cand):
            return cand
        if os.path.isfile(cand):
            return os.path.dirname(cand)
    return ""


def peer_quota_digest(store: BusStore, rendezvous_dir: str) -> List[Dict[str, Any]]:
    """Compact per-peer standby + current-account quota view for routing and handoff decisions."""
    waiters = query_active_waiters(rendezvous_dir) or {}
    quotas = {q["peer_id"]: q for q in store.list_quota_windows()["peers"]}
    out: List[Dict[str, Any]] = []
    for pid in ("claude", "chatgpt", "antigravity"):
        q = quotas.get(pid) or {}
        unknown = bool(q.get("current_account_unknown") or q.get("account_switch_suspected")) or (
            q.get("min_remaining_percent") is None
        )
        out.append(
            {
                "peer_id": pid,
                "label": PEER_LABELS[pid],
                "standby": int(waiters.get(pid, 0) or 0) > 0,
                "remaining_percent": None if unknown else q.get("min_remaining_percent"),
                "exhausted": bool(q.get("exhausted")) and not unknown,
                "switch_hint": q.get("switch_hint") or "",
            }
        )
    return out


def choose_auto_coordinator(digest: List[Dict[str, Any]]) -> str:
    """Pick the peer that should coordinate an un-addressed message: standby first, then quota."""
    best, best_score = "claude", None
    for rank, item in enumerate(digest):
        if item["exhausted"]:
            continue
        rem = item["remaining_percent"]
        score = (2 if item["standby"] else 0) + (1 if rem is None or rem >= 20 else 0) - rank * 0.01
        if best_score is None or score > best_score:
            best, best_score = item["peer_id"], score
    return best


def explain_auto_choice(digest: List[Dict[str, Any]], chosen: str) -> str:
    """One plain sentence on why auto routing picked `chosen`, shown under the user's message."""
    item = next((d for d in digest if d["peer_id"] == chosen), None) or {}
    label = PEER_LABELS.get(chosen, chosen)
    rem = item.get("remaining_percent")
    rem_txt = "余量未知" if rem is None else f"余量 {round(float(rem))}%"
    skipped = [PEER_LABELS.get(d["peer_id"], d["peer_id"]) for d in digest if d.get("exhausted")]
    if item.get("standby"):
        why = f"交给 {label}：它正在待命，{rem_txt}"
    elif any(d.get("standby") for d in digest):
        why = f"交给 {label}：{rem_txt}"
    else:
        why = f"三端都没在待命，先排给 {label}（默认协调者），它上线后处理"
    if skipped:
        why += f"；{'、'.join(skipped)} 余量已耗尽，跳过"
    return why


def _quota_line(item: Dict[str, Any]) -> str:
    rem = item["remaining_percent"]
    rem_txt = "余量未知" if rem is None else f"余量 {round(float(rem))}%"
    state = "待命中" if item["standby"] else "未待命"
    return f"- {item['label']}（{item['peer_id']}）：{state}，{rem_txt}" + ("，已耗尽" if item["exhausted"] else "")


def build_auto_coordinator_prompt(
    text: str, coordinator: str, digest: List[Dict[str, Any]], preferences: List[Dict[str, Any]],
    routing: List[Dict[str, Any]],
) -> str:
    pref_lines = "\n".join(f"- {p['text']}" for p in preferences) or "- （还没有记录）"
    route_lines = "\n".join(
        f"- 「{r['text'][:60]}」→ {', '.join(PEER_LABELS.get(t, t) for t in r['targets'])}" for r in routing[:8]
    ) or "- （还没有记录）"
    return (
        f"{text}\n\n"
        "———— 以上是使用者的原话 ————\n"
        f"你是这次会诊的协调者（使用者没有点名，由你自动分配）。\n"
        "1. 先判断自己做还是拆分：需要另一端时，用 consilium_dispatch_task 把子任务派给它（chatgpt=Codex，antigravity=Antigravity，claude=Claude），"
        "优先派给待命中、余量充足的一端；余量未知可以照常派，已耗尽的不要派。\n"
        "2. 收齐结果后，用 consilium_complete_task 交付你自己的这个任务：result_summary 直接写给使用者看的最终回答。\n"
        "3. 如果这次看出使用者稳定的习惯（例如某类事固定交给谁、喜欢的回答方式），用 consilium_learn_preference 记一句话；不确定就不记。\n"
        "4. 你自己的额度快用完时，用 consilium_handoff_task 把任务连同进度交给另一端。切换账号由使用者手动完成，需要时在回答里提醒。\n"
        "5. 用 consilium_set_title 给会诊起一个不超过 20 字的标题。会诊的文件夹由使用者选定（见 workspace_path），在里面工作；派子任务时写明这个文件夹。"
        "上报进度时用 model_used 写上你实际使用的模型。\n\n"
        f"【各端状态】\n" + "\n".join(_quota_line(i) for i in digest) + "\n\n"
        f"【使用者偏好】\n{pref_lines}\n\n"
        f"【使用者最近的点名分配】\n{route_lines}"
    )


TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "consilium_status",
        "description": "Inspect peer standby states, active socket waiters, and missions/tasks.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "mission_id": {"type": "string", "description": "Optional mission ID for full details."}
            },
        },
    },
    {
        "name": "consilium_create_mission",
        "description": "Create a collaboration mission, dispatch initial tasks, and ring target desktop peers.",
        "inputSchema": {
            "type": "object",
            "required": ["title", "goal", "coordinator_peer"],
            "properties": {
                "title": {"type": "string"},
                "goal": {"type": "string"},
                "coordinator_peer": {"type": "string", "enum": list(VALID_PEERS)},
                "workspace_path": {"type": "string"},
                "isolation_policy": {"type": "string", "enum": list(VALID_ISOLATION), "default": "worktree"},
                "max_rounds": {"type": "integer", "minimum": 1, "default": 3},
                "open_deeplink": {"type": "boolean", "default": False},
                "client_request_id": {"type": "string"},
                "tasks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["assignee_peer", "prompt"],
                        "properties": {
                            "assignee_peer": {"type": "string", "enum": list(VALID_PEERS)},
                            "prompt": {"type": "string"},
                            "model_tier": {"type": "string", "enum": list(VALID_MODEL_TIERS), "default": "balanced"},
                            "model_hint": {"type": "string"},
                            "parallel_fanout": {"type": "integer", "minimum": 1, "default": 1},
                            "worktree_branch": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
    {
        "name": "consilium_dispatch_task",
        "description": "Add a task to an existing mission and ring the assignee peer.",
        "inputSchema": {
            "type": "object",
            "required": ["mission_id", "assignee_peer", "prompt"],
            "properties": {
                "mission_id": {"type": "string"},
                "assignee_peer": {"type": "string", "enum": list(VALID_PEERS)},
                "prompt": {"type": "string"},
                "model_tier": {"type": "string", "enum": list(VALID_MODEL_TIERS), "default": "balanced"},
                "model_hint": {"type": "string"},
                "parallel_fanout": {"type": "integer", "minimum": 1, "default": 1},
                "worktree_branch": {"type": "string"},
                "open_deeplink": {"type": "boolean", "default": False},
                "client_request_id": {"type": "string"},
            },
        },
    },
    {
        "name": "consilium_pickup_inbox",
        "description": (
            "Atomically acknowledge unread events and claim queued tasks assigned to peer_id. "
            "Keep work from different places apart: a standby session passes scope='consilium' "
            "(only conversations the user started in the Consilium window); a session collecting "
            "results for a mission it created passes mission_id. Anything outside the filter stays "
            "unread for the session it belongs to."
        ),
        "inputSchema": {
            "type": "object",
            "required": ["peer_id"],
            "properties": {
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "session_label": {"type": "string"},
                "scope": {
                    "type": "string",
                    "enum": list(PICKUP_SCOPES),
                    "description": "consilium = started in the Consilium window; other = created by a desktop session; all = both. Defaults to consilium when session_label ends with 'standby', otherwise all.",
                },
                "mission_id": {"type": "string", "description": "Only this conversation's events and tasks."},
                "claim_tasks": {"type": "boolean", "default": True},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
            },
        },
    },
    {
        "name": "consilium_report_progress",
        "description": "Update claimed/running task status/progress (requires active attempt_id) and optionally ring coordinator.",
        "inputSchema": {
            "type": "object",
            "required": ["task_id", "peer_id", "progress_note", "attempt_id"],
            "properties": {
                "task_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "attempt_id": {"type": "string"},
                "progress_note": {"type": "string"},
                "status": {"type": "string", "enum": ["claimed", "running"], "default": "running"},
                "model_used": {"type": "string", "maxLength": 80, "description": "The model you are actually running on (e.g. 'Claude Opus 5.5'); shown in the Consilium sidebar."},
                "worktree_branch": {"type": "string"},
                "notify_coordinator": {"type": "boolean", "default": False},
                "client_request_id": {"type": "string"},
            },
        },
    },
    {
        "name": "consilium_complete_task",
        "description": "Mark a claimed/running task terminal (requires active attempt_id), record summary/artifacts, and ring coordinator.",
        "inputSchema": {
            "type": "object",
            "required": ["task_id", "peer_id", "result_summary", "attempt_id"],
            "properties": {
                "task_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "attempt_id": {"type": "string"},
                "result_summary": {"type": "string"},
                "artifacts": {"type": "array", "items": {"type": "string"}},
                "status": {"type": "string", "enum": ["done", "failed", "blocked"], "default": "done"},
                "model_used": {"type": "string", "maxLength": 80},
                "client_request_id": {"type": "string"},
            },
        },
    },
    {
        "name": "consilium_requeue_task",
        "description": "Coordinator-only: archive current attempt and return a claimed, running, failed, or blocked task to queued.",
        "inputSchema": {
            "type": "object",
            "required": ["task_id", "peer_id"],
            "properties": {
                "task_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "reason": {"type": "string"},
                "client_request_id": {"type": "string"},
            },
        },
    },
    {
        "name": "consilium_preferences",
        "description": "Read the user's learned preferences, recent explicit routing choices, and each peer's standby/quota digest before deciding who should do what.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "consilium_bind_session",
        "description": "Record which of your native sessions handles this conversation (e.g. the Claude Code session id you started). Later messages in the conversation carry it as native_session so they can be forwarded there.",
        "inputSchema": {
            "type": "object",
            "required": ["mission_id", "peer_id", "session_ref"],
            "properties": {
                "mission_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "session_ref": {"type": "string", "maxLength": 200},
            },
        },
    },
    {
        "name": "consilium_set_title",
        "description": "Give the conversation a short title (max 40 chars). Name your own native session the same way so the user can find it.",
        "inputSchema": {
            "type": "object",
            "required": ["mission_id", "peer_id", "title"],
            "properties": {
                "mission_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "title": {"type": "string", "maxLength": 40},
            },
        },
    },
    {
        "name": "consilium_set_workspace",
        "description": "Record the project folder (absolute path) this conversation works in, so every peer and the user's sidebar see it.",
        "inputSchema": {
            "type": "object",
            "required": ["mission_id", "peer_id", "path"],
            "properties": {
                "mission_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "path": {"type": "string"},
            },
        },
    },
    {
        "name": "consilium_learn_preference",
        "description": "Record one short, stable preference of the user (e.g. 'code review goes to Codex'). The user can see and delete it. Do not record guesses.",
        "inputSchema": {
            "type": "object",
            "required": ["peer_id", "text"],
            "properties": {
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "text": {"type": "string", "maxLength": 300},
            },
        },
    },
    {
        "name": "consilium_handoff_task",
        "description": "Hand your active task (with the current attempt_id) to another peer, e.g. when your quota is low. The note must say what is done so far and why.",
        "inputSchema": {
            "type": "object",
            "required": ["task_id", "peer_id", "attempt_id", "to_peer", "note"],
            "properties": {
                "task_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "attempt_id": {"type": "string"},
                "to_peer": {"type": "string", "enum": list(VALID_PEERS)},
                "note": {"type": "string"},
            },
        },
    },
    {
        "name": "consilium_request_review",
        "description": "Request cross-peer review while enforcing the mission max_rounds loop guard.",
        "inputSchema": {
            "type": "object",
            "required": ["mission_id", "from_peer", "to_peer", "summary", "question"],
            "properties": {
                "mission_id": {"type": "string"},
                "from_peer": {"type": "string", "enum": list(VALID_PEERS)},
                "to_peer": {"type": "string", "enum": list(VALID_PEERS)},
                "summary": {"type": "string"},
                "question": {"type": "string"},
                "task_id": {"type": "string"},
                "client_request_id": {"type": "string"},
            },
        },
    },
    {
        "name": "consilium_wait",
        "description": "Read-only silent wait until a doorbell event arrives or timeout expires (max 300s). Never auto-claims; call consilium_pickup_inbox explicitly.",
        "inputSchema": {
            "type": "object",
            "required": ["peer_id"],
            "properties": {
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "timeout_sec": {"type": "number", "minimum": 0.1, "maximum": 300, "default": 45},
                "session_label": {"type": "string"},
                "auto_pickup": {"type": "boolean", "default": False, "description": "Deprecated and ignored; wait is always read-only."},
            },
        },
    },
    {
        "name": "consilium_quota",
        "description": "Query or report per-peer remaining account quota percentages and reset times.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "sync_local": {"type": "boolean", "default": True},
                "user_home": {"type": "string"},
                "report_windows": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["peer_id", "window_key", "remaining_percent"],
                        "properties": {
                            "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                            "account_id": {"type": "string"},
                            "window_key": {"type": "string"},
                            "label": {"type": "string"},
                            "source": {"type": "string", "enum": ["manual_report", "demo"]},
                            "remaining_percent": {"type": "number", "minimum": 0, "maximum": 100},
                            "reset_at": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
    {
        "name": "consilium_close_mission",
        "description": "Record final summary and close a mission (coordinator only; validates open tasks unless force=true).",
        "inputSchema": {
            "type": "object",
            "required": ["mission_id", "peer_id", "final_summary"],
            "properties": {
                "mission_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "final_summary": {"type": "string"},
                "status": {
                    "type": "string",
                    "enum": list(VALID_MISSION_CLOSE_STATUSES),
                    "default": "completed",
                },
                "force": {"type": "boolean", "default": False},
                "client_request_id": {"type": "string"},
            },
        },
    },
]


class ConsiliumMCPDispatcher:
    """Routes MCP JSON-RPC 2.0 requests to BusStore and wakeup doorbell logic."""

    def __init__(
        self,
        home_dir: Optional[str] = None,
        default_peer: str = "",
        rendezvous_dir: Optional[str] = None,
        on_mutation: Optional[Callable[[str], None]] = None,
        claim_timeout_sec: Optional[int] = None,
        running_timeout_sec: Optional[int] = None,
        on_reaper_tasks: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    ) -> None:
        self.home_dir = resolve_state_dir(home_dir)
        self.rendezvous_dir = resolve_rendezvous_dir(rendezvous_dir, fallback_dir=home_dir)
        self.default_peer = default_peer
        self.store = BusStore(self.home_dir, rendezvous_dir=self.rendezvous_dir)
        self._on_mutation = on_mutation
        self.claim_timeout_sec = claim_timeout_sec
        self.running_timeout_sec = running_timeout_sec
        self._on_reaper_tasks = on_reaper_tasks

    def _emit_mutation(self, reason: str) -> Optional[str]:
        if self._on_mutation is not None:
            try:
                self._on_mutation(reason)
            except Exception as exc:
                return f"publish_state_update: {type(exc).__name__}: {exc}"
        return None

    def handle_payload(self, payload: Any) -> Optional[Any]:
        """Handle a single JSON-RPC object or batch; return None for pure notifications."""
        if isinstance(payload, list):
            if not payload:
                return self._error(None, -32600, "Invalid Request: empty batch")
            responses = []
            for item in payload:
                if not isinstance(item, dict):
                    responses.append(
                        self._error(None, -32600, "Invalid Request: batch element must be an object")
                    )
                else:
                    responses.append(self._handle_one(item))
            filtered = [r for r in responses if r is not None]
            return filtered or None
        if not isinstance(payload, dict):
            return self._error(None, -32600, "Invalid Request")
        return self._handle_one(payload)

    def _handle_one(self, req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        req_id = req.get("id")
        method = req.get("method")
        raw_params = req.get("params")
        if raw_params is not None and not isinstance(raw_params, dict):
            return self._error(req_id, -32602, "Invalid params: expected object")
        params = raw_params or {}

        if not isinstance(method, str):
            return self._error(req_id, -32600, "Invalid Request: missing method")

        if method == "notifications/initialized" or method.startswith("notifications/"):
            return None

        if method == "initialize":
            requested_proto = str(params.get("protocolVersion") or SUPPORTED_PROTOCOL_VERSION)
            negotiated = (
                requested_proto
                if requested_proto in SUPPORTED_PROTOCOL_VERSIONS
                else SUPPORTED_PROTOCOL_VERSION
            )
            return self._result(
                req_id,
                {
                    "protocolVersion": negotiated,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "consilium", "version": __version__},
                },
            )

        if method == "ping":
            return self._result(req_id, {})

        if method == "tools/list":
            return self._result(req_id, {"tools": TOOL_DEFINITIONS})

        if method == "tools/call":
            return self._handle_tool_call(req_id, params)

        if req_id is None:
            return None
        return self._error(req_id, -32601, f"Method not found: {method}")

    def _handle_tool_call(self, req_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        name = params.get("name")
        args = params.get("arguments") or {}
        if not isinstance(name, str) or not isinstance(args, dict):
            return self._error(req_id, -32602, "Invalid params for tools/call")

        try:
            output = self._dispatch_tool(name, args)
        except KeyError as exc:
            return self._tool_text_result(req_id, {"error": str(exc)}, is_error=True)
        except ValueError as exc:
            return self._tool_text_result(req_id, {"error": str(exc)}, is_error=True)
        except Exception as exc:
            return self._tool_text_result(
                req_id, {"error": f"{type(exc).__name__}: {exc}"}, is_error=True
            )
        return self._tool_text_result(req_id, output, is_error=False)

    def _dispatch_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if name == "consilium_status":
            mission_id = args.get("mission_id")
            try:
                sync_local_quotas(self.store)
            except Exception:
                pass
            snapshot = self.store.get_mission_snapshot(mission_id=mission_id)
            return {
                "peers": self.store.list_peers(),
                "active_waiters": query_active_waiters(self.rendezvous_dir),
                "quotas": self.store.list_quota_windows()["peers"],
                **snapshot,
            }

        if name == "consilium_create_mission":
            open_deeplink = _parse_bool(args, "open_deeplink", False)
            client_req_id = args.get("client_request_id")
            created = self.store.create_mission(
                title=str(args["title"]),
                goal=str(args["goal"]),
                coordinator_peer=str(args["coordinator_peer"]),
                workspace_path=str(args.get("workspace_path", "")),
                isolation_policy=str(args.get("isolation_policy", "worktree")),
                max_rounds=int(args.get("max_rounds", 3)),
                tasks=args.get("tasks") or [],
                client_request_id=str(client_req_id) if client_req_id is not None else None,
            )
            if created.get("idempotent_replay"):
                replay_delivery = _replay_delivery_payload(created, multi=True)
                return {
                    **created,
                    "committed": True,
                    "result": created,
                    "delivery": replay_delivery,
                    "deliveries": replay_delivery.get("deliveries", []),
                }
            mission = created["mission"]
            tasks_by_peer: Dict[str, List[Dict[str, Any]]] = {}
            events_by_peer: Dict[str, List[int]] = {}
            for task in created["tasks"]:
                tasks_by_peer.setdefault(task["assignee_peer"], []).append(task)
            for evt in created["events"]:
                events_by_peer.setdefault(evt["to_peer"], []).append(int(evt["event_id"]))
            deliveries = []
            rec_warnings: List[str] = []
            for assignee, peer_tasks in tasks_by_peer.items():
                task_ids = ", ".join(t["task_id"] for t in peer_tasks)
                tiers = ", ".join(sorted({t["model_tier"] for t in peer_tasks}))
                peer_eids = events_by_peer.get(assignee, [])
                ring_ms = time.time() * 1000.0
                receipt = _safe_ring_peer(
                    store=self.store,
                    home_dir=self.rendezvous_dir,
                    peer_id=assignee,
                    title=f"Consilium: {mission['title']}",
                    body=f"New task(s) {task_ids} ({tiers})",
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={assignee!r}) "
                        f"to claim task(s) {task_ids} in mission {mission['mission_id']}."
                    ),
                    workspace_path=mission.get("workspace_path", ""),
                    open_deeplink=open_deeplink,
                    event_ids=peer_eids,
                )
                rw = _safe_record_event_delivery(
                    self.store, peer_eids, receipt, ring_sent_at_ms=ring_ms
                )
                if rw:
                    rec_warnings.append(rw)
                deliveries.append(receipt)
            pub_warn = self._emit_mutation("create_mission")
            if pub_warn:
                rec_warnings.append(pub_warn)
            delivery_summary: Dict[str, Any] = {
                "deliveries": deliveries,
                "errors": [d for d in deliveries if d.get("delivery") == "error"],
            }
            if delivery_summary["errors"]:
                delivery_summary["error"] = "; ".join(
                    str(d.get("error", "")) for d in delivery_summary["errors"]
                )
            all_warnings = _collect_post_commit_warnings(
                created.get("post_commit_warnings"), delivery_summary, rec_warnings
            )
            if all_warnings:
                delivery_summary["warnings"] = all_warnings
            _safe_save_idempotency_delivery(self.store, client_req_id, delivery_summary)
            return {
                **created,
                "committed": True,
                "result": created,
                "delivery": delivery_summary,
                "deliveries": deliveries,
            }

        if name == "consilium_dispatch_task":
            open_deeplink = _parse_bool(args, "open_deeplink", False)
            client_req_id = args.get("client_request_id")
            dispatched = self.store.dispatch_task(
                mission_id=str(args["mission_id"]),
                assignee_peer=str(args["assignee_peer"]),
                prompt=str(args["prompt"]),
                model_tier=str(args.get("model_tier", "balanced")),
                model_hint=str(args.get("model_hint", "")),
                parallel_fanout=int(args.get("parallel_fanout", 1)),
                worktree_branch=str(args.get("worktree_branch", "")),
                client_request_id=str(client_req_id) if client_req_id is not None else None,
                caller_scope=(f"peer:{self.default_peer}" if self.default_peer else None),
            )
            if dispatched.get("idempotent_replay"):
                replay_delivery = _replay_delivery_payload(dispatched)
                return {
                    **dispatched,
                    "committed": True,
                    "result": dispatched,
                    "delivery": replay_delivery,
                }
            mission = dispatched["mission"]
            task = dispatched["task"]
            evt_id = int(dispatched["event"]["event_id"])
            ring_ms = time.time() * 1000.0
            delivery = _safe_ring_peer(
                store=self.store,
                home_dir=self.rendezvous_dir,
                peer_id=task["assignee_peer"],
                title=f"Consilium: {mission['title']}",
                body=f"Task {task['task_id']} ({task['model_tier']})",
                pickup_prompt=(
                    f"Use consilium_pickup_inbox(peer_id={task['assignee_peer']!r}) "
                    f"to claim task {task['task_id']} in mission {mission['mission_id']}."
                ),
                workspace_path=mission.get("workspace_path", ""),
                open_deeplink=open_deeplink,
                event_ids=[evt_id],
            )
            rw = _safe_record_event_delivery(
                self.store, [evt_id], delivery, ring_sent_at_ms=ring_ms
            )
            pub_warn = self._emit_mutation("dispatch_task")
            all_warnings = _collect_post_commit_warnings(
                dispatched.get("post_commit_warnings"),
                delivery,
                [w for w in (rw, pub_warn) if w] or None,
            )
            if all_warnings:
                delivery["warnings"] = all_warnings
            _safe_save_idempotency_delivery(self.store, client_req_id, delivery)
            return {
                **dispatched,
                "committed": True,
                "result": dispatched,
                "delivery": delivery,
            }

        if name == "consilium_pickup_inbox":
            peer = str(args.get("peer_id") or self.default_peer)
            claim_tasks = _parse_bool(args, "claim_tasks", True)
            try:
                released = self.store.release_stale_claims(
                    claimed_timeout_sec=self.claim_timeout_sec,
                    running_timeout_sec=self.running_timeout_sec,
                )
                if released:
                    if self._on_reaper_tasks is not None:
                        self._on_reaper_tasks(released)
                    else:
                        _deliver_reaper_notifications(
                            self.store, self.rendezvous_dir, released
                        )
            except Exception:
                pass
            label = str(args.get("session_label", ""))
            only_mission = str(args.get("mission_id") or "").strip()
            scope = str(args.get("scope") or "").strip().lower()
            if not scope:
                # A standby session serves the Consilium window; it must not swallow
                # receipts that belong to a session working on its own mission.
                scope = "consilium" if (label.strip().lower().endswith("standby") and not only_mission) else "all"
            picked = self.store.pickup_inbox(
                peer_id=peer,
                session_label=label,
                claim_tasks=claim_tasks,
                limit=int(args.get("limit", 20)),
                scope=scope,
                mission_id=only_mission,
            )
            picked["scope"] = scope
            pub_warn = self._emit_mutation("pickup_inbox")
            if pub_warn:
                picked.setdefault("post_commit_warnings", []).append(pub_warn)
            return picked

        if name == "consilium_report_progress":
            notify_coord = _parse_bool(args, "notify_coordinator", False)
            client_req_id = args.get("client_request_id")
            res = self.store.report_progress(
                task_id=str(args["task_id"]),
                peer_id=str(args["peer_id"]),
                progress_note=str(args["progress_note"]),
                status=str(args.get("status", "running")),
                worktree_branch=args.get("worktree_branch"),
                notify_coordinator=notify_coord,
                attempt_id=str(args["attempt_id"]) if args.get("attempt_id") is not None else None,
                client_request_id=str(client_req_id) if client_req_id is not None else None,
                model_used=str(args.get("model_used") or ""),
            )
            rec_warnings = []
            if res.get("idempotent_replay"):
                delivery = _replay_delivery_payload(res)
            elif res.get("event") and res.get("coordinator_peer"):
                evt_id = int(res["event"]["event_id"])
                ring_ms = time.time() * 1000.0
                delivery = _safe_ring_peer(
                    store=self.store,
                    home_dir=self.rendezvous_dir,
                    peer_id=res["coordinator_peer"],
                    title="Consilium Progress",
                    body=f"{args['peer_id']} updated {args['task_id']}",
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={res['coordinator_peer']!r}) "
                        f"to read progress on {args['task_id']}."
                    ),
                    event_ids=[evt_id],
                )
                rw = _safe_record_event_delivery(
                    self.store, [evt_id], delivery, ring_sent_at_ms=ring_ms
                )
                if rw:
                    rec_warnings.append(rw)
            else:
                delivery = {"delivery": "none"}
            if not res.get("idempotent_replay"):
                pub_warn = self._emit_mutation("report_progress")
                if pub_warn:
                    rec_warnings.append(pub_warn)
                all_warnings = _collect_post_commit_warnings(
                    res.get("post_commit_warnings"), delivery, rec_warnings
                )
                if all_warnings:
                    delivery["warnings"] = all_warnings
                _safe_save_idempotency_delivery(self.store, client_req_id, delivery)
            return {**res, "committed": True, "result": res, "delivery": delivery}

        if name == "consilium_complete_task":
            client_req_id = args.get("client_request_id")
            res = self.store.complete_task(
                task_id=str(args["task_id"]),
                peer_id=str(args["peer_id"]),
                result_summary=str(args["result_summary"]),
                artifacts=args.get("artifacts") or [],
                status=str(args.get("status", "done")),
                attempt_id=str(args["attempt_id"]) if args.get("attempt_id") is not None else None,
                client_request_id=str(client_req_id) if client_req_id is not None else None,
                model_used=str(args.get("model_used") or ""),
            )
            rec_warnings = []
            if res.get("idempotent_replay"):
                delivery = _replay_delivery_payload(res)
            else:
                coord = res["coordinator_peer"]
                evt_id = int(res["event"]["event_id"])
                ring_ms = time.time() * 1000.0
                delivery = _safe_ring_peer(
                    store=self.store,
                    home_dir=self.rendezvous_dir,
                    peer_id=coord,
                    title="Consilium Task Done",
                    body=(
                        f"{args['peer_id']} finished {args['task_id']}"
                        + (" (ALL TASKS FINISHED)" if res["all_tasks_finished"] else "")
                    ),
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={coord!r}) to collect results "
                        f"from {args['peer_id']} on {args['task_id']}."
                    ),
                    event_ids=[evt_id],
                )
                rw = _safe_record_event_delivery(
                    self.store, [evt_id], delivery, ring_sent_at_ms=ring_ms
                )
                if rw:
                    rec_warnings.append(rw)
                pub_warn = self._emit_mutation("complete_task")
                if pub_warn:
                    rec_warnings.append(pub_warn)
                all_warnings = _collect_post_commit_warnings(
                    res.get("post_commit_warnings"), delivery, rec_warnings
                )
                if all_warnings:
                    delivery["warnings"] = all_warnings
                _safe_save_idempotency_delivery(self.store, client_req_id, delivery)
            return {**res, "committed": True, "result": res, "delivery": delivery}

        if name == "consilium_requeue_task":
            client_req_id = args.get("client_request_id")
            res = self.store.requeue_task(
                task_id=str(args["task_id"]),
                reason=str(args.get("reason") or "coordinator_requeue"),
                actor_peer=str(args["peer_id"]),
                client_request_id=str(client_req_id) if client_req_id is not None else None,
            )
            rec_warnings = []
            if res.get("idempotent_replay"):
                delivery = _replay_delivery_payload(res)
            else:
                eid = int(res["event"]["event_id"])
                ring_ms = time.time() * 1000.0
                delivery = _safe_ring_peer(
                    store=self.store,
                    home_dir=self.rendezvous_dir,
                    peer_id=res["assignee_peer"],
                    title="Consilium Task Requeued",
                    body=f"Task {args['task_id']} returned to queued",
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={res['assignee_peer']!r}) "
                        f"to claim requeued task {args['task_id']}."
                    ),
                    event_ids=[eid],
                )
                rw = _safe_record_event_delivery(
                    self.store, [eid], delivery, ring_sent_at_ms=ring_ms
                )
                if rw:
                    rec_warnings.append(rw)
                pub_warn = self._emit_mutation("requeue_task")
                if pub_warn:
                    rec_warnings.append(pub_warn)
                all_warnings = _collect_post_commit_warnings(
                    res.get("post_commit_warnings"), delivery, rec_warnings
                )
                if all_warnings:
                    delivery["warnings"] = all_warnings
                _safe_save_idempotency_delivery(self.store, client_req_id, delivery)
            return {**res, "committed": True, "result": res, "delivery": delivery}

        if name == "consilium_preferences":
            try:
                sync_local_quotas(self.store)
            except Exception:
                pass
            return {
                "preferences": self.store.list_preferences(),
                "recent_user_routing": self.store.recent_user_routing(),
                "peers": peer_quota_digest(self.store, self.rendezvous_dir),
            }

        if name == "consilium_bind_session":
            res = self.store.bind_native_session(
                str(args["mission_id"]), str(args["peer_id"]), str(args["session_ref"])
            )
            pub_warn = self._emit_mutation("bind_session")
            return {"committed": True, "result": res, "delivery": {"warnings": [pub_warn] if pub_warn else []}}

        if name == "consilium_set_title":
            res = self.store.set_mission_title(
                str(args["mission_id"]), str(args["title"]), actor=str(args["peer_id"])
            )
            pub_warn = self._emit_mutation("set_title")
            return {"committed": True, "result": res, "delivery": {"warnings": [pub_warn] if pub_warn else []}}

        if name == "consilium_set_workspace":
            res = self.store.set_mission_workspace(
                str(args["mission_id"]), str(args["path"]), actor=str(args["peer_id"])
            )
            pub_warn = self._emit_mutation("set_workspace")
            return {"committed": True, "result": res, "delivery": {"warnings": [pub_warn] if pub_warn else []}}

        if name == "consilium_learn_preference":
            res = self.store.add_preference(str(args["text"]), source=str(args["peer_id"]))
            pub_warn = self._emit_mutation("learn_preference")
            return {"committed": True, "result": res, "delivery": {"warnings": [pub_warn] if pub_warn else []}}

        if name == "consilium_handoff_task":
            res = self.store.handoff_task(
                task_id=str(args["task_id"]),
                peer_id=str(args["peer_id"]),
                attempt_id=str(args["attempt_id"]),
                to_peer=str(args["to_peer"]),
                note=str(args["note"]),
            )
            eid = int(res["event"]["event_id"])
            ring_ms = time.time() * 1000.0
            target = res["task"]["assignee_peer"]
            delivery = _safe_ring_peer(
                store=self.store,
                home_dir=self.rendezvous_dir,
                peer_id=target,
                title="Consilium 交接",
                body=f"{PEER_LABELS.get(str(args['peer_id']), args['peer_id'])} 把任务交给了你",
                pickup_prompt=f"Use consilium_pickup_inbox(peer_id={target!r}) to claim handed-off task {args['task_id']}.",
                event_ids=[eid],
            )
            rec = []
            rw = _safe_record_event_delivery(self.store, [eid], delivery, ring_sent_at_ms=ring_ms)
            if rw:
                rec.append(rw)
            pub_warn = self._emit_mutation("handoff_task")
            if pub_warn:
                rec.append(pub_warn)
            warns = _collect_post_commit_warnings(res.get("post_commit_warnings"), delivery, rec)
            if warns:
                delivery["warnings"] = warns
            return {**res, "committed": True, "result": res, "delivery": delivery}

        if name == "consilium_request_review":
            client_req_id = args.get("client_request_id")
            res = self.store.request_review(
                mission_id=str(args["mission_id"]),
                from_peer=str(args["from_peer"]),
                to_peer=str(args["to_peer"]),
                summary=str(args["summary"]),
                question=str(args["question"]),
                task_id=args.get("task_id"),
                client_request_id=str(client_req_id) if client_req_id is not None else None,
            )
            rec_warnings = []
            if res.get("idempotent_replay"):
                delivery = _replay_delivery_payload(res)
            else:
                target = str(args["to_peer"]) if res["allowed"] else res["coordinator_peer"]
                sender_norm = str(args["from_peer"]).strip().lower()
                evt_id = int(res["event"]["event_id"])
                ring_ms = time.time() * 1000.0
                if not res["allowed"] and target == sender_norm:
                    delivery = {"delivery": "self_coordinator", "peer_id": target}
                else:
                    title = "Consilium Review Request" if res["allowed"] else "Consilium Loop Guard"
                    body = (
                        f"Round {res['round']}/{res['max_rounds']} from {args['from_peer']}"
                        if res["allowed"]
                        else res["reason"]
                    )
                    delivery = _safe_ring_peer(
                        store=self.store,
                        home_dir=self.rendezvous_dir,
                        peer_id=target,
                        title=title,
                        body=body,
                        pickup_prompt=(
                            f"Use consilium_pickup_inbox(peer_id={target!r}) to handle {title} "
                            f"for mission {args['mission_id']}."
                        ),
                        event_ids=[evt_id],
                    )
                rw = _safe_record_event_delivery(
                    self.store, [evt_id], delivery, ring_sent_at_ms=ring_ms
                )
                if rw:
                    rec_warnings.append(rw)
                pub_warn = self._emit_mutation("request_review")
                if pub_warn:
                    rec_warnings.append(pub_warn)
                all_warnings = _collect_post_commit_warnings(
                    res.get("post_commit_warnings"), delivery, rec_warnings
                )
                if all_warnings:
                    delivery["warnings"] = all_warnings
                _safe_save_idempotency_delivery(self.store, client_req_id, delivery)
            return {**res, "committed": True, "result": res, "delivery": delivery}

        if name == "consilium_wait":
            peer = str(args.get("peer_id") or self.default_peer)
            timeout_sec = min(300.0, max(0.1, float(args.get("timeout_sec", 45.0))))
            # Validate boolean strictly, then ignore value (wait is strictly read-only).
            requested_auto_pickup = _parse_bool(args, "auto_pickup", False)
            wres = wait_for_doorbell(
                store=self.store,
                home_dir=self.rendezvous_dir,
                peer_id=peer,
                timeout_sec=timeout_sec,
                session_label=str(args.get("session_label", "")),
                auto_pickup=False,
                rendezvous_dir=self.rendezvous_dir,
            )
            if requested_auto_pickup:
                wres["auto_pickup_ignored"] = True
                wres["notice"] = (
                    "consilium_wait is strictly read-only; auto_pickup is ignored. "
                    "Call consilium_pickup_inbox explicitly."
                )
            return wres

        if name == "consilium_quota":
            report_windows = args.get("report_windows") or []
            if report_windows:
                normalized_reports = []
                for item in report_windows:
                    entry = dict(item)
                    req_src = str(item.get("source") or "").strip().lower()
                    entry["source"] = "demo" if req_src == "demo" else "manual_report"
                    normalized_reports.append(entry)
                self.store.upsert_quota_windows(normalized_reports)
            if _parse_bool(args, "sync_local", True):
                sync_local_quotas(self.store, user_home=args.get("user_home"))
            return self.store.list_quota_windows(peer_id=args.get("peer_id"))

        if name == "consilium_close_mission":
            force = _parse_bool(args, "force", False)
            client_req_id = args.get("client_request_id")
            res = self.store.close_mission(
                mission_id=str(args["mission_id"]),
                final_summary=str(args["final_summary"]),
                peer_id=str(args["peer_id"]),
                status=str(args.get("status", "completed")),
                force=force,
                client_request_id=str(client_req_id) if client_req_id is not None else None,
            )
            if res.get("idempotent_replay"):
                replay_delivery = _replay_delivery_payload(res, multi=True)
                return {
                    **res,
                    "committed": True,
                    "result": res,
                    "delivery": replay_delivery,
                    "deliveries": replay_delivery.get("deliveries", []),
                }
            deliveries = []
            rec_warnings = []
            for evt in res.get("events", []):
                eid = int(evt["event_id"])
                ring_ms = time.time() * 1000.0
                receipt = _safe_ring_peer(
                    store=self.store,
                    home_dir=self.rendezvous_dir,
                    peer_id=evt["to_peer"],
                    title="Consilium Mission Closed",
                    body=f"Mission {res['mission_id']} closed ({res['status']})",
                    pickup_prompt=(
                        f"Use consilium_pickup_inbox(peer_id={evt['to_peer']!r}) "
                        f"for closed mission {res['mission_id']}."
                    ),
                    event_ids=[eid],
                )
                rw = _safe_record_event_delivery(
                    self.store, [eid], receipt, ring_sent_at_ms=ring_ms
                )
                if rw:
                    rec_warnings.append(rw)
                deliveries.append(receipt)
            pub_warn = self._emit_mutation("close_mission")
            if pub_warn:
                rec_warnings.append(pub_warn)
            delivery_summary = {
                "deliveries": deliveries,
                "errors": [d for d in deliveries if d.get("delivery") == "error"],
            }
            if delivery_summary["errors"]:
                delivery_summary["error"] = "; ".join(
                    str(d.get("error", "")) for d in delivery_summary["errors"]
                )
            all_warnings = _collect_post_commit_warnings(
                res.get("post_commit_warnings"), delivery_summary, rec_warnings
            )
            if all_warnings:
                delivery_summary["warnings"] = all_warnings
            _safe_save_idempotency_delivery(self.store, client_req_id, delivery_summary)
            return {
                **res,
                "committed": True,
                "result": res,
                "delivery": delivery_summary,
                "deliveries": deliveries,
            }

        raise ValueError(f"Unknown tool: {name}")

    @staticmethod
    def _result(req_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    @staticmethod
    def _error(req_id: Any, code: int, message: str) -> Dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}

    @staticmethod
    def _tool_text_result(
        req_id: Any, payload: Dict[str, Any], is_error: bool = False
    ) -> Dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [
                    {"type": "text", "text": json.dumps(payload, ensure_ascii=False, indent=2)}
                ],
                "structuredContent": payload,
                "isError": is_error,
            },
        }


def run_stdio_server(
    home_dir: Optional[str] = None,
    default_peer: str = "",
    stdin: Any = None,
    stdout: Any = None,
    rendezvous_dir: Optional[str] = None,
) -> None:
    """Run a newline-delimited JSON-RPC 2.0 MCP server over stdio."""
    in_stream = stdin if stdin is not None else sys.stdin
    out_stream = stdout if stdout is not None else sys.stdout
    dispatcher = ConsiliumMCPDispatcher(
        home_dir=home_dir,
        default_peer=default_peer,
        rendezvous_dir=rendezvous_dir,
    )
    rebuild_all_peer_seqs(dispatcher.store)

    for raw_line in in_stream:
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except ValueError as exc:
            resp = dispatcher._error(None, -32700, f"Parse error: {exc}")
        else:
            resp = dispatcher.handle_payload(payload)
        if resp is not None:
            out_stream.write(json.dumps(resp, ensure_ascii=False) + "\n")
            out_stream.flush()


def _extract_host_and_port(host_header: str) -> Tuple[str, Optional[int]]:
    raw = (host_header or "").strip().lower()
    if not raw:
        return "", None
    if raw.startswith("["):
        end = raw.find("]")
        if end == -1:
            return raw, None
        host = raw[1:end]
        rest = raw[end + 1 :]
        if rest.startswith(":") and rest[1:].isdigit():
            return host, int(rest[1:])
        return host, None
    if ":" in raw:
        host, port_s = raw.split(":", 1)
        return host, int(port_s) if port_s.isdigit() else None
    return raw, None


def _extract_hostname(host_header: str) -> str:
    return _extract_host_and_port(host_header)[0]


def _is_valid_host(host_header: str) -> bool:
    return _extract_hostname(host_header) in LOCAL_HOSTS


def _is_valid_origin(origin_header: str) -> bool:
    if not origin_header:
        return True
    try:
        parsed = urllib.parse.urlparse(origin_header)
        _ = parsed.port
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    return (parsed.hostname or "").lower() in LOCAL_HOSTS


def _is_exact_daemon_origin(
    origin_header: str,
    host_header: str,
    daemon_host: str,
    daemon_port: int,
) -> bool:
    """Require non-empty Origin matching daemon scheme, host, and port with no path/query/userinfo."""
    if not origin_header:
        return False
    try:
        parsed = urllib.parse.urlparse(origin_header)
        raw_port = parsed.port
    except ValueError:
        return False
    if parsed.scheme != "http":
        return False
    if parsed.path not in ("", None) or parsed.params or parsed.query or parsed.fragment:
        return False
    if parsed.username is not None or parsed.password is not None:
        return False
    expected_host = (daemon_host or "").strip("[]").lower()
    origin_host = (parsed.hostname or "").lower()
    if origin_host not in LOCAL_HOSTS:
        return False
    if expected_host and origin_host != expected_host:
        return False
    origin_port = raw_port if raw_port is not None else 80
    if origin_port != int(daemon_port):
        return False
    host_name, host_port = _extract_host_and_port(host_header)
    if host_name and origin_host != host_name:
        return False
    if host_port is not None and origin_port != host_port:
        return False
    return True


class ConsiliumDaemon:
    """Combined WakeBrokerServer + hardened 127.0.0.1 Streamable HTTP MCP server."""

    def __init__(
        self,
        home_dir: Optional[str] = None,
        host: str = DEFAULT_HTTP_HOST,
        port: int = DEFAULT_HTTP_PORT,
        require_token: bool = True,
        rendezvous_dir: Optional[str] = None,
        reaper_interval_sec: float = DEFAULT_REAPER_INTERVAL_SEC,
        claim_timeout_sec: Optional[int] = None,
        running_timeout_sec: Optional[int] = None,
    ) -> None:
        self.home_dir = resolve_state_dir(home_dir)
        self.rendezvous_dir = resolve_rendezvous_dir(rendezvous_dir, fallback_dir=home_dir)
        self.host = host
        self.port = int(port)
        self.require_token = bool(require_token)
        self._auth_lock = threading.Lock()
        self.auth_token = ensure_auth_token(self.home_dir) if self.require_token else ""
        self._token_stat_sig: Optional[Tuple[int, int]] = self._read_token_stat()
        self._token_version = 1
        self.reaper_interval_sec = max(0.05, float(reaper_interval_sec))
        self.claim_timeout_sec = claim_timeout_sec
        self.running_timeout_sec = running_timeout_sec
        self.started_at = utc_now_iso()
        self.started_monotonic = time.monotonic()
        self.event_hub = EventHub(max_subscribers=SSE_MAX_SUBSCRIBERS)
        self.session_store = SessionStore()
        self.ticket_store = LoginTicketStore()
        self._state_cache_lock = threading.Lock()
        self._cached_state: Optional[Tuple[float, Dict[str, Any]]] = None
        self.dispatcher = ConsiliumMCPDispatcher(
            home_dir=self.home_dir,
            rendezvous_dir=self.rendezvous_dir,
            on_mutation=self.publish_state_update,
            claim_timeout_sec=self.claim_timeout_sec,
            running_timeout_sec=self.running_timeout_sec,
            on_reaper_tasks=self._handle_reaper_released,
        )
        self.wake_broker = WakeBrokerServer(rendezvous_dir=self.rendezvous_dir)
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._reaper_thread: Optional[threading.Thread] = None

    def get_dashboard_state(self, *, sync_quotas: bool = True) -> Dict[str, Any]:
        """Return full dashboard snapshot JSON."""
        return build_dashboard_state(
            self.dispatcher.store,
            self.rendezvous_dir,
            started_at=self.started_at,
            started_monotonic=self.started_monotonic,
            host=self.host,
            port=self.port,
            sse_clients=self.event_hub.subscriber_count(),
            sync_quotas=sync_quotas,
        )

    def get_cached_dashboard_state(self, max_age_sec: float = 0.35) -> Dict[str, Any]:
        """Share a brief snapshot cache across idle SSE pollers to avoid duplicate DB rebuilds."""
        now = time.monotonic()
        with self._state_cache_lock:
            if self._cached_state is not None:
                ts, snap = self._cached_state
                if (now - ts) <= max_age_sec:
                    return snap
            snap = self.get_dashboard_state(sync_quotas=False)
            self._cached_state = (now, snap)
            return snap

    def publish_state_update(self, reason: str = "mutation") -> int:
        """Broadcast a monotonically versioned state update to all connected SSE dashboard clients."""
        with self._state_cache_lock:
            self._cached_state = None
        if self.event_hub.subscriber_count() == 0:
            return self.event_hub.next_version()
        return self.event_hub.publish_state(
            "update", reason, lambda: self.get_dashboard_state(sync_quotas=False)
        )

    def _read_token_stat(self) -> Optional[Tuple[int, int]]:
        token_path = os.path.join(self.home_dir, "auth.token")
        try:
            st = os.stat(token_path)
            return (int(st.st_ino), int(st.st_mtime_ns))
        except OSError:
            return None

    def get_auth_state(self) -> Tuple[str, int]:
        """Return (active_token, token_version), revoking sessions/tickets/SSE on disk rotation."""
        if not self.require_token:
            return "", 0
        close_streams = False
        with self._auth_lock:
            sig = self._read_token_stat()
            if sig is not None and sig != self._token_stat_sig:
                fresh = read_auth_token(self.home_dir)
                self._token_stat_sig = sig
                if fresh and fresh != self.auth_token:
                    self.auth_token = fresh
                    self._token_version += 1
                    self.session_store.revoke_all()
                    self.ticket_store.revoke_all()
                    close_streams = True
            token_val = self.auth_token
            ver_val = self._token_version
        if close_streams:
            self.event_hub.close_all()
        return token_val, ver_val

    def get_active_token(self) -> str:
        """Return the active auth token, live-reloading if auth.token was rotated on disk."""
        return self.get_auth_state()[0]

    def _handle_reaper_released(self, released: List[Dict[str, Any]]) -> None:
        if not released:
            return
        _deliver_reaper_notifications(
            self.dispatcher.store, self.rendezvous_dir, released
        )
        actions = {str(r.get("reaper_action") or "requeued") for r in released}
        reason = "reaper_stale" if actions == {"marked_stale"} else "reaper_requeued"
        self.publish_state_update(reason)

    def ui_send(
        self,
        text: str,
        targets: Any,
        mission_id: Optional[str] = None,
        workspace_hint: str = "",
        new_session: bool = False,
        tier: str = "",
    ) -> Dict[str, Any]:
        """Send a message typed in the dashboard: @-mentioned peers get it directly; no mention = auto."""
        body = str(text or "").strip()
        if not body:
            raise ValueError("message must not be empty")
        if len(body) > 20000:
            raise ValueError("message too long (max 20000 characters)")
        if targets is None:
            targets = []
        if not isinstance(targets, list):
            raise ValueError("targets must be a list")
        chosen: List[str] = []
        for t in targets:
            pid = str(t or "").strip().lower()
            if pid not in VALID_PEERS:
                raise ValueError(f"Unknown target {t!r}")
            if pid not in chosen:
                chosen.append(pid)
        store = self.dispatcher.store
        try:
            sync_local_quotas(store)
        except Exception:
            pass
        digest = peer_quota_digest(store, self.rendezvous_dir)
        title = body.splitlines()[0][:40]
        mode = "direct" if chosen else "auto"
        # The folder is chosen by the user (recent list or the native folder picker), never guessed from text.
        workspace = ""
        hint = str(workspace_hint or "").strip()
        if hint:
            resolved = os.path.realpath(os.path.expanduser(hint))
            if not hint.startswith(("/", "~")) or not os.path.isdir(resolved):
                raise ValueError("请选择一个已存在的文件夹")
            workspace = resolved
        tier_key = {"fast": "fast", "balanced": "balanced", "deep": "pro_reasoning", "pro_reasoning": "pro_reasoning"}.get(
            str(tier or "").strip().lower(), ""
        )

        def _wrap(prompt: str) -> str:
            return prompt
        events: List[Dict[str, Any]] = []

        if mission_id is None:
            if chosen:
                coordinator = USER_PEER
                task_specs = [
                    {"assignee_peer": pid, "prompt": _wrap(body), "origin": "user", "new_session": True,
                     "model_tier": tier_key or "balanced"}
                    for pid in chosen
                ]
            else:
                coordinator = choose_auto_coordinator(digest)
                prompt = build_auto_coordinator_prompt(
                    body, coordinator, digest, store.list_preferences(), store.recent_user_routing()
                )
                task_specs = [
                    {"assignee_peer": coordinator, "prompt": _wrap(prompt), "origin": "user", "new_session": True,
                     "model_tier": tier_key or "balanced"}
                ]
            res = store.create_mission(
                title=title,
                goal=body,
                coordinator_peer=coordinator,
                workspace_path=workspace,
                isolation_policy="shared",
                tasks=task_specs,
                allow_user_coordinator=True,
                user_message={
                    "text": body,
                    "targets": chosen or [coordinator],
                    "mode": mode,
                    "reason": "" if chosen else explain_auto_choice(digest, coordinator),
                },
            )
            mid = res["mission"]["mission_id"]
            if tier_key:
                store.set_mission_tier(mid, tier_key)
            events = list(res.get("events") or [])
            warnings = list(res.get("post_commit_warnings") or [])
        else:
            snap = store.get_mission_snapshot(mission_id=mission_id)
            mission = snap.get("mission") or {}
            if mission.get("status") in VALID_MISSION_CLOSE_STATUSES:
                raise ValueError("这个会诊已经关闭，请新开一个")
            mid = mission_id
            coord = mission.get("coordinator_peer") or USER_PEER
            bindings = store.mission_bindings(mid)
            reason = ""
            if chosen:
                plan = [(pid, body) for pid in chosen]
            elif coord != USER_PEER:
                reason = f"交给这个会诊的协调者 {PEER_LABELS.get(coord, coord)}"
                plan = [(coord, f"【使用者追问】\n{body}\n\n（你是这次会诊的协调者，按需继续分配，最后交付给使用者。）")]
            else:
                pick = choose_auto_coordinator(digest)
                reason = explain_auto_choice(digest, pick)
                plan = [(pick, build_auto_coordinator_prompt(
                    body, pick, digest, store.list_preferences(), store.recent_user_routing()
                ))]
            warnings = []
            for idx, (pid, prompt) in enumerate(plan):
                r = store.dispatch_task(
                    mission_id=mid,
                    assignee_peer=pid,
                    prompt=_wrap(prompt),
                    origin="user",
                    new_session=pid not in bindings,
                    model_tier=(mission.get("tier") or "balanced") if (mission.get("tier") or "") in ("fast", "balanced", "pro_reasoning") else "balanced",
                    user_message=(
                        {"text": body, "targets": chosen or [plan[0][0]], "mode": mode, "reason": reason}
                        if idx == 0 else None
                    ),
                )
                events.append(r["event"])
                warnings.extend(r.get("post_commit_warnings") or [])

        deliveries: List[Dict[str, Any]] = []
        for evt in events:
            eid = int(evt["event_id"])
            pid = evt["to_peer"]
            ring_ms = time.time() * 1000.0
            receipt = _safe_ring_peer(
                store=store,
                home_dir=self.rendezvous_dir,
                peer_id=pid,
                title="Consilium 新消息",
                body=f"使用者：{title}",
                pickup_prompt=(
                    f"Use consilium_pickup_inbox(peer_id={pid!r}) to claim the new Consilium message, "
                    "then follow the consilium-collab skill."
                ),
                event_ids=[eid],
            )
            rw = _safe_record_event_delivery(store, [eid], receipt, ring_sent_at_ms=ring_ms)
            if rw:
                warnings.append(rw)
            deliveries.append(receipt)
        try:
            self.publish_state_update("ui_send")
        except Exception as exc:
            warnings.append(f"publish_state_update: {exc}")
        return {
            "mission_id": mid,
            "mode": mode,
            "targets": [e["to_peer"] for e in events],
            "deliveries": deliveries,
            "warnings": [w for w in warnings if w],
        }

    def run_reaper_once(self) -> List[Dict[str, Any]]:
        """Re-queue stale claimed tasks, mark stale running tasks, and notify peers."""
        released = self.dispatcher.store.release_stale_claims(
            claimed_timeout_sec=self.claim_timeout_sec,
            running_timeout_sec=self.running_timeout_sec,
        )
        self._handle_reaper_released(released)
        return released

    def _reaper_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_reaper_once()
            except Exception:
                pass
            if self._stop_event.wait(self.reaper_interval_sec):
                break

    def _make_handler_class(self) -> Any:
        daemon = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            timeout = HTTP_READ_TIMEOUT_SEC

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _check_host_origin(self) -> bool:
                host_hdr = self.headers.get("Host", "")
                if not _is_valid_host(host_hdr):
                    self._send_json(403, {"error": "Forbidden Host header"})
                    return False
                origin_hdr = self.headers.get("Origin", "")
                if not _is_valid_origin(origin_hdr):
                    self._send_json(403, {"error": "Forbidden cross-origin request"})
                    return False
                return True

            def _authenticate_request(
                self, *, allow_cookie: bool = False
            ) -> Tuple[Optional[str], Optional[SessionRecord], int]:
                auth_hdr = self.headers.get("Authorization", "")
                active_token, token_ver = daemon.get_auth_state()
                if not daemon.require_token:
                    if auth_hdr.startswith("Bearer "):
                        return "bearer", None, token_ver
                    return "open", None, token_ver
                expected_hdr = f"Bearer {active_token}"
                if auth_hdr and hmac.compare_digest(auth_hdr, expected_hdr):
                    return "bearer", None, token_ver
                if allow_cookie:
                    cookies = parse_cookie_header(self.headers.get("Cookie", ""))
                    sess_id = cookies.get(SESSION_COOKIE_NAME, "")
                    session_rec = daemon.session_store.get(sess_id, token_ver)
                    if session_rec is not None:
                        return "cookie", session_rec, token_ver
                return None, None, token_ver

            def _check_security(
                self, *, allow_cookie: bool = False
            ) -> Tuple[bool, Optional[str], Optional[SessionRecord], int]:
                if not self._check_host_origin():
                    return False, None, None, 0
                mode, session_rec, token_ver = self._authenticate_request(allow_cookie=allow_cookie)
                if mode is None:
                    self._send_json(401, {"error": "Unauthorized"})
                    return False, None, None, token_ver
                return True, mode, session_rec, token_ver

            def _check_cookie_write_csrf(
                self, auth_mode: str, session_rec: Optional[SessionRecord]
            ) -> bool:
                if auth_mode == "bearer":
                    return True
                origin_hdr = self.headers.get("Origin", "")
                host_hdr = self.headers.get("Host", "")
                if not _is_exact_daemon_origin(origin_hdr, host_hdr, daemon.host, daemon.port):
                    self._send_json(403, {"error": "Forbidden or missing exact daemon Origin"})
                    return False
                raw_ct = self.headers.get("Content-Type", "")
                media_type = raw_ct.split(";", 1)[0].strip().lower()
                if media_type != "application/json":
                    self._send_json(
                        415, {"error": "Unsupported Media Type; expected application/json"}
                    )
                    return False
                if session_rec is not None:
                    csrf_hdr = self.headers.get("X-Consilium-CSRF", "")
                    if not csrf_hdr or not hmac.compare_digest(csrf_hdr, session_rec.csrf_token):
                        self._send_json(403, {"error": "Missing or invalid X-Consilium-CSRF header"})
                        return False
                return True

            def _parse_strict_content_length(self) -> Tuple[bool, Optional[int], int, str]:
                """Validate HTTP/1.1 Transfer-Encoding and Content-Length framing strictly."""
                if self.headers.get_all("Transfer-Encoding"):
                    return False, None, 501, "Transfer-Encoding is not supported"
                cl_headers = self.headers.get_all("Content-Length") or []
                if len(cl_headers) > 1:
                    return False, None, 400, "Duplicate Content-Length header"
                if not cl_headers:
                    return True, None, 200, ""
                raw_len = cl_headers[0].strip()
                if not raw_len or not raw_len.isascii() or not raw_len.isdigit():
                    return False, None, 400, "Invalid Content-Length"
                return True, int(raw_len), 200, ""

            def _consume_optional_body(self, max_bytes: int = 65536) -> bool:
                """Drain small bounded request bodies on keep-alive POST routes or reject framing errors."""
                if getattr(self, "_body_consumed", False):
                    return True
                ok_len, length, err_status, err_msg = self._parse_strict_content_length()
                if not ok_len:
                    self._send_json(err_status, {"error": err_msg})
                    return False
                if length is None or length == 0:
                    self._body_consumed = True
                    return True
                if length > max_bytes:
                    self._send_json(413, {"error": "Payload too large"})
                    return False
                raw = self.rfile.read(length)
                if len(raw) != length:
                    self._send_json(400, {"error": "Incomplete request body"})
                    return False
                self._body_consumed = True
                return True

            def _send_json(
                self,
                status: int,
                body: Any,
                extra_headers: Optional[Dict[str, str]] = None,
            ) -> None:
                has_unconsumed_body = (
                    not getattr(self, "_body_consumed", False)
                    and bool(
                        self.headers.get_all("Transfer-Encoding")
                        or any(v.strip() != "0" for v in (self.headers.get_all("Content-Length") or []))
                    )
                )
                if status >= 400 or has_unconsumed_body:
                    # Close keep-alive connections on error or unconsumed body so framing never desyncs (P3-2/R015).
                    self.close_connection = True
                encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Referrer-Policy", "no-referrer")
                if extra_headers:
                    for hk, hv in extra_headers.items():
                        self.send_header(hk, hv)
                if self.close_connection:
                    self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(encoded)

            def _send_html(self, status: int, html: str) -> None:
                has_unconsumed_body = (
                    not getattr(self, "_body_consumed", False)
                    and bool(
                        self.headers.get_all("Transfer-Encoding")
                        or any(v.strip() != "0" for v in (self.headers.get_all("Content-Length") or []))
                    )
                )
                if status >= 400 or has_unconsumed_body:
                    self.close_connection = True
                encoded = html.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header(
                    "Content-Security-Policy",
                    (
                        "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
                        "connect-src 'self'; img-src 'self' data:; frame-ancestors 'none';"
                    ),
                )
                if self.close_connection:
                    self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(encoded)

            def _read_json_body(self) -> Tuple[bool, Any]:
                ok_len, length, err_status, err_msg = self._parse_strict_content_length()
                if not ok_len:
                    self._send_json(err_status, {"error": err_msg})
                    return False, None
                if length is None:
                    self._send_json(400, {"error": "Missing or invalid Content-Length"})
                    return False, None
                if length > 2 * 1024 * 1024:
                    self._send_json(413, {"error": "Payload too large"})
                    return False, None
                if length == 0:
                    self._body_consumed = True
                    return True, {}
                raw = self.rfile.read(length)
                if len(raw) != length:
                    self._send_json(400, {"error": "Incomplete request body"})
                    return False, None
                self._body_consumed = True
                try:
                    return True, json.loads(raw.decode("utf-8"))
                except ValueError as exc:
                    self._send_json(
                        400,
                        daemon.dispatcher._error(None, -32700, f"Parse error: {exc}"),
                    )
                    return False, None

            def _handle_sse_stream(
                self,
                parsed_url: urllib.parse.ParseResult,
                session_rec: Optional[SessionRecord],
                auth_token_ver: int,
            ) -> None:
                qs = urllib.parse.parse_qs(parsed_url.query)
                max_events = 0
                if "max_events" in qs:
                    try:
                        max_events = max(1, int(qs["max_events"][0]))
                    except ValueError:
                        max_events = 0

                stream_tok_ver = int(auth_token_ver)
                stream_sess_id = session_rec.session_id if session_rec is not None else ""
                sub = daemon.event_hub.subscribe(
                    session_id=stream_sess_id,
                    token_version=stream_tok_ver,
                )
                if sub is None:
                    self._send_json(
                        503,
                        {"error": f"Too many active SSE streams (limit {SSE_MAX_SUBSCRIBERS})"},
                    )
                    return

                def _stream_still_authorized() -> bool:
                    if daemon._stop_event.is_set() or sub.closed_event.is_set():
                        return False
                    _, cur_ver = daemon.get_auth_state()
                    if daemon.require_token and cur_ver != stream_tok_ver:
                        return False
                    if stream_sess_id and daemon.session_store.get(stream_sess_id, cur_ver) is None:
                        return False
                    return True

                try:
                    if not _stream_still_authorized():
                        self._send_json(401, {"error": "Unauthorized"})
                        return

                    self.connection.settimeout(10.0)
                    self.close_connection = True
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                    self.send_header("Cache-Control", "no-cache, no-store")
                    self.send_header("X-Content-Type-Options", "nosniff")
                    self.send_header("Referrer-Policy", "no-referrer")
                    self.send_header("Connection", "close")
                    self.end_headers()

                    sent_count = 0
                    last_ping = time.monotonic()
                    ver0, initial = daemon.event_hub.snapshot_with_version(
                        lambda: daemon.get_dashboard_state(sync_quotas=False)
                    )
                    # Re-check authorization after snapshot lock before writing initial frame (R012 P1 #2).
                    if not _stream_still_authorized():
                        return
                    last_sent_ver = ver0
                    last_fp = compute_state_fingerprint(initial)
                    last_waiters = dict(initial.get("active_waiters") or {})
                    frame = (
                        f"id: {ver0}\n"
                        f"event: state\n"
                        f"data: {json.dumps(initial, ensure_ascii=False)}\n\n"
                    ).encode("utf-8")
                    self.wfile.write(frame)
                    self.wfile.flush()
                    sent_count += 1
                    if max_events and sent_count >= max_events:
                        return

                    while _stream_still_authorized():
                        try:
                            seq, ev_name, payload = sub.queue.get(timeout=0.5)
                            if ev_name == "__close__" or not _stream_still_authorized():
                                break
                            if seq <= last_sent_ver:
                                continue
                            last_sent_ver = seq
                            ev_frame = (
                                f"id: {seq}\n"
                                f"event: {ev_name}\n"
                                f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                            ).encode("utf-8")
                            self.wfile.write(ev_frame)
                            self.wfile.flush()
                            st = payload.get("state")
                            if isinstance(st, dict):
                                last_fp = compute_state_fingerprint(st)
                                last_waiters = dict(st.get("active_waiters") or {})
                            sent_count += 1
                            if max_events and sent_count >= max_events:
                                return
                        except queue.Empty:
                            if not _stream_still_authorized():
                                break
                            cur_state = daemon.get_cached_dashboard_state(max_age_sec=0.35)
                            cur_fp = compute_state_fingerprint(cur_state)
                            if cur_fp != last_fp:
                                # Re-read state under _publish_lock so a newer version cannot carry an older snapshot (R012 P2).
                                ver_ext, stamped = daemon.event_hub.snapshot_with_version(
                                    lambda: daemon.get_dashboard_state(sync_quotas=False)
                                )
                                if not _stream_still_authorized():
                                    break
                                stamped_fp = compute_state_fingerprint(stamped)
                                stamped_waiters = dict(stamped.get("active_waiters") or {})
                                reason = (
                                    "waiter_change"
                                    if stamped_waiters != last_waiters
                                    else "external_change"
                                )
                                last_fp = stamped_fp
                                last_waiters = stamped_waiters
                                if ver_ext <= last_sent_ver:
                                    continue
                                last_sent_ver = ver_ext
                                ext_frame = (
                                    f"id: {ver_ext}\n"
                                    f"event: update\n"
                                    f"data: {json.dumps({'reason': reason, 'active_waiters': stamped_waiters, 'version': ver_ext, 'state': stamped}, ensure_ascii=False)}\n\n"
                                ).encode("utf-8")
                                self.wfile.write(ext_frame)
                                self.wfile.flush()
                                sent_count += 1
                                if max_events and sent_count >= max_events:
                                    return
                            elif time.monotonic() - last_ping >= 15.0:
                                self.wfile.write(b": keepalive\n\n")
                                self.wfile.flush()
                                last_ping = time.monotonic()
                except (BrokenPipeError, ConnectionResetError, TimeoutError, socket.timeout, OSError):
                    pass
                finally:
                    daemon.event_hub.unsubscribe(sub)

            def do_GET(self) -> None:
                self._body_consumed = False
                if not self._consume_optional_body():
                    return
                parsed = urllib.parse.urlparse(self.path)
                route = parsed.path
                if route in ("/", "/ui", "/ui/auth"):
                    if not self._check_host_origin():
                        return
                    if route == "/ui/auth":
                        qs = urllib.parse.parse_qs(parsed.query)
                        ticket = (qs.get("ticket") or [""])[0].strip()
                        self.send_response(302)
                        self.send_header("Location", "/ui")
                        self.send_header("Cache-Control", "no-store")
                        self.send_header("Referrer-Policy", "no-referrer")
                        self.send_header("X-Content-Type-Options", "nosniff")
                        _, token_ver = daemon.get_auth_state()
                        if not daemon.require_token:
                            session = daemon.session_store.create(token_ver)
                            self.send_header(
                                "Set-Cookie",
                                f"{SESSION_COOKIE_NAME}={session.session_id}; HttpOnly; SameSite=Strict; Path=/",
                            )
                        elif daemon.ticket_store.consume(ticket, token_ver):
                            session = daemon.session_store.create(token_ver)
                            self.send_header(
                                "Set-Cookie",
                                f"{SESSION_COOKIE_NAME}={session.session_id}; HttpOnly; SameSite=Strict; Path=/",
                            )
                        self.send_header("Content-Length", "0")
                        if self.close_connection:
                            self.send_header("Connection", "close")
                        self.end_headers()
                        return
                    if route == "/":
                        self.send_response(302)
                        self.send_header("Location", "/ui")
                        self.send_header("Cache-Control", "no-store")
                        self.send_header("Referrer-Policy", "no-referrer")
                        self.send_header("X-Content-Type-Options", "nosniff")
                        self.send_header("Content-Length", "0")
                        if self.close_connection:
                            self.send_header("Connection", "close")
                        self.end_headers()
                        return
                    self._send_html(200, render_dashboard_html(__version__))
                    return

                if route == "/api/state":
                    ok_sec, _, session_rec, _ = self._check_security(allow_cookie=True)
                    if not ok_sec:
                        return
                    ver_before = daemon.event_hub.current_version()
                    snap = daemon.get_dashboard_state(sync_quotas=True)
                    snap["state_version"] = ver_before
                    if session_rec is not None:
                        snap["csrf_token"] = session_rec.csrf_token
                    self._send_json(200, snap)
                    return

                if route == "/api/stream":
                    ok_sec, _, session_rec, auth_token_ver = self._check_security(allow_cookie=True)
                    if not ok_sec:
                        return
                    self._handle_sse_stream(parsed, session_rec, auth_token_ver)
                    return

                ok_sec, _, _, _ = self._check_security()
                if not ok_sec:
                    return
                if route == "/health":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "version": __version__,
                            "active_waiters": query_active_waiters(daemon.rendezvous_dir),
                        },
                    )
                    return
                if route == "/mcp":
                    self._send_json(
                        405,
                        {"error": "Method Not Allowed; SSE stream not offered on GET /mcp"},
                        extra_headers={"Allow": "POST"},
                    )
                    return
                self._send_json(404, {"error": "Not found"})

            def do_POST(self) -> None:
                self._body_consumed = False
                route = urllib.parse.urlparse(self.path).path

                if route == "/api/login":
                    if not self._check_host_origin():
                        return
                    ok, payload = self._read_json_body()
                    if not ok:
                        return
                    if not isinstance(payload, dict):
                        self._send_json(400, {"error": "Expected JSON object"})
                        return
                    supplied = str(payload.get("token") or "").strip()
                    if not supplied:
                        auth_hdr = self.headers.get("Authorization", "")
                        if auth_hdr.startswith("Bearer "):
                            supplied = auth_hdr[7:].strip()
                    active, token_ver = daemon.get_auth_state()
                    if daemon.require_token:
                        if not supplied or not hmac.compare_digest(supplied, active):
                            self._send_json(401, {"error": "Invalid token"})
                            return
                    session = daemon.session_store.create(token_ver)
                    cookie_hdr = (
                        f"{SESSION_COOKIE_NAME}={session.session_id}; "
                        "HttpOnly; SameSite=Strict; Path=/"
                    )
                    self._send_json(
                        200,
                        {"ok": True, "csrf_token": session.csrf_token},
                        extra_headers={"Set-Cookie": cookie_hdr},
                    )
                    return

                if route == "/api/ui-ticket":
                    ok_sec, _, _, auth_token_ver = self._check_security(allow_cookie=False)
                    if not ok_sec:
                        return
                    if not self._consume_optional_body():
                        return
                    ticket = daemon.ticket_store.issue(token_version=auth_token_ver, ttl_sec=30.0)
                    _, cur_ver = daemon.get_auth_state()
                    if daemon.require_token and cur_ver != auth_token_ver:
                        daemon.ticket_store.consume(ticket, auth_token_ver)
                        self._send_json(401, {"error": "Unauthorized"})
                        return
                    auth_url = f"http://{daemon.host}:{daemon.port}/ui/auth?ticket={ticket}"
                    self._send_json(
                        200,
                        {"ok": True, "ticket": ticket, "expires_in": 30, "url": auth_url},
                    )
                    return

                if route == "/api/logout":
                    if not self._check_host_origin():
                        return
                    cookies = parse_cookie_header(self.headers.get("Cookie", ""))
                    sess_id = cookies.get(SESSION_COOKIE_NAME, "")
                    _, cur_ver = daemon.get_auth_state()
                    existing_sess = daemon.session_store.get(sess_id, cur_ver) if sess_id else None
                    if existing_sess is not None:
                        if not self._check_cookie_write_csrf("cookie", existing_sess):
                            return
                    if not self._consume_optional_body():
                        return
                    if existing_sess is not None:
                        daemon.session_store.revoke(existing_sess.session_id)
                        daemon.event_hub.close_for_session(existing_sess.session_id)
                    self._send_json(
                        200,
                        {"ok": True},
                        extra_headers={
                            "Set-Cookie": (
                                f"{SESSION_COOKIE_NAME}=; Max-Age=0; "
                                "HttpOnly; SameSite=Strict; Path=/"
                            )
                        },
                    )
                    return

                if route == "/api/conversations" or (
                    route.startswith("/api/missions/") and route.endswith("/message")
                ):
                    ok_sec, auth_mode, session_rec, _ver = self._check_security(allow_cookie=True)
                    if not ok_sec or auth_mode is None:
                        return
                    if not self._check_cookie_write_csrf(auth_mode, session_rec):
                        return
                    ok, payload = self._read_json_body()
                    if not ok:
                        return
                    if not isinstance(payload, dict):
                        self._send_json(400, {"error": "Expected JSON object"})
                        return
                    target_mid = None
                    if route != "/api/conversations":
                        target_mid = urllib.parse.unquote(route[len("/api/missions/") : -len("/message")])
                    try:
                        res = daemon.ui_send(
                            str(payload.get("text") or ""),
                            payload.get("targets") or [],
                            target_mid,
                            workspace_hint=str(payload.get("workspace") or ""),
                            tier=str(payload.get("tier") or ""),
                        )
                        self._send_json(200, {"committed": True, "result": res})
                    except KeyError as exc:
                        self._send_json(404, {"error": str(exc)})
                    except ValueError as exc:
                        self._send_json(400, {"error": str(exc)})
                    return

                if route.startswith("/api/missions/") and (
                    route.endswith("/title") or route.endswith("/archive") or route.endswith("/unarchive")
                ):
                    ok_sec, auth_mode, session_rec, _ver = self._check_security(allow_cookie=True)
                    if not ok_sec or auth_mode is None:
                        return
                    if not self._check_cookie_write_csrf(auth_mode, session_rec):
                        return
                    ok, payload = self._read_json_body()
                    if not ok:
                        return
                    if not isinstance(payload, dict):
                        self._send_json(400, {"error": "Expected JSON object"})
                        return
                    tail = route.rsplit("/", 1)[1]
                    act_mid = urllib.parse.unquote(route[len("/api/missions/") : -(len(tail) + 1)])
                    st = daemon.dispatcher.store
                    try:
                        if tail == "title":
                            res = st.set_mission_title(act_mid, str(payload.get("title") or ""), actor=USER_PEER)
                        else:
                            res = st.set_mission_archived(act_mid, tail == "archive")
                        try:
                            daemon.publish_state_update("ui_" + tail)
                        except Exception:
                            pass
                        self._send_json(200, {"committed": True, "result": res})
                    except KeyError as exc:
                        self._send_json(404, {"error": str(exc)})
                    except ValueError as exc:
                        self._send_json(400, {"error": str(exc)})
                    return

                if route.startswith("/api/missions/") and route.endswith("/workspace"):
                    ok_sec, auth_mode, session_rec, _ver = self._check_security(allow_cookie=True)
                    if not ok_sec or auth_mode is None:
                        return
                    if not self._check_cookie_write_csrf(auth_mode, session_rec):
                        return
                    ok, payload = self._read_json_body()
                    if not ok:
                        return
                    if not isinstance(payload, dict):
                        self._send_json(400, {"error": "Expected JSON object"})
                        return
                    ws_mid = urllib.parse.unquote(route[len("/api/missions/") : -len("/workspace")])
                    try:
                        res = daemon.dispatcher.store.set_mission_workspace(
                            ws_mid, str(payload.get("path") or ""), actor=USER_PEER
                        )
                        try:
                            daemon.publish_state_update("ui_set_workspace")
                        except Exception:
                            pass
                        self._send_json(200, {"committed": True, "result": res})
                    except KeyError as exc:
                        self._send_json(404, {"error": str(exc)})
                    except ValueError as exc:
                        self._send_json(400, {"error": str(exc)})
                    return

                if route.startswith("/api/preferences/") and route.endswith("/delete"):
                    ok_sec, auth_mode, session_rec, _ver = self._check_security(allow_cookie=True)
                    if not ok_sec or auth_mode is None:
                        return
                    if not self._check_cookie_write_csrf(auth_mode, session_rec):
                        return
                    if not self._consume_optional_body():
                        return
                    raw_id = route[len("/api/preferences/") : -len("/delete")]
                    if not raw_id.isdigit():
                        self._send_json(400, {"error": "Invalid preference id"})
                        return
                    removed = daemon.dispatcher.store.deactivate_preference(int(raw_id))
                    if removed:
                        try:
                            daemon.publish_state_update("ui_delete_preference")
                        except Exception:
                            pass
                    self._send_json(200 if removed else 404, {"committed": removed})
                    return

                if route.startswith("/api/missions/") and route.endswith("/close"):
                    ok_sec, auth_mode, session_rec, _ = self._check_security(allow_cookie=True)
                    if not ok_sec or auth_mode is None:
                        return
                    if not self._check_cookie_write_csrf(auth_mode, session_rec):
                        return
                    mid = urllib.parse.unquote(route[len("/api/missions/") : -len("/close")])
                    ok, payload = self._read_json_body()
                    if not ok:
                        return
                    if not isinstance(payload, dict):
                        self._send_json(400, {"error": "Expected JSON object"})
                        return
                    try:
                        force = _parse_bool(payload, "force", False)
                        client_req_id = payload.get("client_request_id")
                        res = daemon.dispatcher.store.close_mission(
                            mission_id=mid,
                            final_summary=str(payload.get("final_summary") or "Closed via dashboard."),
                            peer_id=None,
                            status=str(payload.get("status") or "completed"),
                            force=force,
                            client_request_id=(
                                str(client_req_id) if client_req_id is not None else None
                            ),
                            caller_scope="dashboard",
                        )
                        if res.get("idempotent_replay"):
                            replay_delivery = _replay_delivery_payload(res, multi=True)
                            self._send_json(
                                200,
                                {
                                    "committed": True,
                                    "result": res,
                                    "delivery": replay_delivery,
                                },
                            )
                            return
                        deliveries = []
                        rec_warnings: List[str] = []
                        for evt in res.get("events", []):
                            eid = int(evt["event_id"])
                            ring_ms = time.time() * 1000.0
                            receipt = _safe_ring_peer(
                                store=daemon.dispatcher.store,
                                home_dir=daemon.rendezvous_dir,
                                peer_id=evt["to_peer"],
                                title="Consilium Mission Closed",
                                body=f"Mission {res['mission_id']} closed ({res['status']})",
                                pickup_prompt=(
                                    f"Use consilium_pickup_inbox(peer_id={evt['to_peer']!r}) "
                                    f"for closed mission {res['mission_id']}."
                                ),
                                event_ids=[eid],
                            )
                            rw = _safe_record_event_delivery(
                                daemon.dispatcher.store, [eid], receipt, ring_sent_at_ms=ring_ms
                            )
                            if rw:
                                rec_warnings.append(rw)
                            deliveries.append(receipt)
                        delivery_summary: Dict[str, Any] = {
                            "deliveries": deliveries,
                            "errors": [d for d in deliveries if d.get("delivery") == "error"],
                        }
                        if delivery_summary["errors"]:
                            delivery_summary["error"] = "; ".join(
                                str(d.get("error", "")) for d in delivery_summary["errors"]
                            )
                        all_warnings = _collect_post_commit_warnings(
                            res.get("post_commit_warnings"), delivery_summary, rec_warnings
                        )
                        try:
                            daemon.publish_state_update("ui_close_mission")
                        except Exception as pub_exc:
                            all_warnings.append(f"publish_state_update: {pub_exc}")
                        if all_warnings:
                            delivery_summary["warnings"] = all_warnings
                        _safe_save_idempotency_delivery(
                            daemon.dispatcher.store, client_req_id, delivery_summary
                        )
                        self._send_json(
                            200,
                            {
                                "committed": True,
                                "result": res,
                                "delivery": delivery_summary,
                            },
                        )
                    except KeyError as exc:
                        self._send_json(404, {"error": str(exc)})
                    except ConflictError as exc:
                        self._send_json(409, {"error": str(exc)})
                    except ValueError as exc:
                        self._send_json(400, {"error": str(exc)})
                    return

                if route.startswith("/api/tasks/") and route.endswith("/requeue"):
                    ok_sec, auth_mode, session_rec, _ = self._check_security(allow_cookie=True)
                    if not ok_sec or auth_mode is None:
                        return
                    if not self._check_cookie_write_csrf(auth_mode, session_rec):
                        return
                    tid = urllib.parse.unquote(route[len("/api/tasks/") : -len("/requeue")])
                    ok, payload = self._read_json_body()
                    if not ok:
                        return
                    if not isinstance(payload, dict):
                        self._send_json(400, {"error": "Expected JSON object"})
                        return
                    try:
                        client_req_id = payload.get("client_request_id")
                        res = daemon.dispatcher.store.requeue_task(
                            task_id=tid,
                            reason=str(payload.get("reason") or "manual_requeue"),
                            client_request_id=(
                                str(client_req_id) if client_req_id is not None else None
                            ),
                            caller_scope="dashboard",
                        )
                        if res.get("idempotent_replay"):
                            replay_delivery = _replay_delivery_payload(res)
                            self._send_json(
                                200,
                                {
                                    "committed": True,
                                    "result": res,
                                    "delivery": replay_delivery,
                                },
                            )
                            return
                        eid = int(res["event"]["event_id"])
                        ring_ms = time.time() * 1000.0
                        delivery = _safe_ring_peer(
                            store=daemon.dispatcher.store,
                            home_dir=daemon.rendezvous_dir,
                            peer_id=res["assignee_peer"],
                            title="Consilium Task Requeued",
                            body=f"Task {tid} returned to queued",
                            pickup_prompt=(
                                f"Use consilium_pickup_inbox(peer_id={res['assignee_peer']!r}) "
                                f"to claim requeued task {tid}."
                            ),
                            event_ids=[eid],
                        )
                        rw = _safe_record_event_delivery(
                            daemon.dispatcher.store, [eid], delivery, ring_sent_at_ms=ring_ms
                        )
                        all_warnings = _collect_post_commit_warnings(
                            res.get("post_commit_warnings"),
                            delivery,
                            [rw] if rw else None,
                        )
                        try:
                            daemon.publish_state_update("ui_requeue_task")
                        except Exception as pub_exc:
                            all_warnings.append(f"publish_state_update: {pub_exc}")
                        if all_warnings:
                            delivery["warnings"] = all_warnings
                        _safe_save_idempotency_delivery(
                            daemon.dispatcher.store, client_req_id, delivery
                        )
                        self._send_json(
                            200,
                            {"committed": True, "result": res, "delivery": delivery},
                        )
                    except KeyError as exc:
                        self._send_json(404, {"error": str(exc)})
                    except ConflictError as exc:
                        self._send_json(409, {"error": str(exc)})
                    except ValueError as exc:
                        self._send_json(400, {"error": str(exc)})
                    return

                ok_sec, _, _, _ = self._check_security()
                if not ok_sec:
                    return
                if route != "/mcp":
                    self._send_json(404, {"error": "Not found"})
                    return
                ok, payload = self._read_json_body()
                if not ok:
                    return
                resp = daemon.dispatcher.handle_payload(payload)
                if resp is None:
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Referrer-Policy", "no-referrer")
                    self.send_header("X-Content-Type-Options", "nosniff")
                    if self.close_connection:
                        self.send_header("Connection", "close")
                    self.end_headers()
                    return
                self._send_json(200, resp)

        return _Handler

    def handle_socket_once(
        self,
        conn_sock: socket.socket,
        client_addr: Tuple[str, int] = ("127.0.0.1", 0),
    ) -> None:
        """Serve HTTP requests directly on an accepted/connected socket (e.g., socketpair)."""
        handler_cls = self._make_handler_class()
        try:
            handler_cls(conn_sock, client_addr, self)
        finally:
            try:
                conn_sock.close()
            except OSError:
                pass

    def start(self) -> Tuple[str, int]:
        """Start AF_UNIX wake broker, .seq rebuild, reaper thread, and HTTP server."""
        rebuild_all_peer_seqs(self.dispatcher.store)
        try:
            sync_local_quotas(self.dispatcher.store)
        except Exception:
            pass
        self._stop_event.clear()
        self.wake_broker.start()
        handler_cls = self._make_handler_class()

        class _ReusableHTTPServer(ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._httpd = _ReusableHTTPServer((self.host, self.port), handler_cls)
        bound_host, bound_port = self._httpd.server_address[:2]
        self.port = int(bound_port)
        self._http_thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="consilium-http-server",
            daemon=True,
        )
        self._http_thread.start()
        self._reaper_thread = threading.Thread(
            target=self._reaper_loop,
            name="consilium-stale-reaper",
            daemon=True,
        )
        self._reaper_thread.start()
        return str(bound_host), self.port

    def stop(self) -> None:
        """Shut down reaper thread, SSE streams, HTTP server, and AF_UNIX wake broker cleanly."""
        self._stop_event.set()
        self.event_hub.close_all()
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=2.0)
            self._reaper_thread = None
        if self._httpd is not None:
            try:
                self._httpd.shutdown()
                self._httpd.server_close()
            except OSError:
                pass
            self._httpd = None
        if self._http_thread is not None:
            self._http_thread.join(timeout=2.0)
            self._http_thread = None
        self.wake_broker.stop()

    def serve_forever(self) -> None:
        """Start daemon and block until KeyboardInterrupt."""
        host, port = self.start()
        sys.stderr.write(
            f"consilium serving on http://{host}:{port}/mcp (socket: {self.wake_broker.sock_path})\n"
        )
        sys.stderr.flush()
        try:
            while self._http_thread is not None and self._http_thread.is_alive():
                self._http_thread.join(timeout=0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()
