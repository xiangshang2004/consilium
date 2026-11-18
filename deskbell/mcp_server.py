"""Dual-transport MCP server (stdio + hardened 127.0.0.1 Streamable HTTP)."""

from __future__ import annotations

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import socket
import sys
import threading
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from deskbell import __version__
from deskbell.models import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    VALID_ISOLATION,
    VALID_MODEL_TIERS,
    VALID_PEERS,
    ensure_auth_token,
    resolve_home_dir,
)
from deskbell.quota import sync_local_quotas
from deskbell.store import BusStore
from deskbell.wakeup import (
    WakeBrokerServer,
    query_active_waiters,
    ring_peer,
    wait_for_doorbell,
)

SUPPORTED_PROTOCOL_VERSION = "2025-03-26"
LOCAL_HOSTS = ("127.0.0.1", "localhost", "::1")

TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "deskbell_status",
        "description": "Inspect peer standby states, active socket waiters, and missions/tasks.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "mission_id": {"type": "string", "description": "Optional mission ID for full details."}
            },
        },
    },
    {
        "name": "deskbell_create_mission",
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
        "name": "deskbell_dispatch_task",
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
            },
        },
    },
    {
        "name": "deskbell_pickup_inbox",
        "description": "Atomically acknowledge unread events and claim queued tasks assigned to peer_id.",
        "inputSchema": {
            "type": "object",
            "required": ["peer_id"],
            "properties": {
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "session_label": {"type": "string"},
                "claim_tasks": {"type": "boolean", "default": True},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
            },
        },
    },
    {
        "name": "deskbell_report_progress",
        "description": "Update task status/progress and optionally ring the coordinator peer.",
        "inputSchema": {
            "type": "object",
            "required": ["task_id", "peer_id", "progress_note"],
            "properties": {
                "task_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "progress_note": {"type": "string"},
                "status": {"type": "string", "default": "running"},
                "worktree_branch": {"type": "string"},
                "notify_coordinator": {"type": "boolean", "default": False},
            },
        },
    },
    {
        "name": "deskbell_complete_task",
        "description": "Mark a task terminal (done/failed/blocked), record summary/artifacts, and ring the coordinator.",
        "inputSchema": {
            "type": "object",
            "required": ["task_id", "peer_id", "result_summary"],
            "properties": {
                "task_id": {"type": "string"},
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "result_summary": {"type": "string"},
                "artifacts": {"type": "array", "items": {"type": "string"}},
                "status": {"type": "string", "enum": ["done", "failed", "blocked"], "default": "done"},
            },
        },
    },
    {
        "name": "deskbell_request_review",
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
            },
        },
    },
    {
        "name": "deskbell_wait",
        "description": "Block silently until a doorbell event arrives for peer_id or timeout expires (max 300s).",
        "inputSchema": {
            "type": "object",
            "required": ["peer_id"],
            "properties": {
                "peer_id": {"type": "string", "enum": list(VALID_PEERS)},
                "timeout_sec": {"type": "number", "minimum": 0.1, "maximum": 300, "default": 45},
                "session_label": {"type": "string"},
                "auto_pickup": {"type": "boolean", "default": True},
            },
        },
    },
    {
        "name": "deskbell_quota",
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
                            "remaining_percent": {"type": "number", "minimum": 0, "maximum": 100},
                            "reset_at": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
]


class DeskBellMCPDispatcher:
    """Routes MCP JSON-RPC 2.0 requests to BusStore and wakeup doorbell logic."""

    def __init__(self, home_dir: Optional[str] = None, default_peer: str = "") -> None:
        self.home_dir = resolve_home_dir(home_dir)
        self.default_peer = default_peer
        self.store = BusStore(self.home_dir)

    def handle_payload(self, payload: Any) -> Optional[Any]:
        """Handle a single JSON-RPC object or batch; return None for pure notifications."""
        if isinstance(payload, list):
            responses = [self._handle_one(item) for item in payload]
            filtered = [r for r in responses if r is not None]
            return filtered or None
        if not isinstance(payload, dict):
            return self._error(None, -32600, "Invalid Request")
        return self._handle_one(payload)

    def _handle_one(self, req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params") or {}

        if not isinstance(method, str):
            return self._error(req_id, -32600, "Invalid Request: missing method")

        if method == "notifications/initialized" or method.startswith("notifications/"):
            return None

        if method == "initialize":
            client_proto = str(params.get("protocolVersion") or SUPPORTED_PROTOCOL_VERSION)
            return self._result(
                req_id,
                {
                    "protocolVersion": client_proto,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "deskbell", "version": __version__},
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
        if name == "deskbell_status":
            mission_id = args.get("mission_id")
            try:
                sync_local_quotas(self.store)
            except Exception:
                pass
            snapshot = self.store.get_mission_snapshot(mission_id=mission_id)
            return {
                "peers": self.store.list_peers(),
                "active_waiters": query_active_waiters(self.home_dir),
                "quotas": self.store.list_quota_windows()["peers"],
                **snapshot,
            }

        if name == "deskbell_create_mission":
            created = self.store.create_mission(
                title=str(args["title"]),
                goal=str(args["goal"]),
                coordinator_peer=str(args["coordinator_peer"]),
                workspace_path=str(args.get("workspace_path", "")),
                isolation_policy=str(args.get("isolation_policy", "worktree")),
                max_rounds=int(args.get("max_rounds", 3)),
                tasks=args.get("tasks") or [],
            )
            mission = created["mission"]
            open_deeplink = bool(args.get("open_deeplink", False))
            tasks_by_peer: Dict[str, List[Dict[str, Any]]] = {}
            for task in created["tasks"]:
                tasks_by_peer.setdefault(task["assignee_peer"], []).append(task)
            deliveries = []
            for assignee, peer_tasks in tasks_by_peer.items():
                task_ids = ", ".join(t["task_id"] for t in peer_tasks)
                tiers = ", ".join(sorted({t["model_tier"] for t in peer_tasks}))
                receipt = ring_peer(
                    store=self.store,
                    home_dir=self.home_dir,
                    peer_id=assignee,
                    title=f"DeskBell: {mission['title']}",
                    body=f"New task(s) {task_ids} ({tiers})",
                    pickup_prompt=(
                        f"Use deskbell_pickup_inbox(peer_id={assignee!r}) "
                        f"to claim task(s) {task_ids} in mission {mission['mission_id']}."
                    ),
                    workspace_path=mission.get("workspace_path", ""),
                    open_deeplink=open_deeplink,
                )
                deliveries.append(receipt)
            return {**created, "deliveries": deliveries}

        if name == "deskbell_dispatch_task":
            dispatched = self.store.dispatch_task(
                mission_id=str(args["mission_id"]),
                assignee_peer=str(args["assignee_peer"]),
                prompt=str(args["prompt"]),
                model_tier=str(args.get("model_tier", "balanced")),
                model_hint=str(args.get("model_hint", "")),
                parallel_fanout=int(args.get("parallel_fanout", 1)),
                worktree_branch=str(args.get("worktree_branch", "")),
            )
            mission = dispatched["mission"]
            task = dispatched["task"]
            delivery = ring_peer(
                store=self.store,
                home_dir=self.home_dir,
                peer_id=task["assignee_peer"],
                title=f"DeskBell: {mission['title']}",
                body=f"Task {task['task_id']} ({task['model_tier']})",
                pickup_prompt=(
                    f"Use deskbell_pickup_inbox(peer_id={task['assignee_peer']!r}) "
                    f"to claim task {task['task_id']} in mission {mission['mission_id']}."
                ),
                workspace_path=mission.get("workspace_path", ""),
                open_deeplink=bool(args.get("open_deeplink", False)),
            )
            return {**dispatched, "delivery": delivery}

        if name == "deskbell_pickup_inbox":
            peer = str(args.get("peer_id") or self.default_peer)
            return self.store.pickup_inbox(
                peer_id=peer,
                session_label=str(args.get("session_label", "")),
                claim_tasks=bool(args.get("claim_tasks", True)),
                limit=int(args.get("limit", 20)),
            )

        if name == "deskbell_report_progress":
            res = self.store.report_progress(
                task_id=str(args["task_id"]),
                peer_id=str(args["peer_id"]),
                progress_note=str(args["progress_note"]),
                status=str(args.get("status", "running")),
                worktree_branch=args.get("worktree_branch"),
                notify_coordinator=bool(args.get("notify_coordinator", False)),
            )
            if res.get("event") and res.get("coordinator_peer"):
                res["delivery"] = ring_peer(
                    store=self.store,
                    home_dir=self.home_dir,
                    peer_id=res["coordinator_peer"],
                    title="DeskBell Progress",
                    body=f"{args['peer_id']} updated {args['task_id']}",
                    pickup_prompt=(
                        f"Use deskbell_pickup_inbox(peer_id={res['coordinator_peer']!r}) "
                        f"to read progress on {args['task_id']}."
                    ),
                )
            return res

        if name == "deskbell_complete_task":
            res = self.store.complete_task(
                task_id=str(args["task_id"]),
                peer_id=str(args["peer_id"]),
                result_summary=str(args["result_summary"]),
                artifacts=args.get("artifacts") or [],
                status=str(args.get("status", "done")),
            )
            coord = res["coordinator_peer"]
            res["delivery"] = ring_peer(
                store=self.store,
                home_dir=self.home_dir,
                peer_id=coord,
                title="DeskBell Task Done",
                body=(
                    f"{args['peer_id']} finished {args['task_id']}"
                    + (" (ALL TASKS FINISHED)" if res["all_tasks_finished"] else "")
                ),
                pickup_prompt=(
                    f"Use deskbell_pickup_inbox(peer_id={coord!r}) to collect results "
                    f"from {args['peer_id']} on {args['task_id']}."
                ),
            )
            return res

        if name == "deskbell_request_review":
            res = self.store.request_review(
                mission_id=str(args["mission_id"]),
                from_peer=str(args["from_peer"]),
                to_peer=str(args["to_peer"]),
                summary=str(args["summary"]),
                question=str(args["question"]),
                task_id=args.get("task_id"),
            )
            target = str(args["to_peer"]) if res["allowed"] else res["coordinator_peer"]
            title = "DeskBell Review Request" if res["allowed"] else "DeskBell Loop Guard"
            body = (
                f"Round {res['round']}/{res['max_rounds']} from {args['from_peer']}"
                if res["allowed"]
                else res["reason"]
            )
            res["delivery"] = ring_peer(
                store=self.store,
                home_dir=self.home_dir,
                peer_id=target,
                title=title,
                body=body,
                pickup_prompt=(
                    f"Use deskbell_pickup_inbox(peer_id={target!r}) to handle {title} "
                    f"for mission {args['mission_id']}."
                ),
            )
            return res

        if name == "deskbell_wait":
            peer = str(args.get("peer_id") or self.default_peer)
            timeout_sec = min(300.0, max(0.1, float(args.get("timeout_sec", 45.0))))
            return wait_for_doorbell(
                store=self.store,
                home_dir=self.home_dir,
                peer_id=peer,
                timeout_sec=timeout_sec,
                session_label=str(args.get("session_label", "")),
                auto_pickup=bool(args.get("auto_pickup", True)),
            )

        if name == "deskbell_quota":
            report_windows = args.get("report_windows") or []
            if report_windows:
                normalized_reports = []
                for item in report_windows:
                    entry = dict(item)
                    entry["source"] = "manual_report"
                    normalized_reports.append(entry)
                self.store.upsert_quota_windows(normalized_reports)
            if bool(args.get("sync_local", True)):
                sync_local_quotas(self.store, user_home=args.get("user_home"))
            return self.store.list_quota_windows(peer_id=args.get("peer_id"))

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
) -> None:
    """Run a newline-delimited JSON-RPC 2.0 MCP server over stdio."""
    in_stream = stdin if stdin is not None else sys.stdin
    out_stream = stdout if stdout is not None else sys.stdout
    dispatcher = DeskBellMCPDispatcher(home_dir=home_dir, default_peer=default_peer)

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


def _extract_hostname(host_header: str) -> str:
    raw = (host_header or "").strip().lower()
    if not raw:
        return ""
    if raw.startswith("["):
        end = raw.find("]")
        return raw[1:end] if end != -1 else raw
    return raw.split(":", 1)[0]


def _is_valid_host(host_header: str) -> bool:
    return _extract_hostname(host_header) in LOCAL_HOSTS


def _is_valid_origin(origin_header: str) -> bool:
    if not origin_header:
        return True
    parsed = urllib.parse.urlparse(origin_header)
    if parsed.scheme not in ("http", "https"):
        return False
    return (parsed.hostname or "").lower() in LOCAL_HOSTS


class DeskBellDaemon:
    """Combined WakeBrokerServer + hardened 127.0.0.1 Streamable HTTP MCP server."""

    def __init__(
        self,
        home_dir: Optional[str] = None,
        host: str = DEFAULT_HTTP_HOST,
        port: int = DEFAULT_HTTP_PORT,
        require_token: bool = False,
    ) -> None:
        self.home_dir = resolve_home_dir(home_dir)
        self.host = host
        self.port = int(port)
        self.require_token = bool(require_token)
        self.auth_token = ensure_auth_token(self.home_dir) if self.require_token else ""
        self.dispatcher = DeskBellMCPDispatcher(home_dir=self.home_dir)
        self.wake_broker = WakeBrokerServer(home_dir=self.home_dir)
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None

    def _make_handler_class(self) -> Any:
        daemon = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _check_security(self) -> bool:
                host_hdr = self.headers.get("Host", "")
                if not _is_valid_host(host_hdr):
                    self._send_json(403, {"error": "Forbidden Host header"})
                    return False
                origin_hdr = self.headers.get("Origin", "")
                if not _is_valid_origin(origin_hdr):
                    self._send_json(403, {"error": "Forbidden cross-origin request"})
                    return False
                if daemon.require_token:
                    auth_hdr = self.headers.get("Authorization", "")
                    expected = f"Bearer {daemon.auth_token}"
                    if not hmac.compare_digest(auth_hdr, expected):
                        self._send_json(401, {"error": "Unauthorized"})
                        return False
                return True

            def _send_json(self, status: int, body: Any) -> None:
                encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Cache-Control", "no-store")
                if self.close_connection:
                    self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(encoded)

            def do_GET(self) -> None:
                if not self._check_security():
                    return
                if self.path.split("?", 1)[0] == "/health":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "version": __version__,
                            "home_dir": daemon.home_dir,
                            "socket_path": daemon.wake_broker.sock_path,
                            "active_waiters": query_active_waiters(daemon.home_dir),
                        },
                    )
                    return
                self._send_json(404, {"error": "Not found"})

            def do_POST(self) -> None:
                if not self._check_security():
                    return
                if self.path.split("?", 1)[0] != "/mcp":
                    self._send_json(404, {"error": "Not found"})
                    return
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > 2 * 1024 * 1024:
                    self._send_json(413, {"error": "Payload too large"})
                    return
                raw = self.rfile.read(length)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except ValueError as exc:
                    self._send_json(
                        400,
                        daemon.dispatcher._error(None, -32700, f"Parse error: {exc}"),
                    )
                    return
                resp = daemon.dispatcher.handle_payload(payload)
                if resp is None:
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
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
        """Start both AF_UNIX wake broker and Streamable HTTP server."""
        try:
            sync_local_quotas(self.dispatcher.store)
        except Exception:
            pass
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
            name="deskbell-http-server",
            daemon=True,
        )
        self._http_thread.start()
        return str(bound_host), self.port

    def stop(self) -> None:
        """Shut down HTTP server and AF_UNIX wake broker cleanly."""
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
            f"deskbell serving on http://{host}:{port}/mcp (socket: {self.wake_broker.sock_path})\n"
        )
        sys.stderr.flush()
        try:
            while self._http_thread is not None and self._http_thread.is_alive():
                self._http_thread.join(timeout=0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()
