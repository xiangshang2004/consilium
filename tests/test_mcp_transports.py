"""Tests for stdio and wire-level socketpair Streamable HTTP MCP transports."""

from __future__ import annotations

import http.client
import io
import json
import os
import shutil
import socket
import tempfile
import threading
import unittest
from typing import Dict, Tuple

from deskbell.mcp_server import DeskBellDaemon, run_stdio_server


def _raw_http_roundtrip(
    daemon: DeskBellDaemon,
    method: str,
    path: str,
    headers: Dict[str, str],
    body: bytes = b"",
) -> Tuple[int, bytes]:
    srv_sock, cli_sock = socket.socketpair()
    srv_thread = threading.Thread(
        target=daemon.handle_socket_once, args=(srv_sock,), daemon=True
    )
    srv_thread.start()
    try:
        req_headers = {"Host": "127.0.0.1:18920", "Connection": "close"}
        req_headers.update(headers)
        if body:
            req_headers["Content-Length"] = str(len(body))
        lines = [f"{method} {path} HTTP/1.1"]
        for k, v in req_headers.items():
            lines.append(f"{k}: {v}")
        raw_req = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + body
        cli_sock.sendall(raw_req)

        resp = http.client.HTTPResponse(cli_sock)
        resp.begin()
        payload = resp.read()
        return resp.status, payload
    finally:
        try:
            cli_sock.close()
        except OSError:
            pass
        srv_thread.join(timeout=2.0)


class MCPTransportsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_home = tempfile.mkdtemp(prefix="dbell-mcp-", dir="/tmp")
        os.environ["DESKBELL_DRY_RUN"] = "1"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def test_stdio_lifecycle_and_tools(self) -> None:
        requests = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "deskbell_create_mission",
                    "arguments": {
                        "title": "Stdio mission",
                        "goal": "Test stdio roundtrip",
                        "coordinator_peer": "claude",
                        "tasks": [
                            {"assignee_peer": "chatgpt", "prompt": "Check edge cases", "model_tier": "pro_reasoning"}
                        ],
                    },
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "deskbell_pickup_inbox",
                    "arguments": {"peer_id": "chatgpt", "session_label": "codex-main"},
                },
            },
        ]
        stdin_buf = io.StringIO("\n".join(json.dumps(r) for r in requests) + "\n")
        stdout_buf = io.StringIO()

        run_stdio_server(home_dir=self.tmp_home, stdin=stdin_buf, stdout=stdout_buf)
        lines = [json.loads(line) for line in stdout_buf.getvalue().strip().splitlines()]

        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0]["id"], 1)
        self.assertEqual(lines[0]["result"]["serverInfo"]["name"], "deskbell")

        tool_names = [t["name"] for t in lines[1]["result"]["tools"]]
        self.assertEqual(len(tool_names), 9)
        self.assertIn("deskbell_create_mission", tool_names)
        self.assertIn("deskbell_wait", tool_names)
        self.assertIn("deskbell_quota", tool_names)

        mission_payload = lines[2]["result"]["structuredContent"]
        self.assertEqual(mission_payload["deliveries"][0]["delivery"], "cold_notification")

        pickup_payload = lines[3]["result"]["structuredContent"]
        self.assertEqual(len(pickup_payload["claimed_tasks"]), 1)
        self.assertEqual(pickup_payload["claimed_tasks"][0]["status"], "claimed")

    def test_http_security_headers_and_token_enforcement(self) -> None:
        daemon = DeskBellDaemon(
            home_dir=self.tmp_home, host="127.0.0.1", port=18920, require_token=True
        )
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}).encode("utf-8")

        # 1. Missing Bearer token -> 401
        status1, _ = _raw_http_roundtrip(daemon, "GET", "/health", {})
        self.assertEqual(status1, 401)

        # 2. Cross-origin Origin header -> 403
        status2, _ = _raw_http_roundtrip(
            daemon,
            "POST",
            "/mcp",
            {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {daemon.auth_token}",
                "Origin": "https://evil.example",
            },
            body=body,
        )
        self.assertEqual(status2, 403)

        # 3. Spoofed Host header -> 403
        status3, _ = _raw_http_roundtrip(
            daemon,
            "POST",
            "/mcp",
            {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {daemon.auth_token}",
                "Host": "attacker.example:18920",
            },
            body=body,
        )
        self.assertEqual(status3, 403)

        # 4. Authorized local POST /mcp -> 200
        status4, raw4 = _raw_http_roundtrip(
            daemon,
            "POST",
            "/mcp",
            {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {daemon.auth_token}",
                "Origin": "http://localhost:18920",
            },
            body=body,
        )
        self.assertEqual(status4, 200)
        parsed = json.loads(raw4.decode("utf-8"))
        self.assertEqual(parsed["id"], 1)
        self.assertEqual(parsed["result"], {})

        # 5. Authorized GET /health -> 200
        status5, raw5 = _raw_http_roundtrip(
            daemon,
            "GET",
            "/health",
            {"Authorization": f"Bearer {daemon.auth_token}"},
        )
        self.assertEqual(status5, 200)
        health = json.loads(raw5.decode("utf-8"))
        self.assertTrue(health["ok"])
        self.assertIn("active_waiters", health)


if __name__ == "__main__":
    unittest.main()
