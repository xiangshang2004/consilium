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

from consilium.mcp_server import ConsiliumDaemon, run_stdio_server


def _raw_http_roundtrip(
    daemon: ConsiliumDaemon,
    method: str,
    path: str,
    headers: Dict[str, str],
    body: bytes = b"",
) -> Tuple[int, Dict[str, str], bytes]:
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
        resp_headers = {k.lower(): v for k, v in resp.getheaders()}
        return resp.status, resp_headers, payload
    finally:
        try:
            cli_sock.close()
        except OSError:
            pass
        srv_thread.join(timeout=2.0)


class MCPTransportsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="dbell-mstate-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="dbell-mrv-", dir="/tmp")
        os.environ["CONSILIUM_DRY_RUN"] = "1"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)

    def test_stdio_lifecycle_protocol_negotiation_and_close_mission(self) -> None:
        requests = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2099-01-01"}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "consilium_create_mission",
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
                    "name": "consilium_pickup_inbox",
                    "arguments": {"peer_id": "chatgpt", "session_label": "codex-main"},
                },
            },
        ]
        stdin_buf = io.StringIO("\n".join(json.dumps(r) for r in requests) + "\n")
        stdout_buf = io.StringIO()

        run_stdio_server(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            stdin=stdin_buf,
            stdout=stdout_buf,
        )
        lines = [json.loads(line) for line in stdout_buf.getvalue().strip().splitlines()]

        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0]["id"], 1)
        self.assertEqual(lines[0]["result"]["serverInfo"]["name"], "consilium")
        # Unknown client protocolVersion must not be blindly echoed.
        self.assertEqual(lines[0]["result"]["protocolVersion"], "2025-03-26")

        tool_names = [t["name"] for t in lines[1]["result"]["tools"]]
        self.assertEqual(len(tool_names), 17)
        for added in ("consilium_preferences", "consilium_learn_preference", "consilium_handoff_task", "consilium_set_workspace", "consilium_set_title", "consilium_bind_session"):
            self.assertIn(added, tool_names)
        self.assertIn("consilium_create_mission", tool_names)
        self.assertIn("consilium_wait", tool_names)
        self.assertIn("consilium_requeue_task", tool_names)
        self.assertIn("consilium_quota", tool_names)
        self.assertIn("consilium_close_mission", tool_names)

        mission_payload = lines[2]["result"]["structuredContent"]
        self.assertEqual(mission_payload["deliveries"][0]["delivery"], "cold_notification")
        mission_id = mission_payload["mission"]["mission_id"]

        pickup_payload = lines[3]["result"]["structuredContent"]
        self.assertEqual(len(pickup_payload["claimed_tasks"]), 1)
        self.assertEqual(pickup_payload["claimed_tasks"][0]["status"], "claimed")
        attempt_id = pickup_payload["claimed_tasks"][0]["attempt_id"]
        self.assertTrue(attempt_id)

        # Complete and close mission via stdio
        req2 = [
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "consilium_complete_task",
                    "arguments": {
                        "task_id": pickup_payload["claimed_tasks"][0]["task_id"],
                        "peer_id": "chatgpt",
                        "attempt_id": attempt_id,
                        "result_summary": "Done",
                    },
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "consilium_close_mission",
                    "arguments": {
                        "mission_id": mission_id,
                        "peer_id": "claude",
                        "final_summary": "All complete",
                    },
                },
            },
        ]
        stdin2 = io.StringIO("\n".join(json.dumps(r) for r in req2) + "\n")
        stdout2 = io.StringIO()
        run_stdio_server(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            stdin=stdin2,
            stdout=stdout2,
        )
        lines2 = [json.loads(line) for line in stdout2.getvalue().strip().splitlines()]
        self.assertEqual(lines2[1]["result"]["structuredContent"]["status"], "completed")

    def test_http_default_token_security_headers_and_405(self) -> None:
        # require_token defaults to True!
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.assertTrue(daemon.require_token)
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}).encode("utf-8")

        # 1. Missing token -> 401
        status1, _, _ = _raw_http_roundtrip(daemon, "GET", "/health", {})
        self.assertEqual(status1, 401)

        # 2. Cross-origin Origin header -> 403
        status2, _, _ = _raw_http_roundtrip(
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
        status3, _, _ = _raw_http_roundtrip(
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

        # 4. Authorized local POST /mcp via Bearer header -> 200
        status4, _, raw4 = _raw_http_roundtrip(
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

        # 5. ?token= query parameter authentication must be rejected with 401
        status5, _, _ = _raw_http_roundtrip(
            daemon,
            "POST",
            f"/mcp?token={daemon.auth_token}",
            {"Content-Type": "application/json"},
            body=body,
        )
        self.assertEqual(status5, 401)

        # 6. GET /mcp -> 405 with Allow: POST (per Streamable HTTP spec)
        status6, hdrs6, _ = _raw_http_roundtrip(
            daemon,
            "GET",
            "/mcp",
            {"Authorization": f"Bearer {daemon.auth_token}"},
        )
        self.assertEqual(status6, 405)
        self.assertEqual(hdrs6.get("allow"), "POST")

        # 7. Authorized GET /health -> 200 without leaking raw filesystem paths
        status7, _, raw7 = _raw_http_roundtrip(
            daemon,
            "GET",
            "/health",
            {"Authorization": f"Bearer {daemon.auth_token}"},
        )
        self.assertEqual(status7, 200)
        health = json.loads(raw7.decode("utf-8"))
        self.assertTrue(health["ok"])
        self.assertIn("active_waiters", health)
        self.assertNotIn("home_dir", health)
        self.assertNotIn("socket_path", health)


if __name__ == "__main__":
    unittest.main()
