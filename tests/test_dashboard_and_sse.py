"""Tests for Consilium dashboard UI (/ui), /api/state, cookie login, write actions, and ordered SSE."""

from __future__ import annotations

import http.client
import json
import os
import shutil
import socket
import tempfile
import threading
import time
import unittest

from consilium.mcp_server import ConsiliumDaemon
from tests.test_mcp_transports import _raw_http_roundtrip


class DashboardAndSSETests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="dbell-uistate-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="dbell-uirv-", dir="/tmp")
        self.tmp_home = tempfile.mkdtemp(prefix="dbell-uihome-", dir="/tmp")
        self._prev_dry = os.environ.get("CONSILIUM_DRY_RUN")
        self._prev_home = os.environ.get("CONSILIUM_USER_HOME")
        os.environ["CONSILIUM_DRY_RUN"] = "1"
        os.environ["CONSILIUM_USER_HOME"] = self.tmp_home
        self.daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )

    def tearDown(self) -> None:
        self.daemon.stop()
        if self._prev_dry is None:
            os.environ.pop("CONSILIUM_DRY_RUN", None)
        else:
            os.environ["CONSILIUM_DRY_RUN"] = self._prev_dry
        if self._prev_home is None:
            os.environ.pop("CONSILIUM_USER_HOME", None)
        else:
            os.environ["CONSILIUM_USER_HOME"] = self._prev_home
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def test_ui_offline_html_csp_and_host_origin_checks(self) -> None:
        st_redir, hdrs_redir, _ = _raw_http_roundtrip(self.daemon, "GET", "/", {})
        self.assertEqual(st_redir, 302)
        self.assertEqual(hdrs_redir.get("location"), "/ui")

        st, hdrs, raw = _raw_http_roundtrip(self.daemon, "GET", "/ui", {})
        self.assertEqual(st, 200)
        self.assertIn("text/html", hdrs.get("content-type", ""))
        self.assertIn("default-src 'none'", hdrs.get("content-security-policy", ""))
        html = raw.decode("utf-8")
        self.assertIn("Consilium", html)
        self.assertIn("tabular-nums", html)
        self.assertIn("data-set-theme=\"light\"", html)
        self.assertIn("data-set-theme=\"dark\"", html)
        # Verify zero external CDN links
        self.assertNotIn("https://", html)
        self.assertNotIn("http://", html)

        # Bad Origin on /ui rejected with 403
        st_bad, _, _ = _raw_http_roundtrip(
            self.daemon, "GET", "/ui", {"Origin": "https://evil.example"}
        )
        self.assertEqual(st_bad, 403)

    def test_cookie_login_api_state_shape_and_logout(self) -> None:
        # Unauthenticated /api/state -> 401
        st0, _, _ = _raw_http_roundtrip(self.daemon, "GET", "/api/state", {})
        self.assertEqual(st0, 401)

        # Wrong token -> 401
        st_bad, _, _ = _raw_http_roundtrip(
            self.daemon,
            "POST",
            "/api/login",
            {"Content-Type": "application/json"},
            body=json.dumps({"token": "wrong"}).encode("utf-8"),
        )
        self.assertEqual(st_bad, 401)

        # Valid token -> 200 with HttpOnly + SameSite=Strict cookie
        st_ok, hdrs_ok, _ = _raw_http_roundtrip(
            self.daemon,
            "POST",
            "/api/login",
            {"Content-Type": "application/json"},
            body=json.dumps({"token": self.daemon.auth_token}).encode("utf-8"),
        )
        self.assertEqual(st_ok, 200)
        set_cookie = hdrs_ok.get("set-cookie", "")
        self.assertIn("consilium_session=", set_cookie)
        self.assertIn("HttpOnly", set_cookie)
        self.assertIn("SameSite=Strict", set_cookie)
        cookie_pair = set_cookie.split(";", 1)[0]

        # Seed a mission so /api/state has full data
        self.daemon.dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "consilium_create_mission",
                    "arguments": {
                        "title": "Dashboard API test",
                        "goal": "Inspect snapshot shape",
                        "coordinator_peer": "claude",
                        "tasks": [
                            {"assignee_peer": "chatgpt", "prompt": "Diagnose", "model_tier": "fast"}
                        ],
                    },
                },
            }
        )

        # Authenticate /api/state via session cookie!
        st_state, _, raw_state = _raw_http_roundtrip(
            self.daemon, "GET", "/api/state", {"Cookie": cookie_pair}
        )
        self.assertEqual(st_state, 200)
        state = json.loads(raw_state.decode("utf-8"))
        self.assertIn("daemon", state)
        self.assertIn("uptime_seconds", state["daemon"])
        self.assertEqual(len(state["peers"]), 3)
        self.assertEqual(len(state["quotas"]), 3)
        # Missing quota data must remain null (never 100%)
        for q in state["quotas"]:
            self.assertIsNone(q["min_remaining_percent"])
            self.assertIsNone(q["dispatch_recommended"])
        self.assertEqual(len(state["missions"]), 1)
        m = state["missions"][0]
        self.assertEqual(len(m["tasks"]), 1)
        self.assertGreaterEqual(len(m["events"]), 1)
        self.assertEqual(m["events"][0]["payload"]["delivery_mode"], "cold_notification")

        # Logout clears cookie
        st_out, hdrs_out, _ = _raw_http_roundtrip(self.daemon, "POST", "/api/logout", {})
        self.assertEqual(st_out, 200)
        self.assertIn("Max-Age=0", hdrs_out.get("set-cookie", ""))

    def test_dashboard_requeue_and_close_actions(self) -> None:
        auth = {"Authorization": f"Bearer {self.daemon.auth_token}", "Content-Type": "application/json"}
        created = self.daemon.dispatcher.store.create_mission(
            title="Action test",
            goal="Test requeue and close",
            coordinator_peer="claude",
            tasks=[{"assignee_peer": "antigravity", "prompt": "Fix bug"}],
        )
        mid = created["mission"]["mission_id"]
        tid = created["tasks"][0]["task_id"]
        self.daemon.dispatcher.store.pickup_inbox("antigravity", claim_tasks=True)

        # Requeue task via dashboard API
        st_req, _, raw_req = _raw_http_roundtrip(
            self.daemon,
            "POST",
            f"/api/tasks/{tid}/requeue",
            auth,
            body=json.dumps({"reason": "ui_retry"}).encode("utf-8"),
        )
        self.assertEqual(st_req, 200)
        req_body = json.loads(raw_req.decode("utf-8"))
        self.assertTrue(req_body["committed"])
        self.assertEqual(req_body["result"]["task"]["status"], "queued")

        # Close mission (force=True since task is queued) via dashboard API
        st_cls, _, raw_cls = _raw_http_roundtrip(
            self.daemon,
            "POST",
            f"/api/missions/{mid}/close",
            auth,
            body=json.dumps(
                {"final_summary": "## Done\nClosed in test.", "status": "completed", "force": True}
            ).encode("utf-8"),
        )
        self.assertEqual(st_cls, 200)
        cls_body = json.loads(raw_cls.decode("utf-8"))
        self.assertTrue(cls_body["committed"])
        self.assertEqual(cls_body["result"]["status"], "completed")

    def test_sse_stream_pushes_ordered_updates(self) -> None:
        srv_sock, cli_sock = socket.socketpair()
        srv_thread = threading.Thread(
            target=self.daemon.handle_socket_once, args=(srv_sock,), daemon=True
        )
        srv_thread.start()

        def _mutate_sequence() -> None:
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and self.daemon.event_hub.subscriber_count() == 0:
                time.sleep(0.01)
            time.sleep(0.03)
            c = self.daemon.dispatcher.handle_payload(
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "consilium_create_mission",
                        "arguments": {
                            "title": "SSE order test",
                            "goal": "Push ordered frames",
                            "coordinator_peer": "claude",
                            "tasks": [{"assignee_peer": "chatgpt", "prompt": "Step 1"}],
                        },
                    },
                }
            )["result"]["structuredContent"]
            tid = c["tasks"][0]["task_id"]
            self.daemon.dispatcher.handle_payload(
                {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "tools/call",
                    "params": {
                        "name": "consilium_complete_task",
                        "arguments": {
                            "task_id": tid,
                            "peer_id": "chatgpt",
                            "result_summary": "Finished step 1",
                        },
                    },
                }
            )

        mutator = threading.Thread(target=_mutate_sequence, daemon=True)
        mutator.start()

        try:
            raw_req = (
                "GET /api/stream?max_events=3 HTTP/1.1\r\n"
                "Host: 127.0.0.1:18920\r\n"
                f"Authorization: Bearer {self.daemon.auth_token}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            cli_sock.sendall(raw_req)
            resp = http.client.HTTPResponse(cli_sock)
            resp.begin()
            self.assertEqual(resp.status, 200)
            self.assertIn("text/event-stream", resp.getheader("Content-Type", ""))
            body = resp.read().decode("utf-8")
        finally:
            try:
                cli_sock.close()
            except OSError:
                pass
            mutator.join(timeout=3.0)
            srv_thread.join(timeout=3.0)

        blocks = [b.strip() for b in body.strip().split("\n\n") if b.strip()]
        self.assertEqual(len(blocks), 3)
        events = []
        for blk in blocks:
            ev_name = ""
            ev_id = ""
            ev_data = ""
            for line in blk.splitlines():
                if line.startswith("event: "):
                    ev_name = line[7:].strip()
                elif line.startswith("id: "):
                    ev_id = line[4:].strip()
                elif line.startswith("data: "):
                    ev_data = line[6:].strip()
            events.append((ev_name, int(ev_id), json.loads(ev_data)))

        self.assertEqual(events[0][0], "state")
        self.assertEqual(events[1][0], "update")
        self.assertEqual(events[2][0], "update")
        self.assertEqual(events[1][2]["reason"], "create_mission")
        self.assertEqual(events[2][2]["reason"], "complete_task")
        self.assertLess(events[1][1], events[2][1])

    def test_sse_detects_waiter_change_and_latency_only_stamps_unacked(self) -> None:
        from consilium.wakeup import _acquire_lease, _release_lease

        # Verify record_peer_wakeup_latency ignores already-acked events.
        created = self.daemon.dispatcher.store.create_mission(
            title="Latency stamp check",
            goal="Only stamp unacked events",
            coordinator_peer="claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
        )
        first_eid = int(created["events"][0]["event_id"])
        self.daemon.dispatcher.store.record_peer_wakeup_latency("chatgpt", time.time() * 1000.0)
        self.daemon.dispatcher.store.pickup_inbox("chatgpt", claim_tasks=False)
        before_missions, _ = self.daemon.dispatcher.store.get_dashboard_missions()
        first_ev_before = [e for e in before_missions[0]["events"] if e["event_id"] == first_eid][0]
        first_lat = first_ev_before["payload"].get("wakeup_latency_ms")
        self.assertIsNotNone(first_lat)
        time.sleep(0.05)
        self.daemon.dispatcher.store.record_peer_wakeup_latency("chatgpt", time.time() * 1000.0)
        after_missions, _ = self.daemon.dispatcher.store.get_dashboard_missions()
        first_ev_after = [e for e in after_missions[0]["events"] if e["event_id"] == first_eid][0]
        self.assertEqual(first_ev_after["payload"].get("wakeup_latency_ms"), first_lat)

        # Verify SSE detects a DB-free standby waiter lease change.
        srv_sock, cli_sock = socket.socketpair()
        srv_thread = threading.Thread(
            target=self.daemon.handle_socket_once, args=(srv_sock,), daemon=True
        )
        srv_thread.start()

        lease_holder: list = []

        def _enter_wait() -> None:
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and self.daemon.event_hub.subscriber_count() == 0:
                time.sleep(0.01)
            time.sleep(0.05)
            fd, path = _acquire_lease(self.tmp_rv, "claude", time.time() + 10.0, "cli-wait")
            lease_holder.append((fd, path))

        waiter_thread = threading.Thread(target=_enter_wait, daemon=True)
        waiter_thread.start()
        try:
            raw_req = (
                "GET /api/stream?max_events=2 HTTP/1.1\r\n"
                "Host: 127.0.0.1:18920\r\n"
                f"Authorization: Bearer {self.daemon.auth_token}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            cli_sock.sendall(raw_req)
            resp = http.client.HTTPResponse(cli_sock)
            resp.begin()
            self.assertEqual(resp.status, 200)
            body = resp.read().decode("utf-8")
        finally:
            try:
                cli_sock.close()
            except OSError:
                pass
            waiter_thread.join(timeout=3.0)
            for fd, path in lease_holder:
                _release_lease(fd, path)
            srv_thread.join(timeout=3.0)

        blocks = [b.strip() for b in body.strip().split("\n\n") if b.strip()]
        self.assertEqual(len(blocks), 2)
        second_data = None
        for line in blocks[1].splitlines():
            if line.startswith("data: "):
                second_data = json.loads(line[6:].strip())
        self.assertIsNotNone(second_data)
        self.assertEqual(second_data["reason"], "waiter_change")
        claude_peer = [p for p in second_data["state"]["peers"] if p["peer_id"] == "claude"][0]
        self.assertEqual(claude_peer["status"], "standby")

    def test_ui_one_time_ticket_auto_login_and_burn_after_read(self) -> None:
        # Unauthenticated request to mint a UI ticket must be rejected with 401.
        st_unauth, _, _ = _raw_http_roundtrip(self.daemon, "POST", "/api/ui-ticket", {})
        self.assertEqual(st_unauth, 401)

        # Bearer-authenticated CLI mints a single-use ticket.
        st_t, _, raw_t = _raw_http_roundtrip(
            self.daemon,
            "POST",
            "/api/ui-ticket",
            {"Authorization": f"Bearer {self.daemon.auth_token}"},
        )
        self.assertEqual(st_t, 200)
        ticket_body = json.loads(raw_t.decode("utf-8"))
        ticket = ticket_body["ticket"]
        self.assertTrue(ticket)
        self.assertIn(f"/ui/auth?ticket={ticket}", ticket_body["url"])

        # First redemption redirects to clean /ui and sets HttpOnly + SameSite=Strict cookie.
        st_auth, hdrs_auth, _ = _raw_http_roundtrip(
            self.daemon, "GET", f"/ui/auth?ticket={ticket}", {}
        )
        self.assertEqual(st_auth, 302)
        self.assertEqual(hdrs_auth.get("location"), "/ui")
        self.assertEqual(hdrs_auth.get("cache-control"), "no-store")
        set_cookie = hdrs_auth.get("set-cookie", "")
        self.assertIn("consilium_session=", set_cookie)
        self.assertIn("HttpOnly", set_cookie)
        self.assertIn("SameSite=Strict", set_cookie)

        # The issued cookie grants immediate access to /api/state without manual token entry.
        cookie_pair = set_cookie.split(";", 1)[0]
        st_state, _, _ = _raw_http_roundtrip(
            self.daemon, "GET", "/api/state", {"Cookie": cookie_pair}
        )
        self.assertEqual(st_state, 200)

        # Replaying the exact same ticket MUST fail (no Set-Cookie issued).
        st_replay, hdrs_replay, _ = _raw_http_roundtrip(
            self.daemon, "GET", f"/ui/auth?ticket={ticket}", {}
        )
        self.assertEqual(st_replay, 302)
        self.assertEqual(hdrs_replay.get("location"), "/ui")
        self.assertNotIn("set-cookie", hdrs_replay)


if __name__ == "__main__":
    unittest.main()
