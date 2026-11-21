"""Regression tests for v0.7.4 security, stability, privacy, and portability hardening."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from consilium.antigravity_models import models_for_tasks
from consilium.attachments import MAX_IMAGE_BYTES
from consilium.dashboard import compute_state_fingerprint, render_dashboard_html
from consilium.mcp_server import ConsiliumDaemon
from consilium.models import USER_PEER
from consilium.quota import _hash_account_ref, normalize_remaining_pct, normalize_ts_to_iso
from consilium.store import BusStore
from consilium.wakeup import notify_peer, read_peer_seq
from scripts.install import run_installer
from tests.test_mcp_transports import _raw_http_roundtrip


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class V074HardeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="consilium-v074-st-", dir=tempfile.gettempdir())
        self.tmp_rv = tempfile.mkdtemp(prefix="consilium-v074-rv-", dir=tempfile.gettempdir())
        self.addCleanup(shutil.rmtree, self.tmp_state, True)
        self.addCleanup(shutil.rmtree, self.tmp_rv, True)

    def test_hash_account_ref_rejects_spoofed_15_char_email(self) -> None:
        self.assertNotIn("@", _hash_account_ref("acct-1@test.com", always_hash=True))
        self.assertTrue(_hash_account_ref("acct-1@test.com", always_hash=True).startswith("acct-"))
        valid_hashed = "acct-0123456789"
        self.assertEqual(_hash_account_ref(valid_hashed, always_hash=True), valid_hashed)

    def test_quota_numeric_guards_handle_nan_inf_and_overflow(self) -> None:
        self.assertIsNone(normalize_remaining_pct(remaining=float("nan")))
        self.assertIsNone(normalize_remaining_pct(remaining=float("inf")))
        self.assertIsNone(normalize_remaining_pct(used=float("-inf"), is_fraction=True))
        self.assertEqual(normalize_ts_to_iso(float("nan")), "")
        self.assertEqual(normalize_ts_to_iso(float("inf")), "")
        self.assertEqual(normalize_ts_to_iso(1e20), "")

    def test_http_unsupported_methods_duplicate_host_and_oversized_upload(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        auth = {"Authorization": f"Bearer {daemon.auth_token}"}

        for method in ("PUT", "DELETE", "PATCH", "OPTIONS"):
            status, hdrs, _ = _raw_http_roundtrip(daemon, method, "/health", auth)
            self.assertEqual(status, 405, method)
            self.assertEqual(hdrs.get("x-content-type-options"), "nosniff")
            self.assertEqual(hdrs.get("connection"), "close")
            self.assertEqual(hdrs.get("allow"), "GET, POST")

        st_bad_port, _, _ = _raw_http_roundtrip(
            daemon, "GET", "/health", {**auth, "Host": "127.0.0.1:notaport"}
        )
        self.assertEqual(st_bad_port, 403)

        # Oversized image upload header (> MAX_IMAGE_BYTES) rejected cleanly with 413.
        too_big = {
            **auth,
            "Content-Type": "application/octet-stream",
            "Content-Length": str(MAX_IMAGE_BYTES + 1),
        }
        import socket
        import threading
        srv_s, cli_s = socket.socketpair()
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()
        cli_s.sendall(
            (
                f"POST /api/attachments/{'aa' * 16} HTTP/1.1\r\n"
                f"Host: 127.0.0.1:18920\r\n"
                f"Authorization: Bearer {daemon.auth_token}\r\n"
                f"Content-Length: {too_big['Content-Length']}\r\n\r\n"
            ).encode("ascii")
        )
        raw_resp = b""
        while True:
            chunk = cli_s.recv(4096)
            if not chunk:
                break
            raw_resp += chunk
        cli_s.close()
        t.join(timeout=2.0)
        self.assertTrue(raw_resp.startswith(b"HTTP/1.1 413 "))

    def test_release_stale_claims_ui_mission_supports_queued_notification_and_clears_meta(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        store = daemon.dispatcher.store
        with patch("consilium.mcp_server.sync_local_quotas"), patch("consilium.mcp_server._safe_ring_peer", return_value={}):
            res = daemon.ui_send("UI stale test", ["claude"])
        mid = res["mission_id"]
        picked = store.pickup_inbox("claude", mission_id=mid, session_label="dead-s")
        tid = picked["claimed_tasks"][0]["task_id"]
        aid = picked["claimed_tasks"][0]["attempt_id"]
        store.report_progress(
            tid, "claude", "working", attempt_id=aid, status="running",
            model_used="claude-opus-4-6", effort_used="high", tier_used="deep", tier_reason="reason"
        )
        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "UPDATE tasks SET status = 'claimed', claimed_at = '2020-01-01T00:00:00Z' WHERE task_id = ?",
                (tid,),
            )
            conn.commit()
        released = store.release_stale_claims(timeout_sec=60)
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["model_used"], "")
        self.assertEqual(released[0]["effort_used"], "")
        self.assertEqual(released[0]["tier_used"], "")
        self.assertEqual(released[0]["tier_reason"], "")
        # UI queued_notification must locate the requeued event targeted at assignee_peer!
        qnote = store.queued_notification(tid)
        self.assertEqual(qnote["assignee_peer"], "claude")
        self.assertGreater(qnote["event_id"], 0)

    def test_notify_peer_without_store_preserves_scoped_seq_fields(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        store.sync_peer_seq("claude")
        before = read_peer_seq(self.tmp_rv, "claude")
        notify_peer(self.tmp_rv, "claude", store=None)
        after = read_peer_seq(self.tmp_rv, "claude")
        self.assertEqual(after["seq"], before["seq"] + 1)
        self.assertEqual(after["ui_seq"], before["ui_seq"] + 1)
        self.assertEqual(after["ui_unacked"], before["ui_unacked"])
        self.assertEqual(after["ui_queued"], before["ui_queued"])

    def test_fingerprint_includes_preferences_and_version_escaped(self) -> None:
        html = render_dashboard_html('<script>alert("x")</script>')
        self.assertNotIn('<script>alert("x")</script>', html)
        self.assertIn("&lt;script&gt;", html)

        base = {"peers": [], "active_waiters": {}, "quotas": [], "missions": [], "preferences": [], "latest_event_id": 1}
        fp1 = compute_state_fingerprint(base)
        fp2 = compute_state_fingerprint({**base, "preferences": [{"pref_id": 1, "text": "prefer codex"}]})
        self.assertNotEqual(fp1, fp2)

    def test_antigravity_single_report_attributes_preceding_planner_steps(self) -> None:
        brain = Path(self.tmp_state) / "brain"
        logs = brain / "sess1" / ".system_generated" / "logs"
        logs.mkdir(parents=True)
        (logs / "transcript_full.jsonl").write_text(
            "\n".join([
                json.dumps({
                    "type": "PLANNER_RESPONSE", "source": "MODEL",
                    "model": "gemini-pro-orchestrator-neon", "tool_calls": [],
                }),
                json.dumps({
                    "type": "PLANNER_RESPONSE", "source": "MODEL",
                    "model": "gemini-flash-worker-agy",
                    "tool_calls": [{
                        "name": "call_mcp_tool",
                        "args": {
                            "ServerName": "consilium",
                            "ToolName": "consilium_complete_task",
                            "Arguments": {"peer_id": "antigravity", "task_id": "T-9", "attempt_id": "A-1"},
                        },
                    }],
                }),
            ]) + "\n",
            encoding="utf-8",
        )
        out = models_for_tasks(
            [{"assignee_peer": "antigravity", "task_id": "T-9", "attempt_id": "A-1"}],
            brain_dir=brain,
        )
        self.assertEqual(out[("T-9", "A-1")], "Gemini 4 Neon（规划） · Gemini 4 Hydrogen（执行）")

    def test_installer_port_range_and_cli_outside_repo_rotate_token_error(self) -> None:
        with self.assertRaises(ValueError):
            run_installer(
                mode="dry-run",
                target_home=self.tmp_state,
                repo_dir=REPO_ROOT,
                port=70000,
                rendezvous_dir=self.tmp_rv,
                claude_running_check=lambda: False,
            )
        # Running rotate-token via copied package without scripts/ returns clean exit code 1 (no traceback)
        pkg_tmp = os.path.join(self.tmp_rv, "pkgonly")
        shutil.copytree(os.path.join(REPO_ROOT, "consilium"), os.path.join(pkg_tmp, "consilium"),
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        env = dict(os.environ)
        env.pop("CLAUDE_CONFIG_DIR", None)
        env["PYTHONPATH"] = pkg_tmp
        proc = subprocess.run(
            [sys.executable, "-B", "-m", "consilium.cli", "rotate-token", "--data-dir", self.tmp_state],
            cwd=self.tmp_rv,
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 1)
        self.assertNotIn("Traceback", proc.stderr)
        err = json.loads(proc.stderr)
        self.assertIn("scripts/install.py", err["error"])


if __name__ == "__main__":
    unittest.main()
