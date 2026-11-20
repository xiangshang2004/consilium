"""Regressions for Gate 1 supplemental fixes and R014 deferred items (R015)."""

from __future__ import annotations

import builtins
import json
import os
import shutil
import socket
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from consilium.mcp_server import ConsiliumDaemon
from consilium.models import read_auth_token
from consilium.quota import sync_local_quotas
from consilium.store import BusStore
from consilium.wakeup import query_active_waiters, rebuild_all_peer_seqs, wait_for_doorbell
import scripts.install as install_mod


def _http_read_response(sock: socket.socket, initial: bytes = b"") -> tuple:
    buf = initial
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    head, body_rest = buf.split(b"\r\n\r\n", 1)
    lines = head.decode("utf-8", errors="replace").split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    headers = {}
    for ln in lines[1:]:
        if ":" in ln:
            k, v = ln.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    clen = int(headers.get("content-length") or 0)
    while len(body_rest) < clen:
        chunk = sock.recv(4096)
        if not chunk:
            break
        body_rest += chunk
    return status, headers, body_rest[:clen], body_rest[clen:]


class R015RegressionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="consilium-r015-st-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="consilium-r015-rv-", dir="/tmp")
        self.tmp_home = tempfile.mkdtemp(prefix="consilium-r015-hm-", dir="/tmp")
        self._prev_env = {
            k: os.environ.get(k)
            for k in (
                "CONSILIUM_DRY_RUN",
                "CONSILIUM_USER_HOME",
                "CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC",
                "CONSILIUM_SOCKET_FALLBACK_DIR",
                "PATH",
            )
        }
        os.environ["CONSILIUM_DRY_RUN"] = "1"
        os.environ["CONSILIUM_USER_HOME"] = self.tmp_home
        os.environ["CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC"] = "1"
        os.environ["CONSILIUM_SOCKET_FALLBACK_DIR"] = os.path.join(self.tmp_rv, "sock")

    def tearDown(self) -> None:
        for key, prev in self._prev_env.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def _write_claude_stub(self) -> str:
        stub_dir = os.path.join(self.tmp_home, "bin")
        os.makedirs(stub_dir, mode=0o700, exist_ok=True)
        claude_path = os.path.join(stub_dir, "claude")
        with open(claude_path, "w", encoding="utf-8") as h:
            h.write("#!/bin/sh\nexit 0\n")
        os.chmod(claude_path, 0o755)
        return stub_dir

    # 1. GET framing: GET with body must drain or close so follow-up GET /health succeeds cleanly
    def test_r015_get_with_body_followed_by_health_on_same_keepalive_connection(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        tok = read_auth_token(self.tmp_state)

        srv_s, cli_s = socket.socketpair()
        cli_s.settimeout(3.0)
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()
        try:
            body1 = b'{"getBody":1}'
            req1 = (
                "GET /health HTTP/1.1\r\n"
                f"Host: {daemon.host}:{daemon.port}\r\n"
                f"Authorization: Bearer {tok}\r\n"
                f"Content-Length: {len(body1)}\r\n"
                "Connection: keep-alive\r\n\r\n"
            ).encode("ascii") + body1
            req2 = (
                "GET /health HTTP/1.1\r\n"
                f"Host: {daemon.host}:{daemon.port}\r\n"
                f"Authorization: Bearer {tok}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            # Pipeline both requests on the same persistent connection
            cli_s.sendall(req1 + req2)

            st1, _, b1, rest = _http_read_response(cli_s)
            self.assertEqual(st1, 200)
            self.assertTrue(json.loads(b1.decode("utf-8"))["ok"])

            st2, _, b2, _ = _http_read_response(cli_s, initial=rest)
            self.assertEqual(st2, 200)
            self.assertTrue(json.loads(b2.decode("utf-8"))["ok"])
        finally:
            cli_s.close()
            t.join(timeout=3.0)

        # GET with Transfer-Encoding must be rejected (501) and close connection instead of desyncing
        srv_te, cli_te = socket.socketpair()
        cli_te.settimeout(3.0)
        t_te = threading.Thread(target=daemon.handle_socket_once, args=(srv_te,), daemon=True)
        t_te.start()
        try:
            cli_te.sendall(
                (
                    "GET /health HTTP/1.1\r\n"
                    f"Host: {daemon.host}:{daemon.port}\r\n"
                    f"Authorization: Bearer {tok}\r\n"
                    "Transfer-Encoding: chunked\r\n"
                    "Connection: keep-alive\r\n\r\n"
                    "0\r\n\r\n"
                ).encode("ascii")
            )
            st_te, hdr_te, _, _ = _http_read_response(cli_te)
            self.assertEqual(st_te, 501)
            self.assertEqual(hdr_te.get("connection", "").lower(), "close")
        finally:
            cli_te.close()
            t_te.join(timeout=3.0)

    # 2. Cockpit newer codex_accounts.sqlite mtime -> distrust JSON current_account_id without opening sqlite
    def test_r015_cockpit_newer_sqlite_distrusts_json_current_without_opening_sqlite(self) -> None:
        cockpit_dir = os.path.join(self.tmp_home, ".antigravity_cockpit")
        os.makedirs(cockpit_dir, mode=0o700, exist_ok=True)
        json_path = os.path.join(cockpit_dir, "codex_accounts.json")
        acct_sqlite_path = os.path.join(cockpit_dir, "codex_accounts.sqlite")

        with open(json_path, "w", encoding="utf-8") as h:
            json.dump(
                {
                    "current_account_id": "uid-1",
                    "accounts": [
                        {"id": "uid-1", "email": "old@example.com", "plan_type": "plus"},
                        {"id": "uid-2", "email": "new@example.com", "plan_type": "pro"},
                    ],
                },
                h,
            )

        now_unix = int(time.time())
        with sqlite3.connect(os.path.join(cockpit_dir, "codex-quota-history-v1.sqlite")) as conn:
            conn.execute(
                "CREATE TABLE quota_samples (account_id TEXT, identity TEXT, observed_at INTEGER, short_remaining INTEGER, weekly_remaining INTEGER)"
            )
            conn.execute("INSERT INTO quota_samples VALUES ('uid-1', 'id-1', ?, 85, 90)", (now_unix - 30,))
            conn.execute("INSERT INTO quota_samples VALUES ('uid-2', 'id-2', ?, 65, 70)", (now_unix - 20,))
            conn.commit()

        # First sync while sqlite does NOT exist (or is older): uid-1 is trusted as current (85%)
        with open(acct_sqlite_path, "wb") as h:
            h.write(b"SQLite format 3\x00dummy-unopened")
        t_json = time.time() - 120.0
        os.utime(json_path, (t_json, t_json))
        os.utime(acct_sqlite_path, (t_json - 60.0, t_json - 60.0))

        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        first = sync_local_quotas(store, user_home=self.tmp_home)
        gpt_first = {p["peer_id"]: p for p in first["peers"]}["chatgpt"]
        self.assertFalse(gpt_first["current_account_unknown"])
        self.assertEqual(gpt_first["min_remaining_percent"], 85.0)
        self.assertTrue(gpt_first["dispatch_recommended"])

        # Now codex_accounts.sqlite becomes newer than codex_accounts.json
        t_newer = t_json + 60.0
        os.utime(acct_sqlite_path, (t_newer, t_newer))

        opened_paths = []
        sqlite_targets = []
        real_open = builtins.open
        real_sqlite_connect = sqlite3.connect

        def _spy_open(file, *args, **kwargs):
            try:
                opened_paths.append(os.path.abspath(os.fspath(file)))
            except TypeError:
                pass
            return real_open(file, *args, **kwargs)

        def _spy_connect(database, *args, **kwargs):
            sqlite_targets.append(str(database))
            return real_sqlite_connect(database, *args, **kwargs)

        with mock.patch("builtins.open", side_effect=_spy_open), mock.patch(
            "sqlite3.connect", side_effect=_spy_connect
        ):
            second = sync_local_quotas(store, user_home=self.tmp_home)

        abs_sqlite = os.path.abspath(acct_sqlite_path)
        self.assertNotIn(abs_sqlite, opened_paths)
        self.assertFalse(any("codex_accounts.sqlite" in t for t in sqlite_targets))

        gpt_second = {p["peer_id"]: p for p in second["peers"]}["chatgpt"]
        self.assertTrue(gpt_second["current_account_unknown"])
        self.assertEqual(gpt_second["active_account_id"], "")
        self.assertIsNone(gpt_second["min_remaining_percent"])
        self.assertIsNone(gpt_second["dispatch_recommended"])
        self.assertFalse(any(acct["is_current"] for acct in gpt_second["pool"]))

    # 3. R014 P2: rebuild_all_peer_seqs pulses peers with queued_tasks/unacked backlog after rv delete
    def test_r015_rebuild_all_peer_seqs_pulses_backlog_after_rv_delete(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        store.create_mission(
            title="Rebuild pulse test",
            goal="Verify startup seq rebuild wakes waiting peer with backlog",
            coordinator_peer="claude",
            workspace_path="/tmp",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "queued task awaiting wake"}],
        )

        # Wipe rendezvous dir entirely (simulating /tmp cleanup)
        shutil.rmtree(self.tmp_rv)
        os.makedirs(self.tmp_rv, mode=0o700, exist_ok=True)

        gpt_res: dict = {}
        ag_res: dict = {}

        def _wait_gpt() -> None:
            gpt_res.update(
                wait_for_doorbell(
                    store=None,
                    home_dir=self.tmp_rv,
                    rendezvous_dir=self.tmp_rv,
                    peer_id="chatgpt",
                    timeout_sec=4.0,
                    session_label="gpt-backlog",
                )
            )

        def _wait_ag() -> None:
            ag_res.update(
                wait_for_doorbell(
                    store=None,
                    home_dir=self.tmp_rv,
                    rendezvous_dir=self.tmp_rv,
                    peer_id="antigravity",
                    timeout_sec=0.6,
                    session_label="ag-idle",
                )
            )

        tg = threading.Thread(target=_wait_gpt, daemon=True)
        ta = threading.Thread(target=_wait_ag, daemon=True)
        tg.start()
        ta.start()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            active = query_active_waiters(self.tmp_rv)
            if active.get("chatgpt", 0) >= 1 and active.get("antigravity", 0) >= 1:
                break
            time.sleep(0.02)

        rebuilt = rebuild_all_peer_seqs(store)
        self.assertEqual(rebuilt["chatgpt"]["queued_tasks"], 1)
        self.assertEqual(rebuilt["antigravity"]["queued_tasks"], 0)

        tg.join(timeout=3.0)
        ta.join(timeout=2.0)
        self.assertTrue(gpt_res.get("woke"), gpt_res)
        self.assertEqual(gpt_res.get("source"), "kqueue")
        self.assertFalse(ag_res.get("woke"), ag_res)

    # 4. R014 P2: Installer Phase 2 mid-failure reports accurate per-target status with secrets scrubbed
    def test_r015_installer_mid_failure_reports_per_target_status_redacted(self) -> None:
        stub_dir = self._write_claude_stub()
        os.environ["PATH"] = stub_dir + os.pathsep + (os.environ.get("PATH") or "")

        ag_dir = os.path.join(self.tmp_home, ".gemini", "config")
        os.makedirs(ag_dir, exist_ok=True)
        with open(os.path.join(ag_dir, "mcp_config.json"), "w", encoding="utf-8") as h:
            json.dump({"mcpServers": {}}, h)

        def _failing_runner(argv, **kwargs):
            if len(argv) >= 3 and argv[1] == "mcp" and argv[2] == "add-json":
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr="fatal: Bearer should_never_appear_in_report",
                )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with self.assertRaises(install_mod.InstallApplyError) as cm:
            install_mod.run_installer(
                mode="apply",
                target_home=self.tmp_home,
                repo_dir=os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
                port=18920,
                rendezvous_dir=self.tmp_rv,
                targets=["antigravity", "claude_code", "codex"],
                claude_running_check=lambda: False,
                claude_runner=_failing_runner,
            )

        rep = cm.exception.report
        self.assertFalse(rep["ok"])
        self.assertIn("antigravity_mcp", rep["applied_targets"])
        self.assertEqual(rep["failed_target"], "claude_code_mcp")
        self.assertTrue(rep["failed_target_may_be_modified"])
        self.assertIn("codex_mcp", rep["not_attempted_targets"])

        by_target = {r["target"]: r for r in rep["target_results"]}
        self.assertEqual(by_target["antigravity_mcp"]["status"], "applied")
        self.assertEqual(by_target["claude_code_mcp"]["status"], "failed")
        self.assertTrue(by_target["claude_code_mcp"]["may_be_modified"])
        self.assertEqual(by_target["codex_mcp"]["status"], "not_attempted")
        self.assertFalse(by_target["codex_mcp"]["may_be_modified"])

        serialized = json.dumps(rep, ensure_ascii=False)
        self.assertNotIn("should_never_appear_in_report", serialized)
        state_tok = read_auth_token(os.path.join(self.tmp_home, "Library", "Application Support", "consilium"))
        self.assertTrue(state_tok)
        self.assertNotIn(state_tok, serialized)


if __name__ == "__main__":
    unittest.main()
