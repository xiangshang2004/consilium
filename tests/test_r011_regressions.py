"""Regressions for R011: P3-1..3 hardening, Q4 multi-account Cockpit whitelist, and chat UI contract."""

from __future__ import annotations

import builtins
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

from consilium.dashboard import render_dashboard_html
from consilium.mcp_server import ConsiliumDaemon
from consilium.models import read_auth_token
from consilium.quota import sync_local_quotas
from consilium.store import BusStore
from consilium.wakeup import resolve_socket_path


class R011RegressionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="consilium-r011-st-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="consilium-r011-rv-", dir="/tmp")
        self.tmp_home = tempfile.mkdtemp(prefix="consilium-r011-hm-", dir="/tmp")
        self._prev_dry = os.environ.get("CONSILIUM_DRY_RUN")
        self._prev_home = os.environ.get("CONSILIUM_USER_HOME")
        self._prev_no_quota = os.environ.get("CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC")
        os.environ["CONSILIUM_DRY_RUN"] = "1"
        os.environ["CONSILIUM_USER_HOME"] = self.tmp_home
        os.environ["CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC"] = "1"

    def tearDown(self) -> None:
        for key, prev in (
            ("CONSILIUM_DRY_RUN", self._prev_dry),
            ("CONSILIUM_USER_HOME", self._prev_home),
            ("CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC", self._prev_no_quota),
        ):
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def test_r011_p3_1_malformed_origin_port_returns_403_without_drop(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        tok = read_auth_token(self.tmp_state)

        # Login to get cookie + csrf
        srv_s, cli_s = socket.socketpair()
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()
        body = json.dumps({"token": tok}).encode("utf-8")
        cli_s.sendall(
            (
                "POST /api/login HTTP/1.1\r\n"
                "Host: 127.0.0.1:18920\r\n"
                "Origin: http://127.0.0.1:18920\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            + body
        )
        raw = b""
        while True:
            chunk = cli_s.recv(4096)
            if not chunk:
                break
            raw += chunk
        cli_s.close()
        t.join(timeout=2.0)
        hdrs_txt, resp_body = raw.decode("utf-8", errors="replace").split("\r\n\r\n", 1)
        m_ck = re.search(r"(?im)^Set-Cookie:\s*([^;\r\n]+)", hdrs_txt)
        self.assertIsNotNone(m_ck)
        cookie_pair = m_ck.group(1)

        # State check to grab CSRF token
        st_srv, st_cli = socket.socketpair()
        t2 = threading.Thread(target=daemon.handle_socket_once, args=(st_srv,), daemon=True)
        t2.start()
        st_cli.sendall(
            (
                "GET /api/state HTTP/1.1\r\n"
                "Host: 127.0.0.1:18920\r\n"
                f"Cookie: {cookie_pair}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
        )
        raw_st = b""
        while True:
            chunk = st_cli.recv(8192)
            if not chunk:
                break
            raw_st += chunk
        st_cli.close()
        t2.join(timeout=2.0)
        csrf = json.loads(raw_st.decode("utf-8").split("\r\n\r\n", 1)[1])["csrf_token"]

        # Malformed Origin port http://127.0.0.1:abc MUST return HTTP 403 JSON cleanly!
        w_srv, w_cli = socket.socketpair()
        t3 = threading.Thread(target=daemon.handle_socket_once, args=(w_srv,), daemon=True)
        t3.start()
        wb = b'{"reason":"test"}'
        w_cli.sendall(
            (
                "POST /api/tasks/T-NONE/requeue HTTP/1.1\r\n"
                "Host: 127.0.0.1:18920\r\n"
                "Origin: http://127.0.0.1:abc\r\n"
                f"Cookie: {cookie_pair}\r\n"
                "Content-Type: application/json\r\n"
                f"X-Consilium-CSRF: {csrf}\r\n"
                f"Content-Length: {len(wb)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            + wb
        )
        raw_w = b""
        while True:
            chunk = w_cli.recv(4096)
            if not chunk:
                break
            raw_w += chunk
        w_cli.close()
        t3.join(timeout=2.0)
        self.assertIn("403", raw_w.decode("utf-8", errors="replace").splitlines()[0])

    def test_r011_p3_2_early_error_closes_keepalive_no_desync(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)

        srv_s, cli_s = socket.socketpair()
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()

        wb = b'{"reason":"x"}'
        pipelined = (
            "POST /api/tasks/T-NONE/requeue HTTP/1.1\r\n"
            "Host: 127.0.0.1:18920\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(wb)}\r\n"
            "Connection: keep-alive\r\n\r\n"
        ).encode("ascii") + wb + (
            "GET /health HTTP/1.1\r\n"
            "Host: 127.0.0.1:18920\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii")
        cli_s.sendall(pipelined)
        cli_s.shutdown(socket.SHUT_WR)

        resp_raw = b""
        cli_s.settimeout(3.0)
        while True:
            chunk = cli_s.recv(4096)
            if not chunk:
                break
            resp_raw += chunk
        cli_s.close()
        t.join(timeout=3.0)

        decoded = resp_raw.decode("utf-8", errors="replace")
        self.assertTrue(decoded.startswith("HTTP/1.1 401"))
        self.assertIn("Connection: close", decoded)
        self.assertNotIn("501", decoded)

    def test_r011_p3_3_socket_fallback_env_honored(self) -> None:
        fb = os.path.join(self.tmp_rv, "skdir")
        long_rv = os.path.join(self.tmp_rv, "a_" * 55 + "rv")
        os.makedirs(long_rv, mode=0o700, exist_ok=True)
        prev = os.environ.get("CONSILIUM_SOCKET_FALLBACK_DIR")
        os.environ["CONSILIUM_SOCKET_FALLBACK_DIR"] = fb
        try:
            resolved = resolve_socket_path(long_rv)
        finally:
            if prev is None:
                os.environ.pop("CONSILIUM_SOCKET_FALLBACK_DIR", None)
            else:
                os.environ["CONSILIUM_SOCKET_FALLBACK_DIR"] = prev
        self.assertTrue(resolved.startswith(os.path.abspath(fb) + os.sep))
        self.assertEqual(os.stat(fb).st_mode & 0o777, 0o700)

    def test_r011_q4_multi_account_pool_dedupe_history_and_email_privacy(self) -> None:
        cockpit_dir = os.path.join(self.tmp_home, ".antigravity_cockpit")
        os.makedirs(cockpit_dir, mode=0o700, exist_ok=True)

        # Authoritative codex_accounts.json: Account A is active, Account B is backup
        with open(os.path.join(cockpit_dir, "codex_accounts.json"), "w", encoding="utf-8") as h:
            json.dump(
                {
                    "version": "1",
                    "current_account_id": "uid-a",
                    "accounts": [
                        {"id": "uid-a", "email": "alice@example.com", "plan_type": "plus"},
                        {"id": "uid-b", "email": "bob@example.com", "plan_type": "pro"},
                    ],
                },
                h,
            )

        now_unix = int(time.time())
        codex_db = os.path.join(cockpit_dir, "codex-quota-history-v1.sqlite")
        with sqlite3.connect(codex_db) as conn:
            conn.execute(
                """
                CREATE TABLE quota_samples (
                    account_id TEXT NOT NULL,
                    identity TEXT,
                    observed_at INTEGER NOT NULL,
                    short_remaining INTEGER,
                    weekly_remaining INTEGER
                )
                """
            )
            # Alice recorded by account_id AND again by email identity -> must deduplicate!
            conn.execute(
                "INSERT INTO quota_samples VALUES (?, ?, ?, ?, ?)",
                ("uid-a", "alice@example.com", now_unix - 120, 12, 65),
            )
            conn.execute(
                "INSERT INTO quota_samples VALUES (?, ?, ?, ?, ?)",
                ("alice-alt", "alice@example.com", now_unix - 300, 10, 60),
            )
            # Bob healthy backup
            conn.execute(
                "INSERT INTO quota_samples VALUES (?, ?, ?, ?, ?)",
                ("uid-b", "bob@example.com", now_unix - 60, 91, 95),
            )
            # Historical unlisted exhausted account -> must NOT exhaust active peer!
            conn.execute(
                "INSERT INTO quota_samples VALUES (?, ?, ?, ?, ?)",
                ("uid-old", "legacy@example.com", now_unix - 30, 0, 0),
            )
            conn.commit()

        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        res = sync_local_quotas(store, user_home=self.tmp_home)
        gpt = [p for p in res["peers"] if p["peer_id"] == "chatgpt"][0]

        # Active account is Alice (12% low quota, NOT exhausted by legacy 0%!)
        self.assertEqual(gpt["min_remaining_percent"], 12.0)
        self.assertFalse(gpt["exhausted"])
        self.assertTrue(gpt["low_quota"])
        self.assertTrue(gpt["dispatch_recommended"])
        self.assertEqual(gpt["active_account_alias"], "账号 A")
        self.assertEqual(len(gpt["pool"]), 2)
        self.assertTrue(gpt["pool_has_capacity"])
        self.assertIsNotNone(gpt["best_backup"])
        self.assertEqual(gpt["best_backup"]["alias"], "账号 B")
        self.assertIn("账号 B", gpt["switch_hint"])
        self.assertEqual(gpt["historical_accounts_count"], 1)
        self.assertEqual(len(gpt["historical_accounts"]), 1)
        self.assertTrue(gpt["historical_accounts"][0]["is_historical"])
        self.assertEqual(gpt["historical_accounts"][0]["alias"], "历史账号 1")

        # Zero plaintext emails OR raw Cockpit IDs persisted in DB or returned by quota API!
        serialized = json.dumps(res, ensure_ascii=False)
        for sensitive in (
            "alice@example.com",
            "bob@example.com",
            "legacy@example.com",
            "uid-a",
            "uid-b",
            "uid-old",
            "alice-alt",
        ):
            self.assertNotIn(sensitive, serialized)
        with sqlite3.connect(store.db_path) as conn:
            dump = "\n".join(str(r) for r in conn.execute("SELECT * FROM quota_windows").fetchall())
            self.assertNotIn("@example.com", dump)
            self.assertNotIn("uid-a", dump)
            self.assertNotIn("uid-b", dump)

        # Also test backup exhausted (0%) while active is 80% -> NOT exhausted!
        with sqlite3.connect(codex_db) as conn:
            conn.execute(
                "UPDATE quota_samples SET short_remaining = 80, weekly_remaining = 88, observed_at = ? WHERE account_id = 'uid-a'",
                (now_unix + 10,),
            )
            conn.execute(
                "UPDATE quota_samples SET short_remaining = 0, weekly_remaining = 0, observed_at = ? WHERE account_id = 'uid-b'",
                (now_unix + 10,),
            )
            conn.commit()
        res2 = sync_local_quotas(store, user_home=self.tmp_home)
        gpt2 = [p for p in res2["peers"] if p["peer_id"] == "chatgpt"][0]
        self.assertEqual(gpt2["min_remaining_percent"], 80.0)
        self.assertFalse(gpt2["exhausted"])
        self.assertTrue(gpt2["dispatch_recommended"])

    def test_r011_q4_staleness_unknown_multi_account_and_strict_whitelist(self) -> None:
        cockpit_dir = os.path.join(self.tmp_home, ".antigravity_cockpit")
        for sub in ("accounts", "codex_accounts"):
            os.makedirs(os.path.join(cockpit_dir, sub), mode=0o700, exist_ok=True)

        # Forbidden canary files that MUST NEVER be opened by Consilium!
        forbidden_paths = [
            os.path.join(cockpit_dir, "accounts.json"),
            os.path.join(cockpit_dir, "google_accounts.json"),
            # R020 (user: "账号接入cockpit"): current_account.json, claude_accounts.json,
            # provider_current_accounts.json and codex_instances.json hold only "which account is
            # current" pointers and plan types (no credentials) and are now read. Vaults stay forbidden.
            os.path.join(cockpit_dir, "antigravity_accounts.sqlite"),
            os.path.join(cockpit_dir, "provider_current_accounts.sqlite"),
            os.path.join(cockpit_dir, "secure-account-storage.key"),
            os.path.join(cockpit_dir, "codex_accounts.sqlite"),
            os.path.join(cockpit_dir, "claude_accounts.sqlite"),
            os.path.join(cockpit_dir, "accounts", "ag_secret.json"),
            os.path.join(cockpit_dir, "codex_accounts", "cx_secret.json"),
        ]
        for fp in forbidden_paths:
            with open(fp, "w", encoding="utf-8") as h:
                h.write('{"secret":"DO_NOT_READ"}')

        with open(os.path.join(cockpit_dir, "codex_accounts.json"), "w", encoding="utf-8") as h:
            json.dump(
                {
                    "current_account_id": "uid-1",
                    "accounts": [
                        {"id": "uid-1", "email": "one@example.com", "plan_type": "plus"},
                        {"id": "uid-2", "email": "two@example.com", "plan_type": "plus"},
                    ],
                },
                h,
            )

        stale_unix = int(time.time()) - 3600  # 60m > 30m threshold
        with sqlite3.connect(os.path.join(cockpit_dir, "codex-quota-history-v1.sqlite")) as conn:
            conn.execute(
                "CREATE TABLE quota_samples (account_id TEXT, identity TEXT, observed_at INTEGER, short_remaining INTEGER, weekly_remaining INTEGER)"
            )
            conn.execute(
                "INSERT INTO quota_samples VALUES (?, ?, ?, ?, ?)",
                ("uid-1", "one@example.com", stale_unix, 88, 90),
            )
            conn.execute(
                "INSERT INTO quota_samples VALUES (?, ?, ?, ?, ?)",
                ("uid-2", "two@example.com", int(time.time()) - 60, 75, 80),
            )
            conn.commit()

        # Multiple Antigravity accounts without known current account
        with sqlite3.connect(os.path.join(cockpit_dir, "platform-quota-history-v1.sqlite")) as conn:
            conn.execute(
                "CREATE TABLE quota_observations (platform TEXT, account_id TEXT, metric TEXT, label TEXT, observed_at INTEGER, remaining INTEGER, reset_at INTEGER)"
            )
            conn.execute("INSERT INTO quota_observations VALUES ('antigravity', 'ag1', 'm1', 'Pro', ?, 70, 0)", (int(time.time()),))
            conn.execute("INSERT INTO quota_observations VALUES ('antigravity', 'ag2', 'm1', 'Pro', ?, 90, 0)", (int(time.time()),))
            conn.commit()

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

        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        with mock.patch("builtins.open", side_effect=_spy_open), mock.patch(
            "sqlite3.connect", side_effect=_spy_connect
        ):
            res = sync_local_quotas(store, user_home=self.tmp_home)

        for fp in forbidden_paths:
            abs_fp = os.path.abspath(fp)
            self.assertNotIn(abs_fp, opened_paths)
            self.assertFalse(any(os.path.basename(fp) in t for t in sqlite_targets))

        by_peer = {p["peer_id"]: p for p in res["peers"]}
        gpt = by_peer["chatgpt"]
        self.assertTrue(gpt["account_switch_suspected"])
        self.assertIsNone(gpt["min_remaining_percent"])
        self.assertIsNone(gpt["dispatch_recommended"])

        ag = by_peer["antigravity"]
        self.assertTrue(ag["current_account_unknown"])
        self.assertIsNone(ag["min_remaining_percent"])
        self.assertIsNone(ag["dispatch_recommended"])
        self.assertEqual(len(ag["pool"]), 2)
        self.assertNotIn("ag1", json.dumps(ag))
        self.assertNotIn("ag2", json.dumps(ag))

    def test_r011_ui_minimalist_conversation_contract(self) -> None:
        html = render_dashboard_html("0.1.0")
        self.assertNotIn("http://", html)
        self.assertNotIn("https://", html)
        for bad in ("Claude Opus 4.1", "Claude Sonnet 4", "GPT-5.1", "执行输出中", "codex://threads/"):
            self.assertNotIn(bad, html)
        for good in (
            "data-set-theme=\"light\"",
            "data-set-theme=\"dark\"",
            "data-set-theme=\"system\"",
            "background: var(--bg-elevated);",
            'ev.key === "Escape"',
            "claude://code/new?q=",
            "codex://new?prompt=",
            "最后上报于",
            "127.0.0.1:",
            "localhost",
            "只读账号池",
            "模型与账号",
            "\"文件\"",
            "该端未上报",
            "/mission",
            "/files",
            "/export",
            "历史账号",
            "copyTextSafe",
        ):
            self.assertIn(good, html)

        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.S)
        self.assertEqual(len(scripts), 1)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tf:
            tf.write(scripts[0])
            jspath = tf.name
        try:
            proc = subprocess.run(["node", "--check", jspath], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
        finally:
            os.unlink(jspath)


if __name__ == "__main__":
    unittest.main()
