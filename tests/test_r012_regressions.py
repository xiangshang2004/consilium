"""Regressions covering all Codex R012 findings (P1 #1/#2, P2s, P3-2, and §3 Q4)."""

from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from consilium.dashboard import compute_state_fingerprint
from consilium.mcp_server import ConsiliumDaemon, ConsiliumMCPDispatcher
from consilium.models import generate_auth_token, read_auth_token, write_auth_token
from consilium.quota import sync_local_quotas
from consilium.store import BusStore
from consilium.wakeup import rebuild_all_peer_seqs, wait_for_doorbell
import scripts.install as install_mod


def _http_read_response(sock: socket.socket) -> tuple:
    buf = b""
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


class R012RegressionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="consilium-r012-st-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="consilium-r012-rv-", dir="/tmp")
        self.tmp_home = tempfile.mkdtemp(prefix="consilium-r012-hm-", dir="/tmp")
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

    # 1. R012 P1 #1: Token redaction on downstream failure after Claude succeeds + add-json exception rollback
    def test_r012_p1_1_rotation_failure_redacts_tokens_and_rollbacks_on_exception(self) -> None:
        stub_dir = self._write_claude_stub()
        os.environ["PATH"] = f"{stub_dir}{os.pathsep}{os.environ.get('PATH', '')}"

        old_tok = "old-secret-bearer-token-9988"
        write_auth_token(self.tmp_state, old_tok)

        ag_path = os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json")
        os.makedirs(os.path.dirname(ag_path), mode=0o700, exist_ok=True)
        with open(ag_path, "w", encoding="utf-8") as h:
            json.dump(
                {"mcpServers": {"consilium": {"url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": f"Bearer {old_tok}"}}}},
                h,
            )

        cc_path = os.path.join(self.tmp_home, ".claude.json")
        with open(cc_path, "w", encoding="utf-8") as h:
            json.dump(
                {"mcpServers": {"consilium": {"type": "http", "url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": f"Bearer {old_tok}"}}}},
                h,
            )

        cx_path = os.path.join(self.tmp_home, ".codex", "config.toml")
        os.makedirs(os.path.dirname(cx_path), mode=0o700, exist_ok=True)
        with open(cx_path, "w", encoding="utf-8") as h:
            h.write(install_mod._render_codex_block(18920, old_tok))

        # Claude update succeeds, then downstream server_auth_token fails with exception carrying secrets
        def _cc_success_runner(cmd, **kwargs):
            sub = cmd[2] if len(cmd) > 2 else ""
            if sub == "add-json":
                payload = json.loads(cmd[-1])
                with open(cc_path, "w", encoding="utf-8") as h:
                    json.dump({"mcpServers": {"consilium": payload}}, h)
            return SimpleNamespace(returncode=0, stdout="ok", stderr="")

        fixed_new_tok = "new-secret-bearer-token-7766"
        with mock.patch.object(install_mod, "generate_auth_token", return_value=fixed_new_tok), mock.patch.object(
            install_mod,
            "write_auth_token",
            side_effect=OSError(f"boom while swapping {old_tok} -> {fixed_new_tok}"),
        ):
            with self.assertRaises(install_mod.TokenRotationError) as cm:
                install_mod.rotate_token(
                    target_home=self.tmp_home,
                    port=18920,
                    state_dir=self.tmp_state,
                    claude_runner=_cc_success_runner,
                )

        rep = cm.exception.report
        serialized = json.dumps(rep, ensure_ascii=False)
        self.assertEqual(rep["failed_target"], "server_auth_token")
        self.assertIn("claude_code_mcp", rep["updated_targets"])
        self.assertTrue(rep["failed_target_may_be_modified"])
        self.assertEqual(rep["codex_scope"], "managed_block_only")
        self.assertNotIn(old_tok, serialized)
        self.assertNotIn(fixed_new_tok, serialized)
        self.assertNotIn(old_tok, str(cm.exception))
        self.assertNotIn(fixed_new_tok, str(cm.exception))
        self.assertNotIn("Bearer old-", serialized)
        self.assertNotIn("Bearer new-", serialized)
        self.assertIn("[REDACTED]", serialized)

        # Backup directory 0o700 and backed-up client file 0o600
        self.assertEqual(stat.S_IMODE(os.stat(rep["backup_root"]).st_mode), 0o700)
        cc_bak = os.path.join(rep["backup_root"], ".claude.json")
        self.assertTrue(os.path.isfile(cc_bak))
        self.assertEqual(stat.S_IMODE(os.stat(cc_bak).st_mode), 0o600)

        # Also test CLI stderr exit 1 when downstream target fails via monkeypatched stub script
        cli_env = dict(os.environ)
        cli_env.pop("CLAUDE_CONFIG_DIR", None)
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; sys.path.insert(0, '.'); "
                    "import scripts.install as m, consilium.cli as cli; "
                    f"m.generate_auth_token = lambda: '{fixed_new_tok}'; "
                    "m.write_auth_token = lambda *a, **k: (_ for _ in ()).throw(OSError('disk full')); "
                    "sys.exit(cli.main(['rotate-token', '--target-home', "
                    f"{self.tmp_home!r}, '--data-dir', {self.tmp_state!r}]))"
                ),
            ],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            env=cli_env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 1)
        self.assertNotIn(old_tok, proc.stderr)
        self.assertNotIn(fixed_new_tok, proc.stderr)

        # Subprocess exception on `claude mcp add-json` triggers full verified rollback!
        previous_entry = {"type": "http", "url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": f"Bearer {old_tok}"}}
        with open(cc_path, "w", encoding="utf-8") as h:
            json.dump({"mcpServers": {"consilium": previous_entry}}, h)

        def _spawn_exc_runner(cmd, **kwargs):
            sub = cmd[2] if len(cmd) > 2 else ""
            if sub == "remove":
                with open(cc_path, "w", encoding="utf-8") as h:
                    json.dump({"mcpServers": {}}, h)
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if sub == "add-json":
                payload = json.loads(cmd[-1])
                if payload == previous_entry:
                    with open(cc_path, "w", encoding="utf-8") as h:
                        json.dump({"mcpServers": {"consilium": payload}}, h)
                    return SimpleNamespace(returncode=0, stdout="", stderr="")
                raise TimeoutError(f"timed out pushing Bearer {fixed_new_tok}")
            return SimpleNamespace(returncode=0, stdout="ok", stderr="")

        with self.assertRaisesRegex(RuntimeError, "previous entry restored and verified") as r_cm:
            install_mod._apply_claude_code_mcp(
                cc_path,
                {"type": "http", "url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": f"Bearer {fixed_new_tok}"}},
                previous_entry,
                remove=False,
                home_dir=self.tmp_home,
                backup_root=os.path.join(self.tmp_state, "bak-exc"),
                claude_bin=os.path.join(stub_dir, "claude"),
                runner=_spawn_exc_runner,
                secrets_to_redact=(old_tok, fixed_new_tok),
            )
        self.assertNotIn(fixed_new_tok, str(r_cm.exception))
        with open(cc_path, "r", encoding="utf-8") as h:
            self.assertEqual(json.load(h)["mcpServers"]["consilium"], previous_entry)

    # 2. R012 P1 #2: Concurrent rotation race between auth & ticket issue / SSE registration / initial frame
    def test_r012_p1_2_concurrent_rotation_blocks_ticket_and_sse_initial_frame(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        old_tok = read_auth_token(self.tmp_state)
        new_tok = generate_auth_token()

        # Simulate token rotating right after get_auth_state() returns v1 to _authenticate_request
        orig_get_auth = daemon.get_auth_state
        call_counter = {"n": 0}

        def _rotate_after_first_auth():
            tok, ver = orig_get_auth()
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                write_auth_token(self.tmp_state, new_tok)
                # Force bumped mtime_ns immediately in case filesystem mtime is coarse
                daemon._token_version += 1
                daemon._cached_token = new_tok
            return tok, ver

        with mock.patch.object(daemon, "get_auth_state", side_effect=_rotate_after_first_auth):
            srv_s, cli_s = socket.socketpair()
            t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
            t.start()
            req = (
                "POST /api/ui-ticket HTTP/1.1\r\n"
                f"Host: {daemon.host}:{daemon.port}\r\n"
                f"Authorization: Bearer {old_tok}\r\n"
                "Content-Length: 2\r\n\r\n{}"
            ).encode("utf-8")
            cli_s.sendall(req)
            st, _, _, _ = _http_read_response(cli_s)
            cli_s.close()
            t.join(timeout=2.0)
            self.assertEqual(st, 401)
            self.assertEqual(len(daemon.ticket_store._tickets), 0)

        # SSE race: rotate right after _authenticate_request passes with current token
        cur_tok = read_auth_token(self.tmp_state)
        next_tok = generate_auth_token()
        orig_subscribe = daemon.event_hub.subscribe

        def _rotate_on_subscribe(*args, **kwargs):
            sub = orig_subscribe(*args, **kwargs)
            daemon._token_version += 1
            daemon._cached_token = next_tok
            return sub

        with mock.patch.object(daemon.event_hub, "subscribe", side_effect=_rotate_on_subscribe):
            srv_s, cli_s = socket.socketpair()
            t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
            t.start()
            cli_s.sendall(
                (
                    "GET /api/stream HTTP/1.1\r\n"
                    f"Host: {daemon.host}:{daemon.port}\r\n"
                    f"Authorization: Bearer {cur_tok}\r\n\r\n"
                ).encode("utf-8")
            )
            st, _, body, _ = _http_read_response(cli_s)
            cli_s.close()
            t.join(timeout=2.0)
            self.assertEqual(st, 401)
            self.assertNotIn(b"event: state", body)
            self.assertEqual(len(daemon.event_hub._subscribers), 0)

        # SSE header write error still releases subscriber slot cleanly
        daemon._token_version += 1
        tok_after = generate_auth_token()
        write_auth_token(self.tmp_state, tok_after)
        daemon._cached_token = tok_after
        srv_s, cli_s = socket.socketpair()
        cli_s.close()  # Force immediate send error while daemon writes response headers/initial frame
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()
        t.join(timeout=2.0)
        self.assertEqual(len(daemon.event_hub._subscribers), 0)

    # 3. R012 P3-2: Persistent keep-alive POST /api/logout & /api/ui-ticket consume bodies cleanly
    def test_r012_p3_2_keepalive_post_early_return_and_logout_no_desync(self) -> None:
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
            body = b'{"keepalive":true}'
            cli_s.sendall(
                (
                    "POST /api/ui-ticket HTTP/1.1\r\n"
                    f"Host: {daemon.host}:{daemon.port}\r\n"
                    f"Authorization: Bearer {tok}\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Connection: keep-alive\r\n\r\n"
                ).encode("utf-8")
                + body
            )
            st1, _, b1, rest = _http_read_response(cli_s)
            self.assertEqual(st1, 200)
            self.assertEqual(rest, b"")
            self.assertIn("ticket", json.loads(b1.decode("utf-8")))

            cli_s.sendall(
                (
                    "POST /api/logout HTTP/1.1\r\n"
                    f"Host: {daemon.host}:{daemon.port}\r\n"
                    "Content-Length: 2\r\n"
                    "Connection: keep-alive\r\n\r\n{}"
                ).encode("utf-8")
            )
            st2, _, b2, rest2 = _http_read_response(cli_s)
            self.assertEqual(st2, 200)
            self.assertEqual(rest2, b"")
            self.assertTrue(json.loads(b2.decode("utf-8"))["ok"])

            cli_s.sendall(
                (
                    "GET /api/state HTTP/1.1\r\n"
                    f"Host: {daemon.host}:{daemon.port}\r\n"
                    f"Authorization: Bearer {tok}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("utf-8")
            )
            st3, _, b3, _ = _http_read_response(cli_s)
            self.assertEqual(st3, 200)
            self.assertIn("missions", json.loads(b3.decode("utf-8")))
        finally:
            cli_s.close()
            t.join(timeout=2.0)

    # 4. R012 P2: Legacy pre-attempt_id DB migration + email scrubbing
    def test_r012_p2_legacy_db_backfill_attempt_id_and_email_scrub(self) -> None:
        legacy_db = os.path.join(self.tmp_state, "bus.db")
        with sqlite3.connect(legacy_db) as conn:
            conn.executescript(
                """
                CREATE TABLE peers (
                    peer_id TEXT PRIMARY KEY,
                    app_name TEXT NOT NULL,
                    branch_prefix TEXT NOT NULL,
                    session_label TEXT NOT NULL DEFAULT '',
                    supported_models_json TEXT NOT NULL DEFAULT '[]',
                    last_seen_at TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE missions (
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
                CREATE TABLE tasks (
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
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE quota_windows (
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
                """
            )
            conn.execute(
                "INSERT INTO missions VALUES ('M-OLD1','Legacy','Goal','claude','','worktree',3,0,'active','','2026-11-19T00:00:00Z','2026-11-19T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO tasks (task_id, mission_id, assignee_peer, prompt, status, created_at, updated_at) "
                "VALUES ('T-OLD1','M-OLD1','chatgpt','Run','running','2026-11-19T00:00:00Z','2026-11-19T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO quota_windows VALUES ('chatgpt','legacyuser@example.com','5h','5h',50.0,'','manual_report','2026-11-20T00:00:00Z')"
            )
            conn.commit()

        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        with sqlite3.connect(store.db_path) as conn:
            conn.row_factory = sqlite3.Row
            trow = conn.execute("SELECT attempt_id, attempt_seq, claimed_at FROM tasks WHERE task_id='T-OLD1'").fetchone()
            self.assertEqual(trow["attempt_id"], "A-1-legacy-told1")
            self.assertEqual(trow["attempt_seq"], 1)
            self.assertEqual(trow["claimed_at"], "2026-11-19T00:00:00Z")
            qdump = str(conn.execute("SELECT account_id FROM quota_windows").fetchall())
            self.assertNotIn("legacyuser@example.com", qdump)

        requeued = store.requeue_task("T-OLD1", reason="legacy_upgrade_recover", actor_peer="claude")
        self.assertEqual(requeued["task"]["status"], "queued")

    # 5. R012 P2: Idempotent replay preserves original delivery receipt & warnings
    def test_r012_p2_idempotent_replay_returns_original_delivery_receipt(self) -> None:
        disp = ConsiliumMCPDispatcher(home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = disp.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "consilium_create_mission",
                    "arguments": {
                        "title": "Idemp",
                        "goal": "Delivery",
                        "coordinator_peer": "claude",
                    },
                },
            }
        )
        mid = created["result"]["structuredContent"]["mission"]["mission_id"]

        with mock.patch(
            "consilium.mcp_server.ring_peer", side_effect=RuntimeError("simulated ring failure")
        ):
            first = disp.handle_payload(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "consilium_dispatch_task",
                        "arguments": {
                            "mission_id": mid,
                            "assignee_peer": "chatgpt",
                            "prompt": "Check",
                            "client_request_id": "req-r012-1",
                        },
                    },
                }
            )
        sc1 = first["result"]["structuredContent"]
        self.assertTrue(sc1["committed"])
        self.assertEqual(sc1["delivery"]["delivery"], "error")
        self.assertIn("simulated ring failure", "\n".join(sc1["delivery"]["warnings"]))

        replay = disp.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "consilium_dispatch_task",
                    "arguments": {
                        "mission_id": mid,
                        "assignee_peer": "chatgpt",
                        "prompt": "Check",
                        "client_request_id": "req-r012-1",
                    },
                },
            }
        )
        sc2 = replay["result"]["structuredContent"]
        self.assertTrue(sc2["delivery"]["idempotent_replay"])
        self.assertEqual(sc2["delivery"]["delivery"], "error")
        self.assertEqual(sc2["delivery"]["warnings"], sc1["delivery"]["warnings"])

    # 6. R012 P2: Live kqueue hot wakeup via consilium_wait performs ZERO SQLite writes
    def test_r012_p2_wait_real_kqueue_hot_wakeup_zero_sqlite_writes(self) -> None:
        disp = ConsiliumMCPDispatcher(home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv)
        store = disp.store
        rebuild_all_peer_seqs(store)
        created = store.create_mission("ZeroWrite", "Wait test", "claude")
        mid = created["mission"]["mission_id"]

        wait_out = {}

        def _run_wait() -> None:
            resp = disp.handle_payload(
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "consilium_wait",
                        "arguments": {"peer_id": "chatgpt", "timeout_sec": 5.0},
                    },
                }
            )
            wait_out.update(resp["result"]["structuredContent"])

        wt = threading.Thread(target=_run_wait, daemon=True)
        wt.start()
        # Wait briefly until chatgpt lease appears in waiters/chatgpt/
        lease_dir = os.path.join(self.tmp_rv, "waiters", "chatgpt")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if os.path.isdir(lease_dir) and any(f.endswith(".lease") for f in os.listdir(lease_dir)):
                break
            time.sleep(0.02)

        disp.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "consilium_dispatch_task",
                    "arguments": {"mission_id": mid, "assignee_peer": "chatgpt", "prompt": "Wake"},
                },
            }
        )
        with sqlite3.connect(store.db_path) as conn:
            dv_before = conn.execute("PRAGMA data_version").fetchone()[0]
            payload_before = conn.execute(
                "SELECT payload_json FROM events ORDER BY event_id DESC LIMIT 1"
            ).fetchone()[0]

        wt.join(timeout=3.0)
        self.assertTrue(wait_out.get("woke"), wait_out)
        self.assertEqual(wait_out.get("source"), "kqueue", wait_out)
        with sqlite3.connect(store.db_path) as conn:
            dv_after = conn.execute("PRAGMA data_version").fetchone()[0]
            payload_after = conn.execute(
                "SELECT payload_json FROM events ORDER BY event_id DESC LIMIT 1"
            ).fetchone()[0]
        self.assertEqual(dv_before, dv_after)
        self.assertEqual(payload_before, payload_after)
        self.assertNotIn("wakeup_latency_ms", payload_after)

    # 7. R012 P2 & §3: Fingerprint ignores ticking sample_age/reset_in_human & SSE lock ordering
    def test_r012_p2_fingerprint_ignores_ticking_quota_fields_and_q4_stale_backup(self) -> None:
        s1 = {
            "peers": [{"peer_id": "claude", "status": "online", "age_seconds": 1}],
            "active_waiters": {},
            "quotas": [
                {
                    "peer_id": "chatgpt",
                    "min_remaining_percent": 50.0,
                    "earliest_reset_in_human": "4h 59m",
                    "windows": [{"sample_age_seconds": 10, "reset_in_seconds": 17940, "reset_in_human": "4h 59m"}],
                    "pool": [{"alias": "账号 A", "sample_age_seconds": 10, "reset_in_human": "4h 59m"}],
                }
            ],
            "missions": [],
            "latest_event_id": 1,
        }
        s2 = {
            "peers": [{"peer_id": "claude", "status": "online", "age_seconds": 29}],
            "active_waiters": {},
            "quotas": [
                {
                    "peer_id": "chatgpt",
                    "min_remaining_percent": 50.0,
                    "earliest_reset_in_human": "4h 58m",
                    "windows": [{"sample_age_seconds": 65, "reset_in_seconds": 17880, "reset_in_human": "4h 58m"}],
                    "pool": [{"alias": "账号 A", "sample_age_seconds": 65, "reset_in_human": "4h 58m"}],
                }
            ],
            "missions": [],
            "latest_event_id": 1,
        }
        self.assertEqual(compute_state_fingerprint(s1), compute_state_fingerprint(s2))

        # Stale Cockpit backup (>3h) must NOT recommend switch_hint, and missing codex_accounts.json clears is_current
        cockpit_dir = os.path.join(self.tmp_home, ".antigravity_cockpit")
        os.makedirs(cockpit_dir, mode=0o700, exist_ok=True)
        reg_path = os.path.join(cockpit_dir, "codex_accounts.json")
        with open(reg_path, "w", encoding="utf-8") as h:
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
        now_unix = int(time.time())
        with sqlite3.connect(os.path.join(cockpit_dir, "codex-quota-history-v1.sqlite")) as conn:
            conn.execute(
                "CREATE TABLE quota_samples (account_id TEXT, identity TEXT, observed_at INTEGER, short_remaining INTEGER, weekly_remaining INTEGER)"
            )
            conn.execute("INSERT INTO quota_samples VALUES ('uid-1','one@example.com', ?, 5, 10)", (now_unix - 60,))
            conn.execute("INSERT INTO quota_samples VALUES ('uid-2','two@example.com', ?, 95, 95)", (now_unix - 14400,))
            conn.commit()

        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        r1 = sync_local_quotas(store, user_home=self.tmp_home)
        gpt1 = [p for p in r1["peers"] if p["peer_id"] == "chatgpt"][0]
        self.assertEqual(gpt1["switch_hint"], "")
        self.assertFalse(gpt1["pool_has_capacity"])

        os.unlink(reg_path)
        r2 = sync_local_quotas(store, user_home=self.tmp_home)
        gpt2 = [p for p in r2["peers"] if p["peer_id"] == "chatgpt"][0]
        self.assertTrue(gpt2["current_account_unknown"])
        self.assertIsNone(gpt2["min_remaining_percent"])
        self.assertIsNone(gpt2["dispatch_recommended"])

    def test_r012_p2_sse_idle_external_change_re_snapshots_inside_publish_lock(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        tok = read_auth_token(self.tmp_state)

        # Simulate stale cache mismatch vs live SQLite change on idle queue.Empty branch
        orig_cached = daemon.get_cached_dashboard_state
        first_call = {"done": False}

        def _inject_external_change_after_first(*args, **kwargs):
            if not first_call["done"]:
                first_call["done"] = True
                # Out-of-band stdio write directly to bus.db
                out_store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
                out_store.create_mission(
                    "ExtSSE", "Re-snapshot inside publish lock", "claude",
                    user_message={"text": "seed", "targets": [], "mode": "auto"},
                )
                return daemon.get_dashboard_state(sync_quotas=False)
            return orig_cached(*args, **kwargs)

        with mock.patch.object(
            daemon, "get_cached_dashboard_state", side_effect=_inject_external_change_after_first
        ):
            srv_s, cli_s = socket.socketpair()
            cli_s.settimeout(4.0)
            t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
            t.start()
            cli_s.sendall(
                (
                    "GET /api/stream?max_events=2 HTTP/1.1\r\n"
                    f"Host: {daemon.host}:{daemon.port}\r\n"
                    f"Authorization: Bearer {tok}\r\n\r\n"
                ).encode("utf-8")
            )
            chunks = []
            while True:
                data = cli_s.recv(4096)
                if not data:
                    break
                chunks.append(data)
            cli_s.close()
            t.join(timeout=3.0)

        stream_text = b"".join(chunks).decode("utf-8", errors="replace")
        self.assertIn("event: state", stream_text)
        self.assertIn("event: update", stream_text)
        self.assertIn("ExtSSE", stream_text)

    def test_r012_http_framing_rejects_te_dup_cl_and_short_body(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        tok = read_auth_token(self.tmp_state)

        # 1. Transfer-Encoding: chunked -> 501 + Connection: close
        srv_s, cli_s = socket.socketpair()
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()
        cli_s.sendall(
            (
                "POST /mcp HTTP/1.1\r\n"
                f"Host: {daemon.host}:{daemon.port}\r\n"
                f"Authorization: Bearer {tok}\r\n"
                "Transfer-Encoding: chunked\r\n\r\n"
                "0\r\n\r\n"
            ).encode("utf-8")
        )
        st_te, hdr_te, _, _ = _http_read_response(cli_s)
        cli_s.close()
        t.join(timeout=2.0)
        self.assertEqual(st_te, 501)
        self.assertEqual(hdr_te.get("connection"), "close")

        # 2. Duplicate Content-Length -> 400 + Connection: close
        srv_s, cli_s = socket.socketpair()
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()
        cli_s.sendall(
            (
                "POST /mcp HTTP/1.1\r\n"
                f"Host: {daemon.host}:{daemon.port}\r\n"
                f"Authorization: Bearer {tok}\r\n"
                "Content-Length: 2\r\n"
                "Content-Length: 2\r\n\r\n{}"
            ).encode("utf-8")
        )
        st_dup, hdr_dup, _, _ = _http_read_response(cli_s)
        cli_s.close()
        t.join(timeout=2.0)
        self.assertEqual(st_dup, 400)
        self.assertEqual(hdr_dup.get("connection"), "close")

        # 3. Incomplete/short body -> 400 Incomplete request body (never parsed as empty dict)
        srv_s, cli_s = socket.socketpair()
        t = threading.Thread(target=daemon.handle_socket_once, args=(srv_s,), daemon=True)
        t.start()
        cli_s.sendall(
            (
                "POST /mcp HTTP/1.1\r\n"
                f"Host: {daemon.host}:{daemon.port}\r\n"
                f"Authorization: Bearer {tok}\r\n"
                "Content-Length: 20\r\n\r\n{}"
            ).encode("utf-8")
        )
        cli_s.shutdown(socket.SHUT_WR)
        st_short, hdr_short, body_short, _ = _http_read_response(cli_s)
        cli_s.close()
        t.join(timeout=2.0)
        self.assertEqual(st_short, 400)
        self.assertEqual(hdr_short.get("connection"), "close")
        self.assertIn(b"Incomplete request body", body_short)

    def test_r012_q4_same_email_two_workspaces_stay_distinct_by_account_id(self) -> None:
        cockpit_dir = os.path.join(self.tmp_home, ".antigravity_cockpit")
        os.makedirs(cockpit_dir, mode=0o700, exist_ok=True)
        with open(os.path.join(cockpit_dir, "codex_accounts.json"), "w", encoding="utf-8") as h:
            json.dump(
                {
                    "current_account_id": "ws-1",
                    "accounts": [
                        {"id": "ws-1", "email": "shared@example.com", "plan_type": "team"},
                        {"id": "ws-2", "email": "shared@example.com", "plan_type": "team"},
                    ],
                },
                h,
            )
        now_unix = int(time.time())
        with sqlite3.connect(os.path.join(cockpit_dir, "codex-quota-history-v1.sqlite")) as conn:
            conn.execute(
                "CREATE TABLE quota_samples (account_id TEXT, identity TEXT, observed_at INTEGER, short_remaining INTEGER, weekly_remaining INTEGER)"
            )
            conn.execute("INSERT INTO quota_samples VALUES ('ws-1','id-hash-1', ?, 10, 40)", (now_unix - 30,))
            conn.execute("INSERT INTO quota_samples VALUES ('ws-2','id-hash-2', ?, 90, 95)", (now_unix - 20,))
            conn.commit()

        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        res = sync_local_quotas(store, user_home=self.tmp_home)
        gpt = [p for p in res["peers"] if p["peer_id"] == "chatgpt"][0]
        self.assertEqual(len(gpt["pool"]), 2)
        self.assertEqual(gpt["active_account_alias"], "账号 A")
        self.assertEqual(gpt["min_remaining_percent"], 10.0)
        self.assertTrue(gpt["pool_has_capacity"])
        self.assertEqual(gpt["best_backup"]["alias"], "账号 B")
        self.assertIn("账号 B", gpt["switch_hint"])
        self.assertNotIn("shared@example.com", json.dumps(res))
        self.assertNotIn("ws-1", json.dumps(res))
        self.assertNotIn("ws-2", json.dumps(res))


if __name__ == "__main__":
    unittest.main()
