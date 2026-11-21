"""Comprehensive regression tests for R005 P1 (3.1-3.7) & P2 items (R006)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

from consilium import mcp_server
from consilium.mcp_server import ConsiliumDaemon, ConsiliumMCPDispatcher, run_stdio_server
from consilium.models import resolve_rendezvous_dir
from consilium.quota import sync_local_quotas
from consilium.store import BusStore
from consilium.wakeup import read_peer_seq, ring_peer, wait_for_doorbell
from scripts.install import rotate_token, run_installer
from tests.test_mcp_transports import _raw_http_roundtrip


class R006RegressionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="dbell-r6s-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="dbell-r6rv-", dir="/tmp")
        self.tmp_home = tempfile.mkdtemp(prefix="dbell-r6h-", dir="/tmp")
        self._prev_dry = os.environ.get("CONSILIUM_DRY_RUN")
        self._prev_user_home = os.environ.get("CONSILIUM_USER_HOME")
        self._prev_path = os.environ.get("PATH")
        stub_dir = os.path.join(self.tmp_home, "bin")
        os.makedirs(stub_dir, exist_ok=True)
        claude_stub = os.path.join(stub_dir, "claude")
        with open(claude_stub, "w", encoding="utf-8") as handle:
            handle.write(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "p = os.path.join(os.environ['HOME'], '.claude.json')\n"
                "args = sys.argv[1:]\n"
                "data = json.load(open(p)) if os.path.exists(p) else {}\n"
                "sv = data.setdefault('mcpServers', {})\n"
                "if args[:2] == ['mcp', 'get']:\n"
                "    sys.exit(0 if args[2] in sv else 1)\n"
                "if args[:4] == ['mcp', 'remove', '-s', 'user']:\n"
                "    sv.pop(args[4], None)\n"
                "    json.dump(data, open(p, 'w'), indent=2)\n"
                "    sys.exit(0)\n"
                "if args[:4] == ['mcp', 'add-json', '-s', 'user']:\n"
                "    sv[args[4]] = json.loads(args[5])\n"
                "    json.dump(data, open(p, 'w'), indent=2)\n"
                "    sys.exit(0)\n"
                "sys.exit(0)\n"
            )
        os.chmod(claude_stub, 0o755)
        os.environ["PATH"] = stub_dir + os.pathsep + (self._prev_path or "")
        os.environ["CONSILIUM_DRY_RUN"] = "1"
        os.environ["CONSILIUM_USER_HOME"] = self.tmp_home
        self.repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def tearDown(self) -> None:
        if self._prev_path is None:
            os.environ.pop("PATH", None)
        else:
            os.environ["PATH"] = self._prev_path
        if self._prev_dry is None:
            os.environ.pop("CONSILIUM_DRY_RUN", None)
        else:
            os.environ["CONSILIUM_DRY_RUN"] = self._prev_dry
        if self._prev_user_home is None:
            os.environ.pop("CONSILIUM_USER_HOME", None)
        else:
            os.environ["CONSILIUM_USER_HOME"] = self._prev_user_home
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    # ---- R005 3.1: consilium_wait defaults to auto_pickup=False ----
    def test_3_1_wait_defaults_no_auto_pickup_and_timeout_leaves_task_queued(self) -> None:
        dispatcher = ConsiliumMCPDispatcher(
            home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv
        )
        tools_resp = dispatcher.handle_payload(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        )
        wait_tool = [
            t for t in tools_resp["result"]["tools"] if t["name"] == "consilium_wait"
        ][0]
        self.assertFalse(wait_tool["inputSchema"]["properties"]["auto_pickup"]["default"])

        # Wait times out without auto_pickup specified.
        wait_resp = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "consilium_wait",
                    "arguments": {"peer_id": "claude", "timeout_sec": 0.1},
                },
            }
        )
        self.assertFalse(wait_resp["result"]["structuredContent"]["woke"])

        # Dispatch task afterwards -> must remain queued!
        create_resp = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "consilium_create_mission",
                    "arguments": {
                        "title": "After timeout",
                        "goal": "Task must stay queued",
                        "coordinator_peer": "antigravity",
                        "tasks": [{"assignee_peer": "claude", "prompt": "Work"}],
                    },
                },
            }
        )
        mission_id = create_resp["result"]["structuredContent"]["mission"]["mission_id"]

        # Even calling consilium_wait again (immediate wakeup) without auto_pickup leaves task queued!
        wait2 = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "consilium_wait",
                    "arguments": {"peer_id": "claude", "timeout_sec": 0.5},
                },
            }
        )
        self.assertTrue(wait2["result"]["structuredContent"]["woke"])
        self.assertIsNone(wait2["result"]["structuredContent"]["inbox"])

        snap = dispatcher.store.get_mission_snapshot(mission_id)
        self.assertEqual(snap["tasks"][0]["status"], "queued")

    # ---- R005 3.2: .seq locking, startup rebuild, .sig deletion recovery ----
    def test_3_2_seq_concurrent_writers_converge_to_db_truth(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = store.create_mission(
            title="Seq race",
            goal="Verify flocked DB re-query",
            coordinator_peer="antigravity",
            tasks=[{"assignee_peer": "claude", "prompt": "t0"}],
        )
        mid = created["mission"]["mission_id"]

        def _worker(i: int) -> None:
            store.dispatch_task(mid, "claude", f"task-{i}")
            if i % 3 == 0:
                time.sleep(0.01)
                store.sync_peer_seq("claude")

        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(_worker, i) for i in range(12)]
            for f in futs:
                f.result(timeout=10.0)

        expected_seq, expected_unacked, expected_queued = store.query_peer_seq_counts("claude")
        actual = read_peer_seq(self.tmp_rv, "claude")
        self.assertEqual(actual["seq"], expected_seq)
        self.assertEqual(actual["unacked"], expected_unacked)
        self.assertEqual(actual["queued_tasks"], expected_queued)

    def test_3_2_stdio_rebuilds_deleted_signals_dir(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        store.create_mission(
            title="Rebuild seq",
            goal="Recover after /tmp cleanup",
            coordinator_peer="chatgpt",
            tasks=[{"assignee_peer": "claude", "prompt": "Recover me"}],
        )
        sig_dir = os.path.join(self.tmp_rv, "signals")
        shutil.rmtree(sig_dir)
        self.assertFalse(os.path.exists(os.path.join(sig_dir, "claude.seq")))

        run_stdio_server(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            stdin=io.StringIO('{"jsonrpc":"2.0","id":1,"method":"ping"}\n'),
            stdout=io.StringIO(),
        )
        rebuilt = read_peer_seq(self.tmp_rv, "claude")
        self.assertGreaterEqual(rebuilt["seq"], 1)
        self.assertEqual(rebuilt["unacked"], 1)
        self.assertEqual(rebuilt["queued_tasks"], 1)

    def test_3_2_kqueue_recovers_after_sig_deleted_during_wait(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        result = {}

        def _waiter() -> None:
            result["res"] = wait_for_doorbell(
                store=None,
                rendezvous_dir=self.tmp_rv,
                peer_id="claude",
                timeout_sec=5.0,
                auto_pickup=False,
            )

        t = threading.Thread(target=_waiter, daemon=True)
        t.start()
        sig_path = os.path.join(self.tmp_rv, "signals", "claude.sig")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not os.path.exists(sig_path):
            time.sleep(0.01)
        time.sleep(0.05)

        # Delete .sig while waiter is actively blocked in kqueue!
        os.unlink(sig_path)
        time.sleep(0.08)

        # Now create a mission and ring claude -> waiter must still wake!
        created = store.create_mission(
            title="Deleted sig test",
            goal="Must wake after NOTE_DELETE re-attach",
            coordinator_peer="antigravity",
            tasks=[{"assignee_peer": "claude", "prompt": "Wake after delete"}],
        )
        ring_peer(
            store=store,
            home_dir=self.tmp_rv,
            peer_id="claude",
            title="Deleted sig test",
            body="Wake",
            pickup_prompt="Check inbox",
            dry_run=True,
            event_ids=[created["events"][0]["event_id"]],
            escalation_delay_sec=0.0,
        )
        t.join(timeout=2.0)
        self.assertFalse(t.is_alive())
        self.assertTrue(result["res"]["woke"])
        self.assertEqual(result["res"]["source"], "kqueue")

    # ---- R005 3.3: rotate-token updates clients first, auth.token last, live reload ----
    def test_3_3_rotate_token_and_live_daemon_reload(self) -> None:
        os.makedirs(os.path.join(self.tmp_home, ".codex"), exist_ok=True)
        with open(os.path.join(self.tmp_home, ".codex", "config.toml"), "w", encoding="utf-8") as h:
            h.write('model = "gpt-5"\n')
        run_installer(
            mode="apply",
            target_home=self.tmp_home,
            repo_dir=self.repo_dir,
            port=18920,
            rendezvous_dir=self.tmp_rv,
            claude_running_check=lambda: False,
        )
        state_dir = os.path.join(self.tmp_home, "Library", "Application Support", "consilium")
        with open(os.path.join(state_dir, "auth.token"), encoding="utf-8") as h:
            old_token = h.read().strip()

        daemon = ConsiliumDaemon(
            home_dir=state_dir, rendezvous_dir=self.tmp_rv, host="127.0.0.1", port=18920
        )
        s_old_ok, _, _ = _raw_http_roundtrip(
            daemon, "GET", "/health", {"Authorization": f"Bearer {old_token}"}
        )
        self.assertEqual(s_old_ok, 200)

        rot = rotate_token(target_home=self.tmp_home, port=18920, state_dir=state_dir)
        self.assertTrue(rot["rotated"])
        self.assertEqual(rot["updated_targets"][-1], "server_auth_token")

        with open(os.path.join(state_dir, "auth.token"), encoding="utf-8") as h:
            new_token = h.read().strip()
        self.assertNotEqual(old_token, new_token)

        # Antigravity config updated with Bearer header (never ?token=)
        with open(
            os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json"),
            encoding="utf-8",
        ) as h:
            ag_raw = h.read()
        self.assertNotIn("?token=", ag_raw)
        self.assertIn(f"Bearer {new_token}", ag_raw)

        # Running daemon immediately rejects old token and accepts new token!
        s_old_rej, _, _ = _raw_http_roundtrip(
            daemon, "GET", "/health", {"Authorization": f"Bearer {old_token}"}
        )
        self.assertEqual(s_old_rej, 401)
        s_new_ok, _, _ = _raw_http_roundtrip(
            daemon, "GET", "/health", {"Authorization": f"Bearer {new_token}"}
        )
        self.assertEqual(s_new_ok, 200)

    # ---- R005 3.4 + R010 Item 5: Expired running task marked stale and notifies coordinator + assignee ----
    def test_3_4_daemon_reaper_requeues_expired_running_task_and_notifies_coordinator(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = store.create_mission(
            title="Stuck running",
            goal="Mark stale running task",
            coordinator_peer="claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Long job"}],
        )
        task_id = created["tasks"][0]["task_id"]
        picked = store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)
        attempt_id = picked["claimed_tasks"][0]["attempt_id"]
        store.report_progress(
            task_id, "chatgpt", "Started work", status="running", attempt_id=attempt_id
        )

        # Backdate updated_at so running task exceeds timeout.
        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "UPDATE tasks SET updated_at = '2020-01-01T00:00:00Z' WHERE task_id = ?",
                (task_id,),
            )
            conn.commit()

        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            reaper_interval_sec=0.05,
            running_timeout_sec=60,
        )
        released = daemon.run_reaper_once()
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["status"], "running")
        self.assertEqual(released[0]["reaper_action"], "marked_stale")
        self.assertTrue(released[0]["stale_at"])

        coord_inbox = store.pickup_inbox("claude", claim_tasks=False)
        coord_kinds = [e["kind"] for e in coord_inbox["events"]]
        self.assertIn("task_stale", coord_kinds)

        assignee_inbox = store.pickup_inbox("chatgpt", claim_tasks=False)
        assignee_kinds = [e["kind"] for e in assignee_inbox["events"]]
        self.assertIn("task_stale", assignee_kinds)

    # ---- R005 3.5: ring_peer error isolated + client_request_id idempotency ----
    def test_3_5_ring_error_returns_committed_true_and_idempotency_prevents_duplicates(self) -> None:
        dispatcher = ConsiliumMCPDispatcher(
            home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv
        )
        with mock.patch.object(mcp_server, "ring_peer", side_effect=RuntimeError("boom")):
            resp1 = dispatcher.handle_payload(
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "consilium_create_mission",
                        "arguments": {
                            "title": "Fault isolation",
                            "goal": "Committed despite ring failure",
                            "coordinator_peer": "claude",
                            "client_request_id": "req-create-001",
                            "tasks": [{"assignee_peer": "chatgpt", "prompt": "Do once"}],
                        },
                    },
                }
            )
        self.assertFalse(resp1["result"]["isError"])
        body1 = resp1["result"]["structuredContent"]
        self.assertTrue(body1["committed"])
        self.assertIn("boom", body1["delivery"]["error"])
        mid1 = body1["mission"]["mission_id"]

        # Retry with the same client_request_id -> must return first mission, no duplicate!
        resp2 = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "consilium_create_mission",
                    "arguments": {
                        "title": "Fault isolation",
                        "goal": "Committed despite ring failure",
                        "coordinator_peer": "claude",
                        "client_request_id": "req-create-001",
                        "tasks": [{"assignee_peer": "chatgpt", "prompt": "Do once"}],
                    },
                },
            }
        )
        self.assertFalse(resp2["result"]["isError"])
        body2 = resp2["result"]["structuredContent"]
        self.assertEqual(body2["mission"]["mission_id"], mid1)
        self.assertTrue(body2["idempotent_replay"])
        self.assertEqual(len(dispatcher.store.get_mission_snapshot()["missions"]), 1)

        # Dispatch with client_request_id retry must not create a second task
        d1 = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tools/call",
                "params": {
                    "name": "consilium_dispatch_task",
                    "arguments": {
                        "mission_id": mid1,
                        "assignee_peer": "antigravity",
                        "prompt": "Second task",
                        "client_request_id": "req-disp-001",
                    },
                },
            }
        )["result"]["structuredContent"]
        d2 = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 13,
                "method": "tools/call",
                "params": {
                    "name": "consilium_dispatch_task",
                    "arguments": {
                        "mission_id": mid1,
                        "assignee_peer": "antigravity",
                        "prompt": "Second task",
                        "client_request_id": "req-disp-001",
                    },
                },
            }
        )["result"]["structuredContent"]
        self.assertEqual(d1["task"]["task_id"], d2["task"]["task_id"])
        self.assertEqual(len(dispatcher.store.get_mission_snapshot(mid1)["tasks"]), 2)

    # ---- R005 3.6: SKILL.md no longer claims pressing Return joins ----
    def test_3_6_skill_md_removes_press_return_claim(self) -> None:
        skill_path = os.path.join(
            self.repo_dir, "adapters", "skill", "consilium-collab", "SKILL.md"
        )
        with open(skill_path, encoding="utf-8") as h:
            text = h.read()
        self.assertNotIn("press Return", text)
        self.assertNotIn("按一次 Return", text)
        self.assertNotIn("按 Return", text)

    # ---- R005 3.7: Two-phase atomic installer, running Claude.app refusal, rollback ----
    def test_3_7_codex_duplicate_leaves_all_other_configs_untouched(self) -> None:
        ag_path = os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json")
        cd_path = os.path.join(
            self.tmp_home, "Library", "Application Support", "Claude", "claude_desktop_config.json"
        )
        cc_path = os.path.join(self.tmp_home, ".claude.json")
        cx_path = os.path.join(self.tmp_home, ".codex", "config.toml")
        for p, content in (
            (ag_path, '{"mcpServers": {"orig": {"url": "http://127.0.0.1:1"}}}\n'),
            (cd_path, '{"mcpServers": {"keep": {"command": "echo"}}}\n'),
            (cc_path, '{"mcpServers": {}}\n'),
            (cx_path, 'model = "gpt-5"\n[mcp_servers."consilium"]\nurl = "http://127.0.0.1:1"\n'),
        ):
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "w", encoding="utf-8") as h:
                h.write(content)

        before_bytes = {}
        for p in (ag_path, cd_path, cc_path, cx_path):
            with open(p, "rb") as h:
                before_bytes[p] = h.read()
        with self.assertRaises(ValueError):
            run_installer(
                mode="apply",
                target_home=self.tmp_home,
                repo_dir=self.repo_dir,
                port=18920,
                rendezvous_dir=self.tmp_rv,
                claude_running_check=lambda: False,
            )
        for p, expected in before_bytes.items():
            with open(p, "rb") as h:
                self.assertEqual(h.read(), expected)
        self.assertFalse(
            os.path.exists(
                os.path.join(self.tmp_home, "Library", "Application Support", "consilium", "auth.token")
            )
        )

        # Also test dotted key detection: mcp_servers.consilium.url = ...
        with open(cx_path, "w", encoding="utf-8") as h:
            h.write('mcp_servers.consilium.url = "http://127.0.0.1:9999/mcp"\n')
        with self.assertRaises(ValueError):
            run_installer(
                mode="dry-run",
                target_home=self.tmp_home,
                repo_dir=self.repo_dir,
                port=18920,
                rendezvous_dir=self.tmp_rv,
                claude_running_check=lambda: False,
            )

    def test_3_7_running_claude_app_refuses_apply(self) -> None:
        with self.assertRaises(RuntimeError):
            run_installer(
                mode="apply",
                target_home=self.tmp_home,
                repo_dir=self.repo_dir,
                port=18920,
                rendezvous_dir=self.tmp_rv,
                claude_running_check=lambda: True,
            )
        self.assertFalse(
            os.path.exists(
                os.path.join(self.tmp_home, "Library", "Application Support", "consilium", "auth.token")
            )
        )

    def test_3_7_claude_add_failure_restores_previous_entry_and_strips_config_dir(self) -> None:
        cc_path = os.path.join(self.tmp_home, ".claude.json")
        old_entry = {"type": "http", "url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": "Bearer OLD"}}
        with open(cc_path, "w", encoding="utf-8") as h:
            json.dump({"mcpServers": {"consilium": old_entry}}, h)

        calls = []

        def _fake_runner(cmd, **kwargs):
            calls.append((cmd, kwargs.get("env", {})))
            if "add-json" in cmd and "OLD" not in cmd[-1]:
                return subprocess.CompletedProcess(cmd, 1, "", "simulated add failure")
            return subprocess.CompletedProcess(cmd, 0, "ok", "")

        prev_cc_dir = os.environ.get("CLAUDE_CONFIG_DIR")
        os.environ["CLAUDE_CONFIG_DIR"] = "/should/be/stripped"
        try:
            with self.assertRaises(RuntimeError):
                run_installer(
                    mode="apply",
                    target_home=self.tmp_home,
                    repo_dir=self.repo_dir,
                    port=18920,
                    rendezvous_dir=self.tmp_rv,
                    claude_running_check=lambda: False,
                    claude_runner=_fake_runner,
                )
        finally:
            if prev_cc_dir is None:
                os.environ.pop("CLAUDE_CONFIG_DIR", None)
            else:
                os.environ["CLAUDE_CONFIG_DIR"] = prev_cc_dir

        # Verify CLAUDE_CONFIG_DIR was stripped and rollback re-added old_entry!
        self.assertTrue(all("CLAUDE_CONFIG_DIR" not in env for _, env in calls))
        add_cmds = [cmd for cmd, _ in calls if "add-json" in cmd]
        self.assertEqual(len(add_cmds), 2)
        self.assertEqual(json.loads(add_cmds[1][-1]), old_entry)

    # ---- P2 Regressions ----
    def test_p2_1_wait_precheck_sees_queued_task_after_unclaimed_pickup(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        store.create_mission(
            title="Queued check",
            goal="Ack without claiming",
            coordinator_peer="claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Stay queued"}],
        )
        store.pickup_inbox("chatgpt", claim_tasks=False)
        self.assertEqual(store.count_unacked_events("chatgpt"), 0)
        self.assertEqual(store.count_queued_tasks("chatgpt"), 1)

        res = wait_for_doorbell(
            store=None,
            rendezvous_dir=self.tmp_rv,
            peer_id="chatgpt",
            timeout_sec=0.5,
            auto_pickup=False,
        )
        self.assertTrue(res["woke"])
        self.assertEqual(res["source"], "immediate")
        self.assertEqual(res["queued_tasks"], 1)

    def test_p2_2_request_review_guards_closed_mission_and_task_ownership(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        m1 = store.create_mission(
            title="M1",
            goal="G1",
            coordinator_peer="claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "T1"}],
        )
        m2 = store.create_mission(
            title="M2",
            goal="G2",
            coordinator_peer="claude",
            tasks=[{"assignee_peer": "antigravity", "prompt": "T2"}],
        )
        with self.assertRaises(ValueError):
            store.request_review(
                m1["mission"]["mission_id"],
                "chatgpt",
                "claude",
                "s",
                "q",
                task_id=m2["tasks"][0]["task_id"],
            )
        store.close_mission(m1["mission"]["mission_id"], "done", peer_id="claude", force=True)
        with self.assertRaises(ValueError):
            store.request_review(m1["mission"]["mission_id"], "chatgpt", "claude", "s", "q")

    def test_p2_3_strict_rpc_and_http_validation(self) -> None:
        daemon = ConsiliumDaemon(home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv)
        # Non-dict batch element -> -32600
        batch_resp = daemon.dispatcher.handle_payload([123, {"jsonrpc": "2.0", "id": 1, "method": "ping"}])
        self.assertEqual(batch_resp[0]["error"]["code"], -32600)
        self.assertEqual(batch_resp[1]["id"], 1)

        # Non-dict params -> -32602
        bad_params = daemon.dispatcher.handle_payload(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": ["nope"]}
        )
        self.assertEqual(bad_params["error"]["code"], -32602)

        # String "false" for boolean param rejected
        bad_bool = daemon.dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "consilium_wait",
                    "arguments": {"peer_id": "claude", "auto_pickup": "false"},
                },
            }
        )
        self.assertTrue(bad_bool["result"]["isError"])

        # Negative Content-Length -> 400
        st, _, _ = _raw_http_roundtrip(
            daemon,
            "POST",
            "/mcp",
            {
                "Authorization": f"Bearer {daemon.auth_token}",
                "Content-Length": "-5",
            },
            body=b"",
        )
        self.assertEqual(st, 400)

    def test_p2_5_quota_null_unknown_expired_reset_and_window_minutes(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        empty = store.list_quota_windows()
        for p in empty["peers"]:
            self.assertIsNone(p["dispatch_recommended"])

        # Past reset_at on exhausted window must be ignored
        store.upsert_quota_windows(
            [
                {
                    "peer_id": "claude",
                    "account_id": "default",
                    "window_key": "5h",
                    "remaining_percent": 0.0,
                    "reset_at": "2020-01-01T00:00:00Z",
                    "source": "manual",
                }
            ]
        )
        after_expired = store.list_quota_windows("claude")["peers"][0]
        self.assertFalse(after_expired["exhausted"])
        self.assertIsNone(after_expired["dispatch_recommended"])

        # Codex rollout preserves window_minutes and does not use limit_id as account_id
        sess_dir = os.path.join(self.tmp_home, ".codex", "sessions", "2026", "11", "19")
        os.makedirs(sess_dir, exist_ok=True)
        with open(os.path.join(sess_dir, "rollout-1.jsonl"), "w", encoding="utf-8") as h:
            h.write(
                json.dumps(
                    {
                        "timestamp": int(time.time()),
                        "payload": {
                            "rate_limits": {
                                "limit_id": "not-an-account-id",
                                "primary": {
                                    "used_percent": 10.0,
                                    "window_minutes": 90,
                                    "resets_at": int(time.time()) + 3600,
                                },
                            }
                        },
                    }
                )
                + "\n"
            )
        synced = sync_local_quotas(store, user_home=self.tmp_home)
        gpt = [p for p in synced["peers"] if p["peer_id"] == "chatgpt"][0]
        self.assertEqual(gpt["windows"][0]["account_id"], "default")
        self.assertEqual(gpt["windows"][0]["window_key"], "90m")

    def test_demo_a_findings_rendezvous_env_precedence_and_peer_activity(self) -> None:
        prev = os.environ.get("CONSILIUM_RENDEZVOUS_DIR")
        os.environ["CONSILIUM_RENDEZVOUS_DIR"] = self.tmp_rv
        try:
            self.assertEqual(
                resolve_rendezvous_dir(None, fallback_dir=self.tmp_state),
                self.tmp_rv,
            )
        finally:
            if prev is None:
                os.environ.pop("CONSILIUM_RENDEZVOUS_DIR", None)
            else:
                os.environ["CONSILIUM_RENDEZVOUS_DIR"] = prev

        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        store.create_mission(
            title="Peer activity",
            goal="Verify working status",
            coordinator_peer="claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
        )
        store.pickup_inbox("chatgpt", session_label="codex-1", claim_tasks=True)
        peers = {p["peer_id"]: p for p in store.list_peers()}
        self.assertEqual(peers["chatgpt"]["status"], "working")
        self.assertEqual(peers["chatgpt"]["task_counts"]["claimed"], 1)
        self.assertNotEqual(peers["claude"]["last_seen_at"], "")


if __name__ == "__main__":
    unittest.main()
