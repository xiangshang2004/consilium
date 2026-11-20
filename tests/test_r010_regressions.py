"""R010 closed-loop regression tests covering all 16 R008/R009/R010 verification items."""

from __future__ import annotations

import contextlib
import fcntl
import http.client
import io
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

from consilium import cli as consilium_cli
from consilium.dashboard import render_dashboard_html
from consilium.mcp_server import (
    SSE_MAX_SUBSCRIBERS,
    ConsiliumDaemon,
    ConsiliumMCPDispatcher,
    run_stdio_server,
)
from consilium.models import (
    ConflictError,
    generate_auth_token,
    read_auth_token,
    write_auth_token,
)
from consilium.store import BusStore
from consilium.wakeup import (
    ColdDoorbell,
    _acquire_lease,
    _count_live_leases,
    _release_lease,
    query_active_waiters,
    read_peer_seq,
    rebuild_all_peer_seqs,
    ring_peer,
    wait_for_doorbell,
)
from scripts import install as install_mod

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _raw_http_roundtrip_host(
    daemon: ConsiliumDaemon,
    method: str,
    path: str,
    headers: Optional[Dict[str, str]] = None,
    body: bytes = b"",
) -> Tuple[int, Dict[str, str], bytes]:
    srv_sock, cli_sock = socket.socketpair()
    srv_thread = threading.Thread(
        target=daemon.handle_socket_once, args=(srv_sock,), daemon=True
    )
    srv_thread.start()
    try:
        req_headers = {"Host": f"{daemon.host}:{daemon.port}", "Connection": "close"}
        if headers:
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


def _open_sse_socket(
    daemon: ConsiliumDaemon,
    path: str,
    headers: Dict[str, str],
) -> Tuple[socket.socket, http.client.HTTPResponse, threading.Thread]:
    srv_sock, cli_sock = socket.socketpair()
    srv_thread = threading.Thread(
        target=daemon.handle_socket_once, args=(srv_sock,), daemon=True
    )
    srv_thread.start()
    req_headers = {"Host": f"{daemon.host}:{daemon.port}", "Connection": "close"}
    req_headers.update(headers)
    lines = [f"GET {path} HTTP/1.1"]
    for k, v in req_headers.items():
        lines.append(f"{k}: {v}")
    cli_sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))
    resp = http.client.HTTPResponse(cli_sock)
    resp.begin()
    return cli_sock, resp, srv_thread


def _read_sse_events(resp: http.client.HTTPResponse) -> List[Dict[str, Any]]:
    raw = resp.read().decode("utf-8")
    events: List[Dict[str, Any]] = []
    for block in raw.split("\n\n"):
        block = block.strip()
        if not block:
            continue
        ev: Dict[str, Any] = {}
        for line in block.splitlines():
            if line.startswith(":"):
                continue
            if line.startswith("id:"):
                ev["id"] = int(line[3:].strip())
            elif line.startswith("event:"):
                ev["event"] = line[6:].strip()
            elif line.startswith("data:"):
                ev["data"] = json.loads(line[5:].strip())
        if ev:
            events.append(ev)
    return events


def _read_one_sse_frame(resp: http.client.HTTPResponse) -> Optional[Dict[str, Any]]:
    ev: Dict[str, Any] = {}
    while True:
        raw_line = resp.fp.readline()
        if not raw_line:
            return ev if ev else None
        line = raw_line.decode("utf-8").rstrip("\r\n")
        if line == "":
            if ev:
                return ev
            continue
        if line.startswith(":"):
            continue
        if line.startswith("id:"):
            ev["id"] = int(line[3:].strip())
        elif line.startswith("event:"):
            ev["event"] = line[6:].strip()
        elif line.startswith("data:"):
            ev["data"] = json.loads(line[5:].strip())


class R010RegressionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self._saved_env: Dict[str, Optional[str]] = {}
        for key in (
            "CONSILIUM_DRY_RUN",
            "CONSILIUM_USER_HOME",
            "CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC",
            "CONSILIUM_STATE_DIR",
            "CONSILIUM_HOME",
            "CONSILIUM_RENDEZVOUS_DIR",
            "CLAUDE_CONFIG_DIR",
            "PATH",
        ):
            self._saved_env[key] = os.environ.get(key)
        self.addCleanup(self._restore_env)

        self.tmp_state = self._make_tmp_dir("dbell-r10st-")
        self.tmp_rv = self._make_tmp_dir("dbell-r10rv-")
        self.tmp_home = self._make_tmp_dir("dbell-r10hm-")

        os.environ["CONSILIUM_DRY_RUN"] = "1"
        os.environ["CONSILIUM_USER_HOME"] = self.tmp_home
        os.environ["CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC"] = "1"
        os.environ["CONSILIUM_STATE_DIR"] = self.tmp_state
        os.environ["CONSILIUM_RENDEZVOUS_DIR"] = self.tmp_rv
        os.environ.pop("CLAUDE_CONFIG_DIR", None)

    def _restore_env(self) -> None:
        for key, prev in self._saved_env.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev

    def _make_tmp_dir(self, prefix: str) -> str:
        path = tempfile.mkdtemp(prefix=prefix, dir="/tmp")
        self.addCleanup(shutil.rmtree, path, True)
        return path

    def _clean_subproc_env(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        env = dict(os.environ)
        env.pop("CLAUDE_CONFIG_DIR", None)
        env["CONSILIUM_DRY_RUN"] = "1"
        env["CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC"] = "1"
        env["CONSILIUM_USER_HOME"] = self.tmp_home
        env["CONSILIUM_STATE_DIR"] = self.tmp_state
        env["CONSILIUM_RENDEZVOUS_DIR"] = self.tmp_rv
        prev_pp = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{prev_pp}" if prev_pp else REPO_ROOT
        if extra:
            env.update(extra)
        return env

    def _mcp_call(
        self,
        dispatcher: ConsiliumMCPDispatcher,
        name: str,
        arguments: Dict[str, Any],
        req_id: int = 1,
    ) -> Tuple[bool, Dict[str, Any]]:
        resp = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        assert resp is not None
        res = resp["result"]
        is_err = bool(res.get("isError"))
        text = res["content"][0]["text"]
        return is_err, json.loads(text)

    def _login_cookie_and_csrf(self, daemon: ConsiliumDaemon) -> Tuple[str, str]:
        token = read_auth_token(self.tmp_state)
        st, hdrs, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            "/api/login",
            {"Content-Type": "application/json"},
            body=json.dumps({"token": token}).encode("utf-8"),
        )
        self.assertEqual(st, 200)
        cookie = hdrs["set-cookie"].split(";", 1)[0]
        st_s, _, body_s = _raw_http_roundtrip_host(
            daemon, "GET", "/api/state", {"Cookie": cookie}
        )
        self.assertEqual(st_s, 200)
        csrf = json.loads(body_s.decode("utf-8"))["csrf_token"]
        return cookie, csrf

    # ------------------------------------------------------------------
    # Item 1: Dual-process concurrent .seq writes + signals/ deletion
    # ------------------------------------------------------------------
    def test_r010_01a_dual_process_seq_writes_and_signals_deletion(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = store.create_mission("DualProc", "Contention test", "claude")
        mid = created["mission"]["mission_id"]

        worker_code = (
            "import sys\n"
            "from consilium.store import BusStore\n"
            "st, rv, mid, wid, n = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])\n"
            "store = BusStore(st, rendezvous_dir=rv)\n"
            "for i in range(n):\n"
            "    store.dispatch_task(mid, 'claude', f'w{wid}-{i}')\n"
            "    store.sync_peer_seq('claude')\n"
        )

        # Phase 1: Pure dual-process .seq contention (must converge without parent rebuild).
        procs = [
            subprocess.Popen(
                [sys.executable, "-B", "-c", worker_code, self.tmp_state, self.tmp_rv, mid, str(w), "10"],
                env=self._clean_subproc_env(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for w in (1, 2)
        ]
        for p in procs:
            out, err = p.communicate(timeout=15)
            self.assertEqual(p.returncode, 0, msg=f"stderr={err} stdout={out}")

        db_seq, db_unacked, db_queued = store.query_peer_seq_counts("claude")
        seq_obj = read_peer_seq(self.tmp_rv, "claude")
        self.assertEqual((seq_obj["seq"], seq_obj["unacked"], seq_obj["queued_tasks"]), (db_seq, db_unacked, db_queued))

        # Phase 2: Dual-process writes while external thread repeatedly deletes signals/.
        stop_del = threading.Event()
        sig_dir = os.path.join(self.tmp_rv, "signals")

        def _deleter() -> None:
            while not stop_del.is_set():
                shutil.rmtree(sig_dir, ignore_errors=True)
                time.sleep(0.004)

        del_thread = threading.Thread(target=_deleter, daemon=True)
        del_thread.start()
        try:
            procs2 = [
                subprocess.Popen(
                    [sys.executable, "-B", "-c", worker_code, self.tmp_state, self.tmp_rv, mid, str(w), "10"],
                    env=self._clean_subproc_env(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                for w in (3, 4)
            ]
            results2 = []
            for p in procs2:
                out, err = p.communicate(timeout=15)
                results2.append((p.returncode, out, err))
            for rc, out, err in results2:
                self.assertEqual(rc, 0, msg=f"Concurrent rmtree crashed writer: {err}")
        finally:
            stop_del.set()
            del_thread.join(timeout=2.0)

        rebuild_all_peer_seqs(store)
        db_seq2, db_unacked2, db_queued2 = store.query_peer_seq_counts("claude")
        seq_obj2 = read_peer_seq(self.tmp_rv, "claude")
        self.assertEqual((seq_obj2["seq"], seq_obj2["unacked"], seq_obj2["queued_tasks"]), (db_seq2, db_unacked2, db_queued2))

    def test_r010_01b_kqueue_recovers_when_entire_signals_dir_deleted_during_wait(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        wait_res: Dict[str, Any] = {}

        def _waiter() -> None:
            wait_res.update(
                wait_for_doorbell(
                    store=None,
                    rendezvous_dir=self.tmp_rv,
                    peer_id="claude",
                    timeout_sec=5.0,
                    auto_pickup=False,
                )
            )

        t = threading.Thread(target=_waiter, daemon=True)
        t.start()

        sig_path = os.path.join(self.tmp_rv, "signals", "claude.sig")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if query_active_waiters(self.tmp_rv).get("claude", 0) == 1 and os.path.exists(sig_path):
                break
            time.sleep(0.01)
        self.assertEqual(query_active_waiters(self.tmp_rv).get("claude"), 1)

        shutil.rmtree(os.path.join(self.tmp_rv, "signals"))
        time.sleep(0.08)

        created = store.create_mission(
            "AfterDelete",
            "Wake after signals dir deletion",
            "chatgpt",
            tasks=[{"assignee_peer": "claude", "prompt": "Wake up"}],
        )
        eids = [int(created["events"][0]["event_id"])]
        ring_peer(
            store=store,
            home_dir=self.tmp_rv,
            peer_id="claude",
            title="Consilium",
            body="Wake",
            pickup_prompt="pickup",
            dry_run=True,
            event_ids=eids,
        )
        t.join(timeout=3.0)
        self.assertFalse(t.is_alive())
        self.assertTrue(wait_res.get("woke"))
        self.assertEqual(wait_res.get("source"), "kqueue")
        self.assertEqual(query_active_waiters(self.tmp_rv).get("claude"), 0)

    # ------------------------------------------------------------------
    # Item 2: Real background reaper thread scheduling
    # ------------------------------------------------------------------
    def test_r010_02_real_background_reaper_thread_scheduling(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = store.create_mission(
            "BgReaper",
            "Verify real reaper thread",
            "claude",
            tasks=[
                {"assignee_peer": "chatgpt", "prompt": "Stale claimed"},
                {"assignee_peer": "chatgpt", "prompt": "Stale running"},
            ],
        )
        mid = created["mission"]["mission_id"]
        t_claim = created["tasks"][0]["task_id"]
        t_run = created["tasks"][1]["task_id"]

        picked = store.pickup_inbox("chatgpt", session_label="s-reap", claim_tasks=True)
        attempts = {t["task_id"]: t["attempt_id"] for t in picked["claimed_tasks"]}
        store.report_progress(
            t_run,
            "chatgpt",
            "still working",
            status="running",
            attempt_id=attempts[t_run],
        )

        with sqlite3.connect(os.path.join(self.tmp_state, "bus.db")) as conn:
            conn.execute(
                "UPDATE tasks SET claimed_at = '2020-01-01T00:00:00Z' WHERE task_id = ?",
                (t_claim,),
            )
            conn.execute(
                "UPDATE tasks SET updated_at = '2020-01-01T00:00:00Z' WHERE task_id = ?",
                (t_run,),
            )
            conn.commit()

        wait_out: Dict[str, Any] = {}

        def _standby() -> None:
            wait_out.update(
                wait_for_doorbell(
                    store=None,
                    rendezvous_dir=self.tmp_rv,
                    peer_id="chatgpt",
                    timeout_sec=5.0,
                    auto_pickup=False,
                )
            )

        wt = threading.Thread(target=_standby, daemon=True)
        wt.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if query_active_waiters(self.tmp_rv).get("chatgpt", 0) == 1:
                break
            time.sleep(0.01)
        self.assertEqual(query_active_waiters(self.tmp_rv).get("chatgpt"), 1)

        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=0,
            reaper_interval_sec=0.05,
            claim_timeout_sec=1,
            running_timeout_sec=1,
        )
        daemon.start()
        self.addCleanup(daemon.stop)

        snap: Dict[str, Any] = {}
        poll_end = time.monotonic() + 3.0
        while time.monotonic() < poll_end:
            snap = store.get_mission_snapshot(mid)
            by_id = {t["task_id"]: t for t in snap["tasks"]}
            if by_id[t_claim]["status"] == "queued" and by_id[t_run]["stale_at"]:
                break
            time.sleep(0.03)

        by_id = {t["task_id"]: t for t in snap["tasks"]}
        self.assertEqual(by_id[t_claim]["status"], "queued")
        self.assertEqual(by_id[t_claim]["attempt_id"], "")
        self.assertEqual(by_id[t_claim]["attempt_seq"], 1)
        self.assertEqual(len(by_id[t_claim]["attempt_history"]), 1)
        self.assertIn("claimed timeout", by_id[t_claim]["attempt_history"][0]["reason"])

        self.assertEqual(by_id[t_run]["status"], "running")
        self.assertTrue(by_id[t_run]["stale_at"])
        self.assertTrue(by_id[t_run]["running_stale"])
        self.assertEqual(by_id[t_run]["attempt_id"], attempts[t_run])

        requeued_evts = [e for e in snap["events"] if e["kind"] == "task_requeued"]
        stale_evts = [e for e in snap["events"] if e["kind"] == "task_stale"]
        self.assertTrue(any(e["to_peer"] == "claude" for e in requeued_evts))
        self.assertEqual({e["to_peer"] for e in stale_evts}, {"claude", "chatgpt"})

        wt.join(timeout=3.0)
        self.assertFalse(wt.is_alive())
        self.assertTrue(wait_out.get("woke"))
        self.assertIn(wait_out.get("source"), ("kqueue", "socket"))

    # ------------------------------------------------------------------
    # Item 3: Post-commit fault injection across 7 MCP tools + 2 dashboard routes
    # ------------------------------------------------------------------
    def test_r010_03_post_commit_fault_isolation_mcp_and_dashboard(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        disp = daemon.dispatcher
        cookie, csrf = self._login_cookie_and_csrf(daemon)

        def _exercise_all_tools(expected_substr: str) -> None:
            err, cm = self._mcp_call(
                disp,
                "consilium_create_mission",
                {
                    "title": f"Fault-{expected_substr}",
                    "goal": "Isolate post-commit",
                    "coordinator_peer": "claude",
                    "tasks": [{"assignee_peer": "chatgpt", "prompt": "Task 1"}],
                },
            )
            self.assertFalse(err)
            self.assertTrue(cm["committed"])
            self.assertTrue(any(expected_substr in w for w in cm["delivery"].get("warnings", [])))
            mid = cm["mission"]["mission_id"]

            err, dt = self._mcp_call(
                disp,
                "consilium_dispatch_task",
                {"mission_id": mid, "assignee_peer": "antigravity", "prompt": "Task 2"},
            )
            self.assertFalse(err)
            self.assertTrue(dt["committed"])
            self.assertTrue(any(expected_substr in w for w in dt["delivery"].get("warnings", [])))
            tid2 = dt["task"]["task_id"]

            picked = disp.store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)
            a1 = picked["claimed_tasks"][0]["attempt_id"]
            tid1 = picked["claimed_tasks"][0]["task_id"]

            err, rp = self._mcp_call(
                disp,
                "consilium_report_progress",
                {
                    "task_id": tid1,
                    "peer_id": "chatgpt",
                    "progress_note": "halfway",
                    "notify_coordinator": True,
                    "attempt_id": a1,
                },
            )
            self.assertFalse(err)
            self.assertTrue(rp["committed"])
            self.assertTrue(any(expected_substr in w for w in rp["delivery"].get("warnings", [])))

            err, rr = self._mcp_call(
                disp,
                "consilium_request_review",
                {
                    "mission_id": mid,
                    "from_peer": "chatgpt",
                    "to_peer": "claude",
                    "summary": "Review summary",
                    "question": "Looks ok?",
                    "task_id": tid1,
                },
            )
            self.assertFalse(err)
            self.assertTrue(rr["committed"])
            self.assertTrue(any(expected_substr in w for w in rr["delivery"].get("warnings", [])))

            err, ct = self._mcp_call(
                disp,
                "consilium_complete_task",
                {
                    "task_id": tid1,
                    "peer_id": "chatgpt",
                    "result_summary": "Done",
                    "attempt_id": a1,
                },
            )
            self.assertFalse(err)
            self.assertTrue(ct["committed"])
            self.assertTrue(any(expected_substr in w for w in ct["delivery"].get("warnings", [])))

            picked_ag = disp.store.pickup_inbox("antigravity", session_label="s2", claim_tasks=True)
            self.assertEqual(picked_ag["claimed_tasks"][0]["task_id"], tid2)

            err, rq = self._mcp_call(
                disp,
                "consilium_requeue_task",
                {"task_id": tid2, "peer_id": "claude", "reason": "retry"},
            )
            self.assertFalse(err)
            self.assertTrue(rq["committed"])
            self.assertTrue(any(expected_substr in w for w in rq["delivery"].get("warnings", [])))

            err, cl = self._mcp_call(
                disp,
                "consilium_close_mission",
                {
                    "mission_id": mid,
                    "peer_id": "claude",
                    "final_summary": "Force close",
                    "force": True,
                },
            )
            self.assertFalse(err)
            self.assertTrue(cl["committed"])
            self.assertTrue(any(expected_substr in w for w in cl["delivery"].get("warnings", [])))

        with mock.patch.object(disp.store, "sync_peer_seq", side_effect=OSError("disk full seq")):
            _exercise_all_tools("disk full seq")

        with mock.patch.object(
            disp.store, "record_event_delivery", side_effect=RuntimeError("receipt db error")
        ):
            _exercise_all_tools("receipt db error")

        with mock.patch.object(
            disp, "_on_mutation", side_effect=RuntimeError("sse publish failed")
        ):
            _exercise_all_tools("sse publish failed")

        # Verify ring_peer error deduplication across multi-peer create/close
        with mock.patch("consilium.mcp_server.ring_peer", side_effect=RuntimeError("boom")):
            err, cm_ring = self._mcp_call(
                disp,
                "consilium_create_mission",
                {
                    "title": "RingDedup",
                    "goal": "Single warning per error",
                    "coordinator_peer": "claude",
                    "tasks": [{"assignee_peer": "chatgpt", "prompt": "Work"}],
                },
            )
            self.assertFalse(err)
            self.assertTrue(cm_ring["committed"])
            self.assertEqual(cm_ring["delivery"]["warnings"], ["RuntimeError: boom"])

        # Dashboard POST /api/tasks/<id>/requeue and /api/missions/<id>/close fault isolation
        m2 = disp.store.create_mission(
            "DashFault",
            "Dashboard post-commit fault",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
        )
        m2_id = m2["mission"]["mission_id"]
        t2_id = m2["tasks"][0]["task_id"]
        disp.store.pickup_inbox("chatgpt", session_label="s3", claim_tasks=True)

        dash_hdrs = {
            "Cookie": cookie,
            "Origin": f"http://{daemon.host}:{daemon.port}",
            "Content-Type": "application/json",
            "X-Consilium-CSRF": csrf,
        }
        with mock.patch.object(
            daemon, "publish_state_update", side_effect=RuntimeError("dash sse fail")
        ), mock.patch.object(
            disp.store, "sync_peer_seq", side_effect=OSError("dash seq fail")
        ), mock.patch.object(
            disp.store, "record_event_delivery", side_effect=RuntimeError("dash rec fail")
        ):
            st_rq, _, b_rq = _raw_http_roundtrip_host(
                daemon,
                "POST",
                f"/api/tasks/{t2_id}/requeue",
                dash_hdrs,
                body=json.dumps({"reason": "ui retry"}).encode("utf-8"),
            )
            self.assertEqual(st_rq, 200)
            p_rq = json.loads(b_rq.decode("utf-8"))
            self.assertTrue(p_rq["committed"])
            w_rq = " | ".join(p_rq["delivery"]["warnings"])
            self.assertIn("dash sse fail", w_rq)
            self.assertIn("dash seq fail", w_rq)
            self.assertIn("dash rec fail", w_rq)

            st_cl, _, b_cl = _raw_http_roundtrip_host(
                daemon,
                "POST",
                f"/api/missions/{m2_id}/close",
                dash_hdrs,
                body=json.dumps({"final_summary": "ui close", "force": True}).encode("utf-8"),
            )
            self.assertEqual(st_cl, 200)
            p_cl = json.loads(b_cl.decode("utf-8"))
            self.assertTrue(p_cl["committed"])
            w_cl = " | ".join(p_cl["delivery"]["warnings"])
            self.assertIn("dash sse fail", w_cl)
            self.assertIn("dash seq fail", w_cl)
            self.assertIn("dash rec fail", w_cl)

    # ------------------------------------------------------------------
    # Item 4: Stale attempt_id rejection after reclaim / requeue
    # ------------------------------------------------------------------
    def test_r010_04_stale_attempt_id_rejected_after_reclaim_and_requeue(self) -> None:
        disp = ConsiliumMCPDispatcher(home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = disp.store.create_mission(
            "AttemptFence",
            "Stale attempt_id fencing",
            "claude",
            tasks=[
                {"assignee_peer": "chatgpt", "prompt": "Task 1"},
                {"assignee_peer": "chatgpt", "prompt": "Task 2"},
            ],
        )
        mid = created["mission"]["mission_id"]
        t1 = created["tasks"][0]["task_id"]
        t2 = created["tasks"][1]["task_id"]

        picked1 = disp.store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)
        a1_by_task = {t["task_id"]: t["attempt_id"] for t in picked1["claimed_tasks"]}
        a1_t1 = a1_by_task[t1]
        a1_t2 = a1_by_task[t2]

        # Reclaim T1 via stale claimed timeout -> queued (attempt_id == "")
        disp.store.report_progress(t2, "chatgpt", "active", status="running", attempt_id=a1_t2)
        with sqlite3.connect(os.path.join(self.tmp_state, "bus.db")) as conn:
            conn.execute("UPDATE tasks SET claimed_at = '2020-01-01T00:00:00Z' WHERE task_id = ?", (t1,))
            conn.commit()
        disp.store.release_stale_claims(claimed_timeout_sec=1)

        with self.assertRaises(ValueError):
            disp.store.report_progress(t1, "chatgpt", "late", attempt_id=a1_t1)
        with self.assertRaises(ValueError):
            disp.store.complete_task(t1, "chatgpt", "late", attempt_id=a1_t1)
        err_rp1, _ = self._mcp_call(
            disp,
            "consilium_report_progress",
            {"task_id": t1, "peer_id": "chatgpt", "progress_note": "late", "attempt_id": a1_t1},
        )
        err_ct1, _ = self._mcp_call(
            disp,
            "consilium_complete_task",
            {"task_id": t1, "peer_id": "chatgpt", "result_summary": "late", "attempt_id": a1_t1},
        )
        self.assertTrue(err_rp1)
        self.assertTrue(err_ct1)

        # Re-claim T1 (A-2-...), then also requeue T2 and re-claim T2 (A-2-...)
        picked2 = disp.store.pickup_inbox("chatgpt", session_label="s2", claim_tasks=True)
        self.assertEqual(len(picked2["claimed_tasks"]), 1)
        a2_t1 = picked2["claimed_tasks"][0]["attempt_id"]
        self.assertNotEqual(a2_t1, a1_t1)

        with self.assertRaisesRegex(ValueError, "Stale or missing attempt_id"):
            disp.store.report_progress(t1, "chatgpt", "hijack", attempt_id=a1_t1)
        with self.assertRaisesRegex(ValueError, "Stale or missing attempt_id"):
            disp.store.complete_task(t1, "chatgpt", "hijack", attempt_id=a1_t1)
        err_rp2, _ = self._mcp_call(
            disp,
            "consilium_report_progress",
            {"task_id": t1, "peer_id": "chatgpt", "progress_note": "hijack", "attempt_id": a1_t1},
        )
        err_ct2, _ = self._mcp_call(
            disp,
            "consilium_complete_task",
            {"task_id": t1, "peer_id": "chatgpt", "result_summary": "hijack", "attempt_id": a1_t1},
        )
        self.assertTrue(err_rp2)
        self.assertTrue(err_ct2)

        snap = disp.store.get_mission_snapshot(mid)
        t1_obj = [t for t in snap["tasks"] if t["task_id"] == t1][0]
        self.assertEqual(t1_obj["status"], "claimed")
        self.assertEqual(t1_obj["attempt_id"], a2_t1)
        self.assertEqual(t1_obj["progress_note"], "")

        # Current attempt_2 succeeds
        err_ok, _ = self._mcp_call(
            disp,
            "consilium_complete_task",
            {"task_id": t1, "peer_id": "chatgpt", "result_summary": "valid", "attempt_id": a2_t1},
        )
        self.assertFalse(err_ok)

        # Manual requeue T2 -> stale a1_t2 rejected before & after re-claim
        disp.store.requeue_task(t2, reason="coord_reset", actor_peer="claude")
        with self.assertRaises(ValueError):
            disp.store.report_progress(t2, "chatgpt", "late2", attempt_id=a1_t2)
        picked3 = disp.store.pickup_inbox("chatgpt", session_label="s3", claim_tasks=True)
        a2_t2 = picked3["claimed_tasks"][0]["attempt_id"]
        self.assertNotEqual(a2_t2, a1_t2)
        with self.assertRaisesRegex(ValueError, "Stale or missing attempt_id"):
            disp.store.complete_task(t2, "chatgpt", "late2", attempt_id=a1_t2)

    # ------------------------------------------------------------------
    # Item 5: Cookie CSRF, exact Origin, and Content-Type enforcement
    # ------------------------------------------------------------------
    def test_r010_05_cookie_write_origin_csrf_and_content_type_guards(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        created = daemon.dispatcher.store.create_mission(
            "CsrfMission",
            "Test strict origin/csrf/content-type",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
        )
        mid = created["mission"]["mission_id"]
        tid = created["tasks"][0]["task_id"]
        daemon.dispatcher.store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)

        cookie, csrf = self._login_cookie_and_csrf(daemon)
        exact_origin = f"http://{daemon.host}:{daemon.port}"

        for route, body_obj in (
            (f"/api/tasks/{tid}/requeue", {"reason": "r"}),
            (f"/api/missions/{mid}/close", {"final_summary": "s", "force": True}),
            ("/api/logout", {}),
        ):
            payload = json.dumps(body_obj).encode("utf-8")
            # Origin bypass checks -> 403
            for bad_headers in (
                {"Cookie": cookie, "Origin": "http://127.0.0.1:9999", "Content-Type": "application/json", "X-Consilium-CSRF": csrf},
                {"Cookie": cookie, "Origin": "http://127.0.0.1", "Content-Type": "application/json", "X-Consilium-CSRF": csrf},
                {"Cookie": cookie, "Origin": f"https://127.0.0.1:{daemon.port}", "Content-Type": "application/json", "X-Consilium-CSRF": csrf},
                {"Cookie": cookie, "Content-Type": "application/json", "X-Consilium-CSRF": csrf},
                {"Cookie": cookie, "Origin": f"http://localhost:{daemon.port}", "Content-Type": "application/json", "X-Consilium-CSRF": csrf},
            ):
                st, _, _ = _raw_http_roundtrip_host(daemon, "POST", route, bad_headers, body=payload)
                self.assertEqual(st, 403, msg=f"Expected 403 on {route} with {bad_headers}")

            # Non-JSON Content-Type -> 415
            for bad_ct in ("text/plain", "application/x-www-form-urlencoded"):
                st_ct, _, _ = _raw_http_roundtrip_host(
                    daemon,
                    "POST",
                    route,
                    {"Cookie": cookie, "Origin": exact_origin, "Content-Type": bad_ct, "X-Consilium-CSRF": csrf},
                    body=payload,
                )
                self.assertEqual(st_ct, 415, msg=f"Expected 415 on {route} with {bad_ct}")

            # Missing or bogus X-Consilium-CSRF -> 403
            for bad_csrf in (None, "bogus-csrf"):
                hdrs = {"Cookie": cookie, "Origin": exact_origin, "Content-Type": "application/json"}
                if bad_csrf is not None:
                    hdrs["X-Consilium-CSRF"] = bad_csrf
                st_csrf, _, _ = _raw_http_roundtrip_host(daemon, "POST", route, hdrs, body=payload)
                self.assertEqual(st_csrf, 403)

        # Valid exact Origin + application/json + X-Consilium-CSRF -> 200
        valid_hdrs = {
            "Cookie": cookie,
            "Origin": exact_origin,
            "Content-Type": "application/json",
            "X-Consilium-CSRF": csrf,
        }
        st_rq, _, _ = _raw_http_roundtrip_host(
            daemon, "POST", f"/api/tasks/{tid}/requeue", valid_hdrs, body=json.dumps({"reason": "ok"}).encode("utf-8")
        )
        self.assertEqual(st_rq, 200)
        st_cl, _, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            f"/api/missions/{mid}/close",
            valid_hdrs,
            body=json.dumps({"final_summary": "ok", "force": True}).encode("utf-8"),
        )
        self.assertEqual(st_cl, 200)

    # ------------------------------------------------------------------
    # Item 6: Post-token-rotation old ticket invalidation + session 401 + SSE closure
    # ------------------------------------------------------------------
    def test_r010_06_post_token_rotation_revokes_tickets_sessions_and_active_sse(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        old_token = read_auth_token(self.tmp_state)

        st_t, _, body_t = _raw_http_roundtrip_host(
            daemon,
            "POST",
            "/api/ui-ticket",
            {"Authorization": f"Bearer {old_token}", "Content-Type": "application/json"},
            body=b"{}",
        )
        self.assertEqual(st_t, 200)
        old_ticket = json.loads(body_t.decode("utf-8"))["ticket"]

        cookie, csrf = self._login_cookie_and_csrf(daemon)
        cli_sock, sse_resp, sse_thread = _open_sse_socket(
            daemon, "/api/stream", {"Cookie": cookie}
        )
        self.addCleanup(cli_sock.close)
        first_frame = _read_one_sse_frame(sse_resp)
        self.assertIsNotNone(first_frame)
        self.assertEqual(first_frame["event"], "state")

        # Rotate auth.token on disk (bump mtime_ns cleanly as well)
        new_token = generate_auth_token()
        write_auth_token(self.tmp_state, new_token)

        # Old ticket -> 302 without Set-Cookie
        st_auth, hdrs_auth, _ = _raw_http_roundtrip_host(
            daemon, "GET", f"/ui/auth?ticket={old_ticket}", {}
        )
        self.assertEqual(st_auth, 302)
        self.assertNotIn("set-cookie", hdrs_auth)

        # Old session cookie -> 401 on /api/state and cookie-auth write
        created = daemon.dispatcher.store.create_mission(
            "RevokeWrite",
            "Verify old session rejected on write",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
        )
        tid = created["tasks"][0]["task_id"]
        daemon.dispatcher.store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)

        st_state, _, _ = _raw_http_roundtrip_host(
            daemon, "GET", "/api/state", {"Cookie": cookie}
        )
        self.assertEqual(st_state, 401)
        st_write, _, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            f"/api/tasks/{tid}/requeue",
            {
                "Cookie": cookie,
                "Origin": f"http://{daemon.host}:{daemon.port}",
                "Content-Type": "application/json",
                "X-Consilium-CSRF": csrf,
            },
            body=json.dumps({"reason": "stale cookie"}).encode("utf-8"),
        )
        self.assertEqual(st_write, 401)

        # Active SSE stream closes promptly (within 2s)
        sse_thread.join(timeout=2.0)
        self.assertFalse(sse_thread.is_alive())
        tail = sse_resp.read()
        self.assertEqual(tail, b"")

    # ------------------------------------------------------------------
    # Item 7: Scoped idempotency conflict (ConflictError / isError / HTTP 409)
    # ------------------------------------------------------------------
    def test_r010_07_scoped_idempotency_conflicts(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        disp = daemon.dispatcher
        token = read_auth_token(self.tmp_state)

        # 1. Different params on create_mission (store) & dispatch_task (MCP)
        cm = disp.store.create_mission(
            "IdemA",
            "Goal",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
            client_request_id="req-create-1",
        )
        mid = cm["mission"]["mission_id"]
        tid1 = cm["tasks"][0]["task_id"]
        with self.assertRaises(ConflictError):
            disp.store.create_mission(
                "IdemB",
                "Goal",
                "claude",
                client_request_id="req-create-1",
            )

        # 2. Different operation reusing same client_request_id -> MCP isError & store ConflictError
        err_op, op_body = self._mcp_call(
            disp,
            "consilium_dispatch_task",
            {
                "mission_id": mid,
                "assignee_peer": "chatgpt",
                "prompt": "Different op",
                "client_request_id": "req-create-1",
            },
        )
        self.assertTrue(err_op)
        self.assertIn("create_mission", op_body["error"])
        with self.assertRaises(ConflictError):
            disp.store.dispatch_task(mid, "chatgpt", "Different op", client_request_id="req-create-1")

        # 3. Same operation + same id + different params -> MCP isError
        err_ok, dt_ok = self._mcp_call(
            disp,
            "consilium_dispatch_task",
            {
                "mission_id": mid,
                "assignee_peer": "chatgpt",
                "prompt": "Prompt 1",
                "client_request_id": "req-disp-1",
            },
        )
        self.assertFalse(err_ok)
        tid2 = dt_ok["task"]["task_id"]
        err_diff, diff_body = self._mcp_call(
            disp,
            "consilium_dispatch_task",
            {
                "mission_id": mid,
                "assignee_peer": "chatgpt",
                "prompt": "Prompt 2",
                "client_request_id": "req-disp-1",
            },
        )
        self.assertTrue(err_diff)
        self.assertIn("different parameters", diff_body["error"])

        # 4. Different caller_scope across stdio default_peer dispatchers & store/dashboard
        disp_claude = ConsiliumMCPDispatcher(
            home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv, default_peer="claude"
        )
        disp_antigravity = ConsiliumMCPDispatcher(
            home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv, default_peer="antigravity"
        )
        err_c, _ = self._mcp_call(
            disp_claude,
            "consilium_dispatch_task",
            {
                "mission_id": mid,
                "assignee_peer": "chatgpt",
                "prompt": "Scoped",
                "client_request_id": "req-disp-scope",
            },
        )
        self.assertFalse(err_c)
        err_ag, ag_body = self._mcp_call(
            disp_antigravity,
            "consilium_dispatch_task",
            {
                "mission_id": mid,
                "assignee_peer": "chatgpt",
                "prompt": "Scoped",
                "client_request_id": "req-disp-scope",
            },
        )
        self.assertTrue(err_ag)
        self.assertIn("caller scope", ag_body["error"])

        # Different caller on create_mission
        with self.assertRaises(ConflictError):
            disp.store.create_mission(
                "IdemA",
                "Goal",
                "chatgpt",
                tasks=[{"assignee_peer": "claude", "prompt": "Work"}],
                client_request_id="req-create-1",
            )

        # HTTP 409 on dashboard /requeue (caller_scope vs peer & different params)
        disp.store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)
        disp.store.requeue_task(tid1, reason="r1", actor_peer="claude", client_request_id="req-rq-scope")
        st_rq_scope, _, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            f"/api/tasks/{tid1}/requeue",
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            body=json.dumps({"reason": "r1", "client_request_id": "req-rq-scope"}).encode("utf-8"),
        )
        self.assertEqual(st_rq_scope, 409)

        st_rq_ok, _, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            f"/api/tasks/{tid2}/requeue",
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            body=json.dumps({"reason": "dash-r1", "client_request_id": "req-http-rq"}).encode("utf-8"),
        )
        self.assertEqual(st_rq_ok, 200)
        st_rq_409, _, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            f"/api/tasks/{tid2}/requeue",
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            body=json.dumps({"reason": "dash-r2", "client_request_id": "req-http-rq"}).encode("utf-8"),
        )
        self.assertEqual(st_rq_409, 409)

        # HTTP 409 on dashboard /close (different params + different caller scope)
        st_cls_ok, _, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            f"/api/missions/{mid}/close",
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            body=json.dumps({"final_summary": "S1", "force": True, "client_request_id": "req-http-cls"}).encode("utf-8"),
        )
        self.assertEqual(st_cls_ok, 200)
        st_cls_409, _, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            f"/api/missions/{mid}/close",
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            body=json.dumps({"final_summary": "S2", "force": True, "client_request_id": "req-http-cls"}).encode("utf-8"),
        )
        self.assertEqual(st_cls_409, 409)
        with self.assertRaises(ConflictError):
            disp.store.close_mission(mid, "S1", peer_id="claude", force=True, client_request_id="req-http-cls")

    # ------------------------------------------------------------------
    # Item 8: Strictly read-only wait (MCP & CLI)
    # ------------------------------------------------------------------
    def test_r010_08_wait_strictly_read_only_mcp_and_cli(self) -> None:
        disp = ConsiliumMCPDispatcher(home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = disp.store.create_mission(
            "ReadOnlyWait",
            "Verify wait does not mutate",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Stay queued"}],
        )
        mid = created["mission"]["mission_id"]
        tid = created["tasks"][0]["task_id"]

        err_w, wres = self._mcp_call(
            disp,
            "consilium_wait",
            {"peer_id": "chatgpt", "auto_pickup": True, "timeout_sec": 0.2},
        )
        self.assertFalse(err_w)
        self.assertTrue(wres["woke"])
        self.assertIsNone(wres["inbox"])
        self.assertTrue(wres.get("auto_pickup_ignored"))

        snap = disp.store.get_mission_snapshot(mid)
        self.assertEqual(snap["tasks"][0]["status"], "queued")
        self.assertEqual(snap["events"][0]["acked_at"], "")

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            rc = consilium_cli.main(
                [
                    "wait",
                    "--peer",
                    "chatgpt",
                    "--pickup",
                    "--timeout",
                    "0.2",
                    "--data-dir",
                    self.tmp_state,
                    "--rendezvous-dir",
                    self.tmp_rv,
                ]
            )
        self.assertEqual(rc, 0)
        cli_out = json.loads(stdout_buf.getvalue())
        self.assertTrue(cli_out["woke"])
        self.assertIsNone(cli_out["inbox"])
        self.assertTrue(cli_out.get("auto_pickup_ignored"))
        self.assertIn("--pickup", stderr_buf.getvalue())

        snap2 = disp.store.get_mission_snapshot(mid)
        self.assertEqual(snap2["tasks"][0]["task_id"], tid)
        self.assertEqual(snap2["tasks"][0]["status"], "queued")
        self.assertEqual(snap2["events"][0]["acked_at"], "")

    # ------------------------------------------------------------------
    # Item 9: ?token= 401 & consilium serve --help Bearer-only text
    # ------------------------------------------------------------------
    def test_r010_09_query_token_401_and_serve_help_bearer_only(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
            require_token=True,
        )
        self.addCleanup(daemon.stop)
        tok = read_auth_token(self.tmp_state)

        for method, path in (
            ("GET", f"/api/state?token={tok}"),
            ("POST", f"/api/ui-ticket?token={tok}"),
            ("POST", f"/mcp?token={tok}"),
            ("GET", f"/api/stream?token={tok}"),
        ):
            st, _, _ = _raw_http_roundtrip_host(daemon, method, path, {})
            self.assertEqual(st, 401, msg=f"Expected 401 for {method} {path}")

        help_out = io.StringIO()
        with contextlib.redirect_stdout(help_out):
            with self.assertRaises(SystemExit) as cm:
                consilium_cli.main(["serve", "--help"])
        self.assertEqual(cm.exception.code, 0)
        txt = help_out.getvalue()
        self.assertIn("Bearer", txt)
        self.assertNotIn("?token=", txt)
        self.assertNotIn("query token", txt.lower())

    # ------------------------------------------------------------------
    # Item 10: Security response headers across all response paths
    # ------------------------------------------------------------------
    def test_r010_10_security_headers_across_all_routes(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        tok = read_auth_token(self.tmp_state)

        # GET /ui (HTML)
        st_ui, h_ui, _ = _raw_http_roundtrip_host(daemon, "GET", "/ui", {})
        self.assertEqual(st_ui, 200)
        self.assertEqual(h_ui.get("x-content-type-options"), "nosniff")
        self.assertEqual(h_ui.get("referrer-policy"), "no-referrer")
        self.assertEqual(h_ui.get("cache-control"), "no-store")
        self.assertIn("frame-ancestors 'none'", h_ui.get("content-security-policy", ""))

        # GET / (302)
        st_root, h_root, _ = _raw_http_roundtrip_host(daemon, "GET", "/", {})
        self.assertEqual(st_root, 302)
        self.assertEqual(h_root.get("x-content-type-options"), "nosniff")
        self.assertEqual(h_root.get("referrer-policy"), "no-referrer")
        self.assertEqual(h_root.get("cache-control"), "no-store")

        # GET /ui/auth (302)
        st_t, _, b_t = _raw_http_roundtrip_host(
            daemon,
            "POST",
            "/api/ui-ticket",
            {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            body=b"{}",
        )
        self.assertEqual(st_t, 200)
        ticket = json.loads(b_t.decode("utf-8"))["ticket"]
        st_auth, h_auth, _ = _raw_http_roundtrip_host(daemon, "GET", f"/ui/auth?ticket={ticket}", {})
        self.assertEqual(st_auth, 302)
        self.assertEqual(h_auth.get("x-content-type-options"), "nosniff")
        self.assertEqual(h_auth.get("referrer-policy"), "no-referrer")
        self.assertEqual(h_auth.get("cache-control"), "no-store")

        # JSON 200 & 401
        st_json, h_json, _ = _raw_http_roundtrip_host(
            daemon, "GET", "/api/state", {"Authorization": f"Bearer {tok}"}
        )
        self.assertEqual(st_json, 200)
        self.assertEqual(h_json.get("x-content-type-options"), "nosniff")
        self.assertEqual(h_json.get("referrer-policy"), "no-referrer")
        self.assertEqual(h_json.get("cache-control"), "no-store")

        st_401, h_401, _ = _raw_http_roundtrip_host(daemon, "GET", "/api/state", {})
        self.assertEqual(st_401, 401)
        self.assertEqual(h_401.get("x-content-type-options"), "nosniff")
        self.assertEqual(h_401.get("referrer-policy"), "no-referrer")
        self.assertEqual(h_401.get("cache-control"), "no-store")

        # POST /mcp notification -> 202
        st_202, h_202, _ = _raw_http_roundtrip_host(
            daemon,
            "POST",
            "/mcp",
            {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            body=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode("utf-8"),
        )
        self.assertEqual(st_202, 202)
        self.assertEqual(h_202.get("x-content-type-options"), "nosniff")
        self.assertEqual(h_202.get("referrer-policy"), "no-referrer")
        self.assertEqual(h_202.get("cache-control"), "no-store")

        # GET /api/stream (SSE)
        cli_sock, sse_resp, sse_thread = _open_sse_socket(
            daemon, "/api/stream?max_events=1", {"Authorization": f"Bearer {tok}"}
        )
        try:
            sse_hdrs = {k.lower(): v for k, v in sse_resp.getheaders()}
            self.assertEqual(sse_resp.status, 200)
            self.assertEqual(sse_hdrs.get("x-content-type-options"), "nosniff")
            self.assertEqual(sse_hdrs.get("referrer-policy"), "no-referrer")
            self.assertIn("no-store", sse_hdrs.get("cache-control", ""))
            self.assertIn("no-cache", sse_hdrs.get("cache-control", ""))
            _read_sse_events(sse_resp)
        finally:
            cli_sock.close()
            sse_thread.join(timeout=2.0)

    # ------------------------------------------------------------------
    # Item 11: SSE 16-cap, monotonic state_version, stdio external_change
    # ------------------------------------------------------------------
    def test_r010_11_sse_cap_monotonic_versions_and_stdio_external_change(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        tok = read_auth_token(self.tmp_state)
        auth_hdr = {"Authorization": f"Bearer {tok}"}

        # 1. 16-connection cap -> 17th returns 503; closing releases slot
        open_conns: List[Tuple[socket.socket, http.client.HTTPResponse, threading.Thread]] = []
        try:
            for _ in range(SSE_MAX_SUBSCRIBERS):
                open_conns.append(_open_sse_socket(daemon, "/api/stream", auth_hdr))
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and daemon.event_hub.subscriber_count() < SSE_MAX_SUBSCRIBERS:
                time.sleep(0.01)
            self.assertEqual(daemon.event_hub.subscriber_count(), SSE_MAX_SUBSCRIBERS)

            st_17, _, body_17 = _raw_http_roundtrip_host(daemon, "GET", "/api/stream", auth_hdr)
            self.assertEqual(st_17, 503)
            self.assertIn("16", body_17.decode("utf-8"))
        finally:
            daemon.event_hub.close_all()
            for sock, resp, th in open_conns:
                try:
                    resp.close()
                except Exception:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
                th.join(timeout=2.0)

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and daemon.event_hub.subscriber_count() != 0:
            time.sleep(0.01)
        self.assertEqual(daemon.event_hub.subscriber_count(), 0)

        # 2. Monotonic state_version under concurrent publish_state_update threads
        cli_mono, resp_mono, th_mono = _open_sse_socket(
            daemon, "/api/stream?max_events=9", auth_hdr
        )
        try:
            first = _read_one_sse_frame(resp_mono)
            self.assertIsNotNone(first)
            versions = [first["id"]]

            def _pub(idx: int) -> None:
                daemon.publish_state_update(f"conc-{idx}")

            pub_threads = [threading.Thread(target=_pub, args=(i,)) for i in range(8)]
            for pt in pub_threads:
                pt.start()
            for pt in pub_threads:
                pt.join(timeout=2.0)

            while len(versions) < 9:
                nxt = _read_one_sse_frame(resp_mono)
                if nxt is None:
                    break
                versions.append(nxt["id"])
            self.assertEqual(len(versions), 9)
            self.assertTrue(all(versions[i] < versions[i + 1] for i in range(len(versions) - 1)))
        finally:
            cli_mono.close()
            th_mono.join(timeout=2.0)

        # 3. Silent stdio consilium_report_progress(notify_coordinator=False) triggers external_change SSE
        created = daemon.dispatcher.store.create_mission(
            "StdioExt",
            "External change detection",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
        )
        picked = daemon.dispatcher.store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)
        tid = picked["claimed_tasks"][0]["task_id"]
        aid = picked["claimed_tasks"][0]["attempt_id"]

        cli_ext, resp_ext, th_ext = _open_sse_socket(
            daemon, "/api/stream?max_events=2", auth_hdr
        )
        try:
            init_ev = _read_one_sse_frame(resp_ext)
            self.assertIsNotNone(init_ev)
            self.assertEqual(init_ev["event"], "state")

            # Wait past get_cached_dashboard_state max_age_sec=0.35 and run silent stdio mutation
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "consilium_report_progress",
                    "arguments": {
                        "task_id": tid,
                        "peer_id": "chatgpt",
                        "progress_note": "silent stdio update",
                        "notify_coordinator": False,
                        "attempt_id": aid,
                    },
                },
            }
            stdout_buf = io.StringIO()
            run_stdio_server(
                home_dir=self.tmp_state,
                rendezvous_dir=self.tmp_rv,
                stdin=io.StringIO(json.dumps(req) + "\n"),
                stdout=stdout_buf,
            )
            stdio_resp = json.loads(stdout_buf.getvalue().strip())
            self.assertFalse(stdio_resp["result"].get("isError"))

            ext_ev = _read_one_sse_frame(resp_ext)
            self.assertIsNotNone(ext_ev)
            self.assertEqual(ext_ev["event"], "update")
            self.assertEqual(ext_ev["data"]["reason"], "external_change")
            tasks_in_state = ext_ev["data"]["state"]["missions"][0]["tasks"]
            self.assertEqual(tasks_in_state[0]["progress_note"], "silent stdio update")
        finally:
            cli_ext.close()
            th_ext.join(timeout=2.0)

    # ------------------------------------------------------------------
    # Item 12: Atomic .lease (.tmp exclusion, flock before os.replace, JSON, cleanup)
    # ------------------------------------------------------------------
    def test_r010_12_atomic_lease_creation_flock_and_cleanup(self) -> None:
        wdir = os.path.join(self.tmp_rv, "waiters", "claude")
        os.makedirs(wdir, mode=0o700, exist_ok=True)
        tmp_junk = os.path.join(wdir, ".tmp-999-1-abcd")
        with open(tmp_junk, "w", encoding="utf-8") as handle:
            json.dump({"pid": 999, "session_label": "ghost", "expires_at": time.time() + 60.0}, handle)

        self.assertEqual(_count_live_leases(self.tmp_rv, "claude"), 0)
        self.assertTrue(os.path.exists(tmp_junk))

        fd, lease_path = _acquire_lease(
            self.tmp_rv, "claude", time.time() + 60.0, session_label="s-atomic"
        )
        try:
            self.assertTrue(lease_path.endswith(".lease"))
            self.assertTrue(os.path.exists(lease_path))
            self.assertEqual(os.stat(lease_path).st_mode & 0o777, 0o600)
            leftovers = [f for f in os.listdir(wdir) if f.startswith(".tmp-") and f != ".tmp-999-1-abcd"]
            self.assertEqual(leftovers, [])
            with open(lease_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertEqual(payload["pid"], os.getpid())
            self.assertEqual(payload["session_label"], "s-atomic")
            self.assertGreater(float(payload["expires_at"]), time.time())

            fd2 = os.open(lease_path, os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd2)

            self.assertEqual(_count_live_leases(self.tmp_rv, "claude"), 1)
        finally:
            _release_lease(fd, lease_path)

        self.assertFalse(os.path.exists(lease_path))
        self.assertEqual(_count_live_leases(self.tmp_rv, "claude"), 0)

        with mock.patch("consilium.wakeup.os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                _acquire_lease(self.tmp_rv, "claude", time.time() + 60.0, session_label="s-fail")
        self.assertEqual(os.listdir(wdir), [".tmp-999-1-abcd"])

    # ------------------------------------------------------------------
    # Item 13: Installer per-target, rollback verification, rotate-token mid-failure
    # ------------------------------------------------------------------
    def _write_claude_stub_bin(self) -> Tuple[str, str]:
        tmp_bin = self._make_tmp_dir("dbell-r10bin-")
        stub_path = os.path.join(tmp_bin, "claude")
        with open(stub_path, "w", encoding="utf-8") as handle:
            handle.write(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"mcp\" ] && [ \"$2\" = \"add-json\" ]; then\n"
                "  echo 'mock add-json failure' >&2\n"
                "  exit 1\n"
                "fi\n"
                "exit 0\n"
            )
        os.chmod(stub_path, 0o755)
        return tmp_bin, stub_path

    def test_r010_13_installer_targets_and_skip_claude_desktop_while_claude_running(self) -> None:
        _, stub_claude = self._write_claude_stub_bin()

        def _ok_runner(cmd, **kwargs):
            return SimpleNamespace(returncode=0, stdout="{}", stderr="")

        with mock.patch.object(install_mod, "_find_claude_cli", return_value=stub_claude):
            res1 = install_mod.run_installer(
                mode="apply",
                target_home=self.tmp_home,
                repo_dir=REPO_ROOT,
                port=18920,
                rendezvous_dir=self.tmp_rv,
                targets=["claude_code", "codex", "antigravity"],
                claude_running_check=lambda: True,
                claude_runner=_ok_runner,
            )
            self.assertIn("claude_desktop target is excluded", " ".join(res1["warnings"]))
            desktop_cfg = os.path.join(
                self.tmp_home, "Library", "Application Support", "Claude", "claude_desktop_config.json"
            )
            self.assertFalse(os.path.exists(desktop_cfg))
            self.assertTrue(os.path.exists(os.path.join(self.tmp_home, ".codex", "config.toml")))

            res2 = install_mod.run_installer(
                mode="apply",
                target_home=self.tmp_home,
                repo_dir=REPO_ROOT,
                port=18920,
                rendezvous_dir=self.tmp_rv,
                targets=None,
                skip_claude_desktop=True,
                claude_running_check=lambda: True,
                claude_runner=_ok_runner,
            )
            self.assertIn("claude_desktop target is excluded", " ".join(res2["warnings"]))
            self.assertFalse(os.path.exists(desktop_cfg))

            with self.assertRaisesRegex(RuntimeError, "Claude.app is currently running"):
                install_mod.run_installer(
                    mode="apply",
                    target_home=self.tmp_home,
                    repo_dir=REPO_ROOT,
                    port=18920,
                    rendezvous_dir=self.tmp_rv,
                    targets=["claude_desktop", "codex"],
                    claude_running_check=lambda: True,
                    claude_runner=_ok_runner,
                )

    def test_r010_13_claude_code_rollback_verification_failures(self) -> None:
        cc_path = os.path.join(self.tmp_home, ".claude.json")
        previous_entry = {"type": "http", "url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": "Bearer old"}}
        desired_entry = {"type": "http", "url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": "Bearer new"}}
        backup_root = self._make_tmp_dir("dbell-r10bk-")

        def _seed_cc() -> None:
            with open(cc_path, "w", encoding="utf-8") as handle:
                json.dump({"mcpServers": {"consilium": previous_entry}}, handle)

        # Case A: rollback add-json ok, claude mcp get consilium returns rc=1
        _seed_cc()

        def _runner_get_fails(cmd, **kwargs):
            sub = cmd[2] if len(cmd) > 2 else ""
            if sub == "remove":
                with open(cc_path, "w", encoding="utf-8") as h:
                    json.dump({"mcpServers": {}}, h)
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if sub == "add-json":
                payload = json.loads(cmd[-1])
                if payload == desired_entry:
                    return SimpleNamespace(returncode=1, stdout="", stderr="boom")
                with open(cc_path, "w", encoding="utf-8") as h:
                    json.dump({"mcpServers": {"consilium": payload}}, h)
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if sub == "get":
                # First probe ok, verification probe after rollback fails
                if not getattr(_runner_get_fails, "seen_probe", False):
                    _runner_get_fails.seen_probe = True
                    return SimpleNamespace(returncode=0, stdout="ok", stderr="")
                return SimpleNamespace(returncode=1, stdout="", stderr="missing")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "claude mcp get consilium") as cm_a:
            install_mod._apply_claude_code_mcp(
                cc_path,
                desired_entry,
                previous_entry,
                remove=False,
                home_dir=self.tmp_home,
                backup_root=backup_root,
                claude_bin="/usr/bin/true",
                runner=_runner_get_fails,
            )
        self.assertIn(backup_root, str(cm_a.exception))

        # Case B: rollback add-json returns 0, claude mcp get ok, but .claude.json mismatch
        _seed_cc()

        def _runner_json_mismatch(cmd, **kwargs):
            sub = cmd[2] if len(cmd) > 2 else ""
            if sub == "remove":
                with open(cc_path, "w", encoding="utf-8") as h:
                    json.dump({"mcpServers": {}}, h)
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if sub == "add-json":
                payload = json.loads(cmd[-1])
                if payload == desired_entry:
                    return SimpleNamespace(returncode=1, stdout="", stderr="boom")
                # Pretend rollback succeeded without writing previous_entry back to .claude.json
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            return SimpleNamespace(returncode=0, stdout="ok", stderr="")

        with self.assertRaisesRegex(RuntimeError, "does not match saved previous entry") as cm_b:
            install_mod._apply_claude_code_mcp(
                cc_path,
                desired_entry,
                previous_entry,
                remove=False,
                home_dir=self.tmp_home,
                backup_root=backup_root,
                claude_bin="/usr/bin/true",
                runner=_runner_json_mismatch,
            )
        self.assertIn(backup_root, str(cm_b.exception))

        # Case C: full verified rollback restores previous_entry
        _seed_cc()

        def _runner_verified_rb(cmd, **kwargs):
            sub = cmd[2] if len(cmd) > 2 else ""
            if sub == "remove":
                with open(cc_path, "w", encoding="utf-8") as h:
                    json.dump({"mcpServers": {}}, h)
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if sub == "add-json":
                payload = json.loads(cmd[-1])
                if payload == desired_entry:
                    return SimpleNamespace(returncode=1, stdout="", stderr="boom")
                with open(cc_path, "w", encoding="utf-8") as h:
                    json.dump({"mcpServers": {"consilium": payload}}, h)
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            return SimpleNamespace(returncode=0, stdout="ok", stderr="")

        with self.assertRaisesRegex(RuntimeError, "previous entry restored and verified"):
            install_mod._apply_claude_code_mcp(
                cc_path,
                desired_entry,
                previous_entry,
                remove=False,
                home_dir=self.tmp_home,
                backup_root=backup_root,
                claude_bin="/usr/bin/true",
                runner=_runner_verified_rb,
            )

    def test_r010_13_rotate_token_mid_failure_and_cli_exit_1(self) -> None:
        tmp_bin, _ = self._write_claude_stub_bin()
        os.environ["PATH"] = f"{tmp_bin}{os.pathsep}{os.environ.get('PATH', '')}"

        old_tok = "old-token-r010-123"
        write_auth_token(self.tmp_state, old_tok)
        ag_path = os.path.join(self.tmp_home, ".gemini", "config", "mcp_config.json")
        os.makedirs(os.path.dirname(ag_path), mode=0o700, exist_ok=True)
        with open(ag_path, "w", encoding="utf-8") as h:
            json.dump({"mcpServers": {"consilium": {"url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": f"Bearer {old_tok}"}}}}, h)

        cc_path = os.path.join(self.tmp_home, ".claude.json")
        with open(cc_path, "w", encoding="utf-8") as h:
            json.dump({"mcpServers": {"consilium": {"type": "http", "url": "http://127.0.0.1:18920/mcp", "headers": {"Authorization": f"Bearer {old_tok}"}}}}, h)

        cx_path = os.path.join(self.tmp_home, ".codex", "config.toml")
        os.makedirs(os.path.dirname(cx_path), mode=0o700, exist_ok=True)
        with open(cx_path, "w", encoding="utf-8") as h:
            h.write(install_mod._render_codex_block(18920, old_tok))

        def _failing_runner(cmd, **kwargs):
            sub = cmd[2] if len(cmd) > 2 else ""
            if sub == "add-json":
                return SimpleNamespace(returncode=1, stdout="", stderr="add-json failed")
            return SimpleNamespace(returncode=0, stdout="ok", stderr="")

        with self.assertRaises(install_mod.TokenRotationError) as cm:
            install_mod.rotate_token(
                target_home=self.tmp_home,
                port=18920,
                state_dir=self.tmp_state,
                claude_runner=_failing_runner,
            )
        rep = cm.exception.report
        self.assertFalse(rep["rotated"])
        self.assertFalse(rep["server_token_rotated"])
        self.assertEqual(rep["failed_target"], "claude_code_mcp")
        self.assertEqual(rep["updated_targets"], ["antigravity_mcp"])
        self.assertEqual(rep["remaining_targets"], ["claude_code_mcp", "codex_mcp", "server_auth_token"])
        self.assertTrue(os.path.isdir(rep["backup_root"]))
        self.assertTrue(rep["rollback_instructions"])
        self.assertEqual(read_auth_token(self.tmp_state), old_tok)
        with open(cx_path, "r", encoding="utf-8") as h:
            self.assertIn(old_tok, h.read())

        # CLI subprocess rotate-token -> exit code 1 + structured JSON on stderr
        proc = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "consilium.cli",
                "rotate-token",
                "--target-home",
                self.tmp_home,
                "--data-dir",
                self.tmp_state,
                "--port",
                "18920",
            ],
            env=self._clean_subproc_env(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 1)
        cli_rep = json.loads(proc.stderr)
        self.assertFalse(cli_rep["server_token_rotated"])
        self.assertEqual(cli_rep["failed_target"], "claude_code_mcp")
        self.assertIn("antigravity_mcp", cli_rep["updated_targets"])
        self.assertEqual(cli_rep["remaining_targets"], ["claude_code_mcp", "codex_mcp", "server_auth_token"])
        self.assertTrue(cli_rep["rollback_instructions"])

    # ------------------------------------------------------------------
    # Item 14: Requeue scope & multi-attempt attempt_history + attempt_seq
    # ------------------------------------------------------------------
    def test_r010_14_requeue_scope_guards_and_multi_attempt_history(self) -> None:
        disp = ConsiliumMCPDispatcher(home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = disp.store.create_mission(
            "RequeueScope",
            "Test requeueable statuses and attempt_history",
            "claude",
            tasks=[
                {"assignee_peer": "chatgpt", "prompt": "For claimed"},
                {"assignee_peer": "chatgpt", "prompt": "For running"},
                {"assignee_peer": "chatgpt", "prompt": "For failed"},
                {"assignee_peer": "chatgpt", "prompt": "For blocked"},
                {"assignee_peer": "antigravity", "prompt": "For done"},
                {"assignee_peer": "antigravity", "prompt": "For queued"},
            ],
        )
        mid = created["mission"]["mission_id"]
        t_claimed, t_running, t_failed, t_blocked = [t["task_id"] for t in created["tasks"][:4]]
        t_done, t_queued = created["tasks"][4]["task_id"], created["tasks"][5]["task_id"]

        # Queued cannot be requeued
        with self.assertRaises(ValueError):
            disp.store.requeue_task(t_queued, reason="nope", actor_peer="claude")

        # Claim and transition statuses
        p_cg = disp.store.pickup_inbox("chatgpt", session_label="s-cg", claim_tasks=True)
        cg_att = {t["task_id"]: t["attempt_id"] for t in p_cg["claimed_tasks"]}
        disp.store.report_progress(t_running, "chatgpt", "work", status="running", attempt_id=cg_att[t_running])
        disp.store.complete_task(t_failed, "chatgpt", "failed1", status="failed", attempt_id=cg_att[t_failed])
        disp.store.complete_task(t_blocked, "chatgpt", "blocked1", status="blocked", attempt_id=cg_att[t_blocked])

        # Complete t_done, leave t_queued queued
        p_ag = disp.store.pickup_inbox("antigravity", session_label="s-ag", claim_tasks=True)
        ag_att = {t["task_id"]: t["attempt_id"] for t in p_ag["claimed_tasks"]}
        disp.store.complete_task(t_done, "antigravity", "all done", status="done", attempt_id=ag_att[t_done])
        with self.assertRaises(ValueError):
            disp.store.requeue_task(t_done, reason="nope", actor_peer="claude")

        # Non-coordinator rejected by MCP
        err_nc, _ = self._mcp_call(
            disp,
            "consilium_requeue_task",
            {"task_id": t_claimed, "peer_id": "chatgpt", "reason": "non-coord"},
        )
        self.assertTrue(err_nc)

        # Requeue all 4 allowed statuses: claimed, running, failed, blocked
        for tid in (t_claimed, t_running, t_failed, t_blocked):
            err_rq, rq = self._mcp_call(
                disp,
                "consilium_requeue_task",
                {"task_id": tid, "peer_id": "claude", "reason": f"reset-{tid}"},
            )
            self.assertFalse(err_rq)
            tsk = rq["task"]
            self.assertEqual(tsk["status"], "queued")
            self.assertEqual(tsk["attempt_id"], "")
            self.assertEqual(tsk["claimed_by_session"], "")
            self.assertEqual(tsk["claimed_at"], "")
            self.assertEqual(tsk["claimed_at_ms"], 0.0)
            self.assertIsNone(tsk["claim_latency_ms"])
            self.assertEqual(tsk["stale_at"], "")
            self.assertEqual(tsk["progress_note"], "")
            self.assertEqual(tsk["result_summary"], "")
            self.assertEqual(tsk["artifacts"], [])
            self.assertGreater(tsk["dispatched_at_ms"], 0)
            self.assertEqual(rq["event"]["kind"], "task_requeued")

        # Cancelled status guard + closed mission guard
        m2 = disp.store.create_mission(
            "ClosedMissionGuard",
            "Close blocks requeue",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Cancelled/Closed"}],
        )
        m2_id = m2["mission"]["mission_id"]
        t_c = m2["tasks"][0]["task_id"]
        with sqlite3.connect(os.path.join(self.tmp_state, "bus.db")) as conn:
            conn.execute("UPDATE tasks SET status = 'cancelled' WHERE task_id = ?", (t_c,))
            conn.commit()
        with self.assertRaises(ValueError):
            disp.store.requeue_task(t_c, reason="cancelled", actor_peer="claude")
        disp.store.close_mission(m2_id, "close", peer_id="claude", force=True)
        with self.assertRaisesRegex(ValueError, "closed mission"):
            disp.store.requeue_task(t_c, reason="closed", actor_peer="claude")

        # Close m1 so its requeued chatgpt tasks don't mix into m3
        disp.store.close_mission(mid, "close m1", peer_id="claude", force=True)

        # Multi-attempt attempt_history + attempt_seq on isolated mission
        m3 = disp.store.create_mission(
            "MultiAttempt",
            "Track 3 attempts",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Iterate"}],
        )
        m3_id = m3["mission"]["mission_id"]
        t3 = m3["tasks"][0]["task_id"]

        p1 = disp.store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)
        a1 = p1["claimed_tasks"][0]["attempt_id"]
        disp.store.report_progress(t3, "chatgpt", "p1", attempt_id=a1)
        disp.store.complete_task(t3, "chatgpt", "fail1", artifacts=["f1.txt"], status="failed", attempt_id=a1)
        disp.store.requeue_task(t3, reason="r1", actor_peer="claude")

        p2 = disp.store.pickup_inbox("chatgpt", session_label="s2", claim_tasks=True)
        a2 = p2["claimed_tasks"][0]["attempt_id"]
        disp.store.report_progress(t3, "chatgpt", "p2", attempt_id=a2)
        disp.store.requeue_task(t3, reason="r2", actor_peer="claude")

        p3 = disp.store.pickup_inbox("chatgpt", session_label="s3", claim_tasks=True)
        a3 = p3["claimed_tasks"][0]["attempt_id"]
        disp.store.complete_task(t3, "chatgpt", "ok3", artifacts=["ok.txt"], status="done", attempt_id=a3)

        final_t = disp.store.get_mission_snapshot(m3_id)["tasks"][0]
        self.assertEqual(final_t["attempt_seq"], 3)
        self.assertEqual(final_t["attempt_id"], a3)
        self.assertEqual(final_t["status"], "done")
        self.assertEqual(len(final_t["attempt_history"]), 2)
        self.assertEqual(final_t["attempt_history"][0]["attempt_seq"], 1)
        self.assertEqual(final_t["attempt_history"][0]["status"], "failed")
        self.assertEqual(final_t["attempt_history"][0]["progress_note"], "p1")
        self.assertEqual(final_t["attempt_history"][0]["result_summary"], "fail1")
        self.assertEqual(final_t["attempt_history"][0]["artifacts"], ["f1.txt"])
        self.assertEqual(final_t["attempt_history"][0]["reason"], "r1")
        self.assertEqual(final_t["attempt_history"][1]["attempt_seq"], 2)
        self.assertEqual(final_t["attempt_history"][1]["status"], "running")
        self.assertEqual(final_t["attempt_history"][1]["progress_note"], "p2")
        self.assertEqual(final_t["attempt_history"][1]["reason"], "r2")

    # ------------------------------------------------------------------
    # Item 15: claim_latency_ms, 3 cold-notification facts, consilium ui error
    # ------------------------------------------------------------------
    def test_r010_15_claim_latency_cold_facts_and_consilium_ui_errors(self) -> None:
        store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        created = store.create_mission(
            "LatencyFacts",
            "Verify claim_latency_ms and 3 cold facts",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Work"}],
        )
        mid = created["mission"]["mission_id"]
        eid1 = int(created["events"][0]["event_id"])

        d_dry = ring_peer(
            store=store,
            home_dir=self.tmp_rv,
            peer_id="chatgpt",
            title="T",
            body="B",
            pickup_prompt="P",
            dry_run=True,
            event_ids=[eid1],
        )
        self.assertTrue(d_dry["attempted"])
        self.assertEqual(d_dry["command_status"], "dry_run")
        self.assertFalse(d_dry["command_ok"])
        self.assertIsNone(d_dry["command_returncode"])
        self.assertFalse(d_dry["peer_acked"])
        store.record_event_delivery([eid1], d_dry, ring_sent_at_ms=time.time() * 1000.0)
        ev_dry = store.get_mission_snapshot(mid)["events"][0]["payload"]
        self.assertTrue(ev_dry["delivery_attempted"])
        self.assertEqual(ev_dry["command_status"], "dry_run")
        self.assertFalse(ev_dry["command_ok"])
        self.assertFalse(ev_dry["peer_acked"])

        # Temporarily toggle CONSILIUM_DRY_RUN=0 with mocked subprocess.run
        prev_dry = os.environ.get("CONSILIUM_DRY_RUN")
        try:
            os.environ["CONSILIUM_DRY_RUN"] = "0"
            with mock.patch(
                "consilium.wakeup.subprocess.run",
                return_value=SimpleNamespace(returncode=1, stdout="", stderr="err"),
            ):
                d_fail = ColdDoorbell.ring_cold(
                    "chatgpt", "T", "B", "P", dry_run=False
                )
            self.assertTrue(d_fail["attempted"])
            self.assertEqual(d_fail["command_status"], "failed")
            self.assertFalse(d_fail["command_ok"])
            self.assertEqual(d_fail["command_returncode"], 1)
            self.assertFalse(d_fail["peer_acked"])

            with mock.patch(
                "consilium.wakeup.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="", stderr=""),
            ):
                d_ok = ColdDoorbell.ring_cold(
                    "chatgpt", "T", "B", "P", dry_run=False
                )
            self.assertTrue(d_ok["attempted"])
            self.assertEqual(d_ok["command_status"], "ok")
            self.assertTrue(d_ok["command_ok"])
            self.assertEqual(d_ok["command_returncode"], 0)
            self.assertFalse(d_ok["peer_acked"])
            store.record_event_delivery([eid1], d_ok, ring_sent_at_ms=time.time() * 1000.0)
        finally:
            if prev_dry is None:
                os.environ.pop("CONSILIUM_DRY_RUN", None)
            else:
                os.environ["CONSILIUM_DRY_RUN"] = prev_dry

        time.sleep(0.02)
        picked = store.pickup_inbox("chatgpt", session_label="s1", claim_tasks=True)
        claimed = picked["claimed_tasks"][0]
        self.assertIsNotNone(claimed["claim_latency_ms"])
        self.assertGreaterEqual(claimed["claim_latency_ms"], 10.0)
        self.assertGreater(claimed["claimed_at_ms"], claimed["dispatched_at_ms"])

        ev_after = store.get_mission_snapshot(mid)["events"][0]
        self.assertTrue(ev_after["acked_at"])
        self.assertTrue(ev_after["payload"]["peer_acked"])
        self.assertEqual(ev_after["payload"]["claim_latency_ms"], claimed["claim_latency_ms"])
        self.assertEqual(ev_after["payload"]["claimed_by_attempt_id"], claimed["attempt_id"])

        # consilium ui failure modes
        # 1. Unreachable daemon (URLError)
        unused_sock = socket.socket()
        unused_sock.bind(("127.0.0.1", 0))
        unused_port = unused_sock.getsockname()[1]
        unused_sock.close()
        stderr_u = io.StringIO()
        with contextlib.redirect_stderr(stderr_u):
            rc_u = consilium_cli.main(
                ["ui", "--host", "127.0.0.1", "--port", str(unused_port), "--data-dir", self.tmp_state]
            )
        self.assertEqual(rc_u, 1)
        self.assertIn("Cannot reach consilium daemon", stderr_u.getvalue())

        # Start ephemeral live daemon for 401 and `open` failure tests
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=0,
        )
        _, live_port = daemon.start()
        self.addCleanup(daemon.stop)
        good_tok = read_auth_token(self.tmp_state)

        # 2. 401 wrong token
        bad_state = self._make_tmp_dir("dbell-r10badst-")
        write_auth_token(bad_state, "wrong-token")
        stderr_401 = io.StringIO()
        with contextlib.redirect_stderr(stderr_401):
            rc_401 = consilium_cli.main(
                ["ui", "--host", "127.0.0.1", "--port", str(live_port), "--data-dir", bad_state]
            )
        self.assertEqual(rc_401, 1)
        self.assertIn("401", stderr_401.getvalue())

        # 3. `open` returns non-zero & raises OSError
        self.assertEqual(read_auth_token(self.tmp_state), good_tok)
        out_nz = io.StringIO()
        err_nz = io.StringIO()
        with mock.patch(
            "consilium.cli.subprocess.run",
            return_value=subprocess.CompletedProcess(["open"], 1, "", "LSOpenURLsWithRole() failed"),
        ), contextlib.redirect_stdout(out_nz), contextlib.redirect_stderr(err_nz):
            rc_nz = consilium_cli.main(
                ["ui", "--host", "127.0.0.1", "--port", str(live_port), "--data-dir", self.tmp_state]
            )
        self.assertEqual(rc_nz, 1)
        self.assertIn("LSOpenURLsWithRole() failed", err_nz.getvalue())
        self.assertIn("/ui/auth?ticket=", out_nz.getvalue())

        out_os = io.StringIO()
        err_os = io.StringIO()
        with mock.patch(
            "consilium.cli.subprocess.run",
            side_effect=OSError("No such file"),
        ), contextlib.redirect_stdout(out_os), contextlib.redirect_stderr(err_os):
            rc_os = consilium_cli.main(
                ["ui", "--host", "127.0.0.1", "--port", str(live_port), "--data-dir", self.tmp_state]
            )
        self.assertEqual(rc_os, 1)
        self.assertIn("No such file", err_os.getvalue())
        self.assertIn("/ui/auth?ticket=", out_os.getvalue())

    # ------------------------------------------------------------------
    # Item 16: 5-state peer indicators & dashboard UI rules
    # ------------------------------------------------------------------
    def test_r010_16_five_state_peer_indicators_and_dashboard_ui_rules(self) -> None:
        daemon = ConsiliumDaemon(
            home_dir=self.tmp_state,
            rendezvous_dir=self.tmp_rv,
            host="127.0.0.1",
            port=18920,
        )
        self.addCleanup(daemon.stop)
        store = daemon.dispatcher.store
        tok = read_auth_token(self.tmp_state)

        # Configure all 5 states in one snapshot:
        # 1) standby: claude has live waiter lease
        fd_standby, lease_standby = _acquire_lease(
            self.tmp_rv, "claude", time.time() + 60.0, session_label="waiter-1"
        )
        self.addCleanup(_release_lease, fd_standby, lease_standby)

        # 2) working: chatgpt has active running task & fresh last_seen_at
        created = store.create_mission(
            "FiveStates",
            "Verify 5 peer lights",
            "claude",
            tasks=[{"assignee_peer": "chatgpt", "prompt": "Working task"}],
        )
        picked = store.pickup_inbox("chatgpt", session_label="cg-1", claim_tasks=True)
        store.report_progress(
            picked["claimed_tasks"][0]["task_id"],
            "chatgpt",
            "active",
            status="running",
            attempt_id=picked["claimed_tasks"][0]["attempt_id"],
        )

        # 3) online: antigravity has fresh last_seen_at and 0 claimed/running tasks
        store.pickup_inbox("antigravity", session_label="ag-1", claim_tasks=False)

        peers_map = {p["peer_id"]: p for p in store.list_peers(stale_seconds=600)}
        self.assertEqual(peers_map["claude"]["status"], "standby")
        self.assertEqual(peers_map["chatgpt"]["status"], "working")
        self.assertEqual(peers_map["antigravity"]["status"], "online")

        # 4) offline: idle peer with expired last_seen_at (NOT stale!) & never-seen peer
        with sqlite3.connect(os.path.join(self.tmp_state, "bus.db")) as conn:
            conn.execute(
                "UPDATE peers SET last_seen_at = '2020-01-01T00:00:00Z' WHERE peer_id = 'antigravity'"
            )
            conn.commit()
        peers_idle_expired = {p["peer_id"]: p for p in store.list_peers(stale_seconds=600)}
        self.assertEqual(peers_idle_expired["antigravity"]["status"], "offline")
        self.assertTrue(peers_idle_expired["antigravity"]["stale"])

        # 5) stale: active claimed/running task with expired last_seen_at / stale_at
        with sqlite3.connect(os.path.join(self.tmp_state, "bus.db")) as conn:
            conn.execute(
                "UPDATE peers SET last_seen_at = '2020-01-01T00:00:00Z' WHERE peer_id = 'chatgpt'"
            )
            conn.commit()

        st_api, _, body_api = _raw_http_roundtrip_host(
            daemon, "GET", "/api/state", {"Authorization": f"Bearer {tok}"}
        )
        self.assertEqual(st_api, 200)
        api_peers = {p["peer_id"]: p for p in json.loads(body_api.decode("utf-8"))["peers"]}
        self.assertEqual(api_peers["claude"]["status"], "standby")
        self.assertEqual(api_peers["chatgpt"]["status"], "stale")
        self.assertEqual(api_peers["antigravity"]["status"], "offline")

        # Dashboard HTML/JS R008 & R009 UI contract assertions
        html = render_dashboard_html("0.1.0")
        for needle in (
            'standby: "待命"',
            'working: "工作中"',
            'online: "在线"',
            'offline: "离线"',
            'stale: "陈旧"',
            "Claude.app · Code",
            "ChatGPT.app · Codex",
            "Antigravity.app",
            "分支前缀：",
            'w.source === "demo"',
            "演示数据",
            'const barCls = !hasQuota ? "unknown" : (pct <= 0 ? "empty" : (pct < 20 ? "low" : ""));',
            "doneOnlyCnt === totalCnt",
            "var(--status-standby)",
            "btn-subtle-danger",
            'class="btn btn-danger" id="confirm-ok"',
            "peer-pills-bar",
            "chat-container",
            "right-drawer closed",
            "renderInline(noteClip)",
            "const LIMIT = 6;",
            "timeline-fade",
            "p.question || p.summary || p.reason",
        ):
            self.assertIn(needle, html)


if __name__ == "__main__":
    unittest.main()
