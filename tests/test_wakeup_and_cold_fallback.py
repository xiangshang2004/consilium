"""Tests for DB-free kqueue VNODE wakeup, flock leases, escalation, and deep links."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.parse

from consilium.store import BusStore
from consilium.wakeup import (
    ColdDoorbell,
    _acquire_lease,
    _release_lease,
    query_active_waiters,
    resolve_socket_path,
    ring_peer,
    wait_for_doorbell,
)


class WakeupAndColdFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="dbell-wstate-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="dbell-wrv-", dir="/tmp")
        self.store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)

    def test_long_home_path_shortens_unix_socket_inside_private_subdir(self) -> None:
        long_rv = "/tmp/" + ("nested_directory_segment_" * 5) + "consilium"
        try:
            sock_path = resolve_socket_path(long_rv)
            self.assertLessEqual(len(sock_path.encode("utf-8")), 96)
            self.assertTrue(sock_path.startswith(f"/tmp/consilium-{os.getuid()}/sock/"))
        finally:
            shutil.rmtree("/tmp/nested_directory_segment_nested_directory_segment_nested_directory_segment_nested_directory_segment_nested_directory_segment_consilium", ignore_errors=True)

    def test_db_free_kqueue_hot_wakeup_and_lease_cleanup(self) -> None:
        wait_result = {}

        def _waiter() -> None:
            # store=None verifies standby waiter operates purely on signals/<peer>.seq + flock!
            wait_result["res"] = wait_for_doorbell(
                store=None,
                rendezvous_dir=self.tmp_rv,
                peer_id="claude",
                timeout_sec=5.0,
                session_label="claude-standby",
                auto_pickup=False,
            )

        t = threading.Thread(target=_waiter, daemon=True)
        started = time.monotonic()
        t.start()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if query_active_waiters(self.tmp_rv).get("claude", 0) == 1:
                break
            time.sleep(0.02)
        self.assertEqual(query_active_waiters(self.tmp_rv).get("claude"), 1)

        time.sleep(0.06)
        created = self.store.create_mission(
            title="Hot kqueue test",
            goal="Wake standby peer",
            coordinator_peer="antigravity",
            tasks=[{"assignee_peer": "claude", "prompt": "Wake up"}],
        )
        delivery = ring_peer(
            store=self.store,
            home_dir=self.tmp_rv,
            peer_id="claude",
            title="Hot kqueue test",
            body="Wake up",
            pickup_prompt="Check inbox",
            dry_run=True,
            event_ids=[created["events"][0]["event_id"]],
            escalation_delay_sec=0.0,
        )
        t.join(timeout=2.0)
        elapsed = time.monotonic() - started

        self.assertFalse(t.is_alive())
        self.assertLess(elapsed, 0.8)
        self.assertEqual(delivery["delivery"], "hot_wakeup")
        self.assertEqual(delivery["woken_count"], 1)
        self.assertTrue(wait_result["res"]["woke"])
        self.assertEqual(wait_result["res"]["source"], "kqueue")
        self.assertEqual(query_active_waiters(self.tmp_rv).get("claude"), 0)

    def test_flock_prunes_zombie_lease_even_with_live_pid(self) -> None:
        waiter_dir = os.path.join(self.tmp_rv, "waiters", "chatgpt")
        os.makedirs(waiter_dir, mode=0o700, exist_ok=True)

        # Simulate PID reuse: file claims current live PID and future expiry, but holds NO flock!
        zombie_lease = os.path.join(waiter_dir, f"{os.getpid()}-zombie.lease")
        with open(zombie_lease, "w", encoding="utf-8") as handle:
            json.dump({"pid": os.getpid(), "expires_at": time.time() + 3600.0}, handle)

        # Also hold one genuine locked lease.
        fd, real_lease = _acquire_lease(
            self.tmp_rv, "chatgpt", expires_at=time.time() + 3600.0, session_label="real"
        )
        try:
            counts = query_active_waiters(self.tmp_rv)
            self.assertEqual(counts["chatgpt"], 1)
            self.assertFalse(os.path.exists(zombie_lease))
            self.assertTrue(os.path.exists(real_lease))
        finally:
            _release_lease(fd, real_lease)
        self.assertEqual(query_active_waiters(self.tmp_rv)["chatgpt"], 0)

    def test_unacked_hot_wakeup_escalates_to_cold_doorbell(self) -> None:
        fd, real_lease = _acquire_lease(
            self.tmp_rv, "claude", expires_at=time.time() + 10.0, session_label="c1"
        )
        escalations = []
        try:
            created = self.store.create_mission(
                title="Escalation test",
                goal="Escalate if unacked",
                coordinator_peer="antigravity",
                tasks=[{"assignee_peer": "claude", "prompt": "Respond quickly"}],
            )
            delivery = ring_peer(
                store=self.store,
                home_dir=self.tmp_rv,
                peer_id="claude",
                title="Escalation test",
                body="Task waiting",
                pickup_prompt="Check consilium inbox",
                dry_run=True,
                event_ids=[created["events"][0]["event_id"]],
                escalation_delay_sec=0.1,
                on_escalate=escalations.append,
            )
            self.assertEqual(delivery["delivery"], "hot_wakeup")
            self.assertTrue(delivery["escalation_scheduled"])

            # Wait past 0.1s without acking -> escalation should fire once.
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline and not escalations:
                time.sleep(0.02)
            self.assertEqual(len(escalations), 1)
            self.assertEqual(escalations[0]["delivery"], "cold_notification")
        finally:
            _release_lease(fd, real_lease)

    def test_immediate_return_when_event_exists_before_wait(self) -> None:
        self.store.create_mission(
            title="Queued first",
            goal="No missed wakeup",
            coordinator_peer="chatgpt",
            tasks=[{"assignee_peer": "antigravity", "prompt": "Pick up"}],
        )
        res = wait_for_doorbell(
            store=self.store,
            rendezvous_dir=self.tmp_rv,
            peer_id="antigravity",
            timeout_sec=3.0,
            auto_pickup=True,
        )
        self.assertTrue(res["woke"])
        self.assertEqual(res["source"], "immediate")
        self.assertEqual(len(res["inbox"]["claimed_tasks"]), 1)

    def test_deeplink_utf8_pre_encode_truncation_and_codex_prompt_param(self) -> None:
        chinese_prompt = "/请立即审查中文长提示词" * 30
        claude_url = ColdDoorbell.build_deeplink(
            "claude",
            chinese_prompt,
            workspace_path="/Users/up/code/github/consilium",
            max_url_len=295,
        )
        self.assertLessEqual(len(claude_url), 295)
        self.assertTrue(claude_url.startswith("claude://code/new?q="))
        self.assertIn("&folder=%2FUsers%2Fup%2Fcode%2Fgithub%2Fconsilium", claude_url)
        parsed = urllib.parse.urlparse(claude_url)
        params = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        # Leading '/' must be stripped, and UTF-8 must decode cleanly without split %XX!
        self.assertFalse(params["q"][0].startswith("/"))
        self.assertEqual(params["folder"][0], "/Users/up/code/github/consilium")

        codex_url = ColdDoorbell.build_deeplink(
            "chatgpt",
            "/Claim task T-999 from consilium",
            workspace_path="/Users/up/code/github/consilium",
        )
        self.assertTrue(codex_url.startswith("codex://new?prompt="))
        self.assertNotIn("&folder=", codex_url)
        self.assertIn("&path=%2FUsers%2Fup%2Fcode%2Fgithub%2Fconsilium", codex_url)
        codex_params = urllib.parse.parse_qs(urllib.parse.urlparse(codex_url).query)
        self.assertEqual(codex_params["path"][0], "/Users/up/code/github/consilium")
        self.assertNotIn("[cwd:", codex_params["prompt"][0])
        self.assertFalse(codex_params["prompt"][0].startswith("/"))

        claude_no_folder = ColdDoorbell.build_deeplink("claude", "Check inbox")
        self.assertTrue(claude_no_folder.startswith("claude://code/new?q="))
        self.assertNotIn("&folder=", claude_no_folder)

    def test_cold_fallback_dry_run(self) -> None:
        delivery = ring_peer(
            store=self.store,
            home_dir=self.tmp_rv,
            peer_id="claude",
            title='Check "quotes" & newlines\nhere',
            body="Task T-123 ready",
            pickup_prompt='Use consilium_pickup_inbox("claude") for "T-123"',
            workspace_path="/Users/up/code/github/consilium",
            open_deeplink=True,
            dry_run=True,
        )
        self.assertEqual(delivery["delivery"], "cold_notification")
        self.assertTrue(delivery["dry_run"])
        self.assertFalse(delivery["notified"])
        self.assertFalse(delivery["opened"])


if __name__ == "__main__":
    unittest.main()
