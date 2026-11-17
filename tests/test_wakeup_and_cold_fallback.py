"""Tests for sandbox-native kqueue VNODE hot wakeup, lease pruning, and cold fallback."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
import unittest

from deskbell.store import BusStore
from deskbell.wakeup import (
    ColdDoorbell,
    query_active_waiters,
    resolve_socket_path,
    ring_peer,
    wait_for_doorbell,
)


class WakeupAndColdFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_home = tempfile.mkdtemp(prefix="dbell-wake-", dir="/tmp")
        self.store = BusStore(self.tmp_home)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def test_long_home_path_shortens_unix_socket_below_104_bytes(self) -> None:
        long_home = "/tmp/" + ("nested_directory_segment_" * 5) + "deskbell"
        sock_path = resolve_socket_path(long_home)
        self.assertLessEqual(len(sock_path.encode("utf-8")), 96)
        self.assertTrue(sock_path.startswith("/tmp/deskbell-"))

    def test_hot_wakeup_via_kqueue_and_lease_cleanup(self) -> None:
        wait_result = {}

        def _waiter() -> None:
            wait_result["res"] = wait_for_doorbell(
                store=self.store,
                home_dir=self.tmp_home,
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
            if query_active_waiters(self.tmp_home).get("claude", 0) == 1:
                break
            time.sleep(0.02)
        self.assertEqual(query_active_waiters(self.tmp_home).get("claude"), 1)

        time.sleep(0.06)
        self.store.create_mission(
            title="Hot kqueue test",
            goal="Wake standby peer",
            coordinator_peer="antigravity",
            tasks=[{"assignee_peer": "claude", "prompt": "Wake up"}],
        )
        delivery = ring_peer(
            store=self.store,
            home_dir=self.tmp_home,
            peer_id="claude",
            title="Hot kqueue test",
            body="Wake up",
            pickup_prompt="Check inbox",
            dry_run=True,
        )
        t.join(timeout=2.0)
        elapsed = time.monotonic() - started

        self.assertFalse(t.is_alive())
        self.assertLess(elapsed, 0.8)
        self.assertEqual(delivery["delivery"], "hot_wakeup")
        self.assertEqual(delivery["woken_count"], 1)
        self.assertTrue(wait_result["res"]["woke"])
        self.assertEqual(wait_result["res"]["source"], "kqueue")
        self.assertEqual(query_active_waiters(self.tmp_home).get("claude"), 0)

    def test_dead_and_expired_waiter_leases_are_pruned(self) -> None:
        waiter_dir = os.path.join(self.tmp_home, "waiters", "chatgpt")
        os.makedirs(waiter_dir, mode=0o700, exist_ok=True)
        dead_lease = os.path.join(waiter_dir, "99999999-1.json")
        expired_lease = os.path.join(waiter_dir, f"{os.getpid()}-expired.json")
        with open(dead_lease, "w", encoding="utf-8") as handle:
            json.dump({"pid": 99999999, "expires_at": time.time() + 100.0}, handle)
        with open(expired_lease, "w", encoding="utf-8") as handle:
            json.dump({"pid": os.getpid(), "expires_at": time.time() - 10.0}, handle)

        counts = query_active_waiters(self.tmp_home)
        self.assertEqual(counts["chatgpt"], 0)
        self.assertFalse(os.path.exists(dead_lease))
        self.assertFalse(os.path.exists(expired_lease))

    def test_immediate_return_when_event_exists_before_wait(self) -> None:
        self.store.create_mission(
            title="Queued first",
            goal="No missed wakeup",
            coordinator_peer="chatgpt",
            tasks=[{"assignee_peer": "antigravity", "prompt": "Pick up"}],
        )
        res = wait_for_doorbell(
            store=self.store,
            home_dir=self.tmp_home,
            peer_id="antigravity",
            timeout_sec=3.0,
            auto_pickup=True,
        )
        self.assertTrue(res["woke"])
        self.assertEqual(res["source"], "immediate")
        self.assertEqual(len(res["inbox"]["claimed_tasks"]), 1)

    def test_cold_fallback_deeplink_encoding_and_dry_run(self) -> None:
        delivery = ring_peer(
            store=self.store,
            home_dir=self.tmp_home,
            peer_id="claude",
            title='Check "quotes" & newlines\nhere',
            body="Task T-123 ready",
            pickup_prompt='Use deskbell_pickup_inbox("claude") for "T-123"',
            workspace_path="/Users/up/code/github/deskbell",
            open_deeplink=True,
            dry_run=True,
        )
        self.assertEqual(delivery["delivery"], "cold_notification")
        self.assertTrue(delivery["dry_run"])
        self.assertFalse(delivery["notified"])
        self.assertFalse(delivery["opened"])
        self.assertTrue(delivery["deeplink"].startswith("claude://code/new?q="))
        self.assertIn("&folder=%2FUsers%2Fup%2Fcode%2Fgithub%2Fdeskbell", delivery["deeplink"])

        codex_url = ColdDoorbell.build_deeplink(
            "chatgpt", "Claim task T-999 from deskbell"
        )
        self.assertTrue(codex_url.startswith("codex://new?q=Claim%20task%20T-999"))
        self.assertLessEqual(len(codex_url), 295)


if __name__ == "__main__":
    unittest.main()
