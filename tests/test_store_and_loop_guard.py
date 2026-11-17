"""Tests for BusStore SQLite WAL transactions, atomic claiming, and loop guard."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import shutil
import tempfile
import unittest

from deskbell.store import BusStore


class StoreAndLoopGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_home = tempfile.mkdtemp(prefix="dbell-store-", dir="/tmp")
        self.store = BusStore(self.tmp_home)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_home, ignore_errors=True)

    def test_atomic_concurrent_pickup_prevents_double_claim(self) -> None:
        created = self.store.create_mission(
            title="Audit bus",
            goal="Verify single claim under concurrency",
            coordinator_peer="antigravity",
            workspace_path="/tmp/repo",
            isolation_policy="worktree",
            max_rounds=2,
            tasks=[
                {
                    "assignee_peer": "claude",
                    "prompt": "Audit store.py",
                    "model_tier": "pro_reasoning",
                }
            ],
        )
        task_id = created["tasks"][0]["task_id"]

        def _claim(label: str) -> dict:
            return self.store.pickup_inbox(
                peer_id="claude", session_label=label, claim_tasks=True
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(_claim, "claude-sess-1")
            f2 = pool.submit(_claim, "claude-sess-2")
            r1 = f1.result(timeout=5.0)
            r2 = f2.result(timeout=5.0)

        claimed_all = r1["claimed_tasks"] + r2["claimed_tasks"]
        self.assertEqual(len(claimed_all), 1)
        self.assertEqual(claimed_all[0]["task_id"], task_id)
        self.assertEqual(claimed_all[0]["status"], "claimed")
        self.assertIn(claimed_all[0]["claimed_by_session"], ("claude-sess-1", "claude-sess-2"))

    def test_mission_lifecycle_and_all_tasks_finished_flag(self) -> None:
        created = self.store.create_mission(
            title="Build feature",
            goal="Dual-peer execution",
            coordinator_peer="antigravity",
            tasks=[
                {"assignee_peer": "claude", "prompt": "Design review", "model_tier": "pro_reasoning"},
                {"assignee_peer": "chatgpt", "prompt": "Write tests", "model_tier": "balanced"},
            ],
        )
        mission_id = created["mission"]["mission_id"]
        t1, t2 = created["tasks"]

        done1 = self.store.complete_task(
            task_id=t1["task_id"],
            peer_id="claude",
            result_summary="Design looks good",
            artifacts=["reviews/design.md"],
        )
        self.assertFalse(done1["all_tasks_finished"])
        self.assertEqual(done1["coordinator_peer"], "antigravity")

        done2 = self.store.complete_task(
            task_id=t2["task_id"],
            peer_id="chatgpt",
            result_summary="Tests added",
            artifacts=["tests/test_x.py"],
        )
        self.assertTrue(done2["all_tasks_finished"])

        snap = self.store.get_mission_snapshot(mission_id)
        self.assertEqual(snap["mission"]["status"], "ready_to_report")

        closed = self.store.close_mission(mission_id, "Both tasks completed cleanly.")
        self.assertEqual(closed["status"], "completed")
        self.assertEqual(closed["final_summary"], "Both tasks completed cleanly.")

    def test_loop_guard_blocks_after_max_rounds(self) -> None:
        created = self.store.create_mission(
            title="Cross review",
            goal="Bounded review rounds",
            coordinator_peer="antigravity",
            max_rounds=2,
            tasks=[{"assignee_peer": "claude", "prompt": "Review patch"}],
        )
        mission_id = created["mission"]["mission_id"]

        r1 = self.store.request_review(
            mission_id=mission_id,
            from_peer="claude",
            to_peer="chatgpt",
            summary="Draft 1",
            question="Check race condition?",
        )
        self.assertTrue(r1["allowed"])
        self.assertEqual(r1["round"], 1)

        r2 = self.store.request_review(
            mission_id=mission_id,
            from_peer="chatgpt",
            to_peer="claude",
            summary="Reply 1",
            question="Verify updated lock?",
        )
        self.assertTrue(r2["allowed"])
        self.assertEqual(r2["round"], 2)

        r3 = self.store.request_review(
            mission_id=mission_id,
            from_peer="claude",
            to_peer="chatgpt",
            summary="One more check",
            question="Should be blocked",
        )
        self.assertFalse(r3["allowed"])
        self.assertEqual(r3["round"], 2)
        self.assertEqual(r3["max_rounds"], 2)
        self.assertEqual(r3["event"]["kind"], "loop_guard_reached")
        self.assertEqual(r3["event"]["to_peer"], "antigravity")


if __name__ == "__main__":
    unittest.main()
