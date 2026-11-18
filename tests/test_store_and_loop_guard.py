"""Tests for BusStore SQLite WAL transactions, state-machine guards, and loop guard."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import shutil
import sqlite3
import tempfile
import unittest

from deskbell.store import BusStore


class StoreAndLoopGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="dbell-state-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="dbell-rv-", dir="/tmp")
        self.store = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)

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
        self.assertNotEqual(claimed_all[0]["claimed_at"], "")
        self.assertIn(claimed_all[0]["claimed_by_session"], ("claude-sess-1", "claude-sess-2"))

    def test_mission_lifecycle_re_dispatch_and_close_guards(self) -> None:
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

        # Non-assignee cannot update progress or complete task.
        with self.assertRaises(ValueError):
            self.store.report_progress(t1["task_id"], "chatgpt", "wrong peer")
        with self.assertRaises(ValueError):
            self.store.complete_task(t1["task_id"], "chatgpt", "wrong peer")
        # report_progress cannot jump straight to terminal status.
        with self.assertRaises(ValueError):
            self.store.report_progress(t1["task_id"], "claude", "sneak done", status="done")

        done1 = self.store.complete_task(
            task_id=t1["task_id"],
            peer_id="claude",
            result_summary="Design looks good",
            artifacts=["reviews/design.md"],
        )
        self.assertFalse(done1["all_tasks_finished"])
        self.assertEqual(done1["coordinator_peer"], "antigravity")

        # Re-completing terminal task must fail.
        with self.assertRaises(ValueError):
            self.store.complete_task(t1["task_id"], "claude", "duplicate")

        done2 = self.store.complete_task(
            task_id=t2["task_id"],
            peer_id="chatgpt",
            result_summary="Tests added",
            artifacts=["tests/test_x.py"],
        )
        self.assertTrue(done2["all_tasks_finished"])

        snap = self.store.get_mission_snapshot(mission_id)
        self.assertEqual(snap["mission"]["status"], "ready_to_report")

        # Re-dispatching after ready_to_report must reset mission to active.
        re_dispatched = self.store.dispatch_task(
            mission_id=mission_id,
            assignee_peer="claude",
            prompt="Follow-up patch",
        )
        t3 = re_dispatched["task"]
        self.assertEqual(re_dispatched["mission"]["status"], "active")

        # Non-coordinator cannot close mission; invalid close status rejected.
        with self.assertRaises(ValueError):
            self.store.close_mission(mission_id, "Nope", peer_id="claude")
        with self.assertRaises(ValueError):
            self.store.close_mission(mission_id, "Bad status", peer_id="antigravity", status="bogus")
        # Open task blocks close without force=True.
        with self.assertRaises(ValueError):
            self.store.close_mission(mission_id, "Too early", peer_id="antigravity")

        # Force close cancels remaining open task and emits mission_closed event.
        closed = self.store.close_mission(
            mission_id,
            "Closed with cancellation.",
            peer_id="antigravity",
            status="completed",
            force=True,
        )
        self.assertEqual(closed["status"], "completed")
        self.assertEqual(closed["cancelled_task_ids"], [t3["task_id"]])
        self.assertEqual(closed["events"][0]["kind"], "mission_closed")

        # Closed mission rejects further dispatch.
        with self.assertRaises(ValueError):
            self.store.dispatch_task(mission_id, "chatgpt", "Too late")

    def test_release_stale_claims(self) -> None:
        created = self.store.create_mission(
            title="Stale claim recovery",
            goal="Recover orphaned claim",
            coordinator_peer="antigravity",
            tasks=[{"assignee_peer": "claude", "prompt": "Do work"}],
        )
        task_id = created["tasks"][0]["task_id"]
        self.store.pickup_inbox("claude", session_label="dead-session", claim_tasks=True)

        # Backdate claimed_at in DB to simulate an old claim.
        with sqlite3.connect(self.store.db_path) as conn:
            conn.execute(
                "UPDATE tasks SET claimed_at = '2020-01-01T00:00:00Z' WHERE task_id = ?",
                (task_id,),
            )
            conn.commit()

        released = self.store.release_stale_claims(timeout_sec=60)
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["task_id"], task_id)
        self.assertEqual(released[0]["status"], "queued")
        self.assertEqual(released[0]["claimed_at"], "")

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
