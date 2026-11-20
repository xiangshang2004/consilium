from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from consilium.mcp_server import ConsiliumDaemon
from consilium.models import ConflictError


class ConversationStatusSyncTests(unittest.TestCase):
    def test_attempt_receipts_and_duplicate_settlement(self):
        with tempfile.TemporaryDirectory(prefix="consilium-sync-", dir="/tmp") as root:
            state_dir, rv_dir = Path(root) / "state", Path(root) / "rv"
            daemon = ConsiliumDaemon(home_dir=str(state_dir), rendezvous_dir=str(rv_dir))
            store = daemon.dispatcher.store
            self.addCleanup(daemon.stop)
            with patch("consilium.mcp_server.sync_local_quotas"), patch("consilium.mcp_server._safe_ring_peer", return_value={}):
                mission_id = daemon.ui_send("对话状态", ["antigravity","chatgpt"], workspace_hint=root)["mission_id"]
            snap = store.get_dashboard_missions(source="ui")[0][0]
            old = next(task for task in snap["tasks"] if task["assignee_peer"] == "antigravity")
            target = store.queued_notification(old["task_id"])
            store.record_event_delivery([target["event_id"]], {"delivery":"hot_wakeup"})
            store.record_event_delivery([target["event_id"]], {"delivery":"cold_notification","notified":True})
            queued = store.get_dashboard_missions(source="ui")[0][0]
            assignment = next(event for event in queued["events"] if event["event_id"] == target["event_id"])
            self.assertEqual([fact["mode"] for fact in assignment["payload"]["delivery_attempts"]],
                             ["hot_wakeup","cold_notification"])
            self.assertFalse(assignment["acked_at"])
            acquired = store.pickup_inbox("antigravity", mission_id=mission_id)["claimed_tasks"][0]
            with self.assertRaises(ConflictError):
                store.queued_notification(acquired["task_id"])
            store.report_progress(acquired["task_id"], "antigravity", "old", attempt_id=acquired["attempt_id"], status="running")
            delivered = store.pickup_inbox("chatgpt", mission_id=mission_id)["claimed_tasks"][0]
            store.complete_task(delivered["task_id"], "chatgpt", "complete", attempt_id=delivered["attempt_id"])
            self.assertEqual(store.settle_duplicate_task(acquired["task_id"], acquired["attempt_id"], delivered["task_id"])["status"], "cancelled")
            with self.assertRaises(ValueError):
                store.complete_task(acquired["task_id"], "antigravity", "late", attempt_id=acquired["attempt_id"])
            state = daemon.get_dashboard_state(sync_quotas=False)
            closed = next(t for t in state["missions"][0]["tasks"] if t["task_id"] == acquired["task_id"])
            self.assertTrue(closed["last_progress_at"])
            self.assertEqual(closed["attempt_history"][-1]["delivered_task_id"], delivered["task_id"])
            new = ConsiliumDaemon(home_dir=str(state_dir), rendezvous_dir=str(rv_dir))
            self.addCleanup(new.stop)
            self.assertNotEqual(state["state_instance_id"], new.get_dashboard_state(sync_quotas=False)["state_instance_id"])
