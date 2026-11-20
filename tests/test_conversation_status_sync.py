from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from consilium.mcp_server import ConsiliumDaemon
from consilium.models import ConflictError
from tests.test_mcp_transports import _raw_http_roundtrip


class ConversationStatusSyncTests(unittest.TestCase):
    def test_plan_and_goal_intent_survive_routing_and_retries(self):
        with tempfile.TemporaryDirectory(prefix="consilium-intent-", dir="/tmp") as root:
            daemon = ConsiliumDaemon(home_dir=root, rendezvous_dir=root)
            self.addCleanup(daemon.stop)
            auth = {"Authorization": "Bearer " + daemon.auth_token, "Content-Type": "application/json"}

            def send(path, **payload):
                status, _, body = _raw_http_roundtrip(
                    daemon, "POST", path, auth, body=json.dumps(payload).encode("utf-8")
                )
                return status, json.loads(body)

            initial = {
                "text": "先设计", "intent": "plan", "workspace": root,
                "targets": ["claude", "chatgpt", "antigravity"], "client_request_id": "initial-plan",
            }
            with patch("consilium.mcp_server.sync_local_quotas"), patch(
                "consilium.mcp_server._safe_ring_peer", return_value={}
            ):
                status, response = send("/api/conversations", **initial)
                self.assertEqual(status, 200)
                mission_id = response["result"]["mission_id"]
                self.assertEqual(send("/api/conversations", **initial)[1]["result"]["mission_id"], mission_id)
                self.assertEqual(send("/api/conversations", **{**initial, "intent": "goal"})[0], 409)
                followup = "/api/missions/" + mission_id + "/message"
                self.assertEqual(send(followup, text="执行并验证", intent="goal")[0], 200)
                self.assertEqual(send(followup, text="普通消息")[0], 200)
                self.assertEqual(send(followup, text="未知模式", intent="invalid")[0], 400)

            mission = daemon.get_dashboard_state(sync_quotas=False)["missions"][0]
            messages = mission["user_messages"]
            self.assertEqual([(m["text"], m["intent"]) for m in messages],
                             [("先设计", "plan"), ("执行并验证", "goal"), ("普通消息", "")])
            plans = [t for t in mission["tasks"] if t["user_message_id"] == messages[0]["message_id"]]
            self.assertEqual({t["assignee_peer"] for t in plans}, set(initial["targets"]))
            self.assertTrue(all(t["prompt"].startswith("【计划模式】") for t in plans))
            goal = next(t for t in mission["tasks"] if t["user_message_id"] == messages[1]["message_id"])
            self.assertIn("【目标模式】", goal["prompt"])
            pickup = daemon.dispatcher.store.pickup_inbox(goal["assignee_peer"], mission_id=mission_id)
            claimed_goal = next(t for t in pickup["claimed_tasks"] if t["task_id"] == goal["task_id"])
            self.assertIn("【目标模式】", claimed_goal["prompt"])
            self.assertIn("plan", [turn.get("intent") for turn in claimed_goal["conversation"]])

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
