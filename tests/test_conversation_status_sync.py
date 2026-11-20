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
    def test_one_native_parent_and_public_progress_on_the_current_attempt(self):
        with tempfile.TemporaryDirectory(prefix="consilium-native-agent-", dir="/tmp") as root:
            daemon = ConsiliumDaemon(home_dir=root, rendezvous_dir=root)
            self.addCleanup(daemon.stop)
            store = daemon.dispatcher.store
            with patch("consilium.mcp_server.sync_local_quotas"), patch(
                "consilium.mcp_server._safe_ring_peer", return_value={}
            ) as ring:
                initial = daemon.ui_send(
                    "读两项独立结果", [], workspace_hint=root, parallel_fanout=2, client_request_id="native"
                )
                mid = initial["mission_id"]
                self.assertEqual(daemon.ui_send(
                    "读两项独立结果", [], workspace_hint=root, parallel_fanout=2, client_request_id="native"
                )["mission_id"], mid)
                with self.assertRaises(ConflictError):
                    daemon.ui_send("读两项独立结果", [], workspace_hint=root,
                                   parallel_fanout=3, client_request_id="native")
                with self.assertRaises(ValueError):
                    daemon.ui_send("同一任务", ["chatgpt", "claude"], mid, parallel_fanout=2)
                for count in (0, True, 2.5, "2", 1 << 53):
                    with self.subTest(count=count), self.assertRaises(ValueError):
                        daemon.ui_send("无效数量", ["chatgpt"], mid, parallel_fanout=count)

                auth = {"Authorization": "Bearer " + daemon.auth_token, "Content-Type": "application/json"}
                request = {"text": "核对证据", "parallel_fanout": 3, "report_process": True, "intent": "plan"}
                status, _, _ = _raw_http_roundtrip(
                    daemon, "POST", f"/api/missions/{mid}/message", auth, body=json.dumps(request).encode()
                )
                self.assertEqual(status, 200)
                missions = store.get_dashboard_missions(source="ui")[0]
                self.assertEqual(len(missions), 1)
                self.assertEqual(len(missions[0]["tasks"]), 2)
                assignee = initial["targets"][0]
                received = store.pickup_inbox(assignee, mission_id=mid)["claimed_tasks"]
                first = next(task for task in received if task["parallel_fanout"] == 2)
                self.assertEqual(first["parallel_fanout"], 2)
                self.assertIn("spawn_agent", first["prompt"])
                trace_task = next(task for task in missions[0]["tasks"] if task["parallel_fanout"] == 3)
                claimed_tasks = received if trace_task["assignee_peer"] == assignee else store.pickup_inbox(
                    trace_task["assignee_peer"], mission_id=mid
                )["claimed_tasks"]
                claimed = next(task for task in claimed_tasks if task["task_id"] == trace_task["task_id"])
                for marker in ("【计划模式】", "并行启动 3 个原生子代理", "【过程摘要】", "核对证据"):
                    self.assertIn(marker, claimed["prompt"])
                ring.reset_mock()
                for note in ("已分派两项只读核查", "已用隔离样例验证路由"):
                    returned = daemon.dispatcher._dispatch_tool("consilium_report_progress", {
                        "peer_id": claimed["assignee_peer"], "task_id": claimed["task_id"],
                        "attempt_id": claimed["attempt_id"], "progress_note": note,
                    })
                    self.assertTrue(returned["event"]["payload"]["public_report"])
                    self.assertEqual(returned["event"]["to_peer"], "user")
                    self.assertTrue(returned["event"]["acked_at"])
                    self.assertEqual(returned["delivery"]["delivery"], "none")
                ring.assert_not_called()

            snapshot = daemon.get_dashboard_state(sync_quotas=False)["missions"][0]
            self.assertEqual([m["report_process"] for m in snapshot["user_messages"]], [False, True])
            updates = [e["payload"] for e in snapshot["events"] if e["kind"] == "progress"]
            self.assertEqual(len(updates), 2)
            self.assertTrue(all(p["attempt_id"] == claimed["attempt_id"] for p in updates))

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
