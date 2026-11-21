from __future__ import annotations

import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from consilium.dashboard import render_dashboard_html
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

    def test_inline_pending_row_for_claimed_and_running_preserves_edge_and_drawer_states(self):
        html = render_dashboard_html("0.7.2")
        self.assertIn(".chat-pending > .peer-icon { width: 26px; height: 26px; border-radius: 7px; flex-shrink: 0; }", html)
        self.assertIn(".chat-pending-text { min-width: 0; overflow-wrap: anywhere; }", html)
        self.assertIn(".pend-dots { display: inline-flex; gap: 3px; flex-shrink: 0; }", html)
        self.assertIn(".task-receipt {", html)
        self.assertIn("@media (max-width: 860px)", html)

        script_match = re.search(r"<script>\s*(.*?)\s*</script>", html, re.S)
        self.assertIsNotNone(script_match)
        script_body = script_match.group(1)
        marker = "\n  fetchState().then(ok => {"
        self.assertEqual(script_body.count(marker), 1)
        hook = (
            "\n  window.__testExports = {\n"
            "    renderPending, renderConversation, renderTasksTab, compactTaskStatus,\n"
            "    panelContent, setSelected: id => { selectedMissionId = id; }\n"
            "  };\n"
            "  return;"
            + marker
        )
        instrumented = script_body.replace(marker, hook, 1)

        node_runner = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
const code = fs.readFileSync(0, "utf8");
const elements = {};
const makeEl = () => ({
  classList: { add() {}, remove() {}, toggle() {} },
  style: {},
  dataset: {},
  children: [],
  innerHTML: "",
  textContent: "",
  scrollTop: 0,
  scrollHeight: 100,
  clientHeight: 100,
  focus() {},
  addEventListener() {},
  setAttribute() {},
  getAttribute() { return ""; },
  querySelector() { return makeEl(); },
  querySelectorAll() { return []; },
  getBoundingClientRect() { return { top: 0, bottom: 0 }; }
});
const doc = {
  documentElement: makeEl(),
  getElementById(id) { return elements[id] || (elements[id] = makeEl()); },
  querySelector(sel) { return elements[sel] || (elements[sel] = makeEl()); },
  querySelectorAll() { return []; },
  addEventListener() {}
};
const ctx = {
  window: { innerWidth: 1200, addEventListener() {}, webkit: null },
  document: doc,
  localStorage: { getItem() { return null; }, setItem() {} },
  navigator: {},
  crypto: { getRandomValues: arr => arr.fill(1) },
  requestAnimationFrame() {},
  setInterval() { return 0; },
  clearTimeout() {},
  setTimeout() { return 0; },
  fetch: () => Promise.resolve({ ok: false }),
  EventSource: function() {},
  Date, Math, Number, String, Array, Object, Set, Map, JSON, RegExp, Error, Promise, encodeURIComponent,
};
vm.runInNewContext(code, ctx);
const api = ctx.window.__testExports;

const now = "2026-11-21T03:55:00Z";
const mission = {
  mission_id: "M-TEST",
  title: "测试会诊",
  status: "active",
  coordinator_peer: "chatgpt",
  created_at: "2026-11-21T03:50:00Z",
  updated_at: now,
  user_messages: [
    { message_id: 1, text: "第一轮", targets: ["claude", "chatgpt"], mode: "direct", created_at: "2026-11-21T03:50:01Z" },
    { message_id: 2, text: "第二轮并行", targets: ["claude", "antigravity", "chatgpt"], mode: "direct", created_at: "2026-11-21T03:54:00Z" }
  ],
  tasks: [
    {
      task_id: "T-DONE", assignee_peer: "claude", status: "done", user_message_id: 1,
      prompt: "完成任务", result_summary: "已修复", model_used: "Claude Opus", tier_used: "balanced",
      model_tier: "auto", claimed_at: "2026-11-21T03:50:05Z", created_at: "2026-11-21T03:50:01Z", updated_at: "2026-11-21T03:51:00Z"
    },
    {
      task_id: "T-FAIL", assignee_peer: "chatgpt", status: "failed", user_message_id: 1,
      prompt: "失败任务", result_summary: "校验未通过", model_used: "GPT-6", tier_used: "fast",
      model_tier: "fast", claimed_at: "2026-11-21T03:50:05Z", created_at: "2026-11-21T03:50:01Z", updated_at: "2026-11-21T03:51:10Z"
    },
    {
      task_id: "T-RUN", assignee_peer: "claude", status: "running", user_message_id: 2,
      prompt: "运行中任务", progress_note: "正在比对日志", model_used: "Claude Opus", parallel_fanout: 2,
      tier_used: "deep", model_tier: "pro_reasoning", claimed_at: "2026-11-21T03:54:05Z",
      last_progress_at: now, created_at: "2026-11-21T03:54:00Z", updated_at: now
    },
    {
      task_id: "T-CLAIM", assignee_peer: "antigravity", status: "claimed", user_message_id: 2,
      prompt: "已认领任务", progress_note: "", model_tier: "auto",
      claimed_at: "2026-11-21T03:54:06Z", created_at: "2026-11-21T03:54:00Z", updated_at: "2026-11-21T03:54:06Z"
    },
    {
      task_id: "T-QUEUE", assignee_peer: "chatgpt", status: "queued", user_message_id: 2,
      prompt: "排队中任务", model_tier: "balanced", created_at: "2026-11-21T03:54:00Z", updated_at: "2026-11-21T03:54:00Z"
    }
  ],
  events: [
    { event_id: 10, kind: "task_done", from_peer: "claude", task_id: "T-DONE", created_at: "2026-11-21T03:51:00Z",
      payload: { task_id: "T-DONE", status: "done", result_summary: "已修复" } },
    { event_id: 11, kind: "task_done", from_peer: "chatgpt", task_id: "T-FAIL", created_at: "2026-11-21T03:51:10Z",
      payload: { task_id: "T-FAIL", status: "failed", result_summary: "校验未通过" } },
    { event_id: 12, kind: "task_assigned", from_peer: "user", to_peer: "chatgpt", task_id: "T-QUEUE",
      created_at: "2026-11-21T03:54:00Z", acked_at: "",
      payload: { origin: "user", delivery_mode: "cold_notification", notified: true } }
  ]
};
const state = {
  missions: [mission],
  active_waiters: { claude: 1, antigravity: 1, chatgpt: 0 },
  quotas: [
    { peer_id: "claude", exhausted: false },
    { peer_id: "antigravity", exhausted: false },
    { peer_id: "chatgpt", exhausted: false }
  ]
};

// 1. Running & claimed tasks render borderless inline rows with icon, "正在处理", and 3 dots.
const runHtml = api.renderPending(mission.tasks[2], state, mission);
assert.ok(runHtml.includes('class="chat-pending" data-task-pending="T-RUN"'), runHtml);
assert.ok(!runHtml.includes("task-receipt"), runHtml);
assert.ok(runHtml.includes('<span class="peer-icon claude"'), runHtml);
assert.ok(runHtml.includes("Claude 正在处理：正在比对日志 · 请求 2 个原生子代理 · Claude Opus"), runHtml);
assert.ok(runHtml.includes('<span class="pend-dots" aria-hidden="true"><i></i><i></i><i></i></span>'), runHtml);

const claimHtml = api.renderPending(mission.tasks[3], state, mission);
assert.ok(claimHtml.includes('class="chat-pending" data-task-pending="T-CLAIM"'), claimHtml);
assert.ok(!claimHtml.includes("task-receipt"), claimHtml);
assert.ok(claimHtml.includes('<span class="peer-icon antigravity"'), claimHtml);
assert.ok(claimHtml.includes("Antigravity 正在处理"), claimHtml);
assert.ok(claimHtml.includes('<span class="pend-dots" aria-hidden="true"><i></i><i></i><i></i></span>'), claimHtml);

// 2. Queued tasks keep the bordered receipt card and action button.
const queueHtml = api.renderPending(mission.tasks[4], state, mission);
assert.ok(queueHtml.includes("task-receipt"), queueHtml);
assert.ok(queueHtml.includes("Codex · 待认领"), queueHtml);
assert.ok(queueHtml.includes("系统通知已发，等领取任务"), queueHtml);
assert.ok(queueHtml.includes("在 Codex 里取件"), queueHtml);
assert.ok(!queueHtml.includes("pend-dots"), queueHtml);

// 3. Stale lease & exhausted quota edge states keep receipt cards instead of bouncing dots.
const staleTask = Object.assign({}, mission.tasks[2], { stale_at: now });
const staleHtml = api.renderPending(staleTask, state, mission);
assert.ok(staleHtml.includes("task-receipt"), staleHtml);
assert.ok(staleHtml.includes("Claude · 进度过期"), staleHtml);
assert.ok(!staleHtml.includes("pend-dots"), staleHtml);

const exhaustedState = Object.assign({}, state, {
  quotas: [{ peer_id: "claude", exhausted: true, current_account_unknown: false, account_switch_suspected: false }]
});
const exhaustedHtml = api.renderPending(mission.tasks[2], exhaustedState, mission);
assert.ok(exhaustedHtml.includes("task-receipt quota-exhausted"), exhaustedHtml);
assert.ok(exhaustedHtml.includes("Claude 当前账号额度已用完"), exhaustedHtml);
assert.ok(!exhaustedHtml.includes("pend-dots"), exhaustedHtml);

// 4. Full conversation keeps done/failed replies, multi-peer parallel rows, narrow summary, and right drawer.
api.setSelected("M-TEST");
api.renderConversation(state);
const chatHtml = doc.getElementById("chat-container").innerHTML;
assert.ok(chatHtml.includes('data-task-pending="T-RUN"'), chatHtml);
assert.ok(chatHtml.includes('data-task-pending="T-CLAIM"'), chatHtml);
assert.ok(chatHtml.includes('data-task-receipt="T-QUEUE"'), chatHtml);
assert.ok(!chatHtml.includes("T-DONE"), chatHtml);
assert.ok(!chatHtml.includes("T-FAIL"), chatHtml);
assert.ok(chatHtml.includes("已修复"), chatHtml);
assert.ok(chatHtml.includes('<span class="chat-tag bad">失败</span>'), chatHtml);
assert.ok(chatHtml.includes("校验未通过"), chatHtml);

const narrowSummary = api.compactTaskStatus(mission);
assert.ok(narrowSummary.includes("Claude 执行中"), narrowSummary);
assert.ok(narrowSummary.includes("Antigravity 已认领"), narrowSummary);
assert.ok(narrowSummary.includes("Codex 待认领"), narrowSummary);

const drawerHtml = api.renderTasksTab(mission, api.panelContent(mission), state);
assert.ok(drawerHtml.includes("5 项任务 · 1 项完成"), drawerHtml);
assert.ok(drawerHtml.includes('data-toggle-task="T-FAIL"'), drawerHtml);
assert.ok(drawerHtml.includes("快 · GPT-6 · 失败"), drawerHtml);
assert.ok(drawerHtml.includes("已上报进度"), drawerHtml);
assert.ok(drawerHtml.includes("已认领 · 等开始"), drawerHtml);
assert.ok(drawerHtml.includes("待认领"), drawerHtml);
assert.ok(drawerHtml.includes("正在比对日志"), drawerHtml);
"""
        proc = subprocess.run(
            ["node", "-e", node_runner],
            input=instrumented,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=f"node stderr:\n{proc.stderr}\nstdout:\n{proc.stdout}")
