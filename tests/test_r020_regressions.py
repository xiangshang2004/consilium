from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import time
import unittest

from consilium.dashboard import build_dashboard_state, render_dashboard_html
from consilium.mcp_server import ConsiliumDaemon
from consilium.store import BusStore


class R020RegressionsTests(unittest.TestCase):
    """Conversations from different places stay apart; archiving is local to the window."""

    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="consilium-r020-st-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="consilium-r020-rv-", dir="/tmp")
        self._prev = {k: os.environ.get(k) for k in ("CONSILIUM_DRY_RUN", "CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC", "CONSILIUM_SOCKET_FALLBACK_DIR")}
        os.environ["CONSILIUM_DRY_RUN"] = "1"
        os.environ["CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC"] = "1"
        os.environ["CONSILIUM_SOCKET_FALLBACK_DIR"] = os.path.join(self.tmp_rv, "sock")
        self.daemon = ConsiliumDaemon(home_dir=self.tmp_state, rendezvous_dir=self.tmp_rv, host="127.0.0.1", port=0)
        self.store = self.daemon.dispatcher.store

    def tearDown(self) -> None:
        self.daemon.stop()
        for k, v in self._prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(self.tmp_state, ignore_errors=True)
        shutil.rmtree(self.tmp_rv, ignore_errors=True)

    def _tool(self, name: str, **args):
        resp = self.daemon.dispatcher.handle_payload(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": args}}
        )
        self.assertFalse(resp["result"].get("isError"), resp["result"])
        return resp["result"]["structuredContent"]

    def _window_titles(self):
        state = build_dashboard_state(
            self.store, self.tmp_rv, started_at="", started_monotonic=time.monotonic(),
            host="127.0.0.1", port=0, sync_quotas=False,
        )
        return [m["title"] for m in state["missions"]]

    def _other_project_mission(self) -> dict:
        """A session in some other project rings Codex through the bus."""
        created = self._tool(
            "consilium_create_mission", title="别的项目的门铃", goal="和 Consilium 窗口无关",
            coordinator_peer="claude", tasks=[{"assignee_peer": "chatgpt", "prompt": "停掉控制器"}],
        )
        return {"mission_id": created["mission"]["mission_id"], "task_id": created["tasks"][0]["task_id"]}

    def test_window_lists_only_conversations_started_in_it(self) -> None:
        other = self._other_project_mission()
        mine = self.daemon.ui_send("看一下登录页的报错", ["chatgpt"])
        self.assertEqual(self._window_titles(), ["看一下登录页的报错"])
        # The bus still knows the other mission; it is just not shown in the window.
        all_missions, _ = self.store.get_dashboard_missions()
        self.assertEqual(sorted(m["source"] for m in all_missions), ["peer", "ui"])
        self.assertEqual(self._tool("consilium_status", mission_id=other["mission_id"])["mission"]["title"], "别的项目的门铃")
        self.assertNotEqual(mine["mission_id"], other["mission_id"])

    def test_standby_never_swallows_another_sessions_receipt(self) -> None:
        other = self._other_project_mission()
        picked = self._tool("consilium_pickup_inbox", peer_id="chatgpt", mission_id=other["mission_id"])
        task = picked["claimed_tasks"][0]
        self._tool("consilium_complete_task", task_id=task["task_id"], peer_id="chatgpt",
                   attempt_id=task["attempt_id"], result_summary="已停")
        self.daemon.ui_send("帮我看个报错", ["claude"])

        # The Consilium standby session only gets the window's work.
        standby = self._tool("consilium_pickup_inbox", peer_id="claude", session_label="claude-standby")
        self.assertEqual(standby["scope"], "consilium")
        self.assertEqual([t["prompt"] for t in standby["claimed_tasks"]], ["帮我看个报错"])
        self.assertTrue(all(e["mission_id"] != other["mission_id"] for e in standby["events"]))

        # The receipt is still unread for the session that created the mission.
        owner = self._tool("consilium_pickup_inbox", peer_id="claude", mission_id=other["mission_id"])
        self.assertEqual([e["kind"] for e in owner["events"]], ["task_done"])
        self.assertEqual(owner["claimed_tasks"], [])

    def test_scope_other_and_mission_filter_leave_the_window_alone(self) -> None:
        other = self._other_project_mission()
        self.daemon.ui_send("窗口里的消息", ["chatgpt"])
        got = self._tool("consilium_pickup_inbox", peer_id="chatgpt", scope="other")
        self.assertEqual([t["task_id"] for t in got["claimed_tasks"]], [other["task_id"]])
        left = self._tool("consilium_pickup_inbox", peer_id="chatgpt", scope="consilium")
        self.assertEqual([t["prompt"] for t in left["claimed_tasks"]], ["窗口里的消息"])
        bad = self.daemon.dispatcher.handle_payload(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
             "params": {"name": "consilium_pickup_inbox", "arguments": {"peer_id": "chatgpt", "scope": "everything"}}}
        )
        self.assertTrue(bad["result"].get("isError"))

    def test_existing_databases_are_classified_on_upgrade(self) -> None:
        self._other_project_mission()
        self.daemon.ui_send("升级前的窗口会诊", ["chatgpt"])
        self.daemon.ui_send("升级前的自动会诊", [])
        conn = sqlite3.connect(self.store.db_path)
        conn.execute("ALTER TABLE missions DROP COLUMN source")
        conn.commit()
        conn.close()
        upgraded = BusStore(self.tmp_state, rendezvous_dir=self.tmp_rv)
        missions, _ = upgraded.get_dashboard_missions()
        by_title = {m["title"]: m["source"] for m in missions}
        self.assertEqual(by_title["别的项目的门铃"], "peer")
        self.assertEqual(by_title["升级前的窗口会诊"], "ui")
        self.assertEqual(by_title["升级前的自动会诊"], "ui")

    def test_archiving_only_changes_the_window(self) -> None:
        mid = self.daemon.ui_send("归档我", ["chatgpt"])["mission_id"]
        self.store.bind_native_session(mid, "chatgpt", "codex-thread-1")

        def snapshot() -> dict:
            m = next(x for x in self.store.get_dashboard_missions()[0] if x["mission_id"] == mid)
            return {
                "status": m["status"], "native_sessions": m["native_sessions"],
                "tasks": [(t["task_id"], t["status"], t["attempt_id"]) for t in m["tasks"]],
                "events": len(m["events"]),
            }

        before = snapshot()
        self.store.set_mission_archived(mid, True)
        self.assertEqual(snapshot(), before, "archiving must not touch tasks, events or bound app sessions")
        self.assertTrue(next(x for x in self.store.get_dashboard_missions()[0] if x["mission_id"] == mid)["archived"])
        # The archived conversation still receives its queued work.
        self.assertEqual(len(self.store.pickup_inbox("chatgpt", scope="consilium")["claimed_tasks"]), 1)

    def test_archive_lives_in_the_sidebar_and_standby_prompts_are_scoped(self) -> None:
        html = render_dashboard_html("0.1.0")
        self.assertIn('class="mi-archive" data-archive-mission="', html)
        self.assertIn(".mission-item:hover .mi-archive", html)
        self.assertNotIn("esc(cur.mission_id) + '\"' + (cur.archived", html, "archive left the top-right menu")
        self.assertEqual(html.count('scope=\\"consilium\\"'), 3, "all three standby prompts pick up only the window's work")
        self.assertNotIn("<button type=\"button\" class=\"mission-item", html, "no button nested inside a button")

    def test_composer_ignores_ime_enter_clears_on_send_and_follows_the_latest_message(self) -> None:
        html = render_dashboard_html("0.1.0")
        # Enter that confirms an input-method candidate must not send (WebKit reports isComposing=false).
        self.assertIn("ev.keyCode === 229", html)
        self.assertIn('addEventListener("compositionend"', html)
        self.assertIn('ev.key === "Enter" && !ev.shiftKey && !isImeEnter(ev)', html)
        # The box is emptied when the message leaves, and only restored when it was not accepted.
        send = html[html.index("async function sendMessage()"):html.index('getElementById("btn-send").addEventListener')]
        self.assertLess(send.index('inp.value = "";'), send.index("await fetch(path"))
        self.assertIn("if (!accepted) restoreDraft();", send)
        self.assertEqual(send.count('inp.value = "";'), 1)
        # The conversation scrolls to the newest message after sending and for replies while at the bottom.
        self.assertIn("scrollToLatest = true;", send)
        self.assertIn("if (scrollToLatest || switched || (grew && wasAtBottom))", html)
        self.assertIn("scroller.scrollTop = scroller.scrollHeight;", html)
    def test_effort_is_recorded_shown_and_quota_is_not_self_reported(self) -> None:
        self.daemon.ui_send("看个报错", ["chatgpt"])
        task = self._tool("consilium_pickup_inbox", peer_id="chatgpt", scope="consilium")["claimed_tasks"][0]
        self._tool("consilium_report_progress", task_id=task["task_id"], peer_id="chatgpt", attempt_id=task["attempt_id"],
                   progress_note="在看", model_used="GPT-5.5", effort_used="  xhigh  ")
        self.assertEqual(self.store.latest_effort_by_peer(), {"chatgpt": "xhigh"})
        self._tool("consilium_complete_task", task_id=task["task_id"], peer_id="chatgpt", attempt_id=task["attempt_id"],
                   result_summary="好了", model_used="GPT-5.5", effort_used="high")
        state = build_dashboard_state(self.store, self.tmp_rv, started_at="", started_monotonic=time.monotonic(),
                                      host="127.0.0.1", port=0, sync_quotas=False)
        self.assertEqual(state["peer_efforts"], {"chatgpt": "high"})
        self.assertEqual(state["missions"][0]["tasks"][0]["effort_used"], "high")
        html = render_dashboard_html("0.1.0")
        self.assertIn("efforts[pid]", html)
        self.assertIn("task.effort_used", html)
        self.assertIn(".dock-model { grid-column: 1 / -1;", html, "model and effort get their own full-width line")
        self.assertIn('class="dock-model" data-peer-pill="', html)
        self.assertNotIn("text-overflow: ellipsis; white-space: nowrap; }\n.dock-side", html, "the status line is no longer clipped")
        self.assertEqual(html.count("不要自己查询或汇报余量"), 3, "standby prompts tell peers quota comes from Cockpit")
        from consilium.mcp_server import build_auto_coordinator_prompt, _quota_line
        line = _quota_line({"peer_id": "chatgpt", "label": "Codex", "standby": True, "remaining_percent": 88.0,
                            "exhausted": False, "record_age_minutes": 64})
        self.assertIn("余量 88%（Cockpit 64 分钟前的记录）", line)
        prompt = build_auto_coordinator_prompt("x", "claude", [], [], [])
        self.assertIn("effort_used", prompt)
        self.assertIn("不要让各端自己查询或汇报余量", prompt)
    def test_right_panel_follows_claude_code_side_panes(self) -> None:
        html = render_dashboard_html("0.2.0")
        # Icon buttons in the top bar open a panel beside the chat; one tab per kind of content.
        self.assertIn('id="panel-tools"', html)
        for key in ('{ key: "tasks", label: "任务" }', '{ key: "files", label: "文件" }', '{ key: "branches", label: "分支" }'):
            self.assertIn(key, html)
        self.assertIn("a tab exists only while it has something to show", html)
        self.assertIn('data-toggle-panel="1"', html)
        self.assertIn('id="drawer-grip"', html)
        self.assertIn('storePref("consilium.panelWidth", panelWidth)', html)
        self.assertIn('ev.key === "\\\\" && rightTab', html, "Cmd+Backslash closes the panel")
        # Old chips and slash commands still land on the right tab.
        self.assertIn('const PANEL_ALIASES = { artifacts: "files", worktrees: "branches" };', html)
        # Things that are not about one conversation moved to settings; the empty placeholder tab is gone.
        self.assertIn('id="settings-overlay"', html)
        self.assertIn('data-open-settings="', html)
        for gone in ("DRAWER_TABS", "子 Agent", "该端未上报", "阶段 5 开放"):
            self.assertNotIn(gone, html)
        # Revealing a file is delegated to the app and never typed as a URL.
        self.assertIn('postMessage({ action: "revealPath"', html)
        self.assertNotIn("file://", html)
        self.assertNotIn("http://", html)
        self.assertNotIn("https://", html)
    def test_tier_is_automatic_by_default_and_can_be_changed_inside_a_conversation(self) -> None:
        mid = self.daemon.ui_send("看个报错", ["chatgpt"])["mission_id"]
        task = self._tool("consilium_pickup_inbox", peer_id="chatgpt", scope="consilium")["claimed_tasks"][0]
        self.assertEqual((task["model_tier"], task["mission_tier"]), ("auto", "auto"))
        # The peer decides, works at that tier and says which and why.
        bad = self.daemon.dispatcher.handle_payload({"jsonrpc": "2.0", "id": 9, "method": "tools/call", "params": {
            "name": "consilium_report_progress", "arguments": {"task_id": task["task_id"], "peer_id": "chatgpt",
            "attempt_id": task["attempt_id"], "progress_note": "x", "tier_used": "turbo"}}})
        self.assertTrue(bad["result"].get("isError"))
        self._tool("consilium_complete_task", task_id=task["task_id"], peer_id="chatgpt", attempt_id=task["attempt_id"],
                   result_summary="好了", model_used="GPT-5.5", effort_used="xhigh", tier_used="deep",
                   tier_reason="  涉及并发，答错代价大  ")
        row = next(m for m in self.store.get_dashboard_missions()[0] if m["mission_id"] == mid)["tasks"][0]
        self.assertEqual((row["tier_used"], row["tier_reason"]), ("pro_reasoning", "涉及并发，答错代价大"))

        # Changing the tier in an existing conversation applies to that message and sticks to the conversation.
        self.daemon.ui_send("再深入看看", ["chatgpt"], mid, tier="deep")
        t2 = self._tool("consilium_pickup_inbox", peer_id="chatgpt", scope="consilium")["claimed_tasks"][0]
        self.assertEqual((t2["model_tier"], t2["mission_tier"]), ("pro_reasoning", "pro_reasoning"))
        self.daemon.ui_send("继续", ["chatgpt"], mid)  # not said this time: keep the conversation's tier
        t3 = self._tool("consilium_pickup_inbox", peer_id="chatgpt", scope="consilium")["claimed_tasks"][0]
        self.assertEqual(t3["model_tier"], "pro_reasoning")
        self.daemon.ui_send("随便问一句", ["chatgpt"], mid, tier="auto")
        t4 = self._tool("consilium_pickup_inbox", peer_id="chatgpt", scope="consilium")["claimed_tasks"][0]
        self.assertEqual((t4["model_tier"], t4["mission_tier"]), ("auto", "auto"))

        from consilium.mcp_server import build_auto_coordinator_prompt
        auto_prompt = build_auto_coordinator_prompt("x", "claude", [], [], [])
        self.assertIn("由你按难度自己定", auto_prompt)
        self.assertIn("不要让使用者来选", auto_prompt)
        self.assertIn("tier_used", auto_prompt)
        fixed_prompt = build_auto_coordinator_prompt("x", "claude", [], [], [], tier="pro_reasoning")
        self.assertIn("使用者指定了“深度”", fixed_prompt)

        html = render_dashboard_html("0.3.0")
        self.assertIn('["auto", "自动", "按难度自己定，不用你选"]', html)
        self.assertIn('let composeTier = "auto";', html)
        self.assertEqual(html.count("tier: composeTier"), 2, "new and existing conversations both send the tier")
        self.assertIn("optsEl.innerHTML = closedMission ? \"\" : tierBtnHtml + tierMenuHtml;", html,
                      "the tier button is also there in an existing conversation")
        self.assertIn("const tn = tierNote(task);", html)
        self.assertEqual(html.count("由你按难度自己定档位并真的按它做"), 3, "all three standby prompts")

if __name__ == "__main__":
    unittest.main()
