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


if __name__ == "__main__":
    unittest.main()
