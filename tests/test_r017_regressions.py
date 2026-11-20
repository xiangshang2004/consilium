from __future__ import annotations

import http.client
import json
import os
import shutil
import tempfile
import unittest

from consilium.mcp_server import (
    ConsiliumDaemon,
    build_auto_coordinator_prompt,
    choose_auto_coordinator,
)
from consilium.models import USER_PEER, read_auth_token


class R017RegressionsTests(unittest.TestCase):
    """Send from the dashboard, auto routing, learned preferences and quota-aware handoff."""

    def setUp(self) -> None:
        self.tmp_state = tempfile.mkdtemp(prefix="consilium-r017-st-", dir="/tmp")
        self.tmp_rv = tempfile.mkdtemp(prefix="consilium-r017-rv-", dir="/tmp")
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

    def _mission(self, mid: str) -> dict:
        missions, _ = self.store.get_dashboard_missions()
        return next(m for m in missions if m["mission_id"] == mid)

    def test_direct_send_makes_user_the_coordinator_and_replies_come_back_to_ui(self) -> None:
        res = self.daemon.ui_send("看一下登录页的报错", ["chatgpt", "antigravity"])
        self.assertEqual(res["mode"], "direct")
        self.assertEqual(sorted(res["targets"]), ["antigravity", "chatgpt"])
        m = self._mission(res["mission_id"])
        self.assertEqual(m["coordinator_peer"], USER_PEER)
        self.assertEqual([t["origin"] for t in m["tasks"]], ["user", "user"])
        self.assertEqual(m["user_messages"][0]["text"], "看一下登录页的报错")
        self.assertEqual(m["user_messages"][0]["mode"], "direct")

        inbox = self.store.pickup_inbox("chatgpt")
        task = inbox["claimed_tasks"][0]
        self.assertEqual(task["prompt"], "看一下登录页的报错")
        done = self.store.complete_task(task["task_id"], "chatgpt", "是空指针", attempt_id=task["attempt_id"])
        self.assertEqual(done["event"]["to_peer"], USER_PEER)
        self.assertTrue(done["event"]["acked_at"], "events to the dashboard user are acknowledged at once")
        self.assertNotIn(USER_PEER, [p["peer_id"] for p in self.store.list_peers()])

    def test_auto_send_picks_a_coordinator_and_briefs_it_with_preferences_and_quota(self) -> None:
        self.store.add_preference("代码审查默认交给 Codex", source="claude")
        res = self.daemon.ui_send("帮我把这个函数重构一下", [])
        self.assertEqual(res["mode"], "auto")
        m = self._mission(res["mission_id"])
        self.assertIn(m["coordinator_peer"], ("claude", "chatgpt", "antigravity"))
        prompt = m["tasks"][0]["prompt"]
        self.assertTrue(prompt.startswith("帮我把这个函数重构一下"))
        self.assertIn("代码审查默认交给 Codex", prompt)
        self.assertIn("【各端状态】", prompt)
        self.assertIn("consilium_handoff_task", prompt)

        follow = self.daemon.ui_send("顺便加上测试", [], res["mission_id"])
        self.assertEqual(follow["targets"], [m["coordinator_peer"]])
        self.assertEqual(len(self._mission(res["mission_id"])["user_messages"]), 2)

    def test_choose_auto_coordinator_prefers_standby_and_skips_exhausted(self) -> None:
        digest = [
            {"peer_id": "claude", "standby": False, "remaining_percent": 50, "exhausted": False},
            {"peer_id": "chatgpt", "standby": True, "remaining_percent": 60, "exhausted": False},
            {"peer_id": "antigravity", "standby": True, "remaining_percent": 0, "exhausted": True},
        ]
        self.assertEqual(choose_auto_coordinator(digest), "chatgpt")
        digest[1]["standby"] = False
        self.assertEqual(choose_auto_coordinator(digest), "claude")
        prompt = build_auto_coordinator_prompt("hi", "claude", [dict(d, label=d["peer_id"]) for d in digest], [], [])
        self.assertIn("已耗尽", prompt)

    def test_preferences_are_deduplicated_capped_and_removable(self) -> None:
        a = self.store.add_preference("  回答用中文  ", source="claude")
        b = self.store.add_preference("回答用中文", source="chatgpt")
        self.assertTrue(b.get("duplicate"))
        self.assertEqual(a["pref_id"], b["pref_id"])
        with self.assertRaises(ValueError):
            self.store.add_preference("", source="claude")
        with self.assertRaises(ValueError):
            self.store.add_preference("x", source="mallory")
        self.assertTrue(self.store.deactivate_preference(a["pref_id"]))
        self.assertEqual(self.store.list_preferences(), [])

        out = self.daemon.dispatcher._dispatch_tool(
            "consilium_learn_preference", {"peer_id": "claude", "text": "重构前先跑测试"}
        )
        self.assertTrue(out["committed"])
        prefs = self.daemon.dispatcher._dispatch_tool("consilium_preferences", {})
        self.assertEqual([p["text"] for p in prefs["preferences"]], ["重构前先跑测试"])
        self.assertEqual({p["peer_id"] for p in prefs["peers"]}, {"claude", "chatgpt", "antigravity"})

    def test_handoff_moves_task_to_another_peer_and_invalidates_old_attempt(self) -> None:
        res = self.daemon.ui_send("写一份周报", ["claude"])
        task = self.store.pickup_inbox("claude")["claimed_tasks"][0]
        with self.assertRaises(ValueError):
            self.store.handoff_task(task["task_id"], "claude", task["attempt_id"], "claude", "x")
        with self.assertRaises(ValueError):
            self.store.handoff_task(task["task_id"], "claude", "A-0-WRONG", "chatgpt", "额度不够")
        out = self.daemon.dispatcher._dispatch_tool(
            "consilium_handoff_task",
            {"task_id": task["task_id"], "peer_id": "claude", "attempt_id": task["attempt_id"],
             "to_peer": "chatgpt", "note": "额度只剩 5%，提纲已写好"},
        )
        self.assertEqual(out["task"]["assignee_peer"], "chatgpt")
        self.assertEqual(out["task"]["status"], "queued")
        self.assertIn("额度只剩 5%", out["task"]["prompt"])
        self.assertEqual(out["event"]["kind"], "task_handoff")
        with self.assertRaises(ValueError):
            self.store.complete_task(task["task_id"], "claude", "done", attempt_id=task["attempt_id"])
        new = self.store.pickup_inbox("chatgpt")["claimed_tasks"][0]
        self.assertEqual(new["task_id"], task["task_id"])
        self.assertEqual(self._mission(res["mission_id"])["coordinator_peer"], USER_PEER)

    def test_http_send_requires_session_csrf_and_rejects_bad_targets(self) -> None:
        host, port = self.daemon.start()
        token = read_auth_token(self.tmp_state)
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("POST", "/api/login", body=json.dumps({"token": token}), headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        login = json.loads(resp.read())
        cookie = resp.getheader("Set-Cookie").split(";", 1)[0]
        conn.close()
        origin = f"http://{host}:{port}"

        def post(path: str, body: dict, csrf: bool = True) -> tuple:
            c = http.client.HTTPConnection(host, port, timeout=5)
            headers = {"Content-Type": "application/json", "Cookie": cookie, "Origin": origin}
            if csrf:
                headers["X-Consilium-CSRF"] = login["csrf_token"]
            c.request("POST", path, body=json.dumps(body), headers=headers)
            r = c.getresponse()
            data = r.read()
            c.close()
            return r.status, (json.loads(data) if data else {})

        self.assertEqual(post("/api/conversations", {"text": "hi", "targets": ["chatgpt"]}, csrf=False)[0], 403)
        self.assertEqual(post("/api/conversations", {"text": "hi", "targets": ["mallory"]})[0], 400)
        self.assertEqual(post("/api/conversations", {"text": "   ", "targets": []})[0], 400)
        status, body = post("/api/conversations", {"text": "你好", "targets": ["chatgpt"]})
        self.assertEqual(status, 200)
        mid = body["result"]["mission_id"]
        status, body = post(f"/api/missions/{mid}/message", {"text": "再补一句", "targets": ["claude"]})
        self.assertEqual(status, 200)
        self.assertEqual(body["result"]["targets"], ["claude"])
        pref = self.store.add_preference("偏好一", source="claude")
        self.assertEqual(post(f"/api/preferences/{pref['pref_id']}/delete", {})[0], 200)
        self.assertEqual(post(f"/api/preferences/{pref['pref_id']}/delete", {})[0], 404)


if __name__ == "__main__":
    unittest.main()
