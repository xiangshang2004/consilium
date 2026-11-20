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

    def test_model_used_is_recorded_from_progress_and_completion(self) -> None:
        res = self.daemon.ui_send("hi", ["claude"])
        task = self.store.pickup_inbox("claude")["claimed_tasks"][0]
        self.daemon.dispatcher._dispatch_tool(
            "consilium_report_progress",
            {"task_id": task["task_id"], "peer_id": "claude", "attempt_id": task["attempt_id"],
             "progress_note": "处理中", "model_used": "Claude Opus 5.5"},
        )
        self.assertEqual(self._mission(res["mission_id"])["tasks"][0]["model_used"], "Claude Opus 5.5")
        self.store.complete_task(task["task_id"], "claude", "你好", attempt_id=task["attempt_id"], model_used="Claude Fable 5.2")
        self.assertEqual(self._mission(res["mission_id"])["tasks"][0]["model_used"], "Claude Fable 5.2")

    def test_auto_routing_explains_its_choice_and_sidebar_shows_peers_and_board(self) -> None:
        from consilium.dashboard import render_dashboard_html
        from consilium.mcp_server import explain_auto_choice

        digest = [
            {"peer_id": "claude", "standby": True, "remaining_percent": 78, "exhausted": False},
            {"peer_id": "chatgpt", "standby": False, "remaining_percent": None, "exhausted": False},
            {"peer_id": "antigravity", "standby": False, "remaining_percent": 0, "exhausted": True},
        ]
        why = explain_auto_choice(digest, "claude")
        self.assertIn("交给 Claude：它正在待命，余量 78%", why)
        self.assertIn("Antigravity 余量已耗尽，跳过", why)
        nobody = [dict(d, standby=False, exhausted=False) for d in digest]
        self.assertIn("三端都没在待命", explain_auto_choice(nobody, "claude"))

        res = self.daemon.ui_send("你好", [])
        msg = self._mission(res["mission_id"])["user_messages"][0]
        self.assertEqual(msg["mode"], "auto")
        self.assertTrue(msg["reason"])
        direct = self.daemon.ui_send("你好", ["chatgpt"])
        self.assertEqual(self._mission(direct["mission_id"])["user_messages"][0]["reason"], "")

        task = self.store.pickup_inbox("chatgpt")["claimed_tasks"][0]
        self.store.complete_task(task["task_id"], "chatgpt", "ok", attempt_id=task["attempt_id"], model_used="GPT-5.5 Codex")
        self.assertEqual(self.store.latest_model_by_peer().get("chatgpt"), "GPT-5.5 Codex")
        state = self.daemon.get_dashboard_state(sync_quotas=False)
        self.assertEqual(state["peer_models"].get("chatgpt"), "GPT-5.5 Codex")

        html = render_dashboard_html("0.1.0")
        # R019 redesign: one dock card replaces the peer overview and the separate board.
        for needle in ("renderPeerDock", 'id="peer-dock"', "dock-ring", "data-start-standby", "叫醒",
                       "CODEX_STANDBY_PROMPT", "自动 · ", "今天会诊什么？", "--font-serif",
                       'class="new-row"', "is-new", "data-tier-menu", "conv-actions"):
            self.assertIn(needle, html)
        for gone in ("renderPeerOverview", "renderSideBoard", "平均认领用时", "sidebar-foot", "运行 <strong", "今天完成"):
            self.assertNotIn(gone, html)

    def test_folder_is_chosen_never_guessed_from_text(self) -> None:
        folder = tempfile.mkdtemp(prefix="consilium-r018-ws-", dir="/tmp")
        self.addCleanup(shutil.rmtree, folder, True)
        res = self.daemon.ui_send(f"看下 {folder}/a.py", ["chatgpt"])
        self.assertEqual(self._mission(res["mission_id"])["workspace_path"], "", "paths inside the message are not used")
        res = self.daemon.ui_send("看下这个项目", ["chatgpt"], workspace_hint=folder)
        self.assertEqual(self._mission(res["mission_id"])["workspace_path"], os.path.realpath(folder))
        with self.assertRaises(ValueError):
            self.daemon.ui_send("x", ["chatgpt"], workspace_hint=os.path.join(folder, "不存在"))
        with self.assertRaises(ValueError):
            self.daemon.ui_send("x", ["chatgpt"], workspace_hint="relative/path")

    def test_one_conversation_one_native_session_title_lock_and_tier(self) -> None:
        folder = tempfile.mkdtemp(prefix="consilium-r018-ns-", dir="/tmp")
        self.addCleanup(shutil.rmtree, folder, True)
        res = self.daemon.ui_send("整理一下日志", ["claude"], workspace_hint=folder, tier="deep")
        mid = res["mission_id"]
        t1 = self.store.pickup_inbox("claude")["claimed_tasks"][0]
        self.assertEqual(t1["prompt"], "整理一下日志", "the user's words arrive verbatim")
        self.assertEqual(t1["new_session"], 1)
        self.assertEqual(t1["native_session"], "")
        self.assertEqual(t1["workspace_path"], os.path.realpath(folder))
        self.assertEqual(t1["mission_tier"], "pro_reasoning")
        self.assertEqual(t1["model_tier"], "pro_reasoning")
        out = self.daemon.dispatcher._dispatch_tool(
            "consilium_bind_session", {"mission_id": mid, "peer_id": "claude", "session_ref": "local_abc"}
        )
        self.assertEqual(out["result"]["session_ref"], "local_abc")
        self.store.complete_task(t1["task_id"], "claude", "好", attempt_id=t1["attempt_id"])

        self.daemon.ui_send("再看看昨天的", ["claude"], mid)
        t2 = self.store.pickup_inbox("claude")["claimed_tasks"][0]
        self.assertEqual(t2["new_session"], 0)
        self.assertEqual(t2["native_session"], "local_abc")
        convo = t2["conversation"]
        self.assertEqual([c["from"] for c in convo], ["user", "claude", "user"])
        self.assertEqual(convo[0]["text"], "整理一下日志")
        self.assertEqual(convo[1]["text"], "好")
        self.assertEqual(t2["model_tier"], "pro_reasoning")

        self.daemon.dispatcher._dispatch_tool("consilium_set_title", {"mission_id": mid, "peer_id": "claude", "title": "日志整理"})
        self.assertEqual(self._mission(mid)["title"], "日志整理")
        self.store.set_mission_title(mid, "我的日志", actor=USER_PEER)
        locked = self.store.set_mission_title(mid, "被覆盖", actor="claude")
        self.assertTrue(locked.get("locked"))
        self.assertEqual(self._mission(mid)["title"], "我的日志")
        with self.assertRaises(ValueError):
            self.store.set_mission_title(mid, "x" * 41, actor=USER_PEER)

        self.store.set_mission_archived(mid, True)
        self.assertTrue(self._mission(mid)["archived"])
        self.store.set_mission_archived(mid, False)
        self.assertFalse(self._mission(mid)["archived"])

    def test_http_rename_and_archive_routes_require_csrf(self) -> None:
        host, port = self.daemon.start()
        token = read_auth_token(self.tmp_state)
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("POST", "/api/login", body=json.dumps({"token": token}), headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        login = json.loads(resp.read())
        cookie = resp.getheader("Set-Cookie").split(";", 1)[0]
        conn.close()
        mid = self.daemon.ui_send("hi", ["chatgpt"])["mission_id"]

        def post(path: str, body: dict, csrf: bool = True) -> int:
            c = http.client.HTTPConnection(host, port, timeout=5)
            headers = {"Content-Type": "application/json", "Cookie": cookie, "Origin": f"http://{host}:{port}"}
            if csrf:
                headers["X-Consilium-CSRF"] = login["csrf_token"]
            c.request("POST", path, body=json.dumps(body), headers=headers)
            r = c.getresponse()
            r.read()
            c.close()
            return r.status

        self.assertEqual(post(f"/api/missions/{mid}/title", {"title": "新名字"}, csrf=False), 403)
        self.assertEqual(post(f"/api/missions/{mid}/title", {"title": "新名字"}), 200)
        self.assertEqual(self._mission(mid)["title"], "新名字")
        self.assertEqual(post(f"/api/missions/{mid}/archive", {}), 200)
        self.assertTrue(self._mission(mid)["archived"])
        self.assertEqual(post(f"/api/missions/{mid}/unarchive", {}), 200)
        self.assertEqual(post("/api/missions/M-NOPE/archive", {}), 404)

    def test_ui_picks_folders_and_never_asks_for_typed_paths(self) -> None:
        from consilium.dashboard import render_dashboard_html

        html = render_dashboard_html("0.1.0")
        for needle in ("data-folder-menu", "data-open-folder-picker", "选择文件夹…", "pickFolder", "__consiliumFolderPicked",
                       "data-tier", "data-rename-mission", "data-archive-mission", "data-dedicated-session", "在 Claude 里开专属会话", "folder-group", "已归档",
                       "data-start-standby", "让它们待命"):
            self.assertIn(needle, html)
        for gone in ("compose-ws-input", "data-edit-workspace", "新开会话处理", "工作区绝对路径", "/Users/…/项目文件夹"):
            self.assertNotIn(gone, html)
        with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                               "app", "macos", "Sources", "MainWindowController.swift"), "r", encoding="utf-8") as h:
            swift = h.read()
        self.assertIn("NSOpenPanel", swift)
        self.assertIn("canCreateDirectories = true", swift)
        self.assertIn('host == "127.0.0.1"', swift)

    def test_cockpit_current_account_is_confirmed_for_codex_antigravity_and_claude(self) -> None:
        import builtins
        import hashlib
        import sqlite3
        import time
        from unittest import mock

        from consilium.quota import sync_local_quotas

        home = tempfile.mkdtemp(prefix="consilium-r020-hm-", dir="/tmp")
        self.addCleanup(shutil.rmtree, home, True)
        cp = os.path.join(home, ".antigravity_cockpit")
        os.makedirs(os.path.join(cp, "cache", "quota_api_v1_desktop", "authorized"))
        os.makedirs(os.path.join(cp, "accounts"))
        now = int(time.time())

        # Codex: JSON index is OLDER than the SQLite index, pointer says uid-b, Cockpit binds uid-b,
        # and uid-b has the freshest samples.
        with open(os.path.join(cp, "codex_accounts.json"), "w", encoding="utf-8") as h:
            json.dump({"accounts": [{"id": "uid-a", "email": "a@example.com", "plan_type": "plus"},
                                    {"id": "uid-b", "email": "b@example.com", "plan_type": "pro"}],
                       "current_account_id": "uid-b"}, h)
        old = now - 86400
        os.utime(os.path.join(cp, "codex_accounts.json"), (old, old))
        with open(os.path.join(cp, "codex_accounts.sqlite"), "w", encoding="utf-8") as h:
            h.write("VAULT")
        with open(os.path.join(cp, "codex_instances.json"), "w", encoding="utf-8") as h:
            json.dump({"instances": [], "defaultSettings": {"bindAccountId": "uid-b"}}, h)
        db = sqlite3.connect(os.path.join(cp, "codex-quota-history-v1.sqlite"))
        db.execute("CREATE TABLE quota_samples (account_id TEXT, identity TEXT, minute INTEGER, observed_at INTEGER, short_remaining INTEGER, weekly_remaining INTEGER)")
        db.executemany("INSERT INTO quota_samples VALUES (?,?,?,?,?,?)", [
            ("uid-a", "ia", 1, now - 7200, 10, 20), ("uid-b", "ib", 2, now - 120, 88, 91)])
        db.commit(); db.close()

        # Antigravity: three accounts with per-model windows; the current email's cache file was
        # written at the same moment as account ag-2's newest observation.
        email = "me@example.com"
        with open(os.path.join(cp, "current_account.json"), "w", encoding="utf-8") as h:
            json.dump({"email": email, "updated_at": now - 600}, h)
        cache = os.path.join(cp, "cache", "quota_api_v1_desktop", "authorized", hashlib.sha256(email.encode()).hexdigest() + ".json")
        with open(cache, "w", encoding="utf-8") as h:
            h.write('{"secret":"DO_NOT_READ"}')
        os.utime(cache, (now - 60, now - 60))
        with open(os.path.join(cp, "accounts", "antigravity_x.json"), "w", encoding="utf-8") as h:
            h.write('{"ciphertext":"DO_NOT_READ"}')
        db = sqlite3.connect(os.path.join(cp, "platform-quota-history-v1.sqlite"))
        db.execute("CREATE TABLE quota_observations (platform TEXT, account_id TEXT, account_created_at INTEGER, metric TEXT, label TEXT, observed_at INTEGER, remaining INTEGER, reset_at INTEGER)")
        rows = []
        for acct, ts, five, week in (("ag-1", now - 5000, 70, 80), ("ag-2", now - 60, 35, 42), ("ag-3", now - 900, 90, 95)):
            rows += [("antigravity", acct, 0, "gemini-5h", "5h", ts, five, now + 3600),
                     ("antigravity", acct, 0, "gemini-weekly", "weekly", ts, week, now + 86400),
                     ("antigravity", acct, 0, "gemini-4-neon-high", "per-model", ts, 0, now + 3600)]
        db.executemany("INSERT INTO quota_observations VALUES (?,?,?,?,?,?,?,?)", rows)
        db.commit(); db.close()

        # Claude: one account, plan only.
        with open(os.path.join(cp, "claude_accounts.json"), "w", encoding="utf-8") as h:
            json.dump({"accounts": [{"id": "cl-1", "email": "c@example.com", "plan_type": "max"}]}, h)

        opened = []
        real_open = builtins.open

        def spy(file, *a, **k):
            opened.append(os.path.abspath(os.fspath(file)) if not isinstance(file, int) else str(file))
            return real_open(file, *a, **k)

        with mock.patch("builtins.open", side_effect=spy):
            sync_local_quotas(self.store, user_home=home)
        for forbidden in (cache, os.path.join(cp, "accounts", "antigravity_x.json"), os.path.join(cp, "codex_accounts.sqlite")):
            self.assertNotIn(os.path.abspath(forbidden), opened)

        peers = {q["peer_id"]: q for q in self.store.list_quota_windows()["peers"]}
        cx = peers["chatgpt"]
        self.assertFalse(cx["current_account_unknown"])
        self.assertEqual(cx["min_remaining_percent"], 88.0)
        self.assertEqual(cx["active_plan_type"], "pro")
        ag = peers["antigravity"]
        self.assertFalse(ag["current_account_unknown"])
        self.assertEqual(ag["min_remaining_percent"], 35.0, "headline is the account-level limit, not a per-model 0")
        self.assertFalse(ag["exhausted"])
        self.assertEqual(sorted(w["window_key"] for w in ag["windows"]), ["gemini-5h", "gemini-weekly"])
        cl = peers["claude"]
        self.assertEqual(cl["active_plan_type"], "max")
        self.assertIsNone(cl["min_remaining_percent"])
        serialized = json.dumps(peers, ensure_ascii=False)
        for leak in ("example.com", "uid-b", "ag-2", "cl-1"):
            self.assertNotIn(leak, serialized)

        # Cockpit refreshes Codex about hourly: an older record of the confirmed account is
        # still shown, marked with its age, and no longer drives dispatch.
        db = sqlite3.connect(os.path.join(cp, "codex-quota-history-v1.sqlite"))
        db.execute("UPDATE quota_samples SET observed_at = ?", (now - 7200,))
        db.commit(); db.close()
        from consilium.store import BusStore
        st2 = tempfile.mkdtemp(prefix="consilium-r021-st-", dir="/tmp")
        self.addCleanup(shutil.rmtree, st2, True)
        store2 = BusStore(st2, rendezvous_dir=self.tmp_rv)  # the bus keeps the newest record it has seen
        sync_local_quotas(store2, user_home=home)
        cx2 = next(q for q in store2.list_quota_windows()["peers"] if q["peer_id"] == "chatgpt")
        self.assertFalse(cx2["current_account_unknown"])
        self.assertEqual(cx2["min_remaining_percent"], 88.0)
        self.assertTrue(cx2["quota_stale"])
        self.assertGreaterEqual(cx2["sample_age_seconds"], 7200)
        self.assertIsNone(cx2["dispatch_recommended"])

        # Without corroboration (not the bound account, not the most recently refreshed one)
        # the stale pointer is not trusted.
        os.remove(os.path.join(cp, "codex_instances.json"))
        db = sqlite3.connect(os.path.join(cp, "codex-quota-history-v1.sqlite"))
        db.execute("UPDATE quota_samples SET observed_at = ? WHERE account_id != 'uid-b'", (now - 60,))
        db.commit(); db.close()
        sync_local_quotas(self.store, user_home=home)
        cx3 = next(q for q in self.store.list_quota_windows()["peers"] if q["peer_id"] == "chatgpt")
        self.assertTrue(cx3["current_account_unknown"])
        self.assertIsNone(cx3["min_remaining_percent"])

        # Antigravity: two accounts refreshed a minute apart are still told apart by the nearest
        # cache-file time; when they are too close to call, the current account stays unknown.
        ag_ids = [r[0] for r in sqlite3.connect(os.path.join(cp, "platform-quota-history-v1.sqlite")).execute(
            "SELECT DISTINCT account_id FROM quota_observations WHERE platform = 'antigravity'")]
        self.assertGreaterEqual(len(ag_ids), 2)
        cur_ag = next(a for a in ag_ids if a == "ag-2") if "ag-2" in ag_ids else ag_ids[0]
        other = next(a for a in ag_ids if a != cur_ag)
        cache_dir = os.path.join(cp, "cache", "quota_api_v1_desktop", "authorized")
        cache_file = os.path.join(cache_dir, os.listdir(cache_dir)[0])

        def set_ag(cur_age: float, other_age: float) -> dict:
            db2 = sqlite3.connect(os.path.join(cp, "platform-quota-history-v1.sqlite"))
            db2.execute("UPDATE quota_observations SET observed_at = ? WHERE account_id = ?", (int((now - cur_age) * 1000), cur_ag))
            db2.execute("UPDATE quota_observations SET observed_at = ? WHERE account_id = ?", (int((now - other_age) * 1000), other))
            db2.commit(); db2.close()
            os.utime(cache_file, (now - cur_age, now - cur_age))
            sync_local_quotas(self.store, user_home=home)
            return next(q for q in self.store.list_quota_windows()["peers"] if q["peer_id"] == "antigravity")

        self.assertFalse(set_ag(40, 130)["current_account_unknown"], "90 s apart is clearly the nearer account")
        self.assertTrue(set_ag(40, 44)["current_account_unknown"], "4 s apart is too close to call")


if __name__ == "__main__":
    unittest.main()
