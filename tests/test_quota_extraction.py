"""Tests for multi-source local quota & reset-time extraction and MCP quota tool."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
import unittest

from deskbell.mcp_server import DeskBellMCPDispatcher
from deskbell.quota import sync_local_quotas
from deskbell.store import BusStore


class QuotaExtractionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_bus = tempfile.mkdtemp(prefix="dbell-qbus-", dir="/tmp")
        self.tmp_user_home = tempfile.mkdtemp(prefix="dbell-qhome-", dir="/tmp")
        self._prev_user_home = os.environ.get("DESKBELL_USER_HOME")
        os.environ["DESKBELL_USER_HOME"] = self.tmp_user_home
        self.store = BusStore(self.tmp_bus)

    def tearDown(self) -> None:
        if self._prev_user_home is None:
            os.environ.pop("DESKBELL_USER_HOME", None)
        else:
            os.environ["DESKBELL_USER_HOME"] = self._prev_user_home
        shutil.rmtree(self.tmp_bus, ignore_errors=True)
        shutil.rmtree(self.tmp_user_home, ignore_errors=True)

    def test_sync_local_quotas_across_cockpit_codex_and_claude(self) -> None:
        now_unix = int(time.time())
        future_reset = now_unix + 7200

        # 1. Mock Cockpit Tools platform-quota-history-v1.sqlite
        cockpit_dir = os.path.join(self.tmp_user_home, ".antigravity_cockpit")
        os.makedirs(cockpit_dir, mode=0o700, exist_ok=True)
        platform_db = os.path.join(cockpit_dir, "platform-quota-history-v1.sqlite")
        with sqlite3.connect(platform_db) as conn:
            conn.execute(
                """
                CREATE TABLE quota_observations (
                    platform TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    account_created_at INTEGER NOT NULL,
                    metric TEXT NOT NULL,
                    label TEXT NOT NULL,
                    observed_at INTEGER NOT NULL,
                    remaining INTEGER NOT NULL,
                    reset_at INTEGER,
                    PRIMARY KEY(platform, account_id, account_created_at, metric, observed_at)
                ) WITHOUT ROWID
                """
            )
            conn.execute(
                "INSERT INTO quota_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "antigravity",
                    "ag-acct",
                    1,
                    "gemini_pro",
                    "Gemini Pro",
                    now_unix - 10,
                    72,
                    future_reset,
                ),
            )
            conn.execute(
                "INSERT INTO quota_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "claude",
                    "claude-acct",
                    1,
                    "5h",
                    "5h rolling",
                    now_unix - 20,
                    45,
                    future_reset + 1800,
                ),
            )

        # 2. Mock Codex rollout-*.jsonl with rate_limits event
        codex_sess_dir = os.path.join(
            self.tmp_user_home, ".codex", "sessions", "2026", "11", "18"
        )
        os.makedirs(codex_sess_dir, mode=0o700, exist_ok=True)
        rollout_path = os.path.join(codex_sess_dir, "rollout-test.jsonl")
        with open(rollout_path, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "timestamp": now_unix,
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "rate_limits": {
                                "limit_id": "codex-plus",
                                "primary": {
                                    "used_percent": 22.5,
                                    "resets_at": future_reset,
                                },
                                "secondary": {
                                    "used_percent": 60.0,
                                    "resets_at": future_reset + 86400,
                                },
                            },
                        },
                    }
                )
                + "\n"
            )

        # 3. Mock Claude local usage-latest.json showing exhausted 5h window
        claude_dir = os.path.join(self.tmp_user_home, ".claude")
        os.makedirs(claude_dir, mode=0o700, exist_ok=True)
        with open(
            os.path.join(claude_dir, "usage-latest.json"), "w", encoding="utf-8"
        ) as handle:
            json.dump(
                {
                    "account_id": "claude-acct",
                    "observed_at": now_unix,
                    "five_hour": {
                        "remaining": 0.0,
                        "resets_at": future_reset + 3600,
                    },
                },
                handle,
            )

        summary = sync_local_quotas(self.store, user_home=self.tmp_user_home)
        by_peer = {p["peer_id"]: p for p in summary["peers"]}

        ag = by_peer["antigravity"]
        self.assertEqual(ag["min_remaining_percent"], 72.0)
        self.assertTrue(ag["dispatch_recommended"])
        self.assertFalse(ag["exhausted"])
        self.assertEqual(ag["windows"][0]["source"], "cockpit_sqlite")
        self.assertNotEqual(ag["windows"][0]["reset_in_human"], "")

        gpt = by_peer["chatgpt"]
        self.assertEqual(gpt["min_remaining_percent"], 40.0)
        self.assertTrue(gpt["dispatch_recommended"])
        gpt_windows = {w["window_key"]: w for w in gpt["windows"]}
        self.assertEqual(gpt_windows["5h"]["remaining_percent"], 77.5)
        self.assertEqual(gpt_windows["7d"]["remaining_percent"], 40.0)
        self.assertEqual(gpt_windows["5h"]["source"], "codex_rollout")

        cl = by_peer["claude"]
        self.assertEqual(cl["min_remaining_percent"], 0.0)
        self.assertTrue(cl["exhausted"])
        self.assertFalse(cl["dispatch_recommended"])

    def test_mcp_deskbell_quota_manual_report_and_status(self) -> None:
        dispatcher = DeskBellMCPDispatcher(home_dir=self.tmp_bus)
        resp = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {
                    "name": "deskbell_quota",
                    "arguments": {
                        "sync_local": False,
                        "peer_id": "chatgpt",
                        "report_windows": [
                            {
                                "peer_id": "chatgpt",
                                "account_id": "plus-main",
                                "window_key": "5h",
                                "label": "5h rolling",
                                "remaining_percent": 12.0,
                                "reset_at": "2099-01-01T00:00:00Z",
                            }
                        ],
                    },
                },
            }
        )
        payload = resp["result"]["structuredContent"]
        self.assertEqual(len(payload["peers"]), 1)
        peer_info = payload["peers"][0]
        self.assertEqual(peer_info["min_remaining_percent"], 12.0)
        self.assertTrue(peer_info["low_quota"])
        self.assertTrue(peer_info["dispatch_recommended"])
        self.assertEqual(peer_info["windows"][0]["source"], "manual_report")

        status_resp = dispatcher.handle_payload(
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {"name": "deskbell_status", "arguments": {}},
            }
        )
        status_payload = status_resp["result"]["structuredContent"]
        self.assertIn("quotas", status_payload)
        self.assertEqual(len(status_payload["quotas"]), 3)


if __name__ == "__main__":
    unittest.main()
