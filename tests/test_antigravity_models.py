from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from consilium.antigravity_models import models_for_tasks


def _step(model, task=None, attempt=None, complete=False):
    calls = []
    if task:
        calls.append({
            "name": "call_mcp_tool",
            "args": {
                "ServerName": "consilium",
                "ToolName": "consilium_complete_task" if complete else "consilium_report_progress",
                "Arguments": {"peer_id": "antigravity", "task_id": task, "attempt_id": attempt},
            },
        })
    return {"type": "PLANNER_RESPONSE", "source": "MODEL", "model": model, "tool_calls": calls}


class AntigravityModelTests(unittest.TestCase):
    def test_matches_attempt_and_excludes_models_from_an_interleaved_task(self):
        with tempfile.TemporaryDirectory() as temp:
            brain = Path(temp)
            logs = brain / "session" / ".system_generated" / "logs"
            logs.mkdir(parents=True)
            transcript = logs / "transcript_full.jsonl"
            worker = "gemini-flash-worker-agy"
            transcript.write_text("\n".join(json.dumps(step) for step in [
                _step(worker, "T-1", "A-1"),
                _step("gemini-pro-orchestrator-neon"),
                _step("gemini-other", "T-2", "A-1"),
                _step("gemini-other", "T-2", "A-1", complete=True),
                _step(worker, "T-1", "A-1", complete=True),
                _step("gemini-4-argon", "T-1", "A-2"),
            ]) + "\n{", encoding="utf-8")
            results = models_for_tasks([
                {"assignee_peer": "antigravity", "task_id": "T-1", "attempt_id": "A-1"},
                {"assignee_peer": "antigravity", "task_id": "T-1", "attempt_id": "A-2"},
                {"assignee_peer": "antigravity", "task_id": "T-1", "attempt_id": "unreported"},
                {"assignee_peer": "chatgpt", "task_id": "T-2", "attempt_id": "A-1"},
            ], brain_dir=brain)
            self.assertEqual(results, {
                ("T-1", "A-1"): "Gemini 4 Neon（规划） · Gemini 4 Hydrogen（执行）",
                ("T-1", "A-2"): "gemini-4-argon",
            })
