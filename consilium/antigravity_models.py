"""Read the model IDs Antigravity recorded for an exact Consilium attempt."""

from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

Attempt = Tuple[str, str]
_REPORT_NAMES = {"consilium_report_progress", "consilium_complete_task"}
_PLANNERS = {
    "gemini-pro-orchestrator-neon": "Gemini 4 Neon",
    "gemini-pro-orchestrator-carbon": "Gemini 4 Carbon",
    "gemini-pro-orchestrator-argon": "Gemini 4 Argon",
}
_WORKER = "gemini-flash-worker-agy"


def _report_attempt(call: Dict[str, Any]) -> Optional[Attempt]:
    name = call.get("name")
    args = call.get("args")
    if not isinstance(args, dict):
        return None
    if name == "call_mcp_tool":
        operation = args.get("ToolName")
        if args.get("ServerName") != "consilium" or operation not in _REPORT_NAMES:
            return None
        args = args.get("Arguments")
    elif name not in ("mcp_consilium_consilium_report_progress", "mcp_consilium_consilium_complete_task"):
        return None
    if not isinstance(args, dict) or args.get("peer_id") != "antigravity":
        return None
    task = args.get("task_id")
    attempt = args.get("attempt_id")
    if isinstance(task, str) and isinstance(attempt, str) and task and attempt:
        return task, attempt
    return None


@lru_cache(maxsize=128)
def _attempt_models(path: Path, modified_ns: int, size: int) -> Dict[Attempt, Tuple[str, ...]]:
    """Cache each immutable file version and retain only model IDs and attempt keys."""
    timeline = []
    reports: Dict[Attempt, list[int]] = {}
    with path.open(encoding="utf-8", errors="replace") as stream:
        for line in stream:
            if '"model"' not in line or '"PLANNER_RESPONSE"' not in line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if not isinstance(row, dict) or row.get("source") != "MODEL" or row.get("type") != "PLANNER_RESPONSE":
                continue
            model = row.get("model")
            if not isinstance(model, str) or not model.strip():
                continue
            for call in row.get("tool_calls") or []:
                key = _report_attempt(call) if isinstance(call, dict) else None
                if key is not None:
                    reports.setdefault(key, []).append(len(timeline))
            timeline.append(model)

    spans = {
        key: (indexes[0], indexes[-1])
        for key, indexes in reports.items()
    }
    result = {}
    for key, indexes in reports.items():
        start, end = indexes[0], indexes[-1]
        own_steps = set(indexes)
        other_spans = [
            (first, last) for other, (first, last) in spans.items()
            if other != key and first <= end and last >= start
        ]
        model_ids = (
            timeline[index] for index in range(start, end + 1)
            if index in own_steps or not any(first <= index <= last for first, last in other_spans)
        )
        result[key] = tuple(dict.fromkeys(model_ids))
    return result


def _display_models(models: Iterable[str]) -> str:
    ids = tuple(dict.fromkeys(models))
    if ids == (_WORKER,):
        return "Gemini 4 Hydrogen（执行）"
    planners = [model for model in ids if model in _PLANNERS]
    if len(planners) == 1 and _WORKER in ids:
        # Google documents Hydrogen as the implementation model for these variants.
        paired = _PLANNERS[planners[0]] + "（规划） · Gemini 4 Hydrogen（执行）"
        return " · ".join([paired] + [model for model in ids if model not in (planners[0], _WORKER)])
    return " · ".join(_PLANNERS.get(model, model) for model in ids)


def models_for_tasks(
    tasks: Iterable[Dict[str, Any]], brain_dir: Optional[Path] = None
) -> Dict[Attempt, str]:
    """Look up only reports whose task and lease match dashboard tasks."""
    attempts = {}
    for task in tasks:
        if task.get("assignee_peer") != "antigravity" or not task.get("attempt_id"):
            continue
        claimed_at = task.get("claimed_at") or ""
        try:
            claimed = datetime.fromisoformat(claimed_at.replace("Z", "+00:00")).timestamp()
        except ValueError:
            claimed = 0
        attempts[(task["task_id"], task["attempt_id"])] = claimed
    if not attempts:
        return {}

    root = brain_dir if brain_dir is not None else Path.home() / ".gemini" / "antigravity" / "brain"
    earliest = min(attempts.values())
    observed: Dict[Attempt, list] = {}
    try:
        for logs in root.glob("*/.system_generated/logs"):
            full = logs / "transcript_full.jsonl"
            path = full if full.is_file() else logs / "transcript.jsonl"
            try:
                stat = path.stat()
                if stat.st_mtime < earliest:
                    continue
                values = _attempt_models(path, stat.st_mtime_ns, stat.st_size)
            except OSError:
                continue
            for key in attempts.keys() & values.keys():
                if stat.st_mtime >= attempts[key]:
                    observed.setdefault(key, []).extend(values[key])
    except OSError:
        pass
    return {key: _display_models(ids) for key, ids in observed.items()}
