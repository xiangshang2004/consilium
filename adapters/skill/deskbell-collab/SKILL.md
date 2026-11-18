---
name: deskbell-collab
description: Cross-desktop AI collaboration protocol (ChatGPT/Codex, Claude, Antigravity) using local deskbell MCP and native per-app subagent harnesses without LLM CLI wrappers.
---

# `deskbell` Tri-Desktop Collaboration Protocol

You are one of three desktop AI peers sharing a single local collaboration bus (`deskbell`):
- `chatgpt`: `ChatGPT.app` / Codex Desktop (`com.openai.codex`)
- `claude`: `Claude.app` (`com.anthropic.claudefordesktop`)
- `antigravity`: `Antigravity.app` (`com.google.antigravity`)

## 1. Coordinator Mode (When the User Issues a Cross-Desktop Order Here)
Whenever the user gives an instruction here that asks to coordinate across models/apps (or says "让 Claude/ChatGPT 一起看", "多端协作", "按各自模型分派"):
1. Inspect active peers, standby listeners, and quota windows first via `deskbell_status()` (or `deskbell_quota()`).
   - Each peer entry in `quotas` exposes `min_remaining_percent`, `exhausted`, `dispatch_recommended`, `earliest_reset_at`, `earliest_reset_in_human`, and per-window details (`5h`, `7d`, model groups).
   - Avoid dispatching heavy tasks to any peer with `dispatch_recommended == false`; instead route to peers with headroom or inform the user of the peer's remaining quota and `earliest_reset_in_human` (`reset_at`).
2. Decompose the goal into clear, bounded tasks assigned to the best peer + `model_tier` (taking remaining quota headroom into account):
   - `pro_reasoning`: architecture design, hard root-cause debugging, adversarial audit (`opus` / high-reasoning Codex / Gemini `pro`).
   - `balanced`: feature implementation, refactoring, structured test writing (`sonnet` / balanced Codex / default Gemini).
   - `fast`: codebase scan, doc lookup, quick verification (`haiku` / fast Codex / Gemini `flash` or `flash_lite`).
3. Enforce isolation: default `isolation_policy: "worktree"` for code edits, or `"read_only"` for audits/research. Never let two peers edit the same working tree simultaneously.
4. Call `deskbell_create_mission(...)` with `coordinator_peer` set to your own peer ID and `max_rounds: 3` (hard loop guard).
5. Check `deliveries`:
   - `hot_wakeup`: target desktop standby session woke automatically.
   - `cold_notification`: target desktop had no active waiter; briefly tell the user a macOS doorbell notification was sent (with deep link) so they can press Return once if they want that cold app to join.
6. Wait for results cleanly using `deskbell_wait(peer_id=<self>, timeout_sec=300)` or a background `python3 -m deskbell.cli wait --peer <self> --pickup` task (zero tokens while sleeping).
7. Once `all_tasks_finished` is true (or `loop_guard_reached` escalates back), synthesize a single unified report for the user citing exact files, test results, and worktree branches.

## 2. Standby Worker Mode (When Acting as a Background Doorbell Listener)
1. Start a single silent waiter in the background or via `deskbell_wait`:
   - Antigravity / Desktop background task: `python3 -m deskbell.cli wait --peer <self> --pickup` (exits 0 with claimed tasks JSON as soon as the doorbell rings).
2. On wakeup, inspect `claimed_tasks` and fan out **inside your own native harness** according to `model_tier` and `parallel_fanout`:
   - **Antigravity**: call `invoke_subagent` with `Model: "pro" | "flash" | "flash_lite"` and `Workspace: "share" | "branch"` (or `swarm`).
   - **Claude Desktop**: invoke native `Agent` subagents using `opus-architect`, `sonnet-builder`, or `haiku-scout` with worktree isolation when editing code.
   - **ChatGPT / Codex Desktop**: spawn native Codex child agents using `architect-high`, `builder-balanced`, or `scout-fast` profiles.
3. Send milestone updates only when needed via `deskbell_report_progress(...)`.
4. Never enter an unguarded ping-pong loop: cross-peer checks MUST go through `deskbell_request_review(...)`, and if `allowed == false`, stop and hand control back to the coordinator.
5. Finish by calling `deskbell_complete_task(task_id, peer_id=<self>, result_summary=..., artifacts=[...])`, then re-arm your standby waiter if continuing standby duty.
