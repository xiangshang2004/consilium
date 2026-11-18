---
name: deskbell-collab
description: Cross-desktop AI collaboration protocol (ChatGPT/Codex, Claude, Antigravity) using local deskbell MCP and native per-app subagent harnesses without LLM CLI wrappers.
---

# `deskbell` Tri-Desktop Collaboration Protocol

You are one of three desktop AI peers sharing a single local collaboration bus (`deskbell`):
- `chatgpt`: `ChatGPT.app` / Codex Desktop (`com.openai.codex`)
- `claude`: `Claude.app` (`com.anthropic.claudefordesktop`)
- `antigravity`: `Antigravity.app` (`com.google.antigravity`)

## 0. Project Workflow & Worktree Rules (Highest Priority)
1. **Repository Rules Always Win**: Before creating missions or editing code in a repository that defines its own collaboration workflow (e.g., `captalent-collab` / `collab` branch claim tables, `/Users/up/code/AGENTS.md`, or `/Users/up/code/README.md`), follow that project's workflow first. `deskbell` is only the doorbell/coordination bus and **never replaces a project's claim table**.
2. **Branch Prefix Mapping**:
   - `claude` -> `claude/<topic>-<YYYYMMDD>`
   - `chatgpt` -> `codex/<topic>-<YYYYMMDD>`
   - `antigravity` -> `gemini/<topic>-<YYYYMMDD>`
3. **Worktree Isolation & Cleanup**:
   - Use each app's built-in worktree mechanism when editing code; never create non-compliant directories under `/Users/up/code`.
   - Workers report their branch name and full commit SHA upward, and **never** merge, push, or delete branches/worktrees on their own. The coordinator merges and cleans up only after user confirmation.

## 1. Coordinator Mode (When the User Issues a Cross-Desktop Order Here)
Whenever the user gives an instruction here that asks to coordinate across models/apps (or says "让 Claude/ChatGPT 一起看", "多端协作", "按各自模型分派"):
1. Inspect active peers, standby listeners, and quota windows first via `deskbell_status()` (or `deskbell_quota()`).
   - Each peer entry in `quotas` exposes `min_remaining_percent`, `exhausted`, `dispatch_recommended`, `earliest_reset_at`, `earliest_reset_in_human`, and per-window details (`5h`, `7d`, model groups).
   - Avoid dispatching heavy tasks to any peer with `dispatch_recommended == false`; instead route to peers with headroom or inform the user of the peer's remaining quota and `earliest_reset_in_human` (`reset_at`).
2. Decompose the goal into clear, bounded tasks assigned to the best peer + `model_tier` (taking remaining quota headroom into account):
   - `pro_reasoning`: architecture design, hard root-cause debugging, adversarial audit (`opus` / `fable` plan audit / high-reasoning Codex / Gemini `pro`).
   - `balanced`: feature implementation, refactoring, structured test writing (`opus` in Claude / balanced Codex / default Gemini).
   - `fast`: codebase scan, doc lookup, quick verification (`opus` in Claude / fast Codex / Gemini `flash` or `flash_lite`).
3. Enforce isolation: default `isolation_policy: "worktree"` for code edits, or `"read_only"` for audits/research. Never let two peers edit the same working tree simultaneously.
4. Call `deskbell_create_mission(...)` with `coordinator_peer` set to your own peer ID and `max_rounds: 3` (hard loop guard).
5. Check `deliveries`:
   - `hot_wakeup`: target desktop standby session woke automatically (with cold escalation backup if unacked).
   - `cold_notification`: target desktop had no active waiter; briefly tell the user a macOS doorbell notification was sent (with deep link) so they can press Return once if they want that cold app to join.
6. Wait for results cleanly:
   - In **Antigravity / Claude Code tab / Codex**: run `python3 -m deskbell.cli wait --peer <self>` in the background (without `--pickup`), or call `deskbell_wait(peer_id=<self>, timeout_sec=45)` in request-bounded surfaces (such as Claude Chat tab).
7. Once `all_tasks_finished` is true (or `loop_guard_reached` escalates back), synthesize a single unified report for the user citing exact files, test results, and worktree branches, and close the mission with `deskbell_close_mission(mission_id, peer_id=<self>, final_summary=...)`.

## 2. Standby Worker Mode (When Acting as a Background Doorbell Listener)
1. **Surface Rules**:
   - **Claude.app**: Only the **Code tab** can stay on background standby (`run_in_background: true`). The **Chat tab** cannot run background listeners and participates only as coordinator or via cold deep link.
   - Start background standby **WITHOUT `--pickup`**:
     `python3 -m deskbell.cli wait --peer <self> --session-label <label>`
     (Waiter watches `/tmp/deskbell-<uid>/signals/<peer>.seq` with 0% CPU and does not touch SQLite; if `--pickup` ran in the background process and the session closed, tasks could get stranded in `claimed`).
2. On wakeup, call `deskbell_pickup_inbox(peer_id=<self>, session_label=<label>)` to atomically claim tasks, then fan out **inside your own native harness**:
   - **Antigravity**: call `invoke_subagent` with `Model: "pro" | "flash" | "flash_lite"` and `Workspace: "share" | "branch"` (or `swarm`).
   - **Claude Desktop**: invoke native `Agent` subagents using `opus-architect` (or `fable` for plan review), `opus-builder`, or `opus-scout` with worktree isolation when editing code. Do **not** use `sonnet` or `haiku`.
   - **ChatGPT / Codex Desktop**: spawn native Codex child agents using `architect-high`, `builder-balanced`, or `scout-fast` profiles.
3. Send milestone updates only when needed via `deskbell_report_progress(...)`.
4. Never enter an unguarded ping-pong loop: cross-peer checks MUST go through `deskbell_request_review(...)`, and if `allowed == false`, stop and hand control back to the coordinator.
5. Finish by calling `deskbell_complete_task(task_id, peer_id=<self>, result_summary=..., artifacts=[...])`, then re-arm your standby waiter silently if continuing standby duty.
