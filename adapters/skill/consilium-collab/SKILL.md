---
name: consilium-collab
description: Cross-desktop AI collaboration protocol (ChatGPT/Codex, Claude, Antigravity) using local consilium MCP and native per-app subagent harnesses without LLM CLI wrappers.
---

# `consilium` Tri-Desktop Collaboration Protocol

You are one of three desktop AI peers sharing a single local collaboration bus (`consilium`):
- `chatgpt`: `ChatGPT.app` / Codex Desktop (`com.openai.codex`)
- `claude`: `Claude.app` (`com.anthropic.claudefordesktop`)
- `antigravity`: `Antigravity.app` (`com.google.antigravity`)

## 0. Project Workflow & Worktree Rules (Highest Priority)
1. **Repository Rules Always Win**: Before creating missions or editing code in a repository that defines its own collaboration workflow (e.g., `captalent-collab` / `collab` branch claim tables, `/Users/up/code/AGENTS.md`, or `/Users/up/code/README.md`), follow that project's workflow first. `consilium` is only the doorbell/coordination bus and **never replaces a project's claim table**.
2. **Branch Prefix Mapping**:
   - `claude` -> `claude/<topic>-<YYYYMMDD>`
   - `chatgpt` -> `codex/<topic>-<YYYYMMDD>`
   - `antigravity` -> `gemini/<topic>-<YYYYMMDD>`
3. **Worktree Isolation & Cleanup**:
   - Use each app's built-in worktree mechanism when editing code; never create non-compliant directories under `/Users/up/code`.
   - Workers report their branch name and full commit SHA upward, and **never** merge, push, or delete branches/worktrees on their own. The coordinator merges and cleans up only after user confirmation.

## 1. Coordinator Mode (When the User Issues a Cross-Desktop Order Here)
Whenever the user gives an instruction here that asks to coordinate across models/apps (or says "让 Claude/ChatGPT 一起看", "多端协作", "按各自模型分派"):
1. Inspect active peers, standby listeners, and quota windows first via `consilium_status()` (or `consilium_quota()`).
   - Each peer entry in `quotas` exposes `min_remaining_percent`, `exhausted`, `dispatch_recommended`, `earliest_reset_at`, `earliest_reset_in_human`, and per-window details (`5h`, `7d`, model groups).
   - Avoid dispatching heavy tasks to any peer with `dispatch_recommended == false`; instead route to peers with headroom or inform the user of the peer's remaining quota and `earliest_reset_in_human` (`reset_at`).
2. Decompose the goal into clear, bounded tasks assigned to the best peer + `model_tier` (taking remaining quota headroom into account):
   - `pro_reasoning`: architecture design, hard root-cause debugging, adversarial audit (`opus` / `fable` plan audit / high-reasoning Codex / Gemini `pro`).
   - `balanced`: feature implementation, refactoring, structured test writing (`opus` in Claude / balanced Codex / default Gemini).
   - `fast`: codebase scan, doc lookup, quick verification (`opus` in Claude / fast Codex / Gemini `flash` or `flash_lite`).
3. Enforce isolation: default `isolation_policy: "worktree"` for code edits, or `"read_only"` for audits/research. Never let two peers edit the same working tree simultaneously.
4. Call `consilium_create_mission(...)` with `coordinator_peer` set to your own peer ID and `max_rounds: 3` (hard loop guard).
5. Check `deliveries`:
   - `hot_wakeup`: target desktop standby session woke automatically (with cold escalation backup if unacked).
   - `cold_notification`: target desktop had no active waiter; the macOS notification banner does **not** steal focus or open deep links on click. Tell the user briefly that a cold notification was sent so they can manually switch to the target app and run `consilium_pickup_inbox(peer_id=<target>)`. Only enable mission-level `open_deeplink: true` after explicit user consent.
6. Wait for results cleanly:
   - Waiting is **strictly read-only**: in **Antigravity / Claude Code tab / Codex**, run `/usr/bin/python3 -m consilium.cli wait --peer <self>` in the background, or call `consilium_wait(peer_id=<self>, timeout_sec=45)` in request-bounded surfaces (such as Claude Chat tab) (`auto_pickup` / `--pickup` parameters are retained only for compatibility and are ignored). Then explicitly call `consilium_pickup_inbox(peer_id=<self>)` upon wakeup.
   - If you receive `task_stale` (a `running` task missed its progress interval) or a worker hits `failed`/`blocked`, inspect whether the worker is still active before calling `consilium_requeue_task(...)` (valid for `claimed`, `running`, `failed`, or `blocked`; never `done` or `cancelled`).
7. Once `all_tasks_finished` is true (or `loop_guard_reached` escalates back), synthesize a single unified report for the user citing exact files, test results, and worktree branches, and close the mission with `consilium_close_mission(mission_id, peer_id=<self>, final_summary=...)`.

## 2. Standby Worker Mode (When Acting as a Background Doorbell Listener)
1. **Surface Rules**:
   - **Claude.app**: Only the **Code tab** can stay on background standby (`run_in_background: true`). The **Chat tab** cannot run background listeners and participates only as coordinator or via cold deep link.
   - Start background standby (strictly read-only):
     `/usr/bin/python3 -m consilium.cli wait --peer <self> --session-label <label>`
     (Waiter watches `/tmp/consilium-<uid>/signals/<peer>.seq` with 0% CPU, zero SQLite writes, and never claims tasks.)
2. On wakeup, call `consilium_pickup_inbox(peer_id=<self>, session_label=<label>)` to atomically claim tasks. Save each claimed task's **`attempt_id`**, then fan out **inside your own native harness**:
   - **Antigravity**: call `invoke_subagent` with `Model: "pro" | "flash" | "flash_lite"` and `Workspace: "share" | "branch"` (or `swarm`).
   - **Claude Desktop**: invoke native `Agent` subagents using `opus-architect` (or `fable` for plan review), `opus-builder`, or `opus-scout` with worktree isolation when editing code. Do **not** use `sonnet` or `haiku`.
   - **ChatGPT / Codex Desktop**: spawn native Codex child agents using `architect-high`, `builder-balanced`, or `scout-fast` profiles.
3. **Attempt Lease & Progress Interval (Required)**:
   - Immediately transition started work via `consilium_report_progress(task_id, peer_id=<self>, progress_note=..., status="running", attempt_id=<attempt_id>)` (unstarted `claimed` tasks auto-requeue on `claim_timeout_sec`).
   - For long-running tasks, send `consilium_report_progress(..., attempt_id=<attempt_id>, status="running")` at intervals of **at most half the `running_timeout_sec`** (e.g., at least every **<= 1 hour** under the default 2-hour running timeout). Expired `running` tasks are marked `stale` (`task_stale` sent to both coordinator and assignee); if reassigned/requeued, old `attempt_id` writes are rejected.
4. Never enter an unguarded ping-pong loop: cross-peer checks MUST go through `consilium_request_review(...)`, and if `allowed == false`, stop and hand control back to the coordinator.
5. Finish by calling `consilium_complete_task(task_id, peer_id=<self>, attempt_id=<attempt_id>, result_summary=..., artifacts=[...])`, then re-arm your standby waiter silently if continuing standby duty.

## 3. Messages From the Consilium App (Added 2026-11-20)

The user can now type directly in the Consilium window. Each message arrives as an ordinary task (`origin = "user"`) and is picked up exactly like any other task.

- **Addressed to you (`@` you)**: the task prompt is the user's words verbatim. Do the work and answer with `consilium_complete_task`; write `result_summary` as a reply to the user, in their language. The mission's coordinator is `user`, so your reply is shown in the window instead of going to another peer.
- **Auto-routed (no `@`)**: the prompt starts with the user's words, then says you are the coordinator. Before deciding:
  1. Call `consilium_preferences` to read the user's learned preferences, their recent explicit routing, and each peer's standby state and current-account quota.
  2. Do it yourself, or split it with `consilium_dispatch_task`. Prefer peers that are on standby and have quota; unknown quota is fine; never dispatch to an exhausted peer.
  3. Finish with `consilium_complete_task` on your own task. Its `result_summary` is the final answer the user reads.
- **Learning the user's intent**: when a choice the user made is clearly a stable habit (for example "code review always goes to Codex", or "answer briefly in Chinese"), record it in one short sentence with `consilium_learn_preference`. Don't record guesses or one-off details. The user can see and delete every preference in the window.
- **Handing off on low quota**: if your own quota is nearly used up, call `consilium_handoff_task(task_id, peer_id, attempt_id, to_peer, note)`. The note must say what is already done and why you are handing off. Your old `attempt_id` stops working at once. Account switching is always manual: if every peer is low, tell the user which account to switch to in Cockpit.
