---
name: consilium
description: Install, connect, troubleshoot, and operate Consilium — a 100% local macOS menu bar app and MCP collaboration bus coordinating Claude, ChatGPT/Codex, and Antigravity desktop apps without proxy APIs or CLI wrappers. Use when the user wants to install/set up Consilium on macOS, connect desktop AI apps to Consilium MCP, debug daemon/doorbell/quota issues, or collaborate across Claude, Codex, and Antigravity as coordinator or standby worker.
license: CC-BY-NC-SA-4.0
compatibility: macOS 13+ with Apple Command Line Tools (/usr/bin/python3 3.9+ and swiftc) and one or more of Claude, ChatGPT/Codex, or Antigravity desktop apps.
metadata:
  author: xiangshang2004
  version: "0.7.4"
---

# Consilium — Local Tri-Desktop AI Collaboration Skill

**Consilium** is a 100% local-first macOS app (`Consilium.app`) and MCP collaboration bus (`127.0.0.1:18920` HTTP + `stdio`) connecting native **Claude**, **ChatGPT / Codex**, and **Antigravity** desktop apps. It never calls LLM APIs directly or wraps CLI binaries: every task runs inside the user's official desktop apps with their own accounts and quotas.

## Safety & Privacy Boundaries

- **100% local**: Daemon binds strictly to `127.0.0.1`; `/ui` enforces `default-src 'none'` offline CSP.
- **Zero third-party Python dependencies**: Pure Python 3.9+ standard library + Apple Command Line Tools (`swiftc`).
- **Read-only quota isolation**: Cockpit Tools (`~/.antigravity_cockpit/`) integration is strictly read-only on whitelisted history/metadata files; never open encrypted credential vaults or `codex_accounts.sqlite`, and never expose `quota_account_names` (emails/display names) in MCP payloads or peer prompts.
- **Dry-run first**: Always preview `scripts/install.py --dry-run` before running `--apply`, and always preserve timestamped backups when updating desktop app configurations.

---

## Mode Selection

Determine which mode applies right now:

1. **Setup / Connect / Upgrade** (`Consilium.app` or `consilium` MCP tools are not yet configured, or the user asks to install/connect/uninstall Consilium) -> Follow **Mode 1** and read [references/setup-and-install.md](references/setup-and-install.md).
2. **Active Collaboration** (`consilium_*` MCP tools are available and the user asks to coordinate peers, stand by, or handle a message from Consilium) -> Follow **Mode 2**.
3. **Troubleshooting** (Gatekeeper block, missing MCP tools, standby offline, port `18920` conflict, quota unknown, `401` auth error, or build issue) -> Follow **Mode 3** and read [references/troubleshooting.md](references/troubleshooting.md).

---

## Mode 1: Install, Connect & Configure Consilium

1. **Inspect the local environment first** (read-only):
   - Check macOS (`sw_vers`), Apple CLT (`/usr/bin/python3 --version` and `command -v swiftc`), whether `/Applications/Consilium.app` exists, and whether `consilium` MCP tools are already loaded.
   - Do not guess paths or overwrite user configs without previewing `--dry-run` first.
2. **Install `Consilium.app` and connect desktop apps**:
   - Read [references/setup-and-install.md](references/setup-and-install.md) for the exact steps covering:
     - **Option A (Recommended on Apple Silicon `arm64`)**: Download the latest `Consilium-<version>-arm64.dmg` (or `.zip` + `.sha256`) from `https://github.com/xiangshang2004/consilium/releases/latest` and drag `Consilium.app` into `/Applications/`.
     - **Option B (Source build for Intel or Apple Silicon, ~5 seconds)**: Clone `https://github.com/xiangshang2004/consilium.git`, run `./scripts/build_app.sh`, and copy `build/Consilium.app` to `/Applications/`.
     - **Connect desktop apps (`--dry-run` first, confirm with user, then `--apply`)**:
       ```bash
       python3 scripts/install.py --dry-run --targets claude_code codex antigravity
       python3 scripts/install.py --apply --targets claude_code codex antigravity
       ```
       (Supported targets: `claude_code`, `codex`, `antigravity`, `claude_desktop`, `claude_agents`, `codex_agents`, `launchd`, or `all`.)
3. **Activate tools & standby**:
   - Remind the user to start a **new session** (or `⌘Q` quit and relaunch) in each configured desktop app so the newly registered `consilium` MCP tools load.
   - Launch **Consilium.app** (it manages the local daemon lifecycle automatically), optionally enable notifications via menu bar **Consilium -> 开启任务提醒…**, and click **叫醒 (Wake)** on peer cards in the lower-left dock.

---

## Mode 2: Tri-Desktop Collaboration Protocol

You are one of three desktop AI peers sharing a single local collaboration bus (`consilium`):
- `chatgpt`: `ChatGPT.app` / Codex Desktop (`com.openai.codex`)
- `claude`: `Claude.app` (`com.anthropic.claudefordesktop`)
- `antigravity`: `Antigravity.app` (`com.google.antigravity`)

### 0. Project Workflow & Worktree Rules (Highest Priority)
1. **Repository Rules Always Win**: Before creating missions or editing code in a repository that defines its own collaboration workflow or `AGENTS.md`, follow that repository's rules first. `consilium` is only the doorbell/coordination bus and **never replaces a repository's native workflow**.
2. **Branch Prefix Mapping**:
   - `claude` -> `claude/<topic>-<YYYYMMDD>`
   - `chatgpt` -> `codex/<topic>-<YYYYMMDD>`
   - `antigravity` -> `gemini/<topic>-<YYYYMMDD>`
3. **Worktree Isolation & Cleanup**:
   - Use each app's built-in worktree mechanism when editing code; keep temporary artifacts out of tracked source directories.
   - Workers report their branch name and full commit SHA upward, and **never** merge, push, or delete branches/worktrees on their own. The coordinator merges and cleans up only after user confirmation.

### 1. Coordinator Mode (When the User Issues a Cross-Desktop Order Here)
Whenever the user gives an instruction here that asks to coordinate across models/apps (or says "让 Claude/ChatGPT 一起看", "多端协作", "按各自模型分派"):
1. Inspect active peers, standby listeners, and quota windows first via `consilium_status()` (or `consilium_quota()`).
   - Each peer entry in `quotas` exposes `min_remaining_percent`, `exhausted`, `dispatch_recommended`, `earliest_reset_at`, `earliest_reset_in_human`, and per-window details (`5h`, `7d`, model groups).
   - Avoid dispatching heavy tasks to any peer with `dispatch_recommended == false`; instead route to peers with headroom or inform the user of the peer's remaining quota and `earliest_reset_in_human` (`reset_at`).
2. Decompose the goal into clear, bounded tasks assigned to the best peer + `model_tier` (taking remaining quota headroom into account):
   - `pro_reasoning`: architecture design, hard root-cause debugging, adversarial audit (`opus` / high-reasoning Codex / Gemini `pro`).
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

### 2. Standby Worker Mode (When Acting as a Background Doorbell Listener)
1. **Surface Rules**:
   - **Claude.app**: Only the **Code tab** can stay on background standby (`run_in_background: true`). The **Chat tab** cannot run background listeners and participates only as coordinator or via cold deep link.
   - Start background standby (strictly read-only):
     `/usr/bin/python3 -m consilium.cli wait --peer <self> --session-label <label>`
     (Waiter watches `/tmp/consilium-<uid>/signals/<peer>.seq` with 0% CPU, zero SQLite writes, and never claims tasks.)
2. On wakeup, call `consilium_pickup_inbox(peer_id=<self>, session_label=<label>)` to atomically claim tasks. Save each claimed task's **`attempt_id`**, then fan out **inside your own native harness**:
   - **Antigravity**: call `invoke_subagent` with `Model: "pro" | "flash" | "flash_lite"` and `Workspace: "share" | "branch"` (or `swarm`).
   - **Claude Desktop**: invoke native `Agent` subagents using `opus-architect`, `opus-builder`, or `opus-scout` with worktree isolation when editing code (or the user's configured subagents).
   - **ChatGPT / Codex Desktop**: spawn native Codex child agents using `architect-high`, `builder-balanced`, or `scout-fast` profiles.
3. **Attempt Lease & Progress Interval (Required)**:
   - Immediately transition started work via `consilium_report_progress(task_id, peer_id=<self>, progress_note=..., status="running", attempt_id=<attempt_id>)` (unstarted `claimed` tasks auto-requeue on `claim_timeout_sec`).
   - For long-running tasks, send `consilium_report_progress(..., attempt_id=<attempt_id>, status="running")` at intervals of **at most half the `running_timeout_sec`** (e.g., at least every **<= 1 hour** under the default 2-hour running timeout). Expired `running` tasks are marked `stale` (`task_stale` sent to both coordinator and assignee); if reassigned/requeued, old `attempt_id` writes are rejected.
4. Never enter an unguarded ping-pong loop: cross-peer checks MUST go through `consilium_request_review(...)`, and if `allowed == false`, stop and hand control back to the coordinator.
5. Finish by calling `consilium_complete_task(task_id, peer_id=<self>, attempt_id=<attempt_id>, result_summary=..., artifacts=[...])`, then re-arm your standby waiter silently if continuing standby duty.

### 3. Messages From the Consilium App
The Consilium window works like the Claude Code desktop app: **the user picks a folder when starting a conversation; each conversation keeps its own history, and the user can bind a dedicated native session to it.** The user never types paths; never guess a folder from message text.

**What a picked-up task carries**: `mission_title`, `workspace_path` (the folder the user chose), `model_tier` on the task itself (`auto`, or the tier the user fixed: `fast` / `balanced` / `pro_reasoning`), `mission_tier` (the conversation's current choice), `native_session` (the dedicated session the user bound to this conversation for you, or empty), `conversation` (earlier turns of this conversation, oldest first) and `new_session` (1 for the first message to you in this conversation).

**Routing (standby sessions)**:
1. `native_session` is set: the user opened a dedicated session for this conversation. Forward the task to it (Claude Code: `SendMessage` to that session id with `task_id`, `attempt_id`, `peer_id` and the prompt) and keep standing by. If that session no longer exists, handle the task yourself as in step 2.
2. Otherwise handle it yourself. The task's `conversation` field holds this conversation's earlier turns (the user's messages and each peer's replies, oldest first): read it so follow-ups stay in context. Work inside `workspace_path`. Do **not** start new sessions from a standby turn: Claude Desktop does not allow an automatically woken turn to start sessions. The user starts a dedicated session from the Consilium window ("在 Claude 里开专属会话") when they want one.
3. Whoever does the work reports progress with `model_used` and `effort_used` (your reasoning effort, e.g. `high`; omit if your app has none) and answers with `consilium_complete_task` (the `result_summary` is what the user reads, in their language).
4. A dedicated session opened from the Consilium window binds itself first: get its own session id (`get_session("self")`), call `consilium_bind_session(mission_id, peer_id, <id>)`, read the conversation with `consilium_status(mission_id)`, then handle forwarded tasks as in step 3.

**Coordinating auto-routed messages** (the prompt says you are the coordinator): call `consilium_preferences` first; do it yourself or split it with `consilium_dispatch_task` (prefer peers on standby with quota; never an exhausted one); give the conversation a short title with `consilium_set_title` (at most 20 Chinese characters; ignored if the user renamed it); deliver the final answer with `consilium_complete_task`.

**Native subagents and public progress**: On messages requesting `/agents`, `parallel_fanout` is the user's requested count inside the receiving app. Use that app's native child-agent tool to work concurrently on independent pieces. Report how many actually started, their assignments, and any runtime limit. If a message requests `/trace`, use `consilium_report_progress` for concise public updates about observed evidence, decisions, progress, and verification results. Summarize results with `consilium_complete_task`. Keep private reasoning out of both.

**Keep sources apart**: the bus carries two kinds of conversations: those the user started in the Consilium window, and missions a desktop session created itself for its own project. They must not mix.
- A standby session serves the Consilium window only: always call `consilium_pickup_inbox(peer_id=<self>, session_label="<self>-standby", scope="consilium")`. If it returns nothing, re-arm silently and say nothing.
- A session that created its own mission collects results with `consilium_pickup_inbox(peer_id=<self>, mission_id=<that mission>)` or reads `consilium_status(mission_id)`; it never picks up without `mission_id`, so it cannot claim the user's Consilium messages.
- Missions created by a session do not appear in the Consilium window. Archiving in the window only hides a conversation there; it never touches a session in Claude, Codex or Antigravity.

**Tier (decide it yourself)**: the user does not pick a tier unless they want to. When a task's `model_tier` is `auto`, judge the difficulty before you start: **fast** = a quick answer or a small edit; **balanced** = ordinary work; **deep** = hard debugging, architecture, anything where a wrong answer is costly. Then really work at that tier with your own app's means, and never ask the user to choose:
- Claude Code: answer fast and balanced work yourself; for deep work delegate to a native subagent with a higher reasoning effort (the Agent tool's `effort`), keeping to the user's own rules about which models may be used.
- Codex / Antigravity: use your native subagent or reasoning-effort setting the same way; if your app cannot change it from inside a session, say so in `tier_reason` rather than pretending.
- When the user fixed a tier, follow it. A coordinator writes the tier it chose into `model_tier` on every task it dispatches.
- Report `tier_used` (`fast` / `balanced` / `deep`) and a one-sentence `tier_reason` in the user's language, together with `model_used` and `effort_used`.

**Quota**: remaining quota is synced by Consilium from Cockpit's local records. Do not query, estimate or report your own quota, and do not ask other peers to; read `consilium_preferences` / `consilium_quota` when you need it.

**Learning**: record a clearly stable habit of the user in one sentence with `consilium_learn_preference`; never record guesses. The user can delete any preference.

**Low quota**: hand the task off with `consilium_handoff_task(task_id, peer_id, attempt_id, to_peer, note)`; the note says what is done and why. Account switching is always manual; tell the user which account to switch to when every peer is low.

**Standing by**:
- Claude Code: run `PYTHONPATH=/Applications/Consilium.app/Contents/Resources/python /usr/bin/python3 -B -m consilium.cli wait --peer claude --session-label claude-standby --scope consilium --timeout 86400` with `run_in_background: true`, end the turn, and when it exits pick up with `scope="consilium"`, route, and re-arm. Never poll.
- Codex / Antigravity: call `consilium_wait(peer_id=<self>, timeout_sec=300, session_label="<self>-standby", scope="consilium")` in a loop; on `woke: true` pick up with `session_label="<self>-standby", scope="consilium"` and handle; keep each iteration terse.

---

## Mode 3: Diagnostics, Troubleshooting & Uninstall

When something fails to launch, connect, wake, or authenticate, read [references/troubleshooting.md](references/troubleshooting.md).

## References
- [references/setup-and-install.md](references/setup-and-install.md) — Step-by-step macOS installation (DMG/ZIP or source build), desktop MCP registration (`--dry-run` / `--apply`), custom port configuration, upgrading, and complete uninstall.
- [references/troubleshooting.md](references/troubleshooting.md) — Gatekeeper quarantine, missing MCP tools, standby doorbell debugging, port `18920` / `launchd` conflicts, Cockpit quota status, attachment limits, token rotation, and bug-report diagnostics.
