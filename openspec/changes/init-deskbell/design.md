# Design: `deskbell` Architecture

## 1. Two-Layer Data & Wakeup Architecture
- **Split Storage (`~/Library/Application Support/deskbell/` vs `/tmp/deskbell-<uid>/`)**:
  - Persistent state (`bus.db`, `auth.token`, backups) lives under `~/Library/Application Support/deskbell/` (`0o700`), immune to macOS 3-day `tmp_cleaner`. Logs live under `~/Library/Logs/deskbell/`.
  - Ephemeral rendezvous (`signals/<peer>.{sig,seq}`, `waiters/<peer>/*.lease`, `wake.sock`) lives under `/tmp/deskbell-<uid>/` (`0o700`), validated via `os.lstat()` (`S_ISLNK` rejected, `st_uid == os.getuid()` required).
- **Truth Layer (`SQLite` WAL in `<state_dir>/bus.db`)**:
  - `peers`: Tracks standby state, session label, supported models, `waiter_active` (overlaid from `flock` leases), and heartbeat timestamps for `chatgpt`, `claude`, and `antigravity`.
  - `missions`: Top-level user request, `coordinator_peer`, `workspace_path`, `isolation_policy` (`worktree` | `read_only` | `shared`), `max_rounds`, `current_round`, `status`, and `final_summary`.
  - `tasks`: Sub-assignments with `assignee_peer`, `model_tier` (`pro_reasoning` | `balanced` | `fast` | `custom`), `model_hint`, `parallel_fanout`, `worktree_branch`, `status` (`queued` | `claimed` | `running` | `done` | `failed` | `blocked` | `cancelled`), `claimed_at`, `progress_note`, `result_summary`, and `artifacts_json`.
  - `events`: Directed inbox messages (`task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`, `mission_closed`, `task_released`) with `acked_at` semantics.
  - `quota_windows`: Per-peer and per-account remaining quota percentages (`5h`, `7d`, model groups), UTC `reset_at` timestamps, countdown calculations, and `dispatch_recommended` posture extracted locally by `deskbell/quota.py`.
- **Doorbell Layer (DB-Free `kqueue` `.seq` + `fcntl.flock` Leases + Cold Escalation)**:
  - Waiters read `signals/<peer>.seq` and hold `fcntl.flock(LOCK_EX | LOCK_NB)` leases (never touching SQLite and immune to PID reuse).
  - Hot wakeups with `event_ids` schedule a cold fallback escalation timer if the event remains unacked.
  - `ColdDoorbell.build_deeplink` strips leading `/`, truncates before UTF-8 percent-encoding, preserves `folder` for Claude, and uses `codex://new?prompt=` for Codex.

## 2. Dual MCP Transports & Security
- `deskbell mcp-stdio`: Used when a desktop app spawns stdio MCP directly.
- `deskbell serve`: Runs `WakeBrokerServer` + `ThreadingHTTPServer` bound strictly to `127.0.0.1` (and exposes `handle_socket_once` for wire-level testing).
  - Rejects non-localhost `Host` headers and any non-local `Origin` header with `403 Forbidden` (CSRF / DNS-rebinding defense).
  - Supports optional `Authorization: Bearer <token>` backed by `$DESKBELL_HOME/auth.token` (`0600`).

## 3. Native Harness Model Routing & Parallelism
- Instead of opening N top-level desktop windows externally, each awakened standby session fans out inside its own native harness:
  - **Antigravity**: `invoke_subagent` (`Model`: `pro`/`flash`/`flash_lite`, `Workspace`: `share`/`branch`) or `swarm`.
  - **Claude Desktop**: Native `Agent` subagents mapped to `~/.claude/agents/{opus-architect,sonnet-builder,haiku-scout}.md` with worktree isolation.
  - **ChatGPT (Codex Desktop)**: Native agent profiles mapped to `~/.codex/agents/{architect-high,builder-balanced,scout-fast}.toml`.
