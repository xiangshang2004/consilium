# Design: `deskbell` Architecture

## 1. Two-Layer Data & Wakeup Architecture
- **Runtime Home Hardening (`/tmp/deskbell-<uid>`)**:
  - Defaults to `/tmp/deskbell-<uid>` (`0o700`), validated via `os.lstat()` (`S_ISLNK` rejected, `st_uid == os.getuid()` required) so host MCP daemons and macOS Seatbelt-sandboxed desktop commands share a common bus state without touching restricted home paths.
- **Truth Layer (`SQLite` WAL in `$DESKBELL_HOME/bus.db`)**:
  - `peers`: Tracks standby state, session label, supported models, `waiter_active`, and heartbeat timestamps for `chatgpt`, `claude`, and `antigravity`.
  - `missions`: Top-level user request, `coordinator_peer`, `workspace_path`, `isolation_policy` (`worktree` | `read_only` | `shared`), `max_rounds`, `current_round`, `status`, and `final_summary`.
  - `tasks`: Sub-assignments with `assignee_peer`, `model_tier` (`pro_reasoning` | `balanced` | `fast` | `custom`), `model_hint`, `parallel_fanout`, `worktree_branch`, `status` (`queued` | `claimed` | `running` | `done` | `failed` | `blocked`), `progress_note`, `result_summary`, and `artifacts_json`.
  - `events`: Directed inbox messages (`task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`) with `acked_at` semantics.
  - `quota_windows`: Per-peer and per-account remaining quota percentages (`5h`, `7d`, model groups), UTC `reset_at` timestamps, countdown calculations, and `dispatch_recommended` posture extracted locally by `deskbell/quota.py`.
- **Doorbell Layer (`kqueue` `KQ_FILTER_VNODE` + Lease Tracking + `AF_UNIX` Broker + Cold Fallback)**:
  - Seatbelt sandboxes inside macOS desktop apps block outbound `socket.connect()` (`EPERM`), whereas kernel `select.kqueue()` (`KQ_FILTER_VNODE` watching `$DESKBELL_HOME/signals/<peer>.sig`) operates with 0% CPU and millisecond wakeup latency inside sandboxes.
  - Active standby waiters write a PID/expiry lease file under `$DESKBELL_HOME/waiters/<peer>/<pid>-<tid>.json`; stale/dead PID leases are automatically pruned via `os.kill(pid, 0)`.
  - TOCTOU elimination: register `kqueue` watch -> immediately re-check `store.count_unacked_events(peer)` -> block cleanly.
  - On dispatch/review/completion, `ring_peer` pulses `<peer>.sig` (and notifies `WakeBrokerServer` if `wake.sock` is reachable). If active waiters > 0, delivery is `hot_wakeup`. Otherwise, `ColdDoorbell` emits an `argv`-safe macOS notification (`osascript`) and a concise deep link (`claude://code/new?...`, `codex://new?...`, `antigravity://`) without stealing keyboard focus.

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
