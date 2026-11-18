# `deskbell` Architecture & Protocol

## 1. Design Constraints Discovered on This Mac

1. **System Python 3.9 Only**: `/usr/bin/python3` is `3.9.6` with no third-party packages. All code uses Python 3.9 standard library only (`from __future__ import annotations`).
2. **Seatbelt Sandbox Restrictions**: Desktop terminal tools (e.g., `Antigravity.app`, `Claude.app`, `ChatGPT.app`) execute background commands inside macOS Seatbelt sandboxes that block outbound `socket.connect()` (`EPERM` on both `AF_UNIX` and `127.0.0.1`) and hide `~/.deskbell`.
   - **Solution**: Default `$DESKBELL_HOME` to `/tmp/deskbell-<uid>` (hardened via `os.lstat()` ownership & symlink checks, `0o700`), and make `select.kqueue()` (`KQ_FILTER_VNODE` on `signals/<peer>.sig`) the primary zero-CPU wakeup mechanism. Host-process MCP servers (`stdio` / `127.0.0.1` Streamable HTTP) and sandboxed background `deskbell.cli wait` commands share `/tmp/deskbell-<uid>` seamlessly.

## 2. Truth Layer (`deskbell/store.py`)

All persistent state lives in `$DESKBELL_HOME/bus.db` (`SQLite` WAL mode, `busy_timeout=5000`, `BEGIN IMMEDIATE` write transactions):

- **`peers`**: Tracks standby/active status, `session_label`, `supported_models`, `waiter_active`, and `last_seen_at`.
- **`missions`**: Tracks `title`, `goal`, `coordinator_peer`, `workspace_path`, `isolation_policy` (`worktree` | `read_only` | `shared`), `max_rounds`, `current_round`, `status`, and `final_summary`.
- **`tasks`**: Tracks `assignee_peer`, `model_tier` (`pro_reasoning` | `balanced` | `fast` | `custom`), `model_hint`, `parallel_fanout`, `worktree_branch`, `status` (`queued` | `claimed` | `running` | `done` | `failed` | `blocked`), `claimed_by_session`, `progress_note`, `result_summary`, and `artifacts_json`.
- **`events`**: Directed notifications (`task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`) with `acked_at` timestamps.
- **`quota_windows`**: Per-peer and per-account rolling quota percentages (`5h`, `7d`, model groups), normalized UTC `reset_at` timestamps, countdown calculation (`reset_in_human`), and `dispatch_recommended` posture.

### Key Invariants
- **Atomic Claiming**: `pickup_inbox` runs inside `BEGIN IMMEDIATE` and updates `WHERE status = 'queued'`, preventing two concurrent standby sessions from claiming the same task.
- **Loop Guard**: `request_review` checks `current_round < max_rounds` inside `BEGIN IMMEDIATE`. Once exhausted, it rejects further cross-peer pings (`allowed=False`) and emits `loop_guard_reached` back to `coordinator_peer`.
- **Batch Deduplication**: `deskbell_create_mission` groups newly created tasks by `assignee_peer` so each peer is rung at most once per mission creation batch.

## 3. Wakeup Layer (`deskbell/wakeup.py`)

### Hot Standby (`wait_for_doorbell`)
1. Pre-check `store.count_unacked_events(peer) > 0`; if unread events already exist, return immediately (`source="immediate"`).
2. Write a lease file `$DESKBELL_HOME/waiters/<peer>/<pid>-<tid>.json` (`{"pid", "expires_at"}`).
3. Open `$DESKBELL_HOME/signals/<peer>.sig` and register a `KQ_FILTER_VNODE` kevent (`KQ_NOTE_WRITE | KQ_NOTE_EXTEND | KQ_NOTE_ATTRIB`).
4. Immediately re-check `store.count_unacked_events(peer) > 0` after registration (`source="immediate_after_register"`), closing the TOCTOU window between step 1 and step 3.
5. Sleep in `kq.control(None, 1, timeout_sec)` with 0% CPU and 0 LLM tokens until `notify_peer` appends a pulse line (`<ns>\n`) to `<peer>.sig` and calls `fsync()`.
6. On exit (`finally:`), remove the lease file and update `waiter_active=False`. Dead PIDs and expired leases are pruned automatically by `_count_live_leases()` via `os.kill(pid, 0)`.

*(Non-BSD fallback retains `WakeBrokerServer` over `AF_UNIX`—auto-shortened below 96 bytes for macOS `sun_path`—plus a polite `poll_fallback` loop whenever `wake.sock` is absent or blocked.)*

### Cold Fallback (`ColdDoorbell`)
When `notify_peer` reports `woken_count == 0`, `ring_peer` falls back cleanly:
- Passes notification title, body, and subtitle via `osascript` `argv` (`on run argv`) so untrusted task text never touches AppleScript syntax.
- Builds concise URL-encoded deep links (`claude://code/new?q=...&folder=...`, `codex://new?q=...`, `antigravity://`) without injecting keystrokes or stealing focus.

## 4. MCP Tools (`deskbell/mcp_server.py`)

Exposed identically over `deskbell mcp-stdio` and `POST /mcp` (`127.0.0.1:18920`):
1. `deskbell_status`
2. `deskbell_create_mission`
3. `deskbell_dispatch_task`
4. `deskbell_pickup_inbox`
5. `deskbell_report_progress`
6. `deskbell_complete_task`
7. `deskbell_request_review`
8. `deskbell_wait`
9. `deskbell_quota`

## 5. Multi-Source Quota Extraction (`deskbell/quota.py`)

`sync_local_quotas` aggregates quota remaining % and reset times locally (zero LLM token cost, best-effort under Seatbelt):
1. `~/.antigravity_cockpit/platform-quota-history-v1.sqlite` & `codex-quota-history-v1.sqlite` (read-only URI SQLite queries)
2. `~/.antigravity_cockpit/{codex,claude,google,accounts}_accounts.json` (JSON quota/reset fields)
3. `~/.codex/sessions/**/rollout-*.jsonl` (tail scan for latest `rate_limits.primary` 5h & `rate_limits.secondary` 7d `used_percent` + `resets_at`)
4. `~/.claude/{usage-latest,quota}.json` (structured windows or cached `anthropic-ratelimit-unified-*` headers)
5. Direct peer reporting via `deskbell_quota(report_windows=[...])`.
