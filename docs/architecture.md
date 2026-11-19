# `consilium` Architecture & Protocol

## 1. Design Constraints Discovered on This Mac

1. **System Python 3.9 Only**: `/usr/bin/python3` is `3.9.6` with no third-party packages. All code uses Python 3.9 standard library only (`from __future__ import annotations`).
2. **Seatbelt Sandbox vs macOS `/tmp` Cleanup**:
   - Desktop terminal tools execute background commands inside macOS Seatbelt sandboxes that block outbound `socket.connect()` (`EPERM` on both `AF_UNIX` and `127.0.0.1`) and block `~/Library/Application Support`.
   - Meanwhile, macOS `/usr/libexec/tmp_cleaner` purges `/tmp` files untouched for 3 days (`daily_clean_tmps_days="3"`).
   - **Solution (Split Storage)**:
     - **Persistent State (`~/Library/Application Support/consilium/`, `0o700`)**: Holds `bus.db`, `auth.token` (`0o600`), and installer backups—touched only by host processes (`launchd` daemon and desktop-spawned `mcp-stdio`).
     - **Ephemeral Rendezvous (`/tmp/consilium-<uid>/`, `0o700`)**: Holds `signals/<peer>.seq`, `signals/<peer>.sig`, `waiters/<peer>/*.lease`, and `wake.sock`. Sandboxed background `consilium wait` watches `signals/<peer>.seq` via `kqueue` without ever touching SQLite.
     - **Logs (`~/Library/Logs/consilium/`)**: Holds `launchd` stdout/stderr logs so restarts never fail on missing `/tmp` subdirectories.

## 2. Truth Layer (`consilium/store.py`)

All persistent state lives in `<state_dir>/bus.db` (`SQLite` WAL mode, `busy_timeout=5000`, `BEGIN IMMEDIATE` write transactions):

- **`peers`**: Tracks standby/active status, `session_label`, `supported_models`, `waiter_active` (overlaid from live `flock` leases), and `last_seen_at`.
- **`missions`**: Tracks `title`, `goal`, `coordinator_peer`, `workspace_path`, `isolation_policy` (`worktree` | `read_only` | `shared`), `max_rounds`, `current_round`, `status`, and `final_summary`.
- **`tasks`**: Tracks `assignee_peer`, `model_tier` (`pro_reasoning` | `balanced` | `fast` | `custom`), `model_hint`, `parallel_fanout`, `worktree_branch`, `status` (`queued` | `claimed` | `running` | `done` | `failed` | `blocked` | `cancelled`), `claimed_by_session`, `claimed_at`, `claim_stale`, `progress_note`, `result_summary`, and `artifacts_json`.
- **`events`**: Directed notifications (`task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`, `mission_closed`, `task_released`) with `acked_at` timestamps.
- **`quota_windows`**: Per-peer and per-account rolling quota percentages (`5h`, `7d`, model groups), normalized UTC `reset_at` timestamps, countdown calculation (`reset_in_human`), and `dispatch_recommended` posture.

### Key Invariants
- **Atomic Claiming & Stale Recovery**: `pickup_inbox` runs inside `BEGIN IMMEDIATE`, records `claimed_at`, and updates `WHERE status = 'queued'`. `release_stale_claims()` re-queues orphaned `claimed` tasks (default 15m) and stalled `running` tasks (default 2h, heartbeat via `report_progress.updated_at`), emitting `task_requeued` to `coordinator_peer`. `ConsiliumDaemon` runs this reaper every 60s and `consilium_pickup_inbox` invokes it before claiming.
- **Strict State Machine**: `report_progress` allows non-terminal states only and enforces `peer == assignee_peer`; `complete_task` enforces `peer == assignee_peer` and rejects duplicate completion; `dispatch_task` rejects closed missions and transitions `ready_to_report` back to `active`; `request_review` rejects closed missions and validates optional `task_id` membership; `close_mission` requires `peer_id == coordinator_peer` and cancels non-terminal tasks only with `force=True`.
- **Uniform Write Envelope & Idempotency**: Write tools return `{"committed": true, "result": {...}, "delivery": {...}}`, isolate `ring_peer` errors without returning `isError=true`, and accept optional `client_request_id` backed by the `idempotency_keys` table.
- **Loop Guard**: `request_review` checks `current_round < max_rounds` inside `BEGIN IMMEDIATE` and emits `loop_guard_reached` once exhausted.
- **Locked `.seq` Sync & Rebuild**: After each commit touching `events` or `tasks`, `BusStore.sync_peer_seq` locks `signals/<peer>.seq.lock` (`fcntl.flock`), re-queries SQLite for authoritative `(max_event_id, unacked, queued_tasks)`, and atomically writes `signals/<peer>.seq`. Both `ConsiliumDaemon.start()` and `run_stdio_server()` rebuild all three peers' `.seq` files from SQLite on startup.

## 3. Wakeup Layer (`consilium/wakeup.py`)

### Hot Standby (`wait_for_doorbell`)
1. Reads `signals/<peer>.seq` (`unacked > 0` or `queued_tasks > 0` returns immediately with `source="immediate"`). Defaults to `auto_pickup=False` in both CLI and MCP. Never writes to `bus.db` during wait.
2. Acquires an exclusive kernel `fcntl.flock(LOCK_EX | LOCK_NB)` on `<rendezvous_dir>/waiters/<peer>/<pid>-<tid>.lease` (immune to PID reuse).
3. Registers `KQ_FILTER_VNODE` (including `KQ_NOTE_DELETE | KQ_NOTE_RENAME`) on `signals/<peer>.sig`, re-checks `signals/<peer>.seq` (`immediate_after_register`), re-attaches automatically if `.sig` is deleted/renamed, ignores spurious empty wakeups, and sleeps at 0% CPU until `.seq` advances.
4. On exit (`finally:`), unlinks the `.lease` file and releases `flock`.
5. **Cold Escalation Backup**: When `ring_peer` sends a hot wakeup with `event_ids`, a daemon `threading.Timer` (default `60s`) checks `store.is_event_acked(eid)` and escalates to `ColdDoorbell` if the event remains unacknowledged. (Because the timer is a daemon thread, it persists only for the lifetime of the long-lived daemon/stdio process, which is accepted for v1.)

### Cold Fallback (`ColdDoorbell`)
- Passes notification title, body, and subtitle via `osascript` `argv` (`on run argv`). Notifications do not steal focus or auto-open deep links on click; `open_deeplink=True` is opt-in per mission after user consent.
- Truncates prompts **before** UTF-8 percent-encoding (`_truncate_pre_encode`), strips leading `/`, uses `claude://code/new?q=...` (with optional `&folder=...`) for Claude, and uses `codex://new?prompt=...&path=...` for Codex Desktop.

## 4. MCP Tools (`consilium/mcp_server.py`)

Exposed identically over `consilium mcp-stdio` and `POST /mcp` (`127.0.0.1:18920`, token authentication enabled by default, `405 Allow: POST` on `GET /mcp`):
1. `consilium_status`
2. `consilium_create_mission`
3. `consilium_dispatch_task`
4. `consilium_pickup_inbox`
5. `consilium_report_progress`
6. `consilium_complete_task`
7. `consilium_request_review`
8. `consilium_wait`
9. `consilium_quota`
10. `consilium_close_mission`

## 5. Multi-Source Quota Extraction (`consilium/quota.py`)

`sync_local_quotas` aggregates quota remaining % and reset times locally (zero LLM token cost, best-effort under Seatbelt):
1. `~/.antigravity_cockpit/platform-quota-history-v1.sqlite` & `codex-quota-history-v1.sqlite` (read-only URI SQLite queries)
2. `~/.antigravity_cockpit/{codex,claude,google,accounts}_accounts.json` (JSON quota/reset fields)
3. `~/.codex/sessions/**/rollout-*.jsonl` (tail scan for latest `rate_limits.primary` 5h & `rate_limits.secondary` 7d `used_percent` + `resets_at`)
4. `~/.claude/{usage-latest,quota}.json` (structured windows or cached `anthropic-ratelimit-unified-*` headers)
5. Direct peer reporting via `consilium_quota(report_windows=[...])`.
