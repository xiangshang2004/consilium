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

- **`peers`**: Tracks 5-state indicator status (`standby` | `working` | `online` | `offline` | `stale` — active `.lease` -> `standby`; `claimed + running > 0` -> `working` or `stale` if task/heartbeat expired; idle with fresh `last_seen_at` -> `online`; never seen or idle with expired `last_seen_at` -> `offline`), `session_label`, `supported_models`, `waiter_active` (overlaid from live `flock` leases), and `last_seen_at`.
- **`missions`**: Tracks `title`, `goal`, `coordinator_peer`, `workspace_path`, `isolation_policy` (`worktree` | `read_only` | `shared`), `max_rounds`, `current_round`, `status`, and `final_summary`.
- **`tasks`**: Tracks `assignee_peer`, `model_tier` (`pro_reasoning` | `balanced` | `fast` | `custom`), `model_hint`, `parallel_fanout`, `worktree_branch`, `status` (`queued` | `claimed` | `running` | `done` | `failed` | `blocked` | `cancelled`), `attempt_seq`, `attempt_id`, `attempt_history_json`, `claimed_by_session`, `claimed_at`, `stale_at`, `dispatched_at_ms`, `claimed_at_ms`, `claim_latency_ms`, `progress_note`, `result_summary`, and `artifacts_json`.
- **`events`**: Directed notifications (`task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`, `task_stale`, `task_requeued`, `mission_closed`) with `acked_at` timestamps and delivery metadata (`claim_latency_ms`, causal `wakeup_latency_ms`, and 3 cold-notification facts).
- **`idempotency_keys`**: Stores `(client_request_id, operation, caller_scope, request_hash, response_json, created_at)` for scoped replay and conflict detection (`ConflictError` / HTTP `409`).
- **`quota_windows`**: Per-peer and per-account rolling quota percentages (`5h`, `7d`, model groups), `source` (`demo` | `manual_report` | local sync), normalized UTC `reset_at` timestamps, countdown calculation (`reset_in_human`), and `dispatch_recommended` posture.

### Key Invariants
- **Atomic Claiming, Attempt Fencing & Stale Recovery**: `pickup_inbox` runs inside `BEGIN IMMEDIATE`, increments `attempt_seq`, generates `attempt_id` (`A-<seq>-<hex>`), computes `claim_latency_ms = now_ms - dispatched_at_ms`, and updates `WHERE status = 'queued'`. `release_stale_claims()` auto-requeues expired `claimed` tasks (default 15m, archiving the prior attempt into `attempt_history_json`, emitting `task_requeued` to `coordinator_peer`, and waking `assignee_peer`), while expired `running` tasks (default 2h, heartbeat via `updated_at`) are **not** auto-requeued—instead `stale_at` is set (preserving `attempt_id`) and `task_stale` is emitted to both `coordinator_peer` and `assignee_peer` for explicit coordinator/dashboard `requeue_task`. `ConsiliumDaemon` runs `_reaper_thread` periodically and `consilium_pickup_inbox` invokes it before claiming.
- **Strict State Machine & Requeue Scope**: `report_progress` and `complete_task` require matching active `attempt_id` (`A-<seq>-<hex>`), rejecting stale or mismatched attempts from reclaimed/requeued sessions with `ValueError`. `requeue_task` permits only `REQUEUEABLE_TASK_STATUSES` (`claimed` | `running` | `failed` | `blocked`) on open missions (coordinator-only via MCP), rejects `queued`/`done`/`cancelled`/closed missions, archives the previous attempt into `attempt_history_json`, and resets claim/progress/result fields. `close_mission`, `dispatch_task`, and `request_review` enforce coordinator/ownership/open-mission guards.
- **Uniform Write Envelope, Post-Commit Fault Isolation & Scoped Idempotency**: All 7 write MCP tools return `{"committed": true, "result": {...}, "delivery": {...}}` (plus top-level domain keys for compatibility). Post-commit failures in `_safe_sync_peers_seq`, `ring_peer`, `record_event_delivery`, and `publish_state_update` are collected into deduplicated `delivery.warnings` (and `post_commit_warnings` on `pickup_inbox`) without turning a committed write into `isError=true` or HTTP 500. Optional `client_request_id` enforces matching `(operation, caller_scope, request_hash)` (with stdio `--peer` `consilium_dispatch_task` scoped as `peer:<default_peer>` and HTTP as `coordinator`), raising `ConflictError` (HTTP `409` / MCP error) on conflict.
- **Loop Guard**: `request_review` checks `current_round < max_rounds` inside `BEGIN IMMEDIATE` and emits `loop_guard_reached` once exhausted.
- **Locked `.seq` Sync & Single-Snapshot Rebuild**: After each commit touching `events` or `tasks`, `BusStore.sync_peer_seq` locks `<rendezvous_dir>/.<peer>.seq.lock` (`threading.Lock` + `fcntl.flock`, outside `signals/` so deleting `signals/` never rotates the lock inode), executes `query_peer_seq_counts` inside a single `BEGIN; ... COMMIT;` snapshot for authoritative `(max_event_id, unacked, queued_tasks)`, and atomically writes `signals/<peer>.seq` with bounded 10-attempt retry on `OSError(ENOENT | EINVAL)` (`_read_peer_seq_unlocked` reads `.seq` without creating `signals/`). Both `ConsiliumDaemon.start()` and `run_stdio_server()` rebuild all three peers' `.seq` files from SQLite on startup.

## 3. Wakeup Layer (`consilium/wakeup.py`)

### Hot Standby (`wait_for_doorbell`)
1. **Strictly Read-Only**: Reads `signals/<peer>.seq` (`unacked > 0` or `queued_tasks > 0` returns immediately with `source="immediate"`). Both `consilium_wait` and CLI `consilium wait` never call `pickup_inbox` or write to `bus.db` (`auto_pickup=True` / `--pickup` are ignored with `auto_pickup_ignored=True` and `notice`).
2. **Atomic `.lease` Acquisition**: Creates `<rendezvous_dir>/waiters/<peer>/.tmp-<pid>-<tid>-<rand>` with `O_CREAT | O_EXCL`, acquires `fcntl.flock(LOCK_EX | LOCK_NB)` (immune to PID reuse), `fsync`s JSON, and atomically `os.replace`s into `<rendezvous_dir>/waiters/<peer>/<pid>-<tid>.lease` so stale-lease pruning never deletes an initializing lease.
3. Registers `KQ_FILTER_VNODE` (including `KQ_NOTE_DELETE | KQ_NOTE_RENAME`, with bounded `ENOENT | EINVAL` retry in `_open_kqueue_watch`) on `signals/<peer>.sig`, re-checks `signals/<peer>.seq` (`immediate_after_register`), re-attaches automatically if `.sig` or `signals/` is deleted/renamed, ignores spurious empty wakeups, and sleeps at 0% CPU until `.seq` advances.
4. On exit (`finally:`), unlinks the `.lease` file and releases `flock`.
5. **Cold Escalation Backup**: When `ring_peer` sends a hot wakeup with `event_ids`, a daemon `threading.Timer` (default `60s`) checks `store.is_event_acked(eid)` and escalates to `ColdDoorbell` if the event remains unacknowledged. `record_peer_wakeup_latency` stamps only the single newest qualifying unacked non-cold event with `event_id <= wake_seq`.

### Cold Fallback (`ColdDoorbell`)
- Passes notification title, body, and subtitle via `osascript` `argv` (`on run argv`). Notifications do not steal focus or auto-open deep links on click; `open_deeplink=True` is opt-in per mission after user consent.
- Truncates prompts **before** UTF-8 percent-encoding (`_truncate_pre_encode`), strips leading `/`, uses `claude://code/new?q=...` (with optional `&folder=...`) for Claude, and uses `codex://new?prompt=...&path=...` for Codex Desktop.
- `ColdDoorbell.ring_cold` and `record_event_delivery` record three independent cold-notification facts: `attempted`, `command_status`/`command_ok`/`command_returncode`, and `peer_acked` (initialized `false` until `pickup_inbox` claims the task).

## 4. MCP Tools (`consilium/mcp_server.py`)

Exposed identically over `consilium mcp-stdio` and `POST /mcp` (`127.0.0.1:18920`, `Authorization: Bearer <token>` authentication enabled by default—`?token=` query auth rejected with `401`—and `405 Allow: POST` on `GET /mcp`), totaling **11** tools (7 write, 4 read/wait/quota):
1. `consilium_status`
2. `consilium_create_mission`
3. `consilium_dispatch_task`
4. `consilium_pickup_inbox`
5. `consilium_report_progress`
6. `consilium_complete_task`
7. `consilium_requeue_task`
8. `consilium_request_review`
9. `consilium_wait`
10. `consilium_quota`
11. `consilium_close_mission`

## 5. Multi-Source Quota Extraction (`consilium/quota.py`)

`sync_local_quotas` aggregates quota remaining % and reset times locally (zero LLM token cost, best-effort under Seatbelt; suppressed by `CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1` for isolated demos/tests):
1. `~/.antigravity_cockpit/platform-quota-history-v1.sqlite` & `codex-quota-history-v1.sqlite` (read-only URI SQLite queries)
2. `~/.antigravity_cockpit/{codex,claude,google,accounts}_accounts.json` (JSON quota/reset fields)
3. `~/.codex/sessions/**/rollout-*.jsonl` (tail scan for latest `rate_limits.primary` 5h & `rate_limits.secondary` 7d `used_percent` + `resets_at`)
4. `~/.claude/{usage-latest,quota}.json` (structured windows or cached `anthropic-ratelimit-unified-*` headers)
5. Direct peer reporting via `consilium_quota(report_windows=[...])` (accepting `source="demo"` or normalizing other report sources to `"manual_report"`).

## 6. Dashboard, SSE & Installer Security (`consilium/dashboard.py`, `scripts/install.py`)

- **Offline `/ui` & Auth**: `ConsiliumDaemon` serves zero-CDN `/ui`, `/api/state`, `/api/stream`, `/api/login`, `/api/logout`, `/api/ui-ticket` (Bearer-only), `/ui/auth?ticket=...`, `/api/missions/<id>/close`, and `/api/tasks/<id>/requeue`. `SessionStore` holds server-side random sessions (12h TTL, random `csrf_token`, bound `token_version`), and `LoginTicketStore` issues 30s single-use `token_version`-bound tickets for `consilium ui` (which exits `1` with error + fallback URL if `open` fails).
- **CSRF & Security Headers**: Cookie-authenticated writes (`/api/missions/<id>/close`, `/api/tasks/<id>/requeue`, `/api/logout` with active session cookie) enforce exact daemon `Origin` (`http`, exact host & port -> `403`), `Content-Type: application/json` (`415`), and matching `X-Consilium-CSRF` (`403`). All JSON, HTML, redirect (`/` and `/ui/auth`), `202 /mcp`, and SSE responses include `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` (`no-cache, no-store` on SSE), with `frame-ancestors 'none';` in `/ui` CSP.
- **Hardened `EventHub` SSE**: Caps streams at `SSE_MAX_SUBSCRIBERS = 16` (`503` on 17th), enforces `10s` socket write timeouts, linearizes `publish_state` and `snapshot_with_version` via `_publish_lock` with server/client monotonic `state_version` guards, closes streams on token rotation or `/api/logout`, and uses `compute_state_fingerprint` (stripping volatile `age_seconds`/`claim_age_seconds`) to push `external_change` / `waiter_change` updates from silent `stdio`/CLI mutations.
- **Two-Phase Installer & Rotation**: `scripts/install.py` supports `--targets` (`claude_desktop`, `claude_code`, `codex`, `antigravity`, `launchd`, `all`) and `--skip-claude-desktop` (allowing non-desktop targets to install while `Claude.app` runs), validates JSON/TOML configs before writing, verifies Claude Code rollback with `claude mcp get consilium` + `.claude.json` equality check, and raises structured `TokenRotationError` (CLI exit `1` JSON stderr) on mid-rotation failures.
