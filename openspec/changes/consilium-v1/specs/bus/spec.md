# Specification: Consilium Collaboration Bus (`bus`)

## ADDED Requirements

### Requirement: Frozen v1 Peer, State Machine, and Event Contract
The bus MUST enforce the frozen v1 identifiers and lifecycles:
- Valid peers are `claude`, `chatgpt`, and `antigravity` (branch prefixes `claude/`, `codex/`, `gemini/`).
- Task statuses are `queued`, `claimed`, `running`, `done`, `failed`, `blocked`, and `cancelled`.
- Mission statuses are `active`, `ready_to_report`, `completed`, `cancelled`, and `failed`.
- Event kinds include `task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`, `task_stale`, `task_requeued`, and `mission_closed`.

#### Scenario: Closed mission rejects review requests
- **WHEN** a mission is closed (`completed`, `cancelled`, or `failed`) and a peer calls `request_review` on it
- **THEN** `request_review` MUST reject the request with `ValueError` and MUST NOT increment `current_round`.

#### Scenario: Review request validates task ownership
- **WHEN** a peer passes `task_id` to `request_review` where `task_id` does not belong to `mission_id`
- **THEN** `request_review` MUST reject the request with `ValueError`.

### Requirement: Attempt Fencing and Controlled Task Requeue
`pickup_inbox` MUST assign an incrementing `attempt_seq`, unique `attempt_id` (`A-<seq>-<hex>`), and `claim_latency_ms`; `consilium_report_progress` and `consilium_complete_task` MUST require the active `attempt_id` and MUST reject stale or mismatched attempts with `ValueError`. `requeue_task` (`consilium_requeue_task` and dashboard requeue) MUST permit only `claimed`, `running`, `failed`, and `blocked` tasks on open missions (and MUST be restricted to `coordinator_peer` on MCP), MUST reject `queued`, `done`, `cancelled`, or closed missions with `ValueError`, MUST archive the prior attempt into `attempt_history`, and MUST reset claim, progress, and result fields back to `queued`.

#### Scenario: Stale attempt_id is rejected after reclaim or requeue
- **WHEN** a task is claimed with `A-1-...`, reclaimed or requeued, and re-claimed with `A-2-...`, and the prior worker calls `report_progress` or `complete_task` with `A-1-...`
- **THEN** the bus MUST reject the write with `ValueError` and MUST leave `bus.db` untouched.

#### Scenario: Requeue archives attempt history and rejects terminal tasks
- **WHEN** the coordinator calls `consilium_requeue_task` on a `failed`, `blocked`, `claimed`, or `running` task vs. a `done`, `cancelled`, or `queued` task (or closed mission)
- **THEN** allowed statuses MUST return to `queued` with preserved `attempt_history`, while disallowed statuses and closed missions MUST raise `ValueError`.

### Requirement: Uniform Write Envelope, Delivery Isolation, and Scoped Idempotency
All 7 write MCP tools (`consilium_create_mission`, `consilium_dispatch_task`, `consilium_report_progress`, `consilium_complete_task`, `consilium_requeue_task`, `consilium_request_review`, `consilium_close_mission`) MUST return `{"committed": true, "result": {...}, "delivery": ...}` after SQLite commit, MUST isolate post-commit errors in `_safe_sync_peers_seq`, `ring_peer`, `record_event_delivery`, and `publish_state_update` into deduplicated `delivery.warnings` (and `post_commit_warnings` on `consilium_pickup_inbox`) while keeping `committed=true` and `isError=false`, and MUST scope `client_request_id` in `idempotency_keys` by `(operation, caller_scope, request_hash)` (including `peer:<default_peer>` for stdio `--peer` `consilium_dispatch_task`), raising `ConflictError` (HTTP `409` / MCP `isError=true`) on any mismatch.

#### Scenario: Notification and post-commit failures after commit return committed=true
- **WHEN** `.seq` sync, `ring_peer`, `record_event_delivery`, or `publish_state_update` raises an exception after SQLite commit during any write tool or dashboard mutation
- **THEN** the response MUST preserve `committed=true` (and `isError=false` on MCP) while surfacing deduplicated error strings in `delivery.warnings`.

#### Scenario: Retrying with the same client_request_id does not duplicate work
- **WHEN** a caller repeats a write tool with the same `client_request_id`, `operation`, `caller_scope`, and normalized parameters
- **THEN** the bus MUST return the original committed result and cached `delivery` receipt without duplicating side effects.

#### Scenario: Conflicting client_request_id reuse raises ConflictError
- **WHEN** the same `client_request_id` is reused with different parameters, a different operation, or a different `caller_scope`
- **THEN** the bus MUST raise `ConflictError` returning HTTP `409` or MCP `isError=true`.

### Requirement: Strictly Read-Only Wait (consilium_wait and consilium wait)
`consilium_wait` and CLI `consilium wait` MUST be strictly read-only, MUST NEVER claim tasks, acknowledge events, or write to `bus.db`, MUST strictly reject non-boolean `auto_pickup`, and MUST ignore `auto_pickup=true` / `--pickup` while returning `inbox: null`, `auto_pickup_ignored: true`, and `notice`.

#### Scenario: Wait ignores auto_pickup and --pickup without touching DB
- **WHEN** `consilium_wait(auto_pickup=true)` or `consilium wait --pickup` wakes on pending work
- **THEN** it MUST return `woke=true`, `inbox=null`, `auto_pickup_ignored=true`, and queued tasks and unacked events MUST remain untouched in `bus.db` until explicit `consilium_pickup_inbox`.

### Requirement: Locked, Rebuildable .seq, Atomic .lease, and Resilient kqueue Watch
Updates to `signals/<peer>.seq` MUST lock `<rendezvous_dir>/.<peer>.seq.lock` (outside `signals/`) with `fcntl.flock(LOCK_EX)` and re-query SQLite inside a single `BEGIN; ... COMMIT;` snapshot for authoritative `max(event_id)`, `unacked`, and `queued_tasks`, retrying transient `OSError(ENOENT | EINVAL)` up to 10 attempts during `.seq` writes and `_open_kqueue_watch` while reading `.seq` directly in `_read_peer_seq_unlocked` without recreating `signals/`. Waiter `.lease` creation MUST allocate `.tmp-<pid>-<tid>-<rand>` with `O_CREAT | O_EXCL`, acquire `flock(LOCK_EX | LOCK_NB)`, `fsync` JSON, and atomically `os.replace` to `.lease`. Daemon and stdio server startup MUST rebuild all peers' `.seq` files from SQLite. `kqueue` waiters MUST watch `KQ_NOTE_DELETE | KQ_NOTE_RENAME`, re-attach on deletion, ignore spurious wakeups when `.seq` did not advance, and treat `queued_tasks > 0` as pending work during pre-check.

#### Scenario: Concurrent writers converge to authoritative DB state
- **WHEN** concurrent processes/threads write `.seq` at different speeds while `signals/` is concurrently deleted and events are committed
- **THEN** the final `.seq` file MUST equal the true SQLite `MAX(event_id)`, `unacked`, and `queued_tasks`.

#### Scenario: Atomic .lease creation prevents pruner race
- **WHEN** a waiter creates a `.lease` file concurrently with stale-lease pruning
- **THEN** `_acquire_lease` MUST create a `.tmp-*` file excluded from pruning, hold `flock(LOCK_EX | LOCK_NB)`, write valid JSON, and `os.replace` atomically to `.lease`.

#### Scenario: Deleted signals directory recovers on server startup
- **WHEN** `signals/` is deleted and `run_stdio_server` or `ConsiliumDaemon` starts
- **THEN** `.seq` files for all peers MUST be rebuilt from SQLite.

#### Scenario: Deleting .sig during wait does not drop wakeup
- **WHEN** `signals/<peer>.sig` or `signals/` is deleted while a waiter is blocked in `kqueue` and a new event is then delivered
- **THEN** the waiter MUST re-open `.sig` and wake cleanly.

### Requirement: Stale claimed and running Task Reaper
`release_stale_claims` MUST auto-requeue `claimed` tasks exceeding `claim_timeout_sec` (default 15 minutes) back to `queued` (archiving the attempt into `attempt_history`), emitting `task_requeued` to `coordinator_peer` and waking `assignee_peer`; for `running` tasks whose `updated_at` heartbeat exceeds `running_timeout_sec` (default 2 hours), `release_stale_claims` MUST NOT auto-requeue, and MUST instead set `stale_at` (preserving `attempt_id` and skipping already-stale tasks) and emit `task_stale` to both `coordinator_peer` and `assignee_peer`. `ConsiliumDaemon` MUST schedule `run_reaper_once()` periodically on background `_reaper_thread`, and `pickup_inbox` MUST run `release_stale_claims` before claiming.

#### Scenario: Expired running task is marked stale without auto-requeue
- **WHEN** a `running` task has an expired `updated_at` timestamp and `ConsiliumDaemon` runs its reaper
- **THEN** the task MUST stay `running` with `stale_at` set and preserved `attempt_id`, and both `coordinator_peer` and `assignee_peer` MUST receive a `task_stale` event.

#### Scenario: Expired claimed task is auto-requeued by daemon reaper thread
- **WHEN** a `claimed` task exceeds `claim_timeout_sec` while `ConsiliumDaemon` is running
- **THEN** the background `_reaper_thread` MUST auto-requeue the task to `queued`, emit `task_requeued` to `coordinator_peer`, and wake `assignee_peer`.

### Requirement: Header-Based Tokens, Token Rotation, and Two-Phase Atomic Installer
- HTTP endpoints MUST authenticate using `Authorization: Bearer <token>` (plus session cookie on UI/API routes) and MUST reject `?token=` query authentication with HTTP `401 Unauthorized`; `consilium serve --help` MUST reflect Bearer-only auth.
- Antigravity MCP installation MUST configure `Authorization: Bearer <token>` in `headers` instead of `?token=` in URL.
- `consilium rotate-token` MUST update configured clients first and atomically swap `<state_dir>/auth.token` last; the running daemon MUST reload the rotated token. Mid-rotation failure MUST raise `TokenRotationError` carrying `updated_targets`, `remaining_targets`, `server_token_rotated`, `backup_root`, and `rollback_instructions`, and CLI `consilium rotate-token` MUST print the JSON report to stderr and exit `1`.
- `scripts/install.py` MUST support `--targets` and `--skip-claude-desktop` (allowing non-desktop targets to apply while `Claude.app` runs, while refusing `--apply` if `claude_desktop` is selected and `Claude.app` is running); MUST validate all target configs (including quoted `[mcp_servers."consilium"]` and dotted `mcp_servers.consilium.*` duplicates) and CLI availability before writing any file; MUST strip `CLAUDE_CONFIG_DIR` when invoking `claude`; MUST support `--rendezvous-dir`; and MUST restore the previous Claude MCP entry if `add` fails after `remove` and verify restoration via `claude mcp get consilium` and `.claude.json` equality check.

#### Scenario: Query token authentication is rejected with 401
- **WHEN** an HTTP client sends `?token=<valid_token>` without an `Authorization: Bearer` header or session cookie to `/mcp`, `/api/state`, or `/api/ui-ticket`
- **THEN** the server MUST reject the request with HTTP `401 Unauthorized`.

#### Scenario: Validation failure leaves all files untouched
- **WHEN** `~/.codex/config.toml` has an outside duplicate `consilium` table or dotted key and `--apply` runs
- **THEN** the installer MUST fail and leave all other config files byte-for-byte unchanged.

#### Scenario: Running Claude.app blocks claude_desktop apply unless excluded
- **WHEN** `Claude.app` is running and `--apply` runs with `claude_desktop` selected vs. `--targets claude_code` or `--skip-claude-desktop`
- **THEN** selecting `claude_desktop` MUST refuse before modifying any file, while excluding `claude_desktop` MUST apply cleanly to the selected targets.

#### Scenario: Failed Claude MCP add restores prior entry and verifies rollback
- **WHEN** `claude mcp add` fails after `claude mcp remove`
- **THEN** the installer MUST restore the previously captured `consilium` entry and verify restoration via `claude mcp get consilium` and `.claude.json` equality check (failing loudly if rollback verification fails).

#### Scenario: Token rotation mid-failure surfaces structured TokenRotationError
- **WHEN** updating a client config fails mid-rotation after an earlier target succeeded
- **THEN** `rotate_token` MUST raise `TokenRotationError` with `updated_targets`, `remaining_targets`, `server_token_rotated`, and `rollback_instructions`, and `consilium rotate-token` MUST print the JSON report to stderr and exit `1`.

### Requirement: Cold Notification Deep Links, Observability, and Strict Input/Quota Hygiene
- `ColdDoorbell.build_deeplink` MUST use `claude://code/new?q=...` even without `workspace_path`, and `codex://new?prompt=...&path=...` with URL-encoded absolute `path` when `workspace_path` is provided.
- `ColdDoorbell.ring_cold` and `record_event_delivery` MUST record three independent cold-notification facts (`attempted`, `command_status`/`command_ok`/`command_returncode`, `peer_acked` initialized `false` until `pickup_inbox`). `record_peer_wakeup_latency` MUST stamp only the single newest qualifying unacked non-cold event with `event_id <= wake_seq`.
- `SKILL.md` MUST instruct manual switch + `consilium_pickup_inbox` rather than claiming pressing Return joins automatically, and MUST note periodic `consilium_report_progress` updates on long tasks and strictly read-only `consilium_wait`.
- Quota posture MUST return `dispatch_recommended: null` when no quota windows exist or when active-account identity is unknown/stale (`current_account_unknown` / `account_switch_suspected` / empty active windows), MUST preserve dynamic window lengths (`window_minutes`), MUST avoid treating `limit_id` as `account_id`, MUST ignore expired exhausted records (`reset_at <= now`), MUST accept `source="demo"` (normalizing other report sources to `"manual_report"`), MUST suppress default local quota harvesting when `CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1`, and MUST never retain raw Codex rollout lines.
- Local Cockpit harvesting MUST follow a strict read-only whitelist (`codex_accounts.json` metadata `id`/`email`/`plan_type`/`current_account_id`, `codex-quota-history-v1.sqlite`, `platform-quota-history-v1.sqlite`), MUST NOT open encrypted `accounts/` / `codex_accounts/` / `claude_accounts/` blobs or `current_account.json`, MUST anonymize emails/identities to SHA-256 prefixed refs (`_hash_account_ref`), and MUST sync `quota_accounts` with `is_current`/`is_historical`. `list_quota_windows` MUST aggregate `min_remaining_percent` and `dispatch_recommended` solely from the active account while exposing read-only `active_account_id`, `active_account_alias`, `current_account_unknown`, `account_switch_suspected`, `pool`, `pool_has_capacity`, `best_backup`, and `switch_hint` (explicitly preserving non-silent switching: zero account-switch automation).
- HTTP/RPC handling MUST validate batch items, `params` shapes, `Content-Length`, strict booleans, catch `ValueError` in `_is_valid_origin` and `_is_exact_daemon_origin` on malformed/non-numeric Origin ports (`403`), set `close_connection = True` on error responses (`status >= 400`) to prevent keep-alive corruption, and enforce a 30s socket read timeout. `resolve_socket_path` MUST support `CONSILIUM_SOCKET_FALLBACK_DIR` override so tests can isolate Unix socket shortening without creating leftover directories in `/tmp`.

#### Scenario: Deep link encodes Codex path and Claude fallback route
- **WHEN** `ColdDoorbell.build_deeplink` generates links for `chatgpt` with `workspace_path` and for `claude` without `workspace_path`
- **THEN** the Codex link MUST use `codex://new?prompt=...&path=<encoded>` and the Claude link MUST use `claude://code/new?q=...`.

#### Scenario: Cold notification records three independent delivery facts and claim_latency_ms
- **WHEN** a task is dispatched with cold notification and subsequently claimed via `pickup_inbox`
- **THEN** the delivery record MUST reflect `attempted`, `command_status`/`command_ok`, and `peer_acked` transitioning `false -> true`, and `pickup_inbox` MUST record `claim_latency_ms`.

#### Scenario: Missing quota windows yield null dispatch_recommended
- **WHEN** a peer has no recorded quota windows in `bus.db`
- **THEN** `list_quota_windows` MUST report `dispatch_recommended` as `null` (`None`).

#### Scenario: Multi-account quota posture isolates active account and surfaces backup pool hints
- **WHEN** a peer has multiple synced or reported accounts (for example, active `账号 A` exhausted at `0%` and backup `账号 B` at `85%`, or `is_historical=True` from history SQLite only)
- **THEN** `list_quota_windows` MUST compute `min_remaining_percent=0.0` and `dispatch_recommended=False` from `账号 A` alone, populate `pool` (marking historical-only rows with `is_historical=True`), set `pool_has_capacity=True`, and emit `best_backup` + `switch_hint` without mixing backup quota into the active account's `min_remaining_percent`.

#### Scenario: Malformed Origin port returns clean 403 and closes keep-alive connection
- **WHEN** a request presents a malformed `Origin` with a non-numeric port (such as `http://127.0.0.1:notaport`) or an HTTP error (`status >= 400`) occurs on a persistent connection
- **THEN** `_is_valid_origin` / `_is_exact_daemon_origin` MUST return `False` (`403`) without raising an unhandled `ValueError`, and `_send_json` MUST set `close_connection = True`.
