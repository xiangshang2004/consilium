# Specification: Consilium Collaboration Bus (`bus`)

## ADDED Requirements

### Requirement: Frozen v1 Peer, State Machine, and Event Contract
The bus MUST enforce the frozen v1 identifiers and lifecycles:
- Valid peers are `claude`, `chatgpt`, and `antigravity` (branch prefixes `claude/`, `codex/`, `gemini/`).
- Task statuses are `queued`, `claimed`, `running`, `done`, `failed`, `blocked`, and `cancelled`.
- Mission statuses are `active`, `ready_to_report`, `completed`, `cancelled`, and `failed`.
- Event kinds include `task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`, `task_requeued`, and `mission_closed`.

#### Scenario: Closed mission rejects review requests
- **WHEN** a mission is closed (`completed`, `cancelled`, or `failed`) and a peer calls `request_review` on it
- **THEN** `request_review` MUST reject the request with `ValueError` and MUST NOT increment `current_round`.

#### Scenario: Review request validates task ownership
- **WHEN** a peer passes `task_id` to `request_review` where `task_id` does not belong to `mission_id`
- **THEN** `request_review` MUST reject the request with `ValueError`.

### Requirement: Uniform Write Envelope, Delivery Isolation, and Idempotency
All write MCP tools (`consilium_create_mission`, `consilium_dispatch_task`, `consilium_report_progress`, `consilium_complete_task`, `consilium_request_review`, `consilium_close_mission`) MUST return `{"committed": true, "result": {...}, "delivery": ...}` after SQLite commit, MUST catch `ring_peer` errors into `delivery.error` with `isError=false`, and MUST accept optional `client_request_id` to guarantee idempotent retries.

#### Scenario: Notification failure after commit returns committed=true
- **WHEN** `ring_peer` raises an exception during `consilium_create_mission` or `consilium_dispatch_task`
- **THEN** the MCP response MUST have `isError=false`, `committed=true`, and a `delivery` object containing `"error"`.

#### Scenario: Retrying with the same client_request_id does not duplicate work
- **WHEN** a caller repeats `consilium_create_mission`, `consilium_dispatch_task`, or `consilium_complete_task` with the same `client_request_id`
- **THEN** the bus MUST return the original committed result without creating a second mission/task or failing duplicate completion.

### Requirement: Non-Destructive Default Wait (`auto_pickup=false`)
MCP `consilium_wait` MUST default `auto_pickup` to `false` (matching CLI `consilium wait`) so cancelled or timed-out waits never silently claim tasks or acknowledge events.

#### Scenario: Task remains queued after wait timeout
- **WHEN** `consilium_wait` is called without `auto_pickup` and times out, and a task is subsequently dispatched to that peer
- **THEN** the task MUST remain `queued` until the peer explicitly calls `consilium_pickup_inbox`.

### Requirement: Locked, Rebuildable `.seq` and Resilient `kqueue` Watch
Updates to `signals/<peer>.seq` MUST lock `signals/<peer>.seq.lock` with `fcntl.flock(LOCK_EX)` and re-query SQLite inside the lock for authoritative `max(event_id)`, `unacked`, and `queued_tasks`. Daemon and stdio server startup MUST rebuild all peers' `.seq` files from SQLite. `kqueue` waiters MUST watch `KQ_NOTE_DELETE | KQ_NOTE_RENAME`, re-attach on deletion, ignore spurious wakeups when `.seq` did not advance, and treat `queued_tasks > 0` as pending work during pre-check.

#### Scenario: Concurrent writers converge to authoritative DB state
- **WHEN** concurrent processes/threads write `.seq` at different speeds while events are committed
- **THEN** the final `.seq` file MUST equal the true SQLite `MAX(event_id)`, `unacked`, and `queued_tasks`.

#### Scenario: Deleted signals directory recovers on server startup
- **WHEN** `signals/` is deleted and `run_stdio_server` or `ConsiliumDaemon` starts
- **THEN** `.seq` files for all peers MUST be rebuilt from SQLite.

#### Scenario: Deleting .sig during wait does not drop wakeup
- **WHEN** `signals/<peer>.sig` is deleted while a waiter is blocked in `kqueue` and a new event is then delivered
- **THEN** the waiter MUST re-open `.sig` and wake cleanly.

### Requirement: Stale `claimed` and `running` Task Reaper
`release_stale_claims` MUST re-queue both `claimed` tasks exceeding the claim timeout (default 15 minutes) and `running` tasks whose `updated_at` heartbeat exceeds the running timeout (default 2 hours), emitting a `task_requeued` event to `coordinator_peer`. `ConsiliumDaemon` MUST run this reaper periodically on a background thread, and `pickup_inbox` MUST run it before claiming.

#### Scenario: Expired running task is re-queued by daemon reaper
- **WHEN** a `running` task has an expired `updated_at` timestamp and `ConsiliumDaemon` runs its reaper
- **THEN** the task MUST return to `queued` and `coordinator_peer` MUST receive a `task_requeued` event.

### Requirement: Header-Based Tokens, Token Rotation, and Two-Phase Atomic Installer
- Antigravity MCP installation MUST configure `Authorization: Bearer <token>` in `headers` instead of `?token=` in URL.
- `consilium rotate-token` MUST update configured clients first and atomically swap `<state_dir>/auth.token` last; the running daemon MUST reload the rotated token.
- `scripts/install.py` MUST validate all target configs (including quoted `[mcp_servers."consilium"]` and dotted `mcp_servers.consilium.*` duplicates), CLI availability, and `Claude.app` running state before writing any file; MUST refuse `--apply` when `Claude.app` is running; MUST strip `CLAUDE_CONFIG_DIR` when invoking `claude`; MUST support `--rendezvous-dir`; and MUST restore the previous Claude MCP entry if `add` fails after `remove`.

#### Scenario: Validation failure leaves all files untouched
- **WHEN** `~/.codex/config.toml` has an outside duplicate `consilium` table or dotted key and `--apply` runs
- **THEN** the installer MUST fail and leave all other config files byte-for-byte unchanged.

#### Scenario: Running Claude.app blocks apply
- **WHEN** `Claude.app` is running and `--apply` runs
- **THEN** the installer MUST refuse with an error before modifying any file.

#### Scenario: Failed Claude MCP add restores prior entry
- **WHEN** `claude mcp add` fails after `claude mcp remove`
- **THEN** the installer MUST restore the previously captured `consilium` entry.

### Requirement: Cold Notification Deep Links and Strict Input/Quota Hygiene
- `ColdDoorbell.build_deeplink` MUST use `claude://code/new?q=...` even without `workspace_path`, and `codex://new?prompt=...&path=...` with URL-encoded absolute `path` when `workspace_path` is provided.
- `SKILL.md` MUST instruct manual switch + `consilium_pickup_inbox` rather than claiming pressing Return joins automatically.
- Quota posture MUST return `dispatch_recommended: null` when no quota windows exist, preserve dynamic window lengths (`window_minutes`), avoid treating `limit_id` as `account_id`, ignore expired exhausted records (`reset_at <= now`), and never retain raw Codex rollout lines.
- HTTP/RPC handling MUST validate batch items, `params` shapes, `Content-Length`, strict booleans, and enforce a 30s socket read timeout.

#### Scenario: Deep link encodes Codex path and Claude fallback route
- **WHEN** `ColdDoorbell.build_deeplink` generates links for `chatgpt` with `workspace_path` and for `claude` without `workspace_path`
- **THEN** the Codex link MUST use `codex://new?prompt=...&path=<encoded>` and the Claude link MUST use `claude://code/new?q=...`.

#### Scenario: Missing quota windows yield null dispatch_recommended
- **WHEN** a peer has no recorded quota windows in `bus.db`
- **THEN** `list_quota_windows` MUST report `dispatch_recommended` as `null` (`None`).
