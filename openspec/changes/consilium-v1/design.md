# Design: `consilium-v1`

## 1. Frozen v1 Protocol & Data Model

- **Peers & Branches**: `claude` (`claude/`), `chatgpt` (`codex/`), `antigravity` (`gemini/`).
- **Task State Machine**: `queued -> claimed -> running -> done | failed | blocked | cancelled`; stale reaper moves expired `claimed` (default 15m) or `running` (default 2h, heartbeat via `report_progress.updated_at`) back to `queued` and notifies `coordinator_peer` with `task_requeued`.
- **Mission State Machine**: `active <-> ready_to_report -> completed | cancelled | failed`.
- **Events**: `task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`, `task_requeued`, `mission_closed`.
- **Write Envelope & Idempotency**:
  - SQLite table `idempotency_keys(client_request_id PRIMARY KEY, operation, response_json, created_at)` ensures retries of `create_mission`, `dispatch_task`, `complete_task`, `report_progress`, `request_review`, and `close_mission` with the same `client_request_id` return the original committed result without duplicating side effects.
  - All write MCP tools return `{"committed": true, "result": <domain_result>, "delivery": <delivery_or_list>}` (retaining top-level domain keys for backward compatibility while guaranteeing the canonical `committed`/`result`/`delivery` contract). `ring_peer` exceptions are caught and surfaced in `delivery.error` while keeping `isError: false`.
  - Event delivery metadata (`delivery_mode`, `wakeup_latency_ms`, etc.) can be recorded on events so the dashboard timeline displays hot/cold badges and millisecond latencies.

## 2. Concurrency-Safe `.seq` & `kqueue` Wakeup

- Every update to `signals/<peer>.seq` acquires an exclusive lock on `signals/<peer>.seq.lock` (`threading.Lock` + `fcntl.flock(LOCK_EX)`).
- Inside the lock, `BusStore.sync_peer_seq` re-queries SQLite for authoritative `(MAX(event_id), unacked_events, queued_tasks)` and atomically replaces `<peer>.seq`.
- `ConsiliumDaemon.start()` and `run_stdio_server()` rebuild all three peers' `.seq` files from SQLite on startup so `/tmp` cleanup never hides pending work.
- `_wait_via_kqueue` subscribes to `KQ_NOTE_WRITE | KQ_NOTE_EXTEND | KQ_NOTE_ATTRIB | KQ_NOTE_DELETE | KQ_NOTE_RENAME`. On delete/rename it re-opens and re-watches `<peer>.sig`; on every wakeup it re-reads `<peer>.seq` and continues waiting if nothing changed.

## 3. Security, Tokens, and Atomic Installer

- Antigravity MCP uses `url` + `headers: {"Authorization": "Bearer <token>"}` (no `?token=` in URL).
- `consilium rotate-token` validates targets, updates already-configured client configs first, and atomically swaps `<state_dir>/auth.token` last; `ConsiliumDaemon` reloads `auth.token` on mismatch so rotation takes effect immediately.
- `scripts/install.py` executes in two strict phases: Phase 1 reads and validates all JSON/TOML configs (including quoted `[mcp_servers."consilium"]` and dotted `mcp_servers.consilium.*` duplicate checks), verifies `claude` CLI availability, and refuses `--apply` while `Claude.app` is running; Phase 2 writes atomically with timestamped backups, strips `CLAUDE_CONFIG_DIR` when calling `claude`, and rolls back `claude mcp` on `add` failure.

## 4. Offline Dashboard (`/ui`, `/api/state`, `/api/stream`)

- Served directly by `ConsiliumDaemon` (`consilium/dashboard.py`), 100% inline HTML/CSS/JS, zero external CDN.
- Authentication: `POST /api/login` validates Bearer/body token and sets `Set-Cookie: consilium_session=<hmac_token>; HttpOnly; SameSite=Strict; Path=/`; `POST /api/logout` clears the cookie. Existing `Host` and `Origin` checks apply to all `/ui` and `/api/*` endpoints.
- Realtime updates: `GET /api/stream` streams `text/event-stream` (`state` and `event` frames) driven by an in-daemon pub/sub hub triggered after DB writes and periodic heartbeats; frontend automatically reconnects and falls back to 3s polling of `GET /api/state`.
- Controlled write actions: `POST /api/missions/<mission_id>/close` and `POST /api/tasks/<task_id>/requeue`, both guarded by authentication, `Host`/`Origin`, and explicit UI double-confirmation modal.
