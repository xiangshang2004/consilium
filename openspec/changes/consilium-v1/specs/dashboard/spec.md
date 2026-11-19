# Specification: Consilium Live Dashboard (`dashboard`)

## ADDED Requirements

### Requirement: Offline Localhost Dashboard UI (`/ui`) and 5-State Peer Indicators
`ConsiliumDaemon` MUST serve a single-page dashboard at `GET /ui` on `127.0.0.1` with zero external CDN dependencies (all CSS and JS inline). Peer cards MUST render the 5-state indicator model (`standby`, `working`, `online`, `offline`, `stale`) where idle peers (`claimed + running == 0`) with expired `last_seen_at` resolve to `offline` rather than `stale`. Task notes and timeline summaries MUST safely render inline Markdown (`renderInline` with `p.summary` fallback), empty kanban columns MUST collapse, 100% done mission bars MUST use the green completed style, `<20%` quota windows MUST use the low-quota warning color, `source="demo"` / `account_id="demo"` windows MUST display the `演示数据` badge, timeline items beyond 6 MUST fade behind `"显示更多"` / `"收起时间线"`, and mission close MUST use `.btn-subtle-danger`.

#### Scenario: Dashboard renders offline with Light/Dark/System theme support
- **WHEN** an authenticated user loads `GET /ui`
- **THEN** the page MUST render the Consilium header (title, daemon uptime, version, Light/Dark/System theme toggle), three peer status cards (`claude`, `chatgpt`, `antigravity`), the mission list with progress bars and review round indicators, expandable mission detail (status-grouped task cards with attempt/stale pills, reverse-chronological event timeline with hot/cold 3-fact badges and `claim_latency_ms`/ms wakeup latency, Markdown `final_summary`), and tabular numerals (`font-variant-numeric: tabular-nums`) without any external network requests.

#### Scenario: Peer cards render 5-state indicators including idle expired offline
- **WHEN** peers exist in `standby` (live waiter), `working` (active tasks, fresh), `online` (idle, fresh `last_seen_at`), `offline` (no `last_seen_at` or idle with expired `last_seen_at`), and `stale` (active tasks with expired task/heartbeat)
- **THEN** `/api/state` and `/ui` MUST classify and render each of the 5 states accurately, ensuring idle expired peers report `offline` rather than `stale`.

### Requirement: Snapshot API (`/api/state`) and Hardened Realtime SSE (`/api/stream`)
`ConsiliumDaemon` MUST expose authenticated `GET /api/state` returning a complete JSON snapshot (daemon metadata, peers, quotas with `"unknown"` / `null` when missing, missions, tasks, and events) and `GET /api/stream` streaming Server-Sent Events (`text/event-stream`) with `SSE_MAX_SUBSCRIBERS = 16` (`503` beyond capacity), a `10s` socket write timeout, `_publish_lock`-linearized monotonic `state_version` (guarded on both server output and frontend `lastStateVersion`), `compute_state_fingerprint`-based `external_change` / `waiter_change` detection for silent stdio/CLI mutations, and active stream termination on `token_version` change or `POST /api/logout`. The UI MUST automatically reconnect SSE and fall back to 3-second polling when SSE is unavailable.

#### Scenario: SSE enforces 16-subscriber cap and monotonic state_version
- **WHEN** 16 clients subscribe to `GET /api/stream` and concurrent mutations occur while a 17th client connects
- **THEN** the 17th client MUST receive HTTP `503`, while active subscribers MUST receive strictly monotonically increasing `state_version` frames.

#### Scenario: Silent stdio mutation emits external_change SSE update
- **WHEN** an out-of-band `run_stdio_server` or CLI process mutates `bus.db` without calling the HTTP daemon
- **THEN** `EventHub` / stream poller MUST detect the `compute_state_fingerprint` change and emit an `update` SSE frame (`reason="external_change"`).

#### Scenario: Token rotation or logout terminates active SSE streams
- **WHEN** `consilium rotate-token` bumps `token_version` or a user calls `POST /api/logout`
- **THEN** active SSE streams for rotated tokens or the logged-out session MUST terminate promptly.

### Requirement: Dashboard Authentication, Security Headers, and Confirmed Write Actions
The dashboard MUST enforce `Host` checks, server-side random `SessionStore` cookies (`consilium_session`, 12h TTL, `HttpOnly; SameSite=Strict`, bound `csrf_token` & `token_version`), and single-use `token_version`-bound `LoginTicketStore` tickets (`POST /api/ui-ticket` Bearer-only -> `GET /ui/auth?ticket=...` `302` to `/ui`, omitting `Set-Cookie` on invalid/rotated tickets; `consilium ui` exiting `1` with error + fallback URL if `open` fails). Cookie-authenticated writes (`POST /api/missions/<mission_id>/close`, `POST /api/tasks/<task_id>/requeue`, and `POST /api/logout` with an active session cookie) MUST enforce exact daemon `Origin` (`403` on missing/wrong-port/wrong-scheme), `Content-Type: application/json` (`415` otherwise), and matching `X-Consilium-CSRF` (`403` otherwise); Bearer requests bypass cookie-CSRF. All dashboard/API/redirect/`202 /mcp` responses MUST set `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` (`no-cache, no-store` on SSE), and `/ui` HTML MUST set `Content-Security-Policy` including `frame-ancestors 'none';`. Write actions MUST enforce scoped idempotency (`409`), `400` on invalid requeue state, and `delivery.warnings` post-commit fault isolation.

#### Scenario: Unauthenticated API access is rejected
- **WHEN** a client requests `GET /api/state`, `GET /api/stream`, or dashboard write actions without a valid Bearer token or session cookie
- **THEN** the daemon MUST return HTTP `401 Unauthorized`.

#### Scenario: Token login sets hardened session cookie
- **WHEN** a client posts the valid daemon token to `POST /api/login`
- **THEN** the daemon MUST return HTTP `200` with `Set-Cookie` containing `HttpOnly` and `SameSite=Strict`, and an initial `csrf_token`, allowing subsequent `/api/state` and `/api/stream` requests to authenticate via cookie.

#### Scenario: One-time login ticket and token rotation revocation
- **WHEN** a Bearer caller mints `/api/ui-ticket` and token rotation occurs before or after `/ui/auth` redemption
- **THEN** rotated/reused tickets MUST redirect `302` without `Set-Cookie`, previously issued sessions MUST fail with `401`, and active SSE streams MUST close.

#### Scenario: Cookie writes enforce exact Origin, JSON Content-Type, and X-Consilium-CSRF
- **WHEN** a cookie-authenticated `POST` to `/api/missions/<id>/close`, `/api/tasks/<id>/requeue`, or `/api/logout` omits `Origin`, uses a different port/scheme `Origin`, sends non-JSON `Content-Type`, or omits/mismatches `X-Consilium-CSRF`
- **THEN** the daemon MUST reject the write with `403` (Origin/CSRF) or `415` (Content-Type) without mutating `bus.db`.

#### Scenario: Security response headers apply across dashboard and API routes
- **WHEN** clients request `/ui`, `/`, `/ui/auth`, `/api/state`, `/api/stream`, or `POST /mcp` (`202`)
- **THEN** responses MUST include `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` (`no-cache, no-store` on SSE), and `/ui` MUST include `frame-ancestors 'none';` in `Content-Security-Policy`.
