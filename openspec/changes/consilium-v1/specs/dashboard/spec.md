# Specification: Consilium Live Dashboard (`dashboard`)

## ADDED Requirements

### Requirement: Minimalist Offline Conversation UI (`/ui`), 5-State Peer Capsules, and Right Drawer
`ConsiliumDaemon` MUST serve a single-page minimalist conversation dashboard at `GET /ui` on `127.0.0.1` with zero external CDN dependencies (all CSS and JS inline) and Light/Dark/System theme support (`--bg-elevated` opaque surfaces for popovers/modals/palettes). The layout MUST organize into: (1) a retractable left `会诊列表` sidebar (responsive drawer closed by default at `<= 860px`), (2) a compact header + unclipped 3-peer status & active-account quota capsule strip (`.peer-subbar` for `Claude`, `Codex`, `Antigravity` using the 5-state model `standby`/`working`/`online`/`offline`/`stale` where idle peers with expired `last_seen_at` resolve to `offline`, displaying active-account `%` or `"账号未知"`/`"可能已切换"`, `演示数据` badge when `source="demo"`, `N号` account-pool count badge when `pool.length > 1`, and a click popover listing `pool` and `historical_accounts` labeled `"历史账号"` plus switch hints), (3) a centered conversation stream (`max-width: 720px`) displaying the mission banner + `.btn-subtle-danger` close control, chronological event turns (`"最后上报于 ..."` relative timing, hot/cold 3-fact badges, `claim_latency_ms`/ms latency, safe inline Markdown via `renderInline`, `.timeline-fade` fold beyond 6 events, collapsible `review_request`/`loop_guard_reached` strip, and `final_summary` card), (4) a closed-by-default 5-tab right detail drawer (`任务`, `工作树`, `模型与账号`, `子 Agent` showing per-peer `"该端未上报"`, and `文件` listing read-only `artifacts` paths), and (5) a read-only bottom handoff composer with a pure-frontend `/` command palette (`/mission`, `/files`, `/export`, `/tasks`, `/worktrees`, `/accounts`, `/subagents`, dismissible with `Esc`) that builds verified `claude://code/new?q=...` and `codex://new?prompt=...` deep links or uses failure-aware `copyTextSafe` clipboard prompts (never unverified `codex://threads/` links or hardcoded stale model lists). The login overlay MUST warn users to access `127.0.0.1:<port>/ui` rather than `localhost`.

#### Scenario: Dashboard renders minimalist conversation stream offline across Light/Dark and 1440px/390px viewports
- **WHEN** an authenticated user loads `GET /ui` on desktop (`1440px`) or narrow mobile (`390px`) in Light, Dark, or System theme
- **THEN** the page MUST render the retractable `会诊列表` sidebar (collapsed by default on `<= 860px`), compact topbar + unclipped `.peer-subbar` 3-peer capsules (`claude`, `chatgpt`, `antigravity`), centered conversation turns with inline Markdown, `claim_latency_ms`/hot/cold 3-fact pills, tabular numerals (`font-variant-numeric: tabular-nums`), read-only bottom handoff bar + `/` palette, and closed-by-default 5-tab right drawer (`任务`, `工作树`, `模型与账号`, `子 Agent`, `文件`) with zero external requests and zero horizontal overflow (`scrollWidth <= innerWidth`).

#### Scenario: Peer cards render 5-state indicators including idle expired offline
- **WHEN** peers exist in `standby` (live waiter), `working` (active tasks, fresh), `online` (idle, fresh `last_seen_at`), `offline` (no `last_seen_at` or idle with expired `last_seen_at`), and `stale` (active tasks with expired task/heartbeat)
- **THEN** `/api/state` and `/ui` MUST classify and render each of the 5 states accurately, ensuring idle expired peers report `offline` rather than `stale`.

#### Scenario: Pure-frontend composer, slash palette, and active-account pool badges behave truthfully
- **WHEN** a user opens `/`, runs `/mission`, `/files`, or `/export`, inspects the `2号` / `演示数据` peer popover, `"历史账号"` rows, or `模型与账号` / `子 Agent` / `文件` drawer tabs
- **THEN** `.slash-palette` MUST render on opaque `--bg-elevated` and close on `Esc`, verified `claude://code/new?q=...` / `codex://new?prompt=...` links MUST be generated without `codex://threads/` or hardcoded obsolete model strings, `copyTextSafe` MUST report clipboard failure honestly, and active-account quota plus read-only backup/historical account badges MUST match `list_quota_windows()`.

#### Scenario: Rename a conversation from its header
- **WHEN** a user opens a conversation with a long title or in a narrow window
- **THEN** a visible and keyboard-accessible “改名” button MUST appear next to the title in light and dark themes.
- **AND** the button MUST open the same inline editor as the title and the “⋯” menu. Enter saves, Escape cancels, and focus returns to the button.
- **AND** a live update that leaves the selected conversation and title unchanged MUST preserve focus on the button.

#### Scenario: Display the model used on an Antigravity task
- **WHEN** Antigravity's native execution log contains model information associated with the displayed task and attempt
- **THEN** its reported model or verified planning and execution models MUST appear on the reply and in the task details.
- **AND** if the available data contains only the application or Orchestrator name, the display MUST indicate that the specific model was not reported.

### Requirement: Snapshot API (`/api/state`) and Hardened Realtime SSE (`/api/stream`)
`ConsiliumDaemon` MUST expose authenticated `GET /api/state` returning a complete JSON snapshot (daemon metadata, peers, quotas with `"unknown"` / `null` when missing, missions, tasks, and events) and `GET /api/stream` streaming Server-Sent Events (`text/event-stream`) with `SSE_MAX_SUBSCRIBERS = 16` (`503` beyond capacity), `try/finally` subscriber slot release wrapping header/snapshot writes, a `10s` socket write timeout, `auth_token_ver` pinned from `_authenticate_request` and re-verified before headers AND before initial snapshot emission, `_publish_lock`-linearized monotonic `state_version` (including re-snapshotting inside `snapshot_with_version` on idle `external_change` / `waiter_change` frames, guarded on both server output and frontend `lastStateVersion`), `compute_state_fingerprint` ignoring ticking display fields (`age_seconds`, `claim_age_seconds`, `sample_age_seconds`, `reset_in_seconds`, `reset_in_human`, `earliest_reset_in_human`, `updated_at`), and active stream termination on `token_version` change or `POST /api/logout`. The UI MUST automatically reconnect SSE and fall back to 3-second polling when SSE is unavailable.

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
