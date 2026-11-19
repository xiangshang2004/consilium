# Specification: Consilium Live Dashboard (`dashboard`)

## ADDED Requirements

### Requirement: Offline Localhost Dashboard UI (`/ui`)
`DeskBellDaemon` MUST serve a single-page dashboard at `GET /ui` on `127.0.0.1` with zero external CDN dependencies (all CSS and JS inline).

#### Scenario: Dashboard renders offline with Light/Dark/System theme support
- **WHEN** an authenticated user loads `GET /ui`
- **THEN** the page MUST render the Consilium header (title, daemon uptime, version, Light/Dark/System theme toggle), three peer status cards (`claude`, `chatgpt`, `antigravity`), the mission list with progress bars and review round indicators, expandable mission detail (status-grouped task cards, reverse-chronological event timeline with hot/cold badges and ms wakeup latency, Markdown `final_summary`), and tabular numerals (`font-variant-numeric: tabular-nums`) without any external network requests.

### Requirement: Snapshot API (`/api/state`) and Realtime SSE (`/api/stream`)
`DeskBellDaemon` MUST expose authenticated `GET /api/state` returning a complete JSON snapshot (daemon metadata, peers, quotas with `"unknown"` / `null` when missing, missions, tasks, and events) and `GET /api/stream` streaming Server-Sent Events (`text/event-stream`) in event order, while the UI automatically reconnects SSE and falls back to 3-second polling when SSE is unavailable.

#### Scenario: SSE delivers ordered live updates
- **WHEN** a client subscribes to `GET /api/stream` and mission/task mutations occur
- **THEN** the daemon MUST push ordered SSE events reflecting the state transitions.

### Requirement: Dashboard Authentication and Restricted Confirmed Write Actions
The dashboard MUST enforce existing `Host` and `Origin` checks, support token login via `POST /api/login` exchanging the bearer token for an `HttpOnly; SameSite=Strict` cookie (and `POST /api/logout` to clear it), default to read-only, and expose only two write actions (`POST /api/missions/<mission_id>/close` and `POST /api/tasks/<task_id>/requeue`), both requiring explicit double confirmation in the UI.

#### Scenario: Unauthenticated API access is rejected
- **WHEN** a client requests `GET /api/state`, `GET /api/stream`, or dashboard write actions without a valid Bearer token or session cookie
- **THEN** the daemon MUST return HTTP `401 Unauthorized`.

#### Scenario: Token login sets hardened session cookie
- **WHEN** a client posts the valid daemon token to `POST /api/login`
- **THEN** the daemon MUST return HTTP `200` with `Set-Cookie` containing `HttpOnly` and `SameSite=Strict`, allowing subsequent `/api/state` and `/api/stream` requests to authenticate via cookie.
