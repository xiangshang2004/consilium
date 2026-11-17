# Specification: Collaboration Bus (`bus`)

## Requirements

### Requirement: Atomic Task Claiming
`pickup_inbox` MUST execute inside an `IMMEDIATE` SQLite transaction so that concurrent workers calling `pickup_inbox` for the same `peer_id` cannot double-claim the same `queued` task.

#### Scenario: Concurrent inbox pickup
- **WHEN** two workers for peer `claude` concurrently call `pickup_inbox(peer_id="claude", claim_tasks=True)` while 1 `queued` task exists
- **THEN** exactly one caller receives that task in `claimed_tasks` with status `claimed`
- **AND** the second caller receives an empty `claimed_tasks` list for that task.

### Requirement: Cross-Peer Loop Guard
`request_review` MUST enforce `missions.current_round < missions.max_rounds` atomically before incrementing `current_round` and emitting a `review_request` event.

#### Scenario: Max rounds reached
- **WHEN** `current_round` equals `max_rounds` and a peer calls `request_review`
- **THEN** `request_review` MUST return `allowed=False`, MUST NOT increment `current_round`, and MUST emit a `loop_guard_reached` event to `coordinator_peer`.

### Requirement: Race-Free Hot Wakeup (`kqueue` + Leases) & Safe Cold Fallback
`wait_for_doorbell` MUST work inside macOS Seatbelt sandboxes via `kqueue` `KQ_FILTER_VNODE`, MUST detect events created either before or during registration, MUST prune dead/expired waiter leases, and `ring_peer` MUST fall back to non-focus-stealing notification (`ColdDoorbell`) when `woken_count == 0`.

#### Scenario: Standby worker woken via kqueue signal pulse
- **WHEN** a standby worker blocks in `wait_for_doorbell` and another peer calls `ring_peer`
- **THEN** `ring_peer` returns `delivery="hot_wakeup"` and `wait_for_doorbell` wakes promptly with `source="kqueue"` and cleans up its lease file.

#### Scenario: No active waiter falls back safely
- **WHEN** no waiter is active for `peer_id` and `ring_peer` runs with `dry_run=True`
- **THEN** `ring_peer` returns `delivery="cold_notification"` with a properly encoded deep link and performs no live GUI side effects.

### Requirement: Runtime Directory Ownership Hardening
`resolve_home_dir` MUST reject symlinked directories and directories not owned by the current UID (`os.lstat` check) and enforce `0o700` permissions.

### Requirement: Localhost HTTP Transport Hardening
The Streamable HTTP server MUST bind to `127.0.0.1` by default, validate the `Host` header, reject non-local `Origin` headers with `403`, and enforce `Authorization: Bearer` when `require_token=True`.
