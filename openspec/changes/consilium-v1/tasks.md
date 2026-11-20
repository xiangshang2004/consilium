# Tasks: `consilium-v1`

> Supersedes archived `2026-11-19-init-deskbell`. Stages marked with `★` require review or explicit user approval before crossing.

## Stage 0: OpenSpec Baseline
- [x] 0.1 Archive completed `init-deskbell` change.
- [x] 0.2 Author `openspec/changes/consilium-v1/` (`proposal.md`, `design.md`, `specs/bus/spec.md`, `specs/dashboard/spec.md`, `tasks.md`) freezing v1 contracts.

## Stage 1: R006 Reliability & Security Fixes (Separate Commit)
- [x] 1.1 Default `consilium_wait` `auto_pickup` to `false` (R005 3.1) + strict boolean/RPC/HTTP validation (P2.3). (superseded in R010 by strictly read-only wait; see Stage 2.5)
- [x] 1.2 Add `signals/<peer>.seq.lock` (`flock`) + DB re-query inside lock + startup `.seq` rebuild + kqueue `NOTE_DELETE|NOTE_RENAME` recovery + `queued_tasks` pre-check (R005 3.2, P2.1). (R010 moved lock to `<rendezvous_dir>/.<peer>.seq.lock`; see Stage 2.5)
- [x] 1.3 Switch Antigravity config to `headers` (remove `?token=`), add `consilium rotate-token` with live token reload (R005 3.3).
- [x] 1.4 Extend `release_stale_claims` to `claimed` (15m) + `running` (2h) emitting `task_requeued`, and wire into daemon background thread + `pickup_inbox` (R005 3.4). (superseded in R010: running tasks emit `task_stale` without auto-requeue; see Stage 2.5)
- [x] 1.5 Unify write response envelope (`committed`/`result`/`delivery`), isolate `ring_peer` errors, add `client_request_id` idempotency (R005 3.5 & Roadmap §2).
- [x] 1.6 Fix `ColdDoorbell.build_deeplink` (`claude://code/new`, `codex://new?prompt=...&path=...`) and update `SKILL.md` (R005 3.6).
- [x] 1.7 Refactor `scripts/install.py` to two-phase validate-then-write with running Claude.app refusal, expanded Codex duplicate checks, Claude MCP rollback, `--rendezvous-dir`, and `CLAUDE_CONFIG_DIR` isolation (R005 3.7).
- [x] 1.8 Fix R005 P2 items (closed-mission `request_review` guard, quota `null`/expired/`window_minutes`/`limit_id`/isolation) + A-level demo findings (`CONSILIUM_RENDEZVOUS_DIR` precedence, `peers` activity tracking).
- [x] 1.9 Add regression tests for all 7 P1s + P2s, verify full test suite & `13_Consilium` Level-A demo pass, commit cleanly (author `2084413277`), and write `reviews/R006-antigravity-r005-fixes.md`.
- [x] ★ **Gate 1**: Submit R006 for Claude and Codex review approval. (Passed 2026-11-20 per [R014](../../../reviews/R014-claude-r013-review.md) and Codex independent review up to `6e308aa`.)

## Stage 2: Consilium Live Dashboard (Separate Commit)
- [x] 2.1 Implement `consilium/dashboard.py` and mount `/ui`, `/api/login`, `/api/logout`, `/api/state`, `/api/stream` (SSE), `/api/missions/<id>/close`, `/api/tasks/<id>/requeue` in `ConsiliumDaemon`.
- [x] 2.2 Record delivery/wakeup metadata on events so dashboard displays hot/cold badges and ms wakeup latency.
- [x] 2.3 Write unit/wire tests for dashboard API shape, cookie auth, Host/Origin checks, write actions, and ordered SSE streaming.
- [x] 2.4 Drive dashboard live with Level-A demo script, capture Light & Dark mode screenshots (and live progression evidence) into `13_Consilium/03_演示记录/`, commit cleanly (author `2084413277`), and write `reviews/R007-antigravity-dashboard.md`.
- [ ] ★ **Gate 2**: User reviews Light & Dark screenshots and confirms dashboard appearance. (2026-11-20: awaiting Claude/Codex R013 re-check and final user sign-off on updated 2026-11-20 screenshots after R012 + drawer/command alignment; prior R011/chat draft approved at `fd89018` with direction to keep Claude looped in for subsequent optimizations.)

## Stage 2.5: R008/R009 Remediation & Closed-Loop Sync (R010, Before Crossing Gates 1 & 2)
- [x] 2.5.1 Harden truth & wakeup layers: move `.seq` lock to `<rendezvous_dir>/.<peer>.seq.lock`, single-snapshot `query_peer_seq_counts`, `OSError(ENOENT|EINVAL)` retries, atomic `.lease`, strictly read-only `wait`, `attempt_id` fencing + `attempt_history`, `task_stale` for expired `running` tasks on background `_reaper_thread`, scoped `idempotency_keys` (`ConflictError`/409), `consilium_requeue_task`, and `delivery.warnings` post-commit fault isolation.
- [x] 2.5.2 Harden server, dashboard, and installer: Bearer-only HTTP auth (`?token=` 401), random `SessionStore` + `token_version`-bound `/api/ui-ticket` + `/ui/auth` + `consilium ui` failure handling, exact `Origin` + `application/json` + `X-Consilium-CSRF` (403/415), `nosniff`/`Referrer-Policy`/`Cache-Control`/`frame-ancestors 'none'`, SSE 16-cap + monotonic `state_version` + `external_change` fingerprint + rotation stream termination, 5-state peer indicators (`offline` vs `stale`) + R008 A/B UI rules, `--targets`/`--skip-claude-desktop`, verified Claude rollback, and `TokenRotationError`.
- [x] 2.5.3 Add `tests/test_r010_regressions.py` covering all R008/R009 items (1–16) and pass full 59-test `unittest` suite under `CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1`.
- [x] 2.5.4 Update `run_demo_a.py` & `capture_dashboard.mjs`, generate & `ffprobe`/visually verify `2026-11-19` Light/Dark Retina PNGs + live MP4 + `demo_a_timeline.md`, sync `openspec/changes/consilium-v1/` & docs, write `reviews/R010-antigravity-r009-fixes.md`, and commit topically (preserving `★ Gate 1` and `★ Gate 2` unchecked).

## Stage 2.6: R011 P3 Polish, Q4 Multi-Account Quota & Minimalist Chat /ui (R013)
- [x] 2.6.1 Harden HTTP/Origin and test socket isolation (R011 P3-1..4): catch `ValueError` in `_is_valid_origin` and `_is_exact_daemon_origin` on malformed ports (`403`), set `close_connection = True` on `_send_json` for `status >= 400`, add `CONSILIUM_SOCKET_FALLBACK_DIR` in `resolve_socket_path` (updating `test_wakeup_and_cold_fallback.py`), and surface explicit `127.0.0.1` vs `localhost` login warning in `/ui` and `README.md`.
- [x] 2.6.2 Implement Q4 read-only Cockpit quota whitelist and active-account aggregation: restrict `consilium/quota.py` to `codex_accounts.json` metadata (`id`/`email`/`plan_type`/`current_account_id`) plus `codex-quota-history-v1.sqlite` & `platform-quota-history-v1.sqlite`, SHA-256 hash account refs (`_hash_account_ref`), sync `quota_accounts` (`is_current`/`is_historical`), and aggregate active-account posture (`active_account_id`, `active_account_alias`, `current_account_unknown`, `account_switch_suspected`, `pool`, `pool_has_capacity`, `best_backup`, `switch_hint`) in `BusStore.list_quota_windows`.
- [x] 2.6.3 Redesign `/ui` (`consilium/dashboard.py`) as minimalist conversation stream (R011 Gate 2 direction + 6 prototype fixes + Q1 read-only scope): retractable `会诊列表` sidebar (responsive closed on `<= 860px`), unclipped `.peer-subbar` 3-peer capsules + popover, centered conversation turns + `.timeline-fade`, read-only handoff composer + `/` palette (`--bg-elevated`, `Esc` close, `claude://code/new?q=` / `codex://new?prompt=` links only, zero stale hardcoded models), closed-by-default 5-tab right drawer (`任务`, `工作树`, `账号池`, `产物`, `子 Agent`), and full desktop `1440px` + mobile `390px` responsive polish.
- [x] 2.6.4 Add `tests/test_r011_regressions.py` and update `tests/test_r010_regressions.py`, pass full `unittest` suite both with and without `CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1`, re-capture & visually/`ffprobe` verify `2026-11-20` Light/Dark `1440px`/`390px` PNGs + live SSE MP4, sync OpenSpec/READMEs, write `reviews/R013-antigravity-chat-ui-and-q4.md` (reserving `R012` for Codex), and commit topically (keeping `★ Gate 1` & `★ Gate 2` unchecked).

## Stage 2.7: Codex R012 Blocker Remediation & Claude Checklist Alignment (R013)
- [x] 2.7.1 Resolve R012 P1 #1 & Skill param: enforce `0o700`/`0o600` backup permissions in `_backup_file`, scrub all old/new Bearer tokens & `Authorization` headers in `TokenRotationError`/CLI stderr via `_redact_secrets` (no inline `cc_prev` JSON), run verified rollback when `claude mcp add-json` raises exceptions, report `failed_target_may_be_modified` & `codex_scope="managed_block_only"`, and fix `SKILL.md` `note=` -> `progress_note=`.
- [x] 2.7.2 Resolve R012 P1 #2, HTTP/1.1 framing/P3-2, zero-write `consilium_wait`, SSE ordering & P2 delivery replay/migration: pin `auth_token_ver` on `/api/ui-ticket` & `/api/stream` (pre-header + pre-snapshot auth re-checks, `try/finally unsubscribe`), strict `_parse_strict_content_length` (reject `Transfer-Encoding` 501, duplicate/invalid `Content-Length` 400, incomplete short reads 400, drain optional POST bodies via `_consume_optional_body`, `close_connection=True` on error/unconsumed body), SSE idle `_publish_lock` re-snapshot & ticking-field fingerprint strip, zero SQLite writes in `consilium_wait`, `on_escalate` hot-to-cold delivery persistence, `idempotency_keys.delivery_json` faithful replay, and `_init_schema` legacy `A-1-legacy-...` backfill + `@` scrubbing.
- [x] 2.7.3 Tighten Q4 multi-account rules & zero-write RO SQLite: use upstream `accounts[].id` as primary join key in `_load_cockpit_codex_registry` (keeping separate workspaces sharing an email distinct), hash ALL Cockpit & local-Claude IDs/emails (`_hash_account_ref(..., always_hash=True)`), `mode=ro&immutable=1` / `PRAGMA query_only=ON` Cockpit queries, `clear_peer_cockpit_current` when `codex_accounts.json` is absent/corrupt, keep unconfirmed single Cockpit candidates as `current_account_unknown=True`, apply 30-min staleness across all active Cockpit rows + exclude `>30m` backups, and expose `historical_accounts` labeled `"历史账号 N"`.
- [x] 2.7.4 Align minimalist chat `/ui` tabs (`模型与账号`, `子 Agent` showing `"该端未上报"`, `文件`), slash commands (`/mission`, `/files`, `/export`), relative `"最后上报于"` timing, `"账号未知"`/`"可能已切换"`/`"历史账号"` labels, and failure-aware `copyTextSafe`; expand `tests/test_r011_regressions.py` + `tests/test_r012_regressions.py`, verify dual-mode test suite, re-capture `2026-11-20` screenshots (screenshots only per 2026-11-20 user rule), sync OpenSpec/R013/READMEs, and stop at ★ Gates 1 & 2.

## Stage 2.8: Gate 1 Supplemental Fixes & R014 Deferred Items (R015)
- [x] 2.8.1 Fix HTTP/1.1 GET framing in `consilium/mcp_server.py`: `do_GET` invokes `_consume_optional_body()` instead of unconditionally setting `_body_consumed = True`, and `_send_json`/`_send_html` close keep-alive connections whenever unconsumed request bodies exist on any method (`tests/test_r015_regressions.py`).
- [x] 2.8.2 Distrust Cockpit `codex_accounts.json` `current_account_id` in `consilium/quota.py` when `codex_accounts.sqlite` exists with a newer `st_mtime` (comparing `os.stat` mtime ONLY, never opening `codex_accounts.sqlite`), clearing prior Cockpit current flags so the active account reports `未知`.
- [x] 2.8.3 Pulse `.seq` waiters in `rebuild_all_peer_seqs` on daemon startup when `unacked > 0` or `queued_tasks > 0` (`pulse_if_backlog=True` in `consilium/wakeup.py` & `consilium/store.py`), and surface redacted per-target status (`InstallApplyError` & `target_results`) on installer Phase 2 partial failures in `scripts/install.py`.
- [x] 2.8.4 Polish `WKWebView` theme switching in `consilium/dashboard.py` (suppress frozen CSS transitions across `data-theme` switches via `no-theme-transition` class and explicit `color-scheme`).

## Stage 2.9: Claude Takeover Polish (R016)

- [x] 2.9.1 Review R015 (79 tests, build, two-case app smoke, screenshots) and tighten `Consilium.app` deep links to the verified `claude://code/new` and `codex://new` only.
- [x] 2.9.2 Replace user-facing “Mission” with “会诊”, embed the brand mark (inline SVG) and an offline data-URI favicon in `/ui`.
- [x] 2.9.3 Narrow-screen (≤ 860px) layout: single-row top bar with a “⋯” menu for theme, drawer tabs and logout; single-row scrollable peer capsules; slimmer composer. Add `tests/test_r016_regressions.py` and capture 2026-11-20 “b” screenshots.
- [x] 2.9.4 Prepare Stage 3 in `13_Consilium/04_B级试跑/` (trial plan, `.mcp.json` generator/cleaner, stand-in worker), self-tested on an isolated daemon; nothing executed against live apps.

## Stage 3: Level-B Real App Trial (Blocked on ★ Gate 2)
- [ ] 3.1 Add project-scoped `.mcp.json` / `.gitignore` and B-level worker script in `13_Consilium/`.
- [x] 3.1a Trial plan and scripts ready (`13_Consilium/04_B级试跑/试跑方案.md`); Codex project-local MCP requires project trust in `~/.codex/config.toml`, deferred to a separate user consent (B2).
- [x] 3.2a 2026-11-20 Level-B1 trial passed: live Claude Code session in Claude.app coordinated mission `M-0736813C` with stand-in Codex/Antigravity workers (hot-wake claims 14–22 ms, 2/2 done, summary + close in 43 s); record in `13_Consilium/04_B级试跑/试跑记录_2026-11-20.md`. Consilium.app switched to a regular Dock app that opens its window on launch (menu-bar icons can hide behind the MacBook notch).
- [ ] 3.2 Run live Claude Code (+ Codex if trusted) coordinator walkthrough with dashboard open; enable Q2 (composer live dispatch write API) and structured `report_progress(subagents=[...])` reporting after Gate 2 approval.
- [x] ★ **Gate 3**: User confirms workflow smoothness. (Passed 2026-11-20: user said “通过” after the Level-B1 trial and seeing the Consilium.app window.)

## Stage 4: Level-C Live Installation (Blocked on ★ Gate 3 & User Consent)
- [ ] 4.1 Implement `consilium doctor`.
- [x] 4.2 2026-11-20: `--dry-run` plan presented (real home untouched); user chose the recommended scope; `--apply --targets claude_code codex` (MCP entry + `consilium-collab` skill each, backups under `~/Library/Application Support/consilium/backups/`); `claude mcp get consilium` → Connected, `codex mcp list` → consilium enabled (Bearer); `--uninstall` drill then reinstall with the token unchanged. Fixes found on the way: Claude.app detection via `ps`, subagent templates split into `claude_agents` / `codex_agents`, byte-exact Codex uninstall and empty skill-folder cleanup.
- [ ] 4.3 Not installed by user choice: `claude_desktop` (needs Claude.app quit), `antigravity` (no quota), `claude_agents` / `codex_agents`, `launchd` and login item (user: no auto-start for now).

## Stage 4.5: Menu Bar Native Wrapper (`Consilium.app` Built Ahead in R015; Live Deployment Gated on Stage 4)
- [x] 4.5.1 Build native macOS menu bar app `Consilium.app` (`app/macos/Sources/*.swift`, `app/macos/Info.plist`, `scripts/build_app.sh` using Xcode CLT `swiftc` + system `iconutil`/`codesign -s -`, `docs/brand/consilium-mark-template.svg`, committed `AppIcon.iconset` & `StatusIconTemplate*.png`, bundled stdlib `consilium` package, `NSAllowsLocalNetworking` + `WKWebsiteDataStore.nonPersistent()`, direct `POST /api/login` cookie injection, reuse-or-manage daemon lifecycle, and `UNUserNotificationCenter` R003 click deep links) and verify via `scripts/smoke_test_app.sh` + dated `2026-11-20` Light/Dark/menubar screenshots.
- [x] 4.5.2 `Consilium.app` copied to `/Applications` (2026-11-20). Login item and `launchd` intentionally not set up (user: no auto-start for now).

## Stage 5: Daily-Use Polish (Blocked on Stage 4)
- [ ] 5.1 Mission templates, Q5 user-selectable auto-dispatch hints, workspace file tree / read-only diff & source preview (respecting R012 §6 Git read-only isolation rules: `ls-tree -z`, `cat-file blob`, disallow `.gitattributes` filters/submodule traversal/untrusted `.git/config`), Markdown export enhancements, and refined per-app skills.

## Stage 6: Operations & Log Mirror (Blocked on Stage 5)
- [ ] 6.1 Log rotation, daily `bus.db` 7-day backup, daemon/client version mismatch indicator, and Q3 one-way read-only local session JSONL mirror.
