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
- [ ] ★ **Gate 1**: Submit R006 for Claude and Codex review approval.

## Stage 2: Consilium Live Dashboard (Separate Commit)
- [x] 2.1 Implement `consilium/dashboard.py` and mount `/ui`, `/api/login`, `/api/logout`, `/api/state`, `/api/stream` (SSE), `/api/missions/<id>/close`, `/api/tasks/<id>/requeue` in `ConsiliumDaemon`.
- [x] 2.2 Record delivery/wakeup metadata on events so dashboard displays hot/cold badges and ms wakeup latency.
- [x] 2.3 Write unit/wire tests for dashboard API shape, cookie auth, Host/Origin checks, write actions, and ordered SSE streaming.
- [x] 2.4 Drive dashboard live with Level-A demo script, capture Light & Dark mode screenshots (and live progression evidence) into `13_Consilium/03_演示记录/`, commit cleanly (author `2084413277`), and write `reviews/R007-antigravity-dashboard.md`.
- [ ] ★ **Gate 2**: User reviews Light & Dark screenshots and confirms dashboard appearance.

## Stage 2.5: R008/R009 Remediation & Closed-Loop Sync (R010, Before Crossing Gates 1 & 2)
- [x] 2.5.1 Harden truth & wakeup layers: move `.seq` lock to `<rendezvous_dir>/.<peer>.seq.lock`, single-snapshot `query_peer_seq_counts`, `OSError(ENOENT|EINVAL)` retries, atomic `.lease`, strictly read-only `wait`, `attempt_id` fencing + `attempt_history`, `task_stale` for expired `running` tasks on background `_reaper_thread`, scoped `idempotency_keys` (`ConflictError`/409), `consilium_requeue_task`, and `delivery.warnings` post-commit fault isolation.
- [x] 2.5.2 Harden server, dashboard, and installer: Bearer-only HTTP auth (`?token=` 401), random `SessionStore` + `token_version`-bound `/api/ui-ticket` + `/ui/auth` + `consilium ui` failure handling, exact `Origin` + `application/json` + `X-Consilium-CSRF` (403/415), `nosniff`/`Referrer-Policy`/`Cache-Control`/`frame-ancestors 'none'`, SSE 16-cap + monotonic `state_version` + `external_change` fingerprint + rotation stream termination, 5-state peer indicators (`offline` vs `stale`) + R008 A/B UI rules, `--targets`/`--skip-claude-desktop`, verified Claude rollback, and `TokenRotationError`.
- [x] 2.5.3 Add `tests/test_r010_regressions.py` covering all R008/R009 items (1–16) and pass full 59-test `unittest` suite under `CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1`.
- [x] 2.5.4 Update `run_demo_a.py` & `capture_dashboard.mjs`, generate & `ffprobe`/visually verify `2026-11-19` Light/Dark Retina PNGs + live MP4 + `demo_a_timeline.md`, sync `openspec/changes/consilium-v1/` & docs, write `reviews/R010-antigravity-r009-fixes.md`, and commit topically (preserving `★ Gate 1` and `★ Gate 2` unchecked).

## Stage 3: Level-B Real App Trial (Blocked on ★ Gates 1 & 2)
- [ ] 3.1 Add project-scoped `.mcp.json` / `.gitignore` and B-level worker script in `13_Consilium/`.
- [ ] 3.2 Run live Claude Code (+ Codex if trusted) coordinator walkthrough with dashboard open.
- [ ] ★ **Gate 3**: User confirms workflow smoothness.

## Stage 4: Level-C Live Installation (Blocked on ★ Gate 3 & User Consent)
- [ ] 4.1 Implement `consilium doctor`.
- [ ] 4.2 Present `--dry-run` plan → ★ User consent → `--apply` → `consilium doctor` → per-app smoke tests → `--uninstall` drill.

## Stage 5: Daily-Use Polish (Blocked on Stage 4)
- [ ] 5.1 Mission templates, quota-aware dispatch hints, Markdown export, refined per-app skills.

## Stage 6: Operations (Blocked on Stage 5)
- [ ] 6.1 Log rotation, daily `bus.db` 7-day backup, daemon/client version mismatch indicator on dashboard.
