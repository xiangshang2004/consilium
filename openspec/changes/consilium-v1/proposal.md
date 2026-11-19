# Proposal: `consilium-v1` — Production-Ready Tri-Desktop Bus & Live Dashboard

## Why

The `init-deskbell` MVP proved the core bus and `kqueue` doorbell across 18 unit tests and the isolated Level-A terminal demonstration (`13_Consilium/03_演示记录/demo_a_timeline.md`). However, R005 identified 7 P1 reliability/security gaps (default auto-pickup on wait, `.seq` locking/rebuild/delete recovery, token placement in URLs/argv, unhooked stale-claim/running reaper, write/delivery error coupling without idempotency keys, cold notification deep links and `SKILL.md` wording, and non-atomic installer writes), and daily usability requires a clear, offline-capable live dashboard (`/ui`) before onboarding real desktop apps.

## What Changes

1. **Freeze v1 Bus Contract (`specs/bus/spec.md`)**:
   - Canonical peers (`claude`, `chatgpt`, `antigravity`), branch prefixes (`claude/`, `codex/`, `gemini/`), task/mission state machines, and event kinds (`task_assigned`, `progress`, `task_done`, `review_request`, `loop_guard_reached`, `task_requeued`, `mission_closed`).
   - Uniform write response envelope `{"committed": bool, "result": {...}, "delivery": {...}}` with delivery fault isolation (`isError: false` when DB commit succeeded) and optional `client_request_id` idempotency.
2. **Stage 1 (R006 Reliability & Security Fixes)**:
   - Default `deskbell_wait` `auto_pickup` to `false`; protect `signals/<peer>.seq` with `fcntl.flock` + DB re-query + startup rebuild + kqueue `NOTE_DELETE|NOTE_RENAME` recovery; track `queued_tasks` in `.seq` pre-check.
   - Move Antigravity MCP config from `?token=` to `Authorization: Bearer` headers; add `deskbell rotate-token` with live token reload; enforce two-phase atomic `scripts/install.py` validation, Claude.app running refusal, Claude CLI rollback, `--rendezvous-dir`, and `CLAUDE_CONFIG_DIR` isolation.
   - Hook up background stale `claimed`/`running` task reaper (emitting `task_requeued`); fix `ColdDoorbell.build_deeplink` (`claude://code/new`, `codex://new?prompt=...&path=...`) and `SKILL.md`; address R005 P2 items (strict RPC validation, closed-mission `request_review` guard, isolated quota sync & `null` `dispatch_recommended`, `peers` activity tracking, `DESKBELL_RENDEZVOUS_DIR` precedence).
3. **Stage 2 (Consilium Live Dashboard — `specs/dashboard/spec.md`)**:
   - Serve an inline, zero-CDN dashboard at `http://127.0.0.1:<port>/ui` backed by `/api/state`, `/api/stream` (SSE with auto-reconnect and 3s polling fallback), and cookie-based token login (`HttpOnly; SameSite=Strict`).
   - Provide light/dark/system themes, three peer cards (status lights, session/activity, quota bar with `"未知"` for missing data, task counts), mission list with round indicator dots, task board, reverse-chronological event timeline (with hot/cold badges and wakeup latency in ms), Markdown `final_summary` rendering, and two double-confirmed actions (`close mission`, `requeue task`).
4. **Stages 3–6 (Gated Rollout)**:
   - Level-B real Claude/Codex project-scoped trials, Level-C installation with `deskbell doctor` and per-app smoke checklist, daily-use templates/exports, and operational log/DB backup hygiene—each gated behind `★` review/user approval.

## Non-Goals

- Modifying live `~/.claude`, `~/.codex`, `~/.gemini`, or `~/Library/LaunchAgents` before `★` gates and explicit user approval.
- Spawning `codex exec` or external CLI wrapper windows in the background.
- Adding third-party Python pip packages or external frontend CDN dependencies.
- Implementing a macOS menu-bar icon app (deferred to v1.1 pending user approval of Swift/extra dependency).
