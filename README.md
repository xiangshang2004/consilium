# consilium

Local MCP collaboration bus and zero-CLI doorbell wakeup bridge connecting **ChatGPT.app (Codex Desktop)**, **Claude.app (Claude Desktop)**, and **Antigravity.app** on macOS.

> Status: **Stage 0 (`consilium-v1` OpenSpec), Stage 1 (R006), Stage 2/2.5 (R007/R010), and Stage 2.6 ([R013](reviews/R013-antigravity-chat-ui-and-q4.md): minimalist conversation `/ui` redesign, Q4 multi-account active-account quota & Cockpit read-only whitelist, and P3-1..4 hardening) verified (`65` unit tests OK with and without isolation env vars, `openspec validate consilium-v1 --strict` valid); still paused at ★ Gates 1 (awaiting Codex R012) & 2 (awaiting user sign-off on `2026-11-20` screenshots & video) before touching live configs.** See [reviews/R013-antigravity-chat-ui-and-q4.md](reviews/R013-antigravity-chat-ui-and-q4.md), [reviews/README.md](reviews/README.md), and [docs/README.md](docs/README.md).

## Why `consilium`?

When running three separate AI desktop apps on one Mac, you shouldn't have to copy-paste between windows or pay extra API fees by wrapping CLI tools (`claude -p`, `codex exec`, `agy -p`).

`consilium` gives all three desktop apps:
1. **A shared local collaboration bus** (`SQLite` WAL + MCP `stdio` / `127.0.0.1` Streamable HTTP) so you can issue one command in **any** desktop window, let that session act as coordinator, and get a single unified final report.
2. **A zero-CPU hot doorbell (`kqueue` `KQ_FILTER_VNODE`)** that works even inside macOS Seatbelt sandboxes—waking idle standby sessions in milliseconds without polling tokens or stealing keyboard focus.
3. **A safe cold fallback** (argv-safe `osascript` macOS notification banner + concise `claude://` / `codex://` / `antigravity://` deep link) when a target app has no active standby listener.
4. **Native in-app subagent fan-out** (`adapters/`), letting each desktop app dynamically pick models (`pro_reasoning` / `balanced` / `fast`) and spawn parallel subagents inside its own native harness and isolated git worktrees.
5. **Zero-token local quota & reset awareness** (`consilium/quota.py`, `consilium_quota`), harvesting cached remaining percentages and reset timestamps from local Cockpit Tools SQLite/JSON caches, Codex `rollout-*.jsonl` `rate_limits`, and Claude local usage caches (plus optional `manual_report`) so the coordinator avoids dispatching heavy work to exhausted accounts.

## Architecture Overview

```text
  [User orders in ANY Desktop App]
                 │
                 ▼
       Coordinator Desktop App
        (create_mission / dispatch_task)
                 │
   ┌─────────────┴──────────────┐
   ▼                            ▼
Truth Layer                                      Doorbell Layer
~/Library/Application Support/consilium/bus.db    /tmp/consilium-<uid>/signals/<peer>.{sig,seq}
(SQLite WAL + Loop Guard + auth.token)           + waiters/<peer>/*.lease (flock kernel leases)
                                │
          ┌─────────────────────┴─────────────────────┐
          ▼ (live lease > 0)                          ▼ (no active waiter)
    Hot Wakeup (kqueue)                        Cold Doorbell Fallback
  Zero CPU, wakes standby                    osascript banner + deep link
  session in ~5ms (no UI touch)              (no focus-stealing keystrokes)
          │
          ▼
  Awakened Peer fans out via ITS OWN Native Harness
  (Antigravity invoke_subagent / Claude Agents / Codex Profiles)
```

## Quickstart (Safe Verification Only)

Requirements: **macOS `/usr/bin/python3` (`Python 3.9+`)**, zero external `pip` dependencies.

```bash
cd /Users/up/code/github/consilium

# 1. Run full unit & wire-level test suite (dry-run GUI notifications, isolated home/quota)
env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 /usr/bin/python3 -B -W default -m unittest discover -s tests -v

# 2. Preview config changes without touching live ~/.claude, ~/.codex, or ~/.gemini
CONSILIUM_DRY_RUN=1 /usr/bin/python3 scripts/install.py --dry-run

# 3. Inspect bus status, quota windows, or run local HTTP + wake daemon manually
/usr/bin/python3 -m consilium.cli status
/usr/bin/python3 -m consilium.cli quota
/usr/bin/python3 -m consilium.cli serve --host 127.0.0.1 --port 18920
```

## Repository Layout

- `consilium/`: Pure Python 3.9 stdlib package (`models.py`, `store.py`, `quota.py`, `wakeup.py`, `dashboard.py`, `mcp_server.py`, `cli.py`).
- `adapters/`: Shared skill ([SKILL.md](adapters/skill/consilium-collab/SKILL.md)), Claude subagent templates, Codex agent TOML profiles, Antigravity MCP snippet, and `launchd` plist template.
- `scripts/install.py`: Two-phase idempotent installer defaulting to `--dry-run` with `--target-home`, `--rendezvous-dir`, `--targets`, `--skip-claude-desktop` (allowing non-desktop targets while `Claude.app` runs), timestamped backups, verified Claude MCP rollback, `rotate-token` (raising structured `TokenRotationError` on mid-rotation failure), and `--uninstall`.
- `tests/`: `unittest` suite (`65` tests across 9 modules including `test_r010_regressions.py` and `test_r011_regressions.py`) covering SQLite concurrency, loop guard, `attempt_id` fencing, `task_stale` / `consilium_requeue_task`, strictly read-only `wait`, `kqueue` hot wakeup & `signals/` deletion recovery, atomic `.lease`, 3-fact cold fallback, stdio/HTTP MCP transports, `delivery.warnings` fault isolation, scoped idempotency (`409`), active-account multi-account quota pool & Cockpit read-only whitelist, R006/R010/R011 regressions, minimalist conversation `/ui` (5-state indicators, 5-tab right drawer, `/` palette, `1440px`/`390px`), `SessionStore` / `LoginTicketStore` / CSRF / `Origin` (including malformed port `403` & error `close_connection`) / SSE 16-cap & token-rotation revocation, and installer rollback/`TokenRotationError`.
- `openspec/`: OpenSpec baseline and roadmap under [openspec/changes/consilium-v1/](openspec/changes/consilium-v1/proposal.md) (legacy `init-deskbell` archived).
- `docs/` & `reviews/`: Architecture notes and cross-agent audit packets (`R001`–`R013`, with `R012` reserved for Codex).

## Security Model

- Persistent state (`~/Library/Application Support/consilium/`) and ephemeral rendezvous (`/tmp/consilium-<uid>/`) are strictly split so macOS 3-day `tmp_cleaner` never deletes `bus.db` or `auth.token`. Both directories are verified with `os.lstat()` to reject symlinks and foreign UIDs (`0o700`); DB, signal files, leases, and `auth.token` use `0o600`.
- HTTP transport binds strictly to `127.0.0.1`, enables Bearer token authentication by default (`--require-token`, **`?token=` query-string auth rejected with `401`**), serves `/mcp` with Bearer-only auth (`405 Allow: POST` on `GET /mcp`), validates `Host` and `Origin` across endpoints (catching malformed non-numeric ports with `403`), and closes keep-alive sockets on error responses (`status >= 400`).
- **Always open `/ui` via `127.0.0.1:<port>/ui` (not `localhost:<port>/ui`)**: `localhost` and `127.0.0.1` are distinct HTTP origins; even if a session cookie is present, browsers send a `localhost` `Origin` header that exact-origin CSRF guards (`_is_exact_daemon_origin`) intentionally reject with `403`.
- Local Cockpit quota reads use a strict read-only whitelist (`codex_accounts.json` metadata + `codex-quota-history-v1.sqlite` + `platform-quota-history-v1.sqlite`), SHA-256 hash account identifiers (`_hash_account_ref`), never inspect encrypted account blobs, and compute active-account posture solely from the currently selected account while surfacing backup-account switch hints in read-only UI metadata.
- Dashboard `/ui` uses server-side random `SessionStore` cookies (`consilium_session`, 12h TTL, `HttpOnly; SameSite=Strict`) issued via `POST /api/login` or `token_version`-bound 30s single-use tickets (`POST /api/ui-ticket` -> `GET /ui/auth?ticket=...` via `consilium ui`). Token rotation or `/api/logout` immediately revokes unredeemed tickets, existing sessions (`401`), and active SSE streams.
- Cookie-authenticated dashboard writes (`/api/missions/<id>/close`, `/api/tasks/<id>/requeue`, `/api/logout`) enforce exact daemon `Origin` (`scheme://host:port`, `403`), `Content-Type: application/json` (`415`), and `X-Consilium-CSRF` (`403`).
- `/ui` serves a strict offline CSP (`default-src 'none'; ...; frame-ancestors 'none';`) and all HTTP/JSON/SSE/`202` responses set `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` (`no-cache, no-store` on SSE). `EventHub` caps SSE at `16` concurrent clients (`503` beyond cap) with `10s` write timeouts and monotonic `state_version`.
- Cold notifications pass untrusted text via `osascript` `argv` (`on run argv`), strip leading `/` from deep-link prompts, truncate safely before UTF-8 percent-encoding, and never simulate keystrokes.
