# consilium

Local MCP collaboration bus and zero-CLI doorbell wakeup bridge connecting **ChatGPT.app (Codex Desktop)**, **Claude.app (Claude Desktop)**, and **Antigravity.app** on macOS.

> Status: **Stage 0 (`consilium-v1` OpenSpec), Stage 1 (R006 fixes for all 7 R005 P1s), and Stage 2 (R007 offline `/ui` live dashboard + SSE + one-command `consilium ui` auto-login) implemented and verified (40 unit tests OK); paused at ★ Gates 1 & 2 awaiting Claude/Codex review of R006 and user approval of dashboard visuals before touching live configs.** See [reviews/R006-antigravity-r005-fixes.md](reviews/R006-antigravity-r005-fixes.md), [reviews/R007-antigravity-dashboard.md](reviews/R007-antigravity-dashboard.md), and [docs/README.md](docs/README.md).

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

# 1. Run full unit & wire-level test suite (dry-run GUI notifications)
CONSILIUM_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests -v

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
- `scripts/install.py`: Two-phase idempotent installer defaulting to `--dry-run` with `--target-home`, `--rendezvous-dir`, timestamped backups, `rotate-token`, and `--uninstall`.
- `tests/`: `unittest` suite (40 tests) covering SQLite concurrency, loop guard, `kqueue` hot wakeup, lease pruning, cold fallback, stdio/HTTP MCP transports, R005 P1/P2 regressions, dashboard `/ui` & cookie/one-time-ticket auth, ordered SSE `/api/stream`, and installer atomicity.
- `openspec/`: OpenSpec baseline and roadmap under [openspec/changes/consilium-v1/](openspec/changes/consilium-v1/proposal.md) (legacy `init-deskbell` archived).
- `docs/` & `reviews/`: Architecture notes and cross-agent audit packets (`R001`–`R007`).

## Security Model

- Persistent state (`~/Library/Application Support/consilium/`) and ephemeral rendezvous (`/tmp/consilium-<uid>/`) are strictly split so macOS 3-day `tmp_cleaner` never deletes `bus.db` or `auth.token`. Both directories are verified with `os.lstat()` to reject symlinks and foreign UIDs (`0o700`); DB, signal files, leases, and `auth.token` use `0o600`.
- HTTP transport binds strictly to `127.0.0.1`, enables Bearer token authentication by default (`--require-token`) plus `HttpOnly; SameSite=Strict` session cookies for `/ui` via `POST /api/login` (never putting tokens in URLs), validates `Host`, rejects cross-origin `Origin` headers with `403`, serves `/ui` with a strict offline CSP, and returns `405 Allow: POST` on `GET /mcp`.
- Cold notifications pass untrusted text via `osascript` `argv` (`on run argv`), strip leading `/` from deep-link prompts, truncate safely before UTF-8 percent-encoding, and never simulate keystrokes.
