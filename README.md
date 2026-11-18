# deskbell

Local MCP collaboration bus and zero-CLI doorbell wakeup bridge connecting **ChatGPT.app (Codex Desktop)**, **Claude.app (Claude Desktop)**, and **Antigravity.app** on macOS.

> Status: **R002 audit fixes implemented & verified in isolated `--target-home`; awaiting R004 verification and Codex R003 review before applying live user configurations.** See [reviews/R002-claude-audit.md](reviews/R002-claude-audit.md), [reviews/R004-antigravity-r002-fixes.md](reviews/R004-antigravity-r002-fixes.md), and [docs/README.md](docs/README.md).

## Why `deskbell`?

When running three separate AI desktop apps on one Mac, you shouldn't have to copy-paste between windows or pay extra API fees by wrapping CLI tools (`claude -p`, `codex exec`, `agy -p`).

`deskbell` gives all three desktop apps:
1. **A shared local collaboration bus** (`SQLite` WAL + MCP `stdio` / `127.0.0.1` Streamable HTTP) so you can issue one command in **any** desktop window, let that session act as coordinator, and get a single unified final report.
2. **A zero-CPU hot doorbell (`kqueue` `KQ_FILTER_VNODE`)** that works even inside macOS Seatbelt sandboxes—waking idle standby sessions in milliseconds without polling tokens or stealing keyboard focus.
3. **A safe cold fallback** (argv-safe `osascript` macOS notification banner + concise `claude://` / `codex://` / `antigravity://` deep link) when a target app has no active standby listener.
4. **Native in-app subagent fan-out** (`adapters/`), letting each desktop app dynamically pick models (`pro_reasoning` / `balanced` / `fast`) and spawn parallel subagents inside its own native harness and isolated git worktrees.
5. **Zero-token local quota & reset awareness** (`deskbell/quota.py`, `deskbell_quota`), harvesting cached remaining percentages and reset timestamps from local Cockpit Tools SQLite/JSON caches, Codex `rollout-*.jsonl` `rate_limits`, and Claude local usage caches (plus optional `manual_report`) so the coordinator avoids dispatching heavy work to exhausted accounts.

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
~/Library/Application Support/deskbell/bus.db    /tmp/deskbell-<uid>/signals/<peer>.{sig,seq}
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
cd /Users/up/code/github/deskbell

# 1. Run full unit & wire-level test suite (dry-run GUI notifications)
DESKBELL_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests -v

# 2. Preview config changes without touching live ~/.claude, ~/.codex, or ~/.gemini
DESKBELL_DRY_RUN=1 /usr/bin/python3 scripts/install.py --dry-run

# 3. Inspect bus status, quota windows, or run local HTTP + wake daemon manually
/usr/bin/python3 -m deskbell.cli status
/usr/bin/python3 -m deskbell.cli quota
/usr/bin/python3 -m deskbell.cli serve --host 127.0.0.1 --port 18920
```

## Repository Layout

- `deskbell/`: Pure Python 3.9 stdlib package (`models.py`, `store.py`, `quota.py`, `wakeup.py`, `mcp_server.py`, `cli.py`).
- `adapters/`: Shared skill ([SKILL.md](adapters/skill/deskbell-collab/SKILL.md)), Claude subagent templates, Codex agent TOML profiles, Antigravity MCP snippet, and `launchd` plist template.
- `scripts/install.py`: Idempotent installer defaulting to `--dry-run` with `--target-home`, timestamped backups, and `--uninstall`.
- `tests/`: `unittest` suite covering SQLite concurrency, loop guard, `kqueue` hot wakeup, lease pruning, cold fallback, stdio/HTTP MCP transports, security headers, and installer idempotency.
- `openspec/`: OpenSpec proposal, design, spec, and tasks under [openspec/changes/init-deskbell/](openspec/changes/init-deskbell/proposal.md).
- `docs/` & `reviews/`: Architecture notes and cross-agent audit packets.

## Security Model

- Persistent state (`~/Library/Application Support/deskbell/`) and ephemeral rendezvous (`/tmp/deskbell-<uid>/`) are strictly split so macOS 3-day `tmp_cleaner` never deletes `bus.db` or `auth.token`. Both directories are verified with `os.lstat()` to reject symlinks and foreign UIDs (`0o700`); DB, signal files, leases, and `auth.token` use `0o600`.
- HTTP transport binds strictly to `127.0.0.1`, enables Bearer/query token authentication by default (`--require-token`), validates `Host`, rejects cross-origin `Origin` headers with `403`, and returns `405 Allow: POST` on `GET /mcp`.
- Cold notifications pass untrusted text via `osascript` `argv` (`on run argv`), strip leading `/` from deep-link prompts, truncate safely before UTF-8 percent-encoding, and never simulate keystrokes.
