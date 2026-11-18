# R001: `deskbell` MVP Design & Implementation Review Packet (For Claude Audit)

- **Author**: Antigravity (`Gemini`)
- **Date**: 2026-11-18
- **Status**: Ready for `Claude.app` audit (`R002`) — **live user configs (`~/.claude`, `~/.codex`, `~/.gemini`, `~/Library/LaunchAgents`) have NOT been touched yet.**

---

## 1. What Was Built

`deskbell` is a pure Python 3.9 stdlib local MCP collaboration bus + zero-CLI wakeup doorbell connecting `ChatGPT.app` (`com.openai.codex`), `Claude.app` (`com.anthropic.claudefordesktop`), and `Antigravity.app` (`com.google.antigravity`).

Key components:
- `deskbell/models.py`: Constants, `lstat`-hardened `resolve_home_dir()` (`/tmp/deskbell-<uid>` default, `0o700`), and `ensure_auth_token()` (`0o600`).
- `deskbell/store.py`: `SQLite` WAL `BusStore` (`peers`, `missions`, `tasks`, `events`, `quota_windows`) with `BEGIN IMMEDIATE` atomic task claiming, `max_rounds` loop guard, and quota countdown decoding.
- `deskbell/quota.py`: Zero-token multi-source quota & reset-time extractor (`Cockpit Tools` `platform-quota-history-v1.sqlite` / `codex-quota-history-v1.sqlite` / `*_accounts.json`, Codex `rollout-*.jsonl` `rate_limits`, and Claude `~/.claude/usage-latest.json` / `quota.json`).
- `deskbell/wakeup.py`: Seatbelt-compatible `kqueue` `KQ_FILTER_VNODE` hot wakeup + PID/expiry lease tracking (`_count_live_leases`), `AF_UNIX` `WakeBrokerServer`, and `argv`-safe `ColdDoorbell` fallback (`osascript` + URL-encoded deep links).
- `deskbell/mcp_server.py`: Dual-transport MCP JSON-RPC 2.0 (`stdio` + `127.0.0.1` Streamable HTTP with `Host`/`Origin` CSRF checks and optional Bearer token, plus `handle_socket_once` for wire-level testing; 9 tools including `deskbell_quota`).
- `deskbell/cli.py`: `serve`, `mcp-stdio`, `wait`, `status`, `quota`.
- `adapters/`: Shared `SKILL.md`, Claude subagent templates (`opus-architect.md`, `sonnet-builder.md`, `haiku-scout.md`), Codex agent profiles (`architect-high.toml`, `builder-balanced.toml`, `scout-fast.toml`), Antigravity snippet, and `launchd` plist template.
- `scripts/install.py`: Idempotent installer defaulting to `--dry-run`, supporting `--check`, `--apply`, `--uninstall`, `--target-home`, and timestamped backups.

---

## 2. Key Empirical Discovery During Implementation

Testing inside Antigravity's terminal sandbox revealed that macOS Seatbelt sandboxes block `socket.connect()` (`PermissionError` / `EPERM` on both `AF_UNIX` sockets in `/tmp` and `127.0.0.1` loopback TCP) and block `~/.deskbell` writes.

To guarantee standby wakeup works reliably across sandboxed desktop terminals **without requiring `BypassSandbox` or constant polling**:
1. Default runtime directory was moved from `~/.deskbell` to `/tmp/deskbell-<uid>` with `os.lstat()` symlink rejection and UID ownership verification.
2. Primary hot wakeup uses macOS kernel `select.kqueue()` (`KQ_FILTER_VNODE` watching `<home>/signals/<peer>.sig`) paired with lease files under `<home>/waiters/<peer>/`, which was verified inside Seatbelt (`0` CPU, instant wakeup on pulse write, and `0.183s` total test suite runtime).

---

## 3. Verification Results (Pre-Audit)

Ran in `/Users/up/code/github/deskbell`:
```bash
DESKBELL_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests -v
```
- **Result**: `Ran 14 tests — OK` (re-verified after adding `deskbell/quota.py`)
- Covered: concurrent atomic `pickup_inbox`, `mission` completion (`all_tasks_finished`), `max_rounds` loop guard, `kqueue` hot wakeup & lease cleanup, dead/expired lease pruning, immediate pre-check return, cold fallback deep-link encoding & `dry_run`, stdio MCP lifecycle, wire-level `socketpair()` HTTP security checks (`401` missing token, `403` evil `Origin`, `403` spoofed `Host`, `200` authorized `/mcp` & `/health`), installer dry-run/apply/idempotency/uninstall, and multi-source quota/reset extraction (`Cockpit Tools` SQLite + Codex `rollout-*.jsonl` + Claude `usage-latest.json` + `deskbell_quota`).

---

## 4. Specific Audit Questions for Claude (`R002`)

Please review the repository critically and address these 7 points in `reviews/R002-claude-audit.md`:

1. **Codex `config.toml` MCP Table Key (`scripts/install.py`)**:
   - Currently `scripts/install.py` writes `[mcp_servers.deskbell]` into `~/.codex/config.toml`. On this machine (`codex-cli 0.169.0-alpha.11.1` / `ChatGPT.app` `26.1111`), please check the user's real `~/.codex/config.toml` or `codex mcp add` behavior to confirm whether Codex Desktop expects `[mcp_servers.deskbell]` or `[mcpServers.deskbell]`, and whether `url = "http://127.0.0.1:18920/mcp"` works natively without `bearer_token_env_var`.
2. **`Claude.app` (`v5.11492.5`) MCP & Background Waiter Compatibility**:
   - `scripts/install.py` configures `mcp-stdio` in `~/Library/Application Support/Claude/claude_desktop_config.json` AND HTTP in `~/.claude.json` (both pointing to `--data-dir /tmp/deskbell-<uid>`). Please verify how Claude Desktop's Chat / Cowork / Code tabs behave with both entries, and confirm whether a background `python3 -m deskbell.cli wait --peer claude --pickup` task stays alive in Claude Desktop's Code tab or hits a command timeout (if so, recommend the cleanest `Stop` hook or `deskbell_wait` loop).
3. **`/tmp/deskbell-<uid>` Security & Lease Edge Cases**:
   - Audit `resolve_home_dir()` in `deskbell/models.py` and `_count_live_leases()` in `deskbell/wakeup.py` for `/tmp` pre-creation squatting, PID reuse races, or `/tmp` cleanup across macOS reboots (note: `bus.db` in `/tmp` is ephemeral across reboots by design—should long-lived `bus.db` stay in `~/.deskbell` while only `signals/` & `waiters/` live in `/tmp/deskbell-<uid>`, or is keeping everything in `/tmp` preferable for sandbox parity?).
4. **Missing `deskbell_close_mission` MCP Tool**:
   - Currently `BusStore.close_mission()` exists in Python, while MCP exposes 9 tools (`deskbell_complete_task` sets mission status to `ready_to_report` when `all_tasks_finished=True`, and `deskbell_quota` handles quota queries/reports). Should we add an explicit `deskbell_close_mission` MCP tool (10th tool) in MVP+1 so the coordinator records `final_summary` and transitions the mission to `completed` via MCP?
5. **Cold Doorbell Deep-Link Limits & Special Characters**:
   - Verify `claude://code/new?q=...&folder=...` and `codex://new?q=...` lengths/escaping on the installed versions of `Claude.app` and `ChatGPT.app`.
6. **Git Worktree Handover Convention**:
   - Check `adapters/skill/deskbell-collab/SKILL.md` against the user's existing repo workflows (e.g. `captalent-collab` skill and `/Users/up/code/AGENTS.md`) to ensure branch naming and worktree cleanup conventions won't clash.
7. **Real Local Quota Cache Parity (`deskbell/quota.py`)**:
   - Inspect (read-only) `~/.antigravity_cockpit/` (`platform-quota-history-v1.sqlite`, `codex-quota-history-v1.sqlite`, `*_accounts.json`), `~/.codex/sessions/**/rollout-*.jsonl`, and `~/.claude/` on this Mac to verify exact schema/field alignment and confirm whether Claude needs a lightweight `PostInvocation`/`Stop` hook to export `anthropic-ratelimit-unified-*` or Cockpit's `claude_accounts.json` already covers it.

---

## 5. Instructions for Claude (How to Reply)

1. Read this file plus `deskbell/*.py`, `scripts/install.py`, `adapters/skill/deskbell-collab/SKILL.md`, and run `DESKBELL_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests -v`.
2. Also inspect (read-only) the user's live `~/.codex/config.toml` and `~/Library/Application Support/Claude/claude_desktop_config.json` / `~/.claude.json` if accessible from Claude Desktop to verify exact table keys and existing servers.
3. **Do NOT apply changes to live home configs yet.** Write your review findings and concrete patch/spec recommendations to `reviews/R002-claude-audit.md` (and update `reviews/README.md` if appropriate), or output a concise verdict the user can pass back to Antigravity.
