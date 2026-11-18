# Proposal: `deskbell` Tri-Desktop AI Collaboration Bus & Doorbell

## Motivation
The local workstation runs three distinct AI desktop applications (`ChatGPT.app` / Codex Desktop, `Claude.app` / Claude Desktop, and `Antigravity.app`). Today, cross-app coordination relies on the user manually copying messages between desktop windows ("human router"), while CLI-wrapper orchestration misses native desktop UI workflows, subscriptions, and per-app subagent harnesses.

`deskbell` lets the user issue a single command in **any** of the three desktop apps. The receiving session becomes the round coordinator, shares task context via a unified local MCP bus, rings the other desktop apps via a zero-CPU wakeup doorbell (with non-intrusive cold notification + deep-link fallback), lets each app invoke its own native subagent harness with dynamic model selection and parallel fan-out, and collects a unified final report.

## Scope (MVP)
- Local `SQLite` (WAL) + `AF_UNIX` wake broker (`deskbell` Python 3.9 stdlib package, zero third-party deps).
- Dual-transport MCP server (`stdio` and `127.0.0.1` Streamable HTTP) exposing 9 coordination & quota tools.
- Zero-token local quota & reset-time harvesting (`Cockpit Tools` SQLite/JSON caches, Codex `rollout-*.jsonl` `rate_limits`, Claude local usage caches, plus `deskbell_quota` reporting).
- Hot standby wakeup (`deskbell wait` / `deskbell_wait`) plus safe cold-start macOS notification & deep-link fallback (`claude://`, `codex://`, `antigravity://`).
- Loop guard (`max_rounds`) preventing infinite cross-app ping-pong.
- Native subagent/profile templates and shared skill under `adapters/`.
- Safe `--dry-run`-by-default installer (`scripts/install.py`) supporting `--target-home` for isolated testing.

## Non-Goals
- Wrapping `claude -p`, `codex exec`, or `agy -p` as primary workers.
- Injecting AppleScript keystrokes (`System Events`) that steal user keyboard focus.
- Mutating live `~/.claude`, `~/.codex`, `~/.gemini`, or `~/Library/LaunchAgents` before cross-agent audit.
