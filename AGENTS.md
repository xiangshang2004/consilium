# Contributor & Agent Guidelines (`consilium`)

## Architecture & Runtime Constraints

- **Purpose**: Local-first macOS menu bar app (`Consilium.app`) and MCP collaboration bus (`127.0.0.1` HTTP + `stdio`) connecting native `Claude.app`, `ChatGPT.app` / Codex Desktop, and `Antigravity.app` sessions.
- **Pure Python 3.9+ Standard Library**: All backend modules in `consilium/` and scripts in `scripts/` must use **Python 3.9+ standard library only** (`from __future__ import annotations`, zero third-party `pip` dependencies).
- **Zero-Xcode Swift Packaging**: `app/macos/` compiles directly via Apple Command Line Tools (`swiftc`) and signs ad-hoc via `codesign`. Do not introduce Xcode project dependencies.
- **Safety & Privacy Invariants**:
  - Daemon binds strictly to `127.0.0.1`; dashboard HTML stays offline with `default-src 'none'` CSP.
  - Persistent state lives under `~/Library/Application Support/consilium/` (`0700`/`0600`) and ephemeral wakeup sockets/leases under `/tmp/consilium-<uid>/` (`0700`).
  - Cockpit Tools integration (`~/.antigravity_cockpit/`) is strictly **read-only** against whitelisted metadata/history files only; never open encrypted credential vaults or `codex_accounts.sqlite`.
  - Keep `quota_account_names` strictly local to the UI; never expose raw emails/account aliases to MCP tools or peer prompts.

## Verification Commands

Always strip `CLAUDE_CONFIG_DIR` before running tests or builds so local Claude configs cannot leak into isolated test runs:

```bash
# Full Python test suite (isolated mode)
env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 \
  /usr/bin/python3 -B -m unittest discover -s tests -p 'test_*.py'

# Build and smoke-test Consilium.app
./scripts/build_app.sh && ./scripts/smoke_test_app.sh

# Validate OpenSpec specs (if openspec CLI is installed)
openspec validate --all --strict
```

## Change Discipline

- Proposals, specs, and task lists live under `openspec/`.
- Historical multi-agent review logs live under `reviews/`.
- Always run installer scripts in `--dry-run` before `--apply`, and preserve timestamped backups when modifying user configs.
