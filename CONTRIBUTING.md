# Contributing to Consilium

Thank you for contributing to **Consilium**! Before opening a pull request, please keep these project invariants in mind:

## Core Engineering Constraints

1. **Pure Python 3.9+ Standard Library Only** (`consilium/`, `scripts/`):
   - Every Python module starts with `from __future__ import annotations`.
   - Keep `dependencies = []` in [pyproject.toml](pyproject.toml). Do not add third-party PyPI packages.
2. **Zero-Xcode macOS App Build** (`app/macos/`):
   - `Consilium.app` must compile cleanly with Apple Command Line Tools (`swiftc`, `iconutil`, `codesign`) alone via `./scripts/build_app.sh`.
3. **Preserve Privacy & Security Guarantees**:
   - Never bind to `0.0.0.0` or add external CDN/telemetry requests (update [PRIVACY.md](PRIVACY.md) if any local data path changes).
   - Never open `~/.antigravity_cockpit/codex_accounts.sqlite` or encrypted Cockpit credential files (`os.stat` mtime comparison only).
   - Never leak `quota_account_names` into MCP tool outputs or model prompts.

## Local Development & Verification

Always strip `CLAUDE_CONFIG_DIR` when running tests so host Claude settings cannot influence isolated test runs:

```bash
# 1. Full unit test suite (isolated quota sync + dry-run notifications)
env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 \
  /usr/bin/python3 -B -m unittest discover -s tests -p 'test_*.py'

# 2. Default local quota path tests
env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 \
  /usr/bin/python3 -B -m unittest discover -s tests -p 'test_*.py'

# 3. Build and smoke-test Consilium.app (macOS 13+)
./scripts/build_app.sh && ./scripts/smoke_test_app.sh
```

## Licensing Note

Original Consilium source code and documentation are licensed under **CC BY-NC-SA 4.0** ([LICENSE](LICENSE)), with third-party notices in [NOTICE](NOTICE). By submitting a contribution, you agree that your contribution is licensed under the same terms.
