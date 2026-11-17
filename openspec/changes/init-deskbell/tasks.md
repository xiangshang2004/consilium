# Tasks: `deskbell` MVP

- [x] 1. Bootstrap `/Users/up/code/github/deskbell` with Git, OpenSpec, and OMX `AGENTS.md`.
- [x] 2. Implement `deskbell/` Python 3.9 stdlib package (`models.py`, `store.py`, `wakeup.py`, `mcp_server.py`, `cli.py`) with Seatbelt-compatible `kqueue` VNODE wakeup and `/tmp/deskbell-<uid>` ownership checks.
- [x] 3. Add desktop adapter templates (`adapters/skill`, `adapters/claude`, `adapters/codex`, `adapters/antigravity`, `adapters/launchd`) and `--dry-run` safe `scripts/install.py`.
- [x] 4. Write and pass `unittest` suite covering atomic claiming, loop guard, `kqueue` hot wakeup, lease pruning, dry-run cold fallback, stdio/socketpair HTTP MCP transports, HTTP security headers, and installer idempotency.
- [x] 5. Write documentation (`README.md`, `docs/`) and Claude audit handover (`reviews/R001-mvp-design-and-impl.md`).
- [x] 6. Register repo in `/Users/up/code/README.md` and commit cleanly on `main`.
- [ ] 7. Hand off `reviews/R001-mvp-design-and-impl.md` to `Claude.app` for review before applying live user config changes.
