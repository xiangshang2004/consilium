# Tasks: `deskbell` MVP

- [x] 1. Bootstrap `<workspace>/github/deskbell` with Git, OpenSpec, and contributor `AGENTS.md`.
- [x] 2. Implement `deskbell/` Python 3.9 stdlib package (`models.py`, `store.py`, `quota.py`, `wakeup.py`, `mcp_server.py`, `cli.py`) with Seatbelt-compatible `kqueue` VNODE wakeup, `/tmp/deskbell-<uid>` ownership checks, and multi-source local quota/reset extraction.
- [x] 3. Add desktop adapter templates (`adapters/skill`, `adapters/claude`, `adapters/codex`, `adapters/antigravity`, `adapters/launchd`) and `--dry-run` safe `scripts/install.py`.
- [x] 4. Write and pass `unittest` suite covering atomic claiming, loop guard, `kqueue` hot wakeup, lease pruning, dry-run cold fallback, stdio/socketpair HTTP MCP transports, HTTP security headers, and installer idempotency.
- [x] 5. Write documentation (`README.md`, `docs/`) and Claude audit handover (`reviews/R001-mvp-design-and-impl.md`).
- [x] 6. Register repo in `<workspace>/README.md` and commit cleanly on `main`.
- [x] 7. Hand off `reviews/R001-mvp-design-and-impl.md` to `Claude.app` for review before applying live user config changes (completed via R002–R005; superseded by `consilium-v1`).
