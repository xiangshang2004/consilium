## Summary

<!-- Describe what this PR changes and why. -->

## Related Specs / Issues

- Fixes #

## Checklist

- [ ] Pure Python 3.9+ standard library only (`consilium/`, `scripts/`, no external pip dependencies).
- [ ] Preserves `127.0.0.1`-only binding, offline CSP, and `quota_account_names` local isolation (updated `PRIVACY.md` if local data flows changed).
- [ ] Passed `env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 /usr/bin/python3 -B -m unittest discover -s tests -p 'test_*.py'`.
- [ ] Passed `./scripts/build_app.sh && ./scripts/smoke_test_app.sh` (if macOS app or UI changed).
