# Consilium Troubleshooting Reference

Use this guide to diagnose and fix common setup, wakeup, port, quota, attachment, auth, and build issues on macOS.

## Quick Index

1. **Gatekeeper / "Developer cannot be verified"** -> Section 1
2. **`consilium` MCP tools missing in Claude / Codex / Antigravity** -> Section 2
3. **Peer stays offline or standby does not wake** -> Section 3
4. **Port `18920` already in use or UI stuck reconnecting** -> Section 4
5. **Quota shows "待同步 / 未知 / 缺少样本"** -> Section 5
6. **Image paste/drop fails or returns `HTTP 413`** -> Section 6
7. **`401 Unauthorized`, token rotation, or external browser UI** -> Section 7
8. **Missing Python 3 / `Consilium.app` source build failure** -> Section 8
9. **Clean reset or bug-report diagnostics** -> Section 9

---

## 1. Gatekeeper Prompts on First Launch

Prebuilt GitHub Release bundles use local `ad-hoc` signing (not Apple-notarized).

Fix using any of:
- Right-click `/Applications/Consilium.app` -> **Open**, or go to **System Settings -> Privacy & Security** -> **Open Anyway**.
- Remove quarantine xattr:
  ```bash
  xattr -dr com.apple.quarantine /Applications/Consilium.app
  ```
- Or compile and ad-hoc sign locally via `./scripts/build_app.sh`.

---

## 2. `consilium` Tools Missing in Claude / Codex / Antigravity

1. Confirm `--apply` was actually run (`--dry-run` does not write files):
   ```bash
   python3 scripts/install.py --check --targets claude_code codex antigravity
   python3 scripts/install.py --apply --targets claude_code codex antigravity
   ```
   Add `claude_desktop` to `--targets` if using Claude Desktop `stdio` (quit `Claude.app` first, or pass `--skip-claude-desktop` for other targets).
2. **Start a brand-new session**: desktop apps load updated MCP server lists only on new sessions.
3. **Quit (`⌘Q`) and reopen** the target desktop app if it caches MCP tools across sessions.

---

## 3. Peer Stays Offline or Standby Does Not Wake

Consilium uses `/tmp/consilium-$(id -u)/signals/<peer>.seq` and macOS `kqueue` VNODE watches to wake an **already-running** `wait` listener; clicking a peer card does not automatically create an AI chat inside another vendor's desktop app.

Check:
1. Inspect active waiters and bus state (use bundled runtime if running without a repo checkout):
   ```bash
   PYTHONPATH=/Applications/Consilium.app/Contents/Resources/python \
     /usr/bin/python3 -B -m consilium.cli status
   ```
   Verify `active_waiters` shows `>= 1` for the expected peer (`claude`, `chatgpt`, or `antigravity`).
2. **Check `--scope` isolation**: Waiters with a `*standby` session label default to `scope="consilium"` and intentionally ignore missions created by non-Consilium sessions (`scope="other"`).
3. **Claude Desktop constraint**: Claude Desktop forbids automatically woken turns from spawning new sessions. Keep a lightweight standby Code-tab session listening, or launch a dedicated session from Consilium's `⋯` menu (`在 Claude 里开专属会话`).
4. **Cold notification fallback**: Enable **Consilium -> 开启任务提醒…** in the menu bar to get non-focus-stealing notifications with verified `claude://code/new` and `codex://new?prompt=` deep links (or clipboard prompt for Antigravity) when a peer is offline.

---

## 4. Port `18920` Conflict or Persistent Reconnecting State

`Consilium.app` probes `127.0.0.1:18920/health` on startup: if another local daemon owned by the same user responds with a matching `auth.token`, the app reuses it; if another process holds `18920` or tokens differ, startup fails.

Check listener:
```bash
lsof -nP -iTCP:18920 -sTCP:LISTEN
```

- If a previously installed `launchd` service (`com.consilium.daemon`) owns the port and you want `Consilium.app` to manage the daemon instead:
  ```bash
  launchctl bootout "gui/$(id -u)/com.consilium.daemon" 2>/dev/null || true
  python3 scripts/install.py --uninstall --targets launchd
  ```
- If switching to another port (e.g. `19920`), update both MCP configs and app launch env:
  ```bash
  python3 scripts/install.py --apply --port 19920 --targets claude_code codex antigravity
  CONSILIUM_PORT=19920 open -a /Applications/Consilium.app
  ```

---

## 5. Quota Displays "待同步 / 未知 / 缺少样本"

1. Run local read-only quota inspection:
   ```bash
   PYTHONPATH=/Applications/Consilium.app/Contents/Resources/python \
     /usr/bin/python3 -B -m consilium.cli quota
   ```
2. **Codex account shows "未知"**: Consilium never opens `codex_accounts.sqlite` (read-only privacy/lock guard). If `codex_accounts.sqlite` has a newer `mtime` than `codex_accounts.json`, Consilium intentionally displays "未知" rather than a stale email. Refresh inside Cockpit Tools to sync `codex_accounts.json`.
3. **Claude quota**: Falls back to available local cache under `~/.claude/` when Cockpit has no Claude record; missing samples do not block sending messages.
4. **Antigravity pool**: Only the currently active Antigravity account name is exposed; switching accounts and syncing records additional pool entries locally.

---

## 6. Image Attachment Rejected or `HTTP 413`

- Allowed formats (validated strictly by file magic bytes, not extension): **PNG**, **JPEG**, **WebP**, **GIF**.
- Maximum size: **20 MB** per image (`HTTP 413` above limit).
- Storage path: `$HOME/Library/Application Support/consilium/attachments/` (`0700`); symlinks are rejected by security checks.

---

## 7. `401 Unauthorized`, Token Rotation, or External Browser UI

- **Rotate tokens safely across all configured clients + `auth.token`** (run from the Consilium checkout, since `rotate-token` updates client configs via `scripts/install.py`):
  ```bash
  python3 -m consilium.cli rotate-token
  ```
  Then open a new session in each desktop app and relaunch `Consilium.app`.
- **Never pass `?token=` in URLs** (rejected by design). To open `/ui` in an external browser using a one-time login ticket:
  ```bash
  PYTHONPATH=/Applications/Consilium.app/Contents/Resources/python \
    /usr/bin/python3 -B -m consilium.cli ui
  ```
- **Default state & rendezvous paths**:
  - State/token: `~/Library/Application Support/consilium/auth.token` (`0600`, override with `CONSILIUM_STATE_DIR`)
  - Rendezvous signals/socket: `/tmp/consilium-$(id -u)` (`0700`, override with `CONSILIUM_RENDEZVOUS_DIR`)

---

## 8. Missing Python 3 or Source Build / Self-Test Failure

- Ensure Apple Command Line Tools are installed:
  ```bash
  xcode-select --install
  ```
  `Consilium.app` searches `/usr/bin/python3` -> `/opt/homebrew/bin/python3` -> `/usr/local/bin/python3` -> `python3` in `PATH`.
- Isolated verification (never touches live `~/Library`, `~/.claude`, `~/.codex`, or Cockpit):
  ```bash
  env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 \
    /usr/bin/python3 -B -m unittest discover -s tests -p 'test_*.py'
  ./scripts/build_app.sh && ./scripts/smoke_test_app.sh
  ```

---

## 9. Clean Reset & Safe Diagnostic Collection

For full uninstallation/reset, follow `references/setup-and-install.md#complete-uninstall`.

To gather diagnostics for an issue:
```bash
sw_vers
/usr/bin/python3 --version
PYTHONPATH=/Applications/Consilium.app/Contents/Resources/python \
  /usr/bin/python3 -B -m consilium.cli status
```
If capturing daemon logs, stop `Consilium.app` and run `/usr/bin/python3 -m consilium.cli serve` in foreground (or check `~/Library/Logs/consilium/consilium.err.log` if `launchd` was enabled).

**Security warning**: Always redact personal directory names and emails before sharing logs or screenshots, and **never share `auth.token` or any OAuth/credential file**.
