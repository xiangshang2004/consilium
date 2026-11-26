# Consilium Setup & Installation Reference

Use this guide when setting up Consilium from scratch, registering desktop apps, switching ports, upgrading, or uninstalling.

## Requirements

- **OS**: macOS 13 (Ventura) or newer
- **Toolchain**: Apple Command Line Tools (`/usr/bin/python3` 3.9+ and `swiftc`) — install with `xcode-select --install` if missing. **No Xcode and no third-party `pip` packages required.**
- **Desktop Apps**: One or more of **Claude** (`Claude.app` / Claude Code), **ChatGPT / Codex Desktop**, and **Antigravity.app**.

---

## Step 1: Install `Consilium.app`

### Option A: Prebuilt Binary (Apple Silicon `arm64`, Recommended)

1. Download the latest `Consilium-<version>-arm64.dmg` (or `.zip`) and its `.sha256` file from:
   `https://github.com/xiangshang2004/consilium/releases/latest`
2. Verify checksum if desired (`shasum -a 256 -c Consilium-<version>-arm64.dmg.sha256`).
3. Drag **Consilium.app** into `/Applications/`.
4. Releases use local `ad-hoc` signing (not Apple-notarized). On first open, right-click **Consilium.app** -> **Open**, or click **Open Anyway** in **System Settings -> Privacy & Security** (or clear quarantine: `xattr -dr com.apple.quarantine /Applications/Consilium.app`).

### Option B: Build From Source (Apple Silicon or Intel, ~5s)

```bash
git clone https://github.com/xiangshang2004/consilium.git
cd consilium
./scripts/build_app.sh
cp -R build/Consilium.app /Applications/
```

If running directly from `Consilium.app` without keeping a source checkout, its bundled Python runtime lives at:
`/Applications/Consilium.app/Contents/Resources/python`.

---

## Step 2: Connect Desktop Apps (`--dry-run` First)

`scripts/install.py` comes from the Consilium source repository (`https://github.com/xiangshang2004/consilium`). If the user installed `Consilium.app` via DMG/ZIP only, clone the repository (or use a temporary shallow clone) to run `scripts/install.py`:

```bash
git clone --depth 1 https://github.com/xiangshang2004/consilium.git
cd consilium

# 1. Always preview first (writes nothing)
python3 scripts/install.py --dry-run --targets claude_code codex antigravity

# 2. Apply after confirming the planned file edits (writes 0700/0600 timestamped backups)
python3 scripts/install.py --apply --targets claude_code codex antigravity
```

### Available Installer Targets

| Target | Alias(es) | What It Configures |
|---|---|---|
| `claude_code` | `claude-code`, `code` | `~/.claude.json` (`mcpServers.consilium`) + `~/.claude/skills/consilium-collab/SKILL.md` |
| `codex` | — | `~/.codex/config.toml` managed `# BEGIN CONSILIUM MCP` block + `~/.codex/skills/consilium-collab/SKILL.md` |
| `antigravity` | `ag`, `gemini` | `~/.gemini/config/mcp_config.json` (`mcpServers.consilium`) + `~/.gemini/config/skills/consilium-collab/SKILL.md` |
| `claude_desktop` | `claude-desktop`, `desktop`, `claude_app`, `claude-app` | `~/Library/Application Support/Claude/claude_desktop_config.json` (`stdio` bridge; quit `Claude.app` before applying, or pass `--skip-claude-desktop`) |
| `claude_agents` | `claude-agents` | `~/.claude/agents/` (`opus-architect.md`, `opus-builder.md`, `opus-scout.md`) |
| `codex_agents` | `codex-agents` | `~/.codex/agents/` (`architect-high.toml`, `builder-balanced.toml`, `scout-fast.toml`) |
| `launchd` | `plist` | `~/Library/LaunchAgents/com.consilium.daemon.plist` (optional background daemon; skip if `Consilium.app` manages the daemon itself) |
| `all` | — | All targets above |

Useful flags:
- `--check`: Inspect current config status without writing.
- `--port <port>`: Register a custom port instead of default `18920` (must match `CONSILIUM_PORT` when launching `Consilium.app`).
- `--skip-claude-desktop`: Exclude `claude_desktop` when `Claude.app` is currently running.

---

## Step 3: Load MCP Tools, Launch & Wake

1. **Start a new session** in Claude / Codex / Antigravity (or `⌘Q` quit and relaunch the app if an older version caches MCP servers). Running sessions do not hot-load newly written MCP entries.
2. **Launch `Consilium.app`** (`open -a /Applications/Consilium.app`). The menu bar app starts and stops the `127.0.0.1:18920` daemon automatically, or reuses an already-running daemon with matching `auth.token`.
3. **(Optional) Notifications**: Click **Consilium -> 开启任务提醒…** in the menu bar to enable non-focus-stealing macOS banners when a peer is offline.
4. **(Optional) Quota Sync**: Install and refresh [Cockpit Tools](https://github.com/jlcodes99/cockpit-tools/releases) on the same macOS account for Codex / Antigravity quota visibility. Consilium reads `~/.antigravity_cockpit/` strictly read-only.
5. **Wake Standby Sessions**: Click **叫醒 (Wake)** on a peer card in the lower-left dock of Consilium to start or prompt its standby listener.

---

## Custom Port Configuration

```bash
python3 scripts/install.py --apply --port 19920 --targets claude_code codex antigravity
CONSILIUM_PORT=19920 open -a /Applications/Consilium.app
```

---

## Upgrade

- **Prebuilt app**: Replace `/Applications/Consilium.app` with the newer release and relaunch.
- **Source build**: `git pull --ff-only`, `./scripts/build_app.sh`, replace `/Applications/Consilium.app`, and re-run `scripts/install.py --dry-run` / `--apply` if installer templates changed.
- **This Agent Skill**: `npx skills add xiangshang2004/consilium`

---

## Complete Uninstall

```bash
# 1. Remove managed MCP/skill entries written by the installer
python3 scripts/install.py --uninstall --targets claude_code codex antigravity claude_desktop launchd

# 2. Remove app bundle, state DB/tokens, logs, and ephemeral rendezvous socket/signals
rm -rf /Applications/Consilium.app \
       "$HOME/Library/Application Support/consilium" \
       "$HOME/Library/Logs/consilium" \
       "/tmp/consilium-$(id -u)"
```
