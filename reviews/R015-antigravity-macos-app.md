# R015: `Consilium.app` 原生菜单栏封装、★ 关口 1 通过确认与 4 项补充加固

2026-11-20 · Antigravity 交付（本地分主题提交，未 `git push`）· 接续 `6e308aa` 与 [R014](./R014-claude-r013-review.md)

---

## 1. 结论与关口状态

- **★ 关口 1 已正式勾选通过**：依据 [R014](./R014-claude-r013-review.md) 与 Codex 至 `6e308aa` 的独立复核结论，已在 [openspec/changes/consilium-v1/tasks.md](../openspec/changes/consilium-v1/tasks.md) 第 19 行勾选 `★ **Gate 1**`。
- **提前完成阶段 4.5（`Consilium.app` macOS 菜单栏原生封装）**：遵照用户“做成app”与 Claude 设计方案，在 `app/macos/` 与 `scripts/build_app.sh` 落盘原生 Swift 菜单栏 App，仅使用系统自带 Xcode CLT `swiftc`、`/usr/bin/iconutil` 与 `/usr/bin/codesign -s -`（零 Xcode 工程依赖、构建期零 Homebrew 依赖），将封装后的 `Consilium.app` 输出至已 `.gitignore` 忽略的 `build/Consilium.app`。
- **4 项补充与 R014 遗留加固全部落地**：
  1. HTTP/1.1 `GET` 分帧与全方法未消费体连接关闭（`consilium/mcp_server.py`）；
  2. Cockpit `codex_accounts.sqlite` 修改时间新于 `codex_accounts.json` 时拒绝采信 `current_account_id` 且绝不打开 `codex_accounts.sqlite`（`consilium/quota.py`）；
  3. 启动期 `rebuild_all_peer_seqs` 对积压队列/未确认事件发送唤醒脉冲（`consilium/wakeup.py`、`consilium/store.py`）；
  4. 安装器 Phase 2 中途失败抛出带脱敏逐目标状态明细的 `InstallApplyError`（`scripts/install.py`）；另在跨主题切换时修复了 `WKWebView` 离屏按钮 CSS `transition` 冻结导致的深色模式白底残留（`consilium/dashboard.py` + `MainWindowController.swift`）。
- **验证结果**：
  - `scripts/build_app.sh` 从干净源码构建并完成 ad-hoc 签名与严格验签（产物 binary `480K`、`AppIcon.icns` `151K`）；
  - `scripts/smoke_test_app.sh` 隔离双路冒烟通过（托管自启 daemon `/health=200`、免令牌 `/api/login=200`、`/api/state=200`、`WKWebView` 自动登录+`SSE 实时` 且退出 App 同步停 daemon；复用已运行 daemon 时不重复启动且退出 App 保持原 daemon 存活）；
  - `11` 模块共 `79` 项 Python 单元测试（隔离环境变量模式 `17.050s` 与默认未设模式 `18.990s`）全绿（`0` 失败/`0` 跳过）；
  - `openspec validate consilium-v1 --strict` 返回 `Change 'consilium-v1' is valid`；
  - 已采得 2026-11-20 菜单栏下拉与窗口浅色/深色 3 张截图（零录屏、保留全部既有截图）。
- **范围说明**：因本轮先执行用户“做成app”及关口 1 补充修复 4 项，[R014](./R014-claude-r013-review.md) §4 UI 细节微调（`390px` 窄屏顶部折叠、`Mission` 文案改`会诊`、非 dry-run 徽章）与 §6 阶段 3 方案文档（`<external-demo-dir>/04_B级试跑/`）仍留在待办队列，等待本轮 R015 经 Claude 与 Codex 复核后安排。

---

## 2. `Consilium.app` 与 4 项补充逐项对照（含优化说明）

| 要求项 | 实现细节 | 位置 / 验证 |
|---|---|---|
| **1. Swift 源码与免 Homebrew/Xcode 构建** | `app/macos/Sources/*.swift`（`Config`、`Models`、`DaemonManager`、`AuthAndStreamClient`、`NotificationManager`、`StatusBarController`、`MainWindowController`、`AppDelegate`、`main`）与 `app/macos/Info.plist`；`scripts/build_app.sh` 使用系统 `swiftc`（`-swift-version 5`、链接 `Cocoa`/`WebKit`/`UserNotifications`）、`/usr/bin/iconutil` 与 `/usr/bin/codesign`。仓库根 `.gitignore` 已含 `build/`。 | [scripts/build_app.sh](../scripts/build_app.sh) · [app/macos/Info.plist](../app/macos/Info.plist) |
| **2. 菜单栏 `NSStatusItem` 与单色模板图标** | 从 `consilium-mark-light.svg` 派生纯黑透明底 [consilium-mark-template.svg](../docs/brand/consilium-mark-template.svg)，预栅格为 `StatusIconTemplate.png`（18×18）与 `@2x`（36×36）提交进仓库；`StatusBarController` 开启 `isTemplate = true`，展示 `Consilium · 连接态`、三端彩色状态灯圆点 + 当前号余量、当前会诊、`打开 Consilium (⌘O)` 与 `退出 (⌘Q)`。 | [consilium-mark-template.svg](../docs/brand/consilium-mark-template.svg) · [StatusBarController.swift](../app/macos/Sources/StatusBarController.swift) |
| **3. 非持久 `WKWebView` 窗口** | `MainWindowController` 使用 `WKWebsiteDataStore.nonPersistent()` 加载 `http://127.0.0.1:<port>/ui`，在 `Info.plist` 配置精准局部例外 `NSAllowsLocalNetworking = true`（不放开外部明文 HTTP）；`WKNavigationDelegate` 仅放行回环原站，并将点击的 `claude://`、`codex://` 转发给 `NSWorkspace.shared.open`。 | [MainWindowController.swift](../app/macos/Sources/MainWindowController.swift) |
| **4. 免令牌 Cookie 直登** | App 读取 `<state_dir>/auth.token`，经隔离 `URLSessionConfiguration.ephemeral` 调用 `POST /api/login` 取回 `consilium_session`，写入 `WKWebsiteDataStore.httpCookieStore` 后再加载 `/ui`；不走 `/ui/auth?ticket=`。若首帧 `WKWebView` 遇冷启动 Cookie 竞争仍见登录遮罩，自动重试加载一次。 | [AuthAndStreamClient.swift](../app/macos/Sources/AuthAndStreamClient.swift) · [MainWindowController.swift](../app/macos/Sources/MainWindowController.swift) |
| **5. 内置/复用 Daemon 管理** | 启动先探测 `127.0.0.1:<port>` 与 `/health`（Bearer 鉴权）：若 200 则直接挂载复用（`ownsDaemon = false`，退出时不停止原服务）；若未占用则以 `/usr/bin/python3 -B -m consilium.cli serve` 拉起打包于 `Contents/Resources/python/` 的纯标准库 `consilium`（设 `PYTHONDONTWRITEBYTECODE=1` 避免在已签名 App 包内生成 `__pycache__` 破坏签名，剥离 `CLAUDE_CONFIG_DIR`）；App 退出或收到 `SIGTERM`/`SIGINT` 时仅停掉自己拉起的子进程。 | [DaemonManager.swift](../app/macos/Sources/DaemonManager.swift) |
| **6. `UNUserNotificationCenter` 与点击深链** | 首次连接仅建立基线 `highestSeenEventId` 不重弹旧事件；之后新增未确认 `task_assigned` / `review_request` / `loop_guard_reached` / `task_stale` 发系统通知。仅在 `userNotificationCenter(_:didReceive:)`（用户明确点击）里构造经 R003 核实的 `claude://code/new?q=...` 或 `codex://new?prompt=...&path=...` 并打开（Antigravity 复制提示词并聚焦 App 窗口）；传 `--no-notifications` 或 `CONSILIUM_DRY_RUN=1` 时跳过授权弹窗与推送，避免自动化冒烟被 TCC 阻塞。 | [NotificationManager.swift](../app/macos/Sources/NotificationManager.swift) |
| **7. App 图标预渲染集与 ad-hoc 签名** | 已预先把 `docs/brand/consilium-app-icon.svg` 转为完整 10 尺寸 [AppIcon.iconset](../app/macos/Resources/AppIcon.iconset) PNG 提交入仓，使 `build_app.sh` 只用系统 `iconutil` 即可生成 `AppIcon.icns`，并以 `codesign --force --deep --sign -` 签名与 `--verify --deep --strict` 校验。 | [app/macos/Resources/](../app/macos/Resources/AppIcon.iconset) |
| **8. 补充修复 1：GET 分帧** | `do_GET` 改为初始化 `_body_consumed = False` 并调用 `_consume_optional_body()`；`_send_json` 与 `_send_html` 将 `has_unconsumed_post` 扩至全部 HTTP 方法（含 `GET` 未消费请求体或 `Transfer-Encoding` 即强制 `Connection: close`）。 | [consilium/mcp_server.py](../consilium/mcp_server.py) · `test_r015_get_with_body_followed_by_health_on_same_keepalive_connection` |
| **9. 补充修复 2：Cockpit 旧 JSON 索引判旧** | `_load_cockpit_codex_registry` 仅用 `os.stat` 比较 `codex_accounts.sqlite` 与 `codex_accounts.json` 的 `st_mtime`（绝不打开 `codex_accounts.sqlite`）；若 SQLite 较新则置 `json_index_stale = True`、忽略 `current_account_id`，并由 `sync_local_quotas` 清除旧当前号标记，使配额与当前号安全回退为“账号未知”。 | [consilium/quota.py](../consilium/quota.py) · `test_r015_cockpit_newer_sqlite_distrusts_json_current_without_opening_sqlite` |
| **10. 补充修复 3 & 4：`.seq` 启动脉冲与安装器分目标状态** | `write_peer_seq(..., pulse_if_backlog=True)` 与 `rebuild_all_peer_seqs` 对 `unacked > 0` 或 `queued_tasks > 0` 的 peer 触发脉冲，保证会合目录清理后重启能唤醒阻塞 `wait`；`run_installer` Phase 2 新增逐目标结果集 `target_results` 及部分失败异常 `InstallApplyError`（报告 `applied_targets`、`failed_target`、`failed_target_may_be_modified`、`not_attempted_targets` 并经 `_redact_secrets` 脱敏）。 | [consilium/wakeup.py](../consilium/wakeup.py) · [consilium/store.py](../consilium/store.py) · [scripts/install.py](../scripts/install.py) · `test_r015_rebuild_all_peer_seqs_pulses_backlog_after_rv_delete` · `test_r015_installer_mid_failure_reports_per_target_status_redacted` |
| **11. 补充说明：深色模式 `WKWebView` 按钮重绘修复** | 初拍 `app_window_dark_2026-11-20.png` 时发现 WebKit 离屏渲染对带 `transition: background 150ms` 的 `<button>` 会冻结在浅色初始帧。已为 `:root` 与暗色规则补齐 `color-scheme`、`button { appearance: none; }` 及 `no-theme-transition` 切题类，并同步修好 `MainWindowController` JS IIFE 返回值确认，使浅/深两版窗口按钮配色与当前激活选项完全正确。 | [consilium/dashboard.py](../consilium/dashboard.py) · [MainWindowController.swift](../app/macos/Sources/MainWindowController.swift) |

---

## 3. 全量验证与截图证据

### 3.1 `scripts/build_app.sh` 与 `scripts/smoke_test_app.sh`

```text
<repo-root>/build/Consilium.app: replacing existing signature
Built and signed: <repo-root>/build/Consilium.app
-rwxr-xr-x@ 1 <user>  staff   480K Nov 20 14:26 <repo-root>/build/Consilium.app/Contents/MacOS/Consilium
-rw-r--r--@ 1 <user>  staff   151K Nov 20 14:26 <repo-root>/build/Consilium.app/Contents/Resources/AppIcon.icns

[smoke] Case 1: Consilium.app managed daemon lifecycle on 127.0.0.1:54997
[smoke] Case 1 OK: managed daemon started, auto-logged in, served SSE, and stopped cleanly on exit.
[smoke] Case 2: Consilium.app reuse of pre-running daemon on 127.0.0.1:55029
{"health_status": 200, "login_status": 200, "mission_count": 0, "ok": true, "owns_daemon": false, "port": 55029, "rendezvous_dir": "/tmp/consilium-app-smoke.mbJHqH/rv2", "sse_connected": true, "state_dir": "/tmp/consilium-app-smoke.mbJHqH/state2", "state_status": 200, "webview_conn_text": "SSE 实时", "webview_logged_in": true}
[smoke] Case 2 OK: reused existing daemon cleanly and left it running on exit.
```

### 3.2 `openspec validate consilium-v1 --strict`

```text
Change 'consilium-v1' is valid
```

### 3.3 双模式 Python 全量单测（`11` 模块 `79` 项）

新增模块 [tests/test_r015_regressions.py](../tests/test_r015_regressions.py)（4 项回归）连同既有 10 模块共 79 项全部通过：

```text
# 模式 A：隔离环境变量（-u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1）
...............................................................................
----------------------------------------------------------------------
Ran 79 tests in 17.050s

OK

# 模式 B：默认未设环境变量（-u CLAUDE_CONFIG_DIR -u CONSILIUM_DRY_RUN -u CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC）
...............................................................................
----------------------------------------------------------------------
Ran 79 tests in 18.990s

OK
```

### 3.4 `2026-11-20` `Consilium.app` 截图（仅截图，无录屏）

- 菜单栏模板图标与实时下拉菜单：`app_menubar_2026-11-20.png`（早期外部演示归档）（`65K`）
- 原生 App 窗口浅色模式（免令牌直入 + `SSE 实时`）：`app_window_light_2026-11-20.png`（早期外部演示归档）（`513K`）
- 原生 App 窗口深色模式（免令牌直入 + `SSE 实时`）：`app_window_dark_2026-11-20.png`（早期外部演示归档）（`518K`）

---

## 4. 边界遵守确认

- 未安装进 `/Applications`，未配置登录项，未安装 `launchd`，未写入任何 `~/.claude`、`~/.codex`、`~/.gemini`、`~/.antigravity_cockpit`、`~/Library` 实时目录，未增加任何写数据 HTTP 接口，未执行 `git push`。
