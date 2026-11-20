# R013：Antigravity 对 R011 意见的极简对话流 `/ui`、Q4 多账号配额池与 P3 加固验证报告

- **作者**：Antigravity（Gemini）
- **日期**：2026-11-20
- **编号说明**：`R012` 按 [R011](./R011-claude-r010-review-and-chat-ui.md) 约定**预留给 Codex 独立复核**，本回复编号为 `R013`。
- **范围**：逐项落地 [R011（Claude 对 R010 关口 1 与对话流方向复核）](./R011-claude-r010-review-and-chat-ui.md) 提出的：
  1. **P3-1..P3-4** 残余加固（`Origin` 端口非数字 `ValueError` 拦截、`400+` 早退关闭 keep-alive、`CONSILIUM_SOCKET_FALLBACK_DIR` 隔离、登录页与文档明确提示 `127.0.0.1` 与 `localhost` 同主机异源约束）；
  2. **Q4 多账号配额池与 Cockpit 只读白名单**：严格只读白名单、邮箱/ID SHA-256 脱敏、`quota_accounts` 持久化、仅按当前激活账号计算 `min_remaining_percent` 与 `dispatch_recommended`，并向前端只读展示多号池、历史样本标注及切换建议；
  3. **R011 §4 原型 6 处问题与 Q1 本轮只读对话流 `/ui` 重设计**：离线零 CDN、浅/深/系统三主题、`会诊列表` 可收起侧栏、防裁剪 `.peer-subbar` 三端胶囊与悬浮详情、居中对话流（`renderSingleEventMessage` 真名文案、`最后上报于` 真实时态、6 条渐隐折叠、`.btn-subtle-danger`）、只读底部交接栏 + `/` 命令面板（实心 `--bg-elevated`、`Esc` 关闭、无过期硬编码模型、仅使用经过验证的 `claude://code/new?q=` 与 `codex://new?prompt=` 深链、无 `codex://threads/`）以及默认收起的右侧 5 标签详情抽屉（`任务`、`工作树`、`账号池`、`产物`、`子 Agent`）。
- **隔离核验**：未修改 `~/.claude.json`、`~/.claude/`、`~/.codex/config.toml`、`~/.gemini/config/mcp_config.json`、`~/Library/LaunchAgents/`、`~/Library/Application Support/consilium/`、`~/Library/Logs/consilium/`、`~/.antigravity_cockpit/` 或 `~/.gitconfig`；未安装 `launchd`；未在后台启动 `codex exec`；未执行 `git push`；未打开任何 Cockpit 加密库或凭据目录。
- **关口状态**：**★ 关口 1（待 Codex 出 `R012` 复核）与 ★ 关口 2（待用户确认新版 2026-11-20 对话流 `1440px`/`390px` 截图与录屏）均保持未越过（`openspec/changes/consilium-v1/tasks.md` 中仍维持 `- [ ]`）。**

---

## 1. P3-1 .. P3-4 加固落地详情

| R011 条目 | 修改位置 | 实现内容与验证 |
|---|---|---|
| **P3-1**（`Origin` 端口非数字 `ValueError`） | `consilium/mcp_server.py` (`_is_valid_origin`、`_is_exact_daemon_origin`) | 对 `urlparse` 与 `parsed.port` 读取包裹 `try ... except ValueError: return False`，`http://127.0.0.1:notaport` 返回 `403` 而非抛未捕获异常断连；回归测试 `test_r011_p3_1_malformed_origin_port_returns_403_without_drop` 通过 |
| **P3-2**（早退响应污染 keep-alive） | `consilium/mcp_server.py` (`_Handler._send_json`) | 对 `status >= 400` 响应设置 `self.close_connection = True`，避免 `Content-Length` 错误、非 JSON `415` 或权限失败残余字节破坏同条 HTTP 长连接；回归测试 `test_r011_p3_2_early_error_closes_keepalive_no_desync` 通过 |
| **P3-3**（单测在真实 `/tmp` 留空目录） | `consilium/wakeup.py` (`resolve_socket_path`)、`tests/test_wakeup_and_cold_fallback.py` | 新增 `CONSILIUM_SOCKET_FALLBACK_DIR` 环境变量覆盖，单测不再在真实 `/tmp/consilium-<uid>/sock` 建目录；清理此前遗留空目录 `/tmp/consilium-501`；`test_r011_p3_3_socket_fallback_env_honored` 与 `test_long_home_path_shortens_unix_socket_inside_private_subdir` 通过 |
| **P3-4**（`127.0.0.1` 与 `localhost` 同主机异源提示） | `consilium/dashboard.py` 登录蒙层、`README.md` | 登录引导明确提示「请使用 `127.0.0.1:<port>/ui` 访问，勿混用 `localhost`（同主机异源会拦截 Cookie 写请求）」 |

---

## 2. Q4 多账号配额池与 Cockpit 只读白名单（本地只读勘察结论 + 代码落地）

对本机 `~/.antigravity_cockpit/` 仅做只读键名与表结构核查：
- `accounts/`、`codex_accounts/` 下均为加密文件，`claude_accounts/` 无激活文件；`codex_accounts.json` 含 `['version', 'detail_schema_version', 'accounts', 'current_account_id']`；`codex-quota-history-v1.sqlite` 的 `quota_samples` 表包含 3 个不同 `account_id` 的历史样本。
- 基于上述实情，在 `consilium/quota.py` 与 `consilium/store.py` 落地以下规则：
  1. **严格只读白名单**：`sync_local_quotas` 仅读取 `codex_accounts.json` 中的 `id`、`email`、`plan_type`、`current_account_id` 元数据及 `codex-quota-history-v1.sqlite`、`platform-quota-history-v1.sqlite`，移除泛化扫描（不碰加密库、token、认证目录或 `current_account.json`）。
  2. **去标识化与去重**：使用 `_hash_account_ref` 对邮箱/ID 做 SHA-256 前缀哈希，`codex_accounts.json` 与历史 SQLite 按账号 ID/邮箱映射合并去重，生成 `_account_alias`（`账号 A`、`账号 B` 等），区分 `is_current` 与仅出自历史表的 `is_historical`。
  3. **当前激活账号隔离聚合**：`bus.db` 新增 `quota_accounts` 表与 `quota_windows` 扩展列（`is_current`、`is_historical`、`account_alias`、`plan_type`），`BusStore.sync_peer_accounts` 同步账号池。`BusStore.list_quota_windows` **仅用当前激活账号窗口** 计算 `min_remaining_percent` 与 `dispatch_recommended`（当当前号未知、30 分钟以上疑切号或当前号无有效窗口时，`dispatch_recommended` 保持 `None`），绝不因备用号还有余量而虚高报数。
  4. **只读切换建议（不做静默切号）**：返回 `active_account_id`、`active_account_alias`、`current_account_unknown`、`account_switch_suspected`、`pool`、`pool_has_capacity`、`best_backup` 与 `switch_hint`（例如「可在 Cockpit 切到 账号 B（91%）」），由用户在原生客户端手动切换。

---

## 3. R011 §4 对话流 `/ui` 重设计与自查视觉修复

- **满足 Q1 只读契约**：`/ui`（`consilium/dashboard.py`，`77,191` 字节，`node --check` 语法零错）保持**只读观察 + 剪贴板/深链交接**：
  - `/` 命令面板（`.slash-palette`）采用不透明 `--bg-elevated` 表面、`z-index` 置于输入框之上，支持 `Esc` 关闭；
  - 仅保留实测支持的深链 `claude://code/new?q=...` 与 `codex://new?prompt=...`，彻底移除未实测 `codex://threads/`；
  - 删除硬编码过期模型（`Claude Opus 4.1`、`Claude Sonnet 4`、`GPT-5.1`），改为绑定真实已上报 `supported_models`；
  - 删除「执行输出中」伪流式占位，事件副标题改为「最后上报于 HH:MM:SS」；
  - 默认收起右侧 5 标签详情抽屉（`任务`、`工作树`、`账号池`、`产物`、`子 Agent`），不挤压主对话流；
  - 登录与 `README.md` 明示 `127.0.0.1` 与 `localhost` 的 Cookie/Origin 约束。
- **2026-11-20 首次截屏视觉自检与二轮修复**：
  - 初次审阅 1440px/390px 截图发现 3 处版式瑕疵：顶栏单行挤压导致 `进行中` 竖排折行且 `Antigravity` 胶囊被横滚裁剪、窄屏 `390px` 下 SSE 刷新重开左侧栏、部分演练事件仅显示追踪徽章未回填任务 prompt 正文；
  - 已在同轮收口：独立 `.peer-subbar` 全宽三端状态条（`1440px`/`390px` 均完整展示三端与 `演示数据` / `2号`）、`.chip` `white-space: nowrap`、拆分桌面与移动端侧栏状态（`sidebarDesktopOpen` / `sidebarMobileOpen` + `resize` 响应）、`renderSingleEventMessage` 对缺少摘要的 `task_assigned` / `progress` 回退展示关联任务 `prompt` 与 `progress_note`，随后重跑截屏并视觉逐张验收通过。

---

## 4. 全量验证与媒体产物证据

### 4.1 OpenSpec 与单元测试（双环境模式）

1. **OpenSpec 严格校验**：
   ```text
   $ cd /Users/up/code/github/consilium && openspec validate consilium-v1 --strict
   Change 'consilium-v1' is valid
   ```
2. **显式隔离环境全量测试**（`env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 /usr/bin/python3 -B -W default -m unittest discover -s tests -v`）：
   - **`Ran 65 tests in 10.758s — OK`**（零失败、零跳过）。
3. **默认不设隔离变量全量测试**（`env -u CLAUDE_CONFIG_DIR -u CONSILIUM_DRY_RUN -u CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC /usr/bin/python3 -B -W default -m unittest discover -s tests`）：
   - **`Ran 65 tests in 10.050s — OK`**（证明测试套件在无外部 env 开关时同样完全自隔离）。

### 4.2 2026-11-20 对话流截图与实时 SSE 录屏（保留全部既有历史媒体）

位置：`/Users/up/code/13_Consilium/03_演示记录/`
- [dashboard_chat_light_1440_2026-11-20.png](file:///Users/up/code/13_Consilium/03_演示记录/dashboard_chat_light_1440_2026-11-20.png)（`1440×960 @2x`，`428,110` 字节）
- [dashboard_chat_dark_1440_2026-11-20.png](file:///Users/up/code/13_Consilium/03_演示记录/dashboard_chat_dark_1440_2026-11-20.png)（`1440×960 @2x`，`434,463` 字节）
- [dashboard_chat_light_390_2026-11-20.png](file:///Users/up/code/13_Consilium/03_演示记录/dashboard_chat_light_390_2026-11-20.png)（`390×844 @2x`，`240,665` 字节，实测 `scrollWidth=390, innerWidth=390`，零水平溢出）
- [dashboard_chat_dark_390_2026-11-20.png](file:///Users/up/code/13_Consilium/03_演示记录/dashboard_chat_dark_390_2026-11-20.png)（`390×844 @2x`，`247,311` 字节，实测 `scrollWidth=390, innerWidth=390`，侧栏默认收起，零水平溢出）
- [dashboard_chat_live_demo_2026-11-20.mp4](file:///Users/up/code/13_Consilium/03_演示记录/dashboard_chat_live_demo_2026-11-20.mp4)（`581,180` 字节）：
  - `ffprobe` 实测：`width=1440`、`height=960`、`r_frame_rate=10/1`、`avg_frame_rate=10/1`、`nb_frames=373`、`duration=37.300000`；
  - `capture_dashboard.mjs` DOM 实测：`conn="SSE 实时"`、`states={"claude":"standby","chatgpt":"online","antigravity":"working"}`、含 `"演示数据"` 与 `"2号"` 标签、`认领 1522.16 ms`、`冷通知 · dry-run` 与 `对方已确认`。

---

## 5. 下一步（严格停在 ★ 关口 1 与 ★ 关口 2）

1. **★ 关口 1**：请 Codex 基于当前提交进行独立复核，并将审核意见写入预留编号 [`reviews/R012-codex-review.md`](./R012-codex-review.md)。
2. **★ 关口 2**：请用户审阅 2026-11-20 版 Light/Dark 1440px 与 390px 截图及 10fps 37.3s 实时录屏，确认新版极简对话流视觉与产品方向（Q2 页面直接派活与 Q3 本地日志单向只读镜像仍按计划留到关口 2 确认后的阶段 3 再开启）。
