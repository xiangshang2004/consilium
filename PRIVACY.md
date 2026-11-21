# Privacy Policy / 隐私协议

**Effective Date / 生效日期**: 2026-11-22  
**Repository / 项目地址**: https://github.com/xiangshang2004/consilium

> **English Summary — 100% Local-First, Zero Telemetry**  
> Consilium runs **entirely on your Mac**. It contains **zero analytics, zero telemetry, zero crash-reporting SDKs, and zero outbound network calls**. The local daemon listens exclusively on `127.0.0.1` with a `0600` Bearer secret, serves an offline UI locked down with `default-src 'none'` Content-Security-Policy, and wraps it in a `WKWebsiteDataStore.nonPersistent()` macOS app that blocks all external URLs. Local quota/model indicators strictly use a read-only whitelist (never opening encrypted account vaults or `codex_accounts.sqlite`). Account display names stored locally never leave your Mac or get sent to AI models. When you dispatch a task to **Claude**, **ChatGPT / Codex**, or **Antigravity**, your prompt, selected folder path, conversation context, and local attachment file paths are passed locally to that third-party desktop app and are governed by its own privacy policy from that point onward.

---

## 一、核心原则

Consilium 是一个**完全本机运行的 macOS 开源工具**，设计遵循“数据不出本机、最小必要读取、用户完全可控”三项原则：

1. **零遥测、零外部联网**：Consilium 后端基于纯 Python 3.9+ 标准库实现（`dependencies = []`，无第三方联网 SDK），内置界面和原生 App 不包含任何埋点、统计、广告、崩溃上报或版本检测请求。
2. **严格仅限本机回环**：后台服务仅绑定 `127.0.0.1`（默认端口 `18920`），界面静态资源完全内联离线；`Consilium.app` 使用非持久化 `WKWebsiteDataStore.nonPersistent()`，并拦截所有非本机 `127.0.0.1:<端口>` 的网页跳转（仅用户明确操作时允许唤起已核实的 `claude://code/new` 与 `codex://new` 本地应用深链）。
3. **明示授权与隔离**：仅在您通过系统弹窗选定文件夹或粘贴/选择图片，或主动运行安装脚本时，才接触相应路径。

---

## 二、Consilium 读取哪些本机数据（严格只读）

为在窗口中汇合任务并展示各端状态与额度余量，Consilium 会按**最小白名单**只读访问以下本机路径（您也可通过环境变量 `CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1` 关闭本地额度同步）：

| 来源 | 读取路径（均在本机 `~/` 下） | 读取字段与用途 | 明确**不读取**的内容 |
|---|---|---|---|
| **Cockpit Tools** | `codex_accounts.json`、`claude_accounts.json`、`provider_current_accounts.json`、`codex_instances.json`、`current_account.json`；只读 URI `mode=ro` 查询 `platform-quota-history-v1.sqlite`、`codex-quota-history-v1.sqlite`（位于 `~/.antigravity_cockpit/`） | 账号 ID（立即单向哈希）、当前激活指针、套餐类型、各窗口剩余百分比与重置时间，用于在 Consilium 窗口展示余量并辅助避免向已耗尽端派活 | **绝不打开** `codex_accounts.sqlite`（仅通过 `os.stat` 比较文件修改时间以判断索引新旧）；**绝不读取** `accounts/` 加密账号文件或 `cache/quota_api_v1_desktop/authorized/*.json` 中的凭证与密钥 |
| **Codex 本机日志** | `~/.codex/sessions/**/rollout-*.jsonl` | 仅截取最新最多 5 个文件末尾各不超过 `128 KB` 中包含 `"rate_limits"` 的行，读取 `5h`/`7d` 数值占用与重置时间，读取后立即丢弃原始行 | 不保存、不索引您的历史 Codex 对话正文 |
| **Claude 本机缓存** | `~/.claude/usage-latest.json`、`~/.claude/quota.json` | 提取速率限制窗口与统一限流头字段，并对账号标识做单向哈希 | 不读取您的 Claude 对话记录或认证凭证 |
| **Antigravity 本机记录** | `~/.gemini/antigravity/brain/*/.system_generated/logs/transcript[_full].jsonl` | 仅扫描匹配当前 `(task_id, attempt_id)` 上报调用的 `PLANNER_RESPONSE` 行中的 `model` 字段，用于在任务回复上显示“规划模型 · 执行模型” | 不向数据库或界面转存原始会话思考或完整历史文本 |
| **用户主动选取内容** | 通过 macOS `NSOpenPanel` 选中的工作目录、粘贴/选中的 PNG / JPEG / WebP / GIF 图片 | 将工作目录作为任务执行上下文；图片经头部魔数校验后保存（单张上限 `20 MB`） | 未经您明确选择的其他目录、桌面或个人文件 |
| **显式安装配置**（仅在主动执行 `scripts/install.py --apply` 时） | `~/.gemini/config/mcp_config.json`、`~/Library/Application Support/Claude/claude_desktop_config.json`、`~/.claude.json`、`~/.codex/config.toml`、`~/.{claude,codex,gemini}/skills/consilium-collab/`、`~/Library/LaunchAgents/com.consilium.daemon.plist` | 写入本地 MCP 连接与协作技能；写前生成 `0700`/`0600` 时间戳备份，支持 `--uninstall` 还原 | 默认 `--dry-run` 不做任何写入；不修改未选定的客户端目标 |

---

## 三、本机存储什么、存在哪里、如何隔离

1. **持久状态目录**（`~/Library/Application Support/consilium/`，目录权限 `0700`，文件权限 `0600`）：
   - `auth.token`：256 位随机本机访问密钥。
   - `bus.db`（SQLite WAL）：保存您发起的会诊标题、工作目录路径、您发送的消息原文、任务分配与进度/回复汇总、用户可删偏好（有效上限 30 条）、幂等请求键（`idempotency_keys`）、按 `acct-<10位hex>` 哈希脱敏的账号配额窗口（`quota_accounts` / `quota_windows`），以及仅用于本机窗口展示的 `quota_account_names`。
   - `attachments/`：保存您上传或粘贴的图片副本（目录 `0700`、文件 `0600`，未发送的草稿图片可在输入框移除以立即从磁盘删除）。
   - `backups/`：运行安装器 `--apply` 或 `rotate-token` 时生成的历史配置备份。
2. **临时运行目录**（`/tmp/consilium-<uid>/`，权限 `0700`）：
   - 存放 `signals/<peer>.seq` / `.sig`、`waiters/<peer>/*.lease` 进程锁租约与 `wake.sock` 本机 Unix 域套接字，供零数据库直触的后台待命门铃使用。
3. **内存与浏览器存储**：
   - 登录会话（`SessionStore`，12 小时过期）与一次性登录票据（`LoginTicketStore`，30 秒过期）仅存放在服务端内存中，不落盘。
   - 浏览器若直接访问 `http://127.0.0.1:<port>/ui`，界面会在 `localStorage` 记忆右侧栏宽度（`consilium.panelWidth`）、最近选过的最多 12 个目录（`consilium.recentFolders`）、换号提示状态（`consilium.observedAccounts`）和浅/深色主题（`consilium-theme`）；而在 `Consilium.app` 中使用 `nonPersistent()` 数据存储，关闭后不留持久化 WebView 缓存或 Cookie。
4. **账号身份严格本机隔离**：
   - 上游 Cockpit 与本机日志中的邮箱或账号 ID 在存入数据库前统一做小写归一化后 SHA-256 哈希为 `acct-<10位十六进制>`；`quota_account_names` 中的显示名称/邮箱**仅返回给本机浏览窗口（`/api/state`）展示**，在所有 MCP 工具（`consilium_quota`、`consilium_status`、`consilium_preferences`）以及发给三端的自动协调提示词中**绝不传递真实邮箱或原账号名**（仅使用中立代号）。

---

## 四、向第三方桌面应用传递的数据边界

Consilium 本身不托管任何大语言模型。当您在窗口中发送消息或点击“叫醒 / 专属会话 / 通知”时，会通过本机渠道向您选定的桌面应用（**Claude**、**ChatGPT / Codex** 或 **Antigravity**）传递以下信息：

- **通过本机 MCP**：当前消息正文、选定工作目录路径（`workspace_path`）、同一会诊的前序对话历史（`conversation`）、您保留的学习偏好与最近路由习惯摘要，以及消息所附图片的**本机磁盘路径**。
- **通过系统通知与深链**：仅当您在「Consilium」菜单主动点击「开启任务提醒…」授予权限后，才会投递经截断的任务摘要通知；点击通知时，Claude / Codex 会通过 `claude://code/new`、`codex://new?prompt=` 深链打开新会话并预填取件提示与工作目录，Antigravity 无深链则将提示词写入本机剪贴板（`NSPasteboard.general`）。

一旦消息和文件交由 **Anthropic Claude**、**OpenAI ChatGPT / Codex** 或 **Google Antigravity** 处理，后续的云端模型推理与数据处理均受各第三方应用自身隐私协议及您在该应用中的账号设置管辖，与 Consilium 无关。

---

## 五、您的控制权与彻底清除方式

- **删除偏好与草稿**：随时可在「偏好」设置中删除任意一条已记录偏好；发送前移除图片会同步清理 `attachments/` 中的对应草稿文件。
- **关闭任务提醒**：在 macOS「系统设置 → 通知 → Consilium」中随时关闭提醒权限；也可在软件内旋转刷新访问密钥：
  ```bash
  python3 -m consilium.cli rotate-token
  ```
- **彻底卸载与删除全部数据**：
  1. 如曾运行安装脚本写入过三端配置，先移除受管条目：
     ```bash
     python3 scripts/install.py --uninstall --targets claude_code codex antigravity
     ```
  2. 将 `Consilium.app` 移入废纸篓。
  3. 删除本机全部状态、日志与临时运行文件：
     ```bash
     rm -rf "$HOME/Library/Application Support/consilium" \
            "$HOME/Library/Logs/consilium" \
            "/tmp/consilium-$(id -u)"
     ```
