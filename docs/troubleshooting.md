# Consilium 新用户快速排障指南

> 本文覆盖安装启动、三端接入、门铃待命、端口冲突、额度同步与日志采集等常见问题。  
> 如排查后仍未解决，请先阅读 [安全政策（SECURITY.md）](../SECURITY.md) 与 [隐私协议（PRIVACY.md）](../PRIVACY.md)，再前往 [GitHub Issues](https://github.com/xiangshang2004/consilium/issues) 反馈。

## 快速定位表

| 常见症状 | 跳转章节 |
|---|---|
| 首次打开 `Consilium.app` 被 macOS 拦截 / 提示「无法验证开发者」 | [1. Gatekeeper 拦截](#1-首次打开提示无法验证开发者--gatekeeper-拦截) |
| Claude / Codex / Antigravity 里看不到 `consilium` 工具 | [2. 桌面应用找不到工具](#2-claude--codex--antigravity-里看不到-consilium-工具) |
| 点了「叫醒」仍显示离线，或发消息后待命会话不响应 | [3. 门铃待命与系统通知](#3-点了叫醒仍显示离线或待命不醒) |
| 端口 `18920` 被占用、提示认证失败或一直显示重连 | [4. 端口占用与 daemon 冲突](#4-端口-18920-被占用或一直显示重连) |
| 额度显示「待同步」「未知」或缺少样本 | [5. 额度与 Cockpit 显示异常](#5-额度显示待同步-未知或缺少样本) |
| 粘贴/拖入截图失败，或接口返回 `413` | [6. 图片附件限制](#6-粘贴拖入图片失败或返回-413) |
| 接口返回 `401`、需要轮换令牌或在外部浏览器打开面板 | [7. 令牌认证与独立浏览器访问](#7-鉴权失败-401-或需要轮换令牌) |
| 找不到 Python 3 或从源码构建 `Consilium.app` 失败 | [8. Python 依赖与源码编译](#8-缺少-python-3-或从源码构建失败) |
| 需要完整重置环境，或准备提交 Bug 诊断信息 | [9. 完整卸载与提交 Issue 诊断清单](#9-完整卸载与提交-issue-诊断清单) |

---

## 1. 首次打开提示「无法验证开发者」/ Gatekeeper 拦截

**原因**：GitHub Releases 发布的 `Consilium.app` 目前使用本地 `ad-hoc` 签名（未经过 Apple 公证），macOS 会拦截首次双击打开。

**处理方法（任选其一）**：
1. **图形界面放行**（推荐）：右键点击 `/Applications/Consilium.app` → 选择「打开」；或打开「系统设置 → 隐私与安全性」向下滚动，点击 **「仍要打开」**。
2. **命令行移除下载隔离属性**：
   ```bash
   xattr -dr com.apple.quarantine /Applications/Consilium.app
   ```
3. **本机源码重编**：如果你本机已安装 Apple Command Line Tools，也可以自己重新编译并本地签名：
   ```bash
   ./scripts/build_app.sh
   ```

---

## 2. Claude / Codex / Antigravity 里看不到 `consilium` 工具

**排查顺序**：
1. **确认已真正写入安装配置**（`--dry-run` 只演练不写盘）：
   ```bash
   # 先预览将修改的配置文件
   python3 scripts/install.py --dry-run --targets claude_code codex antigravity

   # 确认后正式写入（可选目标还包含 claude_desktop、claude_agents、codex_agents、launchd）
   python3 scripts/install.py --apply --targets claude_code codex antigravity
   ```
   如果你还在使用 Claude Desktop，可额外追加 `claude_desktop`。
2. **在桌面应用中重新开一个新会话**：三家桌面端只在**新建会话**时读取最新 MCP 配置，已开启的历史对话不会热加载新加的工具。
3. **重启被修改配置的应用**：部分版本需要彻底 `⌘Q` 退出再重开才会刷新 MCP server 列表与 `consilium-collab` 技能。

---

## 3. 点了「叫醒」仍显示离线或待命不醒

**先理解原理**：Consilium 使用 `/tmp/consilium-$(id -u)/signals/<peer>.seq` 与 `kqueue` 门铃唤醒已处于 `wait` 状态的待命会话；**点击桌面端应用本身并不会主动启动一个 AI 对话**（待命会话需要由对应端内的会话先运行起来）。

**排查步骤**：
1. **查看当前活跃等待者（Waiter）与门铃状态**：
   ```bash
   python3 -m consilium.cli status
   ```
   检查输出中的 `waiters` 列表是否真的存在对应端（`claude` / `chatgpt` / `antigravity`）的活跃只读等待进程。
2. **检查 `--scope` 来源隔离**：以 `*standby*` 命名的待命会话默认使用 `scope="consilium"`，只会响应由 Consilium 窗口发起的会诊任务，不会误抢该应用自己在其它项目建立的 MCP 任务。
3. **Claude Desktop 专属提示**：Claude Desktop 不会由后台唤醒自动新建聊天；请保持一个轻量待命会话，或通过右上角 `⋯` 菜单开启并绑定当前会诊的专属会话。
4. **开启冷通知回退**：如果某端暂未挂待命，可在顶部菜单栏点击「Consilium → **开启任务提醒…**」授予系统通知权限。发出消息时若对应端离线，Consilium 会发送不抢焦点的通知，点击即可通过 `claude://code/new`、`codex://new?prompt=` 深链或剪贴板快速接手。

---

## 4. 端口 `18920` 被占用或一直显示重连

**行为说明**：`Consilium.app` 启动后会先探测 `127.0.0.1:18920/health`。若已有同一个本机用户的 Consilium daemon 在运行且令牌匹配，App 会**直接复用它**并在退出时不关闭它；若端口被其它程序占用或令牌不一致，则会报错拒绝启动第二个实例。

**排查命令**：
```bash
# 查看是谁占用了 18920 端口
lsof -nP -iTCP:18920 -sTCP:LISTEN
```

- **如果之前启用过 `launchd` 后台常驻服务**（标签为 `com.consilium.daemon`），想交回给 App 独自管理，可先卸载 `launchd`：
  ```bash
  launchctl bootout "gui/$(id -u)/com.consilium.daemon" 2>/dev/null || true
  python3 scripts/install.py --uninstall --targets launchd
  ```
- **需要改用自定义端口（例如 `19920`）**：安装器与 App 的端口必须保持一致：
  ```bash
  # 1. 重新把各端 MCP 配置写入新端口
  python3 scripts/install.py --apply --port 19920 --targets claude_code codex antigravity

  # 2. 用指定端口启动 App（或手动 cli serve --port 19920）
  CONSILIUM_PORT=19920 open -a /Applications/Consilium.app
  ```

---

## 5. 额度显示「待同步 / 未知 / 缺少样本」

**检查步骤**：
1. 运行一次本地只读额度排查命令：
   ```bash
   python3 -m consilium.cli quota
   ```
   它会打印当前采集到的窗口余量、重置时间及采样来源。
2. **关于 Codex 当前账号显示为「未知」**：  
   Consilium 读取 `~/.antigravity_cockpit/` 下的公开历史表，但**出于隐私和文件锁安全，绝不打开 `codex_accounts.sqlite`**。当工具仅比较 `mtime` 发现 `codex_accounts.sqlite` 比 `codex_accounts.json` 更新时，会故意将当前账号标记为 **「未知」**，避免展示过期的错误邮箱；在 Cockpit Tools 里刷新一次即可同步回 JSON。
3. **关于 Claude 额度**：Claude 无 Cockpit 记录时，Consilium 会退回读取 `~/.claude/` 下可用的本地速率缓存；如从未发送过请求或缓存未更新，界面会保留「缺少样本」提示，不会阻断正常发消息（仅停在保守提醒）。
4. **关于 Antigravity 账号 Pool**：Antigravity 只会暴露当前激活的账号名；切换到其它账号并运行过同步后，才会记录到本机历史列表中。

---

## 6. 粘贴/拖入图片失败或返回 `413`

Consilium 只允许纯本机上传以下格式的图片附件：
- **支持格式**（按文件头魔数严格校验，不看伪装后缀）：`PNG`、`JPEG`、`WebP`、`GIF`
- **体积上限**：单张图片不得超过 **20 MB**，超出时接口会返回 `HTTP 413`
- **权限限制**：附件保存在 `$HOME/Library/Application Support/consilium/attachments/`（权限 `0700`），如果该目录被手动改成了符号链接会被安全策略拒绝写入

---

## 7. 鉴权失败 (`401`) 或需要轮换令牌

- **一键轮换全部令牌**（先更新已配置的桌面端，再原子替换 `auth.token`，防止业务断联）：
  ```bash
  python3 scripts/install.py --rotate-token --targets claude_code codex antigravity
  # 或使用 CLI 快捷子命令：
  python3 -m consilium.cli rotate-token
  ```
  轮换后需重开或刷新桌面端会话，并重启 `Consilium.app` 重新登录 `POST /api/login`。
- **切勿手工在地址栏传 `?token=`**：服务端已禁用 URL 查询参数传令牌。若想在系统浏览器里打开面板，请运行一次性票据命令：
  ```bash
  python3 -m consilium.cli ui
  ```
- **状态目录与令牌路径**：默认在 `~/Library/Application Support/consilium/auth.token`（权限 `0600`），可通过 `CONSILIUM_STATE_DIR` 覆盖；临时门铃与 socket 默认在 `/tmp/consilium-$(id -u)`，可通过 `CONSILIUM_RENDEZVOUS_DIR` 覆盖。

---

## 8. 缺少 Python 3 或从源码构建失败

- **安装 Apple Command Line Tools**（提供 `/usr/bin/python3` 3.9+ 与 `swiftc`）：
  ```bash
  xcode-select --install
  ```
  `Consilium.app` 启动后台服务时按顺序查找 `/usr/bin/python3` → `/opt/homebrew/bin/python3` → `/usr/local/bin/python3` → `PATH` 里的 `python3`。
- **完整构建与本机隔离自检**（使用临时状态目录，**绝不触碰**你的真实 `~/Library`、`~/.claude`、`~/.codex` 或 Cockpit）：
  ```bash
  ./scripts/build_app.sh
  ./scripts/smoke_test_app.sh

  # 运行全部 Python 单元测试
  env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 \
    /usr/bin/python3 -B -m unittest discover -s tests -p 'test_*.py'
  ```

---

## 9. 完整卸载与提交 Issue 诊断清单

### 彻底清理 / 从零重置

```bash
# 1. 移除安装器写入各端配置中的受管片段
python3 scripts/install.py --uninstall --targets claude_code codex antigravity claude_desktop launchd

# 2. 清理 App、状态库、日志与临时门铃目录
rm -rf /Applications/Consilium.app \
       "$HOME/Library/Application Support/consilium" \
       "$HOME/Library/Logs/consilium" \
       "/tmp/consilium-$(id -u)"
```

### 提交 Bug Issue 前收集这些信息

```bash
# 1. 导出系统与 Bus 状态摘要
sw_vers
python3 --version
python3 -m consilium.cli status

# 2. 如需抓 daemon stderr 日志，可在停掉 App 后前台跑一次服务：
python3 -m consilium.cli serve
# （若启用过 launchd，日志位于 ~/Library/Logs/consilium/consilium.err.log）
```

> **安全提醒**：贴出日志或截图前，请务必检查并打码任何个人目录名、账号邮箱或局部路径，**绝对不要上传 `auth.token` 或三端 OAuth 凭证文件**。
