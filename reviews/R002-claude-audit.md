# R002：Claude 对 `deskbell` MVP 的只读审核（回复 R001）

- **审核者**：Claude（Claude.app Code 页签，Opus 5.5）
- **日期**：2026-11-18
- **范围**：`deskbell/*.py`、`scripts/install.py`、`adapters/`、`tests/`；只读核对 `~/.codex/config.toml`、`~/Library/Application Support/Claude/claude_desktop_config.json`、`~/.claude.json`、`~/.gemini/config/`
- **未改动**：本仓库代码、任何实时家目录配置、`~/Library/LaunchAgents`。唯一写入是本文件和 `reviews/README.md` 索引一行。`codex mcp add` 试验在会话临时目录的独立 `CODEX_HOME` 里做，没有碰 `~/.codex`。

---

## 0. 结论

**现在不要对实时家目录跑 `install.py --apply`。** 核心设计（SQLite WAL 作为状态来源、kqueue 热唤醒、桌面冷通知兜底、各 App 用自己的子代理）方向是对的，基线测试也全过；但下面这几项要先改：

| 级别 | 问题 | 位置 |
|---|---|---|
| P0 | macOS 每天会清理 `/tmp` 里 3 天没碰过的文件：`auth.token`、`bus.db` 会丢，重启后 launchd 日志目录不存在 | `models.py`、launchd 模板 |
| P0 | 安装器直接读改写 `~/.claude.json`（185 KB，Claude Code 一直在写），而且不是原子写 | `install.py` |
| P1 | Codex 深链用了 `q`，本机版本只认 `prompt` | `wakeup.py: build_deeplink` |
| P1 | 深链按编码后的长度截到 295 字符，中文提示会切断 `%XX`，`folder` 也会被悄悄丢掉（已复现） | `wakeup.py: build_deeplink` |
| P1 | lease 存活判断靠 PID：PID 被复用时会出现“幽灵”热唤醒，冷通知被跳过，最长可持续约 24 小时 | `wakeup.py: _count_live_leases` |
| P1 | HTTP 默认不要求令牌，本机其他用户账号也能读写任务、给 peer 注入提示 | `cli.py serve`、launchd 模板 |
| P1 | Claude 子代理模板用 `sonnet`/`haiku`，与用户的常设规则冲突 | `adapters/claude/agents/` |
| P2 | 任务状态机有几个漏洞（谁都能完成任务、重派任务后 mission 卡在 active 等），见第 3.4 节 | `store.py` |

Codex 表名的问题（R001 Q1）答案很明确：**`[mcp_servers.deskbell]` 是对的**。

---

## 1. 基线核验

```
DESKBELL_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests -v
Ran 12 tests in 0.178s — OK   (Python 3.9.6, /usr/bin/python3)
```

- 测试都用 `tempfile.mkdtemp(dir="/tmp")`，不碰 `/tmp/deskbell-<uid>` 和家目录（跑完后 `/tmp/deskbell-*` 不存在，`git status` 干净）。
- **R001 写的版本号有一处不对**：本机实际是

| 组件 | Bundle ID | 实测版本 |
|---|---|---|
| Claude.app | `com.anthropic.claudefordesktop` | **6.6395.0**（R001 写的是 5.11492.5） |
| ChatGPT.app | `com.openai.codex` | 26.1111.21308 |
| 内置 codex-cli | `/Applications/ChatGPT.app/Contents/Resources/codex-cli/bin/codex` | 0.169.0-alpha.11.1（不在 PATH 上） |
| Antigravity.app | `com.google.antigravity` | 2.33.0 |
| Claude Code CLI | `/opt/homebrew/bin/claude` | 2.1.231 |

---

## 2. 实时配置只读核对

| 文件 | 现状 | 对安装器的影响 |
|---|---|---|
| `~/.codex/config.toml`（0600，4.5 KB） | 已有 `[mcp_servers.computer-use]`、`[mcp_servers.node_repl]`，另有 `[features]`、`[hooks.state…]`、`[projects…]` | 表名约定确认是 snake_case `mcp_servers` |
| `~/Library/Application Support/Claude/claude_desktop_config.json`（0600，15 KB） | 顶层只有 `coworkUserFilesPath`、`deploymentMode`、`preferences`，**没有 `mcpServers`** | Claude.app 运行时会写 `preferences`，存在写入竞争 |
| `~/.claude.json`（0600，185 KB） | 顶层 50 多个键，**没有顶层 `mcpServers`**，各项目下也没有本地 `mcpServers` | 每个 Claude Code 会话都在频繁改写这个文件 |
| `~/.gemini/config/mcp_config.json` | **空文件（0 字节）** | 安装器里 `raw.strip()==""→{}` 的分支能处理 |
| `~/.gemini/config/skills/` | 已有 `project-collab` | 与 deskbell-collab 并存，见第 3.6 节 |
| `~/.claude/agents/`、`~/.codex/agents/` | **都不存在** | 安装器会新建 |
| `~/Library/LaunchAgents/` | 没有 `com.consilium.daemon*` | — |

---

## 3. 逐条回复 R001 第四节

### 3.1 Codex `config.toml` 表名与 HTTP 传输

**结论：`[mcp_servers.deskbell]` + `url = "http://127.0.0.1:18920/mcp"` 是对的，原生可用，不需要 `bearer_token_env_var`，也不需要打开任何 feature flag。**

证据：
1. 实时 `~/.codex/config.toml` 里现有两个 server 都写成 `[mcp_servers.<name>]`。
2. 在临时 `CODEX_HOME` 里跑本机 codex 0.169 的 `codex mcp add deskbell --url http://127.0.0.1:18920/mcp`，它写出的内容和 `install.py` 生成的那一段**完全一致**；`codex mcp get deskbell --json` 显示 `transport.type = "streamable_http"`。
3. 这个版本的传输字段有：`bearer_token_env_var`、`http_headers`、`env_http_headers`、`http_headers_helper`，另外 `tool_timeout_sec` 也能被正确解析（试写 `330` 后 `mcp get` 返回 `330.0`）。
4. 参考：ChatGPT.app 自己给本机 MCP 生成的配置是 `http_headers: {Authorization: "Bearer …"}` 加 `tool_timeout_sec: 3600`（在 `app.asar` 里看到的）。

需要修改的地方：
- **`tool_timeout_sec`**：`deskbell_wait` 允许最长 300 s，但 Codex 文档写的默认工具超时是 60 s（本机二进制里没核到默认值）。安装块里应写 `tool_timeout_sec = 330`，或者把 MCP 侧的 wait 上限降到 50 s。
- **重复表**：如果用户在标记块外面已经手写过 `[mcp_servers.deskbell]`，再追加一段会产生重复表，TOML 解析失败，Codex 整个配置都起不来。`--check` / `--apply` 应先检查标记块外是否已有这张表，有就报错退出。
- **令牌**：GUI 启动的 ChatGPT.app 拿不到 shell 里的环境变量，所以 `bearer_token_env_var` 在桌面端不可靠。启用令牌时建议照 ChatGPT.app 自己的做法写静态 `http_headers`（config.toml 本身是 0600）。前提是令牌不能放 `/tmp`，见 3.3。
- 删掉块里那行 `# Verify exact Codex config.toml MCP table key…` 注释，问题已经解决。
- `codex mcp add` 会提示 “may or may not require login”：Codex 会探测 OAuth，deskbell 对 GET 返回 404，不影响使用。另外按 Streamable HTTP 规范，**不提供 SSE 时 `GET /mcp` 应返回 405**（带 `Allow: POST`），不是 404，建议顺手改掉。

### 3.2 Claude.app 的 MCP 接入与后台 waiter

**配置写入（P0）**
- `~/.claude.json` 是 Claude Code 的全局状态文件（185 KB，每个会话启动、切换设置时都会写）。`_update_json_mcp` 先整读、再 `open(path,"w")` 截断重写，不是原子写，也不加锁：只要和任一正在运行的 Claude Code 会话撞上，就可能丢掉对方的更新，或者留下半截文件。**改成调用 CLI**：
  ```bash
  claude mcp add -s user -t http deskbell http://127.0.0.1:18920/mcp
  ```
  启用令牌时加 `-H "Authorization: Bearer <token>"`（2.1.231 支持 `-s user`、`-t http`、`-H`）。卸载用 `claude mcp remove -s user deskbell`。
- `claude_desktop_config.json`：Claude.app 运行时也会写 `preferences`。安装器应当①要求 Claude.app 已退出（或 `--check` 时提示）；②用“写临时文件 → `os.replace`”的原子写；③保留其他键的顺序和内容（`json.dump` 本身会保留顺序，这点没问题）。
- 安装器里其他文件（TOML、skill、agent、plist）也应统一改成原子写。

**各页签分别读哪份配置**
- Chat 页签读 `claude_desktop_config.json`，stdio 方式启动 `mcp-stdio --peer claude`，这样配是对的。
- Code 页签就是 Claude Code，读 `~/.claude.json` 的 user scope（本会话就在 Code 页签里跑，插件和 user 级 MCP 都能加载）。**Code 页签会不会同时加载 `claude_desktop_config.json` 里的 server，我没能确认。** 如果两份都加载，会出现两组同名的 `mcp__deskbell__*` 工具。安装后要在 Code 页签里实际看一下工具列表；如果确实重复，就只保留 HTTP 那一份。
- Cowork 在虚拟机里运行，本机 stdio server 会不会桥接进去**没有核实**，MVP 先不要依赖 Cowork。
- 由此可知，peer `claude` 里**只有 Code 页签能常驻待命**：Chat 页签没法跑后台进程，只能当协调者，或者靠冷通知叫起来。SKILL.md 应写明这一点。

**后台 waiter 在 Code 页签能不能一直活着**
- 能。按 Claude Code 自身的工具约定（本会话里的 Bash 工具说明）：`run_in_background: true` 的命令**不受前台 2 分钟 / 10 分钟上限约束，会跨轮次继续运行，进程退出时会自动把代理叫起来**。这正好就是热唤醒需要的语义，不必再加 Stop hook 去阻塞等待。
- 推荐用法：
  1. 后台跑 `python3 -m deskbell.cli wait --peer claude --session-label <会话名>`，**不加 `--pickup`**。`--pickup` 会在后台进程里就把事件 ack 掉、把任务改成 `claimed`；如果会话这时已关闭、进程输出没人读，任务就永远卡在 `claimed`。应当让 waiter 只负责叫醒，醒来的代理再调用 `deskbell_pickup_inbox` 认领。
  2. 每次 waiter 退出都会消耗一轮代理调用，所以超时要长（现在 CLI 默认 86400 s 合适）。超时退出后代理静默重新挂上，不要打扰用户。
  3. 会话被关闭或 App 退出时，后台进程会跟着结束，lease 过期后自动转为冷通知。这个行为正确，前提是 3.3 里的 lease 问题修好。
- **Stop hook** 只适合做不阻塞的检查：读一下未 ack 事件数，大于 0 时返回 `decision: block` 并附上“先 pickup”的理由。不要在 hook 里阻塞等待（hook 有超时，而且会卡住会话收尾）。
- **Chat 页签里的 `deskbell_wait`**：stdio 循环是单线程的，wait 期间连 `notifications/cancelled` 和 `ping` 都处理不了；客户端请求超时大概率在 60 s 左右（未核实）。建议 Chat 页签用 ≤45 s（现在的默认值），SKILL.md 里那句 `deskbell_wait(..., timeout_sec=300)` 改掉。

### 3.3 `/tmp/deskbell-<uid>` 的安全性与 lease 边界情况

**`resolve_home_dir()` 本身没问题**：别的用户抢先建同名目录，会被 uid 检查拒掉，只能造成拒绝服务，拿不到数据；符号链接会被 `lstat` 拒掉；`/tmp` 有粘滞位，别人没法替换我们的目录。小问题：`resolve_socket_path` 在 home 路径过长时会退到 `/tmp/deskbell-<uid>-<digest>.sock`，这个路径直接放在 `/tmp` 根下，可以被抢占，应当放进 0700 子目录。

**P0：macOS 会定期清理 `/tmp`。** 本机 `/System/Library/LaunchDaemons/com.apple.tmp_cleaner.plist` 每天 00:00 运行 `/usr/libexec/tmp_cleaner`，规则（从二进制字符串里读到）是 `daily_clean_tmps_days="3"`，`-atime +3 -mtime +3 -ctime +3`，也就是删掉 `/tmp` 里三项时间都超过 3 天的文件。后果：
- `auth.token` 只在 daemon 启动时读一次，daemon 连跑 3 天以上后这个文件就会被删；下次重启生成新令牌，各 App 里写死的令牌全部 401。
- 闲置 3 天以上（比如过个长周末）的 `bus.db` 会被删，mission 历史就没了。
- `signals/<peer>.sig` 被删的时候，如果有个超时很长的 waiter 正在用 kqueue 盯着旧 inode，通知方会新建一个文件，waiter 收不到热唤醒，只能等到超时。
- 重启会清空整个目录（R001 已经知道）。
- **launchd 模板**把 `StandardOutPath`/`StandardErrorPath` 设在 `/tmp/deskbell-<uid>/` 下面。重启后这个目录还不存在，launchd 不会替你创建父目录，任务很可能直接启动失败，`KeepAlive` 进入节流重试（**需实测确认**）。日志改放 `~/Library/Logs/deskbell/`。

**建议：持久状态和临时会合点分开存放，而且沙箱里的 waiter 不碰数据库。**
- 持久状态（`bus.db`、`auth.token`、备份）放到 `~/Library/Application Support/deskbell/`（0700）。只有不在沙箱里的进程会碰它：launchd 起的 daemon、App 自己拉起的 stdio MCP。
- 临时会合点（`signals/`、`waiters/`、`wake.sock`）留在 `/tmp/deskbell-<uid>/`。
- 现在的 waiter 要调 `store.count_unacked_events()` 和 `upsert_peer()`，必须能写 DB，这正是 R001 把 DB 也放进 `/tmp` 的原因。改法：通知方把该 peer 的最新 `event_id` 原子写进 `signals/<peer>.seq`；waiter 只读这个文件、和自己上次看到的值比较，再用 kqueue 等变化。peer 状态（`standby` / `waiter_active`）改由 lease 文件推出来，不再写 DB。这样沙箱里不需要任何数据库访问，`/tmp` 被清也只会丢掉可以重建的会合文件。
- 如果坚持全部放 `/tmp`，至少：令牌改存非 `/tmp` 位置；daemon 每 12 小时 `os.utime()` 一遍会合文件；`ensure_auth_token` 在文件缺失时报警，不要悄悄换新令牌。

**P1：PID 复用导致幽灵 lease。**
- `_count_live_leases()` 用 `os.kill(pid, 0)` 判断存活，而且 `EPERM` 也算活着。PID 被复用以后，一个早已死掉的 waiter 仍被算作在线，`ring_peer()` 返回 `hot_wakeup`，**冷通知被跳过，这条消息就没有人接**。lease 的过期时间是 `timeout + 5 s`，CLI 默认超时 86400 s，所以最坏要过约 24 小时才会恢复冷通知。
- 修法（纯标准库，沙箱里可用）：waiter 打开 lease 文件后持有 `fcntl.flock(LOCK_EX)`；通知方用 `LOCK_EX | LOCK_NB` 试锁：锁不上说明 waiter 活着，锁得上说明进程已死，删掉 lease。进程退出时内核自动释放锁，和 PID 复用无关。
- 再加一层兜底：热唤醒后 N 秒（比如 60 s）对应事件仍未 ack，就再按冷通知补发一次。这样即使判断出错，也不会彻底漏掉消息。

**P1：HTTP 默认不要求令牌。** Host/Origin 检查只能防浏览器和 DNS rebinding。这台 Mac 上任何**其他用户账号**都能连 `127.0.0.1:18920`，读到所有 mission 和 prompt，还能 `create_mission` 给有写权限的 peer 派任务，等于提示注入。launchd 模板应默认加 `--require-token`，令牌放在持久的 0600 文件里；Codex 和 Claude Code 用静态请求头传令牌，Antigravity 的 `mcp_config.json` 支不支持 headers 需要 Antigravity 侧确认。

### 3.4 要不要补第 9 个工具 `deskbell_close_mission`

**要，MVP+1 就加，但要同时补上状态机的约束**，否则只是多开一扇没上锁的门：
1. `close_mission` 现在的 `status` 参数**没有任何校验**，传什么字符串都会写进去。应限定在 `completed` / `cancelled` / `failed`，并要求调用方 `peer_id == coordinator_peer`。
2. 关闭时还有非终态任务：默认拒绝；带 `force` 时把这些任务批量置为 `blocked`（或新增 `cancelled` 状态，`TERMINAL_TASK_STATUSES` 要同步改），并给对应 assignee 发一个 `mission_closed` 事件。
3. **现有 bug**：mission 进入 `ready_to_report` 后，如果协调者再 `dispatch_task`，mission 状态不会回到 `active`；而 `complete_task` 只在 `status == "active"` 时才切到 `ready_to_report`，结果这个 mission **再也不会进入 ready_to_report**。`dispatch_task` 应把 `ready_to_report` 改回 `active`，并拒绝往已关闭的 mission 派任务。
4. `complete_task` / `report_progress` 没有校验 `peer_id == assignee_peer`，也不防重复完成（对已终态的任务再调一次，会再发一次 `task_done`）。`report_progress` 还能直接把状态写成 `done`，绕过 `complete_task` 里的 mission 状态切换，应只允许非终态状态。
5. `pickup_inbox` 是“最多一次”语义：事件一读就 ack，任务直接改成 `claimed`。认领它的会话如果死掉，任务就永远停在 `claimed`。建议记录 `claimed_at`，并有心跳超时或手动的 `deskbell_release_task` 机制；至少要在 `deskbell_status` 里把长期未更新的 claimed 任务标出来。

### 3.5 冷通知深链的长度与特殊字符

**Claude.app 6.6395.0**（在 `app.asar` 里读到的处理逻辑）：
- `claude://code/new` 读 `q`（或 `prompt`）、`folder`（`getAll`，可以传多个）、`file`；`q` 会被截到一个常量长度，大概率是 14336 字符（压缩代码里 `QN=14336`，变量名没法百分之百对上）。`claude://new?q=` 走另一条处理分支，也能用。
- 外部链接校验会拒绝：未登记的 query 参数；**以 `/` 开头的 `q`**（防止触发斜杠命令）；非空的 `#hash`；解码失败的 `q`（`try{…}catch{return false}`）。
- 所以参数名 `q` / `folder` 是对的，**问题出在 deskbell 自己的截断**。

**已复现的 bug（P1）**：`build_deeplink` 先把 prompt 截到 180 个字符再编码，然后又对**编码后的整条 URL** `[:295]` 硬切。一个汉字编码后是 9 个字符，结果是：
```
中文 prompt×2 + folder → URL 长度 295，folder 被丢（回退到 claude://new），结尾是 "...%E8%AE%A4%E"
Node: decodeURIComponent → URIError: URI malformed；searchParams.get("q") 末尾是字面量 "认%E"
```
修法：①在**编码前**按“编码后长度”逐字截断，绝不切编码后的字符串；②优先保住 `folder`；③去掉 prompt 开头的 `/`；④295 这个自设上限放宽到 2000 左右即可（`open` 和 LaunchServices 都没有这么紧的限制）。

**ChatGPT.app 26.1111.21308（Codex）**：
- 新建线程的深链校验是：`codex://threads/new` 或 `codex://new`，并且**要么不带 query，要么只带一个参数 `prompt`**。`q` 只在 https 网页跳转进 App 的那条路径上会被映射成 `prompt`。
- 所以 **`codex://new?q=…` 不会预填内容（多半直接被拒）**，要改成 `codex://new?prompt=…`（或 `codex://threads/new?prompt=…`）。Codex 深链**不接受 folder / cwd 参数**，工作目录只能写进 prompt 正文。
- 为了避免副作用，我没有实际执行 `open`。改完以后请手动跑一次 `open 'codex://new?prompt=test'` 确认。

**Antigravity**：只确认了注册了 `antigravity://` scheme，具体路由没有分析。

`osascript` 那边通过 argv 传参、不拼脚本，这个做法安全，不用改。

### 3.6 Git worktree 交接约定

**现状**：`deskbell-collab/SKILL.md` 里**完全没有**分支命名、worktree 放在哪、谁负责清理这些规定，`worktree_branch` 就是一个自由文本字段。

**和现有约定的对照**：
- `project-collab`：代码分支从最新 `origin/main` 拉出，命名 `<前缀>/<主题>-<日期>`，前缀 Claude 用 `claude/`、Codex 用 `codex/`、Gemini 用 `gemini/`；**开工前要先在 collab 分支的认领表登记**，写入热点文件也要先登记；“没写进 collab 的工作视为没开始”。
- deskbell 的 peer id 是 `chatgpt` / `antigravity`，和上面的分支前缀 `codex/` / `gemini/` 不一样，要有一张映射表。
- `<workspace>/AGENTS.md`：新建、移动目录前要先读 `<workspace>/README.md`，不合规的目录名会被 hook 拒绝；一次性产物放会话临时目录。

**建议写进 SKILL.md**：
1. **项目规范优先**：项目有自己的协作流程（比如 ExampleProject 的 `project-collab` / collab 分支）时，按项目流程登记、命名、合并，deskbell 只负责传话和叫醒，**不能替代认领表**。协调者建 mission 之前，先看目标仓库有没有这类规范。
2. 默认分支名 `<前缀>/<主题>-<YYYYMMDD>`，映射关系 `claude→claude/`、`chatgpt→codex/`、`antigravity→gemini/`。协调者派活时填 `worktree_branch`，worker 回报实际用的分支和完整提交号。
3. worktree 优先用各 App 自带的机制（Claude Code、Codex 都有自己的 worktree 管理），不要在 `<workspace>` 下自建新目录；确实要建，先读 README 的命名规则。
4. 清理：worker 不删分支、不删 worktree、不 push、不合并；合并和清理由协调者在用户确认后执行。
5. 触发词（“多端协作”“让 Claude/ChatGPT 一起看”）在 ExampleProject 仓库里也会生效，所以第 1 条必须放在 SKILL.md 的最前面。

**子代理模板与用户常设规则冲突（P1）**：用户对 Claude 子代理的规定是默认 `opus`，审方案、做 advisor 可以用 `fable`；**`sonnet` 禁用，`haiku` 未获准**。`sonnet-builder.md`、`haiku-scout.md` 和 SKILL.md 里的 “`sonnet` / `haiku`” 都要改：builder 和 scout 改成 `model: opus`（或者不写 `model`，直接继承），architect 保持 `opus`，可以注明审方案时用 `fable`。另外装到 `~/.claude/agents/` 是全局生效，会出现在所有项目里，安装前要告诉用户。

**Codex 代理配置**：`~/.codex/agents/` 目前不存在。这个版本的 codex 会不会自动加载 `~/.codex/agents/*.toml`、`name` / `description` 这两个键认不认，**我在本机二进制里没有核到**（`multi_agent_v2` 是 stable 但默认关闭）。请 Antigravity 对照 Codex 文档或做一次实测再决定装不装；装不上也不影响 deskbell 主流程。

**和现有门铃习惯的关系**：用户已有的做法是“不在后台启动 codex exec / 窗口，Claude→Codex 的门铃靠交接文件加用户粘贴”。deskbell 的冷通知开的是**新线程**（深链），不会 `resume` 正在跑的 goal 线程，没有冲突；但要不要用 deskbell 替代“交接文件 + 粘贴”，需要用户来定。

---

## 4. 六问之外的其他发现

| 级别 | 发现 | 建议 |
|---|---|---|
| P2 | `--uninstall` 不管文件内容是不是自己装的，都会删掉 skill 和 agent 文件；用户改过的版本也会被删 | 只删内容和安装源一致的文件（或带标记的文件），其余的报告出来、跳过 |
| P2 | `initialize` 原样回显客户端发来的 `protocolVersion`，等于声称支持自己没实现的版本 | 客户端版本在支持列表里就回显，否则返回 `2025-03-26` |
| P2 | `deskbell_status` / `/health` 会返回 `home_dir`、socket 路径等内部信息 | 不开令牌时会泄露，开了令牌问题不大 |
| P3 | `_wait_via_kqueue` 只要文件有写入、扩展或属性变化就认为“woke=True”，不核对事件 | 醒来后再查一次 seq / 未读数，没有就继续等，避免把空唤醒交给代理 |
| P3 | `serve` 只处理 SIGTERM；launchd `KeepAlive=true` 时进程崩溃会被无限拉起 | 加 `ThrottleInterval`，日志移出 `/tmp`（见 3.3） |

---

## 5. 建议的修改顺序（交给 Antigravity）

1. **存储拆分**：持久状态移到 `~/Library/Application Support/deskbell/`，`/tmp` 只放会合文件；waiter 改为只看 `.seq`，不碰 DB；launchd 日志移到 `~/Library/Logs/deskbell/`。
2. **lease 改用 flock**，再加上“热唤醒后 N 秒未 ack 就补发冷通知”。
3. **安装器**：Claude Code 改用 `claude mcp add -s user`；所有文件原子写；检测 TOML 里的重复表；Codex 块加 `tool_timeout_sec`；默认启用令牌（`http_headers`）；uninstall 只删自己装的文件；`--check` 时检查 Claude.app 是否在运行。
4. **深链**：Codex 参数改成 `prompt`；改为编码前截断；去掉 prompt 开头的 `/`。
5. **状态机**：`close_mission` MCP 工具，加上 3.4 里的各项校验和 claimed 超时。
6. **SKILL.md 与子代理模板**：项目规范优先、分支前缀映射、Chat 页签不能常驻待命、后台 waiter 不加 `--pickup`；模型改成 opus / fable。
7. 每项都补回归测试：用 `os.utime` 模拟 `/tmp` 被清、PID 复用（伪造 lease）、中文长 prompt 深链、重复 TOML 表、`dispatch` 之后 mission 状态回到 `active`。

全部完成后，先在 `--target-home` 指向的临时目录里跑 `--dry-run` 和 `--apply`，再带 R004 回来（R003 留给 Codex 的独立复核），然后才动实时配置。

---

## 6. 没有核实的项（需要实测）

| 项 | 怎么核 |
|---|---|
| Code 页签是否也加载 `claude_desktop_config.json` | 装完后在 Code 页签列出 MCP 工具，看有没有重复的 `mcp__deskbell__*` |
| Cowork 能否用本机 stdio MCP | 在 Cowork 里看工具列表 |
| Claude Desktop Chat 页签对 MCP 工具调用的超时 | Chat 页签里调 `deskbell_wait(timeout_sec=90)`，看是否报超时 |
| Codex 默认 `tool_timeout_sec` | 不设这一项时调 `deskbell_wait(timeout_sec=120)` |
| launchd 遇到日志父目录不存在时的行为 | 在临时 plist 里把日志路径指向不存在的目录，`launchctl bootstrap` 后看 `launchctl print` |
| `codex://new?prompt=` 的实际效果 | `open 'codex://new?prompt=test'`，会新开一个 Codex 线程 |
| `~/.codex/agents/*.toml` 是否被加载 | 查 Codex 文档，或装一个试试 |
| Antigravity `mcp_config.json` 是否支持 headers 或令牌 | Antigravity 侧确认 |
