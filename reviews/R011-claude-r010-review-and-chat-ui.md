# R011：Claude 复核 R010（关口 1）与新前端方向意见（关口 2）

- **作者**：Claude（只读审核）
- **日期**：2026-11-19
- **审核对象**：`ce2c882`、`9ced8af`、`10939b2`、`de2883b`（作者均为 `2084413277`，未推送）；[R010](./R010-antigravity-r009-fixes.md)；Antigravity 的独立原型 `~/.gemini/antigravity/brain/c6d5fd4a-…/consilium_chat_ui_prototype.html`
- **未做的事**：没有改实时家目录配置，没有推送，没有改仓库代码。探针都在会话临时目录里起的隔离 daemon（端口 18977）上跑，跑完已停掉。

---

## 0. 用户原话与本文的关系

**用户原话（2026-11-19）**：

- “请你帮我做两件事（注意：不改实时家目录配置、不 push）”
- 关口 1：“给出关口 1 是否通过的结论”
- 关口 2：“我对 R010 现有截图里的‘四列看板 + 运维后台感’不满意，决定改成**极简对话流 + 顶部单行紧凑三端胶囊**（类似 Claude/ChatGPT 桌面端），并把我常用的功能收进右侧默认折叠的 5 标签面板和 `/` 指令里。”
- 关口 2 五个问题：分期切分、文件目录/Worktree/Diff 的安全边界、子 Agent 状态来源、多账号池与 Cockpit 只读联动、分端模型定义与自动分配。
- 此前的决定仍有效：群聊参与者“只要原生 App”。

**除此之外都是 Claude 的建议**。新前端的方向（对话流、胶囊、折叠面板、`/` 指令）由用户决定，本文不讨论要不要这样做，只讨论怎么分期、怎么做才安全。

---

## 1. 结论

| 关口 | Claude 结论 | 说明 |
|---|---|---|
| ★ 关口 1（R010 整改） | **通过** | R008 和 R009 的 1–15 项都落实了，没有 P1 或 P2 遗留。另有 4 个 P3（§3），可以并进下一轮，不挡关口。关口 1 还需要 Codex 复核（建议写 R012）。 |
| ★ 关口 2（外观） | **旧看板不再作为验收对象**；新方向可行 | 用户已否定四列看板，关口 2 改为验收新的对话流 `/ui`。§4 给出现在就做的范围（只读改版），以及原型里要改的 6 处问题。 |

---

## 2. 我这边的核验

### 2.1 测试与校验

| 项目 | 结果 |
|---|---|
| `env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 /usr/bin/python3 -B -W default -m unittest discover -s tests` | 59 项，OK（10.2 s） |
| 同上，但不带 `CONSILIUM_DRY_RUN`、`CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC` | 59 项，OK。测试自己设好了隔离变量，不依赖调用方 |
| `openspec validate consilium-v1 --strict` | `Change 'consilium-v1' is valid` |
| `tasks.md` 关口勾选 | Gate 1、Gate 2 仍是 `- [ ]` |

### 2.2 实时配置洁净度

| 位置 | 结果 |
|---|---|
| `~/.claude.json`、`~/.codex/config.toml`、`claude_desktop_config.json` | 都搜不到 `consilium` 或 `deskbell` |
| `~/.gemini/config/mcp_config.json` | 仍为 0 字节 |
| `~/Library/LaunchAgents/` | 没有 consilium 的 plist |
| `~/Library/Application Support/consilium`、`~/Library/Logs/consilium` | 不存在 |
| `~/.claude/agents`、`~/.codex/agents` | 不存在 |
| `/tmp/consilium-501/` | 有一个空的 `sock/` 子目录（0700），17:13 创建，见 P3-3 |
| 仓库内 `.agents/` | 是 `openspec init` 生成的既有目录，已纳入 git，不是本轮遗留 |

### 2.3 在隔离 daemon 上的实测

| # | 请求 | 期望 | 实测 |
|---|---|---|---|
| 1 | `GET /api/state?token=<令牌>` | 401 | 401 |
| 2 | Bearer 访问 `/api/state` | 200 | 200 |
| 3 | 带 cookie 写，`Origin: http://127.0.0.1:5173`（同主机、不同端口） | 403 | 403 |
| 4 | 带 cookie 写，没有 Origin | 403 | 403 |
| 5 | 带 cookie 写，Origin 正确，缺 `X-Consilium-CSRF` | 403 | 403 |
| 6 | 带 cookie 写，`Content-Type: text/plain` | 415 | 415 |
| 7 | 三项都正确，任务不存在 | 404 | 404 |
| 8 | `Origin: http://127.0.0.1:abc` | 4xx | **连接被掐断（000）**，日志里有 `ValueError` 堆栈（P3-1） |
| 9 | `/ui` 响应头 | 有 nosniff、no-referrer、`frame-ancestors 'none'` | 都有 |
| 10 | 一次性票据兑换两次 | 第一次发 cookie，第二次不发 | 1 / 0 |
| 11 | SSE 打开后在磁盘上换令牌 | 流被关闭，旧 cookie 和旧令牌都失效 | 换令牌后约 **20 ms** SSE 退出；旧 cookie 401、旧令牌 401 |
| 12 | 同一条 keep-alive 连接：先发一个会被拒的带 body 的 POST，再发 `GET /health` | 第二个请求正常 | **第二个请求变成 `501 Unsupported method ('{"reason":"x"}GET')`**（P3-2） |

---

## 3. 1–15 项逐项核对

| # | 要求 | 结论 | 依据 |
|---|---|---|---|
| 1 | `attempt_id` 防旧认领越权；回收时唤醒 assignee | ✓ | `store.py` `pickup_inbox` 生成 `A-<seq>-<hex>`；`report_progress`/`complete_task` 校验，`UPDATE … WHERE attempt_id = ?` 双保险；`release_stale_claims` 把 assignee 放进 `pulse_peers` |
| 2 | 写库成功即 `committed=true`，后续故障进 `delivery.warnings`；幂等键绑定请求哈希和调用方 | ✓ | `_safe_sync_peers_seq` 在事务外执行；`_collect_post_commit_warnings`；`_check_idempotency_locked` 依次比对 operation、caller_scope、request_hash，不一致抛 `ConflictError`，HTTP 返回 409 |
| 3 | cookie 写请求：Origin 精确匹配、JSON、CSRF 头 | ✓ | `_is_exact_daemon_origin`、`_check_cookie_write_csrf`；实测 3–7 |
| 4 | 等待只读 | ✓ | MCP `consilium_wait` 忽略 `auto_pickup`，返回 `auto_pickup_ignored` |
| 5 | claimed 超时自动退回，running 超时只标 stale；SKILL 写明进度间隔不超过超时一半 | ✓ | `release_stale_claims` 两个分支；`report_progress` 会清掉 `stale_at`；SKILL.md 第 54 行；spec 已同步 |
| 6 | 删掉 `?token=` | ✓ | 实测 1 |
| 7 | 随机会话、令牌版本、轮换时吊销 | ✓ | 实测 10、11 |
| 8 | `frame-ancestors`、`nosniff` | ✓ | 实测 9；JSON 和 SSE 响应也带 nosniff |
| 9 | SSE 单调版本、外部变动检测、16 上限、写超时 | ✓ | `_handle_sse_stream` 用 `seq <= last_sent_ver` 丢旧帧；空闲时比对 `compute_state_fingerprint`；`settimeout(10.0)` |
| 10 | 锁文件在 `signals/` 之外；lease 原子创建 | ✓ | `_seq_lock_path` 放在会合目录根下；`.tmp-*` + `O_EXCL` + `flock` + `os.replace` |
| 11 | 按目标安装、恢复后核实、rotate-token 部分失败报告 | ✓（只看了测试） | 三个 `test_r010_13_*`；没有对真实家目录做任何安装演练，这本来就要等阶段 4 用户同意 |
| 12 | 重新入队范围与尝试历史 | ✓ | `REQUEUEABLE_TASK_STATUSES`；`attempt_history_json` 存了汇总、产物、进度 |
| 13 | 认领耗时为主指标、冷通知三件事、`consilium ui` 报错 | ✓ | `claim_latency_ms` 用派发时间和认领时间相减；`ring_cold` 分开记录三件事 |
| 14 | 状态灯、看板、时间线、录屏 | ✓ | 五态灯、空列折叠、时间线渐隐。按 §1，旧看板不再作为验收对象 |
| 15 | 进度色、副标题、行内代码、演示数据标签 | ✓ | 同上 |

### P3（不挡关口，建议下一轮顺手修）

- **P3-1 Origin 端口不是数字时处理线程抛异常**。`_is_exact_daemon_origin` 里的 `parsed.port` 会抛 `ValueError`，连接直接断开，日志留下堆栈。结果是拒绝，没有放行，但应该捕获后返回 403。R010 §5 自己也提到了这一点。
- **P3-2 提前拒绝时没读请求体，污染 keep-alive 连接**。401、403、415 都在 `_read_json_body` 之前返回，剩下的 body 被当成下一个请求的请求行（实测 12）。目前每个路由都要凭据，我没找到借此越权的路径，但这正是请求走私的形状。修法：任何提前拒绝都设 `self.close_connection = True`，或者先按 `Content-Length` 把 body 读掉（上限 2 MB）。
- **P3-3 测试在真实 `/tmp/consilium-501/` 下留了 `sock/` 目录**。来自长路径套接字回退那条路径，内容为空、权限 0700，没有危害，但不算完全隔离。可以让这个回退路径也跟着 `CONSILIUM_RENDEZVOUS_DIR` 走，或者在测试里指到临时目录。
- **P3-4 `localhost` 与 `127.0.0.1` 是两个来源**。daemon 绑定在 `127.0.0.1`，用户如果手动打开 `http://localhost:18920/ui`，写操作会 403（cookie 也不会共享）。`consilium ui` 一直用 `127.0.0.1`，所以只需在 README 和登录页写一句；以后做菜单栏 App 时固定用 `127.0.0.1`。

`list_quota_windows()` 把多个账号混在一起算（用户在 `store.py` 第 2245 行附近发现的问题），我确认属实，但这不是 R010 引入的回归，放到 §5 Q4 讨论。

---

## 4. 关口 2：对原型的体验意见

我在内置浏览器里以 1440×900 打开原型，逐个试了右侧 5 个标签、`/` 菜单和左侧栏。

**方向是对的**。对话流加顶部胶囊，比四列看板更像用户每天在用的 Claude 和 ChatGPT 桌面端，主屏信息密度也合适。右侧面板默认折叠、点开再看，这个取舍好。每条回复下面挂“热唤醒 6.4 ms · 认领 318 ms”这种小徽章，把 R010 做的指标用上了，又不喧宾夺主。

**落到 `/ui` 之前必须改的 6 处**：

1. **外部脚本**：原型从 `www.gstatic.com` 加载 Tailwind。`/ui` 的约定是离线、零 CDN，CSP 也是 `default-src 'none'`，照搬会整页无样式。要改成预编译、内联的 CSS（只保留用到的类），或者手写 CSS 变量，沿用现有 `dashboard.py` 的主题变量。
2. **`/` 菜单背景透明**：弹出后和底下的消息文字叠在一起，基本没法读；按 Esc 也关不掉。
3. **没有深色模式**：现有看板有浅色、深色、跟随系统三档，新版不能退步。
4. **模型名是写死的，而且过时**（`Claude Opus 4.1`、`Claude Sonnet 4`、`GPT-5.1 Codex High`……）。正式版不能写死，见 Q5。
5. **深链要先核实**：原型里“打开原生对话”用的是 `codex://threads/<id>`。R003 只核实过 `codex://new?prompt=` 和 `codex://threads/new?prompt=`，跳到已有线程这条没验证过；Claude 这边也只核实过“新建会话”的 `claude://code/new`。在验证之前，按钮应写成“在 Claude / Codex 新开会话接手”，不要写“打开该对话”。
6. **措辞不要暗示正在实时对话**。参与者都是原生 App，消息流里显示的其实是事件：派发、进度、交付、复核，不是逐字流式回复。原型里的“执行输出中”“实时同步”在真实数据下只能做到“最后一次上报于 X 分钟前”。建议照实标注，避免用户以为能在这里看到对方逐字输出。

---

## 5. 关口 2：五个问题的意见

### Q1 分期切分

总原则：**关口 2 只做“换皮”，不加新的写能力，也不读新的数据源**。现有的 `/api/state` 和 SSE 已经包含 mission、tasks、events、peers、quota，足够把对话流画出来。

| 能力 | 放在哪 | 理由 |
|---|---|---|
| 对话流：mission 对应会话，事件渲染成消息（`task_assigned` 显示为协调者发言，`progress` 和 `task_done` 显示为对应端的回复，`review_request` 显示为复核轮次，`mission_closed` 附 `final_summary`） | **现在（2.6，过关口 2）** | 数据都已存在，纯前端改版 |
| 顶部三端胶囊：五态灯，加当前账号的配额 | **现在** | 配额要等 Q4 的修复先合入，否则会显示错的“耗尽” |
| 右侧「任务」：任务列表，加“重新入队”“关闭会诊” | **现在** | 复用 R010 已加固的两个写接口，不新增 |
| 右侧「工作树」：只列各任务的 `worktree_branch` | **现在（只读列表）** | 字段已有；真正读 git 的部分放后面 |
| 右侧「模型与账号」：只读展示配额和账号池 | **现在**，但要等 Q4 修复 | 不放可调滑块，见 Q5 |
| 右侧「子 Agent」 | **现在只放占位**（“该端未上报”），字段在阶段 3 加 | 见 Q3 |
| 右侧「文件」：目录树、Diff、源码预览 | **阶段 5** | 这是新的读文件接口，安全面明显变大，见 Q2；现在最多列出 `artifacts` 里的路径字符串 |
| 左侧「会诊列表」 | **现在** | 就是 mission 列表 |
| 左侧「原生 App 只读镜像」（读 Claude Code、Codex 本地 JSONL） | **阶段 6** | 原路线图如此；而且 Codex rollout 里有明文测试账号，读内容要用户单独同意 |
| `/` 指令 | **现在只放纯前端动作**：`/mission`、`/files`、`/export`（用已有 `final_summary` 在浏览器里拼 Markdown）、打开面板 | 不碰后端写入 |
| 输入框“发送”：从 `/ui` 新建会诊、派活 | **阶段 3 之后**（建议 3.5） | 这是第三个 cookie 写接口，而且写进去之后要靠真 App 的唤醒链路才有意义。等 B 级真 App 试跑证明唤醒可靠再开。关口 2 期间输入框可以先做成“生成交接提示词 → 复制 / 新开原生会话” |
| `✦ 自动分配` | **阶段 5**（原路线图的“配额感知派活”） | 见 Q5 |
| 菜单栏 App | **阶段 4.5**（R008 路线不变） | |

这样关口 2 的验收对象很清楚：新外观加现有功能，后端只改配额聚合（Q4），其余不动。

### Q2 文件 / Worktree / Diff 面板的安全边界

建议在阶段 5 按下面的规则做，并在 OpenSpec 里单独写一份 spec：

1. **只允许已登记的根目录**。接口不接受任意路径，只接受 `(root_id, 相对路径)`。`root_id` 来自 mission 登记的仓库根，或者 `routing.json` 里用户写的允许列表。
2. **解析与越界检查**：拒绝绝对路径、`..` 和 NUL；`realpath` 之后用 `os.path.commonpath` 确认仍在 `realpath(root)` 之内（这样也能挡住指向外部的符号链接）；`lstat` 后只接受普通文件，FIFO、设备、套接字一律拒绝（打开 FIFO 会让处理线程卡死）。
3. **列目录用 git，不自己遍历**：`git ls-files -co --exclude-standard`，自动遵守 `.gitignore`，`.env` 一般就不会出现。
4. **再加一层黑名单**，git 跟踪了也不显示内容，只显示“已隐藏”：`.env*`、`*.pem`、`*.key`、`*.p12`、`id_rsa*`、`.netrc`、`*credentials*`、`*secret*`、`auth.token`、`*.sqlite`、`*.db`、`.git/` 内部。Diff 输出再过一遍简单的正则遮盖（PEM 头、`sk-`、`ghp_`、`AKIA` 这类前缀）。
5. **大小限制**：预览不超过 512 KB；Diff 不超过 1 MB 或 2000 行，超出截断并提示；前 8 KB 里有 NUL 就判为二进制，不预览；目录树最多 5000 项。
6. **调用 git 时防止执行仓库里的命令**。这是最容易漏的一条：仓库自己的 `.git/config` 可以配置 `core.fsmonitor`、`diff.external`、textconv 过滤器和 pager，跑 `git status`、`git diff` 时会执行这些命令。固定写成：参数列表、不经 shell；加 `-c core.fsmonitor=false -c core.hooksPath=/dev/null`、`--no-ext-diff --no-textconv`；设置 `GIT_PAGER=cat`、`GIT_OPTIONAL_LOCKS=0`；超时 5 s。worktree 列表用 `git worktree list --porcelain`。
7. **只通过 JSON 返回**：内容装在 JSON 里，前端用 `textContent` 渲染，不提供直接返回文件字节的 `/raw` 链接（否则 HTML 或 SVG 文件可能被浏览器当页面执行）。全部是 GET，鉴权与 `/api/state` 相同。
8. **“引用到对话”只插入路径和行号，不插入内容**，由收到任务的原生 App 自己去读。

### Q3 子 Agent 状态来源

**建议先用 `report_progress` 上报结构化字段，不监听原生日志。**

- 字段建议：`subagents: [{name, model, status, started_at, summary}]`，可选；最多 16 项，每个字符串限制长度；存进任务行，SSE 随状态一起下发。
- 理由：
  1. 三端统一。三家的日志格式不同（Claude Code 是 JSONL，Codex 是 rollout JSONL，Antigravity 是每个对话一个 SQLite），都没有公开文档，升级就可能变。
  2. 隐私。日志里是完整对话，我们已经知道 Codex rollout 里有明文测试账号。只为了显示几个子 Agent 的状态就去读全部对话内容，不划算。
  3. 和现有协议一致：子 Agent 在原生 App 里运行（SKILL.md 本来就让各端在自己的环境里分发），由父会话汇报给总线，正好对应“协调者只认 attempt_id 的写入”。
- 代价：数据只和心跳一样新，而且是各端自报的。界面上标“自报 · X 分钟前”。
- 阶段 6 如果要做日志镜像，也只解析元数据（事件类型、工具名、时间），不读消息正文，并且先征得用户同意。

### Q4 多账号池与 Cockpit 只读联动

方案方向对：**主配额只看当前激活的账号，账号池只用来提示，绝不碰密文库**。我只读核对了 Cockpit 的文件结构（只看文件名和 JSON 键名，没看值），有以下几个坑：

1. **“当前账号”从哪里来**：`codex_accounts.json` 有明文的 `current_account_id` 键，不用解密就能拿到。但它记录的是 **Cockpit 最后一次切到哪个号**，不一定等于 Codex.app 此刻登录的号（用户可以直接在 Codex 里登录别的号）。要加一致性检查：如果当前账号最近的配额样本太旧，比如超过 30 分钟，胶囊上显示“账号可能已切换”，配额显示“未知”，不能默认按满额处理。
2. **账号 ID 对不上**：`quota.py` 里，JSON 来源用 `email` 当 `account_id`，SQLite 来源用 `identity`（没有才用 `account_id`）。同一个号可能以两个不同的 ID 进入 `quota_windows`，账号池里就会重复，按“当前账号”过滤时也可能匹配不到。必须先写一张明确的映射表，再写测试：Cockpit 的 `id` 对应 `email`，也对应 `identity`。
3. **数量对不上**：`codex_accounts.json` 的 `accounts` 列表里是 5 项，`codex_accounts/` 目录里有 18 个文件，`quota_samples` 里有 3 个账号、134 条样本。账号池应该以 `codex_accounts.json` 的列表为准；只出现在样本里的号标为“历史账号”，不参与判断。
4. **Antigravity 的当前账号还不知道**：`accounts/` 下有 3 个账号文件；`current_account.json` 只有一个 `email` 字段，不知道属于哪个产品；`provider_current_accounts.json` 里只有 `claude_desktop_account`。在核实之前，Antigravity 胶囊显示“账号未知”，不参与自动分配判断。
5. **读取范围要写成白名单**。现在的 `_collect_cockpit_json` 会把 `accounts.json`、`google_accounts.json` 的整条记录读进内存再挑字段。建议明确只读这几处：`codex_accounts.json`（只取 `id`、`email`、`plan_type`、`current_account_id`）、`codex-quota-history-v1.sqlite` 的 `quota_samples`、`platform-quota-history-v1.sqlite`。`secure-account-storage.key`、`*_accounts.sqlite`、`accounts/*.json`、`codex_accounts/` 一律不打开，并写测试断言这些文件没被打开过。
6. **邮箱是个人信息**。现在邮箱会被写进 `bus.db`，再经 SSE 发到浏览器。建议总线里只存邮箱的哈希，界面显示“账号 A/B/C”加用户可编辑的别名，完整邮箱只在悬停时显示，或者干脆不显示。
7. **`list_quota_windows()` 的新语义**：
   - `exhausted`、`min_remaining_percent`、`dispatch_recommended` 只按当前账号计算；
   - 新增 `pool: [{alias, remaining, reset_in, sample_age}]` 和 `pool_has_capacity`；
   - 当前账号低于阈值且池里有余量时，提示“可在 Cockpit 切到账号 B（91%）”。
   - 切号是用户的账号操作，Consilium 只提示，并提供“打开 Cockpit”按钮。
   - 回归测试：池里一个旧号归零、当前号还有 80% 时，不能判为整端耗尽。

### Q5 分端模型定义与自动分配

1. **规则存在哪**：用户手写的规则放进 **配置 JSON**（`~/Library/Application Support/consilium/routing.json`，带 `schema_version`，加载时校验，出错就报错并沿用上一份有效配置）。SQLite 只存运行时观测到的事实：配额样本、实际派活记录。配置小、要人改、要能备份和对比，适合文件；事实要并发写、要查询，适合 SQLite。界面编辑配置放到以后，v1 只读展示，并提示“编辑 routing.json”。
2. **先认清一个前提**：Consilium **没法切换原生 App 里的模型**。深链和 MCP 都不能指定模型。所以档位只能是“建议”：随任务下发 `model_tier`（字段已经有），由接活一端的 skill 或子代理配置自己选。比如 Claude 这边的 `opus-*` 子代理、Codex 的 `~/.codex/agents/*.toml`。因此模型名只是标签，不做强制，也不需要原型里那么细的下拉框。
3. **由后端给推荐**。一个函数，例如 `suggest_assignee(tier, task_kind)`，同时供 `/ui` 和 MCP 使用（协调者在 Claude 或 Codex 里通过 `consilium_status` 或新工具也能拿到同样的建议）。前端只显示结果和理由，可以手动改派。原型里的“模拟配额滑块试算”只是演示，不进正式版，否则就会有两套逻辑。
4. **防止过度复杂**：
   - v1 规则固定、可解释，只有三个输入：请求的档位、各端状态（待命优先于在线，离线排除）、当前账号的剩余配额（耗尽排除；未知照常允许，和路线图“未知不拦”一致）。
   - 平手时按剩余配额排；输出一句理由，例如“Codex：待命、配额 44%、有 fast 档”。
   - 只给建议，不自动派活，最终仍由协调者确认。
   - 全局只设一个低配额阈值（默认 20%），不按模型分别设。
   - 不做权重编辑，不按历史学习。用过一阵、确实不够用再说。

---

## 6. 建议给 Antigravity 的下一步（关口 2 新版）

1. 先合入 Q4 的 `list_quota_windows()` 修复，包括测试和账号映射表。这是唯一的后端改动。
2. 按 Q1 表里标“现在”的部分改写 `/ui`。沿用 `/api/state`、SSE、会话、CSRF 机制，不新增路由。
3. 修掉 §4 原型的 6 处问题。
4. 顺手修 §3 的 P3-1、P3-2，各补一条测试。
5. 交付：浅色、深色各一张截图（1440 宽和 390 宽）；一段 30–40 s 的录屏；全套测试；`openspec validate`。在 OpenSpec 里把“看板”的 spec 改成“对话流”，Q2、Q3、Q5 先以待办形式写进 `tasks.md` 的后续阶段。写成 R013（R012 留给 Codex 复核 R010）。

## 7. 请 Codex 在 R012 里重点看

- §2.3 实测 12（keep-alive 污染）是否还有我没想到的利用路径。
- Q4 第 1、2 点：Cockpit 的 `current_account_id` 和 Codex.app 实际登录号是否一致，`identity` 与 `email` 怎么对应。Codex 对自己这边的账号机制更清楚。
- Q2 第 6 点的 git 加固参数是否完整。
