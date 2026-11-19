# R003 · Codex 独立复核 DeskBell

2026-11-18 · macOS 27.2.1 · ChatGPT.app 26.1111.21308（27194）· 内置 Codex CLI 0.169.0-alpha.11.1

**复核结论。** R002 对基线的 `/tmp` 状态丢失、PID 复用和 HTTP 默认无令牌三项判断成立。Codex 深链应使用 `prompt`，原生入口也支持 `path` 和 `originUrl`。这版 Codex 的运行时默认工具超时为 **300 秒**，用户级 `agents/*.toml` 会自动加载。同轮落下的 R004 修复了基线的若干问题，实时安装前仍要处理通知交付、等待取消、临时状态恢复和凭据传递。

先读 `2e43056` 的 `deskbell/*.py`、安装器和 adapters，在 Python 3.9.6 下跑 `DESKBELL_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests -v`，**14 项通过，0.185 s**。测试进程禁写家目录及仓库、禁写字节码，并隔离了 `deskbell_status` 会隐式读取的真实配额缓存。HTTP 用 `socketpair()`，安装器用临时家目录。随后读 R001、R002。下文 §1–4 的仓库行号对应 `2e43056`。复核期间 HEAD 更新至 `48acd1a`，该提交的差异见 §5。[R001](./R001-mvp-design-and-impl.md) · [R002](./R002-claude-audit.md) · [R004](./R004-antigravity-r002-fixes.md)

## 1. 本机 Codex 的版本证据

### 1.1 深链（R002 §3.5）

**R002 对 `q`、`folder` 的结论成立，对「只接受 `prompt`」的推论过宽。** 本机 `/Applications/ChatGPT.app/Contents/Resources/app.asar` 的 `.vite/build/early-bootstrap-631-c364c4eef36b8fb5.js` 中，模块 `26133` 的 macOS `open-url` 回调走 `F → V → G`。R002 引用的单参数白名单来自同模块的 `B`，调用方是 `early-bootstrap-226-2f6bd181558008cf.js` 的 `isNavigationOnlyLink`。以下结果来自把这几段本机解析函数放进 Node，以固定字符串做的离线验证。

| 示例 | macOS 入口解析结果 | 导航白名单 |
|---|---|---|
| `codex://new?prompt=review`、`codex://threads/new?prompt=review` | 新会话路由，预填 `review` | 放行 |
| `codex://new?q=review` | 无新会话路由 | 拒绝 |
| `codex://threads/new?q=review` | 空白新会话路由，`q` 被忽略 | 拒绝 |
| `codex://new?folder=%2Ftmp` 或仅 `cwd` | 无新会话路由。和有效 `prompt` 同传时，`prompt` 生效，`folder`、`cwd` 被忽略 | 拒绝 |
| `codex://new?prompt=review&path=%2Ftmp`，或 `codex://threads/new?path=%2Ftmp` | 新会话路由携带 `path=/tmp`。后续检查目录是否存在，再设置工作区 | 拒绝 |
| `codex://new?originUrl=…` | 携带 `originUrl`，供后续匹配已有工作区的 Git remote | 拒绝 |
| `codex://threads/new`、`codex://new` | 前者产生空白新会话路由，后者无路由 | 前者放行，后者拒绝 |

`https://chatgpt.com/codex/open-app` 有把网页 `q` 映射到 `prompt` 的分支。基线 `deskbell/wakeup.py:538–556` 直接生成原生 `codex://new?q=…`，预填落空。R004 改对了提示词键。若要绑定工作区，本机原生路由可传已编码的绝对目录 `path`。写在提示词里的 `[cwd: …]` 只会成为输入框文本。实际前台行为仍属于文末待选实测。[ChatGPT 桌面端深链文档](https://learn.chatgpt.com/docs/app/commands#chats)

### 1.2 MCP 超时与配置（R002 §3.1）

**默认 300 秒。** 内置二进制与 `codex-package.json` 都标为 `0.169.0-alpha.11.1`。同版本公开 tag 指向 `226fa889`。该提交的 `DEFAULT_TOOL_TIMEOUT` 为 `Duration::from_secs(300)`，连接管理器在服务器配置缺省时直接使用它。公开指南的 60 秒与这个构建不符。本机 App 给自己的 MCP 填写的 `3600` 是单个服务器的覆盖值。`mcp get` 能读出配置的 `330`，也只证明该字段能解析。[同版本 tag](https://github.com/openai/codex/tree/rust-v0.169.0-alpha.11.1) · [运行时常量](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/codex-mcp/src/rmcp_client.rs#L121-L122) · [默认值选择](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/codex-mcp/src/connection_manager.rs#L523-L530) · [公开 MCP 指南](https://learn.chatgpt.com/docs/extend/mcp?surface=cli#configure-mcp-servers)

**`330` 是可选的余量。** 如果产品需要提供满 300 秒的 `deskbell_wait`，客户端与服务端同为 300 秒容易在注册和返回时撞到截止时间。若日常用默认的 45 秒调用，现有客户端默认值已有余量。仅凭 R002 的 60 秒推断不足以把 MCP 上限改到 50 秒，也不能据此推算 Claude Chat 的超时。取消和认领仍须独立处理。

**MCP 表名成立。** 本机配置有 `[mcp_servers.…]`，同版本源码把有 `url` 的条目解析成 Streamable HTTP，支持 `bearer_token_env_var`、`http_headers`、`env_http_headers` 和本机 `http_headers_helper`。旧模板的表名待核注释可以去掉。`url` 单独就能构成配置，但真实连接还取决于服务是否启动、鉴权与握手。OAuth 探测或 `mcp get` 的输出无法证实 DeskBell 可用。无 SSE 时，R002 建议对 `GET /mcp` 返回 `405` 正确，原来的 `404` 不符合该传输规范。终端的 shell 初始化不保证 Finder 启动的 App 继承环境变量，静态 Authorization 在语法上可用，仍要考虑凭据落盘、备份和轮换。[传输配置源码](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/config/src/mcp_types.rs#L542-L571) · [MCP 2025-03-26 Transport](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#listening-for-messages-from-the-server)

### 1.3 代理配置（R002 §3.6）

**会加载。** 在常规本地用户配置层下，默认 Codex home 为 `~/.codex`。配置构建直接调用 `load_agent_roles`，它查找各活动配置目录下的 `agents/` 并递归读 `.toml`。独立文件使用 `name`、`description` 和 `developer_instructions`，可携带角色专用的模型、推理强度和沙箱配置。仓库三个模板具备独立角色所需字段，当前本机相应目录尚未创建。源码确认 `multi_agent_v2` 为 stable 且默认关闭，该 flag 不控制角色文件的发现和加载。角色可见性、可用模型和实际 spawn 仍受当前运行环境的工具及权限配置约束。[配置构建](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/core/src/config/mod.rs#L4056-L4059) · [角色发现与校验](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/agent-roles/src/loader.rs#L75-L112) · [角色文件格式](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/agent-roles/src/agent_role_config.rs#L20-L88) · [功能默认值](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/features/src/lib.rs#L1649-L1659)

## 2. 三项基线风险（R002 §3.3）

| R002 判断 | 复核 |
|---|---|
| `/tmp` 清理威胁数据库、令牌与日志 | **成立，有条件。** 本机 `/System/Library/LaunchDaemons/com.apple.tmp_cleaner.plist` 在本地日历的 00:00 调度 `/usr/libexec/tmp_cleaner`。后者是 shell 脚本，清理符合 `-atime +3 -mtime +3 -ctime +3` 的普通文件和符合条件的空目录。闲置 `bus.db` 可以消失，令牌在启用鉴权时才创建，删除后运行中进程仍持有旧值，重启时客户端配置才可能和新值不一致。脚本无法证明每次开机都会清空整个目录。基线日志父目录确实可能在首次启动或系统清理后缺失，具体 launchd 启动表现待实测。`os.utime` 无法把 `ctime` 也设成过去，适合测试清理后的恢复逻辑，不足以复刻系统清理条件。`deskbell/models.py:44–85`，`adapters/launchd/com.up.deskbell.plist.tmpl:29–32`。 |
| 目录抢占、信号文件删除和存储拆分 | **机制成立。** 默认目录末级通过 `lstat` 检查 symlink 和 UID，`/tmp` 的 sticky bit 阻挡其他 UID 替换正常目录。预占会让初始化失败，自定义路径的祖先目录仍需可信。超长路径回退的 socket 直接落在 `/tmp` 根下。若只有 `.sig` 被删重建，原 watcher 的 fd 仍指向旧 vnode，能错过新文件写入。基线 VNODE mask 未监听删除或重命名。支持持久状态与会合文件拆分，前提是临时索引能从 DB 恢复，并且 watcher 在信号文件消失时能重新挂载。`deskbell/wakeup.py:22–29, 378–401`。 |
| PID 复用留下幽灵 lease | **成立。** lease 只存 PID 和截止时间，`kill(pid, 0)` 成功或 `EPERM` 仅表示该 PID 目前存在。旧 lease 未被清理、PID 已被复用且尚未到期时，会计入 `woken_count`，从而跳过冷通知。默认 CLI 窗口约 24 小时加 5 秒，显式 CLI `--timeout` 可更长。`flock` 可以消除 PID 身份歧义，但要保证 lease 的创建、锁定和清理没有发布竞态。补发依据宜用目标事件的 ack，并考虑重复提醒与进程退出后如何恢复。`deskbell/cli.py:35–39`，`deskbell/wakeup.py:81–136, 354–366, 499–519, 628–642`。 |
| 默认无令牌，其他本机账号可触达 | **成立。** CLI、构造器及原 plist 都不会默认要求 Bearer。绑定 `127.0.0.1` 限制网络接口，Host/Origin 检查限制报文中的主机，也接受缺少 Origin 的客户端。本机有环回网络权限的其他进程或 UID 可访问 MCP 的读写工具，影响包括读取任务及把不可信提示放进收件箱。是否执行这些提示取决于接收端权限和决策。默认开启认证是合理修复，共享 Bearer 仍只提供访问总线的权限，无法证明申报的 `peer_id`。`deskbell/cli.py:25–29`，`deskbell/mcp_server.py:541–582`，`adapters/launchd/com.up.deskbell.plist.tmpl:7–19`。 |

## 3. R002 其余条目

| 条目 | 判断 |
|---|---|
| §1–2 基线及本机清单 | **基线数字过时。** 当前审计基线是 14 项，含两项 quota 测试。R002 称用例完全不碰真实家目录过宽，原 quota 测试会走 `deskbell_status → sync_local_quotas`。本机 Claude.app `6.6395.0`、Claude Code CLI `2.1.231`、Antigravity `2.33.0` 与 R002 一致。当前会话的 PATH 能解析到内置 Codex CLI。抽查的 Claude 两份配置无顶层 MCP 字典，Antigravity MCP 文件为空，实时 DeskBell plist 未安装。这里只核了配置结构和文件存在性。 |
| §3.1 追加 TOML 管理块 | **风险成立。** 如果同一 server 已在块外定义，追加表可导致配置解析失败。应在任何实际写入前校验整份 TOML，覆盖已存在条目、其他合法键写法和无效输入。`scripts/install.py:92–145`。 |
| §3.2 Claude 配置并发写 | **成立。** 先读后截断写会丢并发更新或留下半文件。官方 CLI 是 Claude Code user scope 支持的入口，原子替换能防半文件，也要防读取旧快照后覆盖别人的新字段。需在变更前处理运行中客户端，现有内容与拥有的条目要能分别识别。`scripts/install.py:45–89, 239–264`。[Claude Code MCP user scope](https://code.claude.com/docs/en/mcp#user-scope) |
| §3.2 Claude 各页签、后台和 Stop hook | **部分成立。** Claude Desktop 的本地 stdio 与 Claude Code user scope 分开配置有据。Code 页签是否同时加载两份、Cowork 转发、Chat 的工具超时均未证实。Claude Code 文档支持前台工具返回后后台命令继续运行，持续时间与回传行为取决于宿主、会话及是谁启动了任务。R002 据此保证本机页签关闭、退出自动唤醒以及 24 小时待命过于绝对。后台 waiter 只唤醒、由活会话认领的建议成立。阻塞 Stop hook 会占住会话收尾，短检查可用。基线 stdio 循环确实会被 wait 阻塞。`deskbell/mcp_server.py:489–512`。[Claude Code 后台命令](https://code.claude.com/docs/en/tools-reference#background-commands) |
| §3.4 关闭任务与 mission 状态 | **大体成立，R002 误述了一处状态。** 基线重派后始终留在 `ready_to_report`，可能一边有未完成任务一边显示可汇报。确有越过 assignee、重复完成、通过 progress 写终态、掉线后 claim 无回收等问题。基线已有 9 个工具，`close_mission` 是第 10 个候选。关闭校验应覆盖真正的会话身份、非终态任务、终态返写与幂等性。仅比对客户端提供的名字不能建立权限。`deskbell/store.py:444–479, 491–558, 560–710, 800–826`。 |
| §3.5 Claude、UTF-8、Antigravity 和 AppleScript | **部分成立。** 本机 Claude 6.6395.0 的 `app.asar` 中，`PW` 的 Code `/new` 分支读取 `q` 或 `prompt`，实际按 `QN=14336` 切片，并读取 `folder` 和 `file`。Chat 的 `/new` 在 `claude.ai` host 分支，原生回调没有 host 为 `new` 的分支，`claude://new?q=…` 兜底不可靠。R002 描述的参数白名单、斜杠和 hash 检查确实在 `Be`，它用于窗口导航，原生 `open-url` 直接走 `PW`。基线用 180 个「中」离线验证，两端都截至 295，尾部剩 `%` 或 `%E`，Claude 丢掉 `folder`。按字符预算后再编码可取，295 和 2000 都缺少本机原生上限依据。Antigravity 本次只证实 scheme。AppleScript argv 避免正文被拼进脚本，通知交付能力见 §4。`deskbell/wakeup.py:538–602`。 |
| §3.6 项目流程与 Claude 模型 | **项目优先成立。** CapTalent 本机 skill 确实要求先认领并使用 `claude/`、`codex/`、`gemini/`。基线只传工作树元数据，未规定交接。前缀、清理和合并由目标项目规范决定，没必要作全局规则。基线两个 Claude 模板确实选了 `sonnet`、`haiku`。若采用 R002 转述的长期模型约定，冲突成立。这次未独立确认该约定或 `fable` 在目标页签的可用性。深链开新会话的判断正确，是否替换既有交接习惯由用户决定。 |
| §4 卸载、版本协商、路径输出、空唤醒、KeepAlive | **多数成立。** 基线卸载按文件名删除，可能删用户修改或预先存在的文件。`initialize` 原样回显不符合版本协商。内部路径由 HTTP `/health` 和 CLI `status` 返回，MCP `deskbell_status` 本身无那两个路径字段。kqueue 对任何匹配的 vnode 事件返回 `woke=True`，需回查实际待办。`KeepAlive=true` 会重启退出的服务，本机 `launchd.plist(5)` 已给出默认 10 秒节流，显式设相同值不改变行为。SIGTERM 清理与日志父目录分别验证。`scripts/install.py:149–165`，`deskbell/mcp_server.py:232–241, 275–287, 595–610`，`deskbell/wakeup.py:378–401`。[MCP 版本协商](https://modelcontextprotocol.io/specification/2025-03-26/basic/lifecycle#version-negotiation) · [Apple launchd 指南](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html) |

## 4. R002 漏掉的实质问题

1. **[P1] 已取消的 MCP wait 仍可能替失联会话认领。** `notifications/cancelled` 在分派器被通用分支忽略。阻塞的 stdio 循环也读不到它，HTTP worker 在等待时不感知客户端断开。默认 `auto_pickup=True`，晚到的事件仍可被 ack、任务变成 `claimed`，随后客户端拿不到结果。R002 只在后台 CLI `--pickup` 场景指出任务搁置。应让等待与认领分离，或让取消传播到实际认领点。`deskbell/mcp_server.py:221–230, 437–447, 489–512, 612–639`，`deskbell/wakeup.py:521–523`。
2. **[P1] 默认冷通知没有可点击的深链。** `display notification` 只设置正文、标题、副标题和声音。深链在返回的 MCP 数据里，只有显式 `open_deeplink` 才交给 `open`。Skill 承诺的「按 Return 即加入」没有相应交互。修正协议参数也不会改变这一点。`deskbell/wakeup.py:558–611`，`adapters/skill/deskbell-collab/SKILL.md:24–27`。
3. **[P1] 持久化成功和通知失败会被合成一次报错。** 创建、重派、完成和求审都先提交 SQLite，再 pulse 或调用系统通知。如果后半段抛异常，MCP 返回工具错误，已提交的任务和事件仍在，重试可重复派单、完成或消耗审核轮次。需要可区分的提交结果与交付结果，以及重试语义。`deskbell/mcp_server.py:256–272, 289–349, 383–435`，`deskbell/wakeup.py:65–72, 596–602`。
4. **[P2] ack 与任务可领取性不是同一个状态。** `pickup_inbox(claim_tasks=False)` 也 ack 分配事件，任务仍是 `queued`。wait 的即时检查只看未 ack 事件，下一次待命可能沉睡到别的事件到来。还有 `request_review` 可以在已关闭 mission 上继续推进轮次，可选 `task_id` 未校验归属。`deskbell/store.py:481–531, 712–798`，`deskbell/wakeup.py:485–497`。
5. **[P2] RPC 边界缺少基本形状与读超时校验。** 非对象批元素或错误形状的 `params` 能在进入工具错误处理前抛异常。HTTP 读取未拒绝负或非法长度，也缺少读超时。工具 schema 仅用于声明，宽松的 `bool(...)` 转换可能让错误类型的布尔参数触发 UI 动作。应在各入口统一验证，确保单个坏请求不会破坏连接后续可用性。`deskbell/mcp_server.py:211–263, 300, 347, 618–631`。
6. **[P2] R001 第七问的 quota 缓存与路由语义被跳过。** 本机只读检查确认 Cockpit 两个 SQLite 目标表具备查询列，其中 `quota_samples` 要读含 WAL 的快照。最新取样的 Codex rollout 有 `used_percent`、`resets_at`、`window_minutes`，样本仅含一个非空窗口。所查的 Cockpit 账户索引未观察到代码期待的配额容器，两个 Claude 本地 JSON 候选均不存在，不能承诺 Claude 实时配额可用。实现把 `limit_id` 当账号、固定把 primary/secondary 命名为 5h/7d，且合并所有账号最小值，重置时间已过的耗尽记录仍能长期阻止派单。无数据时又给 `dispatch_recommended=True`。应保留窗口时长、区分账号和受限项，并把过期或缺失观察表达为未知。`deskbell/quota.py:94–177, 313–387, 391–478`，`deskbell/store.py:869–884, 977–1009`。[同版本 Codex 限额字段](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/codex-api/src/rate_limits.rs#L53-L56) · [窗口时长的解析](https://github.com/openai/codex/blob/226fa88979b1a9d76dc36f339ee9c5fbdb2c8e7f/codex-rs/codex-api/src/rate_limits.rs#L196-L214)

## 5. 同轮提交的 R004 差异

`48acd1a` 已改用 Codex `prompt` 和编码前裁剪、持久 DB/令牌、`flock` lease、默认 HTTP 认证、`GET /mcp` 的 405、原子文件替换，并加了第 10 个工具及若干状态检查。R004 记录这版 18 项通过。R003 的 14 项是基线实跑。当前差异核验用源码和离线深链检查，现版全量回归会新增真实 `claude mcp add/remove` 调用，因此本轮未重跑这套用例。[R004 验证记录](./R004-antigravity-r002-fixes.md#21-单元测试全套-18-项通过)

| 当前仍需关注 | 依据 |
|---|---|
| **令牌进入 URL 和 CLI 参数。** `?token=` 可进入客户端记录的 URL、诊断、错误和日志。`claude mcp … -H` 还将令牌放进子进程 argv。优先确认各客户端可用的认证方式，再决定如何传递同一个长期秘密。另有一个装卸边界，Claude CLI 继承了现有环境，只切换 `HOME`，如果已设置 `CLAUDE_CONFIG_DIR`，临时安装未必操作报告声称的同一份配置。 | [HTTP 校验](../deskbell/mcp_server.py#L646) · [安装器 CLI 配置与 Antigravity URL](../scripts/install.py#L121) |
| **`.seq` 的全局单调性与重建尚未成立。** SQLite 提交后各进程分别读快照，再读旧 `.seq`、算 `max`、`replace`。慢写者能在快写者后落盘，覆盖较新的 seq/unacked。`notify_peer` 也会读改写该文件。原子替换只保证读到完整文件。清理或重启后，纯文件 waiter 看不到旧未读，启动路径没有从 DB 重建每个 peer 的 seq，`.sig` 也仍缺删除/重命名的恢复。 | [SQLite 快照](../deskbell/store.py#L192) · [会合文件写入](../deskbell/wakeup.py#L93) · [kqueue 等待](../deskbell/wakeup.py#L504) · [服务启动](../deskbell/mcp_server.py#L759) |
| **认领回收及身份仍有缺口。** `release_stale_claims` 尚无生产入口调用，且只覆盖 `claimed`，实际 `running` 死会话仍会卡住。wait 的默认自动认领与取消路径仍在。新的 assignee/coordinator 检查使用同一份共享凭据下的请求字段，能防误用，无法约束蓄意冒名。 | [认领回收](../deskbell/store.py#L635) · [MCP wait](../deskbell/mcp_server.py#L478) · [MCP 关闭](../deskbell/mcp_server.py#L505) |
| **安装仍可部分成功。** 重复表检查仍在前几份配置动过之后，正则只识别部分表写法。Claude 已运行的提示也在写入结束后生成。`remove → add` 之间失败会丢旧 entry。`--target-home` 若未另设会合目录，生成的配置仍引用本机默认 `/tmp/deskbell-<uid>`。安装或迁移测试应完整校验计划和目标路径，再写入。 | [TOML 检查](../scripts/install.py#L220) · [安装顺序与运行态提示](../scripts/install.py#L338) |

## 6. 需要用户决定的实测

以上版本判断、静态风险及基线单测已有证据。下列验证会创建实际进程、会话或服务状态，等用户选择。

| 实测 | 可观察结果与副作用 |
|---|---|
| 用无业务数据的 `prompt` 打开 Codex 两种新会话 URI，必要时加 `path`。对 Claude 核对 `claude://code/new` 的无目录和有目录两种形式 | 核对 composer 和实际工作区。会切换前台，可能新建草稿、会话或工作区记录 |
| 在隔离配置中启动 DeskBell，由 Claude Chat、Code、Cowork 及 Antigravity 检查工具列表、认证与真实 wait 取消。若需要，验证指定 Codex 角色可被实际选中 | 会运行进程、更新临时配置或占用会话，验证令牌传递、重复加载及后台 job 生命周期 |
| 用临时 plist 和独立 job label 做 `launchctl` 装载、退出和日志父目录验证 | 会改变当前 launchd 域的 job 状态并写日志。结束需要卸下临时 job |
| 当前提交的安装器集成用例 | 会真实调用 Claude Code CLI，写入临时配置。先确认不会继承指向实时配置的路径，再决定是否执行 |
