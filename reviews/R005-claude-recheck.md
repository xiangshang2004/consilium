# R005：Claude 对 R003、R004 的复核与剩余整改清单

- **作者**：Claude（Claude.app Code 页签，Opus 5.5）
- **日期**：2026-11-18
- **审核对象**：[R003 Codex 独立复核](./R003-codex-review.md)、[R004 Antigravity 整改](./R004-antigravity-r002-fixes.md)，代码基线为提交 `48acd1a`
- **方式**：只读。没有改代码，也没有改任何实时家目录配置。只写了本文件和 `reviews/README.md` 的一行索引。
- **下一步**：交 Antigravity 修复，修完以 **R006** 提交；之后由 Claude、Codex 复核，再决定是否做实测和 `--apply`。

---

## 0. 结论

R004 把 R002 的 7 项都落了地，R003 的复核也很扎实。**但还不能对实时家目录 `--apply`。** 第 3 节列了 7 个 P1 问题，修完并补上回归测试后，再进入第 5 节的实测。

---

## 1. 本轮核实

| 项 | 结果 |
|---|---|
| 测试 | `env -u CLAUDE_CONFIG_DIR DESKBELL_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests`：**18 项通过，1.8 s** |
| 安装器测试是否会写到实时配置 | 不会。`scripts/install.py:165–166` 调 `claude` 前只把 `HOME` 换成临时目录，本机 `CLAUDE_CONFIG_DIR` 没有设置，所以写进的是临时目录。测试跑完后，实时 `~/.claude.json` 的顶层 `mcpServers` 仍然为空。**注意**：如果以后有人设置了 `CLAUDE_CONFIG_DIR`，这套测试就会写进真实配置。R006 里应在测试和安装器中显式清掉（或指定）这个变量。 |
| 实时配置 | `~/.claude.json`、`claude_desktop_config.json` 都没有 `mcpServers`；`~/.codex/config.toml` 的修改时间仍是 11-16、内容不含 deskbell；`~/.gemini/config/mcp_config.json` 仍为 0 字节；`~/.claude/agents`、`~/.codex/agents`、三处 `deskbell-collab` skill、`~/Library/LaunchAgents/com.consilium.daemon.plist`、`~/Library/Application Support/deskbell`、`~/Library/Logs/deskbell` 都不存在 |
| 意外产物 | `/tmp/deskbell-501` 已存在（应是某一方跑 CLI 或测试时建的），属于临时会合目录，无害 |

---

## 2. 对 R003 纠正 R002 的回应

以下几点 R003 是对的，R002 的说法以 R003 为准：

| R002 原说法 | 更正 | 影响 |
|---|---|---|
| Codex 默认工具超时按文档是 60 s | 0.169 实际是 **300 s**（同版本源码 `DEFAULT_TOOL_TIMEOUT`） | `tool_timeout_sec = 330` 只是可选余量，留着也没坏处 |
| Codex 深链“只接受 `prompt`” | 原生入口还接受 `path`（工作区目录）和 `originUrl` | 深链应改用 `path` 传工作区，不要把 `[cwd: …]` 写进提示正文 |
| `~/.codex/agents/*.toml` 能否加载未确认 | **会加载**，不受 `multi_agent_v2` 开关控制 | 三个 Codex 角色模板可以装 |
| `claude://new?q=` 可作兜底 | 原生 `open-url` 没有 host 为 `new` 的分支，不可靠 | 没有 `folder` 时也用 `claude://code/new?q=…` |
| R002 引用的参数白名单 | 那段校验用于窗口内导航，原生 `open-url` 走的是另一条分支 | 不影响结论，参数名仍是 `q`/`folder` |
| 重派任务后 mission “回不到 ready_to_report” | 实际是**一直停在 ready_to_report**，同时还有未完成的任务 | R004 已修，修法正确 |
| Claude Code 后台命令能活多久、何时叫醒代理 | 说得太绝对，这取决于宿主和会话状态 | “后台 waiter 只唤醒，由活会话认领”这条建议不变 |
| 基线测试“完全不碰家目录” | 对 `c55d730`（12 项）成立；`2e43056` 加了 quota 后会读真实配额缓存 | quota 测试需要隔离，见 4.5 |

---

## 3. 剩余 P1（R006 必须修）

每项都附了修法和验收用的回归测试。行号对应 `48acd1a`。

### 3.1 MCP 的 `deskbell_wait` 默认自动认领，取消后仍会替失联会话认领

- **现状**：`mcp_server.py:172` 的 schema 默认值和 `:488` 都是 `auto_pickup=True`；`:257` 直接丢掉 `notifications/cancelled`；stdio 循环在 wait 期间读不到任何消息；HTTP worker 也感知不到客户端已断开。客户端超时或取消后，事件一到仍会被 ack、任务被改成 `claimed`，结果却没有人收到。
- **修法**：MCP 和 CLI 一样默认 `auto_pickup=False`，wait 只返回“有新东西”；认领只能通过显式调用 `deskbell_pickup_inbox`。在 Python 3.9 标准库下做取消传播成本高，先不做。
- **验收**：wait 超时返回后再投一个任务，任务必须保持 `queued`。

### 3.2 `.seq` 序号文件：并发写没加锁、启动时不重建、`.sig` 被删后无法恢复

- **现状**：`wakeup.py:93–127` 的 `write_peer_seq` 是“读旧值 → 取 max → replace”，进程之间没有锁。慢写者可能用旧的 `unacked` 覆盖快写者的新值，`notify_peer` 也会读改写这个文件。daemon 或 stdio 启动时不会从 DB 重建各 peer 的 `.seq`，`/tmp` 被清理后，纯文件 waiter 看不到积压的未读。kqueue 也没有监听 `NOTE_DELETE`/`NOTE_RENAME`。
- **修法**：
  1. 对 `signals/<peer>.seq.lock` 加 `fcntl.flock(LOCK_EX)`，并且**在锁内从 DB 重新查** `max(event_id)` 和未读数后再写。这样最后一个写者写的一定是最新值，不再依赖 `max(旧值)`。
  2. `DeskBellDaemon.start()`、`run_stdio_server()` 启动时，对三个 peer 各重建一次 `.seq`。
  3. waiter 的 kqueue 加上 `NOTE_DELETE | NOTE_RENAME`，触发后重新打开文件再挂上；每次醒来都重读 `.seq` 并和自己记下的值比较，没有变化就继续等，不把空唤醒交给代理。
- **验收**：①两个进程交替快慢写入，最后 `.seq` 等于 DB 的真实值；②删掉 `signals/` 后重启 stdio server，`.seq` 能恢复；③waiter 等待期间删掉 `.sig` 再投事件，仍能被唤醒。

### 3.3 令牌传递：放进了 URL 和进程参数

- **现状**：`install.py:375` 和 `adapters/antigravity/mcp_config.snippet.json` 用的是 `?token=`，令牌会出现在客户端日志、报错和诊断信息里；`install.py:180–192` 用 `claude mcp add -H "Authorization: Bearer …"`，令牌会在子进程 argv 里出现一小段时间。
- **修法**：
  - Antigravity：先查 Antigravity 2.33 的 MCP 配置是否支持 `headers`（或 `serverUrl` 加 headers 的写法）。支持就改用请求头；确实不支持的话，在 R006 里写明这个风险并接受，同时服务端绝不记录 query string。
  - Claude：查一下 `claude mcp add-json -s user` 加 headers 里的环境变量展开能不能避免把令牌放进 argv；做不到就在 R006 里说明“argv 会短暂暴露，同一用户可见”。
  - 无论哪种方式，令牌都要支持轮换：`deskbell rotate-token` 先重写各客户端配置，最后再换服务端的令牌。
- **验收**：安装产物里搜不到 `?token=`（或已在 R006 里明确接受这个风险）。

### 3.4 认领超时回收没有接入

- **现状**：`store.py:635` 的 `release_stale_claims` 只有测试在调用；而且只处理 `claimed`，会话死在 `running` 的任务照样会卡住。
- **修法**：daemon 起一个后台线程，每 60 s 调一次；stdio server 在每次 `pickup_inbox` 前也调一次。`running` 状态以 `report_progress` 更新的 `updated_at` 作为心跳，超时阈值可以配置，默认 `claimed` 15 分钟、`running` 2 小时。回收的任务回到 `queued`，并给协调者发一个 `task_requeued` 事件。
- **验收**：伪造一条过期的 `running` 任务，启动 daemon 后它会回到 `queued`，协调者收到事件。

### 3.5 数据已写库但通知失败时，整体报错，重试会造成重复

- **现状**：create、dispatch、complete、review 都是先提交 SQLite，再调用 `ring_peer`。如果 `ring_peer` 抛异常，MCP 返回 `isError`，调用方重试就会重复派单、重复完成，或者多消耗一轮审核次数。
- **修法**：在 `mcp_server` 里统一用 try/except 包住 `ring_peer`，返回 `{"committed": true, "delivery": {"error": "..."}}`，`isError` 保持 false。create/dispatch/complete 加一个可选参数 `client_request_id`，库里加唯一约束，重复请求直接返回第一次的结果。
- **验收**：让 `ring_peer` 抛错时，返回 `committed=true`；用同一个 `client_request_id` 重试，不会产生第二个任务。

### 3.6 冷通知不能点击打开，SKILL.md 的说法不成立

- **现状**：`wakeup.py:766` 的 `display notification` 只是一条提示，点了不会打开深链；只有显式传 `open_deeplink=true` 时才会调用 `open`（会切换前台）。SKILL.md 里写的“按一次 Return 即加入”做不到。纯标准库没办法做可点击的通知（那需要 `terminal-notifier` 之类的第三方工具）。
- **修法（默认方案，可由用户改）**：通知保持不抢前台；SKILL.md 改成“收到通知后手动切到对应 App，运行 pickup”；mission 级别的 `open_deeplink` 由协调者在用户同意后打开。Codex 深链改用 `codex://new?prompt=…&path=<编码后的绝对路径>`；Claude 深链在没有 `folder` 时也走 `claude://code/new`。
- **验收**：深链单测覆盖 `path` 编码；SKILL.md 中不再出现“按 Return 即加入”。

### 3.7 安装器仍可能装到一半

- **现状**：`run_installer` 依次执行 Antigravity、Claude Desktop、Claude Code、Codex 的写入（`install.py:392–510`）。Codex 重复表的检查要到第四步才做；Claude.app 是否在运行的检查（`:525`）在**全部写完之后**，而且只是提示。`claude mcp remove → add` 之间如果失败，旧条目就丢了（`:167–200`）。`--target-home` 生成的配置仍指向本机真实的 `/tmp/deskbell-<uid>`。
- **修法**：
  1. 拆成两阶段：先把所有目标读进来、校验一遍（重复表、JSON 能否解析、Claude.app 是否在运行、`claude` 能否执行），**任何一项不过就一个字都不写**；全部通过后再写。
  2. 重复表的检测要覆盖 `[mcp_servers.deskbell]`、`[mcp_servers."deskbell"]`，以及以 `mcp_servers.deskbell.` 开头的点号键。
  3. `claude mcp remove` 之前先用 `claude mcp get deskbell` 保存旧条目，add 失败时恢复。
  4. 加 `--rendezvous-dir` 参数，测试里指向临时目录。
  5. 安装器和测试在调用 `claude` 时显式设置 `CLAUDE_CONFIG_DIR=<target_home>/.claude`，或者把它从环境里删掉（见第 1 节）。
- **验收**：①Codex 配置里有块外重复表时，`--apply` 失败，并且**其他三份配置的字节都不变**；②Claude.app 在运行时 `--apply` 直接拒绝（不是只提示）；③模拟 add 失败后，旧条目被恢复。

---

## 4. P2（能顺手就修，否则列进 R006 的已知问题）

1. **ack 和“能否认领”不是同一个状态**：`pickup_inbox(claim_tasks=False)` 会把 `task_assigned` 事件也 ack 掉，但任务仍是 `queued`，下一次 wait 的预检看不到它。预检应同时统计该 peer 名下 `queued` 的任务数。
2. **`request_review` 在已关闭的 mission 上仍能推进轮次**（`store.py:862`），而且没校验可选 `task_id` 是否属于这个 mission。
3. **RPC 入参校验**：批量请求里的非对象元素、形状不对的 `params`、负数或非法的 `Content-Length` 都要拒绝；HTTP handler 设 30 s 读超时；布尔参数用严格判断，不要用 `bool("false") == True` 这种转换。
4. **身份问题**：共享一个 Bearer 令牌，没法证明请求里 `peer_id` 的真假。MVP 先接受，在 R006 里写成已知限制；以后可以给每个 peer 发独立令牌。
5. **quota**：
   - 没有数据时 `dispatch_recommended` 返回 `null`（未知），不要返回 `True`（`store.py:1212`）。
   - 保留 `window_minutes`，不要固定命名成 5h/7d。
   - `limit_id` 不是账号（`quota.py:359`）。
   - `reset_at` 已过的耗尽记录要忽略。
   - 读 `~/.codex/sessions` 时只解析带 `rate_limits` 的行，只保留数值字段，绝不返回或缓存原始行（rollout 里可能有明文账号）。
   - quota 测试要隔离真实缓存。
6. **补发冷通知的定时器**是守护线程，进程退出就没了。daemon 和 stdio 都是常驻进程，所以 MVP 可以接受，在文档里写一句即可。

---

## 5. 需要用户决定的事

| 事项 | 默认方案 | 说明 |
|---|---|---|
| 冷通知要不要切换前台、打开深链 | 默认不切换；每个 mission 由协调者在用户同意后开启 | 3.6 |
| Antigravity 是否接受令牌放在 URL 里 | 先查是否支持 headers，不支持再来问 | 3.3 |
| R003 第 6 节的四项实测（打开深链、启动 daemon 让三端连上、`launchctl` 临时任务、隔离环境跑安装器集成测试） | R006 修完、复核通过后再做，逐项征得同意 | 会切换前台、新建会话、改变 launchd 状态 |
| 是否用 deskbell 取代 Codex 的“交接文件 + 手动粘贴”习惯 | 先并行使用，不替换 | 用户既有的工作方式 |

---

## 6. 交给 Antigravity 的要求（R006）

1. 按第 3 节逐项修复，每项附上对应的回归测试；第 4 节能修的一起修，修不了的写进“已知问题”。
2. 仍然不碰实时家目录；测试和安装器都要处理 `CLAUDE_CONFIG_DIR`（3.7 第 5 条）。
3. R006 写清楚：改了哪些文件、新增了哪些测试、全套测试的输出，以及 3.3 两个令牌问题的调查结论。
4. 提交时只包含 deskbell 相关文件；`reviews/R003-codex-review.md` 和 `reviews/README.md` 目前还没提交，可以和 R005 一起提交。
