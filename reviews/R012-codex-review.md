# R012 Codex 独立技术复核 · 关口 1

2026-11-20 · 当前实现 `a192f9f` · 测试快照 `fd89018`

**★ 关口 1 暂缓通过。** R010 已修正 R009 的默认误认领、旧 attempt 写回、同主机跨端口 Cookie 写入和大部分提交后错误路径。仍有两个阻塞项。令牌轮换失败报告会输出仍有效的旧 Bearer。票据签发和 SSE 注册在并发轮换时可能把旧 Bearer 的认证结果绑定到新版本。R011 对常规路径的核验成立，其「无 P1 或 P2 遗留」结论覆盖不到这些分支。关口 2 已由用户确认，以 `fd89018` 的记录为准。 [R009](./R009-codex-review.md) · [R010](./R010-antigravity-r009-fixes.md) · [R011](./R011-claude-r010-review-and-chat-ui.md) · [R013](./R013-antigravity-chat-ui-and-q4.md) · [关口记录](../openspec/changes/consilium-v1/tasks.md#L19)

复核范围包括 R010 的实现与回归 `ce2c882`、`9ced8af`，以及 R013 的加固、配额与对话流 `065486e`、`749c6bf`、`7a52808`。在 `fd89018` 的临时归档内用系统 Python 3.9.6 运行 `/usr/bin/python3 -B -m unittest discover -s tests`，环境移除 `CLAUDE_CONFIG_DIR`，启用 `CONSILIUM_DRY_RUN=1`、`CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1` 与临时家目录、状态目录、会合目录，系统沙箱保护真实家目录与工作区，IP 连接限于回环。**65 项通过，7.310 秒。** `openspec validate consilium-v1 --strict` 通过。随后 `a192f9f` 只增加了 `AGENTS.md` 的分工约定，沿用同一结果。R011 的浏览器/HTTP 实测与 R013 的默认环境测试、演示媒体属于各报告的证据。本轮复核代码、测试和报告，未复跑这些实测。 [R011 核验](./R011-claude-r010-review-and-chat-ui.md#2-我这边的核验) · [R013 核验](./R013-antigravity-chat-ui-and-q4.md#4-全量验证与媒体产物证据)

## 1. 关口 1 阻塞项

1. **[P1] 轮换失败诊断包含有效令牌。** Claude Code 的配置已更新、后续目标失败时，轮换器在 `rollback_instructions` 内嵌入更新前的完整条目，连同原 Authorization 字段。服务端令牌最后才替换，在该分支通常仍接受旧值。CLI 把整个报告输出至 stderr，复制错误报告或收集日志会随之带出凭据。子进程诊断也未经统一脱敏。回滚说明应引用受保护的备份与目标路径，诊断统一脱敏。现有回归让 Claude 阶段自身失败，未覆盖它成功后别的步骤失败，以及报告中不含新旧令牌的断言。 [轮换失败报告](../scripts/install.py#L873) · [内嵌旧条目](../scripts/install.py#L898) · [CLI 输出](../consilium/cli.py#L182) · [现有回归](../tests/test_r010_regressions.py#L1620)
2. **[P1] 票据与 SSE 在并发轮换时丢失认证版本。** `_authenticate_request` 用当时的令牌验证 Bearer，只返回认证模式。签票和 SSE 注册各自再读取一次当前版本。轮换发生在两次读之间时，凭旧 Bearer 通过的请求可以拿到新版本票据或被登记为新版本 SSE 订阅。循环的版本检查随即把后者视为有效。Cookie 流也在第一次复查会话之前发送初始快照。应把认证版本传到签票、订阅和首次发送，并与撤销检查同步。现有测试先签票、建流，再轮换，证明了顺序路径的撤销。 [认证](../consilium/mcp_server.py#L1324) · [签票](../consilium/mcp_server.py#L1696) · [SSE 注册与首帧](../consilium/mcp_server.py#L1464) · [轮换回归](../tests/test_r010_regressions.py#L853)

## 2. R009 遗留与 R010、R011 核销结论

| 事项 | 当前证据与边界 |
|---|---|
| `.seq`、租约和回收（R010 #1、#2、#12） | **主要修法成立。** 锁移到会合目录根下，同一锁内以单个 SQLite 快照重查、原子发布。新增双进程写入与删除 `signals/` 回归。租约先锁临时文件再发布，扫描器看不到未初始化的 `.lease`。回收线程实测覆盖 `claimed` 超时回队与唤醒、`running` 只标陈旧，MCP pickup 使用同一套 daemon 超时值和通知逻辑。恢复边界仍受会合目录根生命周期约束。锁随整个根目录被清理时会换 inode，启动重建默认不发脉冲；已阻塞 waiter 遇到仅重建积压、没有新写入时未必醒来。现有删除回归随后又派发了新事件。 [锁与重建](../consilium/wakeup.py#L80) · [无脉冲重建](../consilium/wakeup.py#L240) · [租约](../consilium/wakeup.py#L252) · [单快照](../consilium/store.py#L250) · [pickup 前回收](../consilium/mcp_server.py#L679) · [回归](../tests/test_r010_regressions.py#L251) |
| 认领代次与手动重排（R010 #4、#14） | **新任务路径成立。** 进度与完成在事务内检查有效 `attempt_id`，旧尝试失效。只允许对打开的 mission 中的 `claimed`、`running`、`failed`、`blocked` 重排，旧进度、汇总和产物进入尝试历史。**[P2] 旧库升级有断点。** 迁移给已认领或运行中的旧行补空 `attempt_id`，没有恢复规则。旧 worker 无法续报或完成，运行任务超时也只会标陈旧。应定义旧状态如何排空或显式重排，并以旧库 fixture 验证；安装 Skill 的首条进度示例也用了 `note`，工具实际必需 `progress_note`，照例调用会失败。 [迁移](../consilium/store.py#L220) · [进度校验](../consilium/store.py#L1584) · [完成校验](../consilium/store.py#L1711) · [重排](../consilium/store.py#L1420) · [Skill](../adapters/skill/consilium-collab/SKILL.md#L53) · [工具参数](../consilium/mcp_server.py#L282) |
| 提交后异常、幂等与通知（R010 #3、#7、#15） | **提交结果隔离与冲突校验成立。** `.seq`、响铃、回执写库和 SSE 广播的常见故障进入警告。幂等表在业务事务内绑定操作、调用方范围及请求哈希，支持 HTTP 409 与 MCP 工具错误。**[P2] 重放仍没有原交付回执。** 表中保存的是业务结果，多个接口另造 `delivery: replayed`，首笔通知失败或警告在调用方丢失原响应后不可从重试得到。R010 #7 与 OpenSpec 的原回执承诺仍未满足。直接冷通知已区分系统命令结果与对端 ack；热转冷的结果仍只有可选回调，常规分派未接回持久层。`claim_latency_ms` 有独立的派发到认领记录；`wakeup_latency_ms` 按序号上界挑一条未确认事件，入库时间也会被当响铃时间，适合视为近似值。 [幂等落库](../consilium/store.py#L388) · [MCP 重放](../consilium/mcp_server.py#L635) · [HTTP 重放](../consilium/mcp_server.py#L1849) · [冷补发](../consilium/wakeup.py#L1019) · [延迟记录](../consilium/store.py#L957) · [回执契约](../openspec/changes/consilium-v1/specs/bus/spec.md#L38) |
| wait 与取消（R010 #8） | **R003 的失联后误认领路径已关闭。** MCP 和 CLI 都忽略历史自动认领选项，等待不 ack 或领取，须另行 pickup。`notifications/cancelled` 仍被忽略，stdio 仍同步阻塞，HTTP 断开也未取消当次 wait。**[P2] MCP 热唤醒会写 SQLite。** kqueue、socket 等分支调用 `record_peer_wakeup_latency` 更新事件，违反冻结契约的「不写 `bus.db`」。只读回归先派发再 wait，命中预检短路，仅断言 queued 与 ack。若保留观测，应移到独立路径，或经用户确认后校正文档口径。 [通知分派](../consilium/mcp_server.py#L461) · [MCP wait](../consilium/mcp_server.py#L903) · [SQLite 写入](../consilium/store.py#L1009) · [回归](../tests/test_r010_regressions.py#L1094) · [只读契约](../openspec/changes/consilium-v1/specs/bus/spec.md#L46) |
| HTTP 与会话（R010 #5、#6、#9、#10） | **常规路径成立。** 默认 Bearer、移除 URL 查询鉴权、MCP 拒绝仅有 Cookie、Cookie 写入校验完整 Origin/JSON/会话 CSRF、随机会话与单次票据、安全头均有实现和回归。轮换顺序路径可废止原票、原会话和已建的流；并发例外见 §1。R010 §2.3 把登出说得过宽，登出仅撤销当前 Cookie 会话及其流，独立 Bearer 票据仍按签发版本和有效期管理。 [Cookie 写检查](../consilium/mcp_server.py#L1355) · [响应头](../consilium/mcp_server.py#L1379) · [票据](../consilium/dashboard.py#L105) · [登出](../consilium/mcp_server.py#L1709) |
| SSE（R010 #11） | **部分成立。** 正常广播的编号及入队受同一发布锁控制，16 连接上限、10 秒写超时、前端轮询和重连均已接通。**[P2] 单调编号仍可能标到旧快照。** 空闲分支在发布锁外取缓存快照，之后才在锁内发新编号。并发正常更新可以先入队，旧快照反而拿到更大编号，随后丢掉已排队的新内容。另有连接容量缺口，订阅后写响应头发生异常时尚未进入 `try/finally`，订阅槽可能残留。用同一发布顺序生成快照与版本，清理覆盖订阅起始至流结束。 [广播顺序](../consilium/dashboard.py#L218) · [空闲分支](../consilium/mcp_server.py#L1536) · [订阅与清理边界](../consilium/mcp_server.py#L1464) · [前端重连](../consilium/dashboard.py#L2032) |
| 安装与轮换（R010 #13） | **部分成立。** 按目标安装、Claude.app 前置检查、`CLAUDE_CONFIG_DIR` 隔离，以及 add 返回非零时的恢复核验均已落实。add 在进程创建时抛异常仍绕过回滚，旧条目可能已删除，失败目标也未计为可能改动。普通安装第二阶段串行执行，无整批回滚，TOML 预检是常见重复表匹配。结构化 `TokenRotationError` 只用于轮换，轮换还会略过未纳入管理块的手工 Codex 条目。应明确失败目标的实际状态与安全恢复路径，并缩小「所有已配置客户端」的报告口径。 [Claude 更新](../scripts/install.py#L342) · [TOML 预检](../scripts/install.py#L232) · [安装执行](../scripts/install.py#L742) · [轮换范围](../scripts/install.py#L821) |

### R011 的四个 P3

| 项 | 当前判断 |
|---|---|
| P3-1 非数字 Origin 端口 | **已修。** 两个 Origin 校验路径都捕获解析异常，返回 403。 [代码](../consilium/mcp_server.py#L1109) · [回归](../tests/test_r011_regressions.py#L53) |
| P3-2 早退污染 keep-alive | **错误响应已修，成功早退仍需处理。** `400+` 会关闭连接。成功的票据签发与登出仍未读请求体且可保留连接，前端登出确实带 JSON 请求体。后续复用连接时可能把剩余字节当作下一条请求。请统一消耗请求体或关闭连接，并覆盖成功和拒绝两组。 [错误处理](../consilium/mcp_server.py#L1379) · [成功早退](../consilium/mcp_server.py#L1696) · [浏览器登出](../consilium/dashboard.py#L2390) · [RFC 9112 §9.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-9.3) |
| P3-3 长路径测试残留 | **隔离接口已修。** 回退目录可通过 `CONSILIUM_SOCKET_FALLBACK_DIR` 指向临时路径，两处回归都使用临时目录。 [实现](../consilium/wakeup.py#L36) · [回归](../tests/test_r011_regressions.py#L185) |
| P3-4 `localhost` 与 `127.0.0.1` | **文案整改成立。** 登录区域和 README 指明回环数字地址，Cookie 写仍要求精确匹配。 [登录页](../consilium/dashboard.py) · [README](../README.md) |

## 3. R013 的 Q4 与对话流技术核对

- **[P2] 单一候选会被误认成当前账号。** 已登记并明确指向当前的正常场景，主配额独立聚合，其他账号不会混入。缺少当前标记时，只有一个候选的分支却直接设为 `is_current`，旧测试还把一个单独的 Antigravity 历史样本断言为可派发。注册表缺失或读取失败时也沿用旧的当前标记。30 分钟判定只覆盖部分多账号现用场景，备用号筛选不用样本年龄。应区分确知当前、单候选推断和读取失败，未确认的当前配额返回未知，过期备用样本不应产生切换建议。 [注册表同步](../consilium/quota.py#L543) · [当前账号与备用筛选](../consilium/store.py#L2491) · [单样本回归](../tests/test_quota_extraction.py#L134) · [多账号回归](../tests/test_r011_regressions.py#L297)
- **Cockpit 白名单成立，脱敏和历史展示的口径偏宽。** 采集只选三个固定输入，SQLite 查询用 `mode=ro`。实际只哈希含 `@` 的账号标识，其他 ID 可原样进入 SQLite 和 API，旧库既有记录也无转换。历史样本从池中排除，接口只报历史账号数，UI 未逐项标注。若要求对源目录零写入，`mode=ro` 下 WAL 附件仍可能创建，应单独验证当前 Cockpit 数据库。 [标识处理](../consilium/quota.py#L27) · [只读连接](../consilium/quota.py#L164) · [历史过滤](../consilium/store.py#L2409) · [API 字段](../consilium/store.py#L2577) · [SQLite WAL 说明](https://www.sqlite.org/wal.html#read_only_databases)
- **SSE 观察的是总线配额缓存。** 空闲 SSE 刷新不重新采集 Cockpit，采集由启动、状态查询或配额命令触发。指纹还计算配额样本年龄与重置倒计时，单纯时间流逝也会触发 `external_change`、反复渲染整页。应区分采集刷新、源状态变更与显示用计时。 [缓存取数](../consilium/mcp_server.py#L1206) · [指纹](../consilium/dashboard.py#L269) · [配额时间字段](../consilium/store.py#L2219)
- **对话流未新增派活写入口。** 基于固定 scheme 的深链、动态文本转义、纯文本产物路径和离线资源边界均有源码依据。已有关闭/重排继续走原写接口。R013 所称的模型上报绑定与进度正文回填需收窄，代码展示建议档位，关联任务文本回填只覆盖派发。剪贴板权限不足或写入失败时仍显示成功，影响交接可用性。前端回归主要检查字符串与 Node 语法。 [渲染与回填](../consilium/dashboard.py#L1699) · [剪贴板](../consilium/dashboard.py#L2166) · [现有写调用](../consilium/dashboard.py#L2308) · [前端回归](../tests/test_r011_regressions.py#L382)

## 4. 核销与实测范围

先核销两个 P1。对应的验收证据应覆盖任一轮换步骤失败时的诊断脱敏与失败目标状态，以及并发轮换时旧授权无法签出新票、注册新流或发送撤销后的初始快照。其余 P2 可用旧库、真实热唤醒分支、并发快照和配额 fixture 核对。 [关口与阶段](../openspec/changes/consilium-v1/tasks.md#L19)

本轮仅执行上述隔离测试。会切换前台、新建会话或消耗额度的三端 MCP、原生深链和通知试跑，使用真实 Cockpit 数据的验证，以及真实安装、令牌轮换和 `launchctl` 操作，交由用户分别决定。
