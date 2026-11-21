# R009 Codex 独立复核 R006 与 R007

2026-11-19 · R006 `a083ba3` · R007 `3ad2df7`

**关口 1 暂缓通过。** R006 修正了默认等待、正常发布路径的 `.seq` 旧值覆盖、安装产物的令牌传递和深链文案。回收后的旧会话仍能更新任务，数据库提交后仍可能返回工具错误，安装器的第二阶段仍可能部分写入。**关口 2 的外观由用户确认。** 看板的同主机跨端口写请求校验、令牌轮换后的 SSE 与待兑票据、并发快照顺序均有缺口。

先读两个提交的实现及测试，再在 `3ad2df7` 的临时快照里运行 `env -u CLAUDE_CONFIG_DIR DESKBELL_DRY_RUN=1 /usr/bin/python3 -B -m unittest discover -s tests`。系统沙箱保护真实家目录、源码快照和默认会合目录，并阻止 IP 外连。系统 Python 3.9.6 的结果为 **40 项通过（3.197 秒）**。之后阅读 R005–R008 并逐项对照，另只读核对浅色截图、录屏元数据及演示仓库状态。演示仓库干净，只有 `main` 和原工作树。R008 的历史实时配置观察、浏览器免密登录和 375 px 实测沿用该报告的记录，本次未独立复跑。复核期间仓库迁到 `<repo-root>`；下文链接指向迁移后的同名实现，判断和测试以指定提交为准。 [R005](./R005-claude-recheck.md) · [R006](./R006-antigravity-r005-fixes.md) · [R007](./R007-antigravity-dashboard.md) · [R008](./R008-claude-r006-r007-review.md)

## 1. R005 七项与 R008 §2

| R005 | 对 R008 的判断 | 实现与验收边界 |
|---|---|---|
| 3.1 默认 wait 不认领 | **成立，限默认路径。** | MCP schema 与实际分派都默认 `auto_pickup=False`，满足 R005 缩小默认风险的要求。回归在 wait 已经超时后才派任务，并核对默认等待时任务保持 `queued`。显式自动认领和取消的组合见 §3。 [schema](../consilium/mcp_server.py#L207) · [分派](../consilium/mcp_server.py#L637) · [回归](../tests/test_r006_regressions.py#L53) |
| 3.2 `.seq` 加锁、重建和重挂 | **核心修法成立，恢复和覆盖范围部分通过。** | 每端的线程锁及 `flock` 都持有期间，`write_peer_seq` 才调用 `query_fn` 查取 SQLite 的最大事件 ID、未读和待领取任务数，随后在锁内原子替换文件。这消除了同一锁文件上晚到发布者沿用旧计数的竞态。stdio 与 daemon 启动均重建，kqueue 监听删除和重命名并重挂。测试用同进程多线程写入，另验了 stdio 重建和等待中删除单个 `.sig`。R005 要求的双进程和清理整个信号目录的并发情形仍待验证。两个计数查询各自读取 SQLite 快照，目录重建也会换掉锁文件 inode。见 §4。 [锁与替换](../consilium/wakeup.py#L95) · [锁内查库](../consilium/store.py#L201) · [重挂](../consilium/wakeup.py#L618) · [回归](../tests/test_r006_regressions.py#L117) |
| 3.3 令牌传递及轮换 | **安装产物整改成立，运行时保证需收窄。** | Antigravity 模板和安装器改用头部，隔离产物测试覆盖此项。R006 的客户端 schema 调查与真实客户端连线是两种证据。Claude Code 的 `add-json` 仍将明文令牌放入子进程参数，代码无法限定其观察者。daemon 会检测令牌文件替换，新请求使用新值。但轮换逐个写配置后才换服务端令牌，后续目标失败可留下部分客户端已更新的状态，运行中的客户端也需自行重读配置。R008 的 P2-1 有依据，移除 URL 兼容入口时应同步核对现有配置与 CLI 帮助。 [安装](../scripts/install.py#L428) · [轮换](../scripts/install.py#L616) · [服务端重读](../consilium/mcp_server.py#L876) · [兼容入口](../consilium/mcp_server.py#L953) |
| 3.4 认领回收 | **后台线程已接通，完整行为部分通过。** | daemon 启停回收线程，按 `claimed_at` 或运行中 `updated_at` 回收，默认 15 分钟、2 小时，回到 `queued` 并给协调者写事件。回归直接调用 `run_reaper_once()`，验证存储与事件，未覆盖线程调度或真实送达。MCP pickup 前的回收走同一存储函数，却未传 daemon 的 CLI 阈值，也不处理返回的通知对象，能提前回收并让协调者少一次唤醒。R008 的 P2-2 对重复劳动的判断成立。仅标陈旧后人工重排会改变现有 OpenSpec 的自动回收策略，仍需处理旧认领写入，见 §4。固定每 30 分钟报进度缺少必要性依据，心跳应与有效超时及宿主能力相匹配。 [启动](../consilium/mcp_server.py#L1395) · [回收线程](../consilium/mcp_server.py#L896) · [pickup](../consilium/mcp_server.py#L508) · [存储](../consilium/store.py#L897) · [回归](../tests/test_r006_regressions.py#L268) |
| 3.5 写库、交付、幂等 | **部分成立。** | `_safe_ring_peer` 把响铃异常变为交付结果。六个写操作在同一个 `BEGIN IMMEDIATE` 事务中检查和保存可选幂等键及业务结果。相同键和 operation 会返回首笔结果，operation 不同会报冲突。相同 operation 换参数、资源或 peer 仍静默重放旧结果；漏传键或换键也不去重。R008 的 P3 判断成立，重放既不补发通知，也不带第一次的交付回执。提交后的 `.seq` 和交付记录仍会抛错，见 §3。回归覆盖模拟响铃异常和顺序重放，未覆盖这些故障。 [事务](../consilium/store.py#L177) · [幂等检查](../consilium/store.py#L276) · [分派](../consilium/mcp_server.py#L455) · [回归](../tests/test_r006_regressions.py#L303) |
| 3.6 冷通知、深链和 Skill | **源码与文案验收成立。** | Codex 生成 `prompt` 加编码后的绝对 `path`；Claude 有无目录均走 `claude://code/new`；截断发生在编码前。Skill 已要求用户手动切到 App、由活会话 pickup，显式开链接需同意。默认系统通知仍只提示，链接构造和 dry-run 测试仅能证明输入参数与分支，原生接收和焦点效果见 §6。 [构造与交付](../consilium/wakeup.py#L861) · [Skill](../adapters/skill/consilium-collab/SKILL.md) · [深链测试](../tests/test_wakeup_and_cold_fallback.py) |
| 3.7 两阶段安装器 | **预检整改成立，跨目标原子性仍未解决。** | 第一阶段先规划、解析 JSON、检测常见重复 TOML 表、检查 CLI 和 Claude.app；第二阶段串行执行写入。`--rendezvous-dir` 已可显式隔离；`--target-home` 单独使用仍可指向默认会合目录。CLI 子进程设置目标 `HOME` 并剥离 `CLAUDE_CONFIG_DIR`。预检的 TOML 文本检测无法代替完整解析，阶段二无全局回滚或并发变更检查。`claude mcp get` 的返回未用来保存旧条目，恢复使用预读 JSON，只覆盖特定失败分支，恢复命令结果也未校验。三项回归确认已知重复表、正在运行时的前置拒绝、模拟 add 失败后的旧条目恢复。R008 的 P2-3 是可用性建议；整体应用前阻挡 Claude.app 是 R005 的明确要求，若增加按目标安装，应保留所选目标的安全检查。 [CLI 写入](../scripts/install.py#L236) · [预检与计划](../scripts/install.py#L347) · [执行](../scripts/install.py#L590) · [回归](../tests/test_r006_regressions.py#L403) |

## 2. 看板安全与 R008 §3

| 项目 | 独立判断 |
|---|---|
| 未登录访问、Host 和 Origin | 默认要求令牌，`/api/state` 未认证返回 401，明显外部域名的 Origin 返回 403，R008 两项成立。**[P1] 写接口的 Origin 只核对主机名，同一主机上的其他端口仍可通过，scheme 和 port 均未比较。** 两个写入口接受浏览器自动附带的 cookie，JSON 读取不核对媒体类型，二次确认只在前端执行。`SameSite=Strict` 不按端口隔离，外域测试不足以证明本机写接口抗 CSRF。建议对 cookie 写请求校验完整可信源，并明确 JSON 或 CSRF 凭据约束。原生 Bearer 客户端可沿用独立的认证规则。看板凭据具备关闭和重排队权限，默认回环监听缩小了暴露面。 [Host 与 Origin](../consilium/mcp_server.py#L795) · [请求体](../consilium/mcp_server.py#L1019) · [写入口](../consilium/mcp_server.py#L1263) · [RFC 6265 §8.5](https://www.rfc-editor.org/rfc/rfc6265.html#section-8.5) |
| 一次性票据与免密登录 | **正常流程与重放结论成立。** 票据随机，默认 30 秒，在锁内取出即删；登录重定向至 `/ui`，带 `no-store`、`no-referrer`，旧票据再用不签发 cookie。重定向清理的是落地 URL。**轮换例外**是未兑换的票据未绑定签发时的令牌版本，轮换后在剩余有效时间内仍能兑换出新 cookie。将票据绑定认证版本或轮换时作废。票据接口排除了 cookie，但复用 Bearer 或旧查询令牌的检查，显式无令牌模式也适用；因此 R007 的“仅 Bearer”需收窄。 [票据存储](../consilium/dashboard.py#L47) · [兑换](../consilium/mcp_server.py#L1133) · [签发](../consilium/mcp_server.py#L1237) · [回归](../tests/test_dashboard_and_sse.py) |
| 会话 cookie、登出和 MCP 边界 | 默认认证模式下 `/mcp` 拒绝仅持看板 cookie 的请求，仍接受旧查询令牌；R008 的隔离判断成立，“只认 Bearer”不精确。看板 cookie 有 `HttpOnly; SameSite=Strict; Path=/`，按当前令牌派生，全体登录共享一个值，服务端没有独立会话、到期或单会话撤销。登出仅清除浏览器 cookie，轮换使旧值在后续重新认证时失效。Cookie 同样不按端口隔离。随机服务端会话可支持独立撤销，有效期应按使用场景确定。 [派生](../consilium/dashboard.py#L19) · [分路鉴权](../consilium/mcp_server.py#L953) · [登录与登出](../consilium/mcp_server.py#L1210) · [MCP](../consilium/mcp_server.py#L1360) |
| 模板、Markdown 与 CSP | **主要文本路径正确，R007 响应头承诺未实现齐全。** 标题、进度、汇总等文本进入 HTML 前转义，Markdown 先转义再套有限格式；数值插值依赖存储结构，R008 的“统一用 `esc()`”说得过满。源码复核未发现上述字段能直接成为脚本。HTML 无外部资源，实际 CSP 有 `default-src 'none'` 和 `script-src 'unsafe-inline'`，可以再用 nonce 或 hash 约束内联脚本。R007 声称的 `frame-ancestors 'none'` 与 `X-Content-Type-Options: nosniff` 两项均缺失；`frame-ancestors` 无 `default-src` 回退。 [模板](../consilium/dashboard.py#L1170) · [HTML 响应头](../consilium/mcp_server.py#L1004) · [CSP3](https://www.w3.org/TR/CSP3/#directive-frame-ancestors) |
| SSE 鉴权和资源 | **进入流之前鉴权，整个连接期间缺少撤销。** 认证通过后，循环只检查 daemon 停止状态，已建立的流在轮换或其他标签页登出后仍可收到之后的状态。R008 的“鉴权正确”只适用于建连。建议按认证版本或会话撤销结束既有流。R008 对队列和慢客户端的判断成立。每订阅者 128 项，满时丢队头。连接数没有上限，`settimeout(None)` 可使慢消费者占住线程，空闲时每条流还会重复构建快照。连接和写入预算宜按部署需求定。 [建连检查](../consilium/mcp_server.py#L1174) · [流循环](../consilium/mcp_server.py#L1047) · [订阅队列](../consilium/dashboard.py#L82) |

## 3. R003 的两类遗留风险

1. **取消传播仍缺失。** 分派器忽略 `notifications/cancelled`，stdio 的同步循环在 wait 期间无法读取下一条通知，HTTP worker 等待时没有断连取消。默认等待不会替失联客户端 ack 或认领，仍可能暂留 waiter/lease 直到唤醒或到期。显式 `auto_pickup=True` 或后台 CLI `--pickup` 仍能在消费方退出或放弃结果后认领，前提是执行等待的进程或请求线程仍活着。R005 有意把取消推迟，实际风险尚在。可让等待始终只读，或将取消状态传到认领的提交点。MCP 允许发送取消，收到后建议停止相应工作，单凭支持范围不能判定协议违规。 [R003 §4](./R003-codex-review.md#4-r002-漏掉的实质问题) · [通知分派](../consilium/mcp_server.py#L323) · [stdio](../consilium/mcp_server.py#L762) · [HTTP](../consilium/mcp_server.py#L1360) · [实际认领](../consilium/wakeup.py#L787) · [MCP 取消规范](https://modelcontextprotocol.io/specification/2025-03-26/basic/utilities/cancellation)
2. **[P1] 已提交仍可能报错，且交付证据不完整。** 业务事务退出后，`_sync_peers_seq` 可因文件 I/O 失败抛错，响铃后的 `record_event_delivery` 是另一个可失败的 SQLite 事务。这两处超出 `_safe_ring_peer` 的保护范围，MCP 会把已完成的业务返回为错误。看板关闭和重排队也有类似路径，且接口无幂等键。用相同幂等键重试只恢复业务结果，无法补齐已漏写的 `.seq` 或交付回执。应以已提交结果为权威，单独记录可修复的同步和通知失败，并把幂等键绑定到请求内容与调用方范围。 [提交后同步](../consilium/store.py#L571) · [回执事务](../consilium/store.py#L727) · [MCP 写路径](../consilium/mcp_server.py#L480) · [看板写路径](../consilium/mcp_server.py#L1275)
3. **冷通知只提示的产品语义已说明，时间线仍可能高估交付。** 系统命令非零退出时，返回对象可带 `notified=False` 或 `opened=False`，事件记录仅保存交付类型和部分元数据，`冷通知` 标签不足以说明系统已成功提交通知。热路径的唤醒计数也只证明本地信号或 lease 的状态。补发依赖进程内守护定时器，退出后丢失；常规分派未接收补发的实际结果。建议记录尝试、系统命令结果、对端确认三个独立事实。 [冷通知结果](../consilium/wakeup.py#L899) · [升级补发](../consilium/wakeup.py#L955) · [回执字段](../consilium/store.py#L752) · [时间线](../consilium/dashboard.py#L1381)

## 4. R008 漏项及实际影响

除 §2 的同主机 CSRF、票据与流撤销，以及 §3 的提交后失败和取消路径外，还需处理以下问题。

1. **[P1] 超时或人工重排队后，旧认领仍能写。** `report_progress`、`complete_task` 检查分配到的 peer 和任务是否终态，未校验当前的认领代次或会话。旧会话能在任务刚回到队列时写入，也可能覆盖同一 peer 新会话的尝试。先明确每次认领的身份，在进度和完成事务内核对；仅改成手动重排队仍有同一缺口。自动回收当前只给协调者发事件，assignee 与协调者分属两端且 DB-free waiter 已入睡时，同步 assignee 的 `.seq` 无脉冲，回队积压可能要等下一次信号。 [回收与同步](../consilium/store.py#L943) · [进度](../consilium/store.py#L1096) · [完成](../consilium/store.py#L1186)
2. **[P2] `.seq` 是可重建投影，仍会出现失配。** 锁文件随临时信号目录删除时，旧文件描述符和重建的锁文件可以分别持锁。常规锁内查询的正确性依赖这把锁保持同一 inode。另有正常写路径遗漏：`report_progress` 可以改变 `queued` 任务的状态，却只在发 progress 事件时刷新协调者，assignee 的待领取数可能继续过时。应让队列相关写路径统一刷新受影响 peer，并为临时目录丢失及重新发布设计恢复触发。lease 也有创建到上锁之间的窗口，扫描者可能先锁住并删掉尚在初始化的文件，使活跃 waiter 变得不可见。 [进度同步](../consilium/store.py#L1118) · [锁位置](../consilium/wakeup.py#L76) · [lease 注册与扫描](../consilium/wakeup.py#L231)
3. **[P2] SSE 快照可倒序、漏更新，断线回退也不完整。** `EventHub` 在锁内取序号、锁外入队，并发广播可反序到达。初始和外部帧用数据库事件 ID，本地帧用 hub 计数器，前端每收到一帧就直接渲染，迟到的旧快照可能覆盖新状态。外部检测只比较最大事件 ID 和 waiter 计数，stdio 的静默进度更新等可以漏掉。R007 所说的降级后定期重连没有实现。`EventSource` 遇到 401 等终止条件可仅报一次错，现有三次阈值也可能一直停在「重连中」。应使用统一版本、拒绝旧快照、追踪所有可见状态变动，并从关闭状态回到认证检查及轮询/重连。双帧顺序测试是串行发布。 [Hub](../consilium/dashboard.py#L105) · [帧与外部检测](../consilium/mcp_server.py#L1068) · [前端重连](../consilium/dashboard.py#L1551) · [SSE 测试](../tests/test_dashboard_and_sse.py#L188) · [HTML EventSource 规范](https://html.spec.whatwg.org/multipage/server-sent-events.html#processing-model)
4. **[P2] 手动重排队的状态范围宽于 R007。** 在未关闭的 mission 中，服务端和按钮对多种非 `queued` 状态开放，包含已完成或已取消任务；重新入队保留上一轮的汇总、产物和进度，看板可能继续优先显示旧结果。需要明确终态任务是否允许重开，并区分不同执行尝试的结果。 [存储状态检查](../consilium/store.py#L998) · [按钮及结果展示](../consilium/dashboard.py#L1353)
5. **[P3] R008 的延迟口径质疑成立，测量本身还缺少因果关联。** 目前只有 MCP 的部分唤醒分支记样本。原始时间戳在事件入库时填入，响铃记录又可能回填；一次唤醒会为该 peer 最近若干条未确认事件补上延迟，无法证明是哪条事件触发这次唤醒。看板数值不能可靠代表通知到 wait 返回，更不能代表认领。先给样本关联具体事件和尝试，再单独计算派发到认领。若补 CLI 观测，保留后台 wait 的零数据库写入契约。 [事件时间戳](../consilium/store.py#L320) · [延迟计算](../consilium/store.py#L766) · [MCP 记录点](../consilium/mcp_server.py#L637)

## 5. R008 §4–6 的交付与产品意见

| R008 项 | 判断 |
|---|---|
| A1 工作中 | **成立。** `active` 被映射成「工作中」，浅色截图里 ChatGPT/Codex 已出现名下 claimed/running 为零、卡片仍蓝的情形。待命可依 lease，工作中可依当前任务，近期活跃与从未出现/陈旧应有各自语义。 [状态标签](../consilium/dashboard.py#L1136) · [卡片](../consilium/dashboard.py#L1283) · `浅色截图`（早期外部演示归档） |
| A2 看板拥挤 | **截图支持。** 左右分面内再分四列，长词换行与空列同时出现。整行展示或折叠空列是布局取舍。 [布局](../consilium/dashboard.py#L769) · `浅色截图`（早期外部演示归档） |
| A3 时间线截断 | **截图支持，原因是滚动区域。** 第 3 条只露出部分卡片，容器有最大高度和纵向滚动。可改布局或提示后续内容。 [时间线样式](../consilium/dashboard.py#L906) · `浅色截图`（早期外部演示归档） |
| A4 录屏 | **元数据成立。** 原件 30 帧、4 fps、7.5 秒。若要重录，验收应能看清状态变化和轮次更新；R008 给出的停顿、帧率、总长可作为拍摄建议。 `录屏`（早期外部演示归档） |
| B5 关闭按钮 | **描述有偏差。** 当前按钮是红字、描边、浅色危险背景，确认前再降低视觉权重是外观选择。 [样式](../consilium/dashboard.py#L433) |
| B6 进度与配额颜色 | **部分成立。** 100% 仍蓝。进度已结算的任务包含失败、阻塞、取消，若用绿色应避免读成全成功。配额已区分低量和耗尽，20% 属交互选择。 [配额显示](../consilium/dashboard.py#L1283) · [进度计算](../consilium/dashboard.py#L1431) |
| B7 分支前缀 | **描述成立。** 当前副标题是分支前缀；移动到提示并以应用名作主文案是信息层级选择。 [卡片](../consilium/dashboard.py#L1283) |
| B8 进度行内代码 | **成立。** 进度使用纯文本转义；复用汇总的行内渲染时继续先转义。 [Markdown](../consilium/dashboard.py#L1206) · [任务卡](../consilium/dashboard.py#L1353) |
| B9 演示数据 | **成立。** 演示脚本用 `--seed-demo-ui` 写入 78% 和 44%，截图中无演示标签。可标在演示画面或其图注。 `演示脚本`（早期外部演示归档） · `浅色截图`（早期外部演示归档） |
| B10 免密打开提示 | **两处文案已经实现。** CLI 在 daemon 不可达时提示启动，登录框也提示免密命令。实际遗漏是浏览器启动命令以非零退出时，CLI 仍有可能报告成功。 [CLI](../consilium/cli.py#L189) · [登录框](../consilium/dashboard.py#L1103) |

R008 §5 的菜单栏方案有待验证。给 WKWebView 首次请求带令牌不足以覆盖后续 fetch 与 EventSource，仍需会话设计；可点击通知、权限、签名和 daemon 管理各有实际宿主行为。单向本地历史检索是可评估的方向，记录格式、覆盖率和隐私边界要单独确认，本次未读取实时会话内容。深链、总线排队和认领还不能承诺消息自动进入已有原生会话。R008 记载的「只要原生 App」按历史用户决定理解；待命能力、额度消耗、群聊消息身份和会话映射仍待验证，一小时测试和固定循环间隔缺少当前必要性。现有 mission/event 可以作为传输基础。R008 §6 的后续开发排序交由用户决定，本次仅交付复核。 [R008 §5–6](./R008-claude-r006-r007-review.md#5-新产品方向评估)

## 6. 会产生副作用的实测，待用户选择

| 可选实测 | 副作用及能确认的结论 |
|---|---|
| 临时快照中做双进程 `.seq`、回收线程和安装/轮换故障注入 | 使用独立临时目录、进程和 fixture，验证跨进程重建、真实调度、晚到写回和中途失败时的恢复；触及的配置限临时目标。 |
| 独立浏览器资料与回环临时 daemon | 创建临时进程、cookie、数据库和日志，验证登录、可信源拒绝、轮换及登出、旧流终止和离线恢复，也可检查窄屏；回到页面的状态查询会触发本地配额同步，隔离时应禁用。 |
| 三端原生 App 的候选实测 | Deep link、通知及 MCP 等待可能切换前台、新建会话、消耗工具调用或额度。可分别验证链接落点、等待取消、真实认领时间及冷通知交付。 |
| launchd 或正式安装 | 临时 launchd 项也会改变系统任务状态；正式安装、卸载或轮换会写配置或令牌。两类验证分别由用户授权。 |

本轮实际执行的是上文的隔离测试；表中各项未执行。
