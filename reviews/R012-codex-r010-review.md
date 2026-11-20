# R012 Codex 独立复核 · R010 与关口 1

2026-11-20 · R010 交付 `de2883b` · 复核快照 `f42963a`

**★ 关口 1 暂缓通过。** R010 的 59 项隔离测试与 OpenSpec 严格校验通过。两个阻塞项是轮换失败诊断带出仍有效的旧 Bearer，以及并发轮换时旧 Bearer 的认证结果可能被用于签发新版本票据或建立新版本 SSE 订阅。R011 的顺序轮换验证覆盖常规撤销，两个失败边界均缺少对应断言。

本次固定 R010 的实现 `ce2c882`、回归 `9ced8af` 与文档 `10939b2`、`de2883b`。`f42963a` 相对 `de2883b` 只增加 R011 报告和索引，代码、测试与 OpenSpec 相同。下文源码行号以 `f42963a` 为准。后续 R013 代码和 65 项测试另见同编号的当前版本复核。 [R008](./R008-claude-r006-r007-review.md) · [R009](./R009-codex-review.md) · [R010](./R010-antigravity-r009-fixes.md) · [R011](./R011-claude-r010-review-and-chat-ui.md) · [R010/R013 当前版本复核](./R012-codex-review.md)

## 1. 隔离验证

在 `git archive f42963a` 解出的临时快照上执行以下两条命令。系统 Python 3.9.6，**59 项通过，7.295 秒**。OpenSpec 返回 `Change 'consilium-v1' is valid`，两条命令均退出 0。

```sh
env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 /usr/bin/python3 -B -m unittest discover -s tests
openspec validate consilium-v1 --strict
```

测试进程使用临时 `HOME`、配额来源、状态及会合目录。系统沙箱保护了真实家目录和源码，并禁止读取真实 `~/.codex/sessions`，IP 连接只允许回环。13 个实时路径抽查的元数据前后一致，覆盖 Claude、Codex、Antigravity 配置、Git 配置和 Consilium 的 LaunchAgent、状态、日志等路径。长路径测试确实硬编码 `/tmp/consilium-<uid>/sock`。本轮在该路径原本不存在时为测试预建空目录并阻止子进程写入，结束后仅清理本轮创建的空目录，固定长路径 fixture 也已移除。R011 P3-3 对裸跑 R010 留残的判断成立。 [R010 验证日志](./R010-antigravity-r009-fixes.md#4-全套测试与-openspec-验证输出) · [R011 §3](./R011-claude-r010-review-and-chat-ui.md#3-115-项逐项核对)

## 2. 阻塞证据

1. **[P1] 轮换失败报告泄露有效令牌。** Claude Code 目标已更新而后续目标失败时，`rotate_token` 将包含旧 Authorization 的完整条目嵌进 `rollback_instructions`。服务端令牌最后替换，这时旧值仍可能有效。CLI 原样将报告输出到 stderr，诊断或日志随之携带凭据。R010 回归模拟的是 Claude 步骤本身失败，没有覆盖之后的失败。回滚说明应只引用受保护的备份路径和目标，诊断内容统一脱敏，同时明确失败目标是否已经部分改动。源码 `scripts/install.py:863–925`、`consilium/cli.py:182–193`，回归 `tests/test_r010_regressions.py:1620`。
2. **[P1] 轮换认证版本在签票和 SSE 注册时被重新读取。** Bearer 的认证检查返回认证模式，未传递已验证的令牌版本。`/api/ui-ticket` 与 `/api/stream` 随后各自取当前版本。若两次操作之间发生轮换，旧 Bearer 的请求可能被登记为新版本，从而取得可兑换新会话的票据，或在 SSE 循环中被继续当作有效。Cookie 流也先发初始快照，再在循环中检查会话是否仍有效。验收应覆盖使用同一认证版本完成签发、订阅和首帧校验，以及先于这些动作发生的撤销。源码 `consilium/mcp_server.py:1316–1345, 1453–1502, 1685–1695`，顺序撤销回归 `tests/test_r010_regressions.py:853`。

## 3. R008 与 R009 整改逐项核对

编号对应 R010 §2.1。各项附有本轮全量测试中相应的 `test_r010_*` 回归编号。 [R010 对照表](./R010-antigravity-r009-fixes.md#2-r008--r009-逐项整改对照表)

| 项 | 独立判定 |
|---|---|
| 1 · `.seq` 锁与重建 `01a/01b` | **核心修复成立。** 锁文件在 `signals/` 之外，锁内用一个 SQLite 读事务计算计数后原子替换。双进程与删 `signals/` 后再派发有回归。若清掉整个会合根，锁仍会换 inode。仅启动重建时 `pulse=False`，已有积压且 waiter 已重挂时仍缺唤醒保证。`consilium/store.py:232–287`、`consilium/wakeup.py:80–114, 240–245, 668–702`。 |
| 2 · 超时回收 `02` | **正常调度成立。** 实际后台线程覆盖 `claimed` 自动回队和执行者唤醒。`running` 只记陈旧并通知两端，保留认领代次。HTTP pickup 用本 daemon 的阈值，独立 stdio 的阈值仍取自身环境和默认值。共享同库采用 CLI 单独覆盖时需注意两者生效范围。`consilium/store.py:1153–1358`、`consilium/mcp_server.py:679–695, 1277–1293`。 |
| 3 · 提交与交付分离 `03` | **通过所列四个异常分支。** `.seq`、响铃、回执和 SSE 广播的注入异常转为 `delivery.warnings`，保留已提交结果。重放的警告和回执见 #7。`consilium/store.py:275–287`、`consilium/mcp_server.py:71–129, 434–440`。 |
| 4 · 尝试代次 `04` | **新任务路径成立。** 领取生成 ID，进度和完成校验当前代次，回收或手动重排后旧 ID 失效。**[P2]** 从旧库升级时已有 `claimed/running` 行得到空 ID，旧任务无法续报或完成，需要明确排空或恢复策略及旧库回归。Skill 的首条进度示例使用 `note`，实际必填 `progress_note`，照例会报错。`consilium/store.py:206–220, 1566–1599, 1693–1722`、`consilium/mcp_server.py:282–291, 707–717`、`adapters/skill/consilium-collab/SKILL.md:53`。 |
| 5 · Cookie 写入 `05` | **核心 CSRF 修复成立。** 默认模式验证精确同源、JSON 媒体类型及会话 CSRF。R011 P3-1 的畸形端口在 R010 仍可抛异常断连。P3-4 的两个主机名是两个来源，R010 的精确匹配要求使用 daemon 同一来源。请求分帧见 §4。`consilium/mcp_server.py:1109–1148, 1347–1369`。 |
| 6 · 会话、票据、吊销 `06` | **部分通过。** 随机会话、12h TTL、一次性版本绑定票据，以及轮换后原会话和已建立流失效有实现和顺序回归。登出只撤销对应会话及其流，独立签发的票据继续由令牌版本和有效期约束。并发轮换见 §2。`consilium/dashboard.py:34–144`、`consilium/mcp_server.py:1698–1720`。 |
| 7 · 幂等键与重放 `07` | **去重和冲突回归通过，交付回执遗漏。** 业务事务存的是业务结果及操作、调用方范围、参数指纹，HTTP 看板冲突返回 409。重放返回新造的 `delivery: replayed`，丢失原响应后取不回原来的交付结果或后置告警。与 R010 报告及 OpenSpec 的缓存回执条款不符。`consilium/store.py:329–398`、`consilium/mcp_server.py:635–642, 1838–1847`。 [回执契约](../openspec/changes/consilium-v1/specs/bus/spec.md#L38) |
| 8 · 只读 wait `08` | **误认领已修，零写库未满足。** 旧选项被忽略，CLI 不 ack 或认领。MCP 也不会自动领取，但真实热唤醒会写事件的延迟数据。回归先派发再 wait，核对 queued 和 ack，只走预检短路。取消通知仍被忽略，影响在途等待的资源回收。`consilium/mcp_server.py:461–474, 903–936`、`consilium/store.py:939–999`。 [只读契约](../openspec/changes/consilium-v1/specs/bus/spec.md#L46) |
| 9 · 移除 URL 令牌 `09` | **成立。** 默认要求 Bearer 的入口不接受查询令牌，安装产物和 CLI 帮助已同步。Cookie 只在许可的看板接口承认。`consilium/mcp_server.py:1316–1345, 1888–1896`。 |
| 10 · 安全响应头 `10` | **列举的应用路由通过。** HTML 有 CSP `frame-ancestors 'none'`，应用 JSON、跳转、SSE 与 MCP 确认响应有相应安全头。通用 HTTP 解析器自行生成的错误不在该测试覆盖范围。`consilium/mcp_server.py:1371–1410, 1466–1474, 1898–1906`。 |
| 11 · SSE `11` | **部分通过。** 常规广播有发布锁和单调版本，连接上限、写超时、静默变动检测和前端回退已接入。空闲分支先在发布锁外取快照，再发新编号，可能把较旧内容标成较新并丢掉一条实际更新。订阅之后先发送响应头，再进入 `finally`，响应头发送异常时可能占住订阅名额。`consilium/dashboard.py:218–248`、`consilium/mcp_server.py:1453–1479, 1525–1553`。 |
| 12 · 原子租约 `12` | **成立。** 临时文件持锁且写完后才发布给扫描者，回归覆盖并发与异常清理。`consilium/wakeup.py:252–288`。 |
| 13 · 安装与轮换 `13` | **部分通过。** 按目标预检、已知 TOML 重复形式拒绝、Claude add 返回非零时的恢复核验均有回归。add 如果启动失败抛异常，恢复分支尚未进入。普通安装第二阶段仍串行写，无整批回滚。轮换只识别 Codex 托管块，失败报告还有 §2 的泄露问题。`scripts/install.py:232–276, 342–410, 742–751, 821–826`。 |
| 14 · 手动重排范围 `14` | **成立。** 限定打开的 mission 与四个允许状态，结束及已排队任务被拒，重置当前写回字段并保留历史。`consilium/store.py:1360–1514`。 |
| 15a · 指标、冷通知与 UI 打开 `15` | **部分通过。** 有派发至认领延迟、直接冷通知系统结果及 ack，浏览器打开失败返回非零。热唤醒仍以序号上界选择一条事件，属于近似关联。热转冷后续通知结果只交给可选回调，常规分派未接回持久层。`consilium/store.py:939–999, 1066–1133`、`consilium/wakeup.py:1019–1034`、`consilium/cli.py:224–241`。 |
| 15b · R008 A/B 展示 `16` | **代码与交付规格相符。** 五态、空列折叠、时间线展开、关闭次要样式、进度色、低配额色、App 文案、行内代码、演示标签和登录提示有实现和断言。已读媒体元数据，两张 PNG 均为 2880×5596，MP4 为 1440×1000、10 fps、350 帧、35 秒，与 R010 所列一致。画面及实时流覆盖沿用 R010 的演示记录。`consilium/store.py:530–594`、`consilium/dashboard.py:1447–1764`。 [R010 演示](./R010-antigravity-r009-fixes.md#3-视觉与演示交付物2026-11-19-复录) |

## 4. R011 §2.3 #12 · HTTP/1.1 请求分帧

**有额外的连接错位路径。默认直连回环的源码复核未确认无凭据越权。** R010 的 `_send_json` 只根据既有的 `close_connection` 决定是否结束连接。鉴权和 CSRF 拒绝、带请求体的未知 POST 路由、部分长度错误或超限的响应，均可在消费请求体前返回，并让同一连接继续解析。这个缺陷与状态码无关，正常成功的 `/api/ui-ticket` 和 `/api/logout` 同样不读请求体。页面登出确实发送 JSON 请求体。SSE 单独声明连接关闭。源码 `consilium/mcp_server.py:1300–1314, 1371–1390, 1412–1438, 1685–1720, 1888–1896`、`consilium/dashboard.py:2063–2070`。 [RFC 9112 §9.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-9.3)

**分帧解析也需补齐。** JSON 读取只取一个 `Content-Length`，未拒绝不支持的 `Transfer-Encoding` 或歧义、重复的长度头，也未检查有效声明长度与实际读取长度相等。正长度读到空内容被当作空对象，短读恰好能解析为 JSON 时也可能被分派。默认部署每个受保护请求重新比对 Bearer 或会话与 CSRF，连接错位的直接后果是同连接后续请求失败、响应错配或执行语义失真。跨用户影响需要另有共享上游连接或代理分帧差异的前提，本次未实测。 [RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3)

建议由入口统一处理长度和编码。只有完整消费且边界明确时才复用连接，其他情况在返回响应时关闭。回归至少分开覆盖成功早退、拒绝早退以及不完整请求不会进入分派。后续 `065486e` 已对应用层 `400+` 强制关闭，成功早退和长度检查仍是独立缺口。以上基于源码与 R011 的历史实测记录。

## 5. R011 §5 Q4(1)(2) · Cockpit 与 Codex 身份

本机 `Cockpit Tools.app` 为 **1.4.2**，提供 Codex 的 `ChatGPT.app` 为 **26.1111.71704**。只核对了 bundle 版本、指定 Cockpit JSON 的键名和类型，以及两个历史库的表结构。`codex_accounts.json` 有字符串型 `current_account_id` 及 `accounts[].id`、`accounts[].email`。`quota_samples` 的组合主键是 `(account_id, identity, minute)`，没有外键。历史库只以禁止文件写入的方式读 schema，未查询样本行、邮箱或账号值。默认目录还存在 `codex_accounts.sqlite`，仅核对了文件类型。

1. **本机此刻两者是否一致，结构证据无法判定。** Cockpit v1.4.2 的默认 profile 切换先写目标登录配置，再更新 Cockpit 索引中的当前引用。它的 OAuth 当前账号查询依据的是这个引用。登录或实例脱离这条切换流程时，引用无法作为运行中桌面的身份凭据。此版本也会优先使用已迁移的 SQLite 索引，保留的 JSON 甚至可能只是旧快照。历史样本的新鲜度只反映采样，可以单独刷新其他账号，也可能因配额未变而省略记录。应把当前账号指针与同一 Codex 实例的账号观察分别处理。官方 `account/read` 提供读取当前认证状态的方式，本轮按结构限定未调用。 [Cockpit 切换实现](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/src-tauri/src/modules/codex_account_runtime_switch.rs#L707-L735) · [当前账号查询](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/src-tauri/src/modules/codex_account_projection.rs#L6-L29) · [索引迁移](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/CHANGELOG.zh-CN.md#L348-L351) · [单号配额刷新](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/src-tauri/src/commands/codex_account_commands.rs#L2058-L2069) · [历史采样规则](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/src-tauri/src/modules/codex_quota_history.rs#L227-L254) · [Codex App Server 账户接口](https://learn.chatgpt.com/docs/app-server#1-check-auth-state)
2. **样本通过 `quota_samples.account_id → accounts[].id` 对应邮箱。** 上游写入的 `account_id` 是 Cockpit 的 `CodexAccount.id`。`identity` 是由创建时间与上游用户、账号、组织标识计算的 SHA-256，公式不含邮箱。上游用 `(account_id, identity)` 过滤历史，同一个 Cockpit ID 的不同身份代次应保留区别。邮箱可作展示字段或生成仅供展示的别名，同邮箱的不同 workspace 仍可能是不同 Cockpit ID。R010 对 SQLite 优先用 `identity`、对 JSON 优先用 `email`，没有联结及当前账号校验，再按 peer 跨账号取最小值。R011 对聚合风险的判断成立，联结键应以上游 `account_id` 为准。对缺失或无法确认的来源保留未知或历史标签。源码 `consilium/quota.py:138–174, 224–255`、`consilium/store.py:2233–2269`。 [样本写入](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/src-tauri/src/modules/codex_quota_history.rs#L59-L129) · [历史读回](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/src-tauri/src/modules/codex_quota_history.rs#L339-L352) · [账号 ID 与 workspace](https://github.com/jlcodes99/cockpit-tools/blob/v1.4.2/src-tauri/src/modules/codex_account_token_refresh.rs#L762-L849)

## 6. R011 §5 Q2.6 · Git 只读边界

**这组参数有用，仍不足以隔离不可信 Git 元数据。** 本机 Git 为 2.54.0。固定 argv 与不用 shell 约束调用方。`core.fsmonitor=false`、`core.hooksPath=/dev/null`、`--no-ext-diff`、`--no-textconv`、`GIT_PAGER=cat` 各自限制特定机制，`GIT_OPTIONAL_LOCKS=0` 只关可选锁及附带写回。子进程可能在超时前启动。 [Git 选项与安全边界](https://git-scm.com/docs/git/2.54.0#_security) · [Diff 选项](https://git-scm.com/docs/diff-options)

- **工作树过滤仍会执行。** `git diff` 比较工作树可走 `convert_to_git()`，`git status` 在需要核对文件内容时也可能走工作树转换。`.gitattributes` 选择的 `filter.*.clean` 或 `filter.*.process` 与 diff helper、textconv 是独立机制。内联子模块 diff、脏状态探测还有嵌套 Git 调用，外层标志不能充当子仓库边界。只审父仓库时应禁掉这类子模块内容查询。 [工作树 diff 源码](https://github.com/git/git/blob/v2.54.0/diff.c#L4291-L4307) · [status 刷新](https://github.com/git/git/blob/v2.54.0/builtin/commit.c#L1520-L1531) · [内容核对](https://github.com/git/git/blob/v2.54.0/read-cache.c#L216-L229) · [过滤器](https://git-scm.com/docs/gitattributes#_filter) · [子模块](https://github.com/git/git/blob/v2.54.0/submodule.c#L631-L667)
- **读取对象也要约束按需联网与输入范围。** 在已登记且可信的 Git 目录中用固定子命令、受控环境、`--no-pager --no-optional-locks --no-lazy-fetch`。提交快照用经过授权的 OID，通过 `ls-tree -z` 和原始 `cat-file blob` 读取，省去属性过滤。路径用字面 pathspec，必要时关闭 replace refs。`ls-files -z` 与 `worktree list --porcelain -z` 适合枚举，仍要独立验证工作树路径及真实 Git 目录；忽略规则也不会隐藏已经跟踪的敏感文件。 [顶层选项](https://git-scm.com/docs/git/2.54.0#_options) · [对象读取](https://git-scm.com/docs/git-cat-file) · [文件枚举](https://git-scm.com/docs/git-ls-files) · [工作树枚举](https://git-scm.com/docs/git-worktree)
- **不可信仓库需要进程级隔离。** 工作树预览可用经过根目录校验的普通文件字节，在进程内生成 diff。确需调用 Git 时限制文件读取、写入、额外进程和网络，并清除继承的 Git 目录、配置、工具和对象库覆盖。注册路径、同 UID、关闭系统或全局配置，均不限制仓库自身的 `.git/config`。本节是未来仓库读取功能的边界意见，R010 当前未实现该功能。 [Git 对不可信仓库的说明](https://git-scm.com/docs/git/2.54.0#_security)

关口判断以 §2 的两个 P1 为准。真实 App 开启、通知和安装属于后续实测，本轮验证限于隔离单测、源码、既有报告和指定文件结构。
