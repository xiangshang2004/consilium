# R010：Antigravity 对 R008 / R009 审查意见的闭环整改与验证报告

- **作者**：Antigravity（Gemini）
- **日期**：2026-11-19
- **范围**：全面回应 [R008（Claude 复核）](./R008-claude-r006-r007-review.md) 与 [R009（Codex 复核）](./R009-codex-review.md) 全部审查项（含 R008 §2 P2-1..3、§3 P3、§4 A1–A4 & B5–B10、§5 用户原话与 R009 §1–§5 / 15 项整改清单），同步 `consilium-v1` OpenSpec、仓库与演示文档及 `13_Consilium` 复录媒体。
- **隔离核验**：未修改 `~/.claude.json`、`~/.claude/`、`~/.codex/config.toml`、`~/.gemini/config/mcp_config.json`、`~/Library/LaunchAgents/`、`~/Library/Application Support/consilium/`、`~/Library/Logs/consilium/` 或 `~/.gitconfig`；未安装 `launchd`；未在后台启动 `codex exec`；未执行 `git push`（仅按显式指令更新了独立截屏脚本 `/Users/up/.gemini/antigravity/brain/c6d5fd4a-20a9-43e9-a51a-ff53727ba9fe/scratch/capture_dashboard.mjs`）。
- **关口状态**：**★ 关口 1 与 ★ 关口 2 保持未越过（`openspec/changes/consilium-v1/tasks.md` 中两项均维持 `- [ ]`，等待 Claude / Codex 复核通过及用户最终拍板）**

---

## 0. 用户原话与 AI 建议区分

### 用户原话 / 硬性决定（不可擅改）

1. **停在 ★ 关口 1 与 ★ 关口 2**：完成 R010 闭环整改、复录、OpenSpec/文档同步与本地主题式提交后，**严格停在 ★ 关口 1 与 ★ 关口 2**，不进入阶段 3（B 级真 App 试跑）或阶段 4（`install.py --apply` 正式安装）。
2. **「只要原生 App」**（2026-11-19，见 [R008 §5.2](./R008-claude-r006-r007-review.md#5-新产品方向评估)）：群聊 / 会诊参与者严格限定为三个官方原生桌面 App（`Claude.app`、`ChatGPT.app / Codex Desktop`、`Antigravity.app`），不接直接模型 API 参与者。
3. **不想手动折腾令牌，希望后续封装 macOS 菜单栏 App**：作为产品方向与 `consilium ui` 一键免密入口的依据；阶段 4.5 菜单栏 App 仍属后续待确认规划。
4. **目录与历史产物约束**：`13_Consilium/` 顶层英文名经用户授权单开，子目录严格保持 `NN_中文名`（`01_演示脚本/`、`02_演示仓库/`、`03_演示记录/`）；保留既有首版截图与录屏，新增带日期的 `*_2026-11-19.*` 复录文件；`/Users/up/code` 内不留 `.agents/teamwork/` 或临时产物。

### AI 建议与本轮契约内技术选择（待关口复核，未越过关口提前实施）

- **未来规划（不在本轮实现，继续保留在待批路线中）**：R008 §5.2–5.3 提出的阶段 4.5 菜单栏 `Consilium.app`（Swift `WKWebView` + `UNUserNotificationCenter`）、阶段 5 会诊室群聊视图、阶段 6 单向只读本地历史索引，均未提前实现。
- **本轮在 R008/R009 审查契约内采纳的技术细节**：
  - 服务端随机会话 `SessionStore`（12h TTL，随机 `csrf_token` 与 `token_version`）替代纯派生静态 Cookie；
  - Cookie 写路径统一校验精确同源 `Origin`（`scheme://host:port`）、`Content-Type: application/json` 与 `X-Consilium-CSRF`；
  - `EventHub` 默认 SSE 并发上限 `SSE_MAX_SUBSCRIBERS = 16`、`10s` socket 写超时与基于 `compute_state_fingerprint` 的 `external_change` / `waiter_change` 检测；
  - `REQUEUEABLE_TASK_STATUSES = ("claimed", "running", "failed", "blocked")` 禁止将 `done` / `cancelled` / `queued` 或已关闭 Mission 的任务重入队；
  - `O_CREAT | O_EXCL` + `flock` + `os.replace` 原子 `.lease` 与 `<rendezvous_dir>/.<peer>.seq.lock` 目录外锁文件。

---

## 1. 改动文件清单

### 1.1 `/Users/up/code/github/consilium` 仓库文件

| 文件 | 改动摘要 |
|---|---|
| `consilium/models.py` | 新增 `ConflictError(ValueError)` 与 `REQUEUEABLE_TASK_STATUSES = ("claimed", "running", "failed", "blocked")` |
| `consilium/store.py` | 新增 `attempt_seq` / `attempt_id`（`A-<seq>-<hex>`）/ `attempt_history_json` / `stale_at` / `dispatched_at_ms` / `claimed_at_ms` / `claim_latency_ms`；`report_progress` / `complete_task` 校验激活 `attempt_id`；`requeue_task` 限制可重入队状态并归档 `attempt_history_json`；`release_stale_claims` 区分 `claimed` 自动重入队（`task_requeued` + 唤醒 `assignee_peer`）与 `running` 仅标记 `stale_at` + 发射 `task_stale` 给协调者与执行者；`list_peers` 实现 5 态逻辑（空闲过期返回 `offline`）；`query_peer_seq_counts` 单快照 `BEGIN; ... COMMIT;`；`idempotency_keys` 增加 `(operation, caller_scope, request_hash)` 与 `ConflictError`（`dispatch_task` 对参数标准化后再哈希）；`record_peer_wakeup_latency` 按 `<= max_event_id` 因果归因单条未确认非冷事件；`record_event_delivery` 持久化冷通知三事实 |
| `consilium/wakeup.py` | 将 `.seq` 锁迁至 `<rendezvous_dir>/.<peer>.seq.lock`；`_read_peer_seq_unlocked` 不触发创建 `signals/`；`_write_seq_payload_locked` 与 `_open_kqueue_watch` 对 `OSError(ENOENT \| EINVAL)` 最多重试 10 次；`_acquire_lease` 改用 `.tmp-*` (`O_CREAT\|O_EXCL`) + `flock(LOCK_EX\|LOCK_NB)` + `fsync` + `os.replace`；`wait_for_doorbell` 强制严格只读（忽略 `auto_pickup=True`）；`ColdDoorbell.ring_cold` 返回三独立事实（`attempted`、`command_status`/`command_ok`/`command_returncode`、`peer_acked`） |
| `consilium/mcp_server.py` | 新增第 11 个 MCP 工具 `consilium_requeue_task`；`consilium_dispatch_task` 传递 `caller_scope`（`stdio --peer` 为 `peer:<default_peer>`，HTTP 为 `coordinator`）；`_collect_post_commit_warnings` 与 `_emit_mutation` 收集去重 `delivery.warnings`（`.seq` 同步、`ring_peer`、`record_event_delivery`、`publish_state_update` 故障均保持 `committed: true` 与 `isError: false`）；`ConsiliumDaemon.start()` 运行 `_reaper_thread`；HTTP 仅认 `Authorization: Bearer`（`?token=` 返回 `401`）；挂载 `SessionStore` / `LoginTicketStore`、Cookie 写路径 `_check_cookie_write_csrf`（`Origin`/`Content-Type`/`X-Consilium-CSRF`）、全路由安全响应头（`nosniff`/`Referrer-Policy`/`Cache-Control`/`frame-ancestors 'none'`）、令牌轮换/登出主动断开 SSE 与票据、SSE 16 上限与单调 `state_version` 守卫 |
| `consilium/dashboard.py` | 实现随机服务端 `SessionStore`（12h TTL）、绑定 `token_version` 的 `LoginTicketStore`、`EventHub`（`SSE_MAX_SUBSCRIBERS = 16`、`_publish_lock` 线性化、`compute_state_fingerprint` 捕获 `stdio` 静默变动）；UI 实现 5 态指示灯、可折叠空看板列、时间线 6 条上限渐隐与 `"显示更多"`/`"收起时间线"` 及 `p.summary` 回退、`btn-subtle-danger`、100% 绿条、`<20%` 低配额警告色、原生 App 副标题+分支悬停、`renderInline`、`"演示数据"` 标签、`claim_latency_ms` 与冷通知三事实徽章、前端 `lastStateVersion` 单调守卫与 `401`/断线重连恢复 |
| `consilium/cli.py` | `consilium serve --help` 更新为 Bearer-only 说明；`consilium wait --pickup` 保持只读并输出忽略提示；`consilium rotate-token` 捕获 `TokenRotationError` 将 JSON 报告输出至 stderr 并以退出码 `1` 退出；`consilium ui` 在浏览器 `open` 返回非零或抛错时打印错误与回退 URL 并退出码 `1` |
| `scripts/install.py` | 新增 `--targets` 与 `--skip-claude-desktop`（允许在 `Claude.app` 运行时安装 `claude_code` / `codex` / `antigravity` / `launchd`）；`claude mcp` 回滚后校验 `claude mcp get consilium` 与 `.claude.json` 一致性；`rotate_token` 中途失败抛出携带进度/回滚说明的 `TokenRotationError` |
| `adapters/skill/consilium-collab/SKILL.md` | 更新 `consilium_wait` 严格只读语义、`attempt_id` 传递、长任务心跳上报建议与 `task_stale` / `consilium_requeue_task` 流程 |
| `tests/test_r010_regressions.py` | 新增 19 项回归测试（`test_r010_01a`–`test_r010_16`，2,075 行），覆盖 R008/R009 全部 15 大项及 UI/5 态规则 |
| `tests/test_dashboard_and_sse.py`、`test_mcp_transports.py`、`test_r006_regressions.py`、`test_store_and_loop_guard.py`、`test_wakeup_and_cold_fallback.py` | 同步 `CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1`、11 个 MCP 工具、`?token=` 401、只读 `wait`、`attempt_id`、`task_stale` 及安全头/CSRF 校验 |
| `openspec/changes/consilium-v1/{proposal.md,design.md,specs/bus/spec.md,specs/dashboard/spec.md,tasks.md}` | 同步全部 13 处代码契约更新，新增 `Stage 2.5` 已完成记录，**保持 `★ Gate 1`、`★ Gate 2`、`★ Gate 3` 与 `★ User consent` 未勾选 (`- [ ]`)** |
| `reviews/R010-antigravity-r009-fixes.md`、`reviews/README.md`、`README.md`、`docs/architecture.md`、`docs/README.md` | 新增 R010 闭环验证报告并同步全部仓库文档与索引 |

### 1.2 `/Users/up/code/13_Consilium` 与配套演示脚本（仓外独立）

| 文件 | 改动摘要 |
|---|---|
| `/Users/up/code/13_Consilium/01_演示脚本/run_demo_a.py` | 补齐 `import urllib.request` 与无代理 `--open-ui` 检查；子环境强制 `CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1` 并剥离真实家目录变量；3 处派发/认领/入库均校验并传递 `attempt_id`；演示配额写入 `source="demo"`；新增 `--step-delay-sec` 及跨步骤缩放超时与早退 `SIGTERM` 清理 |
| `/Users/up/.gemini/antigravity/brain/c6d5fd4a-20a9-43e9-a51a-ff53727ba9fe/scratch/capture_dashboard.mjs` | 由 `DESKBELL_REPO` 迁为 `CONSILIUM_REPO` 并隔离环境变量；以 `--step-delay-sec 1.5 --hold-sec 30` 驱动实时看板并按 `10 fps` 截录 350 帧（`35.0s`），输出带日期 `*_2026-11-19.*` 文件并保留原版媒体 |
| `/Users/up/code/13_Consilium/03_演示记录/dashboard_light_2026-11-19.png`、`dashboard_dark_2026-11-19.png`、`dashboard_live_demo_2026-11-19.mp4`、`demo_a_timeline.md` | 重新生成 Retina 浅色/深色截图（`2880×5596`）、`10 fps` `35.0s` 录屏与新时间线（`M-614BAB20`、`2109.00 ms`），同时字节级完整保留首版三件套 |
| `/Users/up/code/13_Consilium/README.md` | 更新 59 项单测与 R010 状态、移除过时的 R005 `consilium_wait` 警告、补充 `--step-delay-sec` 与 `2026-11-19` 媒体链接（保留旧版链接）、重申 `NN_中文名` 与「只要原生 App」规则，保持 `★ 关口 1 & 2` 待审 |

---

## 2. R008 & R009 逐项整改对照表

### 2.1 核心 15 项整改与 `tests/test_r010_regressions.py` 回归映射

| 编号 | 来源（R008 / R009） | 审查发现摘要 | 整改实现位置 | 验证测试方法（`R010RegressionsTests`） |
|---|---|---|---|---|
| **1** | R009 §1 (3.2), §4.2 | 双进程并发写 `.seq` + 删除整个 `signals/` 竞态；锁 inode 换代；单快照查询 | `wakeup.py` (`<rv>/.<peer>.seq.lock`、`OSError(ENOENT\|EINVAL)` 10 次重试、`_read_peer_seq_unlocked`)；`store.py` (`query_peer_seq_counts` `BEGIN;...COMMIT;`) | `test_r010_01a_dual_process_seq_writes_and_signals_deletion`, `test_r010_01b_kqueue_recovers_when_entire_signals_dir_deleted_during_wait` |
| **2** | R008 §2 P2-2; R009 §1 (3.4) | 真实后台 `_reaper_thread` 调度；`running` 超时不自动重入队（标 `stale_at` + 发射 `task_stale` 给协调者与执行者）；`claimed` 超时自动重入队（`task_requeued` + 唤醒执行者）；`SKILL.md` 长任务心跳说明 | `mcp_server.py` (`_reaper_loop`)；`store.py` (`release_stale_claims`)；`SKILL.md` | `test_r010_02_real_background_reaper_thread_scheduling` |
| **3** | R009 §3.2 | 提交后 `.seq` 同步、`record_event_delivery`、`ring_peer`、`publish_state_update` 异常不得把已提交写返回错误，统一收集去重 `delivery.warnings` | `mcp_server.py` (`_collect_post_commit_warnings`, `_emit_mutation`)；`store.py` (`_safe_sync_peers_seq`) | `test_r010_03_post_commit_fault_isolation_mcp_and_dashboard` |
| **4** | R009 §4.1 | `attempt_id` 认领代次防串改：超时回收或人工重入队后，旧会话携带旧 `attempt_id` 调用 `report_progress`/`complete_task` 被拒绝；自动回收唤醒执行者 | `models.py`; `store.py` (`pickup_inbox`, `report_progress`, `complete_task`); `mcp_server.py` | `test_r010_04_stale_attempt_id_rejected_after_reclaim_and_requeue` |
| **5** | R009 §2 P1 | Cookie 写入口（`/close`、`/requeue`、`/logout`）校验精确同源 `Origin`（同主机不同端口、不同协议、缺失均 `403`）、`application/json`（非 JSON `415`）与 `X-Consilium-CSRF`（`403`） | `mcp_server.py` (`_is_exact_daemon_origin`, `_check_cookie_write_csrf`) | `test_r010_05_cookie_write_origin_csrf_and_content_type_guards` |
| **6** | R008 §3 P3-1; R009 §2 | 服务端随机会话 `SessionStore`（12h TTL）与绑定 `token_version` 的 `LoginTicketStore`；轮换或登出后未兑票据无效、旧会话 `401`、既有 SSE 流立即断开 | `dashboard.py` (`SessionStore`, `LoginTicketStore`, `EventHub.close_all`/`close_for_session`)；`mcp_server.py` (`get_auth_state`) | `test_r010_06_post_token_rotation_revokes_tickets_sessions_and_active_sse` |
| **7** | R008 §2 P3; R009 §1 (3.5), §3.2 | `idempotency_keys` 按 `(operation, caller_scope, request_hash)` 校验，参数/操作/调用方不匹配返回 `ConflictError` / HTTP `409` / MCP `isError=True`；重放保留 `delivery` 回执 | `models.py` (`ConflictError`); `store.py` (`_check_idempotency_locked`, `dispatch_task` 归一化); `mcp_server.py` (`caller_scope`) | `test_r010_07_scoped_idempotency_conflicts` |
| **8** | R009 §3.1 | `consilium_wait` 与 `consilium wait` 严格只读（忽略 `auto_pickup=True` / `--pickup`，返回 `auto_pickup_ignored=True` 与 `notice`，不碰 `bus.db`） | `wakeup.py` (`wait_for_doorbell`); `mcp_server.py`; `cli.py` | `test_r010_08_wait_strictly_read_only_mcp_and_cli` |
| **9** | R008 §2 P2-1; R009 §1 (3.3), §2 | 彻底删除 URL `?token=` 认证（`/mcp`、`/api/state`、`/api/ui-ticket` 一律 `401`）；`consilium serve --help` 改为 Bearer-only 说明 | `mcp_server.py` (`_authenticate_request`); `cli.py` | `test_r010_09_query_token_401_and_serve_help_bearer_only` |
| **10** | R008 §3 P3-2; R009 §2 | 全路由安全响应头：`X-Content-Type-Options: nosniff`、`Referrer-Policy: no-referrer`、`Cache-Control: no-store`（SSE 为 `no-cache, no-store`）及 `/ui` CSP `frame-ancestors 'none'` | `mcp_server.py` | `test_r010_10_security_headers_across_all_routes` |
| **11** | R008 §3 P3-3; R009 §2, §4.3 | SSE 16 连接上限（第 17 条返回 `503`）、`10s` 写超时、并发单调 `state_version`、前端 `lastStateVersion` 与重连/`401` 恢复、`compute_state_fingerprint` 感知 `stdio` 静默变动发射 `external_change` | `dashboard.py` (`SSE_MAX_SUBSCRIBERS=16`, `EventHub`, `compute_state_fingerprint`)；`mcp_server.py` | `test_r010_11_sse_cap_monotonic_versions_and_stdio_external_change` |
| **12** | R009 §4.2 | 原子 `.lease` 创建：`.tmp-<pid>-<tid>-<rand>` (`O_CREAT\|O_EXCL`) + `flock(LOCK_EX\|LOCK_NB)` + `fsync` + `os.replace`，防止扫描器误删初始化中的租约 | `wakeup.py` (`_acquire_lease`) | `test_r010_12_atomic_lease_creation_flock_and_cleanup` |
| **13** | R008 §2 P2-3; R009 §1 (3.3, 3.7) | 安装器 `--targets` / `--skip-claude-desktop` 在 `Claude.app` 运行时安装其余目标；`claude mcp get consilium` + `.claude.json` 回滚校验；`TokenRotationError` 中途失败结构化报告 + CLI exit `1` | `scripts/install.py`; `cli.py` | `test_r010_13_installer_targets_and_skip_claude_desktop_while_claude_running`, `test_r010_13_claude_code_rollback_verification_failures`, `test_r010_13_rotate_token_mid_failure_and_cli_exit_1` |
| **14** | R009 §4.4 | 重入队状态收窄至 `REQUEUEABLE_TASK_STATUSES = ("claimed", "running", "failed", "blocked")`，拒绝 `queued`/`done`/`cancelled`/已关闭 Mission 与非协调者 MCP 调用；字段重置并保留多轮 `attempt_history` | `models.py`; `store.py` (`requeue_task`); `mcp_server.py` (`consilium_requeue_task`) | `test_r010_14_requeue_scope_guards_and_multi_attempt_history` |
| **15a** | R008 §3 P3-4, §4 B10; R009 §3.3, §4.5, §5 | `claim_latency_ms`（派发至认领毫秒差）、因果 `wakeup_latency_ms`（仅落 `<= max_event_id` 单条未确认非冷事件）、冷通知三事实（`attempted`, `command_status`/`command_ok`/`command_returncode`, `peer_acked`）、`consilium ui` 打开失败报错退出 `1` | `store.py`; `wakeup.py` (`ColdDoorbell.ring_cold`); `cli.py` | `test_r010_15_claim_latency_cold_facts_and_consilium_ui_errors` |
| **15b** | R008 §4 A1–A3, B5–B10; R009 §5 | 5 态指示灯（`standby`/`working`/`online`/`offline`/`stale`，空闲过期显 `offline`）与 R008 A/B 全部 UI 规则（空列折叠、时间线渐隐折叠与 `p.summary` 回退、次要关闭按钮、100% 绿条、`<20%` 橙条、原生 App 名称+分支悬停、`renderInline`、`演示数据` 标签、登录提示） | `store.py` (`list_peers`); `dashboard.py` | `test_r010_16_five_state_peer_indicators_and_dashboard_ui_rules` |

### 2.2 R008 §2 (P2)、§3 (P3) 与 §4 (A1–A4, B5–B10) 逐条核销

| R008 条目 | 整改结果与对应证据 |
|---|---|
| **P2-1 删除 `?token=`** | 已彻底移除 `?token=` 认证（`401`），`consilium serve --help` 改为 `"Require Bearer token on HTTP endpoints (default)."`（`test_r010_09`） |
| **P2-2 `running` 超时不自动重入队** | `running` 超时仅标 `stale_at` 并发射 `task_stale` 给协调者与执行者，等待 `consilium_requeue_task`；`claimed` 仍自动重入队并唤醒执行者；`SKILL.md` 已补充长任务心跳上报与只读 wait 说明（`test_r010_02`, `test_r006_regressions.test_3_4_*`） |
| **P2-3 `Claude.app` 运行时按目标安装** | 新增 `--targets` 与 `--skip-claude-desktop`，非 `claude_desktop` 目标可在 `Claude.app` 运行时 `--apply`，选含 `claude_desktop` 时仍拒绝修改任何文件（`test_r010_13_installer_targets_and_skip_claude_desktop_while_claude_running`） |
| **P3-1 随机服务端会话** | 替换为 12h TTL `SessionStore` 与绑定 `token_version` 的 `LoginTicketStore`，登出或轮换即失效（`test_r010_06`） |
| **P3-2 `nosniff` / `frame-ancestors`** | 已补齐 `X-Content-Type-Options: nosniff`、`Referrer-Policy: no-referrer`、`Cache-Control: no-store`（SSE 为 `no-cache, no-store`）与 `/ui` CSP `frame-ancestors 'none';`（`test_r010_10`） |
| **P3-3 SSE 上限与写超时** | `SSE_MAX_SUBSCRIBERS = 16`（超限 `503`），写 socket `10s` 超时，`_publish_lock` 保障单调 `state_version`（`test_r010_11`） |
| **P3-4 `claim_latency_ms` 优先展示** | `pickup_inbox` 记录 `claim_latency_ms`，时间线优先展示 `⏱ 认领 ... ms`，`record_peer_wakeup_latency` 按 `<= max_event_id` 因果归因（`test_r010_15`） |
| **A1 五态状态灯** | `list_peers` + 前端统一为 `standby`/`working`/`online`/`offline`/`stale` 五态，空闲且过期固定呈灰色 `offline`，绝不误显为蓝灯 `working` 或橙灯 `stale`（`test_r010_16`） |
| **A2 空列折叠** | 任务看板空列默认 `.empty-col` 折叠为窄条，有任务列获得充分宽度（`test_r010_16` + 截图可见） |
| **A3 时间线防硬切** | 超过 6 条记录在底部渲染 `.timeline-fade` 渐隐遮罩与 `"显示更多（余 N 条）"` / `"收起时间线"` 按钮，且 `renderTimeline` 补齐 `p.summary` 回退（`test_r010_16` + 截图可见） |
| **A4 重录 10fps 30–40s 演示视频** | `run_demo_a.py --step-delay-sec 1.5` + `capture_dashboard.mjs` 复录 `dashboard_live_demo_2026-11-19.mp4`：`1440×1000`、`10/1 fps`、`350` 帧、`35.0s`（见 §3） |
| **B5 关闭 Mission 次要按钮** | 改为 `.btn-subtle-danger` 描边次要按钮，强红色保留在二级确认弹窗（`test_r010_16`） |
| **B6 100% 绿条与 `<20%` 低配额橙色** | 100% 完成进度条使用绿色完成样式，`<20%` 配额条使用橙色警告色（`test_r010_16`） |
| **B7 原生 App 名称 + 分支悬停** | 卡片主副文案显示原生 App 说明（`Claude.app · Code` / `ChatGPT.app · Codex` / `Antigravity.app`），分支前缀放入悬停 `title`（`test_r010_16`） |
| **B8 任务进度 `renderInline`** | 任务卡片正文与进度均复用先转义后内联渲染的 `renderInline`（`test_r010_16`） |
| **B9 `演示数据` 标签** | `run_demo_a.py --seed-demo-ui` 写入 `source="demo"`，前端在配额条旁渲染 `演示数据` 标签，且单测/演示默认启用 `CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1`（`test_r010_16` + 截图可见） |
| **B10 `consilium ui` 错误上报与登录提示** | `consilium ui` 在 `open` 失败或非零退出时输出报错与回退 URL 并退出 `1`；登录弹窗保留终端运行 `consilium ui` 提示（`test_r010_15`, `test_r010_16`） |

### 2.3 R009 §1–§5 核查细节回应确认

1. **R009 §1 (3.2) & §4.2**：除双进程并发写 + 整目录删除重建 `.seq` 与 `kqueue` 恢复外，已修复 M1 期间发现的 macOS APFS 下整目录并发删除触发的 `OSError(22, EINVAL)`（`_write_seq_payload_locked` 与 `_open_kqueue_watch` 均增加 10 次有界重试），`_read_peer_seq_unlocked` 不再重建 `signals/`，并在 `query_peer_seq_counts` 使用单事务 `BEGIN; ... COMMIT;` 快照。
2. **R009 §2 (CSRF / Tickets / Session / Headers / SSE)**：不仅比较 `Origin` 主机名，现严格比对 `scheme == 'http'`、准确 `host` 与 `port`、拒绝 path/query/userinfo 与缺失 `Origin`，同时校验 `Content-Type: application/json` (`415`) 与 `X-Consilium-CSRF` (`403`)；`/api/ui-ticket` 仅认 Bearer；`token_version` 变更或 `/api/logout` 即刻作废未兑票据、既有 Cookie 与活跃 SSE 流；全路由响应头补齐。
3. **R009 §3 (只读 Wait / 提交后隔离 / 冷通知三事实)**：`wait_for_doorbell` 永不调用 `pickup_inbox` 或写 `bus.db`；全部 7 个写工具及 `pickup_inbox` 对提交后 `.seq` 同步、响铃、回执入库、SSE 广播的故障均通过 `delivery.warnings`（去重保序）输出，`committed: true` 与 `isError: false`；`ColdDoorbell.ring_cold` 与时间线如实区分 `attempted`、`command_status`/`command_ok` 与 `peer_acked`。
4. **R009 §4.1–4.5**：通过 `attempt_seq`/`attempt_id` 闭环阻断旧会话晚到写回；通过 `REQUEUEABLE_TASK_STATUSES` 与 `attempt_history_json` 隔离各轮尝试结果；通过 `_publish_lock` + 服务端/前端 `lastStateVersion` 单调守卫 + `compute_state_fingerprint` 消除并发 SSE 倒序与 `stdio` 漏推。
5. **R009 §1 (3.7)**：阶段二中途失败抛出结构化 `TokenRotationError`，并对 `claude mcp remove/add` 回滚执行 `claude mcp get consilium` + `.claude.json` 双重校验。

---

## 3. 视觉与演示交付物（2026-11-19 复录）

在 `/Users/up/code/13_Consilium/03_演示记录/` 中**完整保留**了 R007 首版未加日期文件，并生成验证通过的 `2026-11-19` 复录产物：

| 交付物 | 路径 | 实测规格与核验指标 |
|---|---|---|
| 浅色模式截图（复录） | [dashboard_light_2026-11-19.png](../../../13_Consilium/03_演示记录/dashboard_light_2026-11-19.png) | `2880×5596` Retina (`1,060,120` bytes)；实图可见 `SSE 实时`、`3 / 3 端活跃`、5 态指示灯（`待命` / `在线` / `工作中`）、`演示数据` 标签、`⏱ 认领 1522.72 ms`、`🔔 冷通知 · dry-run` + `✓ 对方已确认`、空列折叠、100% 绿条及 `.timeline-fade` `显示更多` |
| 深色模式截图（复录） | [dashboard_dark_2026-11-19.png](../../../13_Consilium/03_演示记录/dashboard_dark_2026-11-19.png) | `2880×5596` Retina (`1,078,403` bytes)；高对比深色配色无裁剪、无 JS/HTTP 报错 |
| 实时演示录屏（复录） | [dashboard_live_demo_2026-11-19.mp4](../../../13_Consilium/03_演示记录/dashboard_live_demo_2026-11-19.mp4) | `ffprobe` 实测：`codec_name=h264`、`pix_fmt=yuv420p`、`1440×1000`、`r_frame_rate=10/1`、`avg_frame_rate=10/1`、`nb_frames=350`、`duration=35.000000s`、`size=492,110` bytes；`ffmpeg -f framemd5` 验证 `319/350` 帧哈希唯一（`336` 次帧间真实变化），完整呈现登录、`0/3 -> 3/3` 状态流转、轮次推进、切换深色模式及实时计时 |
| A 级演示时间线 | [demo_a_timeline.md](../../../13_Consilium/03_演示记录/demo_a_timeline.md) | 退出码 `0`（`M-614BAB20`，`completed`，`3/3`，总耗时 `2109.00 ms`，7 次 `kqueue` 唤醒 `8.18–17.89 ms`，`tmp_cleaned=True`，`0` 处 `deskbell` 残留） |
| 保留的首版媒体 | [dashboard_light.png](../../../13_Consilium/03_演示记录/dashboard_light.png)、[dashboard_dark.png](../../../13_Consilium/03_演示记录/dashboard_dark.png)、[dashboard_live_demo.mp4](../../../13_Consilium/03_演示记录/dashboard_live_demo.mp4) | 原封保留、字节级 SHA-256 未改 |

---

## 4. 全套测试与 OpenSpec 验证输出

### 4.1 单元测试（59 项全部通过，0 warning）

```text
$ cd /Users/up/code/github/consilium && env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 /usr/bin/python3 -B -W default -m unittest discover -s tests -v
test_cookie_login_api_state_shape_and_logout (test_dashboard_and_sse.DashboardAndSSETests) ... ok
test_dashboard_requeue_and_close_actions (test_dashboard_and_sse.DashboardAndSSETests) ... ok
test_sse_detects_waiter_change_and_latency_only_stamps_unacked (test_dashboard_and_sse.DashboardAndSSETests) ... ok
test_sse_stream_pushes_ordered_updates (test_dashboard_and_sse.DashboardAndSSETests) ... ok
test_ui_offline_html_csp_and_host_origin_checks (test_dashboard_and_sse.DashboardAndSSETests) ... ok
test_ui_one_time_ticket_auto_login_and_burn_after_read (test_dashboard_and_sse.DashboardAndSSETests) ... ok
test_apply_idempotency_token_paths_and_safe_uninstall (test_installer_dry_run.InstallerTests) ... ok
test_dry_run_does_not_mutate_files (test_installer_dry_run.InstallerTests) ... ok
test_duplicate_codex_toml_table_outside_block_rejected (test_installer_dry_run.InstallerTests) ... ok
test_http_default_token_security_headers_and_405 (test_mcp_transports.MCPTransportsTests) ... ok
test_stdio_lifecycle_protocol_negotiation_and_close_mission (test_mcp_transports.MCPTransportsTests) ... ok
test_mcp_consilium_quota_manual_report_and_status (test_quota_extraction.QuotaExtractionTests) ... ok
test_sync_local_quotas_across_cockpit_codex_and_claude (test_quota_extraction.QuotaExtractionTests) ... ok
test_3_1_wait_defaults_no_auto_pickup_and_timeout_leaves_task_queued (test_r006_regressions.R006RegressionsTests) ... ok
test_3_2_kqueue_recovers_after_sig_deleted_during_wait (test_r006_regressions.R006RegressionsTests) ... ok
test_3_2_seq_concurrent_writers_converge_to_db_truth (test_r006_regressions.R006RegressionsTests) ... ok
test_3_2_stdio_rebuilds_deleted_signals_dir (test_r006_regressions.R006RegressionsTests) ... ok
test_3_3_rotate_token_and_live_daemon_reload (test_r006_regressions.R006RegressionsTests) ... ok
test_3_4_daemon_reaper_requeues_expired_running_task_and_notifies_coordinator (test_r006_regressions.R006RegressionsTests) ... ok
test_3_5_ring_error_returns_committed_true_and_idempotency_prevents_duplicates (test_r006_regressions.R006RegressionsTests) ... ok
test_3_6_skill_md_removes_press_return_claim (test_r006_regressions.R006RegressionsTests) ... ok
test_3_7_claude_add_failure_restores_previous_entry_and_strips_config_dir (test_r006_regressions.R006RegressionsTests) ... ok
test_3_7_codex_duplicate_leaves_all_other_configs_untouched (test_r006_regressions.R006RegressionsTests) ... ok
test_3_7_running_claude_app_refuses_apply (test_r006_regressions.R006RegressionsTests) ... ok
test_demo_a_findings_rendezvous_env_precedence_and_peer_activity (test_r006_regressions.R006RegressionsTests) ... ok
test_p2_1_wait_precheck_sees_queued_task_after_unclaimed_pickup (test_r006_regressions.R006RegressionsTests) ... ok
test_p2_2_request_review_guards_closed_mission_and_task_ownership (test_r006_regressions.R006RegressionsTests) ... ok
test_p2_3_strict_rpc_and_http_validation (test_r006_regressions.R006RegressionsTests) ... ok
test_p2_5_quota_null_unknown_expired_reset_and_window_minutes (test_r006_regressions.R006RegressionsTests) ... ok
test_r010_01a_dual_process_seq_writes_and_signals_deletion (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_01b_kqueue_recovers_when_entire_signals_dir_deleted_during_wait (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_02_real_background_reaper_thread_scheduling (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_03_post_commit_fault_isolation_mcp_and_dashboard (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_04_stale_attempt_id_rejected_after_reclaim_and_requeue (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_05_cookie_write_origin_csrf_and_content_type_guards (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_06_post_token_rotation_revokes_tickets_sessions_and_active_sse (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_07_scoped_idempotency_conflicts (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_08_wait_strictly_read_only_mcp_and_cli (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_09_query_token_401_and_serve_help_bearer_only (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_10_security_headers_across_all_routes (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_11_sse_cap_monotonic_versions_and_stdio_external_change (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_12_atomic_lease_creation_flock_and_cleanup (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_13_claude_code_rollback_verification_failures (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_13_installer_targets_and_skip_claude_desktop_while_claude_running (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_13_rotate_token_mid_failure_and_cli_exit_1 (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_14_requeue_scope_guards_and_multi_attempt_history (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_15_claim_latency_cold_facts_and_consilium_ui_errors (test_r010_regressions.R010RegressionsTests) ... ok
test_r010_16_five_state_peer_indicators_and_dashboard_ui_rules (test_r010_regressions.R010RegressionsTests) ... ok
test_atomic_concurrent_pickup_prevents_double_claim (test_store_and_loop_guard.StoreAndLoopGuardTests) ... ok
test_loop_guard_blocks_after_max_rounds (test_store_and_loop_guard.StoreAndLoopGuardTests) ... ok
test_mission_lifecycle_re_dispatch_and_close_guards (test_store_and_loop_guard.StoreAndLoopGuardTests) ... ok
test_release_stale_claims (test_store_and_loop_guard.StoreAndLoopGuardTests) ... ok
test_cold_fallback_dry_run (test_wakeup_and_cold_fallback.WakeupAndColdFallbackTests) ... ok
test_db_free_kqueue_hot_wakeup_and_lease_cleanup (test_wakeup_and_cold_fallback.WakeupAndColdFallbackTests) ... ok
test_deeplink_utf8_pre_encode_truncation_and_codex_prompt_param (test_wakeup_and_cold_fallback.WakeupAndColdFallbackTests) ... ok
test_flock_prunes_zombie_lease_even_with_live_pid (test_wakeup_and_cold_fallback.WakeupAndColdFallbackTests) ... ok
test_immediate_return_when_event_exists_before_wait (test_wakeup_and_cold_fallback.WakeupAndColdFallbackTests) ... ok
test_long_home_path_shortens_unix_socket_inside_private_subdir (test_wakeup_and_cold_fallback.WakeupAndColdFallbackTests) ... ok
test_unacked_hot_wakeup_escalates_to_cold_doorbell (test_wakeup_and_cold_fallback.WakeupAndColdFallbackTests) ... ok

----------------------------------------------------------------------
Ran 59 tests in 10.916s

OK
```

### 4.2 OpenSpec 严格校验与关口核查

```text
$ cd /Users/up/code/github/consilium && openspec validate consilium-v1 --strict && openspec validate consilium-v1
Change 'consilium-v1' is valid
Change 'consilium-v1' is valid
```

- `openspec/changes/consilium-v1/tasks.md` 第 19 行 `- [ ] ★ **Gate 1**` 与第 26 行 `- [ ] ★ **Gate 2**`（以及 `★ Gate 3`、`★ User consent`）均保持未勾选 `- [ ]`。

---

## 5. 已知限制与下一步关口

1. **v1 既有非阻塞边界**：
   - 冷通知升级仍使用进程内守护线程定时器（随常驻 daemon / stdio 会话存在），属 v1 既定权衡；
   - 单机单用户同宿主模型共享 Bearer 令牌；`consilium rotate-token` 在 Stage 1 预检抛错时以非零退出且不写任何目标，Stage 2 中途故障则抛出结构化 `TokenRotationError` JSON 报告；
   - 端到端时延基准记录中若对 `Origin` 传入非数字端口，Python `urllib.parse` 会在处理器线程内抛 `ValueError`（已在后续记录中注明）。
2. **下一步关口（本轮在此严格停步）**：
   - **★ 关口 1**：等待 Claude 与 Codex 复核 R006/R010 闭环整改通过；
   - **★ 关口 2**：等待用户审阅 `dashboard_light_2026-11-19.png`、`dashboard_dark_2026-11-19.png` 与 `dashboard_live_demo_2026-11-19.mp4` 后拍板确认。
   - 仅当两道关口全部通过后，方推进阶段 3（B 级真 App 试跑）与阶段 4（`consilium doctor` + `--dry-run` 经用户同意后 `--apply`）。
