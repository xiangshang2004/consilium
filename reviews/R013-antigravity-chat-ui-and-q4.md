# R013: Consilium 极简对话流 `/ui`、Q4 多账号隔离与 R012 闭环整改交付报告

2026-11-20 · Antigravity 交付（待合入前本地分主题提交）· 基线 `a192f9f`（含 Codex `R012` [9f7f6d5 / 991396c](./R012-codex-review.md)）

> **文件命名说明**：用户指令提及交付路径 `reviews/R013-antigravity-chat-ui.md`；由于 Codex 已先在 `main` 提交 [R012-codex-review.md](./R012-codex-review.md) 与 [R012-codex-r010-review.md](./R012-codex-r010-review.md) 并链向 `./R013-antigravity-chat-ui-and-q4.md`，本轮选择**原地更新 `reviews/R013-antigravity-chat-ui-and-q4.md`** 以保持已有审计链接不断裂，并在本文及 [reviews/README.md](./README.md) 中集中记录 R011/R013 全部原始交付与 R012 全量复核闭环。

---

## 1. 结论与关口状态

- **R012 阻塞项与 P2 全量闭环**：全部处理 [R012-codex-review.md](./R012-codex-review.md) §1 两个 P1（令牌轮换失败诊断脱敏与 `claude mcp add-json` 异常回滚；Bearer `auth_token_ver` 跨 `/api/ui-ticket` 与 `/api/stream` 贯通及初始快照二次鉴权）、§2 六个 P2（旧库升级 `A-1-legacy-<task_id>` 与 `@` 邮箱清洗、`SKILL.md` `progress_note` 示例、幂等原 `delivery_json` 回放、热转冷持久化回调、`consilium_wait` 真实热唤醒零写库、SSE `queue.Empty` 锁内重采样与 `try/finally` 退订包络）、§3 Q4/对话流细节（Cockpit `accounts[].id` 主联结键、禁止单候选无主权指针误设 `is_current`、30 分钟阈值覆盖所有现用与备用号、历史账号过滤、全量 Cockpit/Claude ID 哈希、RO/immutable SQLite 防护、`compute_state_fingerprint` 剥离配额与认领动态计时字段、`模型与账号`/`子 Agent` 文案收敛、`copyTextSafe` 失败显错），以及 [R012-codex-r010-review.md](./R012-codex-r010-review.md) §4（HTTP/1.1 严格 `Content-Length` / `501 Transfer-Encoding` / 短读 `400` / POST `/api/ui-ticket` & `/api/logout` 体消费与 `close_connection` 隔离）。
- **验证结果（最近一次全量复测）**：
  - `tests/` **10 个测试模块共 75 项单元测试全绿（0 失败 / 0 跳过）**，涵盖原 `59` 项、`R013` 新增 `6` 项及本次 `tests/test_r012_regressions.py` 新增 `10` 项回归；
  - 隔离环境变量模式（`-u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1`）：`Ran 75 tests in 17.785s — OK`；
  - 默认未设环境变量模式（`-u CLAUDE_CONFIG_DIR -u CONSILIUM_DRY_RUN -u CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC`）：`Ran 75 tests in 23.164s — OK`；
  - `openspec validate consilium-v1 --strict`：`Change 'consilium-v1' is valid`。
- **视觉截图复验（`2026-11-20`）**：[run_demo_a.py](../../../13_Consilium/01_%E6%BC%94%E7%A4%BA%E8%84%9A%E6%9C%AC/run_demo_a.py) 在 SSE 实时连接下采集 4 张截图（浅色/深色 `1440×960 @2x` 双方顶部均稳定显示 `● SSE 实时`，`390×844 @2x` 双方均实现 `scrollWidth === innerWidth === 390` 零横向溢出）；按用户 2026-11-20 最新要求（以后只用截图，不录屏），已删除全部 `.mp4` 录屏文件。
- **边界与约束遵守**：未触碰真实 `~/.claude`、`~/.codex`、`~/.gemini`、`~/.antigravity_cockpit`、`~/Library`，未安装 `launchd`，未启动后台 `codex exec`，测试全部剥离 `CLAUDE_CONFIG_DIR` 并使用临时目录，未执行 `git push`。
- **当前停在关口**：**★ 关口 1（待 Claude 与 Codex 复核本 R013 及 R012 整改）** 与 **★ 关口 2（待用户确认最新 2026-11-20 Light/Dark `1440px`/`390px` 截图外观）**。

---

## 2. R012 逐项整改对照（R012-codex-review & R012-codex-r010-review）

| 来源条目 | 问题描述与要求 | 本轮改动实现 | 对应文件 / 回归测试 |
|---|---|---|---|
| `R012 §1 #1` (P1) | 轮换失败时 `rollback_instructions` 带出 `cc_prev` 明文 Bearer、`claude` 子进程抛异常绕过回滚、CLI 诊断未脱敏、Codex 仅限托管块 | `_backup_file` 强制 `0o700`/`0o600`；新增 `_redact_secrets` 脱敏 `Bearer ...`、JSON/TOML `Authorization` 及显式新旧令牌；`_apply_claude_code_mcp` 用 `try/except` 包裹 `runner(...)` 使进程异常也执行回滚与校验；移除 `cc_prev` 原文，增设 `failed_target_may_be_modified=True` 与 `codex_scope="managed_block_only"` | [scripts/install.py](../scripts/install.py) · `test_r012_p1_1_rotation_failure_redacts_tokens_and_rollbacks_on_exception` |
| `R012 §1 #2` (P1) | `_authenticate_request` 仅返回认证模式，并发轮换时旧 Bearer 通过后可能在 `/api/ui-ticket` 与 `/api/stream` 绑定到新 `token_version`；Cookie 流首帧前未复检会话 | `_authenticate_request` 与 `_check_security` 返回并传递经过核验的 `auth_token_ver`；`POST /api/ui-ticket` 传入该版本给 `LoginTicketStore.issue`；`_handle_sse_stream` 将 `auth_token_ver` 绑定入 `sub`，并强制执行 `_stream_still_authorized()` 二次校验（发响应头前及首帧 `snapshot_with_version` 后） | [consilium/mcp_server.py](../consilium/mcp_server.py) · `test_r012_p1_2_concurrent_rotation_blocks_ticket_and_sse_initial_frame` |
| `R012 §2` · 认领代次与 Skill (P2) | 旧库升级时 `claimed`/`running` 行缺少 `attempt_id` 导致续报/完成死锁；`SKILL.md` 首条进度示例误写 `note` | `BusStore._init_schema` 自动为无 `attempt_id` 的旧库 `claimed`/`running` 行补全 `A-1-legacy-<task_id>` 并保证可续报/重排；`SKILL.md` 第 53 行示例修正为 `progress_note` | [consilium/store.py](../consilium/store.py) · [adapters/skill/consilium-collab/SKILL.md](../adapters/skill/consilium-collab/SKILL.md) · `test_r012_p2_legacy_db_backfill_attempt_id_and_email_scrub` |
| `R012 §2` · 幂等重放与热转冷 (P2) | 幂等重放自造 `delivery: replayed` 丢失原回执与 `warnings`；热转冷回调未落回 `record_event_delivery` 持久层 | `idempotency_keys` 迁移新增 `delivery_json`，新增 `save_idempotency_delivery` 与 `_check_idempotency_locked` 中的 `_cached_delivery` 恢复，经 `_safe_save_idempotency_delivery` / `_replay_delivery_payload` 原样返回首次交付回执；`on_escalate` 经 `_safe_ring_peer` 接入持久层 | [consilium/store.py](../consilium/store.py) · [consilium/mcp_server.py](../consilium/mcp_server.py) · `test_r012_p2_idempotent_replay_returns_original_delivery_receipt` |
| `R012 §2` · 只读 `consilium_wait` (P2) | `consilium_wait` 真实热唤醒分支调用 `record_peer_wakeup_latency` / `_emit_mutation("wait_woke")` 写入 `bus.db` | 移除热唤醒后的写库与广播调用，`consilium_wait` 全路径保持零 SQLite 写入 | [consilium/mcp_server.py](../consilium/mcp_server.py) · `test_r012_p2_wait_real_kqueue_hot_wakeup_zero_sqlite_writes` |
| `R012 §2` · SSE 锁序与清理 (P2) | `queue.Empty` 空闲分支在锁外取快照后再在锁内取新版本号导致乱序；`subscribe` 后、`finally unsubscribe` 前发响应头抛异常可占住订阅名额 | 进入 `subscribe` 后立即接入 `try/finally unsubscribe(sub)`；`queue.Empty` 触发 `external_change` 时改为通过 `snapshot_with_version` 在发布锁内原子重建并配对版本号 | [consilium/mcp_server.py](../consilium/mcp_server.py) · `test_r012_p2_sse_idle_external_change_re_snapshots_inside_publish_lock` |
| `R012-codex-r010 §4` & `P3-2` | `Transfer-Encoding`、重复/畸形 `Content-Length`、短读被解析为 `{}`；成功早退 `/api/ui-ticket` 与 `/api/logout` 未消费体并复用连接 | 新增 `_parse_strict_content_length`（`Transfer-Encoding` 返回 `501`，重复或非 ASCII 数字长度与短读返回 `400 Incomplete request body`）与 `_consume_optional_body`，`400+` 与未完整消费体响应统一置 `close_connection = True` | [consilium/mcp_server.py](../consilium/mcp_server.py) · `test_r012_p3_2_keepalive_post_early_return_and_logout_no_desync` · `test_r012_http_framing_rejects_te_dup_cl_and_short_body` |
| `R012 §3 & §5.2` (Q4) | 单候选无 `current` 标记误设 `is_current`；30 分钟判断未覆盖全账号及备用号筛选；历史账号仅给计数；含无 `@` ID 未哈希；RO WAL 可能建侧文件；`quota_samples` 主键未优先 `accounts[].id` | `_hash_account_ref(..., always_hash=True)` 哈希全部 Cockpit/Claude ID 及旧库 `@` 邮箱；`_load_cockpit_codex_registry` 以 `accounts[].id` 为主联结键，邮箱仅在唯一时作展示映射（同邮箱双 workspace 保持独立），缺注册表时 `clear_peer_cockpit_current("chatgpt")`；SQLite 无 `-wal` 时用 `mode=ro&immutable=1`，有 WAL 时用 `mode=ro` + `PRAGMA query_only=ON`；`list_quota_windows` 对未确认单候选保留 `current_account_unknown=True`、`>30m` 一律置 `account_switch_suspected=True` 并移出 `backups_with_cap`，同时输出 `historical_accounts` 与 `"历史账号 N"` | [consilium/quota.py](../consilium/quota.py) · [consilium/store.py](../consilium/store.py) · `test_r011_q4_*` · `test_r012_p2_fingerprint_ignores_ticking_quota_fields_and_q4_stale_backup` · `test_r012_q4_same_email_two_workspaces_stay_distinct_by_account_id` |
| `R012 §3` · `/ui` 与指纹 (P2) | 配额样本年龄与倒计时随秒跳变导致 `external_change` 误刷新整页；抽屉文案与剪贴板失败仍弹成功 | `_VOLATILE_TICK_KEYS` 统一在 `compute_state_fingerprint` 中剥离 `age_seconds`、`claim_age_seconds`、`sample_age_seconds`、`reset_in_seconds`、`reset_in_human`、`earliest_reset_in_human`、`updated_at`；统一 `DRAWER_TABS` 与 `SLASH_COMMANDS`，补 `supported_models`、`该端未上报`、`"账号未知"`/`"可能已切换"`/`"历史账号"` 与 `"最后上报于 "`，新增失败感知 `copyTextSafe` | [consilium/dashboard.py](../consilium/dashboard.py) · `test_r011_ui_minimalist_conversation_contract` · `test_r012_p2_fingerprint_ignores_ticking_quota_fields_and_q4_stale_backup` |

> **说明（R012 §6 Git 只读边界）**：`R012-codex-r010-review.md` §6 对 `git diff` / `cat-file` / `.gitattributes` 过滤器与不可信仓库的进程级隔离约束属于**阶段 5（文件树、Diff 与源码读取接口）**设计范围；本轮按范围冻结完全未新增 Git 读取接口，已将 §6 全项并入 [openspec/changes/consilium-v1/tasks.md](../openspec/changes/consilium-v1/tasks.md) 阶段 5 阻塞前置。

---

## 3. R011 / R013 原始需求逐项对照

| 用户 / R011 要求 | 实施与偏离说明 | 状态 |
|---|---|---|
| **1. `list_quota_windows()` 主配额按当前激活账号隔离** | 已实现：仅按 `is_current=1`（非历史、非未知、未超 30 分钟）计算 `exhausted`、`min_remaining_percent` 与 `dispatch_recommended`；同账号池历史旧号配额归零但当前号余量充足时，不再误判整端耗尽。 | 完成 |
| **1. `pool` / `pool_has_capacity` / `switch_hint` 与 ID 映射** | 已实现：Cockpit 样本通过 `accounts[].id` 主键联结 `accounts`（邮箱仅唯一映射）；无记录历史样本标 `is_historical=True`（UI 显示 `"历史账号 N"` 并排除出主配额与可切换备用池）；当前号超过 30 分钟标 `account_switch_suspected=True`、主配额归“未知”；Antigravity 未核实当前账号标 `current_account_unknown=True`（`"账号未知"`）。 | 完成 |
| **1. Cockpit 严格只读白名单与零明文邮箱入库** | 已实现：仅读取 `~/.antigravity_cockpit/codex_accounts.json`（白名单字段 `current_account_id`、`id`、`email`、`plan_type`）、`codex-quota-history-v1.sqlite`、`platform-quota-history-v1.sqlite`；单测通过 `builtins.open`、`sqlite3.connect` 与 `os.stat` 间谍断言绝不打开 `secure-account-storage.key`、`*_accounts.sqlite`、`accounts/*.json` 或 `codex_accounts/`；所有邮箱/账户 ID 全量 SHA-256 脱敏入库与输出。 | 完成 |
| **2. `/ui` 极简对话流重设计（R011 §5 Q1）** | 已实现：左侧收敛式会诊列表 + 中间单会话事件流（仅按事件时间线展示，不新增后台写接口） + 顶部单行三端胶囊（`Claude`、`Codex`、`Antigravity` 原生 App 五态灯 + 当前账号配额 + 点击弹层看 `pool`） + 右侧默认折叠 5 标签抽屉（`任务` 复用重排/关闭、`工作树` 只读列 `worktree_branch`、`模型与账号` 含 `supported_models`、`子 Agent` 显示 `"该端未上报"` 占位、`文件` 只列 `artifacts` 路径） + 纯前端 `/` 面板（`/mission`、`/files`、`/export`、`任务`/`工作树`/`账号`/`子 Agent` 跳转） + 只读交接提示词拼装区。 | 完成 |
| **3. 修复 R011 §4 原型 6 处问题** | 1) 去除 CDN Tailwind，全内联离线 CSS，保持原严格 CSP；2) `/` 菜单实底浮层 + `Esc` 关闭；3) 浅色 / 深色 / 跟随系统三态切换；4) 移除写死模型名，改为动态读取 `supported_models`；5) `打开原生对话` 改为 `新开原生会话接手`，禁用未验证 `codex://threads/<id>`；6) 进度统一文案为 `"最后上报于 X 秒/分钟前"`。 | 完成 |
| **4. P3-1..4 加固与回归** | P3-1 畸形 `Origin` 非数字端口返 `403`；P3-2 错误 `400+` 与未消费体 POST 关闭连接 + `ui-ticket`/`logout` 消费可选体 + 严格分帧校验；P3-3 `CONSILIUM_SOCKET_FALLBACK_DIR` 隔离真实 `/tmp` 残留；P3-4 登录区明确区分 `127.0.0.1` 与 `localhost`。 | 完成 |
| **5. 后续阶段待办同步 OpenSpec** | 已将文件树/Diff/Git §6 隔离接口（阶段 5）、界面直接发消息（阶段 3 后）、本地 JSONL 只读镜像（阶段 6）、自动分配（阶段 5）、子 Agent 结构化字段（阶段 3）与菜单栏 App（阶段 4.5）悉数登记进 [tasks.md](../openspec/changes/consilium-v1/tasks.md)。 | 完成 |

> **与用户原要求相比的设计微调记录（供审阅）**：原指令要求输入 `reviews/R013-antigravity-chat-ui.md` 并建议右侧五个标签名及 `/` 指令集合。本实现保留仓库既有文件名 `reviews/R013-antigravity-chat-ui-and-q4.md` 以维持 `R012` 与索引既有相对链接；抽屉标签按 R012 §3 对齐定为 `任务 / 工作树 / 模型与账号 / 子 Agent / 文件`；`/` 面板保留 `/mission`、`/files`、`/export` 并将其余标签快捷跳转统一纳入前端状态切换，无新增后端路由。

---

## 4. 全量验证与媒体产物证据

### 4.1 `openspec validate consilium-v1 --strict`

```text
Change 'consilium-v1' is valid
```

### 4.2 双环境变量模式完整单测（`10` 模块 `75` 项）

测试模块：`test_dashboard_and_sse.py`、`test_installer_dry_run.py`、`test_mcp_transports.py`、`test_quota_extraction.py`、`test_r006_regressions.py`、`test_r010_regressions.py`、`test_r011_regressions.py`、`test_r012_regressions.py`、`test_store_and_loop_guard.py`、`test_wakeup_and_cold_fallback.py`。

```text
# 模式 A：隔离环境变量（剥离 CLAUDE_CONFIG_DIR + CONSILIUM_DRY_RUN=1 + CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1）
...........................................................................
----------------------------------------------------------------------
Ran 75 tests in 17.785s

OK

# 模式 B：默认未设环境变量（剥离 CLAUDE_CONFIG_DIR、CONSILIUM_DRY_RUN、CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC）
...........................................................................
----------------------------------------------------------------------
Ran 75 tests in 23.164s

OK
```

### 4.3 `2026-11-20` 演示截图清单（均在 daemon SSE 在线期内采集）

- DOM 与浏览器校验：无 JS 异常、无控制台报错、登录后无 HTTP 错误；Light 与 Dark `1440×960` 顶部均稳定显示 `conn === "SSE 实时"`、`Claude standby`、`ChatGPT/Codex online`、`Antigravity working`；`390×844` 浅/深双方实测 `scrollWidth=390, innerWidth=390`。
- 录屏清理说明：按用户 2026-11-20 要求（“不用录屏，录屏也删掉好了，以后只用截图”），已删除全部历史与本轮 `.mp4` 录屏文件，后续外观校验统一只提供截图。
- 产物路径（保留 `2026-11-19` 与首版历史 PNG 截图）：
  - [dashboard_chat_light_1440_2026-11-20.png](../../../13_Consilium/03_%E6%BC%94%E7%A4%BA%E8%AE%B0%E5%BD%95/dashboard_chat_light_1440_2026-11-20.png) (`425K`)
  - [dashboard_chat_dark_1440_2026-11-20.png](../../../13_Consilium/03_%E6%BC%94%E7%A4%BA%E8%AE%B0%E5%BD%95/dashboard_chat_dark_1440_2026-11-20.png) (`431K`)
  - [dashboard_chat_light_390_2026-11-20.png](../../../13_Consilium/03_%E6%BC%94%E7%A4%BA%E8%AE%B0%E5%BD%95/dashboard_chat_light_390_2026-11-20.png) (`239K`)
  - [dashboard_chat_dark_390_2026-11-20.png](../../../13_Consilium/03_%E6%BC%94%E7%A4%BA%E8%AE%B0%E5%BD%95/dashboard_chat_dark_390_2026-11-20.png) (`246K`)

---

## 5. 改动文件清单

- 核心代码：[scripts/install.py](../scripts/install.py)、[consilium/mcp_server.py](../consilium/mcp_server.py)、[consilium/store.py](../consilium/store.py)、[consilium/quota.py](../consilium/quota.py)、[consilium/dashboard.py](../consilium/dashboard.py)、[adapters/skill/consilium-collab/SKILL.md](../adapters/skill/consilium-collab/SKILL.md)
- 测试套件：[tests/test_quota_extraction.py](../tests/test_quota_extraction.py)、[tests/test_r011_regressions.py](../tests/test_r011_regressions.py)、[tests/test_r012_regressions.py](../tests/test_r012_regressions.py)
- 规范与文档：[openspec/changes/consilium-v1/design.md](../openspec/changes/consilium-v1/design.md)、[openspec/changes/consilium-v1/tasks.md](../openspec/changes/consilium-v1/tasks.md)、[openspec/changes/consilium-v1/specs/bus/spec.md](../openspec/changes/consilium-v1/specs/bus/spec.md)、[openspec/changes/consilium-v1/specs/dashboard/spec.md](../openspec/changes/consilium-v1/specs/dashboard/spec.md)、[reviews/README.md](./README.md)、[README.md](../README.md)、[docs/architecture.md](../docs/architecture.md)、[13_Consilium/README.md](../../../13_Consilium/README.md)
