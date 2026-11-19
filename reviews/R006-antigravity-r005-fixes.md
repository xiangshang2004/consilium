# R006：Antigravity 阶段 0（OpenSpec）与阶段 1（R005 7 项 P1 + P2）整改交付

- **作者**：Antigravity（Gemini）
- **日期**：2026-11-19
- **范围**：`consilium-v1` 阶段 0（OpenSpec 落盘，提交 `9cc0435`）与阶段 1（本提交，按 [R005](./R005-claude-recheck.md) 第 3、4、6 节与 [Consilium 路线图](../../../13_Consilium/路线图.md) 第二节完成全部修复及回归测试）
- **隔离核验**：未修改 `~/.claude.json`、`~/.codex/config.toml`、`~/.gemini/config/mcp_config.json`、`~/Library/LaunchAgents/`；未装 launchd；未在后台启动 `codex exec`；未 push。
- **关口状态**：**★ 等待 Claude 与 Codex 复核通过**（按路线图要求，不越过关口；阶段 2 看板在独立提交 R007 中交付）。

---

## 0. 用户原话与 AI 建议区分

- **用户原话**：要求尽快把 Consilium 做成“完善且美丽”、可日常使用的版本，按 `/Users/up/code/13_Consilium/路线图.md` 第四节顺序推进（先阶段 0 与阶段 1，阶段 2 分开提交，带 ★ 关口停下来等复核/用户确认），并严格遵守第六节全部隔离与提交约束。
- **对路线图/建议的兼容性调整**：路线图第二节约定写操作返回 `{"committed": bool, "result": {...}, "delivery": {...}}`。为避免破坏既有调用方与 A 级脚本，本轮所有写工具在返回标准 `committed` / `result` / `delivery` 信封的同时，保留了顶层领域字段（如 `mission`、`tasks`、`task`、`deliveries`、`allowed`、`round`、`status`），做到严格满足新契约且 100% 向后兼容。

---

## 1. 改动文件清单

| 文件 | 改动摘要 |
|---|---|
| `deskbell/models.py` | 默认 `DEFAULT_CLAIM_TIMEOUT_SEC = 900`（15m）、`DEFAULT_RUNNING_TIMEOUT_SEC = 7200`（2h）、`DEFAULT_REAPER_INTERVAL_SEC = 60.0`；新增 `read_auth_token` / `write_auth_token` / `generate_auth_token`；修正 `resolve_rendezvous_dir(override, fallback_dir)` 优先级（`override > DESKBELL_RENDEZVOUS_DIR > fallback_dir > /tmp/deskbell-<uid>`） |
| `deskbell/wakeup.py` | 新增 `signals/<peer>.seq.lock` 线程锁 + `fcntl.flock(LOCK_EX)`；`write_peer_seq` 支持锁内 `query_fn` 从 DB 重查；新增 `rebuild_all_peer_seqs`；`_wait_via_kqueue` 监听 `KQ_NOTE_DELETE \| KQ_NOTE_RENAME` 自动重挂并过滤空唤醒；预检同时检查 `queued_tasks`；`ColdDoorbell.build_deeplink` 统一 `claude://code/new?q=...` 与 `codex://new?prompt=...&path=...` |
| `deskbell/store.py` | 新增 `idempotency_keys` 表与 `client_request_id` 幂等支持；`sync_peer_seq` 锁内重查 DB `(max_event_id, unacked, queued_tasks)`；`release_stale_claims` 支持 `claimed` + `running` 超时回收并向协调者发 `task_requeued`；新增 `requeue_task` 与 `record_event_delivery`；`request_review` 校验已关闭 mission 与 `task_id` 归属；各写路径更新 `peers` 活跃时间与状态；配额忽略过期耗尽记录且无数据时 `dispatch_recommended = None` |
| `deskbell/quota.py` | 保留 Codex `window_minutes` 动态命名；不再将 `limit_id` 当作 `account_id`；只解析含 `"rate_limits"` 的行且仅保留数值字段；支持 `DESKBELL_USER_HOME` / `DESKBELL_DISABLE_LOCAL_QUOTA_SYNC` 隔离 |
| `deskbell/mcp_server.py` | `deskbell_wait` schema 与实现默认 `auto_pickup=False`；写工具统一返回 `committed`/`result`/`delivery` 且 `ring_peer` 异常隔离（`isError=False`）；启动时重建 `.seq`；后台 60s 回收线程 + `pickup_inbox` 前置回收；动态检测 `auth.token` inode/mtime 实现 `rotate-token` 即时生效；严格布尔/批处理/params/`Content-Length` 校验 + 30s HTTP 读超时；协调者自触发 loop guard 不给自己弹冷通知 |
| `deskbell/cli.py` | 修复全部子命令 `--data-dir` 覆盖 `DESKBELL_RENDEZVOUS_DIR`；`serve` 新增 `--claim-timeout` / `--running-timeout`；新增 `deskbell rotate-token` 子命令 |
| `scripts/install.py` | 改为严格两阶段（先读入并校验全部 JSON/TOML/CLI/`Claude.app` 运行态，任一失败不写任何字节）；Antigravity 改用 `headers` 彻底移除 `?token=`；Codex 重复表校验覆盖引号表头与点号键；调用 `claude` 前显式剥离 `CLAUDE_CONFIG_DIR` 并在 `add-json` 失败时自动回滚旧条目；新增 `--rendezvous-dir` 与 `rotate_token()` |
| `adapters/antigravity/mcp_config.snippet.json` | 从 `?token=` 改为 `headers: {"Authorization": "Bearer __DESKBELL_TOKEN__"}` |
| `adapters/skill/deskbell-collab/SKILL.md` | 删除“按 Return 加入”说法，改为收到通知后手动切 App 调 `deskbell_pickup_inbox`，并明确 `deskbell_wait` 默认 `auto_pickup=false` |
| `docs/architecture.md` | 同步更新 R006 架构说明（含 P2.6 补发冷通知守护线程说明） |
| `tests/test_r006_regressions.py` | 新增 16 项回归测试，逐条覆盖 R005 3.1–3.7、P2.1–P2.5 及 A 级演示发现的问题 |
| `tests/test_installer_dry_run.py`, `tests/test_wakeup_and_cold_fallback.py`, `tests/test_quota_extraction.py` | 同步更新头文件令牌、`&path=` 深链、隔离 `DESKBELL_USER_HOME` 与 `claude_running_check` 断言 |

---

## 2. R005 3.3 令牌调查结论

1. **Antigravity（`~/.gemini/config/mcp_config.json`）**：
   - 核实 Antigravity 内置定制 schema（`migrate-agent-settings/SKILL.md`：`command, args, env, cwd, url, headers, disabled, variables`），**原生支持 `headers`**。
   - 因此已将 `scripts/install.py` 与 `adapters/antigravity/mcp_config.snippet.json` 全部切换为 `"headers": {"Authorization": "Bearer <token>"}`，安装产物中已彻底消除 `?token=`；服务端 `log_message` 保持静默，不记录 query string。
2. **Claude Code（`~/.claude.json`）**：
   - 实测 `claude mcp add-json -s user deskbell '<json>'`（Claude Code 2.1.231）可正常写入 `type: "http"` 与 `headers`，且支持 `${ENV_VAR}` 语法；但从 macOS GUI 启动的 `Claude.app` 不会继承终端 shell 环境变量，因此仍需写入实际令牌值。
   - **已记录的风险**：安装或轮换时，子进程 `claude mcp add-json` 的 `argv` 会短暂携带令牌（约数十毫秒，仅本机同一 UID 可见）。
   - 同时实测发现：设置 `CLAUDE_CONFIG_DIR` 时 `claude` 会把 `.claude.json` 写到 `$CLAUDE_CONFIG_DIR/.claude.json`；**剥离 `CLAUDE_CONFIG_DIR` + 指定 `HOME=<target_home>`** 可严格隔离写入 `<target_home>/.claude.json`，绝不污染宿主 `~/.claude.json`。
3. **令牌轮换（`deskbell rotate-token`）**：
   - 先校验并更新所有已配置客户端（Antigravity / Claude Code / Codex），**最后**原子替换 `<state_dir>/auth.token`。
   - `DeskBellDaemon.get_active_token()` 通过比对 `auth.token` 的 `(st_ino, st_mtime_ns)` 实时重载，轮换后旧令牌立即返回 `401`，新令牌立即生效。

---

## 3. 逐项验收对照

| R005 项 | 状态 | 对应回归测试 |
|---|---|---|
| 3.1 `deskbell_wait` 默认 `auto_pickup=false` | 已修 | `test_3_1_wait_defaults_no_auto_pickup_and_timeout_leaves_task_queued` |
| 3.2 `.seq` 加锁、启动重建、`.sig` 删除恢复 | 已修 | `test_3_2_seq_concurrent_writers_converge_to_db_truth`、`test_3_2_stdio_rebuilds_deleted_signals_dir`、`test_3_2_kqueue_recovers_after_sig_deleted_during_wait` |
| 3.3 令牌移出 URL + `rotate-token` | 已修 | `test_apply_idempotency_token_paths_and_safe_uninstall`、`test_3_3_rotate_token_and_live_daemon_reload` |
| 3.4 `claimed` + `running` 超时回收接入 daemon 与 pickup | 已修 | `test_3_4_daemon_reaper_requeues_expired_running_task_and_notifies_coordinator` |
| 3.5 通知异常隔离（`committed=true`）+ `client_request_id` 幂等 | 已修 | `test_3_5_ring_error_returns_committed_true_and_idempotency_prevents_duplicates` |
| 3.6 深链 `&path=` / `claude://code/new` + `SKILL.md` 更正 | 已修 | `test_deeplink_utf8_pre_encode_truncation_and_codex_prompt_param`、`test_3_6_skill_md_removes_press_return_claim` |
| 3.7 两阶段安装器、运行中 `Claude.app` 拒绝、回滚、`--rendezvous-dir`、`CLAUDE_CONFIG_DIR` | 已修 | `test_3_7_codex_duplicate_leaves_all_other_configs_untouched`、`test_3_7_running_claude_app_refuses_apply`、`test_3_7_claude_add_failure_restores_previous_entry_and_strips_config_dir` |
| P2.1–P2.5 + A 级发现 | 已修 | `test_p2_1_*`、`test_p2_2_*`、`test_p2_3_*`、`test_p2_5_*`、`test_demo_a_findings_*` |

---

## 4. 全套测试与回归输出

### 4.1 单元测试（34 项全部通过，0 warning）

```text
$ cd /Users/up/code/github/deskbell && env -u CLAUDE_CONFIG_DIR DESKBELL_DRY_RUN=1 /usr/bin/python3 -B -m unittest discover -s tests -v
test_apply_idempotency_token_paths_and_safe_uninstall (test_installer_dry_run.InstallerTests) ... ok
test_dry_run_does_not_mutate_files (test_installer_dry_run.InstallerTests) ... ok
test_duplicate_codex_toml_table_outside_block_rejected (test_installer_dry_run.InstallerTests) ... ok
test_http_default_token_security_headers_and_405 (test_mcp_transports.MCPTransportsTests) ... ok
test_stdio_lifecycle_protocol_negotiation_and_close_mission (test_mcp_transports.MCPTransportsTests) ... ok
test_mcp_deskbell_quota_manual_report_and_status (test_quota_extraction.QuotaExtractionTests) ... ok
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
Ran 34 tests in 6.034s

OK
```

### 4.2 A 级端到端回归（`13_Consilium/01_演示脚本/run_demo_a.py`）

- 退出码 `0`，总耗时 `1970.58 ms`，临时目录自动清理。
- 热唤醒延迟 `5.41 ~ 12.41 ms`（7 次 `kqueue` 全部命中），冷通知 1 次（`dry_run=True`），轮次上限在第 4 次 `request_review` 精确生效（`3/3`），`close_mission` 正常流转至 `completed`。

---

## 5. 已知限制（v1 接受）

1. **共享 Bearer 令牌身份（R005 P2.4）**：三端共用同一份本机 Bearer 令牌，暂无法从传输层强认证 `peer_id`；v1 本机单用户场景先接受，后续可拆分为每端独立令牌。
2. **Claude CLI 安装/轮换瞬态 argv**：`claude mcp add-json` 调用瞬间参数含 Bearer 令牌（仅同一本机用户可见）。
3. **补发冷通知定时器（R005 P2.6）**：`threading.Timer` 为守护线程，仅在常驻 `serve` / `mcp-stdio` 进程存活期间有效。
4. **Antigravity 冷通知深链**：目前固定为 `antigravity://`，暂未验证可传 prompt/workspace 参数的原生深链格式。
5. **实时配置核验**：`~/.claude.json` 不含 `deskbell`，`~/.codex/config.toml` 不含 `deskbell`，`~/.gemini/config/mcp_config.json` 仍为 0 字节，`~/Library/LaunchAgents/com.up.deskbell.plist` 不存在。
