# R007：Antigravity 阶段 2（Consilium 离线实时看板 `/ui` + SSE）交付报告

- **作者**：Antigravity（Gemini）
- **日期**：2026-11-19
- **范围**：`consilium-v1` 阶段 2（独立于 R006 `a083ba3` 的单独提交）——离线单页实时看板 `/ui`、`/api/state`、`/api/stream`（SSE）、Cookie 令牌认证与双确认写操作。
- **隔离核验**：未修改 `~/.claude.json`、`~/.codex/config.toml`、`~/.gemini/config/mcp_config.json`、`~/Library/LaunchAgents/`；未装 launchd；未在后台启动 `codex exec`；未 push。
- **关口状态**：**★ 关口 2：等待用户查看浅色/深色截图与实时录屏后确认外观**（同时阶段 3 需等待 ★ 关口 1 的 Claude/Codex 对 R006 复核通过与 ★ 关口 2 用户确认通过后才推进）。

---

## 0. 用户原话与 AI 建议区分

- **用户原话**：
  - 要求把 Consilium（deskbell 三端协作）做成「完善且美丽」、可以日常使用的版本，按 `<external-demo-dir>/路线图.md` 第四节顺序推进；阶段 2 看板与阶段 1 分开提交，带 `★` 的关口停下来等用户确认或复核，不越过；严格遵守第六节约束。
- **对路线图第三节建议的落实与微调**：
  1. **保留全部克制视觉与交互要求**：不引任何外网 CDN/字体/图表库；原生系统字体栈 + `tabular-nums`；浅色/深色/跟随系统三态（`prefers-color-scheme` + `localStorage`）；三端以柔和主题色区分（Claude 暖陶土、ChatGPT/Codex 翠绿、Antigravity 靛蓝）不放品牌 Logo；四态状态灯（待命绿、工作中蓝、离线灰、陈旧橙）；缺失配额严格显示 `未知` 而非 `100%`；交互只保留「关闭 Mission」和「任务重新入队」两项且均有二次确认弹窗。
  2. **补充了两项可观测性增强**：
     - 在 `deskbell/store.py` 写入事件时记录 `ring_sent_at_ms` 并在热唤醒返回时仅对未确认事件（`acked_at = ''`）写入 `wakeup_latency_ms`，使事件时间线能直接展示精准的毫秒级热唤醒延迟（实测 `1.51–7.18 ms`）与冷通知标签。
     - 在 `GET /api/stream` 的 0.5 秒空闲检查中同时追踪 `latest_event_id` 与 `active_waiters` 变化：不仅能感知外部 `stdio`/CLI 写库，也能在 CLI `deskbell wait` 获取/释放待命租约时（零 DB 写入）实时推送 `waiter_change` 帧，立即把对应端卡片切换为绿色「待命」。
  3. **回应用户实测反馈「不要让我手动找令牌」——新增 `deskbell ui` 一键免密直达**：
     - 新增 `POST /api/ui-ticket`（需 Bearer 令牌认证，签发 30 秒有效、用一次即焚的随机登录票据）与 `GET /ui/auth?ticket=...`（消耗票据 -> 签发 `HttpOnly; SameSite=Strict` 会话 Cookie + `Cache-Control: no-store` -> 302 跳转到干净 `/ui`）。
     - 新增 `python3 -m deskbell.cli ui` 子命令自动读取本机 `auth.token` 换取一次性票据并在浏览器打开已登录的 `/ui`；A 级演示脚本同步新增 `--open-ui` 参数一键拉起已登录看板。长效 `auth.token` 依然绝不进入 URL，且浏览器首次登录一次后刷新免密保持。
  4. **演示脚本配套增强**（在 `<external-demo-dir>/01_演示脚本/run_demo_a.py`，不污染 `github/deskbell` 核心逻辑）：增加 `--open-ui`、`--start-delay-sec`、`--hold-sec`、`--token-file`、`--seed-demo-ui` 可选参数，默认无参数运行行为与原 A 级演示完全一致，同时支持配合看板现场演练与截图录屏。

---

## 1. 改动文件清单

| 文件 | 改动摘要 |
|---|---|
| `deskbell/dashboard.py` | 新增离线单页看板模块：`LoginTicketStore`（30 秒一次性用后即焚登录票据）、`EventHub`（线程安全 SSE 订阅广播器）、`derive_session_cookie`（`HMAC-SHA256(active_token, "consilium-ui-session-v1")`）、`parse_cookie_header`、`build_dashboard_state`、`render_dashboard_html`（完整内联 CSS + SVG 图标 + 原生 JS + 轻量安全 Markdown 渲染器） |
| `deskbell/store.py` | 新增 `get_dashboard_missions`（返回任务看板计数、完整任务列表、倒序事件列表及 `latest_event_id`）；`_insert_event` 写入 `ring_sent_at_ms`；新增 `record_peer_wakeup_latency`（严格过滤 `acked_at = ''` 避免后次唤醒覆写前次延迟） |
| `deskbell/mcp_server.py` | `DeskBellDaemon` 挂载 `GET /`（302 -> `/ui`）、`GET /ui`（严格离线 CSP）、`GET /ui/auth`（消费一次性票据并签发会话 Cookie）、`POST /api/ui-ticket`（仅 Bearer 鉴权签发 30s 单次票据）、`POST /api/login`（签发 `HttpOnly; SameSite=Strict` 会话 Cookie）、`POST /api/logout`、`GET /api/state`、`GET /api/stream`（SSE 实时流，支持 `?max_events=N` 及 `external_db` / `waiter_change` 检测）、`POST /api/missions/<id>/close`、`POST /api/tasks/<id>/requeue`；全部写工具提交后向 `EventHub` 广播最新快照 |
| `deskbell/cli.py` | 新增 `deskbell ui` 子命令：自动读取本地 `auth.token` 换取一次性票据并在默认浏览器直接打开已登录的 `/ui` |
| `tests/test_dashboard_and_sse.py` | 新增 6 项测试：离线 HTML / CSP / Host & Origin 校验、Cookie 登录 / `/api/state` 结构与未知配额 `null` / 退出登录、看板重排队与关闭 Mission 写操作、有序 SSE 推送帧、零 DB 写入 `waiter_change` SSE 推送与 `wakeup_latency_ms` 仅写未确认事件验证、以及一次性票据自动登录与用后即焚防重放验证 |
| `openspec/changes/consilium-v1/tasks.md` | 勾选阶段 2（`2.1–2.4`），保持 `★ Gate 1` 与 `★ Gate 2` 未勾选 |
| `reviews/README.md`、`README.md` | 增量更新评审索引与项目状态摘要 |

---

## 2. 接口、安全与实时推送设计

1. **零外网依赖与 CSP**：
   - `GET /ui` 响应头固定附带 `Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'` 与 `X-Content-Type-Options: nosniff`。
   - 单页 HTML 不包含任何 `http://` / `https://` 外部资源链接（已由自动化单测断言）。
2. **令牌认证（不进 URL）**：
   - 页面首次加载请求 `/api/state` 返回 `401` 时弹出令牌输入框；提交后调用 `POST /api/login` 校验当前生效令牌（支持 `deskbell rotate-token` 动态轮换，轮换后旧 Cookie 自动失效）。
   - 校验通过后签发 `deskbell_session=<hmac>; HttpOnly; SameSite=Strict; Path=/`，后续 `/api/state`、`/api/stream` 与写接口自动复用 Cookie；所有端点继续校验 `Host` 与跨源 `Origin`（跨源返回 `403`）。
3. **实时推送与自动降级**：
   - 前端优先使用 `EventSource('/api/stream')` 订阅有序 `state` / `update` 帧，顶部状态指示「SSE 实时」。
   - 若 SSE 断线，页面自动切换「轮询降级 (3s)」定时拉取 `/api/state`，同时每隔 6 秒尝试重连 SSE，恢复后自动切回实时流。
4. **双确认克制操作**：
   - 仅暴露两项写操作：`POST /api/missions/<id>/close`（支持自定义汇总文本、状态选择及强关勾选）和 `POST /api/tasks/<id>/requeue`（将 `claimed`/`running`/`failed`/`blocked` 任务退回 `queued`）。两者在点击前均弹出二级确认弹窗，提交后由服务端按冻结格式返回 `{"committed": true, "result": ..., "delivery": ...}` 并立即推送 SSE 刷新。

---

## 3. 视觉交付物（★ 请用户过目）

全部截图与录屏已存放在 `<external-demo-dir>/03_演示记录/`（未引入临时产物进仓库）：

| 交付物 | 路径 | 说明 |
|---|---|---|
| 浅色模式截图 | `dashboard_light.png`（早期外部演示归档） | `1440×2029 @2x`：三端卡片（Claude 绿灯待命 78%、ChatGPT 蓝灯工作中 44%、Antigravity 蓝灯工作中 + 未知配额）、已完成与进行中双 Mission、单行 4 列任务看板、Markdown 最终汇总、倒序事件时间线与 `1.51–7.18 ms` 热唤醒徽章 |
| 深色模式截图 | `dashboard_dark.png`（早期外部演示归档） | `1440×2029 @2x`：同页面切换深色模式后的高对比度低眩光配色效果 |
| SSE 实时演进录屏 | `dashboard_live_demo.mp4`（早期外部演示归档） | 30 帧动态录屏，展示 A 级脚本驱动三端协作时，页面无需手动刷新、通过 SSE 实时出现 Mission、任务流转、3 轮交叉审核与触发轮次上限的全过程 |
| A 级时间线记录 | `demo_a_timeline.md`（早期外部演示归档） | 纯标准库端到端跑通时间线（`1981.43 ms`，退出码 0，无残留临时目录） |

---

## 4. 全套测试与验证输出

### 4.1 单元测试（40 项全部通过，0 warning）

```text
$ cd <repo-root> && env -u CLAUDE_CONFIG_DIR DESKBELL_DRY_RUN=1 /usr/bin/python3 -B -W default -m unittest discover -s tests -v
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
Ran 40 tests in 7.036s

OK
```

### 4.2 OpenSpec 与真实浏览器渲染核验

- `openspec validate consilium-v1` -> `Change 'consilium-v1' is valid`。
- 浏览器联调与 Headless Chrome CDP 实时 DOM 核验：
  - 连接状态：`SSE 实时`
  - 三端活跃汇总：`3 / 3 端活跃`（Claude 绿色 `待命`、ChatGPT / Codex 蓝色 `工作中`、Antigravity 蓝色 `工作中`）
  - 配额展示：`["78%", "44%", "未知"]`
  - 实时唤醒徽章：`⚡ 热唤醒 · 1.51 ms` ~ `7.18 ms` 与 `🔔 冷通知 (dry-run)`

---

## 5. 下一步关口

按路线图要求，本轮在此停步，等待：
1. **★ 关口 1**：Claude 与 Codex 对 [R006](./R006-antigravity-r005-fixes.md)（提交 `a083ba3`）复核通过；
2. **★ 关口 2**：用户审阅 `dashboard_light.png`、`dashboard_dark.png` 与 `dashboard_live_demo.mp4`，确认看板布局、配色与信息密度满意（如需微调，可直接在进入阶段 3 前调整）。

两道关口通过后，再推进阶段 3（B 级真 App 试跑：项目级 `.mcp.json` / `.codex/config.toml`）与阶段 4（`deskbell doctor` + `--dry-run` 经用户同意后的 C 级正式安装）。
