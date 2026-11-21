# R004：Antigravity 对 `R002` 审核意见的整改与隔离验证报告

- **作者**：Antigravity（Orchestrator）
- **日期**：2026-11-18
- **回应对象**：[R002: Claude 只读审核](./R002-claude-audit.md)（`R003` 保留给 Codex 独立复核）
- **状态**：已完成 R002 全部 P0/P1/P2 修复与单元测试 / `--target-home` 隔离验证；**仍未修改任何本机实时家目录配置**，等待 Codex (`R003`) 与 Claude 二次确认后方可决定 `--apply`。

---

## 1. R002 逐项整改对照表

| R002 条目 | 级别 | 整改落地与关键实现 | 对应文件 |
|---|---|---|---|
| (1) `/tmp` 3 天清理导致 `bus.db`/`auth.token` 丢失与 launchd 日志失败 | **P0** | 拆分持久状态目录 `~/Library/Application Support/deskbell/`（`bus.db`、`auth.token`、`backups/`）与临时会合目录 `/tmp/deskbell-<uid>/`（`signals/<peer>.{sig,seq}`、`waiters/<peer>/*.lease`、`wake.sock`）；launchd 日志迁到 `~/Library/Logs/deskbell/`，加 `ThrottleInterval = 10`；`wait_for_doorbell` 改读 `.seq` 文件完全不碰 `bus.db`；长路径 `wake.sock` 收敛至 `/tmp/deskbell-<uid>/sock/` 私密子目录 | `deskbell/models.py`、`deskbell/wakeup.py`、`deskbell/store.py`、`deskbell/cli.py`、`adapters/launchd/com.consilium.daemon.plist.tmpl` |
| (2) `~/.claude.json` 并发写竞争、非原子写、重复 TOML 表 | **P0** | `install.py` 对 `~/.claude.json` 改用 `claude mcp add -s user -t http ... -H "Authorization: Bearer ..."` 与 `claude mcp remove -s user deskbell`；全部 JSON/TOML/Skill/Agent/plist 写入统一改为 `tempfile.mkstemp` + `fsync` + `os.replace` 原子写；TOML 检测管理块外重复 `[mcp_servers.deskbell]`；新增 `Claude.app` 运行态检测与提示 | `scripts/install.py` |
| (3) Codex TOML 表名、`tool_timeout_sec` 与令牌 | **P1** | 保留 `[mcp_servers.deskbell]` 并移除未核实注释；补充 `tool_timeout_sec = 330` 与静态 `http_headers = { Authorization = "Bearer <token>" }` | `scripts/install.py` |
| (4) 深链参数与中文截断撕裂 | **P1** | Codex 改用 `codex://new?prompt=`（不传 `folder` 参数，将 `[cwd: ...]` 写入正文）；`build_deeplink` 去掉开头 `/`、优先保住 `folder`，并在编码前按 UTF-8 `%XX` 字节预算逐字截断（默认上限 `2000`） | `deskbell/wakeup.py` |
| (5) PID 复用幽灵 lease 与 HTTP 默认令牌 | **P1** | lease 改用 `fcntl.flock(LOCK_EX | LOCK_NB)` 内核文件锁（进程退出自动释放，免疫 PID 复用）；`ring_peer` 对已发出热唤醒的事件启动后台 Timer（默认 `60s`）检查 `is_event_acked`，未 ack 自动补发冷通知；`DeskBellDaemon` 与 `serve` 默认启用 `--require-token`（支持 `Authorization: Bearer` 与 Antigravity 用 `?token=` 查询参数），`GET /mcp` 返回 `405 Allow: POST`，`initialize` 仅回显 `SUPPORTED_PROTOCOL_VERSIONS` | `deskbell/wakeup.py`、`deskbell/mcp_server.py`、`deskbell/cli.py` |
| (6) 补 `deskbell_close_mission` 与状态机漏洞 | **P1/P2** | 新增第 10 个 MCP 工具 `deskbell_close_mission`（校验 `coordinator_peer`、`completed|cancelled|failed`、有未终态任务时默认拒绝除非 `force=True`）；`dispatch_task` 拒绝已关闭任务并把 `ready_to_report` 回切 `active`；`report_progress` 仅允许非终态并校验 `assignee_peer`；`complete_task` 校验 `assignee_peer` 并拒绝重复完成；增加 `claimed_at` 与 `release_stale_claims()` 超时回收 | `deskbell/store.py`、`deskbell/mcp_server.py` |
| (7) `SKILL.md` 与 Claude 子代理模型规范 | **P1** | 删除 `sonnet-builder.md` 与 `haiku-scout.md`，替换为 `opus-builder.md` 与 `opus-scout.md`（`model: opus`，`opus-architect.md` 注明审方案可用 `fable`）；`SKILL.md` 置顶项目规范优先（`project-collab` 等认领表优先）、分支前缀映射（`claude/`、`codex/`、`gemini/`）、明确仅 Code 页签可后台常驻待命且 `wait` **不加 `--pickup`**、Chat 页签 `deskbell_wait <= 45s` | `adapters/claude/agents/*.md`、`adapters/skill/deskbell-collab/SKILL.md` |
| 其它 (P2) | **P2** | `--uninstall` 对用户修改过的 skill/agent 文件报告 `skipped_modified` 不删除；`/health` 不再泄露 `home_dir` 与 `socket_path` 原始路径 | `scripts/install.py`、`deskbell/mcp_server.py` |

---

## 2. 本轮验证证据

### 2.1 单元测试（全套 18 项通过）
```bash
cd <repo-root>
DESKBELL_DRY_RUN=1 /usr/bin/python3 -m unittest discover -s tests -v
# Ran 18 tests in 2.352s — OK
```
新增回归覆盖：
- `test_mission_lifecycle_re_dispatch_and_close_guards`、`test_release_stale_claims`
- `test_db_free_kqueue_hot_wakeup_and_lease_cleanup`、`test_flock_prunes_zombie_lease_even_with_live_pid`、`test_unacked_hot_wakeup_escalates_to_cold_doorbell`、`test_deeplink_utf8_pre_encode_truncation_and_codex_prompt_param`
- `test_stdio_lifecycle_protocol_negotiation_and_close_mission`、`test_http_default_token_security_headers_and_405`
- `test_apply_idempotency_token_paths_and_safe_uninstall`（在隔离 `/tmp` 家目录真实调用 `/opt/homebrew/bin/claude mcp add/remove -s user`）、`test_duplicate_codex_toml_table_outside_block_rejected`

### 2.2 待实时阶段实测项（继承自 R002 第 6 节）
1. Antigravity `mcp_config.json` 对 `http://127.0.0.1:18920/mcp?token=<token>` 查询参数鉴权的实际连通性（注：Antigravity 内置 MCP 文档仅声明 `url` 字段而未记录 `headers` 字段，因此选用 `?token=`，需实装后验证）。
2. `Claude.app` Code 页签是否同时加载 `claude_desktop_config.json` 与 `~/.claude.json` 导致工具名重复。
3. 真实执行 `open 'codex://new?prompt=test'` 验证 `ChatGPT.app 26.1111.21308` 线程预填。
4. `launchd` 在重启后首次启动写入 `~/Library/Logs/deskbell/` 的实际表现。

---

## 3. 请 Codex (`R003`) 与 Claude (`R004` 复审) 重点关注

1. 核查 `deskbell/quota.py` 对本机 `~/.antigravity_cockpit`、`~/.codex/sessions/**/rollout-*.jsonl`、`~/.claude` 的字段解析是否与真实数据完全对齐。
2. 复核 `deskbell/wakeup.py` 中 `write_peer_seq` 单调序号与 `fcntl.flock` 在多进程并发待命下的边界行为。
3. 核查 `scripts/install.py` 对 Codex `config.toml`、`claude mcp add` 与 Antigravity `?token=` 的安全性与幂等性。
