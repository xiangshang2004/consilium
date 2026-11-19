# `consilium` Cross-Agent Review Index

> 2026-11-19 用户要求把项目名从 deskbell 改为 **Consilium**：包名、命令、MCP 工具名（`consilium_*`）、环境变量（`CONSILIUM_*`）、路径和仓库目录都已改。R001–R008 与 `openspec/changes/archive/` 是历史记录，保留原名不改。

- [R001: MVP Design & Implementation Audit Handover](./R001-mvp-design-and-impl.md) — Built by Antigravity (`Gemini`), awaiting audit by `Claude.app` before touching live home configs.
- [R002: Claude Read-Only Audit](./R002-claude-audit.md) — Claude 的只读审核，回复 R001 的 6 个问题：暂不 `--apply`；修完 P0/P1（/tmp 清理、`~/.claude.json` 写入、深链、lease、令牌、子代理模型）后再带 R004 回来（R003 留给 Codex 复核）。
- [R003: Codex Independent Review](./R003-codex-review.md) — Codex 版本行为、R002 逐项判定、漏项及 R004 差异复核。
- [R004: Antigravity R002 Remediation & Isolated Verification](./R004-antigravity-r002-fixes.md) — 已完成 R002 全部 7 项修复并在 `--target-home` 隔离目录与 18 项单测中验证通过（R003 留给 Codex 独立复核）。
- [R005: Claude Re-check of R003/R004](./R005-claude-recheck.md) — 在 `48acd1a` 上核实 18 项测试通过、实时配置未被改动；认可 R003 对 R002 的更正；列出 7 个 P1（wait 自动认领、`.seq` 竞态与重建、令牌传递、认领回收、写库与通知失败、冷通知、安装器原子性），交 Antigravity 以 R006 修复。
- [R006: Antigravity Stage 0 & Stage 1 (R005 P1/P2 Fixes)](./R006-antigravity-r005-fixes.md) — 落盘 `consilium-v1` OpenSpec，修完 R005 全部 7 个 P1 与 P2 项及 A 级演示问题，34 项单元测试与 A 级端到端回归全部通过；★ 等待 Claude 与 Codex 复核。
- [R007: Antigravity Stage 2 Consilium Live Dashboard](./R007-antigravity-dashboard.md) — 实现离线单页 `/ui` 看板、`/api/state`、`/api/stream`（SSE）、`consilium ui` 一次性票据一键免密登录与双确认操作，40 项测试通过并完成浅色/深色截图及实时录屏；★ 等待用户确认看板视觉效果。
- [R008: Claude Review of R006/R007](./R008-claude-r006-r007-review.md) — 关口 1：Claude 侧通过（40 项测试、实时配置未动），P2 三项（删除 `?token=`、`running` 超时不自动入队、Claude.app 运行时可装其余部分）；关口 2：看板好看、免密登录实测通过，建议先改“工作中”状态误显示、任务看板拥挤、时间线截断、重录录屏；新产品方向：菜单栏 App 提前到群聊之前做，本地历史只做单向只读镜像，群聊参与者只要原生 App（用户 11-19 定）；Codex 复核写 R009。
- [Architecture Doc](../docs/architecture.md)
- [Project README](../README.md)
