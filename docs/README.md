# `consilium` Documentation Index

- [Project README](../README.md) — 中文说明：能做什么、怎么装、怎么用、安全与限制、版本与发布
- [CHANGELOG](../CHANGELOG.md) — 每个版本的变化；发布用 `scripts/release.sh <版本>`，产物在 GitHub Releases
- [Screenshots](./screenshots/chat-light.png) — README 用的三张演示截图（[浅色对话](./screenshots/chat-light.png)、[深色对话](./screenshots/chat-dark.png)、[新会诊](./screenshots/new-conversation.png)），内容是演示数据
- [Architecture & Protocol](./architecture.md) — SQLite WAL truth layer, macOS `kqueue` VNODE doorbell, Seatbelt sandbox compatibility, dual MCP transports, and dashboard/SSE/installer security boundaries
- [OpenSpec Proposal (`consilium-v1`)](../openspec/changes/consilium-v1/proposal.md) — Motivation, v1 scope, frozen contracts, and non-goals
- [OpenSpec Design (`consilium-v1`)](../openspec/changes/consilium-v1/design.md) — Bus reliability, security, and offline dashboard design
- [OpenSpec Bus Spec](../openspec/changes/consilium-v1/specs/bus/spec.md) — Normative bus & installer requirements
- [OpenSpec Dashboard Spec](../openspec/changes/consilium-v1/specs/dashboard/spec.md) — Normative `/ui`, `/api/state`, `/api/stream` (SSE), and auth requirements
- [Cross-Agent Reviews](../reviews/README.md) — Design audit packets exchanged with Claude & Codex
- [Brand / Logo](./brand/preview.png) — Consilium 标志（2026-11-20 Claude 初稿）：缺口朝右的环读作 “C”，三段弧沿用看板里三端的颜色（Claude 橙、Codex 绿、Antigravity 蓝），中心圆点表示会诊结论；含浅色/深色标志、单色菜单栏模板 SVG（[consilium-mark-template.svg](./brand/consilium-mark-template.svg)）、横版组合（标志 + Consilium 字标）、macOS App 图标和 favicon（`brand/*.svg`）
