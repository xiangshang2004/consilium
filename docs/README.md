# `consilium` Documentation Index

- [项目说明 (中文)](../README.md) · [English README](../README.en.md) — 功能介绍、工作原理、安装接入、卸载与开发命令
- [Agent Skill (skills/consilium)](../skills/consilium/SKILL.md) — `skills.sh` / Agent Skills 标准技能包，含安装引导、三端协作协议与独立排障参考
- [新用户快速排障指南](./troubleshooting.md) — Gatekeeper 放行、工具未加载、待命不醒、端口冲突、额度与图片限制及日志排查
- [隐私协议（PRIVACY.md）](../PRIVACY.md) — 本机零遥测原则、只读数据白名单、账户哈希隔离与第三方桌面端边界
- [安全政策（SECURITY.md）](../SECURITY.md) — 威胁模型（`127.0.0.1` / CSP / CSRF / 附件安全）与漏洞报告方式
- [贡献指南（CONTRIBUTING.md）](../CONTRIBUTING.md) — 纯 Python 标准库规范、免 Xcode 编译与测试要求
- [Cockpit Tools 额度接入](../README.md#使用指南) — 本机设置、查看入口及缺少样本时的处理
- [许可全文](../LICENSE) · [权属说明](../NOTICE) — Consilium 原创内容与第三方材料的授权范围
- [CHANGELOG](../CHANGELOG.md) — 每个版本的变化；发布用 `scripts/release.sh <版本>`，产物在 GitHub Releases
- [Screenshots](./screenshots/chat-light.png) — README 用的演示动图与静态截图（[演示动图](./screenshots/consilium-demo.gif)、[浅色对话](./screenshots/chat-light.png)、[深色对话](./screenshots/chat-dark.png)、[新会诊](./screenshots/new-conversation.png)），内容是演示数据
- [Architecture & Protocol](./architecture.md) — SQLite WAL truth layer, macOS `kqueue` VNODE doorbell, Seatbelt sandbox compatibility, dual MCP transports, and dashboard/SSE/installer security boundaries
- [重试与断线恢复](./architecture.md#重试与断线恢复) — 请求去重、执行轮次和跨 HTTP/SSE 的状态顺序
- [OpenSpec Proposal (`consilium-v1`)](../openspec/changes/consilium-v1/proposal.md) — Motivation, v1 scope, frozen contracts, and non-goals
- [OpenSpec Design (`consilium-v1`)](../openspec/changes/consilium-v1/design.md) — Bus reliability, security, and offline dashboard design
- [OpenSpec Bus Spec](../openspec/changes/consilium-v1/specs/bus/spec.md) — Normative bus & installer requirements
- [OpenSpec Dashboard Spec](../openspec/changes/consilium-v1/specs/dashboard/spec.md) — Normative `/ui`, `/api/state`, `/api/stream` (SSE), and auth requirements
- [Cross-Agent Reviews](../reviews/README.md) — Design audit packets exchanged with Claude & Codex
- [Brand / Logo](./brand/preview.png) — Consilium 标志（缺口朝右的环读作 “C”，三段弧沿用看板里三端的颜色，中心圆点表示会诊结论；含浅色/深色标志、单色菜单栏模板 SVG [consilium-mark-template.svg](./brand/consilium-mark-template.svg)、横版组合、macOS App 图标、状态徽章及 GitHub 封面图 [social-preview.png](./brand/social-preview.png) / [social-preview.svg](./brand/social-preview.svg)）
