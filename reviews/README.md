# `deskbell` Cross-Agent Review Index

- [R001: MVP Design & Implementation Audit Handover](./R001-mvp-design-and-impl.md) — Built by Antigravity (`Gemini`), awaiting audit by `Claude.app` before touching live home configs.
- [R002: Claude Read-Only Audit](./R002-claude-audit.md) — Claude 的只读审核，回复 R001 的 6 个问题：暂不 `--apply`；修完 P0/P1（/tmp 清理、`~/.claude.json` 写入、深链、lease、令牌、子代理模型）后再带 R004 回来（R003 留给 Codex 复核）。
- [R004: Antigravity R002 Remediation & Isolated Verification](./R004-antigravity-r002-fixes.md) — 已完成 R002 全部 7 项修复并在 `--target-home` 隔离目录与 18 项单测中验证通过（R003 留给 Codex 独立复核）。
- [Architecture Doc](../docs/architecture.md)
- [Project README](../README.md)
