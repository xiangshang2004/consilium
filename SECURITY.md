# Security Policy

## Local Threat Model

Consilium is designed as a **strictly loopback (`127.0.0.1`) macOS desktop collaboration bus**:

- **Authentication**: All HTTP API/MCP access requires a 256-bit Bearer token (`auth.token`, mode `0600`) or an `HttpOnly; SameSite=Strict` session cookie obtained via `POST /api/login`. Query-string `?token=` authentication is rejected (`401`), and secrets are redacted from error reports.
- **CSRF & Origin Isolation**: Cookie-authenticated state-changing endpoints validate an exact daemon `Origin` header (`http://127.0.0.1:<port>`), `Content-Type`, and a per-session `X-Consilium-CSRF` header. Spoofed, malformed, or duplicate `Host` headers are rejected (`403`).
- **Offline Dashboard CSP & Native App Containment**: `/ui` sends `Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data: blob:; frame-ancestors 'none';` alongside `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store`. `Consilium.app` uses `WKWebsiteDataStore.nonPersistent()`, restricts `WKScriptMessageHandler` bridges to the main frame at `http://127.0.0.1:<port>`, and only permits `claude://code/new` and `codex://new` deep links from the local dashboard.
- **File & Attachment Safety**: Uploaded images are capped at 20 MiB, verified by file magic bytes (`png`, `jpg`, `webp`, `gif`), stored under `0700`/`0600` directories with symlink protection, and isolated from `delete`/`discard` races.
- **Cockpit Vault Boundary**: `consilium/quota.py` reads only whitelisted JSON metadata and `mode=ro` quota history SQLite databases and never opens `codex_accounts.sqlite` or encrypted account credential files.

## Supported Versions

Security fixes are applied to the latest `0.7.x` release line on `main`.

## Reporting a Vulnerability

If you discover a security issue (for example: loopback CSRF bypass, HTTP request-smuggling/framing bug, deep-link validation bypass, token leak, or unsafe file/symlink handling):

1. **Please do not open a public GitHub issue** with exploit details before a fix is ready.
2. Report privately via **GitHub Security Advisories** (`Security` → `Advisories` → `Report a vulnerability` on [xiangshang2004/consilium](https://github.com/xiangshang2004/consilium/security/advisories)), or contact the maintainer through GitHub with steps to reproduce and affected file paths.
3. We aim to acknowledge reports within 48 hours and release a patched version alongside regression tests promptly.
