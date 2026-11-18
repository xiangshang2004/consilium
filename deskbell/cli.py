"""CLI entrypoint for deskbell daemon, stdio MCP bridge, zero-CPU waiter, and status."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from typing import List, Optional

from deskbell.models import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, VALID_PEERS, resolve_home_dir
from deskbell.mcp_server import DeskBellDaemon, run_stdio_server
from deskbell.quota import sync_local_quotas
from deskbell.store import BusStore
from deskbell.wakeup import query_active_waiters, wait_for_doorbell


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deskbell",
        description="Local MCP collaboration bus and wakeup doorbell for desktop AI apps.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve_p = sub.add_parser("serve", help="Run AF_UNIX wake broker + Streamable HTTP MCP daemon.")
    serve_p.add_argument("--host", default=DEFAULT_HTTP_HOST)
    serve_p.add_argument("--port", type=int, default=DEFAULT_HTTP_PORT)
    serve_p.add_argument("--data-dir", default=None)
    serve_p.add_argument("--require-token", action="store_true")

    stdio_p = sub.add_parser("mcp-stdio", help="Run stdio JSON-RPC 2.0 MCP server.")
    stdio_p.add_argument("--peer", default="", choices=("",) + VALID_PEERS)
    stdio_p.add_argument("--data-dir", default=None)

    wait_p = sub.add_parser("wait", help="Zero-CPU standby waiter that wakes on doorbell event.")
    wait_p.add_argument("--peer", required=True, choices=VALID_PEERS)
    wait_p.add_argument("--timeout", type=float, default=86400.0)
    wait_p.add_argument("--session-label", default="")
    wait_p.add_argument("--pickup", action="store_true")
    wait_p.add_argument("--data-dir", default=None)

    status_p = sub.add_parser("status", help="Print peers, active waiters, quotas, and missions as JSON.")
    status_p.add_argument("--mission-id", default=None)
    status_p.add_argument("--data-dir", default=None)

    quota_p = sub.add_parser("quota", help="Harvest local quota caches and print remaining quota & reset windows.")
    quota_p.add_argument("--peer", default=None, choices=VALID_PEERS)
    quota_p.add_argument("--user-home", default=None)
    quota_p.add_argument("--data-dir", default=None)
    quota_p.add_argument("--no-sync", action="store_true")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Dispatch CLI subcommands."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    home_dir = resolve_home_dir(args.data_dir)

    if args.command == "serve":
        daemon = DeskBellDaemon(
            home_dir=home_dir,
            host=args.host,
            port=args.port,
            require_token=args.require_token,
        )

        def _handle_sigterm(signum: int, frame: object) -> None:
            daemon.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handle_sigterm)
        daemon.serve_forever()
        return 0

    if args.command == "mcp-stdio":
        run_stdio_server(home_dir=home_dir, default_peer=args.peer)
        return 0

    if args.command == "wait":
        store = BusStore(home_dir)
        result = wait_for_doorbell(
            store=store,
            home_dir=home_dir,
            peer_id=args.peer,
            timeout_sec=args.timeout,
            session_label=args.session_label,
            auto_pickup=args.pickup,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "status":
        store = BusStore(home_dir)
        try:
            sync_local_quotas(store)
        except Exception:
            pass
        snapshot = store.get_mission_snapshot(mission_id=args.mission_id)
        output = {
            "home_dir": home_dir,
            "peers": store.list_peers(),
            "active_waiters": query_active_waiters(home_dir),
            "quotas": store.list_quota_windows()["peers"],
            **snapshot,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    if args.command == "quota":
        store = BusStore(home_dir)
        if not args.no_sync:
            sync_local_quotas(store, user_home=args.user_home)
        output = store.list_quota_windows(peer_id=args.peer)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
