"""CLI entrypoint for deskbell daemon, stdio MCP bridge, zero-CPU waiter, status, and quota."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from typing import List, Optional

from deskbell.models import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    VALID_PEERS,
    resolve_rendezvous_dir,
    resolve_state_dir,
)
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
    serve_p.add_argument("--rendezvous-dir", default=None)
    serve_p.add_argument("--claim-timeout", type=int, default=None, help="Stale claimed task timeout in seconds (default 900).")
    serve_p.add_argument("--running-timeout", type=int, default=None, help="Stale running task timeout in seconds (default 7200).")
    tok_grp = serve_p.add_mutually_exclusive_group()
    tok_grp.add_argument(
        "--require-token",
        dest="require_token",
        action="store_true",
        help="Require Bearer/query token on HTTP endpoints (default).",
    )
    tok_grp.add_argument(
        "--no-require-token",
        dest="require_token",
        action="store_false",
        help="Disable HTTP token authentication.",
    )
    serve_p.set_defaults(require_token=True)

    stdio_p = sub.add_parser("mcp-stdio", help="Run stdio JSON-RPC 2.0 MCP server.")
    stdio_p.add_argument("--peer", default="", choices=("",) + VALID_PEERS)
    stdio_p.add_argument("--data-dir", default=None)
    stdio_p.add_argument("--rendezvous-dir", default=None)

    wait_p = sub.add_parser("wait", help="Zero-CPU standby waiter watching signals/<peer>.seq without touching DB.")
    wait_p.add_argument("--peer", required=True, choices=VALID_PEERS)
    wait_p.add_argument("--timeout", type=float, default=86400.0)
    wait_p.add_argument("--session-label", default="")
    wait_p.add_argument("--pickup", action="store_true")
    wait_p.add_argument("--data-dir", default=None)
    wait_p.add_argument("--rendezvous-dir", default=None)

    status_p = sub.add_parser("status", help="Print peers, active waiters, quotas, and missions as JSON.")
    status_p.add_argument("--mission-id", default=None)
    status_p.add_argument("--data-dir", default=None)
    status_p.add_argument("--rendezvous-dir", default=None)

    quota_p = sub.add_parser("quota", help="Harvest local quota caches and print remaining quota & reset windows.")
    quota_p.add_argument("--peer", default=None, choices=VALID_PEERS)
    quota_p.add_argument("--user-home", default=None)
    quota_p.add_argument("--data-dir", default=None)
    quota_p.add_argument("--rendezvous-dir", default=None)
    quota_p.add_argument("--no-sync", action="store_true")

    rot_p = sub.add_parser("rotate-token", help="Rotate Bearer token across configured clients first, then auth.token.")
    rot_p.add_argument("--target-home", default=os.path.expanduser("~"))
    rot_p.add_argument("--data-dir", default=None)
    rot_p.add_argument("--port", type=int, default=DEFAULT_HTTP_PORT)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Dispatch CLI subcommands."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "serve":
        state_dir = resolve_state_dir(args.data_dir)
        rv_dir = resolve_rendezvous_dir(args.rendezvous_dir, fallback_dir=args.data_dir)
        daemon = DeskBellDaemon(
            home_dir=state_dir,
            rendezvous_dir=rv_dir,
            host=args.host,
            port=args.port,
            require_token=args.require_token,
            claim_timeout_sec=args.claim_timeout,
            running_timeout_sec=args.running_timeout,
        )

        def _handle_sigterm(signum: int, frame: object) -> None:
            daemon.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handle_sigterm)
        daemon.serve_forever()
        return 0

    if args.command == "mcp-stdio":
        state_dir = resolve_state_dir(args.data_dir)
        rv_dir = resolve_rendezvous_dir(args.rendezvous_dir, fallback_dir=args.data_dir)
        run_stdio_server(home_dir=state_dir, default_peer=args.peer, rendezvous_dir=rv_dir)
        return 0

    if args.command == "wait":
        rv_dir = resolve_rendezvous_dir(args.rendezvous_dir, fallback_dir=args.data_dir)
        store = (
            BusStore(resolve_state_dir(args.data_dir), rendezvous_dir=rv_dir)
            if args.pickup
            else None
        )
        result = wait_for_doorbell(
            store=store,
            home_dir=rv_dir,
            rendezvous_dir=rv_dir,
            peer_id=args.peer,
            timeout_sec=args.timeout,
            session_label=args.session_label,
            auto_pickup=args.pickup,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "status":
        state_dir = resolve_state_dir(args.data_dir)
        rv_dir = resolve_rendezvous_dir(args.rendezvous_dir, fallback_dir=args.data_dir)
        store = BusStore(state_dir, rendezvous_dir=rv_dir)
        try:
            sync_local_quotas(store)
        except Exception:
            pass
        snapshot = store.get_mission_snapshot(mission_id=args.mission_id)
        output = {
            "state_dir": state_dir,
            "rendezvous_dir": rv_dir,
            "peers": store.list_peers(),
            "active_waiters": query_active_waiters(rv_dir),
            "quotas": store.list_quota_windows()["peers"],
            **snapshot,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    if args.command == "quota":
        state_dir = resolve_state_dir(args.data_dir)
        rv_dir = resolve_rendezvous_dir(args.rendezvous_dir, fallback_dir=args.data_dir)
        store = BusStore(state_dir, rendezvous_dir=rv_dir)
        if not args.no_sync:
            sync_local_quotas(store, user_home=args.user_home)
        output = store.list_quota_windows(peer_id=args.peer)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    if args.command == "rotate-token":
        from scripts.install import rotate_token

        report = rotate_token(
            target_home=args.target_home,
            port=args.port,
            state_dir=args.data_dir,
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
