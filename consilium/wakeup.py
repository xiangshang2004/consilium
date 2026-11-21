"""DB-free kqueue VNODE wakeup, flock leases, AF_UNIX broker, and safe macOS cold fallback."""

from __future__ import annotations

from contextlib import contextmanager
import errno
import fcntl
import hashlib
import json
import os
import secrets
import select
import selectors
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Tuple

from consilium.models import (
    VALID_PEERS,
    _ensure_private_dir,
    resolve_rendezvous_dir,
    validate_peer,
)

if TYPE_CHECKING:
    from consilium.store import BusStore

MAX_UNIX_PATH_BYTES = 96
DEFAULT_DEEPLINK_MAX_LEN = 2000
DEFAULT_ESCALATION_SEC = 60.0


def resolve_socket_path(rendezvous_dir: str) -> str:
    """Resolve an AF_UNIX socket path inside a private 0700 directory below 104 bytes."""
    resolved_rv = resolve_rendezvous_dir(rendezvous_dir)
    candidate = os.path.join(resolved_rv, "wake.sock")
    if len(candidate.encode("utf-8")) <= MAX_UNIX_PATH_BYTES:
        return candidate
    fallback_override = (os.environ.get("CONSILIUM_SOCKET_FALLBACK_DIR") or "").strip()
    sock_base = fallback_override or f"/tmp/consilium-{os.getuid()}/sock"
    sock_dir = _ensure_private_dir(sock_base, "socket")
    digest = hashlib.sha256(resolved_rv.encode("utf-8")).hexdigest()[:10]
    return os.path.join(sock_dir, f"{digest}.sock")


def _is_dry_run(dry_run: bool = False) -> bool:
    return bool(dry_run or os.environ.get("CONSILIUM_DRY_RUN") == "1")


def _send_json_line(sock: socket.socket, payload: Dict[str, Any]) -> None:
    data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    sock.sendall(data)


def _read_json_line(file_obj: Any) -> Optional[Dict[str, Any]]:
    raw = file_obj.readline()
    if not raw:
        return None
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    return json.loads(text)


def _signals_dir(rendezvous_dir: str) -> str:
    return _ensure_private_dir(os.path.join(rendezvous_dir, "signals"), "signals")


def _signal_path(rendezvous_dir: str, peer_id: str) -> str:
    return os.path.join(_signals_dir(rendezvous_dir), f"{peer_id}.sig")


def _seq_path(rendezvous_dir: str, peer_id: str) -> str:
    return os.path.join(_signals_dir(rendezvous_dir), f"{peer_id}.seq")


def _seq_lock_path(rendezvous_dir: str, peer_id: str) -> str:
    """Place the flock file directly under rendezvous root so deleting signals/ does not rotate its inode."""
    return os.path.join(resolve_rendezvous_dir(rendezvous_dir), f".{peer_id}.seq.lock")


_SEQ_THREAD_LOCKS_GUARD = threading.Lock()
_SEQ_THREAD_LOCKS: Dict[Tuple[str, str], threading.Lock] = {}


def _get_seq_thread_lock(resolved_rv: str, peer: str) -> threading.Lock:
    key = (resolved_rv, peer)
    with _SEQ_THREAD_LOCKS_GUARD:
        lock = _SEQ_THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _SEQ_THREAD_LOCKS[key] = lock
        return lock


@contextmanager
def _seq_lock(resolved_rv: str, peer: str) -> Iterator[None]:
    """Acquire both an in-process thread lock and cross-process flock on .<peer>.seq.lock."""
    tlock = _get_seq_thread_lock(resolved_rv, peer)
    with tlock:
        lock_path = _seq_lock_path(resolved_rv, peer)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                yield
            finally:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            try:
                os.close(fd)
            except OSError:
                pass


def _ensure_signal_file(rendezvous_dir: str, peer_id: str) -> str:
    path = _signal_path(rendezvous_dir, peer_id)
    if not os.path.exists(path):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        os.close(fd)
    return path


def _pulse_signal_file(rendezvous_dir: str, peer_id: str) -> None:
    path = _signal_path(rendezvous_dir, peer_id)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, f"{time.time_ns()}\n".encode("ascii"))
        os.fsync(fd)
    finally:
        os.close(fd)


def _read_peer_seq_unlocked(resolved_rv: str, peer: str) -> Dict[str, Any]:
    target = os.path.join(resolved_rv, "signals", f"{peer}.seq")
    if not os.path.exists(target):
        return {"seq": 0, "unacked": 0, "queued_tasks": 0, "updated_at": 0.0}
    try:
        with open(target, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        seq_all = int(data.get("seq", 0))
        unacked_all = int(data.get("unacked", 0))
        queued_all = int(data.get("queued_tasks", 0))
        return {
            "seq": seq_all,
            "unacked": unacked_all,
            "queued_tasks": queued_all,
            "updated_at": float(data.get("updated_at", 0.0)),
            # Work started in the Consilium window ("ui") vs. missions a session created itself.
            # A file written before these fields existed counts everything as window work, so
            # nobody misses a wake-up.
            "ui_seq": int(data.get("ui_seq", seq_all)),
            "ui_unacked": int(data.get("ui_unacked", unacked_all)),
            "ui_queued": int(data.get("ui_queued", queued_all)),
            "other_seq": int(data.get("other_seq", seq_all)),
        }
    except (OSError, ValueError, TypeError):
        return {"seq": 0, "unacked": 0, "queued_tasks": 0, "updated_at": 0.0}


def _write_seq_payload_locked(
    resolved_rv: str,
    peer: str,
    payload: str,
    *,
    pulse: bool = False,
) -> None:
    for attempt in range(10):
        try:
            sig_dir = _signals_dir(resolved_rv)
            target = os.path.join(sig_dir, f"{peer}.seq")
            fd, tmp_path = tempfile.mkstemp(prefix=f".{peer}.seq.", dir=sig_dir)
            try:
                os.chmod(tmp_path, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(payload + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, target)
            except BaseException:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            if pulse:
                _pulse_signal_file(resolved_rv, peer)
            return
        except OSError as exc:
            if exc.errno not in (errno.ENOENT, errno.EINVAL) or attempt >= 9:
                raise


def write_peer_seq(
    rendezvous_dir: str,
    peer_id: str,
    seq: Optional[int] = None,
    unacked: Optional[int] = None,
    *,
    queued_tasks: Optional[int] = None,
    query_fn: Optional[Callable[[], Tuple[int, int, int]]] = None,
    pulse: bool = False,
    pulse_if_backlog: bool = False,
) -> Dict[str, Any]:
    """Atomically write signals/<peer>.seq under an exclusive flock on <peer>.seq.lock.

    When query_fn is provided, it is invoked *inside* the lock so the last writer
    always persists the authoritative DB (max_event_id, unacked_events, queued_tasks).
    """
    peer = validate_peer(peer_id)
    resolved_rv = resolve_rendezvous_dir(rendezvous_dir)
    with _seq_lock(resolved_rv, peer):
        scoped: Optional[Dict[str, int]] = None
        if query_fn is not None:
            counts = query_fn()
            db_seq, db_unacked, db_queued = counts[0], counts[1], counts[2]
            if len(counts) > 3 and isinstance(counts[3], dict):
                scoped = counts[3]
            seq_val = int(db_seq)
            unacked_val = int(db_unacked)
            queued_val = int(db_queued)
        else:
            prev = _read_peer_seq_unlocked(resolved_rv, peer)
            seq_val = int(seq) if seq is not None else int(prev.get("seq", 0))
            unacked_val = int(unacked) if unacked is not None else int(prev.get("unacked", 0))
            queued_val = (
                int(queued_tasks)
                if queued_tasks is not None
                else int(prev.get("queued_tasks", 0))
            )
        state = {
            "seq": seq_val,
            "unacked": unacked_val,
            "queued_tasks": queued_val,
            "updated_at": time.time(),
            "ui_seq": int((scoped or {}).get("ui_seq", seq_val)),
            "ui_unacked": int((scoped or {}).get("ui_unacked", unacked_val)),
            "ui_queued": int((scoped or {}).get("ui_queued", queued_val)),
            "other_seq": int((scoped or {}).get("other_seq", seq_val)),
        }
        payload = json.dumps(state)
        effective_pulse = bool(pulse) or (
            bool(pulse_if_backlog) and (unacked_val > 0 or queued_val > 0)
        )
        _write_seq_payload_locked(resolved_rv, peer, payload, pulse=effective_pulse)
        return state


def read_peer_seq(rendezvous_dir: str, peer_id: str) -> Dict[str, Any]:
    """Read the latest signals/<peer>.seq state without touching SQLite."""
    peer = validate_peer(peer_id)
    resolved_rv = resolve_rendezvous_dir(rendezvous_dir)
    return _read_peer_seq_unlocked(resolved_rv, peer)


def rebuild_all_peer_seqs(store: "BusStore") -> Dict[str, Dict[str, Any]]:
    """Reconstruct signals/<peer>.seq for all peers directly from SQLite, pulsing any backlog."""
    rebuilt: Dict[str, Dict[str, Any]] = {}
    for peer in VALID_PEERS:
        rebuilt[peer] = store.sync_peer_seq(peer, pulse=False, pulse_if_backlog=True)
    return rebuilt


def _waiter_dir(rendezvous_dir: str, peer_id: str) -> str:
    return _ensure_private_dir(os.path.join(rendezvous_dir, "waiters", peer_id), "waiters")


def _acquire_lease(
    rendezvous_dir: str,
    peer_id: str,
    expires_at: float,
    session_label: str = "",
) -> Tuple[int, str]:
    """Create and exclusively flock a temp file, write payload, then atomically rename to .lease."""
    wdir = _waiter_dir(rendezvous_dir, peer_id)
    tid = threading.get_ident()
    tmp_path = os.path.join(wdir, f".tmp-{os.getpid()}-{tid}-{secrets.token_hex(4)}")
    lease_path = os.path.join(wdir, f"{os.getpid()}-{tid}-{secrets.token_hex(2)}.lease")
    fd = os.open(tmp_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "session_label": session_label,
                "expires_at": float(expires_at),
            }
        ).encode("utf-8")
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, payload)
        os.fsync(fd)
        os.replace(tmp_path, lease_path)
        return fd, lease_path
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def _release_lease(lease_fd: Optional[int], lease_path: Optional[str]) -> None:
    """Remove lease file while still holding flock, then unlock and close fd."""
    if lease_path:
        try:
            os.unlink(lease_path)
        except OSError:
            pass
    if lease_fd is not None:
        try:
            fcntl.flock(lease_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(lease_fd)
        except OSError:
            pass


def _count_live_leases(rendezvous_dir: str, peer_id: str) -> int:
    """Count waiter leases guarded by active kernel flock; prune dead or expired ones."""
    wdir = os.path.join(rendezvous_dir, "waiters", peer_id)
    if not os.path.isdir(wdir):
        return 0
    now = time.time()
    live = 0
    for name in os.listdir(wdir):
        if not (name.endswith(".lease") or name.endswith(".json")):
            continue
        path = os.path.join(wdir, name)
        try:
            fd = os.open(path, os.O_RDWR)
        except OSError:
            continue
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                # An active process holds the exclusive flock; check expiry only.
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        data = json.load(handle)
                    expires_at = float(data.get("expires_at", 0.0))
                except (OSError, ValueError, TypeError):
                    expires_at = now + 1.0
                if expires_at <= now:
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                else:
                    live += 1
            else:
                # Lock acquired => previous owner exited/crashed (even if PID was reused).
                try:
                    os.unlink(path)
                except OSError:
                    pass
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
    return live


class _WaiterEntry:
    """Single connected waiter tracked by WakeBrokerServer."""

    def __init__(self, peer_id: str) -> None:
        self.peer_id = peer_id
        self.read_fd, self.write_fd = os.pipe()
        self.reason = "timeout"
        self.woke = False
        self._closed = False
        self._lock = threading.Lock()

    def signal(self, reason: str = "notify") -> None:
        with self._lock:
            if self._closed:
                return
            self.reason = reason
            self.woke = reason == "notify"
            try:
                os.write(self.write_fd, b"1")
            except OSError:
                pass

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for fd in (self.read_fd, self.write_fd):
                try:
                    os.close(fd)
                except OSError:
                    pass


class WakeBrokerServer:
    """Threaded AF_UNIX socket broker holding zero-CPU standby waiters."""

    def __init__(self, rendezvous_dir: Optional[str] = None) -> None:
        self.rendezvous_dir = resolve_rendezvous_dir(rendezvous_dir)
        self.home_dir = self.rendezvous_dir
        self.sock_path = resolve_socket_path(self.rendezvous_dir)
        self._lock = threading.Lock()
        self._waiters: Dict[str, List[_WaiterEntry]] = {p: [] for p in VALID_PEERS}
        self._client_threads: set = set()
        self._server_sock: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> str:
        """Bind the AF_UNIX socket and start the accept loop thread."""
        with self._lock:
            if self._running:
                return self.sock_path
            if os.path.exists(self.sock_path):
                try:
                    os.unlink(self.sock_path)
                except OSError:
                    pass
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(self.sock_path)
                try:
                    os.chmod(self.sock_path, 0o600)
                except OSError:
                    pass
                sock.listen(64)
                sock.settimeout(0.5)
            except BaseException:
                try:
                    sock.close()
                except OSError:
                    pass
                raise
            self._server_sock = sock
            self._running = True
            self._accept_thread = threading.Thread(
                target=self._accept_loop, name="consilium-wake-accept", daemon=True
            )
            self._accept_thread.start()
            return self.sock_path

    def stop(self) -> None:
        """Stop the broker, wake any pending waiters, and remove the socket."""
        with self._lock:
            self._running = False
            for entries in self._waiters.values():
                for entry in list(entries):
                    entry.signal(reason="shutdown")
            self._waiters = {p: [] for p in VALID_PEERS}
            srv_sock = self._server_sock
            self._server_sock = None
            accept_thread = self._accept_thread
            self._accept_thread = None
            clients = list(self._client_threads)
        if srv_sock is not None:
            try:
                srv_sock.close()
            except OSError:
                pass
        if accept_thread is not None:
            accept_thread.join(timeout=2.0)
        for worker in clients:
            worker.join(timeout=0.5)
        if os.path.exists(self.sock_path):
            try:
                os.unlink(self.sock_path)
            except OSError:
                pass

    def __enter__(self) -> "WakeBrokerServer":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    def active_counts(self) -> Dict[str, int]:
        """Return active waiter counts keyed by peer_id."""
        with self._lock:
            return {p: len(self._waiters.get(p, [])) for p in VALID_PEERS}

    def notify(self, target_peer: str) -> int:
        """Wake all active waiters for target_peer (or '*' for all peers)."""
        if target_peer == "*":
            peers = list(VALID_PEERS)
        else:
            peers = [validate_peer(target_peer)]

        to_wake: List[_WaiterEntry] = []
        with self._lock:
            for peer in peers:
                entries = self._waiters.get(peer, [])
                to_wake.extend(entries)
                self._waiters[peer] = []

        for entry in to_wake:
            entry.signal(reason="notify")
        return len(to_wake)

    def _accept_loop(self) -> None:
        while True:
            with self._lock:
                running = self._running
                srv_sock = self._server_sock
            if not running or srv_sock is None:
                break
            try:
                conn, _ = srv_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            worker = threading.Thread(
                target=self._handle_client,
                args=(conn,),
                name="consilium-wake-client",
                daemon=True,
            )
            with self._lock:
                if not self._running:
                    try:
                        conn.close()
                    except OSError:
                        pass
                    break
                self._client_threads.add(worker)
            worker.start()

    def _handle_client(self, conn: socket.socket) -> None:
        entry: Optional[_WaiterEntry] = None
        cur_thread = threading.current_thread()
        try:
            conn.settimeout(5.0)
            with conn.makefile("rb") as reader:
                req = _read_json_line(reader)
                if not isinstance(req, dict):
                    _send_json_line(conn, {"ok": False, "error": "invalid request"})
                    return
                op = req.get("op")
                if op == "status":
                    _send_json_line(
                        conn, {"ok": True, "active_waiters": self.active_counts()}
                    )
                    return
                if op == "notify":
                    count = self.notify(str(req.get("peer", "")))
                    _send_json_line(conn, {"ok": True, "woken_count": count})
                    return
                if op == "wait":
                    peer = validate_peer(str(req.get("peer", "")))
                    timeout_sec = max(0.1, float(req.get("timeout_sec", 300.0)))
                    candidate = _WaiterEntry(peer)
                    with self._lock:
                        if not self._running:
                            candidate.close()
                            _send_json_line(conn, {"ok": True, "woke": False, "reason": "shutdown"})
                            return
                        entry = candidate
                        self._waiters[peer].append(entry)
                    conn.settimeout(None)
                    _send_json_line(conn, {"ok": True, "state": "registered"})

                    with selectors.DefaultSelector() as sel:
                        sel.register(conn, selectors.EVENT_READ, data="conn")
                        sel.register(entry.read_fd, selectors.EVENT_READ, data="pipe")
                        events = sel.select(timeout=timeout_sec)
                    triggered = {key.data for key, _ in events}
                    if "pipe" in triggered:
                        payload = {"ok": True, "woke": entry.woke, "reason": entry.reason}
                        _send_json_line(conn, payload)
                    elif "conn" in triggered:
                        pass
                    else:
                        _send_json_line(conn, {"ok": True, "woke": False, "reason": "timeout"})
                    return

                _send_json_line(conn, {"ok": False, "error": f"unknown op: {op!r}"})
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        finally:
            if entry is not None:
                with self._lock:
                    entries = self._waiters.get(entry.peer_id, [])
                    if entry in entries:
                        entries.remove(entry)
                entry.close()
            with self._lock:
                self._client_threads.discard(cur_thread)
            try:
                conn.close()
            except OSError:
                pass


def _broker_rpc(
    rendezvous_dir: str,
    request: Dict[str, Any],
    timeout: float = 1.5,
) -> Optional[Dict[str, Any]]:
    sock_path = resolve_socket_path(rendezvous_dir)
    if not os.path.exists(sock_path):
        return None
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(sock_path)
        _send_json_line(sock, request)
        with sock.makefile("rb") as reader:
            return _read_json_line(reader)
    except OSError:
        return None
    finally:
        try:
            sock.close()
        except OSError:
            pass


def notify_peer(
    rendezvous_dir: str,
    peer_id: str,
    *,
    store: Optional["BusStore"] = None,
) -> int:
    """Wake standby waiters via locked .seq sync + kqueue signal pulse + optional AF_UNIX broker."""
    resolved_rv = resolve_rendezvous_dir(rendezvous_dir)
    peers = list(VALID_PEERS) if peer_id == "*" else [validate_peer(peer_id)]
    lease_count = 0
    for p in peers:
        lease_count += _count_live_leases(resolved_rv, p)
        if store is not None:
            store.sync_peer_seq(p, pulse=True)
        else:
            with _seq_lock(resolved_rv, p):
                cur = _read_peer_seq_unlocked(resolved_rv, p)
                seq_next = int(cur["seq"]) + 1
                payload = json.dumps(
                    {
                        "seq": seq_next,
                        "unacked": int(cur["unacked"]),
                        "queued_tasks": int(cur["queued_tasks"]),
                        "updated_at": time.time(),
                        "ui_seq": int(cur.get("ui_seq", cur["seq"])) + 1,
                        "ui_unacked": int(cur.get("ui_unacked", cur["unacked"])),
                        "ui_queued": int(cur.get("ui_queued", cur["queued_tasks"])),
                        "other_seq": int(cur.get("other_seq", cur["seq"])) + 1,
                    }
                )
                _write_seq_payload_locked(resolved_rv, p, payload, pulse=True)

    target = "*" if peer_id == "*" else peers[0]
    resp = _broker_rpc(resolved_rv, {"op": "notify", "peer": target}, timeout=1.0)
    sock_woken = int(resp.get("woken_count", 0)) if (resp and resp.get("ok")) else 0
    return max(lease_count, sock_woken)


def query_active_waiters(rendezvous_dir: str) -> Dict[str, int]:
    """Query active standby waiter counts across flock leases and WakeBrokerServer."""
    resolved_rv = resolve_rendezvous_dir(rendezvous_dir)
    lease_counts = {p: _count_live_leases(resolved_rv, p) for p in VALID_PEERS}
    resp = _broker_rpc(resolved_rv, {"op": "status"}, timeout=1.0)
    sock_counts = resp.get("active_waiters", {}) if (resp and resp.get("ok")) else {}
    return {p: max(lease_counts[p], int(sock_counts.get(p, 0))) for p in VALID_PEERS}


WAIT_SCOPES = ("all", "consilium", "other")


def scoped_seq(cur: Dict[str, Any], scope: str = "all") -> Dict[str, int]:
    """The (seq, unacked, queued_tasks) a waiter with this scope should react to."""
    seq_all = int(cur.get("seq", 0))
    unacked_all = int(cur.get("unacked", 0))
    queued_all = int(cur.get("queued_tasks", 0))
    if scope == "consilium":
        return {
            "seq": int(cur.get("ui_seq", seq_all)),
            "unacked": int(cur.get("ui_unacked", unacked_all)),
            "queued_tasks": int(cur.get("ui_queued", queued_all)),
        }
    if scope == "other":
        return {
            "seq": int(cur.get("other_seq", seq_all)),
            "unacked": max(0, unacked_all - int(cur.get("ui_unacked", unacked_all))),
            "queued_tasks": max(0, queued_all - int(cur.get("ui_queued", queued_all))),
        }
    return {"seq": seq_all, "unacked": unacked_all, "queued_tasks": queued_all}


def default_wait_scope(session_label: str, scope: str = "") -> str:
    """A standby session serves the Consilium window; it must not spin on other projects' events."""
    chosen = str(scope or "").strip().lower()
    if chosen:
        if chosen not in WAIT_SCOPES:
            raise ValueError("scope must be one of: " + ", ".join(WAIT_SCOPES))
        return chosen
    return "consilium" if str(session_label or "").strip().lower().endswith("standby") else "all"


def _seq_has_work(cur: Dict[str, Any], initial_seq: int, scope: str = "all") -> bool:
    view = scoped_seq(cur, scope)
    return view["unacked"] > 0 or view["queued_tasks"] > 0 or view["seq"] > initial_seq


def _open_kqueue_watch(rendezvous_dir: str, peer: str) -> Tuple[int, Any]:
    for attempt in range(10):
        try:
            sig_path = _ensure_signal_file(rendezvous_dir, peer)
            fd = os.open(sig_path, os.O_RDONLY)
        except OSError as exc:
            if exc.errno not in (errno.ENOENT, errno.EINVAL) or attempt >= 9:
                raise
            continue
        kq = select.kqueue()
        try:
            fflags = (
                select.KQ_NOTE_WRITE
                | select.KQ_NOTE_EXTEND
                | select.KQ_NOTE_ATTRIB
                | select.KQ_NOTE_DELETE
                | select.KQ_NOTE_RENAME
            )
            kev = select.kevent(
                fd,
                filter=select.KQ_FILTER_VNODE,
                flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR,
                fflags=fflags,
            )
            kq.control([kev], 0)
            return fd, kq
        except BaseException:
            try:
                kq.close()
            except OSError:
                pass
            try:
                os.close(fd)
            except OSError:
                pass
            raise
    raise FileNotFoundError(f"Failed to open kqueue watch for {peer}")


def _wait_via_kqueue(
    rendezvous_dir: str,
    peer: str,
    timeout_sec: float,
    initial_seq: int,
    scope: str = "all",
) -> Tuple[bool, str]:
    fd, kq = _open_kqueue_watch(rendezvous_dir, peer)
    try:
        cur = read_peer_seq(rendezvous_dir, peer)
        if _seq_has_work(cur, initial_seq, scope):
            return True, "immediate_after_register"

        deadline = time.monotonic() + timeout_sec
        delete_or_rename = select.KQ_NOTE_DELETE | select.KQ_NOTE_RENAME
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False, "timeout"
            events = kq.control(None, 1, remaining)
            if not events:
                return False, "timeout"
            triggered_flags = events[0].fflags
            if triggered_flags & delete_or_rename:
                old_kq, old_fd = kq, fd
                kq, fd = None, -1
                if old_kq is not None:
                    try:
                        old_kq.close()
                    except OSError:
                        pass
                if old_fd >= 0:
                    try:
                        os.close(old_fd)
                    except OSError:
                        pass
                fd, kq = _open_kqueue_watch(rendezvous_dir, peer)
            cur = read_peer_seq(rendezvous_dir, peer)
            if _seq_has_work(cur, initial_seq, scope):
                return True, "kqueue"
    finally:
        if kq is not None:
            try:
                kq.close()
            except OSError:
                pass
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass


def _wait_via_unix_or_poll(
    rendezvous_dir: str,
    peer: str,
    timeout_sec: float,
    poll_fallback_sec: float,
    initial_seq: int,
    scope: str = "all",
) -> Tuple[bool, str]:
    sock_path = resolve_socket_path(rendezvous_dir)
    woke = False
    source = "timeout"
    # The broker wakes every waiter of a peer; a scoped waiter polls the counts instead.
    sock_failed = scope != "all" or not os.path.exists(sock_path)

    if not sock_failed:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(5.0)
            sock.connect(sock_path)
            _send_json_line(
                sock, {"op": "wait", "peer": peer, "timeout_sec": timeout_sec}
            )
            with sock.makefile("rb") as reader:
                reg = _read_json_line(reader)
                if reg and reg.get("state") == "registered":
                    cur = read_peer_seq(rendezvous_dir, peer)
                    if _seq_has_work(cur, initial_seq, scope):
                        woke = True
                        source = "immediate_after_register"
                    else:
                        sock.settimeout(timeout_sec + 2.0)
                        msg = _read_json_line(reader)
                        if msg and msg.get("woke"):
                            woke = True
                            source = "socket"
                        elif msg:
                            source = str(msg.get("reason", "timeout"))
                else:
                    sock_failed = True
        except OSError:
            sock_failed = True
        finally:
            try:
                sock.close()
            except OSError:
                pass

    if not woke and sock_failed:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            cur = read_peer_seq(rendezvous_dir, peer)
            if _seq_has_work(cur, initial_seq, scope):
                woke = True
                source = "poll_fallback"
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(poll_fallback_sec, remaining))
    return woke, source


def wait_for_doorbell(
    store: Optional["BusStore"] = None,
    home_dir: Optional[str] = None,
    peer_id: str = "",
    timeout_sec: float = 45.0,
    poll_fallback_sec: float = 0.5,
    session_label: str = "",
    auto_pickup: bool = False,
    rendezvous_dir: Optional[str] = None,
    scope: str = "",
) -> Dict[str, Any]:
    """Block silently with zero DB writes using signals/<peer>.seq + flock leases. Always read-only.

    `scope` decides which work wakes the waiter: "consilium" (started in the Consilium window;
    the default for a session labelled "...standby"), "other", or "all".
    """
    peer = validate_peer(peer_id)
    wait_scope = default_wait_scope(session_label, scope)
    if rendezvous_dir:
        resolved_rv = resolve_rendezvous_dir(rendezvous_dir)
    elif store is not None:
        resolved_rv = store.rendezvous_dir
    else:
        resolved_rv = resolve_rendezvous_dir(home_dir)

    ignored_notice: Optional[str] = (
        "wait is strictly read-only; auto_pickup is ignored. Call consilium_pickup_inbox explicitly."
        if auto_pickup
        else None
    )

    capped_timeout = max(0.1, float(timeout_sec))
    seq_before = read_peer_seq(resolved_rv, peer)
    if (
        seq_before["unacked"] == 0
        and seq_before["queued_tasks"] == 0
        and store is not None
    ):
        db_unacked = store.count_unacked_events(peer)
        db_queued = store.count_queued_tasks(peer)
        if db_unacked > 0 or db_queued > 0:
            seq_before = store.sync_peer_seq(peer, pulse=False)

    view_before = scoped_seq(seq_before, wait_scope)
    if view_before["unacked"] > 0 or view_before["queued_tasks"] > 0:
        view_after = scoped_seq(read_peer_seq(resolved_rv, peer), wait_scope)
        out: Dict[str, Any] = {
            "peer_id": peer,
            "woke": True,
            "source": "immediate",
            "scope": wait_scope,
            "unacked_events": view_after["unacked"],
            "queued_tasks": view_after["queued_tasks"],
            "wake_seq": view_after["seq"],
            "inbox": None,
        }
        if ignored_notice:
            out["auto_pickup_ignored"] = True
            out["notice"] = ignored_notice
        return out

    lease_fd: Optional[int] = None
    lease_path: Optional[str] = None
    woke = False
    source = "timeout"

    try:
        lease_fd, lease_path = _acquire_lease(
            resolved_rv,
            peer,
            time.time() + capped_timeout + 5.0,
            session_label=session_label,
        )
        if hasattr(select, "kqueue"):
            woke, source = _wait_via_kqueue(
                resolved_rv, peer, capped_timeout, initial_seq=view_before["seq"], scope=wait_scope
            )
        else:
            woke, source = _wait_via_unix_or_poll(
                resolved_rv,
                peer,
                capped_timeout,
                poll_fallback_sec,
                initial_seq=view_before["seq"],
                scope=wait_scope,
            )
    finally:
        _release_lease(lease_fd, lease_path)

    view_after = scoped_seq(read_peer_seq(resolved_rv, peer), wait_scope)
    out = {
        "peer_id": peer,
        "woke": woke,
        "source": source,
        "scope": wait_scope,
        "unacked_events": view_after["unacked"],
        "queued_tasks": view_after["queued_tasks"],
        "wake_seq": view_after["seq"],
        "inbox": None,
    }
    if ignored_notice:
        out["auto_pickup_ignored"] = True
        out["notice"] = ignored_notice
    return out


def _truncate_pre_encode(text: str, max_encoded_chars: int) -> str:
    """Truncate text BEFORE URL encoding so multi-byte UTF-8 %XX sequences are never split."""
    if max_encoded_chars <= 0 or not text:
        return ""
    out: List[str] = []
    used = 0
    for ch in text:
        enc = urllib.parse.quote(ch, safe="")
        if used + len(enc) > max_encoded_chars:
            break
        out.append(ch)
        used += len(enc)
    return "".join(out)


class ColdDoorbell:
    """Safe, non-focus-stealing macOS notification and deep-link fallback."""

    @staticmethod
    def build_deeplink(
        peer_id: str,
        pickup_prompt: str,
        workspace_path: str = "",
        max_url_len: int = DEFAULT_DEEPLINK_MAX_LEN,
    ) -> str:
        """Build a valid native desktop deep-link URL truncated safely before percent-encoding."""
        peer = validate_peer(peer_id)
        clean = (pickup_prompt or "").strip().lstrip("/").strip() or "Check consilium inbox"

        if peer == "claude":
            prefix = "claude://code/new?q="
            folder_param = (
                "&folder=" + urllib.parse.quote(workspace_path, safe="")
                if workspace_path
                else ""
            )
            budget = max(32, max_url_len - len(prefix) - len(folder_param))
            safe_q = _truncate_pre_encode(clean, budget)
            return f"{prefix}{urllib.parse.quote(safe_q, safe='')}{folder_param}"

        if peer == "chatgpt":
            prefix = "codex://new?prompt="
            path_param = (
                "&path=" + urllib.parse.quote(os.path.abspath(workspace_path), safe="")
                if workspace_path
                else ""
            )
            budget = max(32, max_url_len - len(prefix) - len(path_param))
            safe_prompt = _truncate_pre_encode(clean, budget)
            return f"{prefix}{urllib.parse.quote(safe_prompt, safe='')}{path_param}"

        return "antigravity://"

    @classmethod
    def ring_cold(
        cls,
        peer_id: str,
        title: str,
        body: str,
        pickup_prompt: str,
        workspace_path: str = "",
        open_deeplink: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Emit an argv-safe macOS notification (and optional deep link) without keystrokes."""
        peer = validate_peer(peer_id)
        deeplink = cls.build_deeplink(peer, pickup_prompt, workspace_path=workspace_path)
        effective_dry_run = _is_dry_run(dry_run)
        subtitle = f"Target: {peer}"
        notified = False
        opened = False

        if not effective_dry_run:
            cmd = [
                "osascript",
                "-e",
                "on run argv",
                "-e",
                (
                    "display notification (item 1 of argv) "
                    "with title (item 2 of argv) "
                    "subtitle (item 3 of argv) "
                    'sound name "Glass"'
                ),
                "-e",
                "end run",
                "--",
                body[:240],
                title[:100],
                subtitle,
            ]
            try:
                res = subprocess.run(
                    cmd, capture_output=True, text=True, check=False, timeout=10.0
                )
                notified = res.returncode == 0
                command_returncode: Optional[int] = int(res.returncode)
                command_status = "ok" if notified else "failed"
            except (OSError, subprocess.TimeoutExpired):
                notified = False
                command_returncode = None
                command_status = "failed"
            if open_deeplink:
                try:
                    open_res = subprocess.run(
                        ["open", deeplink],
                        capture_output=True,
                        text=True,
                        check=False,
                        timeout=10.0,
                    )
                    opened = open_res.returncode == 0
                except (OSError, subprocess.TimeoutExpired):
                    opened = False
        else:
            command_returncode = None
            command_status = "dry_run"

        return {
            "delivery": "cold_notification",
            "peer_id": peer,
            "deeplink": deeplink,
            "pickup_prompt": pickup_prompt,
            "attempted": True,
            "dry_run": effective_dry_run,
            "command_status": command_status,
            "command_ok": (not effective_dry_run) and notified,
            "command_returncode": command_returncode,
            "peer_acked": False,
            "notified": notified,
            "opened": opened,
        }


def ring_peer(
    store: Optional["BusStore"],
    home_dir: str,
    peer_id: str,
    title: str,
    body: str,
    pickup_prompt: str,
    workspace_path: str = "",
    open_deeplink: bool = False,
    dry_run: bool = False,
    event_ids: Optional[List[int]] = None,
    escalation_delay_sec: Optional[float] = None,
    on_escalate: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Ring peer_id via hot kqueue/socket wakeup, escalating to cold if unacked."""
    peer = validate_peer(peer_id)
    rv_dir = store.rendezvous_dir if store is not None else resolve_rendezvous_dir(home_dir)
    woken_count = notify_peer(rv_dir, peer, store=store)
    if woken_count > 0:
        delay = (
            float(escalation_delay_sec)
            if escalation_delay_sec is not None
            else float(os.environ.get("CONSILIUM_ESCALATION_SEC", str(DEFAULT_ESCALATION_SEC)))
        )
        escalation_scheduled = False
        if store is not None and event_ids and delay > 0:
            tracked_ids = list(event_ids)
            escalation_scheduled = True

            def _escalate_if_unacked() -> None:
                try:
                    if any(not store.is_event_acked(eid) for eid in tracked_ids):
                        cold_res = ColdDoorbell.ring_cold(
                            peer_id=peer,
                            title=f"{title} (Escalated)",
                            body=body,
                            pickup_prompt=pickup_prompt,
                            workspace_path=workspace_path,
                            open_deeplink=open_deeplink,
                            dry_run=dry_run,
                        )
                        if on_escalate is not None:
                            on_escalate(cold_res)
                except Exception:
                    pass

            timer = threading.Timer(delay, _escalate_if_unacked)
            timer.daemon = True
            timer.start()

        return {
            "delivery": "hot_wakeup",
            "peer_id": peer,
            "woken_count": woken_count,
            "escalation_scheduled": escalation_scheduled,
        }
    return ColdDoorbell.ring_cold(
        peer_id=peer,
        title=title,
        body=body,
        pickup_prompt=pickup_prompt,
        workspace_path=workspace_path,
        open_deeplink=open_deeplink,
        dry_run=dry_run,
    )
