#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_BIN="${REPO_ROOT}/build/Consilium.app/Contents/MacOS/Consilium"

if [[ ! -x "${APP_BIN}" ]]; then
  echo "Consilium.app binary not found; run scripts/build_app.sh first." >&2
  exit 1
fi

unset CLAUDE_CONFIG_DIR
export PYTHONDONTWRITEBYTECODE=1
export CONSILIUM_DRY_RUN=1
export CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1

TMP_ROOT="$(mktemp -d /tmp/consilium-app-smoke.XXXXXX)"
EXT_PID=""

cleanup() {
  local rc=$?
  if [[ -n "${EXT_PID}" ]] && kill -0 "${EXT_PID}" 2>/dev/null; then
    kill -TERM "${EXT_PID}" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      kill -0 "${EXT_PID}" 2>/dev/null || break
      sleep 0.2
    done
    kill -KILL "${EXT_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_ROOT}"
  exit "${rc}"
}
trap cleanup EXIT INT TERM

pick_free_port() {
  /usr/bin/python3 -B -c '
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
'
}

PORT_1="$(pick_free_port)"
STATE_1="${TMP_ROOT}/state1"
RV_1="${TMP_ROOT}/rv1"
SMOKE_1="${TMP_ROOT}/smoke1.json"

echo "[smoke] Case 1: Consilium.app managed daemon lifecycle on 127.0.0.1:${PORT_1}"
(
  cd /tmp
  env \
    -u CLAUDE_CONFIG_DIR \
    CONSILIUM_PORT="${PORT_1}" \
    CONSILIUM_STATE_DIR="${STATE_1}" \
    CONSILIUM_RENDEZVOUS_DIR="${RV_1}" \
    CONSILIUM_DRY_RUN=1 \
    CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    "${APP_BIN}" --smoke-out "${SMOKE_1}" --no-notifications
)

DAEMON_PID_1="$(/usr/bin/python3 -B -c '
import json, socket, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    rep = json.load(fh)
print(json.dumps(rep, ensure_ascii=False))
assert rep["ok"] is True, rep
assert rep["owns_daemon"] is True, rep
assert rep["daemon_pid"] and int(rep["daemon_pid"]) > 0, rep
assert rep["health_status"] == 200, rep
assert rep["login_status"] == 200, rep
assert rep["state_status"] == 200, rep
assert rep["sse_connected"] is True, rep
assert rep["webview_logged_in"] is True, rep
assert rep["webview_conn_text"] == "SSE 实时", rep
print(rep["daemon_pid"])
' "${SMOKE_1}" | tail -n 1)"

sleep 0.3
if kill -0 "${DAEMON_PID_1}" 2>/dev/null; then
  echo "FAIL: managed daemon PID ${DAEMON_PID_1} still alive after Consilium.app exited" >&2
  exit 1
fi
/usr/bin/python3 -B -c '
import socket, sys
port = int(sys.argv[1])
s = socket.socket()
try:
    s.connect(("127.0.0.1", port))
except OSError:
    sys.exit(0)
finally:
    s.close()
sys.exit(1)
' "${PORT_1}"
echo "[smoke] Case 1 OK: managed daemon started, auto-logged in, served SSE, and stopped cleanly on exit."

PORT_2="$(pick_free_port)"
STATE_2="${TMP_ROOT}/state2"
RV_2="${TMP_ROOT}/rv2"
SMOKE_2="${TMP_ROOT}/smoke2.json"
mkdir -p "${STATE_2}" "${RV_2}"

echo "[smoke] Case 2: Consilium.app reuse of pre-running daemon on 127.0.0.1:${PORT_2}"
PYTHONPATH="${REPO_ROOT}" CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 PYTHONDONTWRITEBYTECODE=1 \
  /usr/bin/python3 -B -m consilium.cli serve \
    --host 127.0.0.1 \
    --port "${PORT_2}" \
    --data-dir "${STATE_2}" \
    --rendezvous-dir "${RV_2}" >"${TMP_ROOT}/ext.log" 2>&1 &
EXT_PID=$!

/usr/bin/python3 -B -c '
import json, os, sys, time, urllib.request
port, state_dir = int(sys.argv[1]), sys.argv[2]
tok_path = os.path.join(state_dir, "auth.token")
deadline = time.monotonic() + 10.0
while time.monotonic() < deadline:
    if os.path.exists(tok_path):
        tok = open(tok_path, encoding="utf-8").read().strip()
        if tok:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/health",
                headers={"Authorization": f"Bearer {tok}"},
            )
            try:
                with urllib.request.urlopen(req, timeout=1.5) as resp:
                    if resp.status == 200:
                        sys.exit(0)
            except Exception:
                pass
    time.sleep(0.1)
sys.exit(1)
' "${PORT_2}" "${STATE_2}"

(
  cd /tmp
  env \
    -u CLAUDE_CONFIG_DIR \
    CONSILIUM_PORT="${PORT_2}" \
    CONSILIUM_STATE_DIR="${STATE_2}" \
    CONSILIUM_RENDEZVOUS_DIR="${RV_2}" \
    CONSILIUM_DRY_RUN=1 \
    CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    "${APP_BIN}" --smoke-out "${SMOKE_2}" --no-notifications
)

/usr/bin/python3 -B -c '
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    rep = json.load(fh)
print(json.dumps(rep, ensure_ascii=False))
assert rep["ok"] is True, rep
assert rep["owns_daemon"] is False, rep
assert rep.get("daemon_pid") is None, rep
assert rep["health_status"] == 200, rep
assert rep["login_status"] == 200, rep
assert rep["state_status"] == 200, rep
assert rep["sse_connected"] is True, rep
assert rep["webview_logged_in"] is True, rep
assert rep["webview_conn_text"] == "SSE 实时", rep
' "${SMOKE_2}"

if ! kill -0 "${EXT_PID}" 2>/dev/null; then
  echo "FAIL: external daemon PID ${EXT_PID} was unexpectedly stopped when Consilium.app exited" >&2
  exit 1
fi
echo "[smoke] Case 2 OK: reused existing daemon cleanly and left it running on exit."
