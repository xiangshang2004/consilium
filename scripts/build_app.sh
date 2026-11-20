#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_SRC="${REPO_ROOT}/app/macos"
BUILD_ROOT="${REPO_ROOT}/build"
APP_BUNDLE="${BUILD_ROOT}/Consilium.app"
CONTENTS_DIR="${APP_BUNDLE}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
RESOURCES_DIR="${CONTENTS_DIR}/Resources"
PY_DEST="${RESOURCES_DIR}/python/consilium"
ARCH="$(uname -m)"

command -v iconutil >/dev/null 2>&1 || { echo "Missing system tool: iconutil" >&2; exit 1; }
command -v codesign >/dev/null 2>&1 || { echo "Missing system tool: codesign" >&2; exit 1; }
SWIFTC_BIN="$(command -v swiftc || xcrun --find swiftc 2>/dev/null || true)"
if [[ -z "${SWIFTC_BIN}" || ! -x "${SWIFTC_BIN}" ]]; then
  echo "Missing Command Line Tools swiftc" >&2
  exit 1
fi

rm -rf "${APP_BUNDLE}"
mkdir -p "${MACOS_DIR}" "${RESOURCES_DIR}" "${PY_DEST}"

cp "${APP_SRC}/Info.plist" "${CONTENTS_DIR}/Info.plist"
cp "${APP_SRC}/Resources/StatusIconTemplate.png" "${RESOURCES_DIR}/StatusIconTemplate.png"
cp "${APP_SRC}/Resources/StatusIconTemplate@2x.png" "${RESOURCES_DIR}/StatusIconTemplate@2x.png"

/usr/bin/iconutil -c icns "${APP_SRC}/Resources/AppIcon.iconset" -o "${RESOURCES_DIR}/AppIcon.icns"

# Copy pure-stdlib consilium Python package without __pycache__/.pyc so ad-hoc signature stays intact.
/usr/bin/python3 -B -c '
import os, shutil, sys
src, dst = sys.argv[1], sys.argv[2]
shutil.copytree(
    src,
    dst,
    dirs_exist_ok=True,
    ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo", ".DS_Store"),
)
' "${REPO_ROOT}/consilium" "${PY_DEST}"

"${SWIFTC_BIN}" \
  -O \
  -swift-version 5 \
  -target "${ARCH}-apple-macosx13.0" \
  -framework Cocoa \
  -framework WebKit \
  -framework UserNotifications \
  "${APP_SRC}/Sources"/*.swift \
  -o "${MACOS_DIR}/Consilium"

/usr/bin/codesign --force --deep --sign - "${APP_BUNDLE}"
/usr/bin/codesign --verify --deep --strict "${APP_BUNDLE}"

echo "Built and signed: ${APP_BUNDLE}"
ls -lh "${MACOS_DIR}/Consilium" "${RESOURCES_DIR}/AppIcon.icns"
