#!/usr/bin/env bash
# Cut a GitHub Release: bump the version, test, build, package, tag, push, publish.
#   ./scripts/release.sh 0.2.0            # the real thing
#   ./scripts/release.sh 0.2.0 --dry-run  # checks, build and package only
set -euo pipefail

VERSION="${1:-}"
DRY_RUN=0
[[ "${2:-}" == "--dry-run" ]] && DRY_RUN=1
if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 <major.minor.patch> [--dry-run]" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"
TAG="v${VERSION}"
ARCH="$(uname -m)"
DIST="${REPO_ROOT}/dist"
ZIP="${DIST}/Consilium-${VERSION}-${ARCH}.zip"
DMG="${DIST}/Consilium-${VERSION}-${ARCH}.dmg"
NOTES="${DIST}/release-notes-${VERSION}.md"

fail() { echo "release: $*" >&2; exit 1; }

command -v gh >/dev/null 2>&1 || fail "gh is not installed"
[[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || fail "run this on main"
[[ -z "$(git status --porcelain)" ]] || fail "working tree is not clean; commit or stash first"
git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null && fail "tag ${TAG} already exists"
grep -q "^## \[${VERSION}\]" CHANGELOG.md || fail "CHANGELOG.md has no '## [${VERSION}]' section"

mkdir -p "${DIST}"
# Release notes = this version's section of the changelog.
awk -v v="${VERSION}" '
  $0 ~ "^## \\[" v "\\]" { on = 1; next }
  on && /^## \[/ { exit }
  on { print }
' CHANGELOG.md > "${NOTES}"
[[ -s "${NOTES}" ]] || fail "the ${VERSION} section of CHANGELOG.md is empty"

echo "release: setting version ${VERSION}"
BUILD_NUMBER="$(( $(git rev-list --count HEAD) + 1 ))"
/usr/bin/python3 -B - "${VERSION}" "${BUILD_NUMBER}" <<'PY'
import re, sys
version, build = sys.argv[1], sys.argv[2]
def rewrite(path, pattern, repl):
    text = open(path, encoding="utf-8").read()
    new, n = re.subn(pattern, repl, text, count=1, flags=re.M | re.S)
    if n != 1:
        raise SystemExit(f"release: could not find the version in {path}")
    if new != text:
        open(path, "w", encoding="utf-8").write(new)
rewrite("pyproject.toml", r'^version = "[^"]+"', f'version = "{version}"')
rewrite("consilium/__init__.py", r'^__version__ = "[^"]+"', f'__version__ = "{version}"')
rewrite("app/macos/Info.plist",
        r"(<key>CFBundleShortVersionString</key>\s*<string>)[^<]+(</string>)", rf"\g<1>{version}\g<2>")
rewrite("app/macos/Info.plist",
        r"(<key>CFBundleVersion</key>\s*<string>)[^<]+(</string>)", rf"\g<1>{build}\g<2>")
PY

echo "release: running tests"
env -u CLAUDE_CONFIG_DIR CONSILIUM_DRY_RUN=1 CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC=1 \
  /usr/bin/python3 -B -m unittest discover -s tests

echo "release: building and smoke-testing the app"
./scripts/build_app.sh
./scripts/smoke_test_app.sh

echo "release: packaging ${ZIP}"
rm -f "${ZIP}" "${ZIP}.sha256"
# No resource forks or extended attributes: they only add ._ files to the archive.
/usr/bin/ditto -c -k --norsrc --noextattr --noqtn --keepParent build/Consilium.app "${ZIP}"
VERIFY_DIR="$(mktemp -d /tmp/consilium-release-verify.XXXXXX)"
trap 'rm -rf "${VERIFY_DIR}"' EXIT
/usr/bin/ditto -x -k "${ZIP}" "${VERIFY_DIR}"
/usr/bin/codesign --verify --deep --strict "${VERIFY_DIR}/Consilium.app" || fail "the packaged app does not pass codesign verification"
ln -s /Applications "${VERIFY_DIR}/Applications"
/usr/bin/hdiutil create -quiet -ov -format UDZO -volname "Consilium ${VERSION}" -srcfolder "${VERIFY_DIR}" "${DMG}"
/usr/bin/hdiutil verify "${DMG}"
for package in "${ZIP}" "${DMG}"; do
  name="$(basename "${package}")"
  ( cd "${DIST}" && /usr/bin/shasum -a 256 "${name}" > "${name}.sha256" )
done

if [[ "${DRY_RUN}" == "1" ]]; then
  git checkout -- pyproject.toml consilium/__init__.py app/macos/Info.plist
  echo "release: dry run finished; package is at ${ZIP}, nothing committed or pushed"
  exit 0
fi

if [[ -n "$(git status --porcelain)" ]]; then
  git add pyproject.toml consilium/__init__.py app/macos/Info.plist
  git commit -q -m "release: ${TAG}"
fi
git tag -a "${TAG}" -m "Consilium ${VERSION}"
git push origin main
git push origin "${TAG}"
gh release create "${TAG}" "${ZIP}" "${ZIP}.sha256" "${DMG}" "${DMG}.sha256" \
  --title "Consilium ${VERSION}" --notes-file "${NOTES}" --verify-tag
echo "release: published ${TAG}"
