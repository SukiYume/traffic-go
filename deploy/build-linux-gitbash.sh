#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

find_go() {
  if command -v go >/dev/null 2>&1; then
    command -v go
    return 0
  fi

  if [[ -f "${ROOT_DIR}/.tools/go/bin/go.exe" ]]; then
    printf '%s\n' "${ROOT_DIR}/.tools/go/bin/go.exe"
    return 0
  fi

  return 1
}

find_npm() {
  if command -v npm.cmd >/dev/null 2>&1; then
    printf '%s\n' "npm.cmd"
    return 0
  fi

  if command -v npm >/dev/null 2>&1; then
    printf '%s\n' "npm"
    return 0
  fi

  return 1
}

GO_BIN="$(find_go || true)"
NPM_BIN="$(find_npm || true)"

if [[ -z "${GO_BIN}" ]]; then
  echo "go not found. Install Go or place it at .tools/go/bin/go.exe" >&2
  exit 1
fi

if [[ -z "${NPM_BIN}" ]]; then
  echo "npm not found. Install Node.js so the frontend can be built." >&2
  exit 1
fi

echo "Using Go: ${GO_BIN}"
echo "Using npm: ${NPM_BIN}"

APP_NAME="traffic-go" \
GO="${GO_BIN}" \
NPM="${NPM_BIN}" \
WEB_DIR="web" \
RELEASE_ROOT="release" \
bash "${SCRIPT_DIR}/package-release.sh"
