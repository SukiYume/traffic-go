#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RELEASE_DIR="${ROOT_DIR}/release/linux-amd64"
ARCHIVE_PATH="${ROOT_DIR}/release/traffic-go-linux-amd64.tar.gz"

find_go() {
  if command -v go >/dev/null 2>&1; then
    command -v go
    return 0
  fi

  if [[ -x "${ROOT_DIR}/.tools/go/bin/go.exe" ]]; then
    printf '%s\n' "${ROOT_DIR}/.tools/go/bin/go.exe"
    return 0
  fi

  return 1
}

find_npm() {
  if command -v npm >/dev/null 2>&1; then
    command -v npm
    return 0
  fi

  if command -v npm.cmd >/dev/null 2>&1; then
    command -v npm.cmd
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

mkdir -p "${ROOT_DIR}/release"
rm -rf "${RELEASE_DIR}"
mkdir -p "${RELEASE_DIR}"

pushd "${ROOT_DIR}/web" >/dev/null
"${NPM_BIN}" run build
popd >/dev/null

mkdir -p "${ROOT_DIR}/internal/embed/dist"
rm -rf "${ROOT_DIR}/internal/embed/dist"/*
cp -R "${ROOT_DIR}/web/dist/." "${ROOT_DIR}/internal/embed/dist/"

pushd "${ROOT_DIR}" >/dev/null
env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 "${GO_BIN}" build -o "${RELEASE_DIR}/traffic-go" ./cmd/traffic-go
popd >/dev/null

cp "${ROOT_DIR}/deploy/config.example.yaml" "${RELEASE_DIR}/config.yaml"
cp "${ROOT_DIR}/deploy/install-centos7.sh" "${RELEASE_DIR}/install-centos7.sh"
chmod +x "${RELEASE_DIR}/install-centos7.sh"

tar -C "${RELEASE_DIR}" -czf "${ARCHIVE_PATH}" .

cat <<EOF
Build complete.

Output directory:
  ${RELEASE_DIR}

Archive:
  ${ARCHIVE_PATH}

Upload one of these options to the server:
  1. ${ARCHIVE_PATH}
  2. ${RELEASE_DIR}/traffic-go
     ${RELEASE_DIR}/config.yaml
     ${RELEASE_DIR}/install-centos7.sh
EOF
