#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

APP_NAME="${APP_NAME:-traffic-go}"
GO_BIN="${GO:-go}"
NPM_BIN="${NPM:-npm}"
WEB_DIR="${WEB_DIR:-web}"
RELEASE_ROOT="${RELEASE_ROOT:-release}"
RELEASE_DIR="${ROOT_DIR}/${RELEASE_ROOT}/linux-amd64"
ARCHIVE_PATH="${ROOT_DIR}/${RELEASE_ROOT}/${APP_NAME}-linux-amd64.tar.gz"

run_npm() {
  if [[ "${NPM_BIN,,}" == *.cmd ]]; then
    cmd.exe //c "${NPM_BIN} $*"
    return
  fi
  "${NPM_BIN}" "$@"
}

sync_frontend_assets() {
  mkdir -p "${ROOT_DIR}/internal/embed/dist"
  rm -rf "${ROOT_DIR}/internal/embed/dist"/*
  cp -R "${ROOT_DIR}/${WEB_DIR}/dist/." "${ROOT_DIR}/internal/embed/dist/"
}

copy_release_assets() {
  cp "${ROOT_DIR}/deploy/config.example.yaml" "${RELEASE_DIR}/config.yaml"
  cp "${ROOT_DIR}/deploy/config.example.yaml" "${RELEASE_DIR}/config.example.yaml"
  cp "${ROOT_DIR}/deploy/install-centos7.sh" "${RELEASE_DIR}/install-centos7.sh"
  cp "${ROOT_DIR}/deploy/traffic-go.service" "${RELEASE_DIR}/traffic-go.service"
  chmod +x "${RELEASE_DIR}/install-centos7.sh"
}

mkdir -p "${ROOT_DIR}/${RELEASE_ROOT}"
rm -rf "${RELEASE_DIR}"
mkdir -p "${RELEASE_DIR}"
rm -f "${ARCHIVE_PATH}"

pushd "${ROOT_DIR}" >/dev/null
run_npm --prefix "${WEB_DIR}" run test
run_npm --prefix "${WEB_DIR}" run build
sync_frontend_assets
"${GO_BIN}" test ./...
env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 "${GO_BIN}" build -o "${RELEASE_DIR}/${APP_NAME}" "./cmd/${APP_NAME}"
popd >/dev/null

copy_release_assets

tar -C "${RELEASE_DIR}" -czf "${ARCHIVE_PATH}" .

cat <<EOF
Build complete.

Output directory:
  ${RELEASE_DIR}

Archive:
  ${ARCHIVE_PATH}
EOF
