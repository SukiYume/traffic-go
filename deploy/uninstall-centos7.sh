#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
SYSTEMD_UNIT="/etc/systemd/system/traffic-go.service"
RUN_USER="traffic-go"
RUN_GROUP="traffic-go"

RUNTIME_FILES=(
  "${SCRIPT_DIR}/traffic-go"
  "${SCRIPT_DIR}/config.yaml"
  "${SCRIPT_DIR}/traffic.db"
)

remove_path() {
  local target="$1"
  if [[ -e "${target}" || -L "${target}" ]]; then
    rm -rf "${target}"
    echo "Removed ${target}"
    return 0
  fi
  echo "Skip ${target} (not found)"
}

remove_glob_matches() {
  local pattern="$1"
  local matched=0
  shopt -s nullglob
  for target in ${pattern}; do
    matched=1
    remove_path "${target}"
  done
  shopt -u nullglob
  if [[ "${matched}" -eq 0 ]]; then
    echo "Skip ${pattern} (not found)"
  fi
}

stop_and_disable_service() {
  if systemctl list-unit-files traffic-go.service >/dev/null 2>&1; then
    systemctl stop traffic-go >/dev/null 2>&1 || true
    systemctl disable traffic-go >/dev/null 2>&1 || true
  fi
}

remove_service_unit() {
  remove_path "${SYSTEMD_UNIT}"
  systemctl daemon-reload
  systemctl reset-failed traffic-go >/dev/null 2>&1 || true
}

remove_service_account() {
  if id -u "${RUN_USER}" >/dev/null 2>&1; then
    userdel "${RUN_USER}" >/dev/null 2>&1 || true
  fi
  if getent group "${RUN_GROUP}" >/dev/null 2>&1; then
    groupdel "${RUN_GROUP}" >/dev/null 2>&1 || true
  fi
}

cleanup_install_root() {
  local target
  for target in "${RUNTIME_FILES[@]}"; do
    remove_path "${target}"
  done

  remove_glob_matches "${SCRIPT_DIR}/config.yaml.bak.*"

  if [[ -d "${SCRIPT_DIR}" ]] && [[ -z "$(find "${SCRIPT_DIR}" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    rmdir "${SCRIPT_DIR}" >/dev/null 2>&1 || true
  fi
}

stop_and_disable_service
remove_service_unit
cleanup_install_root
remove_service_account

echo
echo "traffic-go current-layout uninstall completed."
