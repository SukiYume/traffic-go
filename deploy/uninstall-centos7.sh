#!/usr/bin/env bash
set -euo pipefail

PURGE_DATA=0
ASSUME_YES=0

usage() {
  cat <<'EOF'
Usage: uninstall-centos7.sh [--purge-data] [--yes]

By default the service, binary, and systemd unit are removed, while the nologin
service account, config.yaml, traffic.db, SQLite sidecars, and backups remain.

  --purge-data  Permanently delete retained configuration and database files.
  --yes         Confirm --purge-data non-interactively. Has no effect otherwise.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --purge-data)
      PURGE_DATA=1
      ;;
    --yes)
      ASSUME_YES=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root." >&2
  exit 1
fi
if [[ -L "${BASH_SOURCE[0]}" ]]; then
  echo "Refusing to run the uninstaller through a symlink." >&2
  exit 1
fi

for early_command in flock stat; do
  if ! command -v "${early_command}" >/dev/null 2>&1; then
    echo "Required command not found: ${early_command}" >&2
    exit 1
  fi
done
DEPLOY_LOCK="/run/traffic-go-install.lock"
if [[ -L "${DEPLOY_LOCK}" || ( -e "${DEPLOY_LOCK}" && ! -f "${DEPLOY_LOCK}" ) ]]; then
  echo "Deployment lock must be a regular, non-symlink file: ${DEPLOY_LOCK}" >&2
  exit 1
fi
if [[ -e "${DEPLOY_LOCK}" && "$(stat -c '%u' "${DEPLOY_LOCK}")" != 0 ]]; then
  echo "Deployment lock must be owned by root: ${DEPLOY_LOCK}" >&2
  exit 1
fi
umask 077
exec 9>"${DEPLOY_LOCK}"
if [[ "$(stat -c '%u' "${DEPLOY_LOCK}")" != 0 ]]; then
  echo "Deployment lock must be owned by root: ${DEPLOY_LOCK}" >&2
  exit 1
fi
chmod 0600 "${DEPLOY_LOCK}"
if ! flock -n 9; then
  echo "Another traffic-go install or uninstall transaction is already running." >&2
  exit 1
fi

SCRIPT_DIR_LOGICAL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -L)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
if [[ "${SCRIPT_DIR_LOGICAL}" != "${SCRIPT_DIR}" ]]; then
  echo "Refusing to run from an install path containing symlink components: ${SCRIPT_DIR_LOGICAL}" >&2
  exit 1
fi
for command_name in awk getfacl grep id pgrep readlink stat systemctl; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done
LAYOUT_FILE="${SCRIPT_DIR}/.traffic-go-state-dir"
INSTALL_MARKER="${SCRIPT_DIR}/.traffic-go-managed-install"
AUTHORITY_FILE="/etc/traffic-go-install-layout"
RECORDED_STATE_DIR=""
RECORDED_DB_PATH=""
validate_managed_path() {
  local label="$1"
  local value="$2"
  if [[ "${value}" != /* || ! "${value}" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
    echo "Unsafe ${label}: ${value}" >&2
    return 1
  fi
  if [[ "${value#/}" != */* ]]; then
    echo "${label} must not be a top-level filesystem directory: ${value}" >&2
    return 1
  fi
  case "${value}" in
    /|/bin|/boot|/dev|/etc|/home|/lib|/lib32|/lib64|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/usr/local|/var|/var/lib)
      echo "${label} resolves to a protected system directory: ${value}" >&2
      return 1
      ;;
  esac
  case "${value}" in
    /usr/local/*|/var/lib/*) ;;
    /bin/*|/boot/*|/dev/*|/etc/*|/home/*|/lib/*|/lib32/*|/lib64/*|/proc/*|/root/*|/run/*|/sbin/*|/sys/*|/tmp/*|/usr/*|/var/*)
      echo "${label} must not be nested under a protected system tree: ${value}" >&2
      return 1
      ;;
  esac
  case "${value##*/}" in
    traffic-go|traffic-go-*|traffic-go_*) ;;
    *)
      echo "${label} must use a dedicated leaf directory named traffic-go or traffic-go-*: ${value}" >&2
      return 1
      ;;
  esac
}

reject_symlink_components() {
  local label="$1"
  local value="$2"
  local probe="${value%/}"
  while [[ -n "${probe}" && "${probe}" != / ]]; do
    if [[ -L "${probe}" ]]; then
      echo "${label} must not contain a symlink component: ${probe}" >&2
      return 1
    fi
    probe="${probe%/*}"
    [[ -n "${probe}" ]] || probe=/
  done
}

has_unsafe_write_acl() {
  local target="$1"
  local acl
  if ! acl="$(getfacl -cp -- "${target}")"; then
    echo "Unable to inspect POSIX ACLs: ${target}" >&2
    return 0
  fi
  awk -F: '
    /^user:[^:]+:/ && $3 ~ /w/ { unsafe = 1 }
    /^group:[^:]+:/ && $3 ~ /w/ { unsafe = 1 }
    /^default:/ && $0 ~ /w/ { unsafe = 1 }
    END { exit unsafe ? 0 : 1 }
  ' <<<"${acl}"
}

require_secure_parent_chain() {
  local label="$1"
  local target="$2"
  local probe="${target%/*}"
  local owner mode
  [[ -n "${probe}" ]] || probe=/
  while true; do
    if [[ ! -d "${probe}" || -L "${probe}" ]]; then
      echo "${label} parent chain must contain only existing real directories: ${probe}" >&2
      return 1
    fi
    read -r owner mode < <(stat -c '%u %a' -- "${probe}")
    if [[ "${owner}" != 0 ]] || (( (8#${mode} & 8#022) != 0 )) || has_unsafe_write_acl "${probe}"; then
      echo "${label} parent chain must be root-owned and not writable through mode bits or ACLs: ${probe}" >&2
      return 1
    fi
    [[ "${probe}" == / ]] && break
    probe="${probe%/*}"
    [[ -n "${probe}" ]] || probe=/
  done
}

require_managed_marker() {
  local marker="$1"
  local role="$2"
  local path="$3"
  if [[ ! -f "${marker}" || -L "${marker}" || "$(stat -c '%u' "${marker}" 2>/dev/null || true)" != 0 ]]; then
    echo "Refusing to uninstall without a root-owned regular ${role} management marker: ${marker}" >&2
    return 1
  fi
  if [[ "$(<"${marker}")" != $'role='"${role}"$'\npath='"${path}" ]]; then
    echo "Management marker does not bind this ${role} path: ${marker}" >&2
    return 1
  fi
}

validate_managed_path "install root" "${SCRIPT_DIR}"
reject_symlink_components "Install root" "${SCRIPT_DIR_LOGICAL}"
require_secure_parent_chain "Install root" "${SCRIPT_DIR}"
require_managed_marker "${INSTALL_MARKER}" install "${SCRIPT_DIR}"
if [[ ! -f "${LAYOUT_FILE}" || -L "${LAYOUT_FILE}" || "$(stat -c '%u' "${LAYOUT_FILE}" 2>/dev/null || true)" != 0 ]]; then
  echo "Refusing to uninstall without the root-owned traffic-go layout file: ${LAYOUT_FILE}" >&2
  exit 2
fi
while IFS= read -r layout_line; do
  case "${layout_line}" in
    state_dir=*) RECORDED_STATE_DIR="${layout_line#state_dir=}" ;;
    db_path=*) RECORDED_DB_PATH="${layout_line#db_path=}" ;;
  esac
done <"${LAYOUT_FILE}"
if [[ -n "${TRAFFIC_GO_STATE_DIR:-}" || -n "${TRAFFIC_GO_DB_PATH:-}" ]]; then
  echo "Uninstall paths are bound by ${LAYOUT_FILE}; environment path overrides are not accepted." >&2
  exit 2
fi
if [[ -z "${RECORDED_STATE_DIR}" || -z "${RECORDED_DB_PATH}" ]]; then
  echo "Layout file is missing state_dir or db_path; refusing destructive inference." >&2
  exit 2
fi
reject_symlink_components "State directory" "${RECORDED_STATE_DIR}"
reject_symlink_components "Database path" "${RECORDED_DB_PATH}"
STATE_DIR="$(readlink -m -- "${RECORDED_STATE_DIR}")"
validate_managed_path "state directory" "${STATE_DIR}"
require_secure_parent_chain "State directory" "${STATE_DIR}"
case "${SCRIPT_DIR}/" in
  "${STATE_DIR}/"*) echo "Install root must not be inside the state directory." >&2; exit 2 ;;
esac
case "${STATE_DIR}/" in
  "${SCRIPT_DIR}/"*) echo "State directory must not be inside the install root." >&2; exit 2 ;;
esac
STATE_MARKER="${STATE_DIR}/.traffic-go-managed-state"
require_managed_marker "${STATE_MARKER}" state "${STATE_DIR}"
DB_PATH="$(readlink -m -- "${RECORDED_DB_PATH}")"
case "${DB_PATH}" in
  "${STATE_DIR}"/*) ;;
  *) echo "Recorded database path escapes the validated state directory: ${DB_PATH}" >&2; exit 2 ;;
esac
if [[ "${DB_PATH%/*}" != "${STATE_DIR}" ]]; then
  echo "Recorded database must be a direct file below the managed state directory: ${DB_PATH}" >&2
  exit 2
fi
STATE_METADATA_FROZEN=0
STATE_ORIGINAL_UID=""
STATE_ORIGINAL_GID=""
STATE_ORIGINAL_MODE=""
RUN_UID="$(id -u traffic-go 2>/dev/null || true)"
read -r STATE_ORIGINAL_UID STATE_ORIGINAL_GID STATE_ORIGINAL_MODE < <(stat -c '%u %g %a' -- "${STATE_DIR}")
if [[ "${PURGE_DATA}" -ne 1 ]]; then
  if [[ -z "${RUN_UID}" || "${STATE_ORIGINAL_UID}" != "${RUN_UID}" ]]; then
    echo "Default uninstall requires the state directory to be owned by the dedicated traffic-go account." >&2
    exit 2
  fi
fi
WAS_ACTIVE=0
WAS_ENABLED=0
SERVICE_STATE_RECORDED=0
DELETION_STARTED=0

restore_state_namespace() {
  [[ "${STATE_METADATA_FROZEN}" -eq 1 ]] || return 0
  local failed=0
  if [[ -d "${STATE_DIR}" && ! -L "${STATE_DIR}" ]]; then
    chown "${STATE_ORIGINAL_UID}:${STATE_ORIGINAL_GID}" "${STATE_DIR}" || failed=1
    chmod "${STATE_ORIGINAL_MODE}" "${STATE_DIR}" || failed=1
  fi
  if [[ "${failed}" -eq 0 ]]; then
    STATE_METADATA_FROZEN=0
  fi
  return "${failed}"
}

on_exit() {
  local status=$?
  local state_restored=1
  local service_restore_failed=0
  trap - EXIT
  if ! restore_state_namespace; then
    state_restored=0
    [[ "${status}" -ne 0 ]] || status=1
  fi
  if [[ "${state_restored}" -eq 1 && "${status}" -ne 0 && "${SERVICE_STATE_RECORDED}" -eq 1 && "${DELETION_STARTED}" -eq 0 ]]; then
    systemctl daemon-reload >/dev/null 2>&1 || service_restore_failed=1
    if [[ "${WAS_ENABLED}" -eq 1 ]]; then
      systemctl enable traffic-go.service >/dev/null 2>&1 || service_restore_failed=1
    fi
    if [[ "${WAS_ACTIVE}" -eq 1 && "${service_restore_failed}" -eq 0 ]]; then
      systemctl restart traffic-go.service >/dev/null 2>&1 || service_restore_failed=1
    elif [[ "${WAS_ACTIVE}" -eq 1 ]]; then
      echo "Uninstall preflight failed and the original service could not be safely restarted." >&2
    fi
    if [[ "${service_restore_failed}" -ne 0 ]]; then
      echo "Failed to restore the original traffic-go service state after aborting uninstall." >&2
    fi
  fi
  exit "${status}"
}
trap on_exit EXIT
SYSTEMD_UNIT="/etc/systemd/system/traffic-go.service"
MODULES_CONFIG="/etc/modules-load.d/traffic-go.conf"
SYSCTL_CONFIG="/etc/sysctl.d/99-traffic-go.conf"
if [[ ! -f "${AUTHORITY_FILE}" || -L "${AUTHORITY_FILE}" || "$(stat -c '%u' "${AUTHORITY_FILE}" 2>/dev/null || true)" != 0 ]]; then
  echo "Refusing to touch the global service without its root-owned authority file: ${AUTHORITY_FILE}" >&2
  exit 2
fi
AUTH_INSTALL_ROOT=""
AUTH_STATE_DIR=""
AUTH_DB_PATH=""
while IFS= read -r authority_line; do
  case "${authority_line}" in
    install_root=*) AUTH_INSTALL_ROOT="${authority_line#install_root=}" ;;
    state_dir=*) AUTH_STATE_DIR="${authority_line#state_dir=}" ;;
    db_path=*) AUTH_DB_PATH="${authority_line#db_path=}" ;;
  esac
done <"${AUTHORITY_FILE}"
if [[ "${AUTH_INSTALL_ROOT}" != "${SCRIPT_DIR}" || "${AUTH_STATE_DIR}" != "${STATE_DIR}" || "${AUTH_DB_PATH}" != "${DB_PATH}" ]]; then
  echo "This uninstaller is not authoritative for the currently registered traffic-go service." >&2
  echo "Registered install=${AUTH_INSTALL_ROOT} state=${AUTH_STATE_DIR} db=${AUTH_DB_PATH}" >&2
  exit 2
fi
if [[ -e "${SYSTEMD_UNIT}" || -L "${SYSTEMD_UNIT}" ]]; then
  if [[ ! -f "${SYSTEMD_UNIT}" || -L "${SYSTEMD_UNIT}" || "$(stat -c '%u' "${SYSTEMD_UNIT}" 2>/dev/null || true)" != 0 ]]; then
    echo "Global traffic-go unit must be a root-owned regular file: ${SYSTEMD_UNIT}" >&2
    exit 2
  fi
  if ! grep -Fxq "WorkingDirectory=${STATE_DIR}" "${SYSTEMD_UNIT}" ||
     ! grep -Fxq "ExecStart=${SCRIPT_DIR}/traffic-go -config ${SCRIPT_DIR}/config.yaml" "${SYSTEMD_UNIT}"; then
    echo "Global traffic-go unit is not bound to this install/state layout; refusing uninstall." >&2
    exit 2
  fi
fi
remove_file() {
  local target="$1"
  if [[ -d "${target}" && ! -L "${target}" ]]; then
    echo "Refusing to remove directory where a file was expected: ${target}" >&2
    return 1
  fi
  if [[ -e "${target}" || -L "${target}" ]]; then
    rm -f -- "${target}"
    echo "Removed ${target}"
  fi
}

record_and_stop_service() {
  local active_state enabled_state
  active_state="$(systemctl is-active traffic-go.service 2>/dev/null || true)"
  case "${active_state}" in
    active|activating|reloading) WAS_ACTIVE=1 ;;
  esac
  enabled_state="$(systemctl is-enabled traffic-go.service 2>/dev/null || true)"
  case "${enabled_state}" in
    enabled|enabled-runtime|linked|linked-runtime|alias) WAS_ENABLED=1 ;;
  esac
  SERVICE_STATE_RECORDED=1
  systemctl stop traffic-go.service >/dev/null 2>&1 || true
  active_state="$(systemctl is-active traffic-go.service 2>/dev/null || true)"
  case "${active_state}" in
    inactive|failed|unknown) ;;
    *)
      echo "traffic-go.service state is '${active_state:-unavailable}'; refusing to unlink live executable or SQLite files without an explicit inactive result." >&2
      return 1
      ;;
  esac

}

confirm_purge() {
  if [[ "${PURGE_DATA}" -ne 1 || "${ASSUME_YES}" -eq 1 ]]; then
    return 0
  fi
  if [[ ! -t 0 ]]; then
    echo "--purge-data requires an interactive PURGE confirmation or --yes." >&2
    exit 2
  fi
  local answer
  printf 'Type PURGE to permanently delete traffic-go config and database files: '
  read -r answer
  if [[ "${answer}" != "PURGE" ]]; then
    echo "Purge confirmation did not match; cancelling the entire uninstall." >&2
    exit 2
  fi
}

retained_files=()
collect_data_files() {
  local candidate
  for candidate in \
    "${SCRIPT_DIR}/config.yaml" \
    "${SCRIPT_DIR}/traffic.db" \
    "${SCRIPT_DIR}/traffic.db-wal" \
    "${SCRIPT_DIR}/traffic.db-shm" \
    "${DB_PATH}" \
    "${DB_PATH}-wal" \
    "${DB_PATH}-shm"; do
    if [[ -e "${candidate}" || -L "${candidate}" ]]; then
      retained_files+=("${candidate}")
    fi
  done
  shopt -s nullglob
  for candidate in "${SCRIPT_DIR}"/config.yaml.bak.*; do
    retained_files+=("${candidate}")
  done
  shopt -u nullglob
}

preserve_or_purge_data() {
  local target failed=0
  if [[ "${PURGE_DATA}" -eq 1 ]]; then
    for target in "${retained_files[@]}"; do
      remove_file "${target}"
    done
    return 0
  fi

  for target in "${retained_files[@]}"; do
    if [[ -f "${target}" && ! -L "${target}" ]]; then
      if ! chmod 0600 "${target}"; then
        echo "Unable to restrict retained file permissions: ${target}" >&2
        failed=1
      fi
      case "${target}" in
        "${STATE_DIR}"/*) ;;
        *)
          if ! chown root:root "${target}"; then
            echo "Unable to restore root ownership on retained file: ${target}" >&2
            failed=1
          fi
          ;;
      esac
    fi
  done
  return "${failed}"
}

preflight_file_removals() {
  local target
  for target in "$@"; do
    if [[ -d "${target}" && ! -L "${target}" ]]; then
      echo "Refusing to start uninstall because a managed file target is now a directory: ${target}" >&2
      return 1
    fi
  done
}

confirm_purge
record_and_stop_service
PROCESS_UIDS=("${STATE_ORIGINAL_UID}")
if [[ -n "${RUN_UID}" && "${RUN_UID}" != "${STATE_ORIGINAL_UID}" ]]; then
  PROCESS_UIDS+=("${RUN_UID}")
fi
for process_uid in "${PROCESS_UIDS[@]}"; do
  if [[ "${process_uid}" != 0 ]] && pgrep -u "${process_uid}" >/dev/null 2>&1; then
    echo "Processes for traffic-go or the recorded state owner (uid ${process_uid}) remain after stopping the unit; refusing destructive cleanup." >&2
    pgrep -a -u "${process_uid}" >&2 || true
    exit 2
  fi
done
STATE_METADATA_FROZEN=1
chown root:root "${STATE_DIR}"
chmod 0700 "${STATE_DIR}"
# The service account owns the state directory, so repeat every destructive
# binding check after the process has stopped and immediately before unlinking.
reject_symlink_components "Install root" "${SCRIPT_DIR}"
reject_symlink_components "State directory" "${STATE_DIR}"
reject_symlink_components "Database path" "${DB_PATH}"
require_managed_marker "${INSTALL_MARKER}" install "${SCRIPT_DIR}"
require_managed_marker "${STATE_MARKER}" state "${STATE_DIR}"
if [[ ! -f "${AUTHORITY_FILE}" || -L "${AUTHORITY_FILE}" || "$(stat -c '%u' "${AUTHORITY_FILE}" 2>/dev/null || true)" != 0 ]] ||
   ! grep -Fxq "install_root=${SCRIPT_DIR}" "${AUTHORITY_FILE}" ||
   ! grep -Fxq "state_dir=${STATE_DIR}" "${AUTHORITY_FILE}" ||
   ! grep -Fxq "db_path=${DB_PATH}" "${AUTHORITY_FILE}"; then
  echo "Install authority changed while stopping the service; refusing destructive cleanup." >&2
  exit 2
fi
collect_data_files
if [[ "${PURGE_DATA}" -ne 1 ]]; then
  for sqlite_file in "${DB_PATH}" "${DB_PATH}-wal" "${DB_PATH}-shm"; do
    if [[ -e "${sqlite_file}" || -L "${sqlite_file}" ]]; then
      if [[ ! -f "${sqlite_file}" || -L "${sqlite_file}" || "$(stat -c '%u' "${sqlite_file}")" != "${RUN_UID}" ]]; then
        echo "Retained SQLite files must be regular, non-symlink files owned by traffic-go: ${sqlite_file}" >&2
        exit 2
      fi
    fi
  done
fi
removal_targets=(
  "${SYSTEMD_UNIT}"
  "${MODULES_CONFIG}"
  "${SYSCTL_CONFIG}"
  "${SCRIPT_DIR}/traffic-go"
)
if [[ "${PURGE_DATA}" -eq 1 ]]; then
  removal_targets+=(
    "${retained_files[@]}"
    "${STATE_MARKER}"
    "${LAYOUT_FILE}"
    "${INSTALL_MARKER}"
    "${AUTHORITY_FILE}"
  )
fi
preflight_file_removals "${removal_targets[@]}"
if [[ "${WAS_ENABLED}" -eq 1 ]]; then
  systemctl disable traffic-go.service >/dev/null
fi
DELETION_STARTED=1
remove_file "${SYSTEMD_UNIT}"
remove_file "${MODULES_CONFIG}"
remove_file "${SYSCTL_CONFIG}"
remove_file "${SCRIPT_DIR}/traffic-go"
preserve_or_purge_data
if [[ "${PURGE_DATA}" -eq 1 ]]; then
  remove_file "${STATE_DIR}/.traffic-go-managed-state"
  if [[ -d "${STATE_DIR}" ]]; then
    if ! rmdir "${STATE_DIR}" 2>/dev/null; then
      echo "State directory is not empty and was retained without recursive deletion: ${STATE_DIR}" >&2
    fi
  fi
  remove_file "${LAYOUT_FILE}"
  remove_file "${SCRIPT_DIR}/.traffic-go-managed-install"
  remove_file "${AUTHORITY_FILE}"
fi
restore_state_namespace
systemctl daemon-reload
systemctl reset-failed traffic-go.service >/dev/null 2>&1 || true

echo
echo "traffic-go service uninstall completed."
if [[ "${PURGE_DATA}" -eq 1 ]]; then
  echo "Configuration and database files were purged by explicit request."
  echo "The nologin traffic-go system account was retained because config may have referenced data outside ${SCRIPT_DIR}."
  echo "After verifying no custom paths remain, it can be removed with: userdel traffic-go; groupdel traffic-go"
else
  echo "Configuration was retained under ${SCRIPT_DIR}; database files were retained at ${DB_PATH}."
  echo "The disabled traffic-go system account was retained to preserve ownership of any custom database path."
  echo "To remove them permanently, rerun with --purge-data and confirm PURGE."
fi
