#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root." >&2
  exit 1
fi
if [[ -L "${BASH_SOURCE[0]}" ]]; then
  echo "Refusing to run the installer through a symlink." >&2
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BINARY_SRC="${1:-${SCRIPT_DIR}/traffic-go}"
CONFIG_SRC="${2:-${SCRIPT_DIR}/config.yaml}"
SERVICE_TEMPLATE="${3:-${SCRIPT_DIR}/traffic-go.service}"
UNINSTALL_SRC="${SCRIPT_DIR}/uninstall-centos7.sh"
CHECKSUM_FILE="${SCRIPT_DIR}/SHA256SUMS"
ALLOW_UNVERIFIED_PACKAGE="${TRAFFIC_GO_ALLOW_UNVERIFIED_PACKAGE:-0}"
ALLOW_UNVERIFIED_INPUTS="${TRAFFIC_GO_ALLOW_UNVERIFIED_INPUTS:-0}"

RUN_USER="traffic-go"
RUN_GROUP="traffic-go"
INSTALL_ROOT="${TRAFFIC_GO_INSTALL_ROOT:-${SCRIPT_DIR}}"
STATE_DIR="${TRAFFIC_GO_STATE_DIR:-/var/lib/traffic-go}"
INSTALL_BIN="${INSTALL_ROOT}/traffic-go"
INSTALL_CONFIG="${INSTALL_ROOT}/config.yaml"
LAYOUT_FILE="${INSTALL_ROOT}/.traffic-go-state-dir"
UNINSTALL_DEST="${INSTALL_ROOT}/uninstall-centos7.sh"
SYSTEMD_UNIT="/etc/systemd/system/traffic-go.service"
MODULES_CONFIG="/etc/modules-load.d/traffic-go.conf"
SYSCTL_CONFIG="/etc/sysctl.d/99-traffic-go.conf"
AUTHORITY_FILE="/etc/traffic-go-install-layout"
REPLACE_CONFIG="${TRAFFIC_GO_REPLACE_CONFIG:-0}"
ALLOW_LAYOUT_TAKEOVER="${TRAFFIC_GO_ALLOW_LAYOUT_TAKEOVER:-0}"
ROLLBACK_DIR="$(mktemp -d /tmp/traffic-go-install.XXXXXX)"
INSTALL_SUCCEEDED=0
WAS_ACTIVE=0
WAS_ENABLED=0
MIGRATED_LEGACY_DB=0
PREVIOUS_ACCT_VALUE=""
CREATED_RUN_USER=0
CREATED_RUN_GROUP=0
DB_ARTIFACTS_RECORDED=0
DB_BACKUP_DIR=""
EFFECTIVE_DB=""
CONFIG_BACKUP=""
MIGRATION_BACKUP=""
TEMP_FILES=()
PRESERVE_ROLLBACK=0
MANAGED_FILES_MUTATED=0
STATE_MARKER_BACKUP_NAME="state-marker"

cleanup() {
  local temporary cleanup_failed=0
  set +e
  for temporary in "${TEMP_FILES[@]}"; do
    [[ -n "${temporary}" ]] || continue
    if [[ -d "${temporary}" && ! -L "${temporary}" ]]; then
      echo "Refusing to remove a directory found at temporary-file path: ${temporary}" >&2
      cleanup_failed=1
    elif ! rm -f -- "${temporary}"; then
      echo "Unable to remove temporary file: ${temporary}" >&2
      cleanup_failed=1
    fi
  done
  if [[ "${PRESERVE_ROLLBACK}" -eq 1 ]]; then
    echo "Preserving rollback files for manual recovery: ${ROLLBACK_DIR} ${DB_BACKUP_DIR}" >&2
  else
    case "${ROLLBACK_DIR}" in
      /tmp/traffic-go-install.*) rm -rf -- "${ROLLBACK_DIR}" || cleanup_failed=1 ;;
      *) echo "Refusing to clean unexpected rollback path: ${ROLLBACK_DIR}" >&2 ;;
    esac
    if [[ -n "${DB_BACKUP_DIR}" && -d "${DB_BACKUP_DIR}" && ! -L "${DB_BACKUP_DIR}" ]]; then
      case "${DB_BACKUP_DIR}" in
        "${STATE_DIR%/*}/.traffic-go-db-rollback."*) rm -rf -- "${DB_BACKUP_DIR}" || cleanup_failed=1 ;;
        *) echo "Refusing to clean unexpected database rollback path: ${DB_BACKUP_DIR}" >&2 ;;
      esac
    fi
  fi
  if [[ "${cleanup_failed}" -ne 0 ]]; then
    echo "One or more temporary paths could not be cleaned; inspect the warnings above." >&2
  fi
  return 0
}
trap cleanup EXIT

consume_temporary_file() {
  local target="$1"
  local index
  if [[ -d "${target}" && ! -L "${target}" ]]; then
    echo "Temporary file was replaced by a directory: ${target}" >&2
    return 1
  fi
  rm -f -- "${target}"
  for index in "${!TEMP_FILES[@]}"; do
    if [[ "${TEMP_FILES[index]}" == "${target}" ]]; then
      TEMP_FILES[index]=""
    fi
  done
}

consume_all_temporary_files() {
  local temporary failed=0
  for temporary in "${TEMP_FILES[@]}"; do
    [[ -n "${temporary}" ]] || continue
    consume_temporary_file "${temporary}" || failed=1
  done
  return "${failed}"
}

require_regular_file() {
  local target="$1"
  local label="$2"
  if [[ ! -f "${target}" || -L "${target}" ]]; then
    echo "${label} must be a regular, non-symlink file: ${target}" >&2
    exit 1
  fi
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
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

require_secure_input_file() {
  local target="$1"
  local label="$2"
  local owner mode
  require_regular_file "${target}" "${label}"
  require_secure_parent_chain "${label}" "${target}"
  read -r owner mode < <(stat -c '%u %a' -- "${target}")
  if [[ "${owner}" != 0 ]] || (( (8#${mode} & 8#022) != 0 )) || has_unsafe_write_acl "${target}"; then
    echo "${label} must be root-owned and not writable through mode bits or ACLs: ${target}" >&2
    return 1
  fi
}

for command_name in awk getfacl pgrep readlink; do
  require_command "${command_name}"
done
require_regular_file "${BINARY_SRC}" "Binary"
require_regular_file "${CONFIG_SRC}" "Config"
require_regular_file "${SERVICE_TEMPLATE}" "Service template"
BINARY_SRC="$(readlink -f -- "${BINARY_SRC}")"
CONFIG_SRC="$(readlink -f -- "${CONFIG_SRC}")"
SERVICE_TEMPLATE="$(readlink -f -- "${SERVICE_TEMPLATE}")"
require_secure_input_file "${BINARY_SRC}" "Binary"
require_secure_input_file "${CONFIG_SRC}" "Config"
require_secure_input_file "${SERVICE_TEMPLATE}" "Service template"
if [[ -e "${UNINSTALL_SRC}" || -L "${UNINSTALL_SRC}" ]]; then
  UNINSTALL_SRC="$(readlink -f -- "${UNINSTALL_SRC}")"
  require_secure_input_file "${UNINSTALL_SRC}" "Uninstaller"
fi
for command_name in awk curl find getent groupadd groupdel id install journalctl modprobe mountpoint readlink runuser sed seq sha256sum stat systemctl systemd-analyze sysctl useradd userdel; do
  require_command "${command_name}"
done

if [[ -e "${CHECKSUM_FILE}" || -L "${CHECKSUM_FILE}" ]]; then
  require_secure_input_file "${CHECKSUM_FILE}" "Package checksum manifest"
  echo "Verifying packaged files with SHA256SUMS."
  (cd "${SCRIPT_DIR}" && sha256sum --strict --check SHA256SUMS)
elif [[ "${ALLOW_UNVERIFIED_PACKAGE}" == 1 ]]; then
  echo "WARNING: SHA256SUMS is absent; continuing only because TRAFFIC_GO_ALLOW_UNVERIFIED_PACKAGE=1." >&2
else
  echo "SHA256SUMS is required. Set TRAFFIC_GO_ALLOW_UNVERIFIED_PACKAGE=1 only for an intentional source-tree install." >&2
  exit 1
fi

for supplied_pair in \
  "${BINARY_SRC}|${SCRIPT_DIR}/traffic-go|binary" \
  "${CONFIG_SRC}|${SCRIPT_DIR}/config.yaml|config" \
  "${SERVICE_TEMPLATE}|${SCRIPT_DIR}/traffic-go.service|service template"; do
  supplied_path="$(readlink -f -- "${supplied_pair%%|*}")"
  remainder="${supplied_pair#*|}"
  packaged_path="$(readlink -m -- "${remainder%%|*}")"
  supplied_label="${remainder##*|}"
  if [[ "${supplied_path}" != "${packaged_path}" ]]; then
    if [[ "${ALLOW_UNVERIFIED_INPUTS}" != 1 ]]; then
      echo "Custom ${supplied_label} is not covered by the package manifest: ${supplied_path}" >&2
      echo "Set TRAFFIC_GO_ALLOW_UNVERIFIED_INPUTS=1 only after independently verifying every custom input." >&2
      exit 1
    fi
    echo "WARNING: installing independently supplied ${supplied_label}: ${supplied_path}" >&2
  fi
done

validate_managed_path() {
  local label="$1"
  local value="$2"
  if [[ "${value}" != /* || ! "${value}" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
    echo "${label} must be an absolute path containing only letters, digits, '.', '_', '-', and '/': ${value}" >&2
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
  if [[ "${value}" != /* ]]; then
    echo "${label} must be absolute before canonicalization: ${value}" >&2
    return 1
  fi
  while [[ -n "${probe}" && "${probe}" != / ]]; do
    if [[ -L "${probe}" ]]; then
      echo "${label} must not contain a symlink component: ${probe}" >&2
      return 1
    fi
    probe="${probe%/*}"
    [[ -n "${probe}" ]] || probe=/
  done
}

reject_symlink_components "Install root" "${INSTALL_ROOT}"
reject_symlink_components "State directory" "${STATE_DIR}"
INSTALL_ROOT="$(readlink -m -- "${INSTALL_ROOT}")"
STATE_DIR="$(readlink -m -- "${STATE_DIR}")"
validate_managed_path "Install root" "${INSTALL_ROOT}"
validate_managed_path "State directory" "${STATE_DIR}"
require_secure_parent_chain "Install root" "${INSTALL_ROOT}"
require_secure_parent_chain "State directory" "${STATE_DIR}"
case "${INSTALL_ROOT}/" in
  "${STATE_DIR}/"*) echo "Install root must not be inside the state directory." >&2; exit 1 ;;
esac
case "${STATE_DIR}/" in
  "${INSTALL_ROOT}/"*) echo "State directory must not be inside the install root." >&2; exit 1 ;;
esac

case "${ALLOW_LAYOUT_TAKEOVER}" in
  0|1) ;;
  *) echo "TRAFFIC_GO_ALLOW_LAYOUT_TAKEOVER must be 0 or 1." >&2; exit 1 ;;
esac
REGISTERED_INSTALL_ROOT=""
REGISTERED_STATE_DIR=""
REGISTERED_DB_PATH=""
if [[ -e "${AUTHORITY_FILE}" || -L "${AUTHORITY_FILE}" ]]; then
  if [[ ! -f "${AUTHORITY_FILE}" || -L "${AUTHORITY_FILE}" || "$(stat -c '%u' "${AUTHORITY_FILE}" 2>/dev/null || true)" != 0 ]]; then
    echo "Existing install authority must be a root-owned regular file: ${AUTHORITY_FILE}" >&2
    exit 1
  fi
  while IFS= read -r authority_line; do
    case "${authority_line}" in
      install_root=*) REGISTERED_INSTALL_ROOT="${authority_line#install_root=}" ;;
      state_dir=*) REGISTERED_STATE_DIR="${authority_line#state_dir=}" ;;
      db_path=*) REGISTERED_DB_PATH="${authority_line#db_path=}" ;;
    esac
  done <"${AUTHORITY_FILE}"
  if [[ -z "${REGISTERED_INSTALL_ROOT}" || -z "${REGISTERED_STATE_DIR}" || -z "${REGISTERED_DB_PATH}" ]]; then
    echo "Existing install authority is incomplete; refusing to infer a replacement layout." >&2
    exit 1
  fi
  if [[ "${REGISTERED_DB_PATH%/*}" != "${REGISTERED_STATE_DIR}" ]]; then
    echo "Existing install authority records a nested or escaping database path; refusing unsafe automatic upgrade." >&2
    exit 1
  fi
  if [[ "${REGISTERED_INSTALL_ROOT}" != "${INSTALL_ROOT}" || "${REGISTERED_STATE_DIR}" != "${STATE_DIR}" ]]; then
    if [[ "${ALLOW_LAYOUT_TAKEOVER}" != 1 ]]; then
      echo "Another traffic-go layout is already authoritative; refusing silent takeover." >&2
      echo "Registered install=${REGISTERED_INSTALL_ROOT} state=${REGISTERED_STATE_DIR} db=${REGISTERED_DB_PATH}" >&2
      echo "Set TRAFFIC_GO_ALLOW_LAYOUT_TAKEOVER=1 only for a reviewed migration." >&2
      exit 1
    fi
    echo "WARNING: explicitly taking over a different registered traffic-go layout." >&2
  fi
fi

record_path_metadata() {
  local target="$1"
  local name="$2"
  if [[ -e "${target}" && ! -d "${target}" ]]; then
    echo "Expected a directory but found another file type: ${target}" >&2
    return 1
  fi
  if [[ -d "${target}" ]]; then
    printf '%s\n' 1 >"${ROLLBACK_DIR}/${name}.dir-existed"
    stat -c '%u %g %a' "${target}" >"${ROLLBACK_DIR}/${name}.dir-meta"
  else
    printf '%s\n' 0 >"${ROLLBACK_DIR}/${name}.dir-existed"
  fi
}

validate_existing_managed_dir() {
  local target="$1"
  local role="$2"
  local marker="${target}/.traffic-go-managed-${role}"
  local owner_uid
  [[ -d "${target}" ]] || return 0

  owner_uid="$(stat -c '%u' "${target}")"
  if [[ "${role}" == install && "${owner_uid}" != 0 ]]; then
    echo "Existing install root must already be root-owned: ${target}" >&2
    return 1
  fi
  if [[ "${role}" == state && "${owner_uid}" != 0 ]]; then
    local existing_run_uid
    existing_run_uid="$(id -u "${RUN_USER}" 2>/dev/null || true)"
    if [[ -z "${existing_run_uid}" || "${owner_uid}" != "${existing_run_uid}" ]]; then
      echo "Existing state directory must be root-owned or owned by the existing ${RUN_USER} account: ${target}" >&2
      return 1
    fi
  fi
  if [[ -e "${marker}" || -L "${marker}" ]]; then
    if [[ ! -f "${marker}" || -L "${marker}" || "$(stat -c '%u' "${marker}")" != 0 ]]; then
      echo "Managed-directory marker must be a root-owned regular file: ${marker}" >&2
      return 1
    fi
    if [[ "$(<"${marker}")" != $'role='"${role}"$'\npath='"${target}" ]]; then
      echo "Managed-directory marker does not match ${role} path ${target}." >&2
      return 1
    fi
    return 0
  fi

  if [[ "${role}" == install ]]; then
    if [[ -f "${target}/traffic-go" && ! -L "${target}/traffic-go" && -f "${target}/config.yaml" && ! -L "${target}/config.yaml" ]]; then
      return 0
    fi
    if [[ "${target}" == "${SCRIPT_DIR}" && -f "${CHECKSUM_FILE}" && ! -L "${CHECKSUM_FILE}" ]]; then
      return 0
    fi
  else
    local entry base
    local entries_found=0
    shopt -s nullglob dotglob
    for entry in "${target}"/*; do
      entries_found=1
      base="${entry##*/}"
      case "${base}" in
        traffic.db|traffic.db-wal|traffic.db-shm) ;;
        *)
          shopt -u nullglob dotglob
          echo "Existing unmarked state directory contains an unmanaged entry: ${entry}" >&2
          return 1
          ;;
      esac
    done
    shopt -u nullglob dotglob
    [[ "${entries_found}" -eq 0 ]] && return 0
    return 0
  fi

  echo "Existing ${role} directory is not recognizably managed by traffic-go: ${target}" >&2
  echo "Use an empty dedicated traffic-go directory or restore its root-owned management marker." >&2
  return 1
}

record_path_metadata "${INSTALL_ROOT}" install-root
record_path_metadata "${STATE_DIR}" state-dir
validate_existing_managed_dir "${INSTALL_ROOT}" install
validate_existing_managed_dir "${STATE_DIR}" state
for managed_parent in "${INSTALL_ROOT%/*}" "${STATE_DIR%/*}"; do
  if [[ ! -d "${managed_parent}" || -L "${managed_parent}" ]]; then
    echo "Managed-directory parent must already exist as a real directory: ${managed_parent}" >&2
    exit 1
  fi
done
INSTALL_BIN="${INSTALL_ROOT}/traffic-go"
INSTALL_CONFIG="${INSTALL_ROOT}/config.yaml"
LAYOUT_FILE="${INSTALL_ROOT}/.traffic-go-state-dir"
UNINSTALL_DEST="${INSTALL_ROOT}/uninstall-centos7.sh"
INSTALL_MARKER="${INSTALL_ROOT}/.traffic-go-managed-install"
STATE_MARKER="${STATE_DIR}/.traffic-go-managed-state"

resolve_nologin_shell() {
  local candidate
  for candidate in /usr/sbin/nologin /sbin/nologin /bin/false; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  printf '%s\n' /bin/false
}

if systemctl is-active --quiet traffic-go.service; then
  WAS_ACTIVE=1
fi
if systemctl is-enabled --quiet traffic-go.service; then
  WAS_ENABLED=1
fi
PREVIOUS_ACCT_VALUE="$(sysctl -n net.netfilter.nf_conntrack_acct 2>/dev/null || true)"

backup_file() {
  local source="$1"
  local name="$2"
  if [[ -e "${source}" || -L "${source}" ]]; then
    cp -a -- "${source}" "${ROLLBACK_DIR}/${name}"
    printf '%s\n' 1 >"${ROLLBACK_DIR}/${name}.existed"
  else
    printf '%s\n' 0 >"${ROLLBACK_DIR}/${name}.existed"
  fi
}

validate_managed_target_file() {
  local target="$1"
  local label="$2"
  if [[ -L "${target}" ]]; then
    echo "Existing managed ${label} must not be a symlink: ${target}" >&2
    return 1
  fi
  if [[ -e "${target}" && ! -f "${target}" ]]; then
    echo "Existing managed ${label} must be a regular file: ${target}" >&2
    return 1
  fi
}

restore_file() {
  local target="$1"
  local name="$2"
  if [[ -d "${target}" && ! -L "${target}" ]]; then
    echo "Rollback found a directory where managed file ${target} was expected; leaving it untouched for manual recovery." >&2
    return 1
  fi
  rm -f -- "${target}"
  if [[ "$(<"${ROLLBACK_DIR}/${name}.existed")" == 1 ]]; then
    cp -a -- "${ROLLBACK_DIR}/${name}" "${target}"
  fi
}

validate_file_backup() {
  local name="$1"
  local existed_file="${ROLLBACK_DIR}/${name}.existed"
  local existed
  if [[ ! -f "${existed_file}" || -L "${existed_file}" ]]; then
    echo "Rollback metadata is missing or unsafe for managed file backup: ${name}" >&2
    return 1
  fi
  existed="$(<"${existed_file}")"
  case "${existed}" in
    0) return 0 ;;
    1)
      if [[ ! -f "${ROLLBACK_DIR}/${name}" || -L "${ROLLBACK_DIR}/${name}" ]]; then
        echo "Rollback managed file backup is missing or unsafe: ${name}" >&2
        return 1
      fi
      ;;
    *)
      echo "Rollback metadata has an invalid existence flag for managed file backup: ${name}" >&2
      return 1
      ;;
  esac
}

restore_database_artifacts() {
  [[ "${DB_ARTIFACTS_RECORDED}" -eq 1 ]] || return 0
  local suffix artifact_name target existed_file existed failed=0
  for suffix in "" -wal -shm; do
    artifact_name="db-artifact${suffix:-main}"
    target="${EFFECTIVE_DB}${suffix}"
    if [[ -d "${target}" && ! -L "${target}" ]]; then
      echo "Rollback found a directory where SQLite file ${target} was expected; refusing partial database restore." >&2
      failed=1
    fi
    existed_file="${ROLLBACK_DIR}/${artifact_name}.existed"
    if [[ ! -f "${existed_file}" || -L "${existed_file}" ]]; then
      echo "Rollback database metadata is missing or unsafe: ${existed_file}" >&2
      failed=1
      continue
    fi
    existed="$(<"${existed_file}")"
    if [[ "${existed}" != 0 && "${existed}" != 1 ]]; then
      echo "Rollback database metadata has an invalid existence flag: ${existed_file}" >&2
      failed=1
    elif [[ "${existed}" == 1 ]] &&
         [[ ! -f "${DB_BACKUP_DIR}/${artifact_name}" || -L "${DB_BACKUP_DIR}/${artifact_name}" ]]; then
      echo "Rollback database snapshot is missing or unsafe: ${DB_BACKUP_DIR}/${artifact_name}" >&2
      failed=1
    fi
  done
  [[ "${failed}" -eq 0 ]] || return 1
  for suffix in "" -wal -shm; do
    artifact_name="db-artifact${suffix:-main}"
    target="${EFFECTIVE_DB}${suffix}"
    if [[ -d "${target}" && ! -L "${target}" ]]; then
      echo "Rollback found a directory where SQLite file ${target} was expected; leaving it for manual recovery." >&2
      failed=1
      continue
    fi
    if ! rm -f -- "${target}"; then
      echo "Rollback could not remove the changed SQLite artifact: ${target}" >&2
      failed=1
      continue
    fi
    if [[ "$(<"${ROLLBACK_DIR}/${artifact_name}.existed")" == 1 ]] &&
       ! cp -a -- "${DB_BACKUP_DIR}/${artifact_name}" "${target}"; then
      echo "Rollback could not restore the SQLite artifact: ${target}" >&2
      failed=1
    fi
  done
  return "${failed}"
}

restore_path_metadata() {
  local target="$1"
  local name="$2"
  local failed=0
  if [[ "$(<"${ROLLBACK_DIR}/${name}.dir-existed")" == 1 ]]; then
    local uid gid mode
    read -r uid gid mode <"${ROLLBACK_DIR}/${name}.dir-meta"
    if [[ ! -d "${target}" || -L "${target}" ]]; then
      echo "Rollback cannot restore directory metadata on an absent or symlink path: ${target}" >&2
      return 1
    fi
    chown "${uid}:${gid}" "${target}" >/dev/null 2>&1 || failed=1
    chmod "${mode}" "${target}" >/dev/null 2>&1 || failed=1
  else
    if [[ -e "${target}" || -L "${target}" ]]; then
      if [[ ! -d "${target}" || -L "${target}" ]] || ! rmdir "${target}" >/dev/null 2>&1; then
        echo "Rollback could not remove newly created managed directory: ${target}" >&2
        failed=1
      fi
    fi
  fi
  return "${failed}"
}

for pair in \
  "${INSTALL_BIN}|binary" \
  "${INSTALL_CONFIG}|config" \
  "${LAYOUT_FILE}|layout" \
  "${INSTALL_MARKER}|install-marker" \
  "${STATE_MARKER}|state-marker" \
  "${UNINSTALL_DEST}|uninstaller" \
  "${AUTHORITY_FILE}|authority" \
  "${SYSTEMD_UNIT}|unit" \
  "${MODULES_CONFIG}|modules" \
  "${SYSCTL_CONFIG}|sysctl"; do
  validate_managed_target_file "${pair%%|*}" "${pair##*|}"
  backup_file "${pair%%|*}" "${pair##*|}"
done
INITIAL_STATE_MARKER_EXISTED="$(<"${ROLLBACK_DIR}/state-marker.existed")"

ensure_service_stopped() {
  local active_state
  systemctl stop traffic-go.service >/dev/null 2>&1 || true
  active_state="$(systemctl is-active traffic-go.service 2>/dev/null || true)"
  case "${active_state}" in
    inactive|failed|unknown) return 0 ;;
    *)
      echo "traffic-go.service state is '${active_state:-unavailable}'; refusing to modify live executable or SQLite files without an explicit inactive result." >&2
      return 1
      ;;
  esac
}

rollback() {
  local status="$1"
  local rollback_failed=0
  trap - ERR
  echo "Installation failed; restoring the previous traffic-go files." >&2
  if ! ensure_service_stopped; then
    PRESERVE_ROLLBACK=1
    cat >&2 <<'EOF'
Automatic rollback was deliberately stopped because traffic-go.service could
not be confirmed inactive. Managed files may already be partially updated, so
the rollback snapshots are being preserved. Stop the unit manually, preserve
the current files, and restore the recorded version during a maintenance window.
EOF
    exit "${status}"
  fi
  if id -u "${RUN_USER}" >/dev/null 2>&1 && pgrep -u "${RUN_USER}" >/dev/null 2>&1; then
    echo "Rollback cannot safely continue while processes for ${RUN_USER} remain." >&2
    pgrep -a -u "${RUN_USER}" >&2 || true
    PRESERVE_ROLLBACK=1
    exit "${status}"
  fi
  if [[ -d "${STATE_DIR}" && ! -L "${STATE_DIR}" ]]; then
    if ! chown root:root "${STATE_DIR}" || ! chmod 0700 "${STATE_DIR}"; then
      PRESERVE_ROLLBACK=1
      exit "${status}"
    fi
  fi
  set +e
  restore_database_artifacts || rollback_failed=1
  if [[ "${MIGRATED_LEGACY_DB}" -eq 1 ]]; then
    local suffix
    for suffix in "" -wal -shm; do
      if [[ -e "${STATE_DIR}/traffic.db${suffix}" && ! -e "${INSTALL_ROOT}/traffic.db${suffix}" ]]; then
        mv -- "${STATE_DIR}/traffic.db${suffix}" "${INSTALL_ROOT}/traffic.db${suffix}" || rollback_failed=1
      elif [[ -e "${STATE_DIR}/traffic.db${suffix}" && -e "${INSTALL_ROOT}/traffic.db${suffix}" ]]; then
        echo "Rollback found both migrated and legacy SQLite artifacts for suffix '${suffix}'; manual reconciliation is required." >&2
        rollback_failed=1
      fi
    done
  fi
  if [[ "${MANAGED_FILES_MUTATED}" -eq 1 ]]; then
    local -a restore_pairs=(
      "${INSTALL_BIN}|binary"
      "${INSTALL_CONFIG}|config"
      "${LAYOUT_FILE}|layout"
      "${INSTALL_MARKER}|install-marker"
      "${STATE_MARKER}|${STATE_MARKER_BACKUP_NAME}"
      "${UNINSTALL_DEST}|uninstaller"
      "${AUTHORITY_FILE}|authority"
      "${SYSTEMD_UNIT}|unit"
      "${MODULES_CONFIG}|modules"
      "${SYSCTL_CONFIG}|sysctl"
    )
    local managed_backups_safe=1 restore_pair
    for restore_pair in "${restore_pairs[@]}"; do
      validate_file_backup "${restore_pair##*|}" || managed_backups_safe=0
    done
    if [[ "${managed_backups_safe}" -eq 1 ]]; then
      for restore_pair in "${restore_pairs[@]}"; do
        restore_file "${restore_pair%%|*}" "${restore_pair##*|}" || rollback_failed=1
      done
    else
      echo "Managed file rollback was not started because one or more backups failed preflight." >&2
      rollback_failed=1
    fi
  fi
  if [[ -n "${CONFIG_BACKUP}" ]] && ! rm -f -- "${CONFIG_BACKUP}"; then
    rollback_failed=1
  fi
  if [[ -n "${MIGRATION_BACKUP}" ]] && ! rm -f -- "${MIGRATION_BACKUP}"; then
    rollback_failed=1
  fi
  if [[ -n "${PREVIOUS_ACCT_VALUE}" ]]; then
    sysctl -w "net.netfilter.nf_conntrack_acct=${PREVIOUS_ACCT_VALUE}" >/dev/null 2>&1 || rollback_failed=1
  fi
  consume_all_temporary_files || rollback_failed=1
  restore_path_metadata "${STATE_DIR}" state-dir || rollback_failed=1
  restore_path_metadata "${INSTALL_ROOT}" install-root || rollback_failed=1
  if [[ "${CREATED_RUN_USER}" -eq 1 ]]; then
    local created_uid owned_path=""
    created_uid="$(id -u "${RUN_USER}" 2>/dev/null || true)"
    if [[ -n "${created_uid}" && -d "${STATE_DIR}" ]]; then
      owned_path="$(find "${STATE_DIR}" -xdev -uid "${created_uid}" -print -quit 2>/dev/null || true)"
    fi
    if [[ -z "${owned_path}" ]]; then
      userdel "${RUN_USER}" >/dev/null 2>&1 || rollback_failed=1
    else
      echo "Retaining newly created ${RUN_USER} account because rollback found an owned path: ${owned_path}" >&2
      rollback_failed=1
    fi
  fi
  if [[ "${CREATED_RUN_GROUP}" -eq 1 ]] && ! id -u "${RUN_USER}" >/dev/null 2>&1; then
    groupdel "${RUN_GROUP}" >/dev/null 2>&1 || rollback_failed=1
  fi
  systemctl daemon-reload >/dev/null 2>&1 || rollback_failed=1
  if [[ "${WAS_ENABLED}" -eq 1 ]]; then
    systemctl enable traffic-go.service >/dev/null 2>&1 || rollback_failed=1
  else
    systemctl disable traffic-go.service >/dev/null 2>&1 || rollback_failed=1
  fi
  if [[ "${WAS_ACTIVE}" -eq 1 && "${rollback_failed}" -eq 0 ]]; then
    systemctl restart traffic-go.service >/dev/null 2>&1 || rollback_failed=1
  elif [[ "${WAS_ACTIVE}" -eq 1 ]]; then
    echo "Rollback was incomplete; leaving traffic-go.service stopped to protect the database." >&2
  fi
  if [[ "${rollback_failed}" -ne 0 ]]; then
    PRESERVE_ROLLBACK=1
    echo "Rollback attempted every managed component, but one or more items require manual recovery." >&2
  fi
  exit "${status}"
}

on_error() {
  local status=$?
  if [[ "${INSTALL_SUCCEEDED}" -ne 1 ]]; then
    rollback "${status}"
  fi
  exit "${status}"
}
trap on_error ERR

# Confirm the old process is stopped before the first managed filesystem
# mutation. An unavailable systemd state is not treated as inactive.
ensure_service_stopped
if id -u "${RUN_USER}" >/dev/null 2>&1; then
  if pgrep -u "${RUN_USER}" >/dev/null 2>&1; then
    echo "Processes for the dedicated ${RUN_USER} account remain after stopping the unit; refusing a filesystem transaction." >&2
    pgrep -a -u "${RUN_USER}" >&2 || true
    false
  fi
fi

[[ -d "${INSTALL_ROOT}" ]] || mkdir "${INSTALL_ROOT}"
[[ -d "${STATE_DIR}" ]] || mkdir "${STATE_DIR}"
if [[ "$(cd "${INSTALL_ROOT}" && pwd -P)" != "${INSTALL_ROOT}" || "$(cd "${STATE_DIR}" && pwd -P)" != "${STATE_DIR}" ]]; then
  echo "Managed directory changed while it was being created." >&2
  false
fi
if mountpoint -q "${INSTALL_ROOT}" || mountpoint -q "${STATE_DIR}"; then
  echo "Install and state directories must not themselves be filesystem mount points." >&2
  false
fi
# Freeze the service-writable namespace for the root transaction. The final
# ownership is restored immediately before systemd starts the new process.
chown root:root "${STATE_DIR}"
chmod 0700 "${STATE_DIR}"
validate_existing_managed_dir "${STATE_DIR}" state
validate_managed_target_file "${STATE_MARKER}" state-marker
if [[ "${INITIAL_STATE_MARKER_EXISTED}" == 1 && ! -f "${STATE_MARKER}" ]]; then
  echo "The managed state marker changed while stopping the service; refusing installation." >&2
  false
fi
backup_file "${STATE_MARKER}" state-marker-frozen
STATE_MARKER_BACKUP_NAME="state-marker-frozen"

if ! getent group "${RUN_GROUP}" >/dev/null 2>&1; then
  groupadd --system "${RUN_GROUP}"
  CREATED_RUN_GROUP=1
fi
if ! id -u "${RUN_USER}" >/dev/null 2>&1; then
  useradd \
    --system \
    --gid "${RUN_GROUP}" \
    --home-dir "${STATE_DIR}" \
    --shell "$(resolve_nologin_shell)" \
    --no-create-home \
    "${RUN_USER}"
  CREATED_RUN_USER=1
else
  EXPECTED_GID="$(getent group "${RUN_GROUP}" | awk -F: '{print $3}')"
  if [[ -z "${EXPECTED_GID}" || "$(id -g "${RUN_USER}")" != "${EXPECTED_GID}" ]]; then
    echo "Existing ${RUN_USER} account does not use the dedicated ${RUN_GROUP} primary group." >&2
    false
  fi
  EXISTING_SHELL="$(getent passwd "${RUN_USER}" | awk -F: '{print $7}')"
  case "${EXISTING_SHELL}" in
    /usr/sbin/nologin|/sbin/nologin|/bin/false) ;;
    *) echo "Existing ${RUN_USER} account must use a nologin shell, found ${EXISTING_SHELL}." >&2; false ;;
  esac
  for member_gid in $(id -G "${RUN_USER}"); do
    if [[ "${member_gid}" != "${EXPECTED_GID}" ]]; then
      echo "Existing ${RUN_USER} account has supplementary groups; remove them before installation." >&2
      false
    fi
  done
fi
if [[ "$(id -u "${RUN_USER}")" -eq 0 ]]; then
  echo "Refusing to use a service account with uid 0: ${RUN_USER}" >&2
  false
fi
chown root:"${RUN_GROUP}" "${INSTALL_ROOT}"
chmod 0750 "${INSTALL_ROOT}"
chown root:"${RUN_GROUP}" "${STATE_DIR}"
chmod 0750 "${STATE_DIR}"
if ! runuser -u "${RUN_USER}" -- test -x "${INSTALL_ROOT}" || ! runuser -u "${RUN_USER}" -- test -x "${STATE_DIR}"; then
  cat >&2 <<EOF
The ${RUN_USER} account cannot traverse the final install/state paths.
Move the release under /opt or /usr/local, or grant that account a precise
traverse ACL on every private parent directory, then rerun the installer.
EOF
  false
fi
INSTALL_MARKER_TMP="$(mktemp "${INSTALL_MARKER}.new.XXXXXXXX")"
TEMP_FILES+=("${INSTALL_MARKER_TMP}")
printf 'role=install\npath=%s\n' "${INSTALL_ROOT}" >"${INSTALL_MARKER_TMP}"
MANAGED_FILES_MUTATED=1
install -m 0600 -o root -g root "${INSTALL_MARKER_TMP}" "${INSTALL_MARKER}"
consume_temporary_file "${INSTALL_MARKER_TMP}"
STATE_MARKER_TMP="$(mktemp "${STATE_MARKER}.new.XXXXXXXX")"
TEMP_FILES+=("${STATE_MARKER_TMP}")
printf 'role=state\npath=%s\n' "${STATE_DIR}" >"${STATE_MARKER_TMP}"
install -m 0600 -o root -g root "${STATE_MARKER_TMP}" "${STATE_MARKER}"
consume_temporary_file "${STATE_MARKER_TMP}"

BINARY_TMP="$(mktemp "${INSTALL_BIN}.new.XXXXXXXX")"
TEMP_FILES+=("${BINARY_TMP}")
install -m 0755 -o root -g root "${BINARY_SRC}" "${BINARY_TMP}"
"${BINARY_TMP}" -version >/dev/null
mv -f -- "${BINARY_TMP}" "${INSTALL_BIN}"
consume_temporary_file "${BINARY_TMP}"

if [[ ! -f "${INSTALL_CONFIG}" ]]; then
  install -m 0640 -o root -g "${RUN_GROUP}" "${CONFIG_SRC}" "${INSTALL_CONFIG}"
elif [[ "${REPLACE_CONFIG}" == 1 && "${CONFIG_SRC}" != "${INSTALL_CONFIG}" ]]; then
  CONFIG_BACKUP="$(mktemp "${INSTALL_CONFIG}.bak.XXXXXXXX")"
  cp -a -- "${INSTALL_CONFIG}" "${CONFIG_BACKUP}"
  CONFIG_TMP="$(mktemp "${INSTALL_CONFIG}.new.XXXXXXXX")"
  TEMP_FILES+=("${CONFIG_TMP}")
  install -m 0640 -o root -g "${RUN_GROUP}" "${CONFIG_SRC}" "${CONFIG_TMP}"
  mv -f -- "${CONFIG_TMP}" "${INSTALL_CONFIG}"
  consume_temporary_file "${CONFIG_TMP}"
else
  echo "Preserving existing ${INSTALL_CONFIG}; set TRAFFIC_GO_REPLACE_CONFIG=1 to replace it."
  chown root:"${RUN_GROUP}" "${INSTALL_CONFIG}"
  chmod 0640 "${INSTALL_CONFIG}"
fi

LISTEN_ADDR="$("${INSTALL_BIN}" -config "${INSTALL_CONFIG}" -check-config)"
DB_PATH="$("${INSTALL_BIN}" -config "${INSTALL_CONFIG}" -print-db-path)"
if [[ "${DB_PATH}" == /* ]]; then
  EFFECTIVE_DB="$(readlink -m -- "${DB_PATH}")"
else
  EFFECTIVE_DB="$(readlink -m -- "${STATE_DIR}/${DB_PATH}")"
  case "${EFFECTIVE_DB}" in
    "${STATE_DIR}"/*) ;;
    *)
      echo "Relative db_path escapes the dedicated state directory: ${DB_PATH}" >&2
      false
      ;;
  esac
fi
LEGACY_DB="$(readlink -m -- "${INSTALL_ROOT}/traffic.db")"
DEFAULT_STATE_DB="$(readlink -m -- "${STATE_DIR}/traffic.db")"

db_artifacts_present() {
  local base="$1"
  local suffix
  for suffix in "" -wal -shm; do
    if [[ -e "${base}${suffix}" || -L "${base}${suffix}" ]]; then
      return 0
    fi
  done
  return 1
}

print_db_artifacts() {
  local label="$1"
  local base="$2"
  local suffix
  echo "${label}:"
  for suffix in "" -wal -shm; do
    if [[ -e "${base}${suffix}" || -L "${base}${suffix}" ]]; then
      stat -c '  %n size=%s mtime=%y owner=%u:%g mode=%a' "${base}${suffix}" 2>/dev/null || echo "  ${base}${suffix} (unreadable metadata)"
    fi
  done
}

if db_artifacts_present "${LEGACY_DB}" && db_artifacts_present "${DEFAULT_STATE_DB}"; then
  echo "Both legacy and state-directory SQLite files exist; refusing to select one silently." >&2
  print_db_artifacts "Legacy database" "${LEGACY_DB}" >&2
  print_db_artifacts "State database" "${DEFAULT_STATE_DB}" >&2
  echo "Back up both sets, then explicitly reconcile or remove the obsolete copy before retrying." >&2
  false
fi
if db_artifacts_present "${LEGACY_DB}" && [[ ! -f "${LEGACY_DB}" ]]; then
  echo "Legacy SQLite sidecars exist without a main database: ${LEGACY_DB}" >&2
  false
fi
if db_artifacts_present "${DEFAULT_STATE_DB}" && [[ ! -f "${DEFAULT_STATE_DB}" ]]; then
  echo "State SQLite sidecars exist without a main database: ${DEFAULT_STATE_DB}" >&2
  false
fi

if [[ "${EFFECTIVE_DB}" == "${LEGACY_DB}" ]]; then
  echo "Migrating legacy db_path from the executable directory to ${DEFAULT_STATE_DB}."
  MIGRATION_BACKUP="$(mktemp "${INSTALL_CONFIG}.bak.migration.XXXXXXXX")"
  cp -a -- "${INSTALL_CONFIG}" "${MIGRATION_BACKUP}"
  CONFIG_MIGRATION_TMP="$(mktemp "${INSTALL_CONFIG}.migration.new.XXXXXXXX")"
  TEMP_FILES+=("${CONFIG_MIGRATION_TMP}")
  if ! awk -v state_db="${DEFAULT_STATE_DB}" '
    BEGIN { replaced = 0 }
    /^db_path:[[:space:]]*/ && !replaced {
      print "db_path: \"" state_db "\""
      replaced = 1
      next
    }
    { print }
    END { if (!replaced) exit 42 }
  ' "${INSTALL_CONFIG}" >"${CONFIG_MIGRATION_TMP}"; then
    rm -f -- "${CONFIG_MIGRATION_TMP}"
    echo "Could not safely update the legacy top-level db_path; set it to ${DEFAULT_STATE_DB} and rerun." >&2
    false
  fi
  chown root:"${RUN_GROUP}" "${CONFIG_MIGRATION_TMP}"
  chmod 0640 "${CONFIG_MIGRATION_TMP}"
  mv -f -- "${CONFIG_MIGRATION_TMP}" "${INSTALL_CONFIG}"
  consume_temporary_file "${CONFIG_MIGRATION_TMP}"
  "${INSTALL_BIN}" -config "${INSTALL_CONFIG}" -check-config >/dev/null
  DB_PATH="${DEFAULT_STATE_DB}"
  EFFECTIVE_DB="${DEFAULT_STATE_DB}"
fi

case "${EFFECTIVE_DB}" in
  "${STATE_DIR}"/*) ;;
  *)
    echo "The system installer requires db_path to stay inside the dedicated state directory: ${EFFECTIVE_DB}" >&2
    echo "Set db_path below ${STATE_DIR}; customize that root with TRAFFIC_GO_STATE_DIR when needed." >&2
    false
    ;;
esac
if [[ "${EFFECTIVE_DB%/*}" != "${STATE_DIR}" ]]; then
  echo "The system installer requires db_path to be a direct file below ${STATE_DIR}: ${EFFECTIVE_DB}" >&2
  false
fi
if [[ -n "${REGISTERED_DB_PATH}" && "${REGISTERED_DB_PATH}" != "${EFFECTIVE_DB}" && "${ALLOW_LAYOUT_TAKEOVER}" != 1 ]]; then
  echo "The registered database path differs from the candidate layout; refusing silent data-source replacement." >&2
  echo "Registered db=${REGISTERED_DB_PATH}; candidate db=${EFFECTIVE_DB}" >&2
  echo "Set TRAFFIC_GO_ALLOW_LAYOUT_TAKEOVER=1 only for a reviewed migration." >&2
  false
fi

if [[ "${EFFECTIVE_DB}" == "${DEFAULT_STATE_DB}" ]] && db_artifacts_present "${LEGACY_DB}" && ! db_artifacts_present "${DEFAULT_STATE_DB}"; then
  MIGRATED_LEGACY_DB=1
  for suffix in "" -wal -shm; do
    if [[ -e "${LEGACY_DB}${suffix}" ]]; then
      mv -- "${LEGACY_DB}${suffix}" "${DEFAULT_STATE_DB}${suffix}"
    fi
  done
fi

RUN_UID="$(id -u "${RUN_USER}")"
for sqlite_file in "${EFFECTIVE_DB}" "${EFFECTIVE_DB}-wal" "${EFFECTIVE_DB}-shm"; do
  if [[ -e "${sqlite_file}" || -L "${sqlite_file}" ]]; then
    if [[ ! -f "${sqlite_file}" || -L "${sqlite_file}" ]]; then
      echo "SQLite path must be a regular, non-symlink file: ${sqlite_file}" >&2
      false
    fi
    if [[ "$(stat -c '%u' "${sqlite_file}")" != "${RUN_UID}" ]]; then
      echo "SQLite file must be owned by ${RUN_USER} before installation: ${sqlite_file}" >&2
      false
    fi
  fi
done

DB_BACKUP_DIR="$(mktemp -d "${STATE_DIR%/*}/.traffic-go-db-rollback.XXXXXXXX")"
chown root:root "${DB_BACKUP_DIR}"
chmod 0700 "${DB_BACKUP_DIR}"
for suffix in "" -wal -shm; do
  artifact_name="db-artifact${suffix:-main}"
  if [[ -e "${EFFECTIVE_DB}${suffix}" || -L "${EFFECTIVE_DB}${suffix}" ]]; then
    printf '%s\n' 1 >"${ROLLBACK_DIR}/${artifact_name}.existed"
    cp -a --reflink=auto -- "${EFFECTIVE_DB}${suffix}" "${DB_BACKUP_DIR}/${artifact_name}"
  else
    printf '%s\n' 0 >"${ROLLBACK_DIR}/${artifact_name}.existed"
  fi
done
DB_ARTIFACTS_RECORDED=1

LAYOUT_TMP="$(mktemp "${LAYOUT_FILE}.new.XXXXXXXX")"
TEMP_FILES+=("${LAYOUT_TMP}")
printf 'state_dir=%s\ndb_path=%s\n' "${STATE_DIR}" "${EFFECTIVE_DB}" >"${LAYOUT_TMP}"
install -m 0600 -o root -g root "${LAYOUT_TMP}" "${LAYOUT_FILE}"
consume_temporary_file "${LAYOUT_TMP}"
AUTHORITY_TMP="$(mktemp "${AUTHORITY_FILE}.new.XXXXXXXX")"
TEMP_FILES+=("${AUTHORITY_TMP}")
printf 'install_root=%s\nstate_dir=%s\ndb_path=%s\n' "${INSTALL_ROOT}" "${STATE_DIR}" "${EFFECTIVE_DB}" >"${AUTHORITY_TMP}"
install -m 0600 -o root -g root "${AUTHORITY_TMP}" "${AUTHORITY_FILE}"
consume_temporary_file "${AUTHORITY_TMP}"

if [[ -f "${UNINSTALL_SRC}" && ! -L "${UNINSTALL_SRC}" ]]; then
  if [[ "$(readlink -f -- "${UNINSTALL_SRC}")" != "$(readlink -f -- "${UNINSTALL_DEST}" 2>/dev/null || true)" ]]; then
    UNINSTALL_TMP="$(mktemp "${UNINSTALL_DEST}.new.XXXXXXXX")"
    TEMP_FILES+=("${UNINSTALL_TMP}")
    install -m 0750 -o root -g "${RUN_GROUP}" "${UNINSTALL_SRC}" "${UNINSTALL_TMP}"
    mv -f -- "${UNINSTALL_TMP}" "${UNINSTALL_DEST}"
    consume_temporary_file "${UNINSTALL_TMP}"
  else
    chown root:"${RUN_GROUP}" "${UNINSTALL_DEST}"
    chmod 0750 "${UNINSTALL_DEST}"
  fi
fi

MODULES_TMP="$(mktemp "${MODULES_CONFIG}.new.XXXXXXXX")"
TEMP_FILES+=("${MODULES_TMP}")
printf '%s\n' nf_conntrack >"${MODULES_TMP}"
install -m 0644 -o root -g root "${MODULES_TMP}" "${MODULES_CONFIG}"
consume_temporary_file "${MODULES_TMP}"
SYSCTL_TMP="$(mktemp "${SYSCTL_CONFIG}.new.XXXXXXXX")"
TEMP_FILES+=("${SYSCTL_TMP}")
printf '%s\n' 'net.netfilter.nf_conntrack_acct = 1' >"${SYSCTL_TMP}"
install -m 0644 -o root -g root "${SYSCTL_TMP}" "${SYSCTL_CONFIG}"
consume_temporary_file "${SYSCTL_TMP}"

modprobe nf_conntrack || {
  if [[ ! -e /proc/sys/net/netfilter/nf_conntrack_acct ]]; then
    echo "Unable to load nf_conntrack and accounting sysctl is unavailable." >&2
    false
  fi
}
sysctl -p "${SYSCTL_CONFIG}" >/dev/null
if [[ "$(sysctl -n net.netfilter.nf_conntrack_acct)" != 1 ]]; then
  echo "net.netfilter.nf_conntrack_acct did not remain enabled." >&2
  false
fi

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[\\/&|]/\\&/g'
}

VERIFY_DIR="${ROLLBACK_DIR}/verify"
mkdir -p "${VERIFY_DIR}"
sed \
  -e "s|__RUN_USER__|$(escape_sed_replacement "${RUN_USER}")|g" \
  -e "s|__RUN_GROUP__|$(escape_sed_replacement "${RUN_GROUP}")|g" \
  -e "s|__STATE_DIR__|$(escape_sed_replacement "${STATE_DIR}")|g" \
  -e "s|__INSTALL_BIN__|$(escape_sed_replacement "${INSTALL_BIN}")|g" \
  -e "s|__INSTALL_CONFIG__|$(escape_sed_replacement "${INSTALL_CONFIG}")|g" \
  "${SERVICE_TEMPLATE}" >"${VERIFY_DIR}/traffic-go.service"

if ! systemd-analyze verify "${VERIFY_DIR}/traffic-go.service"; then
  cat >&2 <<'EOF'
The local systemd does not accept the hardened traffic-go unit. In particular,
older unpatched systemd 219 builds lack AmbientCapabilities support and cannot
safely grant the collector's narrow capabilities to a non-root service. Update
systemd or use a supported distribution; this installer will not fall back to
running the HTTP service as root.
EOF
  false
fi
install -m 0644 -o root -g root "${VERIFY_DIR}/traffic-go.service" "${SYSTEMD_UNIT}"

case "${LISTEN_ADDR}" in
  0.0.0.0:*) HEALTH_ADDR="127.0.0.1:${LISTEN_ADDR##*:}" ;;
  \[::\]:*) HEALTH_ADDR="[::1]:${LISTEN_ADDR##*:}" ;;
  :*) HEALTH_ADDR="127.0.0.1:${LISTEN_ADDR##*:}" ;;
  *) HEALTH_ADDR="${LISTEN_ADDR}" ;;
esac
HEALTH_URL="${TRAFFIC_GO_HEALTH_URL:-http://${HEALTH_ADDR}/api/v1/readyz}"
case "${HEALTH_URL}" in
  http://*|https://*) ;;
  *) echo "TRAFFIC_GO_HEALTH_URL must use http:// or https://" >&2; false ;;
esac

systemctl daemon-reload
systemctl enable traffic-go.service >/dev/null
if ! consume_all_temporary_files; then
  echo "Temporary install files remain; refusing to expose the state namespace." >&2
  false
fi
if pgrep -u "${RUN_USER}" >/dev/null 2>&1; then
  echo "A process for ${RUN_USER} appeared during the frozen install transaction; refusing to expose the state namespace." >&2
  false
fi
chown "${RUN_USER}:${RUN_GROUP}" "${STATE_DIR}"
chmod 0700 "${STATE_DIR}"
systemctl restart traffic-go.service

READY=0
for _ in $(seq 1 30); do
  if curl --fail --silent --show-error --max-time 2 "${HEALTH_URL}" >/dev/null; then
    READY=1
    break
  fi
  sleep 1
done
if [[ "${READY}" -ne 1 ]]; then
  echo "Readiness check failed: ${HEALTH_URL}" >&2
  journalctl -u traffic-go.service -n 100 --no-pager >&2 || true
  false
fi

INSTALL_SUCCEEDED=1
trap - ERR
echo
echo "traffic-go installed and ready."
echo "Install root: ${INSTALL_ROOT}"
echo "State directory: ${STATE_DIR}"
echo "Binary: ${INSTALL_BIN}"
echo "Config: ${INSTALL_CONFIG}"
echo "Runtime user: ${RUN_USER}"
echo "Readiness: ${HEALTH_URL}"
"${INSTALL_BIN}" -version
