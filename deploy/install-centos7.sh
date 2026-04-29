#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BINARY_SRC="${1:-${SCRIPT_DIR}/traffic-go}"
CONFIG_SRC="${2:-${SCRIPT_DIR}/config.yaml}"
SERVICE_TEMPLATE="${3:-${SCRIPT_DIR}/traffic-go.service}"

RUN_USER="traffic-go"
RUN_GROUP="traffic-go"
INSTALL_ROOT="${SCRIPT_DIR}"
INSTALL_BIN="${INSTALL_ROOT}/traffic-go"
INSTALL_CONFIG="${INSTALL_ROOT}/config.yaml"
INSTALL_DB="${INSTALL_ROOT}/traffic.db"
SYSTEMD_UNIT="/etc/systemd/system/traffic-go.service"
RUNTIME_NOTE=""

if [[ ! -f "${BINARY_SRC}" ]]; then
  echo "Binary not found: ${BINARY_SRC}" >&2
  exit 1
fi

if [[ ! -f "${CONFIG_SRC}" ]]; then
  echo "Config not found: ${CONFIG_SRC}" >&2
  exit 1
fi

if [[ ! -f "${SERVICE_TEMPLATE}" ]]; then
  echo "Service template not found: ${SERVICE_TEMPLATE}" >&2
  exit 1
fi

resolve_nologin_shell() {
  local candidate
  for candidate in /usr/sbin/nologin /sbin/nologin /bin/false; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  printf '%s\n' "/bin/false"
}

resolve_existing_path() {
  local target="$1"
  local dir
  dir="$(cd "$(dirname "${target}")" && pwd -P)"
  printf '%s/%s\n' "${dir}" "$(basename "${target}")"
}

same_existing_path() {
  local left="$1"
  local right="$2"
  [[ -e "${left}" && -e "${right}" ]] || return 1
  [[ "$(resolve_existing_path "${left}")" == "$(resolve_existing_path "${right}")" ]]
}

resolve_command() {
  local name="$1"
  local fallback="$2"
  if command -v "${name}" >/dev/null 2>&1; then
    command -v "${name}"
    return 0
  fi
  printf '%s\n' "${fallback}"
}

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[\/&]/\\&/g'
}

parent_dirs_publicly_traversable() {
  local current mode
  current="$(dirname "${INSTALL_ROOT}")"

  while [[ "${current}" != "/" ]]; do
    mode="$(stat -c '%a' "${current}" 2>/dev/null || true)"
    if [[ -z "${mode}" ]]; then
      return 1
    fi
    if (( (8#${mode} & 1) == 0 )); then
      return 1
    fi
    current="$(dirname "${current}")"
  done

  return 0
}

select_runtime_identity() {
  if parent_dirs_publicly_traversable; then
    return 0
  fi

  RUN_USER="root"
  RUN_GROUP="root"
  RUNTIME_NOTE="Install root is under a private parent path; service will run as root so systemd can execute ${INSTALL_BIN}."
}

ensure_service_account() {
  if [[ "${RUN_USER}" == "root" && "${RUN_GROUP}" == "root" ]]; then
    return 0
  fi

  if ! getent group "${RUN_GROUP}" >/dev/null 2>&1; then
    groupadd --system "${RUN_GROUP}"
  fi

  if ! id -u "${RUN_USER}" >/dev/null 2>&1; then
    useradd \
      --system \
      --gid "${RUN_GROUP}" \
      --home-dir "${INSTALL_ROOT}" \
      --shell "$(resolve_nologin_shell)" \
      --no-create-home \
      "${RUN_USER}"
  fi
}

ensure_install_root() {
  mkdir -p "${INSTALL_ROOT}"
  chown root:"${RUN_GROUP}" "${INSTALL_ROOT}"
  chmod 1770 "${INSTALL_ROOT}"
}

install_binary() {
  if same_existing_path "${BINARY_SRC}" "${INSTALL_BIN}"; then
    chown root:root "${INSTALL_BIN}"
    chmod 0755 "${INSTALL_BIN}"
    return 0
  fi
  install -m 0755 "${BINARY_SRC}" "${INSTALL_BIN}"
}

install_config() {
  if [[ -f "${INSTALL_CONFIG}" ]]; then
    cp "${INSTALL_CONFIG}" "${INSTALL_CONFIG}.bak.$(date +%Y%m%d%H%M%S)"
  fi

  if same_existing_path "${CONFIG_SRC}" "${INSTALL_CONFIG}"; then
    chown root:"${RUN_GROUP}" "${INSTALL_CONFIG}"
    chmod 0640 "${INSTALL_CONFIG}"
    return 0
  fi

  install -m 0640 -o root -g "${RUN_GROUP}" "${CONFIG_SRC}" "${INSTALL_CONFIG}"
}

has_any_process_log_settings() {
  awk '
    BEGIN { in_block = 0; found = 0 }
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*process_log_dirs:[[:space:]]*$/ { in_block = 1; next }
    in_block {
      if ($0 ~ /^[^[:space:]]/) { in_block = 0; next }
      if ($0 ~ /^[[:space:]]+[A-Za-z0-9_.-]+:[[:space:]]*[^#[:space:]]/) { found = 1; exit }
    }
    END { exit(found ? 0 : 1) }
  ' "${INSTALL_CONFIG}"
}

config_has_process_log_key() {
  local key="$1"
  awk -v target="${key}" '
    BEGIN { in_block = 0; found = 0 }
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*process_log_dirs:[[:space:]]*$/ { in_block = 1; next }
    in_block {
      if ($0 ~ /^[^[:space:]]/) { in_block = 0; next }
      if ($0 ~ ("^[[:space:]]+" target ":[[:space:]]*[^#[:space:]]")) { found = 1; exit }
    }
    END { exit(found ? 0 : 1) }
  ' "${INSTALL_CONFIG}"
}

append_default_process_log_dirs() {
  if grep -Eq '^[[:space:]]*process_log_dirs:[[:space:]]*$' "${INSTALL_CONFIG}"; then
    local tmp_config
    tmp_config="$(mktemp "${INSTALL_CONFIG}.XXXXXX")"
    awk '
      BEGIN { inserted = 0 }
      {
        print
        if (!inserted && $0 ~ /^[[:space:]]*process_log_dirs:[[:space:]]*$/) {
          print "  nginx: /var/log/nginx"
          print "  ss-server: /var/log/shadowsocks"
          print "  ss-manager: /var/log/shadowsocks"
          print "  obfs-server: /var/log/shadowsocks"
          inserted = 1
        }
      }
    ' "${INSTALL_CONFIG}" >"${tmp_config}"
    mv "${tmp_config}" "${INSTALL_CONFIG}"
    chown root:"${RUN_GROUP}" "${INSTALL_CONFIG}"
    chmod 0640 "${INSTALL_CONFIG}"
    return 0
  fi

  cat >>"${INSTALL_CONFIG}" <<'EOF'

process_log_dirs:
  nginx: /var/log/nginx
  ss-server: /var/log/shadowsocks
  ss-manager: /var/log/shadowsocks
  obfs-server: /var/log/shadowsocks
EOF
  chown root:"${RUN_GROUP}" "${INSTALL_CONFIG}"
  chmod 0640 "${INSTALL_CONFIG}"
}

ensure_db_path() {
  if grep -q '^db_path:' "${INSTALL_CONFIG}"; then
    sed -i "s|^db_path:.*$|db_path: ${INSTALL_DB}|" "${INSTALL_CONFIG}"
  else
    printf '\ndb_path: %s\n' "${INSTALL_DB}" >>"${INSTALL_CONFIG}"
  fi
}

configured_listen() {
  local listen_value
  listen_value="$(awk '
    /^[[:space:]]*listen:/ {
      sub(/^[[:space:]]*listen:[[:space:]]*/, "", $0)
      gsub(/"/, "", $0)
      gsub(/\047/, "", $0)
      print $0
      exit
    }
  ' "${INSTALL_CONFIG}" 2>/dev/null || true)"
  if [[ -n "${listen_value}" ]]; then
    printf '%s\n' "${listen_value}"
    return 0
  fi
  printf '%s\n' '127.0.0.1:8080'
}

best_effort_kernel_prereqs() {
  local modprobe_bin sysctl_bin
  modprobe_bin="$(resolve_command modprobe /usr/sbin/modprobe)"
  sysctl_bin="$(resolve_command sysctl /usr/sbin/sysctl)"

  "${modprobe_bin}" nf_conntrack >/dev/null 2>&1 || true
  "${sysctl_bin}" -w net.netfilter.nf_conntrack_acct=1 >/dev/null 2>&1 || true
}

render_service_unit() {
  local modprobe_bin sysctl_bin tmp_unit
  modprobe_bin="$(resolve_command modprobe /usr/sbin/modprobe)"
  sysctl_bin="$(resolve_command sysctl /usr/sbin/sysctl)"
  tmp_unit="$(mktemp "${SYSTEMD_UNIT}.XXXXXX")"

  sed \
    -e "s|__RUN_USER__|$(escape_sed_replacement "${RUN_USER}")|g" \
    -e "s|__RUN_GROUP__|$(escape_sed_replacement "${RUN_GROUP}")|g" \
    -e "s|__INSTALL_ROOT__|$(escape_sed_replacement "${INSTALL_ROOT}")|g" \
    -e "s|__INSTALL_BIN__|$(escape_sed_replacement "${INSTALL_BIN}")|g" \
    -e "s|__INSTALL_CONFIG__|$(escape_sed_replacement "${INSTALL_CONFIG}")|g" \
    -e "s|__MODPROBE_BIN__|$(escape_sed_replacement "${modprobe_bin}")|g" \
    -e "s|__SYSCTL_BIN__|$(escape_sed_replacement "${sysctl_bin}")|g" \
    "${SERVICE_TEMPLATE}" >"${tmp_unit}"

  install -m 0644 "${tmp_unit}" "${SYSTEMD_UNIT}"
  rm -f "${tmp_unit}"
}

ensure_database_file() {
  if [[ ! -f "${INSTALL_DB}" ]]; then
    : >"${INSTALL_DB}"
  fi
  chown "${RUN_USER}:${RUN_GROUP}" "${INSTALL_DB}"
  chmod 0660 "${INSTALL_DB}"
}

select_runtime_identity
ensure_service_account
ensure_install_root
install_binary
install_config
ensure_db_path
ensure_database_file
best_effort_kernel_prereqs
render_service_unit

if ! has_any_process_log_settings; then
  echo "No process_log_dirs found in ${INSTALL_CONFIG}."
  echo "Appending default nginx + shadowsocks log paths so usage/explain can scan logs."
  append_default_process_log_dirs
fi

if ! config_has_process_log_key "nginx"; then
  echo "Warning: nginx log path is not configured in ${INSTALL_CONFIG}."
fi
if ! config_has_process_log_key "ss-server"; then
  echo "Warning: ss-server log path is not configured in ${INSTALL_CONFIG}."
fi

LISTEN_ADDR="$(configured_listen)"
LISTEN_HOST="${LISTEN_ADDR%:*}"
LISTEN_PORT="${LISTEN_ADDR##*:}"
if [[ "${LISTEN_HOST}" == "0.0.0.0" || "${LISTEN_HOST}" == "::" || "${LISTEN_HOST}" == "[::]" ]]; then
  QUICKCHECK_ADDR="127.0.0.1:${LISTEN_PORT}"
else
  QUICKCHECK_ADDR="${LISTEN_ADDR}"
fi

systemctl daemon-reload
systemctl enable --now traffic-go

echo
echo "traffic-go installed."
echo "Install root: ${INSTALL_ROOT}"
echo "Binary: ${INSTALL_BIN}"
echo "Config: ${INSTALL_CONFIG}"
echo "Database: ${INSTALL_DB}"
echo "User: ${RUN_USER}"
if [[ -n "${RUNTIME_NOTE}" ]]; then
  echo "Note: ${RUNTIME_NOTE}"
fi
echo
systemctl --no-pager --full status traffic-go || true
echo
echo "Quick checks:"
echo "  curl http://${QUICKCHECK_ADDR}/api/v1/healthz"
echo "  journalctl -u traffic-go -n 100 --no-pager"
