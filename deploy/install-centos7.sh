#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_SRC="${1:-${SCRIPT_DIR}/traffic-go}"
CONFIG_SRC="${2:-${SCRIPT_DIR}/config.yaml}"
SERVICE_TEMPLATE="${3:-${SCRIPT_DIR}/traffic-go.service}"

RUN_USER="traffic-go"
RUN_GROUP="traffic-go"
INSTALL_BIN="/usr/local/bin/traffic-go"
INSTALL_CONFIG_DIR="/etc/traffic-go"
INSTALL_CONFIG="${INSTALL_CONFIG_DIR}/config.yaml"
INSTALL_STATE_DIR="/var/lib/traffic-go"
INSTALL_DB="${INSTALL_STATE_DIR}/traffic.db"
SYSTEMD_UNIT="/etc/systemd/system/traffic-go.service"
MODULES_LOAD_CONF="/etc/modules-load.d/nf_conntrack.conf"
SYSCTL_CONF="/etc/sysctl.d/90-conntrack-acct.conf"

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

ensure_service_account() {
  if ! getent group "${RUN_GROUP}" >/dev/null 2>&1; then
    groupadd --system "${RUN_GROUP}"
  fi

  if ! id -u "${RUN_USER}" >/dev/null 2>&1; then
    useradd \
      --system \
      --gid "${RUN_GROUP}" \
      --home-dir "${INSTALL_STATE_DIR}" \
      --shell "$(resolve_nologin_shell)" \
      --create-home \
      "${RUN_USER}"
  fi
}

has_any_process_log_settings() {
  awk '
    BEGIN { in_block = 0; found = 0 }
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*(nginx_log_dir|ss_log_dir):[[:space:]]*[^#[:space:]]/ { found = 1; exit }
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
    $0 ~ ("^[[:space:]]*" target ":[[:space:]]*[^#[:space:]]") { found = 1; exit }
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
    return 0
  fi

  cat >>"${INSTALL_CONFIG}" <<'EOF'

process_log_dirs:
  nginx: /var/log/nginx
  ss-server: /var/log/shadowsocks
  ss-manager: /var/log/shadowsocks
  obfs-server: /var/log/shadowsocks
EOF
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

ensure_service_account
mkdir -p /usr/local/bin "${INSTALL_CONFIG_DIR}" "${INSTALL_STATE_DIR}" /etc/modules-load.d /etc/sysctl.d
chown "${RUN_USER}:${RUN_GROUP}" "${INSTALL_STATE_DIR}"
chmod 0750 "${INSTALL_STATE_DIR}"

if command -v modprobe >/dev/null 2>&1; then
  modprobe nf_conntrack || true
fi
printf 'nf_conntrack\n' >"${MODULES_LOAD_CONF}"

if command -v sysctl >/dev/null 2>&1; then
  sysctl -w net.netfilter.nf_conntrack_acct=1 || true
fi
printf 'net.netfilter.nf_conntrack_acct = 1\n' >"${SYSCTL_CONF}"

if [[ -f "${INSTALL_CONFIG}" ]]; then
  cp "${INSTALL_CONFIG}" "${INSTALL_CONFIG}.bak.$(date +%Y%m%d%H%M%S)"
fi

install -m 0755 "${BINARY_SRC}" "${INSTALL_BIN}"
install -m 0640 -o root -g "${RUN_GROUP}" "${CONFIG_SRC}" "${INSTALL_CONFIG}"
install -m 0644 "${SERVICE_TEMPLATE}" "${SYSTEMD_UNIT}"

if grep -q '^db_path:' "${INSTALL_CONFIG}"; then
  sed -i "s|^db_path:.*$|db_path: ${INSTALL_DB}|" "${INSTALL_CONFIG}"
else
  printf '\ndb_path: %s\n' "${INSTALL_DB}" >>"${INSTALL_CONFIG}"
fi

if ! has_any_process_log_settings; then
  echo "No process_log_dirs/legacy log dirs found in ${INSTALL_CONFIG}."
  echo "Appending default nginx + shadowsocks log paths so usage/explain can scan logs."
  append_default_process_log_dirs
fi

if ! config_has_process_log_key "nginx" && ! config_has_process_log_key "nginx_log_dir"; then
  echo "Warning: nginx log path is not configured in ${INSTALL_CONFIG}."
fi
if ! config_has_process_log_key "ss-server" && ! config_has_process_log_key "ss_log_dir"; then
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
echo "Binary: ${INSTALL_BIN}"
echo "Config: ${INSTALL_CONFIG}"
echo "State dir: ${INSTALL_STATE_DIR}"
echo "User: ${RUN_USER}"
echo
systemctl --no-pager --full status traffic-go || true
echo
echo "Quick checks:"
echo "  curl http://${QUICKCHECK_ADDR}/api/v1/healthz"
echo "  journalctl -u traffic-go -n 100 --no-pager"
