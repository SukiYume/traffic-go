#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_SRC="${1:-${SCRIPT_DIR}/traffic-go}"
CONFIG_SRC="${2:-${SCRIPT_DIR}/config.yaml}"

INSTALL_ROOT="${INSTALL_ROOT:-/root/traffic-go-release}"
INSTALL_BIN="${INSTALL_ROOT}/traffic-go"
INSTALL_CONFIG="${INSTALL_ROOT}/config.yaml"
INSTALL_DB="${INSTALL_ROOT}/traffic.db"
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

mkdir -p "${INSTALL_ROOT}"
mkdir -p /etc/modules-load.d /etc/sysctl.d

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

if [[ "${BINARY_SRC}" != "${INSTALL_BIN}" ]]; then
  install -m 0755 "${BINARY_SRC}" "${INSTALL_BIN}"
else
  chmod 0755 "${INSTALL_BIN}"
fi

if [[ "${CONFIG_SRC}" != "${INSTALL_CONFIG}" ]]; then
  install -m 0644 "${CONFIG_SRC}" "${INSTALL_CONFIG}"
fi

if grep -q '^db_path:' "${INSTALL_CONFIG}"; then
  sed -i "s|^db_path:.*$|db_path: ${INSTALL_DB}|" "${INSTALL_CONFIG}"
else
  printf '\ndb_path: %s\n' "${INSTALL_DB}" >>"${INSTALL_CONFIG}"
fi

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

LISTEN_ADDR="$(configured_listen)"
LISTEN_HOST="${LISTEN_ADDR%:*}"
LISTEN_PORT="${LISTEN_ADDR##*:}"
if [[ "${LISTEN_HOST}" == "0.0.0.0" || "${LISTEN_HOST}" == "::" || "${LISTEN_HOST}" == "[::]" ]]; then
  QUICKCHECK_ADDR="127.0.0.1:${LISTEN_PORT}"
else
  QUICKCHECK_ADDR="${LISTEN_ADDR}"
fi

cat >"${SYSTEMD_UNIT}" <<EOF
[Unit]
Description=traffic-go network flow monitor
Documentation=man:systemd.service(5)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_ROOT}
ExecStart=${INSTALL_BIN} -config ${INSTALL_CONFIG}
Restart=on-failure
RestartSec=3
TimeoutStartSec=30
LimitNOFILE=65536
UMask=0027
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now traffic-go

echo
echo "traffic-go installed."
echo "Install root: ${INSTALL_ROOT}"
echo
systemctl --no-pager --full status traffic-go || true
echo
echo "Quick checks:"
echo "  curl http://${QUICKCHECK_ADDR}/api/v1/healthz"
echo "  journalctl -u traffic-go -n 100 --no-pager"
