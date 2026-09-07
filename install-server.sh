#!/usr/bin/env bash
set -Eeuo pipefail

SERVER_PACKAGE_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SERVER_DATA_ROOT="${1:-}"
if [[ -z "${SERVER_DATA_ROOT}" ]]; then
  echo "用法: ./install-server.sh /五分钟数据根目录 [用户1 用户2 ...]" >&2
  echo "加sudo安装为系统服务；不加sudo安装为当前用户服务。" >&2
  exit 2
fi
shift
SERVER_USERS=("$@")

if [[ ! -d "${SERVER_DATA_ROOT}" || ! -f "${SERVER_DATA_ROOT}/catalog.json" ]]; then
  echo "数据目录必须已经存在，并包含 catalog.json: ${SERVER_DATA_ROOT}" >&2
  exit 2
fi
if [[ "${SERVER_DATA_ROOT}" != /* ]]; then
  echo "五分钟数据根目录必须使用绝对路径。" >&2
  exit 2
fi
SERVER_DATA_ROOT="$(readlink -f -- "${SERVER_DATA_ROOT}")"
if [[ "${SERVER_DATA_ROOT}" == *$'\n'* ]]; then
  echo "数据目录不能包含换行符。" >&2
  exit 2
fi
SERVER_PYZ_SOURCE="${SERVER_PACKAGE_ROOT}/bin/mdapi-gateway.pyz"
SERVER_USER_TOOL="${SERVER_PACKAGE_ROOT}/scripts/update_users.py"
SERVER_ADMIN_TOOL_SOURCE="${SERVER_PACKAGE_ROOT}/scripts/manage_users.py"
if [[ ${EUID} -eq 0 ]]; then
  if ! id quant >/dev/null 2>&1; then
    echo "系统服务模式要求87上存在运行账号 quant。" >&2
    exit 2
  fi
  SERVER_INSTALL_MODE="system"
  SERVER_INSTALL_ROOT="/opt/market-data-api"
  SERVER_CONFIG_ROOT="/etc/market-data-api"
  SERVER_TOKEN_FILE="${SERVER_CONFIG_ROOT}/users.json"
  SERVER_UNIT_SOURCE="${SERVER_PACKAGE_ROOT}/deploy/server/market-data-gateway.service"
  SERVER_UNIT_TARGET="/etc/systemd/system/market-data-gateway.service"
  SERVER_ADMIN_TOOL_TARGET="/usr/local/sbin/mdapi-user"
  SERVER_CACHE_ROOT="/var/cache/market-data-api"
else
  SERVER_INSTALL_MODE="user"
  SERVER_INSTALL_ROOT="${HOME}/.local/share/market-data-api-server"
  SERVER_CONFIG_ROOT="${HOME}/.config/market-data-api-server"
  SERVER_TOKEN_FILE="${SERVER_CONFIG_ROOT}/users.json"
  SERVER_UNIT_SOURCE="${SERVER_PACKAGE_ROOT}/deploy/server/market-data-gateway-user.service"
  SERVER_UNIT_TARGET="${HOME}/.config/systemd/user/market-data-gateway.service"
  SERVER_ADMIN_TOOL_TARGET="${HOME}/.local/bin/mdapi-user"
  SERVER_CACHE_ROOT="${HOME}/.cache/market-data-api-server"
fi
if [[ ! -f "${SERVER_PYZ_SOURCE}" || ! -f "${SERVER_UNIT_SOURCE}" || ! -f "${SERVER_USER_TOOL}" || ! -f "${SERVER_ADMIN_TOOL_SOURCE}" ]]; then
  echo "发布包不完整：找不到单文件网关或systemd模板。" >&2
  exit 2
fi

/usr/bin/python3 "${SERVER_PYZ_SOURCE}" --help >/dev/null
install -d -m 0755 "${SERVER_INSTALL_ROOT}"
install -m 0755 "${SERVER_PYZ_SOURCE}" "${SERVER_INSTALL_ROOT}/mdapi-gateway.pyz"
if [[ "${SERVER_INSTALL_MODE}" == "system" ]]; then
  install -d -m 0750 -o root -g quant "${SERVER_CONFIG_ROOT}"
  install -d -m 0700 -o quant -g quant "${SERVER_CACHE_ROOT}"
else
  install -d -m 0700 "${SERVER_CONFIG_ROOT}"
  install -d -m 0700 "${SERVER_CACHE_ROOT}"
  install -d -m 0755 "$(dirname -- "${SERVER_UNIT_TARGET}")"
fi

SERVER_CONFIG_TEMP="$(mktemp)"
trap 'rm -f -- "${SERVER_CONFIG_TEMP}"' EXIT
/usr/bin/python3 "${SERVER_PACKAGE_ROOT}/scripts/write_gateway_config.py" \
  "${SERVER_CONFIG_TEMP}" "${SERVER_CONFIG_ROOT}/gateway.env" \
  "${SERVER_DATA_ROOT}" "${SERVER_TOKEN_FILE}" "${SERVER_CACHE_ROOT}/footers.sqlite3"
if [[ "${SERVER_INSTALL_MODE}" == "system" ]]; then
  install -m 0640 -o root -g quant \
    "${SERVER_CONFIG_TEMP}" "${SERVER_CONFIG_ROOT}/gateway.env"
else
  install -m 0600 "${SERVER_CONFIG_TEMP}" "${SERVER_CONFIG_ROOT}/gateway.env"
fi

/usr/bin/python3 "${SERVER_USER_TOOL}" \
  "${SERVER_TOKEN_FILE}" "${SERVER_USERS[@]}"
if [[ "${SERVER_INSTALL_MODE}" == "system" ]]; then
  chown root:quant "${SERVER_TOKEN_FILE}"
  chmod 0640 "${SERVER_TOKEN_FILE}"
else
  chmod 0600 "${SERVER_TOKEN_FILE}"
fi

install -m 0644 "${SERVER_UNIT_SOURCE}" "${SERVER_UNIT_TARGET}"
install -d -m 0755 "$(dirname -- "${SERVER_ADMIN_TOOL_TARGET}")"
install -m 0755 "${SERVER_ADMIN_TOOL_SOURCE}" "${SERVER_ADMIN_TOOL_TARGET}"
if [[ "${SERVER_INSTALL_MODE}" == "system" ]]; then
  systemctl daemon-reload
  systemctl enable market-data-gateway
  systemctl restart market-data-gateway
else
  systemctl --user daemon-reload
  systemctl --user enable market-data-gateway
  systemctl --user restart market-data-gateway
fi

SERVER_HEALTH_OK=0
for _ in {1..40}; do
  if /usr/bin/python3 "${SERVER_PACKAGE_ROOT}/scripts/check_gateway_health.py" \
    "${SERVER_CONFIG_ROOT}/gateway.env" \
    >/dev/null 2>&1; then
    SERVER_HEALTH_OK=1
    break
  fi
  sleep 0.25
done
if [[ ${SERVER_HEALTH_OK} -ne 1 ]]; then
  echo "网关没有在10秒内通过健康检查，请执行:" >&2
  if [[ "${SERVER_INSTALL_MODE}" == "system" ]]; then
    echo "  systemctl status market-data-gateway" >&2
    echo "  journalctl -u market-data-gateway -n 100" >&2
  else
    echo "  systemctl --user status market-data-gateway" >&2
    echo "  journalctl --user -u market-data-gateway -n 100" >&2
  fi
  exit 1
fi

echo
echo "服务器安装完成；地址和端口以 ${SERVER_CONFIG_ROOT}/gateway.env 为准。"
echo "安装模式: ${SERVER_INSTALL_MODE}"
echo "数据目录只读使用: ${SERVER_DATA_ROOT}"
echo "压缩元数据缓存: ${SERVER_CACHE_ROOT}/footers.sqlite3（行情目录之外）"
echo "用户令牌保存在: ${SERVER_TOKEN_FILE}"
echo "用户管理命令: ${SERVER_ADMIN_TOOL_TARGET}"
if [[ ${#SERVER_USERS[@]} -eq 0 ]]; then
  SERVER_USER_COUNT="$(/usr/bin/python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))))' "${SERVER_TOKEN_FILE}")"
  if [[ "${SERVER_USER_COUNT}" == "0" ]]; then
    echo "当前没有用户：健康接口可用，所有数据接口均保持锁定。"
  else
    echo "已保留原有 ${SERVER_USER_COUNT} 个用户及令牌。"
  fi
fi
if [[ "${SERVER_INSTALL_MODE}" == "user" ]]; then
  SERVER_LINGER="$(loginctl show-user "${USER}" -p Linger --value 2>/dev/null || true)"
  if [[ "${SERVER_LINGER}" != "yes" ]]; then
    echo "提示：当前是用户服务且Linger未开启；管理员执行"
    echo "  sudo loginctl enable-linger ${USER}"
    echo "后可保证无人登录和重启后仍自动运行。"
  fi
fi
