#!/usr/bin/env bash
set -Eeuo pipefail

CLIENT_PACKAGE_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_NATIVE=0
if [[ "${1:-}" == "--native" ]]; then
  CLIENT_NATIVE=1
  shift
fi
CLIENT_GATEWAY_HOST="${1:-10.10.10.87}"
CLIENT_GATEWAY_TOKEN="${2:-}"
if [[ $# -gt 2 ]]; then
  echo "用法: ./install-client.sh [--native] [服务器地址] [令牌]" >&2
  exit 2
fi
if [[ -z "${CLIENT_GATEWAY_TOKEN}" ]]; then
  read -r -s -p "请输入管理员分配的令牌: " CLIENT_GATEWAY_TOKEN
  echo
fi
if [[ -z "${CLIENT_GATEWAY_TOKEN}" || "${CLIENT_GATEWAY_TOKEN}" == *$'\n'* ]]; then
  echo "令牌不能为空或包含换行符。" >&2
  exit 2
fi
if [[ -z "${CLIENT_GATEWAY_HOST}" || "${CLIENT_GATEWAY_HOST}" =~ [[:space:]] ]]; then
  echo "服务器地址不能为空或包含空白字符。" >&2
  exit 2
fi

CLIENT_PYTHON="${MDAPI_PYTHON:-}"
if [[ -z "${CLIENT_PYTHON}" ]]; then
  CLIENT_PYTHON="$(command -v python3 || true)"
fi
if [[ -z "${CLIENT_PYTHON}" ]]; then
  echo "没有找到Python 3.10或更高版本。" >&2
  exit 2
fi
"${CLIENT_PYTHON}" -c \
  'import sys; assert sys.version_info >= (3, 10), "需要Python 3.10或更高版本"'

CLIENT_DATA_PARENT="${XDG_DATA_HOME:-${HOME}/.local/share}"
CLIENT_CONFIG_PARENT="${XDG_CONFIG_HOME:-${HOME}/.config}"
CLIENT_BIN_PARENT="${MDAPI_BIN_DIR:-${HOME}/.local/bin}"
CLIENT_INSTALL_ROOT="${CLIENT_DATA_PARENT}/market-data-api"
CLIENT_CONFIG_ROOT="${CLIENT_CONFIG_PARENT}/market-data-api"
CLIENT_VENV="${CLIENT_INSTALL_ROOT}/venv"
CLIENT_CONFIG="${CLIENT_CONFIG_ROOT}/client.json"
CLIENT_WHEEL="${CLIENT_PACKAGE_ROOT}/wheels/market_data_api-0.5.0-py3-none-any.whl"

if [[ ! -f "${CLIENT_WHEEL}" ]]; then
  echo "发布包不完整：找不到 ${CLIENT_WHEEL}" >&2
  exit 2
fi
mkdir -p "${CLIENT_INSTALL_ROOT}" "${CLIENT_CONFIG_ROOT}" "${CLIENT_BIN_PARENT}"
if [[ ${CLIENT_NATIVE} -eq 1 ]]; then
  "${CLIENT_PYTHON}" -m pip install --upgrade "${CLIENT_WHEEL}[client]"
  "${CLIENT_PYTHON}" "${CLIENT_PACKAGE_ROOT}/scripts/write_client_config.py" \
    "${CLIENT_CONFIG}" "${CLIENT_GATEWAY_HOST}" "${CLIENT_GATEWAY_TOKEN}"
  echo "原生客户端已安装到当前 Python 环境。使用 MarketDataClient.connect() 即可读取。"
  exit 0
fi
if ! "${CLIENT_PYTHON}" -m venv "${CLIENT_VENV}"; then
  echo "无法创建Python虚拟环境；Ubuntu可先安装 python3-venv。" >&2
  exit 1
fi
"${CLIENT_VENV}/bin/pip" install --upgrade "${CLIENT_WHEEL}[api]"

"${CLIENT_PYTHON}" "${CLIENT_PACKAGE_ROOT}/scripts/write_client_config.py" \
  "${CLIENT_CONFIG}" "${CLIENT_GATEWAY_HOST}" "${CLIENT_GATEWAY_TOKEN}"

CLIENT_WRAPPER_TEMP="$(mktemp)"
trap 'rm -f -- "${CLIENT_WRAPPER_TEMP}"' EXIT
{
  printf '#!/usr/bin/env bash\n'
  printf 'exec %q serve-local --config %q "$@"\n' \
    "${CLIENT_VENV}/bin/mdapi" "${CLIENT_CONFIG}"
} >"${CLIENT_WRAPPER_TEMP}"
install -m 0755 "${CLIENT_WRAPPER_TEMP}" "${CLIENT_BIN_PARENT}/mdapi-local"

echo
echo "客户端安装完成。"
echo "需要取数时运行: ${CLIENT_BIN_PARENT}/mdapi-local"
echo "看到 Uvicorn running 后，本机API地址为: http://127.0.0.1:18788"
echo "按 Ctrl+C 即可停止；没有安装任何系统服务。"
if [[ ":${PATH}:" != *":${CLIENT_BIN_PARENT}:"* ]]; then
  echo "提示：${CLIENT_BIN_PARENT} 不在PATH中，请使用上面的完整命令。"
fi
