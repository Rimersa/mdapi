"""Check the configured address without printing tokens or other private settings."""

import json
import sys
import urllib.request
from pathlib import Path

values = dict(
    line.split("=", 1)
    for line in Path(sys.argv[1]).read_text().splitlines()
    if "=" in line
)
host = values["MDAPI_GATEWAY_HOST"]
if host in ("0.0.0.0", "::"):
    host = "127.0.0.1"
url = f"http://{host}:{int(values['MDAPI_GATEWAY_PORT'])}/health"
with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
    url, timeout=1
) as response:
    health = json.load(response)
if health.get("status") != "ok" or "range_bundles_v1" not in health.get(
    "capabilities", []
):
    raise RuntimeError("网关未就绪或不是支持按块读取的新版本")
