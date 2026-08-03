from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("用法: write_client_config.py CONFIG HOST TOKEN")
    path = Path(sys.argv[1]).expanduser()
    host = sys.argv[2]
    token = sys.argv[3]
    if not host or any(char.isspace() for char in host):
        raise ValueError("服务器地址不能为空或包含空白字符")
    if not token or "\n" in token or "\r" in token:
        raise ValueError("令牌不能为空或包含换行符")
    payload = {
        "gateway_host": host,
        "gateway_port": 18787,
        "gateway_token": token,
        "cache_root": str(Path.home() / ".cache" / "market-data-api"),
        "local_host": "127.0.0.1",
        "local_port": 18788,
        "arrow_compression": "zstd",
        "network_retries": 3,
        "network_retry_backoff": 0.25,
        "object_request_size": 12,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".client.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    os.chmod(path, 0o600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
