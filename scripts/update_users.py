from __future__ import annotations

import json
import os
import re
import secrets
import sys
import tempfile
from pathlib import Path


USER_PATTERN = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("用法: update_users.py TOKEN_FILE [USER ...]")
    path = Path(sys.argv[1])
    users = list(dict.fromkeys(sys.argv[2:]))
    invalid = [user for user in users if not USER_PATTERN.fullmatch(user)]
    if invalid:
        raise ValueError(f"非法用户名: {invalid}")
    if path.exists():
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("现有用户令牌文件不是JSON对象")
        tokens = {str(key): str(value) for key, value in raw.items()}
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        tokens = {}
    if len(tokens.values()) != len(set(tokens.values())):
        raise ValueError("现有用户令牌文件包含重复令牌")
    created = {}
    for user in users:
        if user not in tokens:
            created[user] = secrets.token_urlsafe(32)
            tokens[user] = created[user]

    fd, temporary = tempfile.mkstemp(prefix=".users.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, 0o640)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(tokens, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise

    if not users:
        if tokens:
            print(f"保留原有 {len(tokens)} 个用户及令牌。")
        else:
            print("未配置任何用户；网关数据接口将保持锁定。")
    elif created:
        print("新用户令牌（请分别安全交给对应用户）：")
        for user, token in created.items():
            print(f"  {user}: {token}")
    else:
        print("指定用户均已存在，原令牌保持不变。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
