#!/usr/bin/python3
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import stat
import sys
import tempfile
from pathlib import Path


USER_PATTERN = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")


def default_token_file() -> Path:
    system = Path("/etc/market-data-api/users.json")
    if os.geteuid() == 0:
        return system
    return Path.home() / ".config" / "market-data-api-server" / "users.json"


def load_tokens(path: Path) -> dict[str, str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("用户令牌文件必须是JSON对象")
    tokens = {str(key): str(value) for key, value in raw.items()}
    invalid = [user for user in tokens if not USER_PATTERN.fullmatch(user)]
    if invalid:
        raise ValueError(f"用户令牌文件包含非法用户名: {invalid}")
    if any(not token for token in tokens.values()):
        raise ValueError("用户令牌不能为空")
    if len(tokens.values()) != len(set(tokens.values())):
        raise ValueError("不同用户不能使用相同令牌")
    return tokens


def atomic_write(path: Path, tokens: dict[str, str]) -> None:
    previous = path.stat()
    mode = stat.S_IMODE(previous.st_mode)
    fd, temporary = tempfile.mkstemp(prefix=".users.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        if hasattr(os, "fchown"):
            os.fchown(fd, previous.st_uid, previous.st_gid)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(tokens, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def require_user(value: str) -> str:
    if not USER_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError(
            "用户名只能包含字母、数字、下划线、点、@和连字符，最长64字符"
        )
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="管理Market Data API用户（热加载）")
    parser.add_argument("--file", type=Path, default=default_token_file())
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("add", "show", "rotate", "remove"):
        child = subparsers.add_parser(command)
        child.add_argument("user", type=require_user)
    subparsers.add_parser("list")
    args = parser.parse_args()
    path = args.file.expanduser().resolve(strict=True)
    tokens = load_tokens(path)

    if args.command == "list":
        if not tokens:
            print("（暂无用户，数据接口处于锁定状态）")
        else:
            for user in sorted(tokens):
                print(user)
        return 0

    user = args.user
    if args.command == "show":
        if user not in tokens:
            raise KeyError(f"用户不存在: {user}")
        print(f"{user}: {tokens[user]}")
        return 0

    if args.command == "add":
        if user not in tokens:
            tokens[user] = secrets.token_urlsafe(32)
            atomic_write(path, tokens)
            print("用户已新增，令牌已立即生效：")
        else:
            print("用户已存在，令牌未改变：")
        print(f"{user}: {tokens[user]}")
        return 0

    if args.command == "rotate":
        if user not in tokens:
            raise KeyError(f"用户不存在: {user}")
        tokens[user] = secrets.token_urlsafe(32)
        atomic_write(path, tokens)
        print("令牌已轮换并立即生效：")
        print(f"{user}: {tokens[user]}")
        return 0

    if user not in tokens:
        raise KeyError(f"用户不存在: {user}")
    del tokens[user]
    atomic_write(path, tokens)
    print(f"用户已移除并立即失效: {user}")
    if not tokens:
        print("当前已无用户，数据接口恢复锁定状态。")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, ValueError, OSError, json.JSONDecodeError) as exc:
        detail = exc.args[0] if isinstance(exc, KeyError) else str(exc)
        print(f"错误: {detail}", file=sys.stderr)
        raise SystemExit(2)
