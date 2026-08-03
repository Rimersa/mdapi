from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mdapi")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("serve-local", help="启动本机 FastAPI", add_help=False)
    subparsers.add_parser(
        "serve-gateway",
        help="启动87只读数据网关",
        add_help=False,
    )
    args, remaining = parser.parse_known_args(argv)
    if args.command == "serve-local":
        from .local_api import main as command
    else:
        from .gateway import main as command
    return command(remaining)


if __name__ == "__main__":
    raise SystemExit(main())
