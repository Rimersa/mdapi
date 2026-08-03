from __future__ import annotations

import argparse
import shutil
import tempfile
import zipapp
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "market_data_api"


def main() -> int:
    parser = argparse.ArgumentParser(description="构建纯标准库单文件只读网关")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mdapi-pyz-") as temporary:
        stage = Path(temporary)
        shutil.copytree(
            PACKAGE_ROOT,
            stage / "market_data_api",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        zipapp.create_archive(
            stage,
            target=output,
            interpreter="/usr/bin/env python3",
            main="market_data_api.gateway:main",
            compressed=True,
        )
    output.chmod(0o755)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
