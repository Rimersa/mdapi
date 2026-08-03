from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import shutil
from pathlib import Path

from market_data_api.catalog import CatalogStore, ObjectEntry
from market_data_api.model import SHANGHAI


def main() -> int:
    parser = argparse.ArgumentParser(
        description="在/dev/shm创建临时零拷贝网络上限测试集"
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--object-mib", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=4)
    parser.add_argument(
        "--source",
        type=Path,
        help="可选：复制一个真实文件到内存盘；省略时创建稀疏测试文件",
    )
    args = parser.parse_args()
    root = args.root.resolve(strict=False)
    shm = Path("/dev/shm").resolve(strict=True)
    if not root.is_relative_to(shm) or not root.name.startswith(
        "mdapi_line_rate_"
    ):
        raise ValueError("root 必须是 /dev/shm/mdapi_line_rate_* 专属目录")
    if root.exists():
        raise FileExistsError(f"拒绝覆盖已有目录: {root}")
    if args.object_mib < 1 or args.repeats < 1:
        raise ValueError("object-mib 和 repeats 必须 >= 1")

    root.mkdir(parents=True)
    payload = root / "payload.bin"
    if args.source is not None:
        source = args.source.resolve(strict=True)
        if not source.is_file():
            raise ValueError(f"source 不是普通文件: {source}")
        shutil.copyfile(source, payload)
        size = payload.stat().st_size
    else:
        size = args.object_mib * 1024**2
        with payload.open("wb") as handle:
            handle.truncate(size)
    start = dt.datetime(2026, 1, 1, 9, 15, tzinfo=SHANGHAI)
    end = start + dt.timedelta(minutes=5)
    entries = []
    for index in range(args.repeats):
        object_id = hashlib.sha256(f"line-rate-{index}".encode()).hexdigest()
        entries.append(
            ObjectEntry(
                object_id=object_id,
                dataset="snapshots",
                trade_date="2026-01-01",
                bucket_start=start.isoformat(timespec="milliseconds"),
                bucket_end=end.isoformat(timespec="milliseconds"),
                relative_path="payload.bin",
                bytes=size,
                rows=0,
                uncompressed_bytes=size,
                source_fingerprint="synthetic-line-rate-fixture",
                version="synthetic-a",
            )
        )
    catalog = CatalogStore.initialize_for_write(root)
    catalog.replace_partition("snapshots", "2026-01-01", entries)
    print(
        {
            "root": str(root),
            "object_bytes": size,
            "repeats": args.repeats,
            "response_payload_bytes": size * args.repeats,
            "allocated_bytes": payload.stat().st_blocks * 512,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
