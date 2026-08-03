from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GATEWAY_ARCHIVE = PROJECT_ROOT / "bin" / "mdapi-gateway.pyz"
if GATEWAY_ARCHIVE.is_file():
    sys.path.insert(0, str(GATEWAY_ARCHIVE))
else:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from market_data_api.catalog import CatalogStore
from market_data_api.model import DataRequest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="按本机可用内存预算预热87派生对象到Linux页缓存"
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--max-gib", type=float, default=16.0)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    catalog = CatalogStore(root)
    request = DataRequest.from_values(
        dataset=args.dataset,
        start=args.start,
        end=args.end,
    )
    selected = catalog.selected(request)
    budget = int(args.max_gib * 1024**3)
    advised = 0
    objects = 0
    for entry in selected:
        if advised + entry.bytes > budget:
            break
        path = (root / entry.relative_path).resolve(strict=True)
        if not path.is_relative_to(root):
            raise RuntimeError(f"catalog 路径越界: {entry.relative_path}")
        with path.open("rb", buffering=0) as handle:
            if hasattr(os, "posix_fadvise"):
                os.posix_fadvise(
                    handle.fileno(),
                    0,
                    0,
                    os.POSIX_FADV_WILLNEED,
                )
            while handle.read(4 * 1024**2):
                pass
        advised += entry.bytes
        objects += 1
    print(
        json.dumps(
            {
                "objects": objects,
                "advised_bytes": advised,
                "budget_bytes": budget,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
