from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GATEWAY_ARCHIVE = PROJECT_ROOT / "bin" / "mdapi-gateway.pyz"
if GATEWAY_ARCHIVE.is_file():
    sys.path.insert(0, str(GATEWAY_ARCHIVE))
else:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from market_data_api.catalog import (
    CATALOG_FORMAT,
    CATALOG_INDEX_FORMAT,
    Catalog,
    migrate_legacy_catalog,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="把单一巨大catalog v1无损迁移为按数据集/交易日分片的v2索引"
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="真正写入分片并最后原子切换catalog.json；默认只打印计划",
    )
    parser.add_argument(
        "--keep-backup",
        action="store_true",
        help="额外保留catalog.v1.backup.json；默认不留下旧格式副本",
    )
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    index_path = root / "catalog.json"
    raw = json.loads(index_path.read_text(encoding="utf-8"))
    catalog_format = raw.get("format")
    if catalog_format == CATALOG_INDEX_FORMAT:
        print(json.dumps({"status": "already_v2", "root": str(root)}))
        return 0
    if catalog_format != CATALOG_FORMAT:
        raise ValueError(f"不支持的catalog格式: {catalog_format!r}")
    legacy = Catalog.from_dict(raw)
    partitions = {
        (entry.dataset, entry.trade_date) for entry in legacy.objects
    }
    plan = {
        "status": "plan",
        "root": str(root),
        "objects": len(legacy.objects),
        "shards": len(partitions),
        "keeps_parquet_unchanged": True,
        "backup": (
            str(root / "catalog.v1.backup.json")
            if args.keep_backup
            else None
        ),
        "execute": args.execute,
    }
    print(json.dumps(plan, ensure_ascii=False))
    if not args.execute:
        return 0
    result = migrate_legacy_catalog(root, keep_backup=args.keep_backup)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
