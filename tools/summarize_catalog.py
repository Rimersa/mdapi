from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GATEWAY_ARCHIVE = PROJECT_ROOT / "bin" / "mdapi-gateway.pyz"
if GATEWAY_ARCHIVE.is_file():
    sys.path.insert(0, str(GATEWAY_ARCHIVE))
else:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from market_data_api.catalog import CatalogStore


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    catalog = CatalogStore(args.root)
    entries = catalog.all_current_entries()
    grouped = defaultdict(lambda: {"objects": 0, "rows": 0, "bytes": 0})
    dates = set()
    for entry in entries:
        dates.add(entry.trade_date)
        item = grouped[entry.dataset]
        item["objects"] += 1
        item["rows"] += entry.rows
        item["bytes"] += entry.bytes
    output = {
        "generated_at": catalog.generated_at,
        "dates": sorted(dates),
        "date_count": len(dates),
        "objects": len(entries),
        "rows": sum(entry.rows for entry in entries),
        "bytes": sum(entry.bytes for entry in entries),
        "datasets": dict(sorted(grouped.items())),
    }
    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
