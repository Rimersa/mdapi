"""Insert or update one day of a directly-scanned derived table.

This is the only administrative write entry for the 0.8 direct-table layout.
It has no manifest and no table version directory: supplied columns are merged
into ``<root>/<table>/trade_date=YYYY-MM-DD/data.parquet``.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from market_data_api.tables import DEFAULT_KEYS, upsert_daily


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True, help="直接派生表根目录")
    p.add_argument("--table", required=True, help="表名，如 daily_quality / factors_5m")
    p.add_argument("--date", required=True, help="交易日 YYYY-MM-DD")
    p.add_argument("--input", required=True, help="当天结果 Parquet；可含 time/symbol 或只有 symbol")
    p.add_argument("--keys", default=",".join(DEFAULT_KEYS), help="合并键，默认 time,symbol")
    p.add_argument("--granularity", help="表粒度说明，如 daily / 5m")
    p.add_argument("--description", help="表说明")
    p.add_argument("--field-info", help="JSON文件：字段名 -> description/unit")
    args = p.parse_args()
    keys = tuple(item.strip() for item in args.keys.split(",") if item.strip())
    fields = json.loads(Path(args.field_info).read_text()) if args.field_info else None
    result = upsert_daily(
        args.root,
        args.table,
        args.date,
        args.input,
        keys=keys,
        granularity=args.granularity,
        description=args.description,
        fields=fields,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
