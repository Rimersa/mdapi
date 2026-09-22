"""Publish new factor columns or window tables without changing the gateway."""

import argparse
import json
from pathlib import Path

from market_data_api.publish import publish_table


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--name", required=True)
    p.add_argument(
        "--input", required=True, help="trade_date=YYYY-MM-DD/*.parquet directories"
    )
    p.add_argument("--granularity", default="daily")
    p.add_argument("--description", default="")
    p.add_argument("--field-info", help="JSON: field name -> description/unit metadata")
    args = p.parse_args()
    parts = {
        d.name.split("=", 1)[1]: sorted(d.glob("*.parquet"))
        for d in Path(args.input).glob("trade_date=*")
        if d.is_dir()
    }
    print(
        json.dumps(
            publish_table(
                args.root,
                args.name,
                parts,
                description=args.description,
                granularity=args.granularity,
                field_info=(
                    json.loads(Path(args.field_info).read_text())
                    if args.field_info
                    else None
                ),
            ),
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
