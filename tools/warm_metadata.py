"""Populate gateway metadata caches for an explicit date range; never fetch data columns."""

import argparse
import json
from market_data_api import MarketDataClient
from market_data_api.model import DataRequest
from market_data_api.selective import MetadataCache, ReadStats, load_metadata


def main():
    parser = argparse.ArgumentParser(
        description="预热独立元数据缓存，不下载或重写行情文件"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--datasets", nargs="+", default=["orders", "trades", "snapshots"]
    )
    args = parser.parse_args()
    with MarketDataClient.connect(config=args.config, cores=1) as client:
        for dataset in args.datasets:
            request = DataRequest.from_query(
                dict(
                    dataset=dataset,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    daily_start="00:00",
                    daily_end="23:59:59.999999",
                )
            )
            selection = client.service.preflight(request).selection
            if "parquet_footers_v1" not in selection.capabilities:
                raise RuntimeError("网关不支持元数据预热，请先升级至 0.5")
            stats = ReadStats()
            for offset in range(0, len(selection.entries), 12):
                load_metadata(
                    client.service.pool,
                    selection.entries[offset : offset + 12],
                    MetadataCache(0),
                    stats,
                    3,
                    0.25,
                )
            print(
                json.dumps(
                    {
                        "dataset": dataset,
                        "objects": len(selection.entries),
                        "metadata_bytes": stats.metadata_bytes,
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    main()
