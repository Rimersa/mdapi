"""Run against a candidate gateway explicitly configured with --points-root."""

from market_data_api import MarketDataClient


def main():
    with MarketDataClient.connect(cores=2) as api:
        query = {
            "dataset": "flow_points",
            "start_date": "2026-09-02",
            "end_date": "2026-09-02",
            "symbols": ["000001.SZ", "600000.SH"],
            "columns": ["symbol", "time", "amount", "volume"],
        }
        print(api.estimate(query)["coverage"])
        rows = 0
        for batch in api.iter_batches(query):
            rows += batch.num_rows
        print({"rows": rows, "stats": api.last_read_stats})


if __name__ == "__main__":
    main()
