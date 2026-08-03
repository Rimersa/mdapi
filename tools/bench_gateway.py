from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import math
import statistics
import time

from market_data_api.client import GatewayPool, LimitedReader
from market_data_api.model import DataRequest


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="10.10.10.87")
    parser.add_argument("--port", type=int, default=18787)
    parser.add_argument("--token")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--daily-start")
    parser.add_argument("--daily-end")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--requests", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument(
        "--nominal-link-mbps",
        type=float,
        default=1000.0,
        help="仅用于对比标称链路；虚拟交换网络可能不执行该限速",
    )
    args = parser.parse_args()

    if args.start_date or args.end_date:
        if not all(
            (args.start_date, args.end_date, args.daily_start, args.daily_end)
        ):
            parser.error(
                "日期区间模式需要 --start-date/--end-date/"
                "--daily-start/--daily-end"
            )
        first = dt.date.fromisoformat(args.start_date)
        last = dt.date.fromisoformat(args.end_date)
        request = DataRequest.from_values(
            dataset=args.dataset,
            start=f"{first.isoformat()}T00:00:00+08:00",
            end=(
                dt.datetime.combine(
                    last + dt.timedelta(days=1),
                    dt.time(),
                ).isoformat()
                + "+08:00"
            ),
            daily_start=args.daily_start,
            daily_end=args.daily_end,
        )
    else:
        if not args.start or not args.end:
            parser.error("连续区间模式需要 --start 和 --end")
        request = DataRequest.from_values(
            dataset=args.dataset,
            start=args.start,
            end=args.end,
        )
    pool = GatewayPool(
        args.host,
        args.port,
        token=args.token,
        connections=args.concurrency,
    )

    def once() -> dict:
        payload = 0

        def discard(_header: dict, source: LimitedReader) -> None:
            nonlocal payload
            while source.remaining:
                payload += len(source.read(min(source.remaining, 1024 * 1024)))

        started = time.perf_counter()
        with pool.connection() as connection:
            metrics = connection.consume_time_range(request, discard)
        elapsed = time.perf_counter() - started
        return {
            "elapsed": elapsed,
            "payload_bytes": payload,
            "wire_bytes": metrics.wire_bytes,
            "queue_ms": metrics.queue_ms,
            "MBps": payload / elapsed / 1_000_000,
            "nominal_link_pct": (
                payload
                / elapsed
                / (args.nominal_link_mbps * 1_000_000 / 8)
                * 100
            ),
        }

    try:
        for _ in range(args.warmup):
            once()
        started = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.concurrency
        ) as executor:
            results = list(executor.map(lambda _: once(), range(args.requests)))
        wall = time.perf_counter() - started
    finally:
        pool.close()

    latencies = [item["elapsed"] for item in results]
    total_payload = sum(item["payload_bytes"] for item in results)
    output = {
        "concurrency": args.concurrency,
        "requests": args.requests,
        "payload_bytes_each": results[0]["payload_bytes"],
        "p50_seconds": statistics.median(latencies),
        "p95_seconds": percentile(latencies, 0.95),
        "wall_seconds": wall,
        "aggregate_MBps": total_payload / wall / 1_000_000,
        "nominal_link_mbps": args.nominal_link_mbps,
        "aggregate_nominal_link_pct": (
            total_payload
            / wall
            / (args.nominal_link_mbps * 1_000_000 / 8)
            * 100
        ),
        "results": results,
    }
    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
