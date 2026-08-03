from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import statistics
import time
import urllib.request


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:18788/v1/data")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--daily-start")
    parser.add_argument("--daily-end")
    parser.add_argument("--mode", choices=("direct", "cache"), default="direct")
    parser.add_argument("--update", default="missing_only")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--requests", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    args = parser.parse_args()
    payload = {
        "dataset": args.dataset,
        "mode": args.mode,
        "update": args.update,
    }
    if args.start_date or args.end_date:
        if not all(
            (args.start_date, args.end_date, args.daily_start, args.daily_end)
        ):
            parser.error(
                "日期区间模式需要 --start-date/--end-date/"
                "--daily-start/--daily-end"
            )
        payload.update(
            {
                "start_date": args.start_date,
                "end_date": args.end_date,
                "daily_start": args.daily_start,
                "daily_end": args.daily_end,
            }
        )
    else:
        if not args.start or not args.end:
            parser.error("连续区间模式需要 --start 和 --end")
        payload.update({"start": args.start, "end": args.end})
    body = json.dumps(payload).encode()

    def once() -> dict:
        request = urllib.request.Request(
            args.url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept-Encoding": "identity",
            },
        )
        started = time.perf_counter()
        received = 0
        with urllib.request.urlopen(request, timeout=600) as response:
            while True:
                raw = response.read(1024 * 1024)
                if not raw:
                    break
                received += len(raw)
            rows = int(response.headers.get("X-MDAPI-Rows", "0"))
            source_bytes = int(
                response.headers.get("X-MDAPI-Source-Bytes", "0")
            )
        elapsed = time.perf_counter() - started
        return {
            "elapsed": elapsed,
            "received_bytes": received,
            "source_bytes": source_bytes,
            "rows_estimate": rows,
            "response_MBps": received / elapsed / 1_000_000,
            "source_MBps": source_bytes / elapsed / 1_000_000,
        }

    for _ in range(args.warmup):
        once()
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        results = list(executor.map(lambda _: once(), range(args.requests)))
    wall = time.perf_counter() - started
    latencies = [item["elapsed"] for item in results]
    output = {
        "mode": args.mode,
        "concurrency": args.concurrency,
        "requests": args.requests,
        "p50_seconds": statistics.median(latencies),
        "p95_seconds": percentile(latencies, 0.95),
        "wall_seconds": wall,
        "aggregate_response_MBps": sum(
            item["received_bytes"] for item in results
        )
        / wall
        / 1_000_000,
        "results": results,
    }
    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
