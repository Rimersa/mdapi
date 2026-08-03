from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import threading
import time
from pathlib import Path

from market_data_api.client import GatewayConnection, LimitedReader
from market_data_api.model import DataRequest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="10.10.10.87")
    parser.add_argument("--port", type=int, default=18787)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--requests-per-user", type=int, default=2)
    args = parser.parse_args()
    tokens = json.loads(args.token_file.read_text(encoding="utf-8"))
    if not isinstance(tokens, dict) or len(tokens) < 2:
        parser.error("token-file 至少需要两个用户")
    request = DataRequest.from_values(
        dataset=args.dataset,
        start=args.start,
        end=args.end,
    )
    barrier = threading.Barrier(len(tokens))

    def run_user(item: tuple[str, str]) -> list[dict]:
        user_id, token = item
        connection = GatewayConnection(args.host, args.port, token=token)
        values: list[dict] = []
        barrier.wait()
        try:
            for sequence in range(1, args.requests_per_user + 1):
                payload = 0

                def discard(_header: dict, source: LimitedReader) -> None:
                    nonlocal payload
                    while source.remaining:
                        payload += len(
                            source.read(min(source.remaining, 1024 * 1024))
                        )

                started = time.perf_counter()
                metrics = connection.consume_time_range(request, discard)
                completed = time.perf_counter()
                values.append(
                    {
                        "user_id": user_id,
                        "sequence": sequence,
                        "started_offset": started,
                        "completed_offset": completed,
                        "elapsed": completed - started,
                        "queue_ms": metrics.queue_ms,
                        "payload_bytes": payload,
                    }
                )
        finally:
            connection.close()
        return values

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=len(tokens)
    ) as executor:
        nested = list(executor.map(run_user, tokens.items()))
    wall = time.perf_counter() - started
    results = [value for values in nested for value in values]
    for value in results:
        value["started_offset"] -= started
        value["completed_offset"] -= started
    results.sort(key=lambda value: value["completed_offset"])
    total_bytes = sum(value["payload_bytes"] for value in results)
    print(
        json.dumps(
            {
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "users": len(tokens),
                "requests_per_user": args.requests_per_user,
                "wall_seconds": wall,
                "aggregate_MBps": total_bytes / wall / 1_000_000,
                "completion_order": [
                    f"{value['user_id']}#{value['sequence']}"
                    for value in results
                ],
                "results": results,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
