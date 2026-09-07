"""Read-only real-data acceptance benchmark. Credentials never enter output files."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import time

import psutil

from market_data_api import MarketDataClient


STOCKS = [
    "000001.SZ",
    "000002.SZ",
    "000333.SZ",
    "000651.SZ",
    "000858.SZ",
    "002594.SZ",
    "600000.SH",
    "600036.SH",
    "600519.SH",
    "601318.SH",
]


def gateway_io(host, pid):
    if not pid:
        return {}
    command = f"import json,pathlib; p=pathlib.Path('/proc/{int(pid)}/io'); print(json.dumps(dict(x.split(': ') for x in p.read_text().strip().splitlines())))"
    result = subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "StrictHostKeyChecking=yes",
            f"quant@{host}",
            "python3",
            "-",
        ],
        input=command,
        text=True,
        capture_output=True,
        check=True,
        timeout=15,
    )
    return {k: int(v) for k, v in json.loads(result.stdout).items()}


def run(client, query):
    peak = psutil.Process().memory_info().rss
    stopped = threading.Event()

    def sample():
        nonlocal peak
        while not stopped.wait(0.02):
            peak = max(peak, psutil.Process().memory_info().rss)

    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    started = time.monotonic()
    first = None
    rows = arrow_bytes = 0
    try:
        for batch in client.iter_batches(query):
            if batch.num_rows and first is None:
                first = time.monotonic() - started
            rows += batch.num_rows
            arrow_bytes += batch.nbytes
        elapsed = time.monotonic() - started
    finally:
        stopped.set()
        monitor.join()
    return dict(
        seconds=elapsed,
        first_batch_seconds=first,
        rows=rows,
        arrow_bytes=arrow_bytes,
        process_peak_rss=peak,
        stats=client.last_read_stats,
    )


def main():
    parser = argparse.ArgumentParser(
        description="真实数据按需读取验收；仅执行 direct 查询"
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        required=True,
        help="私有 JSON：host、port、tokens（用户名到令牌的映射）",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--date", default="2026-08-26")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--gateway-pid", type=int)
    parser.add_argument(
        "--include-100", action="store_true", help="增加分散的 100 股票测试"
    )
    parser.add_argument(
        "--suite", choices=["matrix", "concurrency", "long"], default="matrix"
    )
    args = parser.parse_args()
    credentials = json.loads(args.credentials.read_text())
    host, port = credentials["host"], int(credentials["port"])
    tokens = list(credentials["tokens"].values())
    output = {
        "date": args.date,
        "suite": args.suite,
        "gateway": f"{host}:{port}",
        "started_at": dt.datetime.now().astimezone().isoformat(),
        "cache_policy": "direct; no page-cache eviction; gateway /proc I/O delta when enabled",
        "results": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save(result):
        output["results"].append(result)
        args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(result, ensure_ascii=False), flush=True)

    with tempfile.TemporaryDirectory(prefix="mdapi-perf-") as temporary:
        root = Path(temporary)
        clients = [
            MarketDataClient.connect(
                gateway_host=host,
                gateway_port=port,
                gateway_token=token,
                cache_root=root / str(i),
                cores=2,
            )
            for i, token in enumerate(tokens)
        ]
        try:
            point = dict(
                start=f"{args.date}T09:30:00+08:00",
                end=f"{args.date}T09:35:00+08:00",
                mode="direct",
            )
            if args.suite == "matrix":
                cases = []
                hundred = None
                if args.include_100:
                    sample = clients[0].read_table(
                        dict(
                            dataset="snapshots",
                            start=f"{args.date}T09:15:00+08:00",
                            end=f"{args.date}T09:15:01+08:00",
                            columns=["symbol"],
                            mode="direct",
                        )
                    )
                    universe = sorted(set(sample.column("symbol").to_pylist()))
                    if len(universe) < 100:
                        raise RuntimeError("用于分散股票采样的数据不足")
                    hundred = [
                        universe[i * (len(universe) - 1) // 99] for i in range(100)
                    ]
                for dataset in ["orders", "trades", "snapshots"]:
                    price, volume = (
                        ("last_px_i32", "volume_i64")
                        if dataset == "snapshots"
                        else ("price_i32", "qty_i32")
                    )
                    cases.append(
                        (
                            f"{dataset}_one_stock_all_columns",
                            dict(point, dataset=dataset, symbols=STOCKS[:1]),
                        )
                    )
                    if hundred is not None:
                        cases.append(
                            (
                                f"{dataset}_hundred_stocks_four_columns",
                                dict(
                                    point,
                                    dataset=dataset,
                                    symbols=hundred,
                                    columns=["symbol", "event_time", price, volume],
                                ),
                            )
                        )
                    cases.append(
                        (
                            f"{dataset}_ten_stocks_four_columns",
                            dict(
                                point,
                                dataset=dataset,
                                symbols=STOCKS,
                                columns=["symbol", "event_time", price, volume],
                            ),
                        )
                    )
                cases.extend(
                    [
                        (
                            "snapshots_market_auction_all_columns",
                            dict(
                                dataset="snapshots",
                                mode="direct",
                                start=f"{args.date}T09:15:00+08:00",
                                end=f"{args.date}T09:25:00+08:00",
                            ),
                        ),
                        (
                            "snapshots_market_four_columns",
                            dict(
                                point,
                                dataset="snapshots",
                                columns=[
                                    "symbol",
                                    "event_time",
                                    "last_px_i32",
                                    "volume_i64",
                                ],
                            ),
                        ),
                    ]
                )
                for name, query in cases:
                    row_counts = set()
                    for repeat in range(args.repeats):
                        for strategy in ("auto", "sequential"):
                            before = gateway_io(host, args.gateway_pid)
                            result = run(
                                clients[0], dict(query, read_strategy=strategy)
                            )
                            after = gateway_io(host, args.gateway_pid)
                            result.update(
                                case=name,
                                strategy=strategy,
                                repeat=repeat,
                                gateway_disk_read_bytes=after.get("read_bytes", 0)
                                - before.get("read_bytes", 0),
                            )
                            row_counts.add(result["rows"])
                            save(result)
                    assert len(row_counts) == 1, (name, row_counts)
                    if query.get("symbols") is not None:
                        first = clients[0].read_table(dict(query, read_strategy="auto"))
                        second = clients[0].read_table(
                            dict(query, read_strategy="sequential")
                        )
                        assert first.equals(second), name
                        save(
                            dict(
                                case=name,
                                acceptance="exact_arrow_content_equal",
                                rows=first.num_rows,
                            )
                        )
            elif args.suite == "concurrency":
                if len(clients) < 4:
                    raise ValueError("四用户测试需要四个独立令牌")
                barrier = threading.Barrier(4)

                def worker(i):
                    cases = [
                        dict(point, dataset="orders", symbols=[STOCKS[i]]),
                        dict(point, dataset="trades", symbols=STOCKS[:5]),
                        dict(
                            dataset="snapshots",
                            start=f"{args.date}T09:15:00+08:00",
                            end=f"{args.date}T09:25:00+08:00",
                        ),
                    ]
                    results = []
                    for repeat in range(args.repeats):
                        barrier.wait(timeout=30)
                        for case, query in enumerate(cases):
                            result = run(clients[i], dict(query, mode="direct"))
                            result.update(user=i, case=case, repeat=repeat)
                            results.append(result)
                    return results

                begin = time.monotonic()
                before = gateway_io(host, args.gateway_pid)
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(worker, i) for i in range(4)]
                    for future in futures:
                        for result in future.result(timeout=300):
                            save(result)
                after = gateway_io(host, args.gateway_pid)
                save(
                    dict(
                        acceptance="four_users_completed",
                        seconds=time.monotonic() - begin,
                        requests=4 * 3 * args.repeats,
                        gateway_disk_read_bytes=after.get("read_bytes", 0)
                        - before.get("read_bytes", 0),
                    )
                )
            else:
                end = dt.date.fromisoformat(args.date)
                start = end - dt.timedelta(days=6)
                for dataset in ["snapshots", "trades", "orders"]:
                    query = dict(
                        dataset=dataset,
                        start_date=start.isoformat(),
                        end_date=end.isoformat(),
                        daily_start="09:30",
                        daily_end="15:00",
                        symbols=STOCKS[:1],
                        columns=["symbol", "event_time", "time_int"],
                        mode="direct",
                    )
                    for repeat in range(2):
                        before = gateway_io(host, args.gateway_pid)
                        result = run(clients[0], query)
                        after = gateway_io(host, args.gateway_pid)
                        result.update(
                            dataset=dataset,
                            start_date=start.isoformat(),
                            end_date=end.isoformat(),
                            repeat=repeat,
                            gateway_disk_read_bytes=after.get("read_bytes", 0)
                            - before.get("read_bytes", 0),
                        )
                        save(result)
            assert not list(root.glob("*/objects/**/*.parquet")), (
                "direct unexpectedly persisted data"
            )
            output["no_persistent_parquet"] = True
            output["completed_at"] = dt.datetime.now().astimezone().isoformat()
            args.output.write_text(
                json.dumps(output, ensure_ascii=False, indent=2) + "\n"
            )
        finally:
            for client in clients:
                client.close()


if __name__ == "__main__":
    main()
