"""Loopback acceptance on real daily files. Never deploys or restarts any service."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
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


def client_run(args):
    import psutil
    from market_data_api import MarketDataClient

    query = json.loads(Path(args.query).read_text())
    process = psutil.Process()
    gateway = psutil.Process(args.gateway_pid)
    peak = {"client": process.memory_info().rss, "gateway": gateway.memory_info().rss}
    stop = threading.Event()

    def sample():
        while not stop.wait(0.01):
            peak["client"] = max(peak["client"], process.memory_info().rss)
            peak["gateway"] = max(peak["gateway"], gateway.memory_info().rss)

    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    digest, rows, arrow_bytes = hashlib.sha256(), 0, 0
    first = None
    began = time.perf_counter()
    try:
        with MarketDataClient.connect(
            gateway_host="127.0.0.1",
            gateway_port=args.port,
            gateway_token="points-local-test",
            cores=2,
            cache_root=Path(args.work) / "client-cache",
        ) as client:
            for batch in client.iter_batches(query):
                if first is None:
                    first = time.perf_counter() - began
                rows += batch.num_rows
                arrow_bytes += batch.nbytes
                digest.update(batch.column("volume").to_numpy().tobytes())
            seconds = time.perf_counter() - began
            stats = client.last_read_stats
            coverage = stats.pop("coverage")
            stats["coverage_dates"] = [d["date"] for d in coverage["days"]]
            stats["dates_without_files_count"] = len(coverage["dates_without_files"])
    finally:
        stop.set()
        monitor.join()
    print(
        json.dumps(
            {
                "query": query,
                "seconds": seconds,
                "first_batch_seconds": first,
                "rows": rows,
                "arrow_bytes": arrow_bytes,
                "volume_sequence_sha256": digest.hexdigest(),
                "peak_rss_bytes": peak,
                "stats": stats,
            }
        )
    )


def oracle(sources, query):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    digest, rows = hashlib.sha256(), 0
    for folder in sorted(
        sources, key=lambda p: json.loads((p / "day.json").read_text())["day"]
    ):
        day = json.loads((folder / "day.json").read_text())["day"]
        if not query["start_date"] <= day <= query["end_date"]:
            continue
        columns = ["volume", "symbol"] if query.get("symbols") else ["volume"]
        for batch in pq.ParquetFile(folder / "points.parquet").iter_batches(
            columns=columns, batch_size=131072
        ):
            if query.get("symbols"):
                batch = batch.filter(
                    pc.is_in(
                        batch.column("symbol"), value_set=pa.array(query["symbols"])
                    )
                )
            rows += batch.num_rows
            digest.update(batch.column("volume").to_numpy().tobytes())
    return rows, digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, action="append")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--query")
    parser.add_argument("--port", type=int)
    parser.add_argument("--gateway-pid", type=int)
    args = parser.parse_args()
    if args.query:
        client_run(args)
        return
    assert args.source and args.output and args.work
    args.work.mkdir(parents=True, exist_ok=False)
    from market_data_api.catalog import CatalogStore

    CatalogStore.initialize_for_write(args.work / "ticks")
    before, dates = {}, []
    for source in args.source:
        receipt = json.loads((source / "day.json").read_text())
        day = receipt["day"]
        dates.append(day)
        target = args.work / "points" / ("trade_date=" + day)
        target.mkdir(parents=True)
        original = source / "points.parquet"
        before[str(original)] = [
            original.stat().st_ino,
            original.stat().st_size,
            original.stat().st_mtime_ns,
        ]
        os.link(original, target / "points.parquet")
        for name in ["day.json", "quality.json"]:
            if (source / name).exists():
                shutil.copy2(source / name, target / name)
    dates.sort()
    gateway = args.work / "gateway.pyz"
    subprocess.run(
        [sys.executable, str(ROOT / "tools/build_gateway_pyz.py"), str(gateway)],
        check=True,
    )
    log_path = args.work / "gateway.log"
    with log_path.open("w") as log:
        server = subprocess.Popen(
            [
                "/usr/bin/python3",
                str(gateway),
                "--root",
                str(args.work / "ticks"),
                "--points-root",
                str(args.work / "points"),
                "--host",
                "127.0.0.1",
                "--port",
                "0",
                "--token",
                "points-local-test",
                "--metadata-index",
                str(args.work / "footers.sqlite3"),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            for _ in range(200):
                lines = log_path.read_text().splitlines()
                if lines and lines[0].startswith("{"):
                    ready = json.loads(lines[0])
                    break
                if server.poll() is not None:
                    raise RuntimeError(log_path.read_text())
                time.sleep(0.05)
            else:
                raise RuntimeError("Isolated gateway did not start")
            base = dict(dataset="flow_points", start_date=dates[-1], end_date=dates[-1])
            cases = [
                ("full_market_one_day", dict(base)),
                ("one_stock", dict(base, symbols=STOCKS[:1])),
                (
                    "one_stock_sequential",
                    dict(base, symbols=STOCKS[:1], read_strategy="sequential"),
                ),
                (
                    "ten_stocks_four_columns",
                    dict(
                        base,
                        symbols=STOCKS,
                        columns=["symbol", "time", "amount", "volume"],
                    ),
                ),
                ("full_market_two_files", dict(base, start_date=dates[0])),
            ]
            results, expected = [], {}
            for label, query in cases:
                query_path = args.work / (label + ".query.json")
                query_path.write_text(json.dumps(query))
                measured = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--query",
                        str(query_path),
                        "--port",
                        str(ready["port"]),
                        "--gateway-pid",
                        str(server.pid),
                        "--work",
                        str(args.work),
                    ],
                    text=True,
                    capture_output=True,
                    check=True,
                )
                result = json.loads(measured.stdout)
                key = (
                    query["start_date"],
                    query["end_date"],
                    tuple(query.get("symbols", [])),
                )
                if key not in expected:
                    expected[key] = oracle(args.source, query)
                assert (result["rows"], result["volume_sequence_sha256"]) == expected[
                    key
                ], label
                result.update(case=label, oracle_equal=True)
                results.append(result)
                print(
                    json.dumps(
                        {
                            k: result[k]
                            for k in [
                                "case",
                                "rows",
                                "seconds",
                                "first_batch_seconds",
                                "peak_rss_bytes",
                                "oracle_equal",
                            ]
                        }
                    ),
                    flush=True,
                )
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    after = {
        str(p): [p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns]
        for p in [folder / "points.parquet" for folder in args.source]
    }
    assert before == after
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "kind": "local-loopback-real-files",
                "source_files": before,
                "python": sys.version,
                "cores": 2,
                "cache_note": "OS cache not cleared; one fresh client process per case",
                "only_these_dates_mounted": dates,
                "sources_unchanged": True,
                "server_stopped": server.poll() is not None,
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
