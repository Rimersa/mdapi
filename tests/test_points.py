from __future__ import annotations

import contextlib
import concurrent.futures
import datetime as dt
import json
import os
import socket
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_api.catalog import CatalogStore
from market_data_api.gateway import (
    GatewayState,
    MarketDataGatewayHandler,
    MarketDataGatewayServer,
)
from market_data_api.local_api import create_app
from market_data_api.model import DataRequest
from market_data_api.native import RemoteMarketDataClient
from market_data_api.points import PointsStore
from market_data_api.sdk import MarketDataAPIError
from market_data_api.service import DataService, ServiceLimits

SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("time", pa.timestamp("ns", tz="Asia/Shanghai")),
        ("active_order_id", pa.int64()),
        ("side", pa.string()),
        ("depth", pa.uint8()),
        ("mode_mask", pa.uint8()),
        ("amount", pa.float64()),
        ("volume", pa.int64()),
        ("order_amount", pa.float64()),
        ("order_volume", pa.int64()),
    ]
)


def write_day(root, day, *, empty=False, status="complete"):
    folder = root / "machine=m01" / ("trade_date=" + day)
    folder.mkdir(parents=True)
    n = 0 if empty else 64 * 256
    start = dt.datetime.fromisoformat(day + "T09:30:00+08:00")
    values = {
        "symbol": [f"{i // 256:06d}.SZ" for i in range(n)],
        "time": [start + dt.timedelta(microseconds=i % 256) for i in range(n)],
        "active_order_id": list(range(n)),
        "side": ["B" if i % 2 else "S" for i in range(n)],
        "depth": [i % 5 + 1 for i in range(n)],
        "mode_mask": [i % 3 + 1 for i in range(n)],
        "amount": [i + 0.01 for i in range(n)],
        "volume": [i + 1 for i in range(n)],
        "order_amount": [i + 10.01 for i in range(n)],
        "order_volume": [i + 10 for i in range(n)],
    }
    table = pa.Table.from_pydict(values, schema=SCHEMA)
    path = folder / "points.parquet"
    pq.write_table(table, path, row_group_size=128, compression="zstd")
    (folder / "day.json").write_text(
        json.dumps(
            {
                "day": day,
                "status": status,
                "output": {"bytes": path.stat().st_size, "rows": n},
            }
        )
    )
    (folder / "quality.json").write_text(json.dumps({"statuses": {"match": 64}}))
    return table


@pytest.fixture
def points(tmp_path):
    root = tmp_path / "points"
    tables = [
        write_day(root, "2026-09-01"),
        write_day(root, "2026-09-03", status="partial"),
        write_day(root, "2026-09-04", empty=True),
    ]
    return root, tables


class QuietHandler(MarketDataGatewayHandler):
    def log_message(self, *_):
        pass


@contextlib.contextmanager
def running(points, tmp_path, handler=QuietHandler):
    ticks = tmp_path / "ticks"
    CatalogStore.initialize_for_write(ticks)
    state = GatewayState(
        ticks,
        token=None,
        user_tokens={"a": "test-a", "b": "test-b"},
        max_streams=2,
        max_objects=100,
        queue_timeout=2,
        points_root=points[0],
    )
    server = MarketDataGatewayServer(("127.0.0.1", 0), handler, state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = RemoteMarketDataClient(
        gateway_host="127.0.0.1",
        gateway_port=server.server_port,
        gateway_token="test-a",
        cache_root=tmp_path / "cache",
        cores=2,
        read_options={"bundle_bytes": 32 * 1024, "coalesce_gap_bytes": 0},
        network_retry_backoff=0,
    )
    try:
        yield client, server
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(3)


def query(**kw):
    return dict(
        dataset="flow_points", start_date="2026-09-01", end_date="2026-09-03", **kw
    )


def test_dates_all_market_coverage_and_unchanged_points(points, tmp_path):
    before = {
        str(p): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in points[0].rglob("*")
        if p.is_file()
    }
    with running(points, tmp_path) as (client, _):
        estimate = client.estimate(query())
        assert estimate["coverage"]["dates_without_files"] == ["2026-09-02"]
        assert estimate["coverage"]["partial_dates"] == ["2026-09-03"]
        got = pa.Table.from_batches(
            list(client.iter_points("2026-09-01", "2026-09-03"))
        )
        assert got.equals(pa.concat_tables(points[1][:2]))
        assert client.last_read_stats["coverage"] == estimate["coverage"]
        assert client.last_read_stats["returned_rows"] == got.num_rows
    assert before == {
        str(p): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in points[0].rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("strategy", ["auto", "ranges", "sequential"])
def test_symbols_columns_and_bounded_parts(points, tmp_path, strategy):
    class Observe(QuietHandler):
        sizes = []

        def _sendfile_ranges(self, path, ranges):
            type(self).sizes.append(sum(n for _, n in ranges))
            return super()._sendfile_ranges(path, ranges)

    with running(points, tmp_path, Observe) as (client, _):
        q = query(
            symbols=["000002.SZ"], columns=["volume", "amount"], read_strategy=strategy
        )
        got = client.read_table(q)
        expected = pa.concat_tables(
            [t.slice(512, 256).select(["volume", "amount"]) for t in points[1][:2]]
        )
        assert got.equals(expected)
        assert max(Observe.sizes) <= 32 * 1024
        if strategy != "sequential":
            assert (
                client.last_read_stats["transfer_bytes"]
                < client.last_read_stats["source_bytes"] / 10
            )


def test_timezone_daily_window_and_half_open_end(points, tmp_path):
    with running(points, tmp_path) as (client, _):
        q = query(
            symbols=["000002.SZ"],
            daily_start="09:30:00.000100",
            daily_end="09:30:00.000200",
        )
        got = client.read_table(q)
        assert got.equals(
            pa.concat_tables([t.slice(512 + 100, 100) for t in points[1][:2]])
        )
        got = client.read_table(
            dict(
                dataset="flow_points",
                symbols=["000002.SZ"],
                start="2026-09-01T01:30:00.000100Z",
                end="2026-09-01T01:30:00.000200Z",
            )
        )
        assert got.equals(points[1][0].slice(612, 100))


def test_empty_day_unknown_symbol_and_invalid_request(points, tmp_path):
    with running(points, tmp_path) as (client, _):
        assert client.read_table(query(symbols=["999999.SZ"])).schema == SCHEMA
        assert client.read_table(query(symbols=["999999.SZ"])).num_rows == 0
        assert (
            client.read_table(
                dict(
                    dataset="flow_points",
                    start_date="2026-09-04",
                    end_date="2026-09-04",
                )
            ).num_rows
            == 0
        )
        for overrides in [
            dict(symbols=[]),
            dict(columns=["not_a_column"]),
            dict(mode="cache"),
        ]:
            with pytest.raises(MarketDataAPIError):
                client.read_table(query(**overrides))
        with pytest.raises(MarketDataAPIError) as caught:
            client.read_table(
                dict(
                    dataset="flow_points",
                    start_date="2025-01-01",
                    end_date="2025-01-02",
                )
            )
        assert caught.value.status == 404


def test_auth_and_http_arrow_stream(points, tmp_path):
    from fastapi.testclient import TestClient

    with running(points, tmp_path) as (client, server):
        unauthorized = RemoteMarketDataClient(
            gateway_host="127.0.0.1",
            gateway_port=server.server_port,
            gateway_token="wrong",
            cache_root=tmp_path / "unauthorized",
        )
        with unauthorized, pytest.raises(MarketDataAPIError):
            unauthorized.read_table(query())
        service = DataService(
            gateway_host="127.0.0.1",
            gateway_port=server.server_port,
            gateway_token="test-a",
            cache_root=tmp_path / "http",
            limits=ServiceLimits(user_cores=2),
        )
        with TestClient(create_app(service)) as http:
            payload = query(symbols=["000002.SZ"], columns=["symbol", "volume"])
            estimate = http.post("/v1/estimate", json=payload)
            assert estimate.status_code == 200
            assert estimate.json()["coverage"]["partial_dates"] == ["2026-09-03"]
            response = http.post("/v1/data", json=payload)
            assert response.status_code == 200
            got = pa.ipc.open_stream(response.content).read_all()
            assert got.equals(client.read_table(payload))


def test_cancel_then_read_and_interrupted_part_retry(points, tmp_path):
    class Drop(QuietHandler):
        dropped = False

        def _sendfile_ranges(self, path, ranges):
            if not type(self).dropped:
                type(self).dropped = True
                with path.open("rb") as handle:
                    handle.seek(ranges[0][0])
                    self.wfile.write(handle.read(min(256, ranges[0][1])))
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                raise BrokenPipeError("deliberate test fault")
            return super()._sendfile_ranges(path, ranges)

    with running(points, tmp_path, Drop) as (client, _):
        got = client.read_table(query())
        assert got.equals(pa.concat_tables(points[1][:2]))
        assert client.last_read_stats["retries"] == 1
        stream = client.iter_batches(query())
        next(stream)
        stream.close()
        assert client.read_table(query(symbols=["000002.SZ"])).num_rows == 512


def test_pinned_version_republication_and_mutation(points):
    root, _ = points
    original = root / "machine=m01/trade_date=2026-09-01"
    immutable = root / ".versions/old"
    immutable.parent.mkdir()
    original.rename(immutable)
    original.symlink_to(immutable)
    store = PointsStore(root)
    entry = store.selected(DataRequest.from_query(query()))[0]
    ref = {
        k: getattr(entry, k) for k in ("object_id", "dataset", "trade_date", "version")
    }
    original.unlink()
    write_day(root, "2026-09-01", empty=True)
    assert store.selected_refs([ref]) == [entry]
    path = immutable / "points.parquet"
    before = path.stat()
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1000000))
    with pytest.raises(ValueError, match="变化"):
        store.selected_refs([ref])


def test_duplicate_and_escape_are_rejected(points, tmp_path):
    root, _ = points
    duplicate = root / "machine=other/trade_date=2026-09-01"
    duplicate.parent.mkdir()
    duplicate.symlink_to(root / "machine=m01/trade_date=2026-09-01")
    with pytest.raises(ValueError, match="重复"):
        PointsStore(root).selected(DataRequest.from_query(query()))
    duplicate.unlink()
    outside = tmp_path / "outside"
    outside.mkdir()
    duplicate.symlink_to(outside)
    (root / "machine=m01/trade_date=2026-09-01").rename(root / "old")
    with pytest.raises(ValueError, match="越界"):
        PointsStore(root).selected(DataRequest.from_query(query()))


def test_two_users_get_exact_results_without_shared_state(points, tmp_path):
    with running(points, tmp_path) as (_, server):

        def read(user, symbol):
            with RemoteMarketDataClient(
                gateway_host="127.0.0.1",
                gateway_port=server.server_port,
                gateway_token="test-" + user,
                cache_root=tmp_path / user,
                cores=1,
                read_options={"bundle_bytes": 16 * 1024},
            ) as client:
                got = client.read_table(query(symbols=[symbol]))
                return got, client.last_read_stats["coverage"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            a, b = [
                future.result()
                for future in [
                    pool.submit(read, "a", "000002.SZ"),
                    pool.submit(read, "b", "000005.SZ"),
                ]
            ]
        assert a[0].equals(pa.concat_tables([t.slice(512, 256) for t in points[1][:2]]))
        assert b[0].equals(
            pa.concat_tables([t.slice(1280, 256) for t in points[1][:2]])
        )
        assert a[1] == b[1]


def test_unconfigured_dataset_and_day_integrity(points, tmp_path):
    ticks = tmp_path / "ticks"
    CatalogStore.initialize_for_write(ticks)
    state = GatewayState(
        ticks, token=None, max_streams=1, max_objects=10, queue_timeout=1
    )
    with pytest.raises(ValueError, match="points-root"):
        state.select(DataRequest.from_query(query()))
    path = points[0] / "machine=m01/trade_date=2026-09-01/day.json"
    receipt = json.loads(path.read_text())
    receipt["output"]["bytes"] += 1
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="不一致"):
        PointsStore(points[0]).selected(DataRequest.from_query(query()))


def test_unpublished_day_and_broken_quality_do_not_block_points(points, tmp_path):
    (points[0] / "machine=m01/trade_date=2026-09-02").mkdir()
    (points[0] / "machine=m01/trade_date=2026-09-01/quality.json").write_text("[]")
    with running(points, tmp_path) as (client, _):
        assert client.read_table(query()).equals(pa.concat_tables(points[1][:2]))
        coverage = client.last_read_stats["coverage"]
        assert coverage["dates_without_files"] == ["2026-09-02"]
        assert coverage["days"][0]["quality_metadata_status"] == "unreadable"


def test_points_does_not_change_tick_publisher_contract(tmp_path):
    from market_data_api.catalog import catalog_shard_path

    assert DataRequest.from_query(query()).dataset == "flow_points"
    with pytest.raises(ValueError, match="未知数据集"):
        catalog_shard_path(tmp_path, "flow_points", "2026-09-01")


def test_http_midstream_error_is_not_a_successful_partial_table():
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from market_data_api.service import _arrow_ipc_pipe
    from market_data_api.sdk import MarketDataClient

    class BrokenArrow(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()

            def batches():
                yield pa.record_batch({"value": [1, 2, 3]})
                raise RuntimeError("deliberate error after first batch")

            try:
                for chunk in _arrow_ipc_pipe(batches(), compression=None):
                    self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
            except RuntimeError:
                self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), BrokenArrow)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(MarketDataAPIError, match="中断|不完整"):
            MarketDataClient(f"http://127.0.0.1:{server.server_port}").read_table(
                query()
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
