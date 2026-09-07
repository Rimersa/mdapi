from __future__ import annotations

import concurrent.futures
import contextlib
import datetime as dt
import hashlib
import http.client
import socket
import urllib.parse
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_api.catalog import CatalogStore, ObjectEntry, catalog_shard_path
from market_data_api.gateway import MarketDataGatewayHandler
from market_data_api.local_api import create_app
from market_data_api.model import DataRequest
from market_data_api.sdk import MarketDataAPIError, MarketDataClient
from market_data_api.selective import ReadOptions, coalesce_ranges
from market_data_api.service import DataService, ServiceLimits
from test_integration import _start_gateway


@pytest.fixture
def source(tmp_path):
    root = tmp_path / "source"
    store = CatalogStore.initialize_for_write(root)
    entries = []
    for minute in (15, 20):
        start = dt.datetime(2026, 9, 4, 9, minute)
        rows = 64 * 512
        times = [start + dt.timedelta(milliseconds=i % 512) for i in range(rows)]
        table = pa.table(
            {
                "symbol": pa.array(
                    [f"S{i // 512:04d}" for i in range(rows)], type=pa.large_string()
                ),
                "event_time": pa.array(times, type=pa.timestamp("us")),
                "time_int": [
                    91_500_000 + (minute - 15) * 100000 + i % 512 for i in range(rows)
                ],
                "value": [i for i in range(rows)],
                "unused": [
                    hashlib.sha256(str(i).encode()).hexdigest() for i in range(rows)
                ],
            }
        )
        path = root / f"bucket-{minute}.parquet"
        pq.write_table(table, path, row_group_size=512, compression="zstd")
        meta = pq.read_metadata(path)
        entries.append(
            ObjectEntry(
                object_id=f"bucket-{minute}",
                dataset="snapshots",
                trade_date="2026-09-04",
                bucket_start=f"2026-09-04T09:{minute}:00.000+08:00",
                bucket_end=f"2026-09-04T09:{minute + 5}:00.000+08:00",
                relative_path=path.name,
                bytes=path.stat().st_size,
                rows=rows,
                uncompressed_bytes=sum(
                    meta.row_group(i).total_byte_size
                    for i in range(meta.num_row_groups)
                ),
                source_fingerprint="test",
                version="r1",
            )
        )
    store.replace_partition("snapshots", "2026-09-04", entries)
    return root, entries


@contextlib.contextmanager
def running(source, tmp_path, *, handler=MarketDataGatewayHandler, tokens=None):
    root, _ = source
    server, thread = _start_gateway(
        root, handler=handler, user_tokens=tokens, auth_required=bool(tokens)
    )
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=server.server_port,
        gateway_token=next(iter(tokens.values())) if tokens else None,
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(
            user_cores=2,
            read_options=ReadOptions(coalesce_gap_bytes=0, seek_cost_bytes=0),
        ),
    )
    try:
        yield service, server
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(3)


def query(**kwargs):
    raw = dict(
        dataset="snapshots",
        start="2026-09-04T09:15:00.100+08:00",
        end="2026-09-04T09:25:00+08:00",
        symbols=["S0002", "S0020"],
        columns=["value", "event_time"],
    )
    raw.update(kwargs)
    return DataRequest.from_values(**raw)


def read(service, request):
    plan, batches = service.batches(request)
    return plan, pa.Table.from_batches(list(batches))


def test_selective_exact_rows_columns_and_bytes(source, tmp_path):
    before = {
        p.name: (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
        for p in source[0].glob("*.parquet")
    }
    with running(source, tmp_path) as (service, _):
        full_plan, full = read(service, query(read_strategy="sequential"))
        partial_plan, partial = read(service, query(read_strategy="ranges"))
        assert partial.equals(full)
        assert partial.column_names == ["value", "event_time"]
        assert partial.num_rows == 2 * (512 - 100) + 2 * 512
        assert partial_plan.stats.transfer_bytes < full_plan.stats.transfer_bytes / 10
        _, cached = read(service, query(mode="cache"))
        assert cached.equals(partial)
        again, _ = read(service, query())
        assert again.stats.metadata_bytes == 0
    after = {
        p.name: (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
        for p in source[0].glob("*.parquet")
    }
    assert after == before


def test_empty_stock_keeps_schema(source, tmp_path):
    with running(source, tmp_path) as (service, _):
        plan, table = read(service, query(symbols=["DOES_NOT_EXIST"]))
        assert table.num_rows == 0
        assert table.column_names == ["value", "event_time"]
        assert plan.stats.transfer_bytes == 0
        assert plan.stats.skipped_objects == 2


def test_http_validation_before_success_headers(source, tmp_path):
    from fastapi.testclient import TestClient

    with running(source, tmp_path) as (service, _):
        client = TestClient(create_app(service), raise_server_exceptions=False)
        payload = dict(
            dataset="snapshots",
            start="2026-09-04T09:15:00+08:00",
            end="2026-09-04T09:20:00+08:00",
        )
        assert (
            client.post("/v1/data", json=dict(payload, stocks=["S0002"])).status_code
            == 422
        )
        assert (
            client.post(
                "/v1/data", json=dict(payload, columns=["bad_column"])
            ).status_code
            == 422
        )
        assert (
            client.post("/v1/data", json=dict(payload, symbols=[])).status_code == 422
        )
        response = client.post(
            "/v1/data", json=dict(payload, symbols=["NO_MATCH"], columns=["symbol"])
        )
        assert response.status_code == 200
        assert pa.ipc.open_stream(response.content).read_all().num_rows == 0


def test_standard_http_ranges_auth_bounds_and_version(source, tmp_path):
    with running(source, tmp_path, tokens={"a": "test-token"}) as (_, server):
        entry = source[1][0]
        url = "/v1/object?" + urllib.parse.urlencode(
            {
                k: getattr(entry, k)
                for k in ["object_id", "dataset", "trade_date", "version"]
            }
        )
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
        auth = {"Authorization": "Bearer test-token"}
        connection.request("GET", url)
        response = connection.getresponse()
        assert response.status == 401
        response.read()
        for header, status, expected in [
            ({"Range": "bytes=0-3"}, 206, b"PAR1"),
            ({"Range": "bytes=-4"}, 206, b"PAR1"),
            ({"Range": "bytes=999999999999-"}, 416, b""),
            ({"If-Match": '"wrong-version"'}, 412, None),
        ]:
            connection.request("GET", url, headers=dict(auth, **header))
            response = connection.getresponse()
            body = response.read()
            assert response.status == status
            if expected is not None:
                assert body == expected
        connection.close()


def test_range_transfer_recovers_without_duplicate_rows(source, tmp_path):
    class DropOnce(MarketDataGatewayHandler):
        calls = 0

        def _sendfile_ranges(self, path, ranges):
            type(self).calls += 1
            if type(self).calls == 2:
                with path.open("rb") as handle:
                    handle.seek(ranges[0][0])
                    self.wfile.write(handle.read(8))
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                raise BrokenPipeError("injected disconnect")
            return super()._sendfile_ranges(path, ranges)

    with running(source, tmp_path, handler=DropOnce) as (service, _):
        plan, result = read(service, query())
        assert plan.stats.retries == 1
        assert result.num_rows == 1848
        assert DropOnce.calls == 3


def test_four_users_native_clients(source, tmp_path):
    tokens = {f"user{i}": f"token{i}" for i in range(4)}
    with running(source, tmp_path, tokens=tokens) as (_, server):

        def worker(i):
            with MarketDataClient.connect(
                gateway_host="127.0.0.1",
                gateway_port=server.server_port,
                gateway_token=f"token{i}",
                cache_root=tmp_path / f"native-{i}",
                cores=1,
                read_options={"coalesce_gap_bytes": 0, "seek_cost_bytes": 0},
            ) as client:
                table = client.read_table(
                    {
                        "dataset": "snapshots",
                        "start_date": "2026-09-04",
                        "end_date": "2026-09-04",
                        "daily_start": "09:15",
                        "daily_end": "09:25",
                        "symbols": [f"S{i:04d}"],
                        "columns": ["symbol", "value"],
                    }
                )
                assert set(table.column("symbol").to_pylist()) == {f"S{i:04d}"}
                assert client.last_read_stats["returned_rows"] == 1024
                return table.num_rows

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(worker, i) for i in range(4)]
            assert [f.result(timeout=15) for f in futures] == [1024] * 4
        assert server.gateway_state.stream_scheduler.snapshot()["active_streams"] == 0


def test_missing_published_version_is_not_silently_skipped(source):
    root, entries = source
    catalog_shard_path(root, "snapshots", "2026-09-04", "r1").unlink()
    with pytest.raises(FileNotFoundError, match="current"):
        CatalogStore(root).selected(query())


def test_coalescing_and_query_validation():
    assert coalesce_ranges([(30, 10), (0, 10), (12, 10)], 2) == ((0, 22), (30, 10))
    with pytest.raises(ValueError):
        query(columns=[])
    with pytest.raises(ValueError):
        query(symbols="S0001")
    with pytest.raises(ValueError):
        DataRequest.from_query(
            dict(
                dataset="snapshots",
                start="2026-09-04",
                end="2026-09-05",
                stocks=["S0001"],
            )
        )


def test_seek_cost_fallback_preserves_results(source, tmp_path):
    with running(source, tmp_path) as (service, _):
        _, expected = read(
            service, query(symbols=None, columns=["value"], read_strategy="sequential")
        )
        service.limits = replace(
            service.limits,
            read_options=ReadOptions(coalesce_gap_bytes=0, seek_cost_bytes=10**9),
        )
        plan, actual = read(service, query(symbols=None, columns=["value"]))
        assert actual.equals(expected)
        assert plan.stats.sequential_objects == 2
        assert plan.stats.transfer_bytes == sum(e.bytes for e in source[1])


def test_closing_consumer_releases_connection_and_memory(source, tmp_path):
    with running(source, tmp_path) as (service, server):
        _, batches = service.batches(query())
        assert next(batches).num_rows > 0
        batches.close()
        assert service.pool._queue.qsize() == service.remote_connections
        assert service.memory_gate._available == service.memory_gate.capacity
        assert server.gateway_state.stream_scheduler.snapshot()["active_streams"] == 0


def test_simultaneous_cache_misses_share_fill(source, tmp_path, monkeypatch):
    import market_data_api.service as service_module

    calls = []
    original = service_module.fetch_cache_objects

    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(service_module, "fetch_cache_objects", counted)
    with running(source, tmp_path) as (service, _):
        request = query(mode="cache")
        plans = [service.preflight(request), service.preflight(request)]

        def consume(plan):
            _, batches = service.batches(request, plan)
            return sum(b.num_rows for b in batches)

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = [pool.submit(consume, p) for p in plans]
            assert [f.result(timeout=10) for f in results] == [1848, 1848]
        assert len(calls) == 1


def test_storage_profiles_and_native_strict_query(source, tmp_path):
    assert (
        ReadOptions.for_profile("ssd").seek_cost_bytes
        < ReadOptions.for_profile("hdd").seek_cost_bytes
    )
    with pytest.raises(ValueError):
        ReadOptions.for_profile("invalid")
    with running(source, tmp_path) as (_, server):
        with MarketDataClient.connect(
            gateway_host="127.0.0.1",
            gateway_port=server.server_port,
            gateway_token="",
            cache_root=tmp_path / "native",
            cores=1,
        ) as client:
            with pytest.raises(MarketDataAPIError) as caught:
                client.read_table({"dataset": "snapshots", "stocks": ["S0001"]})
            assert caught.value.status == 422


def test_persistent_metadata_cache_never_writes_source(source, tmp_path, monkeypatch):
    from market_data_api.range_gateway import FooterCache, FooterStore
    from market_data_api.gateway import SelectedObject
    from pathlib import Path

    root, entries = source
    item = SelectedObject(entries[0], root / entries[0].relative_path, b"")
    database = tmp_path / "service-cache" / "footers.sqlite3"
    first = FooterCache(store=FooterStore(database)).get(item)
    original_open = Path.open

    def block_data_open(path, *args, **kwargs):
        if path == item.path:
            raise AssertionError("warm metadata should not reopen the data file")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", block_data_open)
    assert FooterCache(store=FooterStore(database)).get(item) == first


def test_gateway_rejects_metadata_cache_under_data_root(source):
    from market_data_api.gateway import GatewayState

    root, _ = source
    with pytest.raises(ValueError, match="行情数据根目录之外"):
        GatewayState(
            root,
            token=None,
            max_streams=2,
            max_objects=100,
            queue_timeout=1,
            metadata_index=root / "forbidden.sqlite3",
        )
    assert not (root / "forbidden.sqlite3").exists()
