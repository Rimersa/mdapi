from __future__ import annotations

import contextlib
import datetime as dt
import json
import threading
import urllib.error
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_api.catalog import CatalogStore
from market_data_api.derived import DerivedStore
from market_data_api.gateway import (
    GatewayState,
    MarketDataGatewayHandler,
    MarketDataGatewayServer,
)
from market_data_api.local_api import create_app
from market_data_api.model import DataRequest, SHANGHAI
from market_data_api.native import RemoteMarketDataClient
from market_data_api.sdk import MarketDataAPIError
from market_data_api.service import DataService
from market_data_api.tables import upsert_daily


class QuietHandler(MarketDataGatewayHandler):
    def log_message(self, *_):
        pass


def quality_table(day, *, extra=False, window=False, values=None):
    if values is None:
        values = [0, 99, 101]
    if window:
        times = [
            dt.datetime.fromisoformat(day).replace(tzinfo=SHANGHAI)
            + dt.timedelta(minutes=minutes)
            for minutes in (570, 575, 580)
        ]
    else:
        times = [dt.datetime.fromisoformat(day).replace(tzinfo=SHANGHAI)] * 3
    table = pa.table(
        dict(
            time=pa.array(times, type=pa.timestamp("us", tz="Asia/Shanghai")),
            symbol=["000001.SZ", "000002.SZ", "000003.SZ"],
            tick_volume=pa.array(values, type=pa.int64()),
            base_volume=pa.array(values, type=pa.int64()),
            reference_volume=pa.array([100, 100, 100], type=pa.int64()),
            volume_error_ratio=[-1.0, -0.01, 0.01],
            volume_error_pct=[-100.0, -1.0, 1.0],
        )
    )
    if extra:
        table = table.append_column("buy_amount_3", pa.array([1.0, 2.0, 3.0]))
    return table


def write_table_rows(root, table, day, rows):
    upsert_daily(root, table, day, pa.Table.from_pylist(rows))


@contextlib.contextmanager
def running(tmp_path):
    ticks = tmp_path / "ticks"
    CatalogStore.initialize_for_write(ticks)
    root = tmp_path / "derived"
    root.mkdir(exist_ok=True)
    state = GatewayState(
        ticks,
        token="test",
        max_streams=2,
        max_objects=100,
        queue_timeout=2,
        derived_root=root,
    )
    server = MarketDataGatewayServer(("127.0.0.1", 0), QuietHandler, state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = RemoteMarketDataClient(
        gateway_host="127.0.0.1",
        gateway_port=server.server_port,
        gateway_token="test",
        cache_root=tmp_path / "cache",
        cores=2,
        network_retry_backoff=0,
    )
    try:
        yield root, client, server
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(3)


def test_direct_columns_tables_and_typed_history_nulls(tmp_path):
    with running(tmp_path) as (root, client, server):
        upsert_daily(root, "daily_quality", "2026-09-01", quality_table("2026-09-01"))
        got = client.read_derived("daily_quality", "2026-09-01", "2026-09-01")
        assert got["volume_error_pct"].to_pylist() == [-100.0, -1.0, 1.0]
        assert client.tables()["tables"][0]["name"] == "daily_quality"
        # Same server and same client instance must discover a later column.
        upsert_daily(
            root,
            "daily_quality",
            "2026-09-02",
            quality_table("2026-09-02", extra=True),
        )
        t = client.read_derived("daily_quality", "2026-09-01", "2026-09-02")
        assert t.num_rows == 6 and t["buy_amount_3"].to_pylist() == [
            None,
            None,
            None,
            1.0,
            2.0,
            3.0,
        ]
        only = client.read_derived(
            "daily_quality",
            "2026-09-01",
            "2026-09-02",
            columns=["buy_amount_3"],
            symbols=["000002.SZ"],
        )
        assert only.to_pydict() == {"buy_amount_3": [None, 2.0]}
        empty = client.read_derived(
            "daily_quality",
            "2026-09-01",
            "2026-09-02",
            columns=["buy_amount_3"],
            symbols=["999999.SZ"],
        )
        assert empty.num_rows == 0
        assert empty.schema.field("buy_amount_3").type == pa.float64()
        with pytest.raises(MarketDataAPIError, match="不存在这些列"):
            client.read_derived(
                "daily_quality", "2026-09-01", "2026-09-02", columns=["typo"]
            )
        # A window table is just another directory; no server/client release needed.
        upsert_daily(
            root,
            "orders_5m",
            "2026-09-03",
            quality_table("2026-09-03", extra=True, window=True),
            granularity="5m",
        )
        assert len(client.tables()["tables"]) == 2
        window = client.read_derived(
            "orders_5m",
            "2026-09-03",
            "2026-09-03",
            daily_start="09:35",
            daily_end="09:40",
        )
        assert window["symbol"].to_pylist() == ["000002.SZ"]
        sequential = client.read_derived(
            "daily_quality", "2026-09-01", "2026-09-02", read_strategy="sequential"
        )
        assert sequential.equals(t)


def test_pinned_plan_rejects_mixed_file_replacements(tmp_path):
    with running(tmp_path) as (root, client, server):
        upsert_daily(root, "daily_quality", "2026-09-01", quality_table("2026-09-01"))
        req = DataRequest.from_query(
            dict(
                dataset="derived.daily_quality",
                start_date="2026-09-01",
                end_date="2026-09-01",
            )
        )
        store = server.gateway_state.derived_store
        entry = store.selected(req)[0]
        path = store.path_for(entry)
        # A writer atomically replaces the file; the old plan must not silently
        # combine the old manifest with new bytes.
        upsert_daily(
            root,
            "daily_quality",
            "2026-09-01",
            quality_table("2026-09-01", values=[5, 6, 7]),
        )
        with pytest.raises(ValueError, match="变化"):
            store.path_for(entry)
        fresh = store.selected(req)[0]
        assert pq.ParquetFile(store.path_for(fresh)).read()["tick_volume"].to_pylist() == [5, 6, 7]
        assert fresh.object_id != entry.object_id


def test_registry_auth_name_validation(tmp_path):
    with running(tmp_path) as (root, client, server):
        upsert_daily(root, "daily_quality", "2026-09-01", quality_table("2026-09-01"))
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/v1/tables")
        assert error.value.code == 401
        for name in ["derived../private", "derived.AAA", "derived.a/../../x", "derived."]:
            with pytest.raises(ValueError):
                DataRequest.from_query(
                    dict(dataset=name, start_date="2026-09-01", end_date="2026-09-01")
                )


def test_upsert_merges_columns_and_window_tables(tmp_path):
    root = tmp_path / "derived"
    upsert_daily(
        root, "daily_quality", "2026-09-01", quality_table("2026-09-01")
    )
    upsert_daily(
        root,
        "daily_quality",
        "2026-09-01",
        pa.table({"symbol": ["000001.SZ", "000002.SZ"], "order_buy_amount_3": [11.5, 22.5]}),
    )
    table = pq.ParquetFile(
        root / "daily_quality/trade_date=2026-09-01/data.parquet"
    ).read()
    assert table["order_buy_amount_3"].to_pylist() == [11.5, 22.5, None]
    assert table["tick_volume"].to_pylist() == [0, 99, 101]
    assert table["time"].to_pylist()[0].date().isoformat() == "2026-09-01"

    upsert_daily(
        root,
        "orders_5m",
        "2026-09-02",
        quality_table("2026-09-02", window=True),
        granularity="5m",
        description="test window table",
    )
    meta = DerivedStore(root).describe("orders_5m")
    assert meta["granularity"] == "5m"
    assert meta["description"] == "test window table"
    assert any(c["name"] == "volume_error_pct" for c in meta["columns"])

    bad = pa.table({"symbol": ["000001.SZ"], "tick_volume": ["not-an-int"]})
    with pytest.raises(ValueError, match="类型"):
        upsert_daily(root, "daily_quality", "2026-09-01", bad)


def test_local_http_schema_discovery_and_arrow(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    with running(tmp_path) as (root, client, server):
        upsert_daily(root, "daily_quality", "2026-09-01", quality_table("2026-09-01"))
        service = DataService(
            gateway_host="127.0.0.1",
            gateway_port=server.server_port,
            gateway_token="test",
            cache_root=tmp_path / "local-cache",
        )
        with TestClient(create_app(service)) as http:
            assert http.get("/v1/tables").json()["tables"][0]["name"] == "daily_quality"
            assert http.get("/v1/tables/daily_quality").json()["granularity"] == "daily"
            response = http.post(
                "/v1/data",
                json=dict(
                    dataset="derived.daily_quality",
                    start_date="2026-09-01",
                    end_date="2026-09-01",
                    symbols=["000001.SZ"],
                ),
            )
            assert response.status_code == 200, response.text
            got = pa.ipc.open_stream(response.content).read_all()
            assert got["volume_error_pct"].to_pylist() == [-100.0]


def test_coverage_keeps_legacy_arrow_schema(tmp_path):
    root = tmp_path / "derived"
    upsert_daily(root, "daily_quality", "2026-09-01", quality_table("2026-09-01"))
    req = DataRequest.from_query(
        dict(
            dataset="derived.daily_quality",
            start_date="2026-09-01",
            end_date="2026-09-01",
        )
    )
    _, coverage = DerivedStore(root).selection(req)
    assert coverage["table"]["columns"]
    assert coverage["table"].get("arrow_schema")
