from __future__ import annotations

import contextlib
import datetime as dt
import json
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_api.catalog import CatalogStore
from market_data_api.derived import DerivedStore
from market_data_api.gateway import GatewayState, MarketDataGatewayServer
from market_data_api.local_api import create_app
from market_data_api.model import DataRequest, SHANGHAI
from market_data_api.native import RemoteMarketDataClient
from market_data_api.publish import publish_table
from market_data_api.sdk import MarketDataAPIError
from market_data_api.service import DataService
from test_points import QuietHandler


def partition(tmp_path, day, *, extra=False, window=False):
    times = [
        dt.datetime.fromisoformat(day).replace(tzinfo=SHANGHAI)
        + dt.timedelta(minutes=m)
        for m in ([570, 575, 580] if window else [0, 0, 0])
    ]
    t = pa.table(
        dict(
            time=pa.array(times, type=pa.timestamp("ns", tz="Asia/Shanghai")),
            symbol=["000001.SZ", "000002.SZ", "000003.SZ"],
            tick_volume=pa.array([0, 99, 101], type=pa.int64()),
            reference_volume=[100, 100, 100],
            volume_error_ratio=[-1.0, -0.01, 0.01],
            volume_error_pct=[-100.0, -1.0, 1.0],
        )
    )
    if extra:
        t = t.append_column("buy_amount_3", pa.array([1.0, 2.0, 3.0]))
    p = tmp_path / (
        day + ("-extra" if extra else "") + ("-window" if window else "") + ".parquet"
    )
    pq.write_table(t, p, row_group_size=1)
    return p


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


def test_live_columns_tables_and_typed_history_nulls(tmp_path):
    with running(tmp_path) as (root, client, server):
        publish_table(
            root, "daily_quality", {"2026-09-01": [partition(tmp_path, "2026-09-01")]}
        )
        got = client.read_derived("daily_quality", "2026-09-01", "2026-09-01")
        assert got["volume_error_pct"].to_pylist() == [-100.0, -1.0, 1.0]
        assert client.tables()["tables"][0]["name"] == "daily_quality"
        # Same server and same client instance must discover the new column.
        publish_table(
            root,
            "daily_quality",
            {"2026-09-02": [partition(tmp_path, "2026-09-02", extra=True)]},
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
        assert (
            empty.num_rows == 0
            and empty.schema.field("buy_amount_3").type == pa.float64()
        )
        with pytest.raises(MarketDataAPIError, match="不存在这些列"):
            client.read_derived(
                "daily_quality", "2026-09-01", "2026-09-02", columns=["typo"]
            )
        publish_table(
            root,
            "orders_5m",
            {
                "2026-09-03": [
                    partition(tmp_path, "2026-09-03", extra=True, window=True)
                ]
            },
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


def test_pinned_selection_and_incompatible_publish(tmp_path):
    with running(tmp_path) as (root, client, server):
        p = partition(tmp_path, "2026-09-01")
        publish_table(root, "daily_quality", {"2026-09-01": [p]})
        req = DataRequest.from_query(
            dict(
                dataset="derived.daily_quality",
                start_date="2026-09-01",
                end_date="2026-09-01",
            )
        )
        plan = client.service.preflight(req)
        publish_table(
            root,
            "daily_quality",
            {"2026-09-01": [partition(tmp_path, "2026-09-01", extra=True)]},
        )
        _, batches = client.service.batches(req, plan)
        old = pa.Table.from_batches(list(batches))
        assert "buy_amount_3" not in old.column_names
        assert (
            "buy_amount_3"
            in client.read_derived(
                "daily_quality", "2026-09-01", "2026-09-01"
            ).column_names
        )
        before = (root / "daily_quality/table.json").read_bytes()
        bad = (
            pq.ParquetFile(p)
            .read()
            .set_column(2, "tick_volume", pa.array([0.0, 99.0, 101.0]))
        )
        pq.write_table(bad, p)
        with pytest.raises(ValueError, match="Incompatible field type"):
            publish_table(root, "daily_quality", {"2026-09-01": [p]})
        assert (root / "daily_quality/table.json").read_bytes() == before


def test_registry_auth_path_escape_and_mutation(tmp_path):
    with running(tmp_path) as (root, client, server):
        publish_table(
            root, "daily_quality", {"2026-09-01": [partition(tmp_path, "2026-09-01")]}
        )
        import urllib.request, urllib.error

        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/v1/tables")
        assert error.value.code == 401
        req = DataRequest.from_query(
            dict(
                dataset="derived.daily_quality",
                start_date="2026-09-01",
                end_date="2026-09-01",
            )
        )
        store = server.gateway_state.derived_store
        entries = store.selected(req)
        path = store.path_for(entries[0])
        path.write_bytes(path.read_bytes() + b"changed")
        with pytest.raises(ValueError, match="变化"):
            store.path_for(entries[0])
        current = root / "daily_quality/table.json"
        doc = json.loads(current.read_text())
        external = tmp_path / "outside.parquet"
        pq.write_table(pa.table({"x": [1]}), external)
        doc["objects"][0].update(
            relative_path="../outside.parquet", bytes=external.stat().st_size
        )
        current.write_text(json.dumps(doc))
        with pytest.raises(ValueError, match="越界"):
            store.selected(req)
    for name in ["derived../private", "derived.AAA", "derived.a/../../x", "derived."]:
        with pytest.raises(ValueError):
            DataRequest.from_query(
                dict(dataset=name, start_date="2026-09-01", end_date="2026-09-01")
            )


def test_local_http_schema_discovery_and_arrow(tmp_path):
    from fastapi.testclient import TestClient

    with running(tmp_path) as (root, client, server):
        publish_table(
            root, "daily_quality", {"2026-09-01": [partition(tmp_path, "2026-09-01")]}
        )
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


def test_quality_export_signed_zero_and_missing(tmp_path, monkeypatch):
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "quality_export",
        Path(__file__).resolve().parents[1] / "tools/publish_daily_quality.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    original_parquet_file = pq.ParquetFile

    def no_points_recheck(path, *args, **kwargs):
        assert (
            Path(path).name != "points.parquet"
        ), "Quality export must not re-read point values"
        return original_parquet_file(path, *args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", no_points_recheck)
    from test_points import write_day

    root = tmp_path / "points"
    write_day(root, "2026-09-01", empty=True)
    folder = root / "machine=m01/trade_date=2026-09-01"
    cases = [
        (0, 100, "complete"),
        (100, 100, "complete"),
        (101, 100, "complete"),
        (0, 0, "complete"),
        (50, None, "reference_missing"),
    ]
    q = pa.Table.from_pylist(
        [
            dict(
                date="2026-09-01",
                symbol=f"{i:06}.SZ",
                scope="continuous",
                tick_volume=t,
                base_volume=t,
                volume_basis="points_order_mode",
                reference_volume=r,
                reference_status=s,
                volume_diff=[-100, 0, 1, 0, None][i],
                volume_error_ratio=[-1.0, 0.0, 0.01, None, None][i],
            )
            for i, (t, r, s) in enumerate(cases)
        ]
    )
    pq.write_table(q, folder / "quality.parquet")
    from market_data_api.publish import sha256

    (folder / "quality.json").write_text(
        json.dumps(
            dict(
                format="flow-base-continuous-quality-v2",
                quality_parquet_sha256=sha256(folder / "quality.parquet"),
            )
        )
    )
    module.export(root, tmp_path / "derived")
    store = DerivedStore(tmp_path / "derived")
    req = DataRequest.from_query(
        dict(
            dataset="derived.daily_quality",
            start_date="2026-09-01",
            end_date="2026-09-01",
        )
    )
    entry = store.selected(req)[0]
    t = pq.ParquetFile(store.path_for(entry)).read()
    assert t["volume_error_pct"].to_pylist() == [-100.0, 0.0, 1.0, None, None]
    assert t["volume_diff"].to_pylist() == [-100, 0, 1, 0, None]
    assert t.select(q.column_names).equals(q)
    assert t["deviation_status"].to_pylist() == ["available"] * 3 + [
        "zero_reference_volume",
        "reference_unavailable",
    ]
    factors = t.append_column("buy_amount_3", pa.array([1.0, 2.0, 3.0, 4.0, 5.0]))
    path = tmp_path / "with-factors.parquet"
    pq.write_table(factors, path)
    publish_table(tmp_path / "derived", "daily_quality", {"2026-09-01": [path]})
    module.export(root, tmp_path / "derived")
    entry = store.selected(req)[0]
    after = pq.ParquetFile(store.path_for(entry)).read()
    assert after["buy_amount_3"].to_pylist() == [1.0, 2.0, 3.0, 4.0, 5.0]
    with pytest.raises(RuntimeError, match="Table changed"):
        publish_table(
            tmp_path / "derived",
            "daily_quality",
            {"2026-09-01": [path]},
            expected_version="absent",
        )
