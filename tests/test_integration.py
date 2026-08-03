from __future__ import annotations

import datetime as dt
import concurrent.futures
import hashlib
import io
import json
import socket
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_api.catalog import (
    CATALOG_INDEX_FORMAT,
    Catalog,
    CatalogStore,
    ObjectEntry,
    atomic_write_json,
    migrate_legacy_catalog,
)
from market_data_api.client import (
    GatewayConnection,
    GatewayError,
    NetworkTransferError,
)
from market_data_api.gateway import (
    GatewayState,
    MarketDataGatewayHandler,
    MarketDataGatewayServer,
)
from market_data_api.model import DataRequest
from market_data_api.service import DataService, ServiceLimits


def _write_bucket(
    root: Path,
    *,
    name: str,
    bucket_start: dt.datetime,
    seconds: list[int],
    version: str = "source-a",
) -> ObjectEntry:
    path = root / "objects" / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    times = [
        (bucket_start + dt.timedelta(seconds=value)).replace(tzinfo=None)
        for value in seconds
    ]
    table = pa.table(
        {
            "symbol": [f"S{value}" for value in seconds],
            "event_time": pa.array(times, type=pa.timestamp("us")),
            "time_int": [
                value.hour * 10_000_000
                + value.minute * 100_000
                + value.second * 1_000
                for value in times
            ],
            "value": seconds,
        }
    )
    pq.write_table(table, path, compression="zstd")
    metadata = pq.ParquetFile(path).metadata
    uncompressed = sum(
        metadata.row_group(i).total_byte_size
        for i in range(metadata.num_row_groups)
    )
    end = bucket_start + dt.timedelta(minutes=5)
    object_id = hashlib.sha256(name.encode()).hexdigest()
    return ObjectEntry(
        object_id=object_id,
        dataset="snapshots",
        trade_date=bucket_start.date().isoformat(),
        bucket_start=bucket_start.isoformat(timespec="milliseconds"),
        bucket_end=end.isoformat(timespec="milliseconds"),
        relative_path=str(path.relative_to(root)),
        bytes=path.stat().st_size,
        rows=table.num_rows,
        uncompressed_bytes=uncompressed,
        source_fingerprint=version,
        version=version,
    )


def _start_gateway(
    root: Path,
    *,
    user_tokens: dict[str, str] | None = None,
    auth_required: bool = False,
    token_file: Path | None = None,
    handler=MarketDataGatewayHandler,
    max_objects: int = 100,
):
    state = GatewayState(
        root,
        token=None,
        user_tokens=user_tokens,
        auth_required=auth_required,
        token_file=token_file,
        max_streams=2,
        max_objects=max_objects,
        queue_timeout=5,
    )
    server = MarketDataGatewayServer(
        ("127.0.0.1", 0),
        handler,
        state,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _drop_once_handler(drop_on_send: int):
    class DropOnceHandler(MarketDataGatewayHandler):
        sent_paths: list[str] = []
        send_count = 0
        dropped = False
        fault_lock = threading.Lock()

        def _sendfile(self, path: Path) -> None:
            with self.fault_lock:
                type(self).send_count += 1
                current = type(self).send_count
                type(self).sent_paths.append(path.name)
                should_drop = (
                    not type(self).dropped and current == drop_on_send
                )
                if should_drop:
                    type(self).dropped = True
            if not should_drop:
                return super()._sendfile(path)
            with path.open("rb", buffering=0) as handle:
                partial = handle.read(max(1, path.stat().st_size // 2))
            self.connection.sendall(partial)
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.close_connection = True
            raise BrokenPipeError("测试注入：对象传输中途断网")

    return DropOnceHandler


def _read_arrow(stream) -> pa.Table:
    raw = b"".join(stream)
    return pa.ipc.open_stream(raw).read_all()


def test_direct_and_cache_have_identical_arrow(tmp_path: Path):
    tz = dt.timezone(dt.timedelta(hours=8))
    root = tmp_path / "remote"
    first = _write_bucket(
        root,
        name="0915",
        bucket_start=dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz),
        seconds=[0, 60, 240],
    )
    second = _write_bucket(
        root,
        name="0920",
        bucket_start=dt.datetime(2026, 5, 29, 9, 20, tzinfo=tz),
        seconds=[0, 60, 240],
    )
    catalog = Catalog(generated_at="2026-07-31T00:00:00+00:00", objects=[first, second])
    atomic_write_json(root / "catalog.json", catalog.as_dict())
    server, thread = _start_gateway(root)
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=server.server_address[1],
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(
            max_response_uncompressed=1024**3,
            gateway_connections=2,
            user_cores=2,
        ),
    )
    try:
        direct = DataRequest.from_values(
            dataset="snapshots",
            start="2026-05-29T09:16:00+08:00",
            end="2026-05-29T09:21:00+08:00",
            mode="direct",
        )
        _, direct_stream = service.arrow_stream(direct)
        direct_table = _read_arrow(direct_stream)
        assert direct_table.num_rows == 3

        def concurrent_direct(_):
            _, stream = service.arrow_stream(direct)
            return _read_arrow(stream).num_rows

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            assert list(executor.map(concurrent_direct, range(6))) == [3] * 6

        daily = DataRequest.from_values(
            dataset="snapshots",
            start="2026-05-29T00:00:00+08:00",
            end="2026-05-30T00:00:00+08:00",
            daily_start="09:16:00",
            daily_end="09:21:00",
            mode="direct",
        )
        _, daily_stream = service.arrow_stream(daily)
        assert _read_arrow(daily_stream).equals(direct_table)

        projected = DataRequest.from_values(
            dataset="snapshots",
            start=direct.start,
            end=direct.end,
            columns=["symbol", "value"],
            mode="direct",
        )
        _, projected_stream = service.arrow_stream(projected)
        projected_table = _read_arrow(projected_stream)
        assert projected_table.schema.names == ["symbol", "value"]
        assert projected_table.num_rows == 3

        cached = DataRequest.from_values(
            dataset="snapshots",
            start=direct.start,
            end=direct.end,
            mode="cache",
        )
        cache_plan, cache_stream = service.arrow_stream(cached)
        cache_table = _read_arrow(cache_stream)
        assert cache_plan.missing_cache_bytes == first.bytes + second.bytes
        assert cache_table.equals(direct_table)

        second_plan = service.preflight(cached)
        assert second_plan.missing_cache_bytes == 0
        _, second_stream = service.arrow_stream(cached, second_plan)
        assert _read_arrow(second_stream).equals(direct_table)

        victim = service.cache.selected(cached)[0]
        victim.path.unlink()
        repair_plan = service.preflight(cached)
        expected_repair = sum(
            entry.bytes
            for entry in repair_plan.selection.entries
            if entry.bucket_start == victim.bucket_start
        )
        assert repair_plan.missing_cache_bytes == expected_repair
        _, repair_stream = service.arrow_stream(cached, repair_plan)
        assert _read_arrow(repair_stream).equals(direct_table)

        first_v2 = _write_bucket(
            root,
            name="0915-v2",
            bucket_start=dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz),
            seconds=[120, 180],
            version="source-v2",
        )
        second_v2 = _write_bucket(
            root,
            name="0920-v2",
            bucket_start=dt.datetime(2026, 5, 29, 9, 20, tzinfo=tz),
            seconds=[0, 120],
            version="source-v2",
        )
        catalog.replace_partition(
            "snapshots",
            "2026-05-29",
            [first_v2, second_v2],
        )
        atomic_write_json(root / "catalog.json", catalog.as_dict())

        default_plan = service.preflight(cached)
        assert default_plan.missing_cache_bytes == 0

        changed = DataRequest.from_values(
            dataset="snapshots",
            start=direct.start,
            end=direct.end,
            mode="cache",
            update="if_changed",
        )
        changed_plan = service.preflight(changed)
        assert changed_plan.missing_cache_bytes == first_v2.bytes + second_v2.bytes
        _, changed_stream = service.arrow_stream(changed, changed_plan)
        changed_table = _read_arrow(changed_stream)
        assert changed_table.num_rows == 3
        assert changed_table.column("value").to_pylist() == [120, 180, 0]
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("mode", ["direct", "cache"])
def test_object_transfer_resumes_after_mid_file_disconnect(
    tmp_path: Path,
    mode: str,
):
    tz = dt.timezone(dt.timedelta(hours=8))
    root = tmp_path / "remote"
    first = _write_bucket(
        root,
        name="0915",
        bucket_start=dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz),
        seconds=[0, 60, 240],
    )
    second = _write_bucket(
        root,
        name="0920",
        bucket_start=dt.datetime(2026, 5, 29, 9, 20, tzinfo=tz),
        seconds=[0, 60, 240],
    )
    catalog = Catalog(
        generated_at="2026-07-31T00:00:00+00:00",
        objects=[first, second],
    )
    atomic_write_json(root / "catalog.json", catalog.as_dict())
    handler = _drop_once_handler(drop_on_send=2)
    server, thread = _start_gateway(root, handler=handler)
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=server.server_address[1],
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(
            user_cores=1,
            network_retries=3,
            network_retry_backoff=0,
            object_request_size=12,
        ),
    )
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:25:00+08:00",
        mode=mode,
    )
    try:
        _, stream = service.arrow_stream(request)
        table = _read_arrow(stream)
        assert table.num_rows == 6
        assert table.column("value").to_pylist() == [0, 60, 240, 0, 60, 240]
        assert handler.sent_paths.count(Path(first.relative_path).name) == 1
        assert handler.sent_paths.count(Path(second.relative_path).name) == 2
        if mode == "cache":
            cached = service.cache.selected(request)
            assert {item.object_id for item in cached} == {
                first.object_id,
                second.object_id,
            }
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_cache_keeps_complete_bucket_when_retry_budget_is_exhausted(
    tmp_path: Path,
):
    tz = dt.timezone(dt.timedelta(hours=8))
    root = tmp_path / "remote"
    first = _write_bucket(
        root,
        name="0915",
        bucket_start=dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz),
        seconds=[0, 60],
    )
    second = _write_bucket(
        root,
        name="0920",
        bucket_start=dt.datetime(2026, 5, 29, 9, 20, tzinfo=tz),
        seconds=[0, 60],
    )
    atomic_write_json(
        root / "catalog.json",
        Catalog(
            generated_at="2026-07-31T00:00:00+00:00",
            objects=[first, second],
        ).as_dict(),
    )
    handler = _drop_once_handler(drop_on_send=2)
    server, thread = _start_gateway(root, handler=handler)
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=server.server_address[1],
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(
            user_cores=1,
            network_retries=0,
            network_retry_backoff=0,
        ),
    )
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:25:00+08:00",
        mode="cache",
    )
    try:
        with pytest.raises(NetworkTransferError) as captured:
            service.arrow_stream(request)
        assert captured.value.completed_objects == 1
        assert [item.object_id for item in service.cache.selected(request)] == [
            first.object_id
        ]

        retry_plan = service.preflight(request)
        assert retry_plan.missing_cache_bytes == second.bytes
        _, stream = service.arrow_stream(request, retry_plan)
        assert _read_arrow(stream).num_rows == 4
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_long_selection_is_transferred_in_fair_chunks(tmp_path: Path):
    tz = dt.timezone(dt.timedelta(hours=8))
    root = tmp_path / "remote"
    start = dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz)
    entries = [
        _write_bucket(
            root,
            name=f"bucket-{index:02d}",
            bucket_start=start + dt.timedelta(minutes=5 * index),
            seconds=[0],
        )
        for index in range(15)
    ]
    atomic_write_json(
        root / "catalog.json",
        Catalog(
            generated_at="2026-07-31T00:00:00+00:00",
            objects=entries,
        ).as_dict(),
    )
    # The whole selection exceeds the gateway's per-transfer cap.  Manifest
    # remains metadata-only and the client splits the exact IDs into chunks.
    server, thread = _start_gateway(root, max_objects=5)
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=server.server_address[1],
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(
            user_cores=1,
            object_request_size=4,
            network_retry_backoff=0,
        ),
    )
    request = DataRequest.from_values(
        dataset="snapshots",
        start=start.isoformat(),
        end=(start + dt.timedelta(minutes=75)).isoformat(),
        mode="direct",
    )
    try:
        plan, stream = service.arrow_stream(request)
        assert len(plan.selection.entries) == 15
        assert _read_arrow(stream).num_rows == 15
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gateway_reads_v2_date_shards_and_hot_reloads_new_day(tmp_path: Path):
    tz = dt.timezone(dt.timedelta(hours=8))
    root = tmp_path / "remote"
    first = _write_bucket(
        root,
        name="day1-0915",
        bucket_start=dt.datetime(2026, 5, 28, 9, 15, tzinfo=tz),
        seconds=[0, 60],
    )
    atomic_write_json(
        root / "catalog.json",
        Catalog(
            generated_at="2026-07-31T00:00:00+00:00",
            objects=[first],
        ).as_dict(),
    )
    assert migrate_legacy_catalog(root)["status"] == "migrated"
    assert (root / "catalog.json").stat().st_size < 512
    assert not (root / "catalog.v1.backup.json").exists()

    server, thread = _start_gateway(root)
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=server.server_address[1],
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(
            user_cores=1,
            network_retry_backoff=0,
        ),
    )
    first_request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-28T09:15:00+08:00",
        end="2026-05-28T09:20:00+08:00",
        mode="direct",
    )
    try:
        plan, stream = service.arrow_stream(first_request)
        assert len(plan.selection.entries) == 1
        assert _read_arrow(stream).num_rows == 2
        assert server.gateway_state.catalog_store.format == CATALOG_INDEX_FORMAT

        second = _write_bucket(
            root,
            name="day2-0915",
            bucket_start=dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz),
            seconds=[0, 60, 120],
        )
        writer = CatalogStore(root)
        assert writer.replace_partition(
            "snapshots",
            "2026-05-29",
            [second],
        )

        two_days = DataRequest.from_values(
            dataset="snapshots",
            start="2026-05-28T09:15:00+08:00",
            end="2026-05-29T09:20:00+08:00",
            mode="cache",
        )
        plan, stream = service.arrow_stream(two_days)
        assert len(plan.selection.entries) == 2
        assert _read_arrow(stream).num_rows == 5
        assert (root / "catalog.json").stat().st_size < 512
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_v2_manifest_reference_survives_partition_update(tmp_path: Path):
    tz = dt.timezone(dt.timedelta(hours=8))
    root = tmp_path / "remote"
    first = _write_bucket(
        root,
        name="old-version",
        bucket_start=dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz),
        seconds=[0, 60],
        version="source-a",
    )
    atomic_write_json(
        root / "catalog.json",
        Catalog(generated_at="legacy", objects=[first]).as_dict(),
    )
    migrate_legacy_catalog(root)
    server, thread = _start_gateway(root)
    connection = GatewayConnection(
        "127.0.0.1",
        server.server_address[1],
    )
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
    )
    try:
        old_selection = connection.manifest(request)
        assert [item.object_id for item in old_selection.entries] == [
            first.object_id
        ]

        second = _write_bucket(
            root,
            name="new-version",
            bucket_start=dt.datetime(2026, 5, 29, 9, 15, tzinfo=tz),
            seconds=[120, 180, 240],
            version="source-v2",
        )
        CatalogStore(root).replace_partition(
            "snapshots",
            "2026-05-29",
            [second],
        )

        downloaded: list[io.BytesIO] = []

        def consume(_header, source):
            target = io.BytesIO()
            while source.remaining:
                target.write(source.read(min(source.remaining, 1024 * 1024)))
            target.seek(0)
            downloaded.append(target)

        metrics = connection.consume_objects(old_selection.entries, consume)
        assert metrics.objects == 1
        assert pq.read_table(downloaded[0]).column("value").to_pylist() == [
            0,
            60,
        ]
        current = connection.manifest(request)
        assert [item.object_id for item in current.entries] == [second.object_id]
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_app_can_be_created(tmp_path: Path):
    from market_data_api.local_api import create_app

    root = tmp_path / "remote"
    root.mkdir()
    atomic_write_json(root / "catalog.json", Catalog.empty().as_dict())
    server, thread = _start_gateway(root)
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=server.server_address[1],
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(user_cores=1),
    )
    try:
        app = create_app(service)
        assert app.title == "Market Data API"
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gateway_authenticates_named_user_tokens(tmp_path: Path):
    root = tmp_path / "remote"
    root.mkdir()
    atomic_write_json(root / "catalog.json", Catalog.empty().as_dict())
    server, thread = _start_gateway(
        root,
        user_tokens={"alice": "secret-a", "bob": "secret-b"},
    )
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
    )
    try:
        alice = GatewayConnection(
            "127.0.0.1",
            server.server_address[1],
            token="secret-a",
        )
        assert alice.manifest(request).entries == []
        alice.close()

        wrong = GatewayConnection(
            "127.0.0.1",
            server.server_address[1],
            token="wrong",
        )
        with pytest.raises(GatewayError) as captured:
            wrong.manifest(request)
        assert captured.value.status == 401
        wrong.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gateway_with_empty_token_file_is_locked(tmp_path: Path):
    root = tmp_path / "remote"
    root.mkdir()
    atomic_write_json(root / "catalog.json", Catalog.empty().as_dict())
    server, thread = _start_gateway(
        root,
        user_tokens={},
        auth_required=True,
    )
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
    )
    try:
        connection = GatewayConnection(
            "127.0.0.1",
            server.server_address[1],
        )
        with pytest.raises(GatewayError) as captured:
            connection.manifest(request)
        assert captured.value.status == 401
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gateway_hot_reloads_user_tokens_without_restart(tmp_path: Path):
    root = tmp_path / "remote"
    root.mkdir()
    atomic_write_json(root / "catalog.json", Catalog.empty().as_dict())
    token_file = tmp_path / "users.json"
    atomic_write_json(token_file, {})
    server, thread = _start_gateway(
        root,
        user_tokens={},
        auth_required=True,
        token_file=token_file,
    )
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
    )
    try:
        atomic_write_json(token_file, {"alice": "first-token"})
        alice = GatewayConnection(
            "127.0.0.1",
            server.server_address[1],
            token="first-token",
        )
        assert alice.manifest(request).entries == []

        atomic_write_json(token_file, {"alice": "rotated-token"})
        with pytest.raises(GatewayError) as captured:
            alice.manifest(request)
        assert captured.value.status == 401
        alice.close()

        rotated = GatewayConnection(
            "127.0.0.1",
            server.server_address[1],
            token="rotated-token",
        )
        assert rotated.manifest(request).entries == []
        rotated.close()

        atomic_write_json(token_file, {})
        locked = GatewayConnection(
            "127.0.0.1",
            server.server_address[1],
            token="rotated-token",
        )
        with pytest.raises(GatewayError) as captured:
            locked.manifest(request)
        assert captured.value.status == 401
        locked.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
