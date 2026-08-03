from __future__ import annotations

import datetime as dt
import json

import pytest

from market_data_api.catalog import (
    CATALOG_INDEX_FORMAT,
    Catalog,
    CatalogStore,
    ObjectEntry,
    atomic_write_json,
    catalog_shard_path,
    migrate_legacy_catalog,
)
from market_data_api.model import DataRequest, FetchMode, floor_bucket, parse_datetime


def test_parse_datetime_defaults_to_shanghai():
    value = parse_datetime("2026-05-29T09:15:00")
    assert value.utcoffset() == dt.timedelta(hours=8)
    assert value.isoformat() == "2026-05-29T09:15:00+08:00"


def test_request_is_half_open_and_validated():
    request = DataRequest.from_values(
        dataset="orders",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
        mode="cache",
    )
    assert request.mode is FetchMode.CACHE
    assert floor_bucket(request.start) == request.start
    with pytest.raises(ValueError):
        DataRequest.from_values(
            dataset="orders",
            start="2026-05-29T09:20:00+08:00",
            end="2026-05-29T09:20:00+08:00",
        )


def test_catalog_selects_overlapping_buckets_only():
    first = ObjectEntry(
        object_id="a",
        dataset="orders",
        trade_date="2026-05-29",
        bucket_start="2026-05-29T09:15:00.000+08:00",
        bucket_end="2026-05-29T09:20:00.000+08:00",
        relative_path="a.parquet",
        bytes=10,
        rows=1,
        uncompressed_bytes=20,
        source_fingerprint="f",
        version="v",
    )
    second = ObjectEntry(
        **{
            **first.as_dict(),
            "object_id": "b",
            "bucket_start": "2026-05-29T09:20:00.000+08:00",
            "bucket_end": "2026-05-29T09:25:00.000+08:00",
            "relative_path": "b.parquet",
        }
    )
    catalog = Catalog(generated_at="now", objects=[first, second])
    exact_first = DataRequest.from_values(
        dataset="orders",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
    )
    assert [item.object_id for item in catalog.selected(exact_first)] == ["a"]


def test_catalog_supports_repeated_daily_window():
    entries = []
    for day in ("2026-05-28", "2026-05-29"):
        for hhmm in ("09:15", "09:20"):
            start = f"{day}T{hhmm}:00.000+08:00"
            minute = 20 if hhmm == "09:15" else 25
            end = f"{day}T09:{minute:02d}:00.000+08:00"
            entries.append(
                ObjectEntry(
                    object_id=f"{day}-{hhmm}",
                    dataset="orders",
                    trade_date=day,
                    bucket_start=start,
                    bucket_end=end,
                    relative_path=f"{day}/{hhmm}.parquet",
                    bytes=10,
                    rows=1,
                    uncompressed_bytes=20,
                    source_fingerprint="f",
                    version="v",
                )
            )
    request = DataRequest.from_values(
        dataset="orders",
        start="2026-05-28T00:00:00+08:00",
        end="2026-05-30T00:00:00+08:00",
        daily_start="09:15:00",
        daily_end="09:20:00",
    )
    selected = Catalog(generated_at="now", objects=entries).selected(request)
    assert [item.object_id for item in selected] == [
        "2026-05-28-09:15",
        "2026-05-29-09:15",
    ]


def _catalog_entry(
    trade_date: str,
    *,
    dataset: str = "snapshots",
    minute: int = 15,
) -> ObjectEntry:
    start = f"{trade_date}T09:{minute:02d}:00.000+08:00"
    end = f"{trade_date}T09:{minute + 5:02d}:00.000+08:00"
    return ObjectEntry(
        object_id=f"{dataset}-{trade_date}-{minute}",
        dataset=dataset,
        trade_date=trade_date,
        bucket_start=start,
        bucket_end=end,
        relative_path=f"objects/{dataset}/{trade_date}/{minute}.parquet",
        bytes=100,
        rows=10,
        uncompressed_bytes=400,
        source_fingerprint="fingerprint",
        version="v1",
    )


def test_sharded_catalog_keeps_root_constant_and_loads_only_requested_days(
    tmp_path,
):
    store = CatalogStore.initialize_for_write(tmp_path)
    first_day = dt.date(2020, 1, 1)
    for offset in range(40):
        trade_date = (first_day + dt.timedelta(days=offset)).isoformat()
        store.replace_partition(
            "snapshots",
            trade_date,
            [_catalog_entry(trade_date)],
        )

    root_raw = json.loads((tmp_path / "catalog.json").read_text())
    assert root_raw["format"] == CATALOG_INDEX_FORMAT
    assert "objects" not in root_raw
    assert (tmp_path / "catalog.json").stat().st_size < 512

    unrelated = catalog_shard_path(tmp_path, "orders", "1999-01-01")
    unrelated.parent.mkdir(parents=True, exist_ok=True)
    unrelated.write_text("not-json", encoding="utf-8")

    reader = CatalogStore(tmp_path, shard_cache_size=3)
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2020-01-20T09:15:00+08:00",
        end="2020-01-20T09:20:00+08:00",
    )
    assert [item.object_id for item in reader.selected(request)] == [
        "snapshots-2020-01-20-15"
    ]
    assert reader.cached_shards == 1

    wide = DataRequest.from_values(
        dataset="snapshots",
        start="2020-01-01T09:15:00+08:00",
        end="2020-02-10T09:20:00+08:00",
    )
    assert len(reader.selected(wide)) == 40
    assert reader.cached_shards == 3


def test_legacy_catalog_migration_preserves_objects_and_is_atomic_metadata_only(
    tmp_path,
):
    sentinel = tmp_path / "objects" / "sentinel.parquet"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_bytes(b"immutable-parquet-placeholder")
    before = (sentinel.stat().st_size, sentinel.stat().st_mtime_ns)
    entries = [
        _catalog_entry("2026-05-28"),
        _catalog_entry("2026-05-29"),
        _catalog_entry("2026-05-29", dataset="orders"),
    ]
    legacy = Catalog(generated_at="legacy", objects=entries)
    atomic_write_json(tmp_path / "catalog.json", legacy.as_dict())

    result = migrate_legacy_catalog(tmp_path, keep_backup=True)
    assert result["status"] == "migrated"
    assert result["shards"] == 3
    assert result["objects"] == 3
    assert json.loads((tmp_path / "catalog.json").read_text())["format"] == (
        CATALOG_INDEX_FORMAT
    )
    assert Catalog.load(tmp_path / "catalog.v1.backup.json").objects == entries
    assert (sentinel.stat().st_size, sentinel.stat().st_mtime_ns) == before

    store = CatalogStore(tmp_path)
    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-28T00:00:00+08:00",
        end="2026-05-30T00:00:00+08:00",
    )
    assert store.selected(request) == entries[:2]
    references = [
        {
            "object_id": entries[1].object_id,
            "dataset": entries[1].dataset,
            "trade_date": entries[1].trade_date,
            "version": entries[1].version,
        }
    ]
    assert store.selected_refs(references) == [entries[1]]
    assert migrate_legacy_catalog(tmp_path)["status"] == "already_v2"


def test_sharded_catalog_keeps_old_version_addressable_during_hot_update(
    tmp_path,
):
    store = CatalogStore.initialize_for_write(tmp_path)
    first = _catalog_entry("2026-05-29")
    assert store.replace_partition("snapshots", "2026-05-29", [first])
    old_reference = {
        "object_id": first.object_id,
        "dataset": first.dataset,
        "trade_date": first.trade_date,
        "version": first.version,
    }
    second = ObjectEntry.from_dict(
        {
            **first.as_dict(),
            "object_id": "snapshots-2026-05-29-v2",
            "relative_path": "objects/snapshots/2026-05-29/v2.parquet",
            "version": "v2",
        }
    )
    assert store.replace_partition("snapshots", "2026-05-29", [second])

    request = DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
    )
    assert store.selected(request) == [second]
    assert store.selected_refs([old_reference]) == [first]
    assert catalog_shard_path(
        tmp_path, "snapshots", "2026-05-29", "v1"
    ).is_file()
    assert catalog_shard_path(
        tmp_path, "snapshots", "2026-05-29", "v2"
    ).is_file()

    conflicting = ObjectEntry.from_dict(
        {
            **second.as_dict(),
            "object_id": "same-version-different-content",
        }
    )
    with pytest.raises(ValueError, match="不可变catalog版本内容冲突"):
        store.replace_partition("snapshots", "2026-05-29", [conflicting])
