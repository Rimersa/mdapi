from __future__ import annotations

from pathlib import Path

import pytest

from market_data_api.catalog import ObjectEntry
from market_data_api.client import Selection
from market_data_api.model import DataRequest, LocalResources
from market_data_api.service import (
    DataService,
    LocalMemoryExhausted,
    RequestRejected,
    RuntimeMemoryMonitor,
    ServiceLimits,
    _arrow_ipc_pipe,
)


GIB = 1024**3


def _large_selection() -> Selection:
    source_bytes = 100 * 1024**2
    entry = ObjectEntry(
        object_id="large",
        dataset="snapshots",
        trade_date="2026-05-29",
        bucket_start="2026-05-29T09:15:00.000+08:00",
        bucket_end="2026-05-29T09:20:00.000+08:00",
        relative_path="unused.parquet",
        bytes=source_bytes,
        rows=1,
        uncompressed_bytes=source_bytes * 4,
        source_fingerprint="source",
        version="v1",
    )
    return Selection(
        entries=[entry],
        source_bytes=source_bytes,
        uncompressed_bytes=source_bytes * 4,
        rows=1,
        catalog_generated_at="now",
    )


def _request() -> DataRequest:
    return DataRequest.from_values(
        dataset="snapshots",
        start="2026-05-29T09:15:00+08:00",
        end="2026-05-29T09:20:00+08:00",
    )


def test_default_has_no_total_response_limit(monkeypatch, tmp_path: Path) -> None:
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=1,
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(user_cores=1),
    )
    monkeypatch.setattr(service, "_selection", lambda _request: _large_selection())
    monkeypatch.setattr(
        "market_data_api.service.detect_local_resources",
        lambda _path: LocalResources(
            cpus=8,
            total_memory=64 * GIB,
            available_memory=60 * GIB,
            free_disk=100 * GIB,
        ),
    )
    try:
        plan = service.preflight(_request())
        assert plan.estimated_arrow_memory > 2 * GIB
        assert service.limits.max_response_uncompressed is None
    finally:
        service.close()


def test_optional_result_cap_and_dynamic_working_limit(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=1,
        cache_root=tmp_path / "cache",
        limits=ServiceLimits(
            user_cores=1,
            max_response_uncompressed=2 * GIB,
        ),
    )
    monkeypatch.setattr(service, "_selection", lambda _request: _large_selection())
    monkeypatch.setattr(
        "market_data_api.service.detect_local_resources",
        lambda _path: LocalResources(
            cpus=8,
            total_memory=64 * GIB,
            available_memory=60 * GIB,
            free_disk=100 * GIB,
        ),
    )
    try:
        with pytest.raises(RequestRejected, match="显式设置") as captured:
            service.preflight(_request())
        assert captured.value.code == "response_too_large"
    finally:
        service.close()

    service = DataService(
        gateway_host="127.0.0.1",
        gateway_port=1,
        cache_root=tmp_path / "cache-low",
        limits=ServiceLimits(user_cores=1),
    )
    monkeypatch.setattr(service, "_selection", lambda _request: _large_selection())
    monkeypatch.setattr(
        "market_data_api.service.detect_local_resources",
        lambda _path: LocalResources(
            cpus=2,
            total_memory=8 * GIB,
            available_memory=4 * GIB,
            free_disk=100 * GIB,
        ),
    )
    try:
        with pytest.raises(RequestRejected) as captured:
            service.preflight(_request())
        assert captured.value.code == "working_memory_too_large"
    finally:
        service.close()


def test_runtime_memory_monitor_and_arrow_oom_conversion() -> None:
    monitor = RuntimeMemoryMonitor(
        lambda: 100,
        reserve=200,
        check_interval=0,
    )
    with pytest.raises(LocalMemoryExhausted) as captured:
        monitor.check("test", force=True)
    assert captured.value.available_memory == 100
    assert captured.value.required_reserve == 200
    assert captured.value.stage == "test"

    def out_of_memory_batches():
        raise MemoryError("simulated")
        yield

    with pytest.raises(LocalMemoryExhausted, match="Arrow处理过程中"):
        list(_arrow_ipc_pipe(out_of_memory_batches(), compression="zstd"))
