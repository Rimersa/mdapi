from __future__ import annotations

import contextlib
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator

from .cache import CachePlan, LocalCache
from .catalog import ObjectEntry
from .client import (
    GatewayPool,
    Selection,
    fetch_cache_objects,
    iter_cached_batches,
    iter_remote_batches,
)
from .model import DataRequest, FetchMode, LocalResources, detect_local_resources
from .selective import MetadataCache, ReadOptions, ReadStats, iter_selective_batches

ARROW_MEMORY_FACTOR = {
    "snapshots": 24,
    "orders": 16,
    "trades": 10,
}


class RequestRejected(RuntimeError):
    def __init__(self, code: str, detail: str, estimates: dict) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.estimates = estimates


class NoData(RuntimeError):
    pass


def _version_map(entries):
    grouped = {}
    for entry in entries:
        grouped.setdefault(entry.bucket_start[:10], set()).add(entry.version)
    return {day: sorted(values) for day, values in sorted(grouped.items())}


class LocalMemoryExhausted(RuntimeError):
    def __init__(
        self,
        detail: str,
        *,
        available_memory: int | None = None,
        required_reserve: int | None = None,
        stage: str | None = None,
    ) -> None:
        super().__init__(detail)
        self.available_memory = available_memory
        self.required_reserve = required_reserve
        self.stage = stage


@dataclass(frozen=True)
class ServiceLimits:
    max_response_uncompressed: int | None = None
    max_memory_fraction: float = 0.40
    runtime_memory_reserve_fraction: float = 0.05
    runtime_memory_reserve_min: int = 512 * 1024**2
    memory_check_interval: float = 0.25
    cache_disk_headroom: float = 1.20
    batch_rows: int = 131_072
    arrow_compression: str | None = "zstd"
    gateway_connections: int = 2
    user_cores: int | None = None
    network_retries: int = 3
    network_retry_backoff: float = 0.25
    object_request_size: int = 12
    read_options: ReadOptions = field(default_factory=ReadOptions)


@dataclass(frozen=True)
class Preflight:
    selection: Selection
    cache_plan: CachePlan | None
    estimated_arrow_memory: int
    estimated_working_memory: int
    working_memory_limit: int
    memory_reserve: int
    total_memory: int
    available_memory: int
    missing_cache_bytes: int
    free_disk: int
    local_cpus: int
    recommended_workers: int
    selective: bool = False
    stats: ReadStats = field(default_factory=ReadStats)

    def as_dict(self) -> dict:
        return {
            "objects": len(self.selection.entries),
            "rows": self.selection.rows,
            "rows_estimate_kind": "candidate_rows_upper_bound",
            "source_bytes": self.selection.source_bytes,
            "uncompressed_bytes": self.selection.uncompressed_bytes,
            "estimated_arrow_memory": self.estimated_arrow_memory,
            "estimated_arrow_memory_kind": "unfiltered_candidate_upper_bound",
            "estimated_working_memory": self.estimated_working_memory,
            "working_memory_limit": self.working_memory_limit,
            "memory_reserve": self.memory_reserve,
            "total_memory": self.total_memory,
            "available_memory": self.available_memory,
            "missing_cache_bytes": self.missing_cache_bytes,
            "free_disk": self.free_disk,
            "local_cpus": self.local_cpus,
            "recommended_workers": self.recommended_workers,
            "catalog_generated_at": self.selection.catalog_generated_at,
            "read_path": "adaptive_ranges" if self.selective else "whole_objects",
            "symbols_supported": True,
            "working_memory_estimate_kind": "per_bundle_runtime_checked"
            if self.selective
            else "largest_bucket",
            "estimated_transfer_bytes": None
            if self.selective
            else self.missing_cache_bytes
            if self.cache_plan is not None
            else self.selection.source_bytes,
        }


def _estimated_arrow_memory(selection: Selection, dataset: str) -> int:
    return max(
        selection.uncompressed_bytes,
        selection.source_bytes * ARROW_MEMORY_FACTOR[dataset],
    )


def _bucket_working_bytes(entries: list[ObjectEntry], dataset: str) -> int:
    grouped: dict[str, int] = {}
    for entry in entries:
        grouped[entry.bucket_start] = grouped.get(entry.bucket_start, 0) + entry.bytes
    largest = max(grouped.values(), default=0) * ARROW_MEMORY_FACTOR[dataset]
    return largest * 2 + 128 * 1024**2


def _memory_reserve(resources: LocalResources, limits: ServiceLimits) -> int:
    return max(
        limits.runtime_memory_reserve_min,
        int(resources.total_memory * limits.runtime_memory_reserve_fraction),
    )


def _working_memory_limit(resources: LocalResources, limits: ServiceLimits) -> int:
    usable = max(0, resources.available_memory - _memory_reserve(resources, limits))
    return int(usable * limits.max_memory_fraction)


class RuntimeMemoryMonitor:
    def __init__(
        self,
        probe: Callable[[], int],
        *,
        reserve: int,
        check_interval: float,
    ) -> None:
        self.probe = probe
        self.reserve = max(1, reserve)
        self.check_interval = max(0.0, check_interval)
        self._next_check = 0.0
        self._lock = threading.Lock()

    def check(self, stage: str, *, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            if not force and now < self._next_check:
                return
            available = self.probe()
            self._next_check = now + self.check_interval
        if available < self.reserve:
            raise LocalMemoryExhausted(
                "本机可用内存已低于运行安全余量，数据流已主动终止",
                available_memory=available,
                required_reserve=self.reserve,
                stage=stage,
            )


class WeightedMemoryGate:
    def __init__(self, capacity: int, slots: int) -> None:
        self.capacity = max(1, capacity)
        self.slots = threading.BoundedSemaphore(max(1, slots))
        self._available = self.capacity
        self._condition = threading.Condition()

    @contextlib.contextmanager
    def acquire(self, requested: int):
        weight = min(max(1, requested), self.capacity)
        if not self.slots.acquire(timeout=300):
            raise TimeoutError("等待本机读取槽位超时")
        try:
            with self._condition:
                if not self._condition.wait_for(
                    lambda: self._available >= weight, timeout=300
                ):
                    raise TimeoutError("等待本机读取内存额度超时")
                self._available -= weight
            try:
                yield
            finally:
                with self._condition:
                    self._available += weight
                    self._condition.notify_all()
        finally:
            self.slots.release()


class DataService:
    def __init__(
        self,
        *,
        gateway_host: str,
        gateway_port: int,
        cache_root: Path,
        gateway_token: str | None = None,
        limits: ServiceLimits | None = None,
    ) -> None:
        self.limits = limits or ServiceLimits()
        if not 0 < self.limits.max_memory_fraction <= 1:
            raise ValueError("max_memory_fraction 必须在 (0, 1] 范围内")
        if not 0 <= self.limits.runtime_memory_reserve_fraction < 1:
            raise ValueError("runtime_memory_reserve_fraction 必须在 [0, 1) 范围内")
        if self.limits.runtime_memory_reserve_min < 0:
            raise ValueError("runtime_memory_reserve_min 不能为负数")
        if self.limits.max_response_uncompressed is not None:
            if self.limits.max_response_uncompressed < 1:
                raise ValueError("max_response_uncompressed 必须为正数或 None")
        if self.limits.network_retries < 0:
            raise ValueError("network_retries 不能为负数")
        if self.limits.network_retry_backoff < 0:
            raise ValueError("network_retry_backoff 不能为负数")
        if self.limits.object_request_size < 1:
            raise ValueError("object_request_size 必须 >= 1")
        cache_root.mkdir(parents=True, exist_ok=True)
        resources = detect_local_resources(str(cache_root))
        requested_cores = self.limits.user_cores
        if requested_cores is not None:
            if requested_cores < 1:
                raise ValueError("cores 必须 >= 1")
            core_budget = min(requested_cores, resources.cpus)
        else:
            core_budget = min(8, max(1, resources.cpus // 2))
        remote_connections = min(
            self.limits.gateway_connections,
            core_budget,
            max(1, resources.available_memory // (2 * 1024**3)),
        )
        local_workers = min(
            core_budget,
            max(1, resources.available_memory // (2 * 1024**3)),
        )
        import pyarrow as pa

        pa.set_cpu_count(core_budget)
        pa.set_io_thread_count(max(1, min(core_budget, 4)))
        self.core_budget = core_budget
        self.pool = GatewayPool(
            gateway_host,
            gateway_port,
            token=gateway_token,
            connections=remote_connections,
        )
        self.cache = LocalCache(cache_root)
        self.metadata_cache = MetadataCache(
            self.limits.read_options.metadata_cache_bytes
        )
        self._cache_fill_lock = threading.Lock()
        self.resources = resources
        self.workers = local_workers
        self.remote_connections = remote_connections
        self.memory_gate = WeightedMemoryGate(
            max(1, _working_memory_limit(resources, self.limits)),
            local_workers,
        )

    def _memory_monitor(self, reserve: int) -> RuntimeMemoryMonitor:
        return RuntimeMemoryMonitor(
            lambda: detect_local_resources(str(self.cache.root)).available_memory,
            reserve=reserve,
            check_interval=self.limits.memory_check_interval,
        )

    def close(self) -> None:
        self.pool.close()

    def _selection(self, request: DataRequest) -> Selection:
        with self.pool.connection() as connection:
            return connection.manifest(request)

    def preflight(self, request: DataRequest) -> Preflight:
        resources = detect_local_resources(str(self.cache.root))
        selection = self._selection(request)
        if not selection.entries:
            raise NoData(
                f"{request.dataset} 在 [{request.start.isoformat()}, "
                f"{request.end.isoformat()}) 没有已发布数据"
            )
        estimated_arrow_memory = _estimated_arrow_memory(
            selection,
            request.dataset,
        )
        working = _bucket_working_bytes(selection.entries, request.dataset)
        capable = (
            "range_bundles_v1" in selection.capabilities
            and "parquet_footers_v1" in selection.capabilities
        )
        if request.read_strategy == "ranges" and not capable:
            raise ValueError("ranges 读取需要 0.5 或更新的服务器网关")
        selective = (
            request.mode == FetchMode.DIRECT
            and capable
            and request.read_strategy != "sequential"
            and (
                request.symbols is not None
                or request.columns is not None
                or request.read_strategy == "ranges"
            )
        )
        if selective:
            # Each actual bundle is separately estimated and admitted before allocating data buffers.
            working = min(
                working, 64 * 1024**2 + 2 * self.limits.read_options.bundle_bytes
            )
        cache_plan = (
            self.cache.plan(request, selection.entries)
            if request.mode == FetchMode.CACHE
            else None
        )
        missing_cache_bytes = cache_plan.missing_bytes if cache_plan else 0
        estimates = {
            "source_bytes": selection.source_bytes,
            "uncompressed_bytes": selection.uncompressed_bytes,
            "estimated_arrow_memory": estimated_arrow_memory,
            "estimated_working_memory": working,
            "total_memory": resources.total_memory,
            "available_memory": resources.available_memory,
            "missing_cache_bytes": missing_cache_bytes,
            "free_disk": resources.free_disk,
        }
        if (
            self.limits.max_response_uncompressed is not None
            and estimated_arrow_memory > self.limits.max_response_uncompressed
        ):
            raise RequestRejected(
                "response_too_large",
                "请求的估算解压数据量超过用户显式设置的 API 上限",
                estimates,
            )
        memory_reserve = _memory_reserve(resources, self.limits)
        memory_limit = _working_memory_limit(resources, self.limits)
        estimates["memory_reserve"] = memory_reserve
        estimates["working_memory_limit"] = memory_limit
        if working > memory_limit:
            raise RequestRejected(
                "working_memory_too_large",
                "API流水线的估算工作内存超过本机当前安全额度",
                estimates,
            )
        if request.mode == FetchMode.CACHE:
            required_disk = int(missing_cache_bytes * self.limits.cache_disk_headroom)
            if required_disk > resources.free_disk:
                raise RequestRejected(
                    "cache_disk_insufficient",
                    "本地缓存盘空间不足",
                    estimates,
                )
        concurrency_cap = (
            self.remote_connections
            if request.mode == FetchMode.DIRECT
            else self.workers
        )
        recommended = min(
            concurrency_cap,
            max(1, resources.cpus // 4),
            max(1, memory_limit // max(1, working)),
        )
        return Preflight(
            selection=selection,
            cache_plan=cache_plan,
            estimated_arrow_memory=estimated_arrow_memory,
            estimated_working_memory=working,
            working_memory_limit=memory_limit,
            memory_reserve=memory_reserve,
            total_memory=resources.total_memory,
            available_memory=resources.available_memory,
            missing_cache_bytes=missing_cache_bytes,
            free_disk=resources.free_disk,
            local_cpus=resources.cpus,
            recommended_workers=recommended,
            selective=selective,
            stats=ReadStats(
                source_bytes=selection.source_bytes,
                versions=_version_map(selection.entries),
            ),
        )

    @contextlib.contextmanager
    def _claim_scan_memory(self, requested):
        resources = detect_local_resources(str(self.cache.root))
        limit = _working_memory_limit(resources, self.limits)
        if requested > limit:
            raise RequestRejected(
                "working_memory_too_large",
                "选中数据块的工作内存超过当前安全额度",
                {"estimated_working_memory": requested, "working_memory_limit": limit},
            )
        with self.memory_gate.acquire(requested):
            self._memory_monitor(_memory_reserve(resources, self.limits)).check(
                "range_start", force=True
            )
            yield

    @staticmethod
    def _count_batches(batches, stats):
        try:
            for batch in batches:
                stats.returned_rows += batch.num_rows
                yield batch
        finally:
            close = getattr(batches, "close", None)
            if close is not None:
                close()

    def batches(
        self,
        request: DataRequest,
        preflight: Preflight | None = None,
    ) -> tuple[Preflight, Iterator]:
        plan = preflight or self.preflight(request)
        monitor = self._memory_monitor(plan.memory_reserve)
        if plan.selective:
            batches = iter_selective_batches(
                self.pool,
                request,
                plan.selection.entries,
                cache=self.metadata_cache,
                options=self.limits.read_options,
                stats=plan.stats,
                claim_memory=self._claim_scan_memory,
                memory_check=monitor.check,
                batch_rows=self.limits.batch_rows,
                network_retries=self.limits.network_retries,
                network_retry_backoff=self.limits.network_retry_backoff,
                object_request_size=self.limits.object_request_size,
            )
            return plan, self._count_batches(batches, plan.stats)
        if request.mode == FetchMode.DIRECT:

            def direct_iterator():
                with self.memory_gate.acquire(plan.estimated_working_memory):
                    with self.pool.connection() as connection:
                        monitor.check("direct_start", force=True)
                        yield from iter_remote_batches(
                            connection,
                            request,
                            plan.selection.entries,
                            batch_rows=self.limits.batch_rows,
                            memory_check=monitor.check,
                            network_retries=self.limits.network_retries,
                            network_retry_backoff=(self.limits.network_retry_backoff),
                            object_request_size=self.limits.object_request_size,
                            stats=plan.stats,
                        )

            return plan, self._count_batches(direct_iterator(), plan.stats)

        assert plan.cache_plan is not None
        with self._cache_fill_lock:
            # Recheck after the lock: simultaneous cache misses share the completed fill.
            current_cache_plan = self.cache.plan(request, plan.selection.entries)
            if current_cache_plan.remote_objects:
                with self.memory_gate.acquire(plan.estimated_working_memory):
                    monitor.check("cache_fill_start", force=True)
                    with self.pool.connection() as connection:
                        fetch_cache_objects(
                            connection,
                            self.cache,
                            current_cache_plan.remote_objects,
                            network_retries=self.limits.network_retries,
                            network_retry_backoff=self.limits.network_retry_backoff,
                            object_request_size=self.limits.object_request_size,
                            stats=plan.stats,
                        )
            expected = {entry.bucket_start for entry in plan.selection.entries}
            cached = [
                entry
                for entry in self.cache.selected(request)
                if entry.bucket_start in expected
            ]
        expected_buckets = {entry.bucket_start for entry in plan.selection.entries}
        cached_buckets = {entry.bucket_start for entry in cached}
        plan.stats.versions = _version_map(cached)
        if cached_buckets != expected_buckets:
            missing = sorted(expected_buckets - cached_buckets)
            raise RuntimeError(f"缓存发布后仍缺少桶: {missing}")

        def cache_iterator():
            with self.memory_gate.acquire(plan.estimated_working_memory):
                monitor.check("cache_read_start", force=True)
                yield from iter_cached_batches(
                    cached,
                    request,
                    batch_rows=self.limits.batch_rows,
                    memory_check=monitor.check,
                )

        return plan, self._count_batches(cache_iterator(), plan.stats)

    def arrow_stream(
        self,
        request: DataRequest,
        preflight: Preflight | None = None,
    ) -> tuple[Preflight, Iterator[bytes]]:
        plan, batches = self.batches(request, preflight)
        return plan, _arrow_ipc_pipe(
            batches,
            compression=self.limits.arrow_compression,
        )


def _arrow_ipc_pipe(
    batches: Iterator,
    *,
    compression: str | None,
    chunk_size: int = 1024 * 1024,
) -> Iterator[bytes]:
    import pyarrow as pa

    read_fd, write_fd = os.pipe()
    read_file = os.fdopen(read_fd, "rb", buffering=0)
    write_file = os.fdopen(write_fd, "wb", buffering=0)
    errors: list[BaseException] = []

    def write_stream() -> None:
        try:
            first = next(batches)
            options = pa.ipc.IpcWriteOptions(compression=compression)
            with pa.ipc.new_stream(
                write_file,
                first.schema,
                options=options,
            ) as writer:
                writer.write_batch(first)
                for batch in batches:
                    writer.write_batch(batch)
        except StopIteration:
            errors.append(NoData("精确时间过滤后没有数据行"))
        except BaseException as exc:
            memory_error_types: tuple[type[BaseException], ...] = (MemoryError,)
            arrow_memory_error = getattr(pa, "ArrowMemoryError", None)
            if isinstance(arrow_memory_error, type):
                memory_error_types += (arrow_memory_error,)
            if isinstance(exc, LocalMemoryExhausted):
                errors.append(exc)
            elif isinstance(exc, memory_error_types):
                errors.append(
                    LocalMemoryExhausted("本机在Arrow处理过程中内存不足，数据流已终止")
                )
            else:
                errors.append(exc)
        finally:
            write_file.close()
            close = getattr(batches, "close", None)
            if close is not None:
                close()

    thread = threading.Thread(
        target=write_stream,
        name="mdapi-arrow-writer",
        daemon=True,
    )
    thread.start()
    try:
        while True:
            chunk = read_file.read(chunk_size)
            if not chunk:
                break
            yield chunk
        thread.join()
        if errors:
            raise errors[0]
    finally:
        read_file.close()
        thread.join(timeout=5)
