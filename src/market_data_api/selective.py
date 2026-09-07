"""Bounded Parquet planning and sparse reads; Arrow is imported only by clients."""

from __future__ import annotations

import base64
import gc
import bisect
import io
import json
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from .range_gateway import decompress_footer

from .client import (
    _is_retryable_transfer_error,
    _iter_filtered_batches_from_parquet,
    _retry_wait,
    NetworkTransferError,
)


@dataclass(frozen=True)
class ReadOptions:
    """HDD defaults; settings change transfer planning, never query results."""

    coalesce_gap_bytes: int = 512 * 1024
    seek_cost_bytes: int = 1024 * 1024
    sequential_threshold: float = 0.85
    bundle_bytes: int = 16 * 1024**2
    metadata_cache_bytes: int = 64 * 1024**2

    def __post_init__(self):
        if (
            min(
                self.coalesce_gap_bytes, self.seek_cost_bytes, self.metadata_cache_bytes
            )
            < 0
        ):
            raise ValueError("读取参数字节数不能为负数")
        if self.bundle_bytes < 1 or not 0 < self.sequential_threshold <= 1:
            raise ValueError("bundle_bytes 必须为正；sequential_threshold 必须在 (0,1]")

    @classmethod
    def for_profile(cls, profile):
        if profile == "hdd":
            return cls()
        if profile == "ssd":
            return cls(coalesce_gap_bytes=32 * 1024, seek_cost_bytes=64 * 1024)
        raise ValueError("io_profile 必须是 hdd 或 ssd")


@dataclass
class ReadStats:
    source_bytes: int = 0
    metadata_bytes: int = 0
    transfer_bytes: int = 0
    planned_bytes: int = 0
    range_count: int = 0
    skipped_objects: int = 0
    sequential_objects: int = 0
    returned_rows: int = 0
    network_requests: int = 0
    retries: int = 0
    queue_ms: float = 0.0
    versions: dict[str, list[str]] = field(default_factory=dict)

    def as_dict(self):
        return asdict(self)


def object_ref(entry):
    return {
        key: getattr(entry, key)
        for key in ("object_id", "dataset", "trade_date", "version")
    }


@dataclass(frozen=True)
class ParquetMetadata:
    footer: bytes
    arrow: object


def parse_metadata(footer):
    """Close the Python file wrapper immediately; returned metadata owns its contents."""
    import pyarrow.parquet as pq

    with io.BytesIO(b"PAR1" + footer) as source:
        reader = pq.ParquetFile(source)
        try:
            return reader.metadata
        finally:
            reader.close()


class MetadataCache:
    """Bounded raw-footer LRU; parse only the current window to bound Arrow allocations."""

    def __init__(self, capacity):
        self.capacity = capacity
        self.bytes = 0
        self._items = OrderedDict()
        self._lock = threading.Lock()

    def get(self, entry):
        key = (entry.object_id, entry.version, entry.bytes)
        with self._lock:
            item = self._items.get(key)
            if item is not None:
                self._items.move_to_end(key)
        if item is not None:
            return ParquetMetadata(item, parse_metadata(item))
        return None

    def put(self, entry, item):
        key = (entry.object_id, entry.version, entry.bytes)
        cost = len(item.footer)
        if cost > self.capacity:
            return
        with self._lock:
            previous = self._items.pop(key, None)
            if previous is not None:
                self.bytes -= len(previous)
            self._items[key] = item.footer
            self.bytes += cost
            while self.bytes > self.capacity:
                _, old = self._items.popitem(last=False)
                self.bytes -= len(old)


def load_metadata(pool, entries, cache, stats, retries, backoff):
    """Fetch only missing footers in one authenticated, version-pinned request."""
    result = {e.object_id: cache.get(e) for e in entries}
    missing = [e for e in entries if result[e.object_id] is None]
    if not missing:
        return result
    body = json.dumps({"objects": [object_ref(e) for e in missing]}).encode()
    for attempt in range(retries + 1):
        try:
            with pool.connection() as connection:
                stats.network_requests += 1
                response = connection._response("POST", "/v1/metadata", body)
                try:
                    raw = response.read()
                    stats.metadata_bytes += len(raw)
                    payload = json.loads(raw)
                except BaseException:
                    connection.close()
                    raise
            by_id = {item["object_id"]: item for item in payload["objects"]}
            if len(by_id) != len(missing) or set(by_id) != {
                e.object_id for e in missing
            }:
                raise ValueError("网关元数据对象集合不匹配")
            for entry in missing:
                item = by_id[entry.object_id]
                if (
                    item["version"] != entry.version
                    or item["file_bytes"] != entry.bytes
                ):
                    raise ValueError("网关元数据版本或文件大小不匹配")
                footer = base64.b64decode(item["footer"], validate=True)
                codec = payload.get("footer_codec")
                if codec == "zlib":
                    footer = decompress_footer(footer)
                elif codec is not None:
                    raise ValueError(f"未知元数据压缩格式: {codec}")
                metadata = parse_metadata(footer)
                if metadata.num_rows != entry.rows:
                    raise ValueError("Parquet footer 行数与 catalog 不一致")
                parsed = ParquetMetadata(footer, metadata)
                cache.put(entry, parsed)
                result[entry.object_id] = parsed
            return result
        except BaseException as exc:
            if not _is_retryable_transfer_error(exc) or attempt == retries:
                raise
            stats.retries += 1
            _retry_wait(attempt + 1, backoff)


def coalesce_ranges(ranges, gap):
    """Read nearby chunks together, trading small overreads for fewer HDD seeks."""
    merged = []
    for start, length in sorted(ranges):
        end = start + length
        if merged and start <= merged[-1][1] + gap:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return tuple((start, end - start) for start, end in merged)


@dataclass(frozen=True)
class ObjectReadPlan:
    entry: object
    metadata: ParquetMetadata
    row_groups: tuple[int, ...]
    ranges: tuple[tuple[int, int], ...]
    max_row_group_bytes: int
    sequential: bool

    @property
    def transfer_bytes(self):
        return sum(size for _, size in self.ranges)


def plan_object(entry, metadata, request, options):
    """Prune using conservative min/max statistics, then compare seek and scan cost."""
    schema = metadata.arrow.schema.to_arrow_schema()
    names = set(schema.names)
    if request.columns is not None and set(request.columns) - names:
        raise ValueError(f"不存在这些列: {sorted(set(request.columns) - names)}")
    required = {"event_time"}
    if request.symbols is not None:
        required.add("symbol")
    if request.daily_start is not None:
        required.add("time_int")
    if required - names:
        raise ValueError(f"Parquet 缺少过滤字段: {sorted(required - names)}")
    wanted = (set(request.columns) | required) if request.columns is not None else names
    symbols = sorted(request.symbols) if request.symbols is not None else None
    start, end = request.start.replace(tzinfo=None), request.end.replace(tzinfo=None)
    groups, chunks, max_decoded = [], [], 0
    indexes = {}
    if metadata.arrow.num_row_groups:
        first_group = metadata.arrow.row_group(0)
        indexes = {
            first_group.column(j).path_in_schema: j
            for j in range(first_group.num_columns)
        }
    wanted_indexes = [
        j for name, j in indexes.items() if name.split(".", 1)[0] in wanted
    ]
    for i in range(metadata.arrow.num_row_groups):
        group = metadata.arrow.row_group(i)
        keep = True
        for name in ("symbol", "event_time"):
            col = group.column(indexes[name]) if name in indexes else None
            st = col.statistics if col is not None else None
            if st is None or not st.has_min_max:
                continue
            try:
                low, high = st.min, st.max
                if name == "symbol" and symbols is not None:
                    if isinstance(low, bytes):
                        low, high = low.decode(), high.decode()
                    position = bisect.bisect_left(symbols, low)
                    if position == len(symbols) or symbols[position] > high:
                        keep = False
                elif name == "event_time" and (high < start or low >= end):
                    keep = False
            except (TypeError, ValueError, UnicodeError):
                pass  # Unknown statistics must never exclude potentially matching rows.
        if not keep:
            continue
        groups.append(i)
        decoded = 0
        for index in wanted_indexes:
            col = group.column(index)
            offsets = [
                v
                for v in (col.dictionary_page_offset, col.data_page_offset)
                if v is not None and v >= 4
            ]
            if not offsets or col.total_compressed_size <= 0:
                raise ValueError("Parquet 列块偏移或大小非法")
            offset, length = min(offsets), col.total_compressed_size
            if offset + length > entry.bytes:
                raise ValueError("Parquet 列块超出对象边界")
            chunks.append((offset, length))
            decoded += col.total_uncompressed_size
        max_decoded = max(max_decoded, decoded)
    ranges = coalesce_ranges(chunks, options.coalesce_gap_bytes)
    cost = sum(n for _, n in ranges) + max(0, len(ranges) - 1) * options.seek_cost_bytes
    sequential = bool(ranges) and (
        len(ranges) > 4096
        or (
            request.read_strategy == "auto"
            and cost >= entry.bytes * options.sequential_threshold
        )
    )
    if sequential:
        ranges = ((0, entry.bytes),)
    return ObjectReadPlan(
        entry, metadata, tuple(groups), ranges, max_decoded, sequential
    )


class SparseFile(io.RawIOBase):
    """Seekable view of fetched segments without allocating the full file size."""

    def __init__(self, size, segments):
        super().__init__()
        self.size = size
        self.position = 0
        # Drop fully covered header/footer duplicates when the plan chose a whole file.
        self.segments = []
        for offset, raw in sorted(segments, key=lambda x: (x[0], -len(x[1]))):
            if self.segments and offset + len(raw) <= self.segments[-1][0] + len(
                self.segments[-1][1]
            ):
                continue
            self.segments.append((offset, raw))
        self.starts = [offset for offset, _ in self.segments]

    def readable(self):
        return True

    def close(self):
        self.segments.clear()
        self.starts.clear()
        super().close()

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        position = (
            offset
            if whence == 0
            else self.position + offset
            if whence == 1
            else self.size + offset
        )
        if position < 0:
            raise ValueError("negative seek")
        self.position = position
        return position

    def read(self, size=-1):
        length = (
            max(0, self.size - self.position)
            if size < 0
            else min(size, max(0, self.size - self.position))
        )
        pieces = []
        while length:
            i = bisect.bisect_right(self.starts, self.position) - 1
            if i < 0:
                raise OSError("读取器请求了计划外的字节")
            offset, raw = self.segments[i]
            local = self.position - offset
            count = min(length, len(raw) - local)
            if count <= 0:
                raise OSError(f"读取器请求了未下载的字节: {self.position}")
            pieces.append(raw[local : local + count])
            self.position += count
            length -= count
        return b"".join(pieces)


def _fetch_plans(pool, plans, stats, retries, backoff, memory_check):
    """Retry only unfinished objects; no partial object is returned to Arrow."""
    completed = []
    attempts = 0
    while len(completed) < len(plans):
        remaining = plans[len(completed) :]
        body = json.dumps(
            {"objects": [dict(object_ref(p.entry), ranges=p.ranges) for p in remaining]}
        ).encode()
        before = len(completed)
        expected = iter(remaining)

        def consume(header, source):
            plan = next(expected)
            if (
                header.get("object_id") != plan.entry.object_id
                or header.get("version") != plan.entry.version
                or header.get("file_bytes") != plan.entry.bytes
                or header.get("bytes") != plan.transfer_bytes
                or header.get("ranges") != [list(r) for r in plan.ranges]
            ):
                raise ValueError("范围响应与固定版本读取计划不一致")
            segments = [
                (0, b"PAR1"),
                (plan.entry.bytes - len(plan.metadata.footer), plan.metadata.footer),
            ]
            for offset, length in plan.ranges:
                memory_check("range_receive")
                chunks = []
                left = length
                while left:
                    block = source.read(min(left, 1024**2))
                    stats.transfer_bytes += len(block)
                    chunks.append(block)
                    left -= len(block)
                segments.append((offset, b"".join(chunks)))
            completed.append(SparseFile(plan.entry.bytes, segments))

        try:
            with pool.connection() as connection:
                stats.network_requests += 1
                response = connection._response("POST", "/v1/ranges", body)
                metrics = connection._consume_response(response, consume)
                stats.queue_ms += metrics.queue_ms
                if metrics.objects != len(remaining) or len(completed) != len(plans):
                    raise EOFError("范围响应对象数量不完整")
            return completed
        except BaseException as exc:
            if not _is_retryable_transfer_error(exc):
                raise
            if len(completed) > before:
                attempts = 0
            if len(completed) == len(plans):
                return completed
            if attempts >= retries:
                raise NetworkTransferError(
                    str(exc),
                    completed_objects=len(completed),
                    remaining_objects=len(plans) - len(completed),
                    retries=retries,
                ) from exc
            attempts += 1
            stats.retries += 1
            _retry_wait(attempts, backoff)


def iter_selective_batches(
    pool,
    request,
    entries,
    *,
    cache,
    options,
    stats,
    claim_memory,
    memory_check,
    batch_rows=131072,
    network_retries=3,
    network_retry_backoff=0.25,
    object_request_size=12,
):
    """Plan bounded windows, download coalesced ranges, and return exact Arrow rows."""
    import pyarrow as pa

    def window_batches(window):
        memory_check("metadata_start")
        metadata = load_metadata(
            pool, window, cache, stats, network_retries, network_retry_backoff
        )
        plans = [
            plan_object(e, metadata[e.object_id], request, options) for e in window
        ]
        active = []
        for plan in plans:
            stats.planned_bytes += plan.transfer_bytes
            stats.range_count += len(plan.ranges)
            stats.sequential_objects += int(plan.sequential)
            if not plan.row_groups:
                stats.skipped_objects += 1
                schema = plan.metadata.arrow.schema.to_arrow_schema()
                if request.columns is not None:
                    schema = pa.schema(
                        [schema.field(name) for name in request.columns],
                        metadata=schema.metadata,
                    )
                yield pa.RecordBatch.from_arrays(
                    [pa.array([], type=f.type) for f in schema], schema=schema
                )
            else:
                active.append(plan)
        first = 0
        while first < len(active):
            last = first + 1
            total = active[first].transfer_bytes
            while (
                last < len(active)
                and total + active[last].transfer_bytes <= options.bundle_bytes
            ):
                total += active[last].transfer_bytes
                last += 1
            selected = active[first:last]
            working = (
                2 * total
                + 4 * max(p.max_row_group_bytes for p in selected)
                + 32 * 1024**2
            )
            with claim_memory(working):
                buffers = _fetch_plans(
                    pool,
                    selected,
                    stats,
                    network_retries,
                    network_retry_backoff,
                    memory_check,
                )
                for plan, buffer in zip(selected, buffers):
                    try:
                        yield from _iter_filtered_batches_from_parquet(
                            buffer,
                            request,
                            batch_rows=batch_rows,
                            memory_check=memory_check,
                            metadata=plan.metadata.arrow,
                            row_groups=list(plan.row_groups),
                        )
                    finally:
                        buffer.close()
            first = last

    width = min(12, max(1, object_request_size))
    for offset in range(0, len(entries), width):
        try:
            yield from window_batches(entries[offset : offset + width])
        finally:
            # Arrow metadata can own large C++ allocations through tiny Python cycles.
            # Collect only young temporary wrappers after each bounded window.
            gc.collect(0)
