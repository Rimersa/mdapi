from __future__ import annotations

import contextlib
import errno
import http.client
import io
import json
import queue
import socket
import struct
import threading
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Callable, Iterator

from .cache import CachedObject, LocalCache
from .catalog import ObjectEntry
from .model import DataRequest
from .protocol import (
    BUNDLE_MAGIC,
    HEADER_SIZE,
    MAX_PART_HEADER,
    decode_part_header,
)


class GatewayError(RuntimeError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"87 gateway HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


class NetworkTransferError(ConnectionError):
    """A retryable transfer failed after the configured retry budget."""

    def __init__(
        self,
        detail: str,
        *,
        completed_objects: int,
        remaining_objects: int,
        retries: int,
    ) -> None:
        super().__init__(detail)
        self.completed_objects = completed_objects
        self.remaining_objects = remaining_objects
        self.retries = retries


@dataclass(frozen=True)
class BundleMetrics:
    objects: int
    payload_bytes: int
    wire_bytes: int
    queue_ms: float


@dataclass(frozen=True)
class Selection:
    entries: list[ObjectEntry]
    source_bytes: int
    uncompressed_bytes: int
    rows: int
    catalog_generated_at: str


class LimitedReader:
    def __init__(self, source: BinaryIO, length: int) -> None:
        self.source = source
        self.remaining = length

    def read(self, size: int = -1) -> bytes:
        if self.remaining == 0:
            return b""
        if size < 0 or size > self.remaining:
            size = self.remaining
        raw = self.source.read(size)
        if not raw:
            raise EOFError(f"bundle object 提前结束，还差 {self.remaining} bytes")
        self.remaining -= len(raw)
        return raw

    def drain(self) -> None:
        while self.remaining:
            self.read(min(self.remaining, 1024 * 1024))


def _read_exact(source: BinaryIO, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        raw = source.read(remaining)
        if not raw:
            raise EOFError(f"响应提前结束，还差 {remaining} bytes")
        chunks.append(raw)
        remaining -= len(raw)
    return b"".join(chunks)


class GatewayConnection:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        token: str | None = None,
        timeout: float = 300.0,
    ) -> None:
        self.host = host
        self.port = port
        self.token = token
        self.timeout = timeout
        self._connection: http.client.HTTPConnection | None = None

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _conn(self) -> http.client.HTTPConnection:
        if self._connection is None:
            self._connection = http.client.HTTPConnection(
                self.host,
                self.port,
                timeout=self.timeout,
            )
        return self._connection

    def _headers(self) -> dict[str, str]:
        result = {"Accept-Encoding": "identity"}
        if self.token:
            result["Authorization"] = f"Bearer {self.token}"
        return result

    def _response(self, method: str, path: str, body: bytes | None = None):
        connection = self._conn()
        headers = self._headers()
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
        except (BrokenPipeError, ConnectionResetError, http.client.RemoteDisconnected):
            self.close()
            connection = self._conn()
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
        if response.status >= 400:
            raw = response.read()
            try:
                payload = json.loads(raw)
                detail = payload.get("detail", raw.decode("utf-8", "replace"))
            except Exception:
                detail = raw.decode("utf-8", "replace")
            raise GatewayError(response.status, detail)
        return response

    @staticmethod
    def _query(request: DataRequest) -> str:
        values = {
            "dataset": request.dataset,
            "start": request.start.isoformat(),
            "end": request.end.isoformat(),
        }
        if request.daily_start is not None:
            values["daily_start"] = request.daily_start.isoformat()
            values["daily_end"] = request.daily_end.isoformat()
        return urllib.parse.urlencode(values)

    def manifest(self, request: DataRequest) -> Selection:
        response = self._response("GET", f"/v1/manifest?{self._query(request)}")
        payload = json.loads(response.read())
        summary = payload["summary"]
        return Selection(
            entries=[ObjectEntry.from_dict(item) for item in payload["objects"]],
            source_bytes=int(summary["source_bytes"]),
            uncompressed_bytes=int(summary["uncompressed_bytes"]),
            rows=int(summary["rows"]),
            catalog_generated_at=str(payload["catalog_generated_at"]),
        )

    def _consume_response(
        self,
        response: http.client.HTTPResponse,
        consumer: Callable[[dict, LimitedReader], None],
    ) -> BundleMetrics:
        try:
            magic = _read_exact(response, len(BUNDLE_MAGIC))
            if magic != BUNDLE_MAGIC:
                raise ValueError(f"bundle magic 错误: {magic!r}")
            objects = 0
            payload_bytes = 0
            while True:
                header_size = struct.unpack(
                    "!I", _read_exact(response, HEADER_SIZE)
                )[0]
                if header_size == 0:
                    break
                if header_size > MAX_PART_HEADER:
                    raise ValueError(f"bundle header 过大: {header_size}")
                header = decode_part_header(_read_exact(response, header_size))
                length = int(header["bytes"])
                limited = LimitedReader(response, length)
                consumer(header, limited)
                limited.drain()
                objects += 1
                payload_bytes += length
            response.read()
            return BundleMetrics(
                objects=objects,
                payload_bytes=payload_bytes,
                wire_bytes=int(response.getheader("Content-Length", "0")),
                queue_ms=float(response.getheader("X-MDAPI-Queue-Ms", "0")),
            )
        except BaseException:
            self.close()
            raise

    def consume_time_range(
        self,
        request: DataRequest,
        consumer: Callable[[dict, LimitedReader], None],
    ) -> BundleMetrics:
        response = self._response("GET", f"/v1/data?{self._query(request)}")
        return self._consume_response(response, consumer)

    def consume_objects(
        self,
        entries: list[ObjectEntry],
        consumer: Callable[[dict, LimitedReader], None],
    ) -> BundleMetrics:
        body = json.dumps(
            {
                "objects": [
                    {
                        "object_id": entry.object_id,
                        "dataset": entry.dataset,
                        "trade_date": entry.trade_date,
                        "version": entry.version,
                    }
                    for entry in entries
                ]
            },
            separators=(",", ":"),
        ).encode()
        response = self._response("POST", "/v1/objects", body)
        return self._consume_response(response, consumer)


class GatewayPool:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        token: str | None = None,
        connections: int = 2,
        timeout: float = 300.0,
    ) -> None:
        if connections < 1:
            raise ValueError("connections 必须 >= 1")
        self._queue: queue.LifoQueue[GatewayConnection] = queue.LifoQueue()
        self._connections = [
            GatewayConnection(host, port, token=token, timeout=timeout)
            for _ in range(connections)
        ]
        for connection in self._connections:
            self._queue.put(connection)

    @contextlib.contextmanager
    def connection(self):
        connection = self._queue.get()
        try:
            yield connection
        finally:
            self._queue.put(connection)

    def close(self) -> None:
        for connection in self._connections:
            connection.close()


def _copy_limited(source: LimitedReader, target: BinaryIO) -> int:
    written = 0
    while source.remaining:
        raw = source.read(min(source.remaining, 1024 * 1024))
        target.write(raw)
        written += len(raw)
    return written


_RETRYABLE_ERRNOS = {
    errno.EPIPE,
    errno.ECONNABORTED,
    errno.ECONNREFUSED,
    errno.ECONNRESET,
    errno.ETIMEDOUT,
    errno.ENETDOWN,
    errno.ENETRESET,
    errno.ENETUNREACH,
    errno.EHOSTDOWN,
    errno.EHOSTUNREACH,
}


def _is_retryable_transfer_error(exc: BaseException) -> bool:
    if isinstance(exc, GatewayError):
        return False
    if isinstance(
        exc,
        (
            EOFError,
            ConnectionError,
            TimeoutError,
            socket.timeout,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
        ),
    ):
        return True
    return isinstance(exc, OSError) and exc.errno in _RETRYABLE_ERRNOS


def _validate_object_header(entry: ObjectEntry, header: dict) -> None:
    object_id = str(header.get("object_id", ""))
    if object_id != entry.object_id:
        raise ValueError(
            "网关对象顺序或标识错误: "
            f"expected={entry.object_id}, actual={object_id}"
        )
    header_bytes = int(header.get("bytes", -1))
    if header_bytes != entry.bytes:
        raise ValueError(
            f"网关对象大小元数据错误: {object_id}: "
            f"{header_bytes} != {entry.bytes}"
        )
    if str(header.get("version", "")) != entry.version:
        raise ValueError(f"网关对象版本已改变: {object_id}")


def _retry_wait(retries_used: int, backoff: float) -> None:
    delay = backoff * (2 ** max(0, retries_used - 1))
    if delay > 0:
        time.sleep(delay)


def _network_failure(
    exc: BaseException,
    *,
    completed: int,
    remaining: int,
    retries: int,
) -> NetworkTransferError:
    return NetworkTransferError(
        "网络传输在自动重试后仍未完成；"
        f"已完成 {completed} 个对象，剩余 {remaining} 个对象；"
        f"最后错误: {type(exc).__name__}: {exc}",
        completed_objects=completed,
        remaining_objects=remaining,
        retries=retries,
    )


def fetch_cache_objects(
    connection: GatewayConnection,
    cache: LocalCache,
    entries: list[ObjectEntry],
    *,
    network_retries: int = 3,
    network_retry_backoff: float = 0.25,
    object_request_size: int = 12,
) -> list[CachedObject]:
    if not entries:
        return []
    if network_retries < 0:
        raise ValueError("network_retries 不能为负数")
    if network_retry_backoff < 0:
        raise ValueError("network_retry_backoff 不能为负数")
    if object_request_size < 1:
        raise ValueError("object_request_size 必须 >= 1")

    ordered = sorted(
        entries,
        key=lambda item: (item.bucket_start, item.relative_path),
    )
    by_bucket: dict[str, list[ObjectEntry]] = {}
    bucket_order: list[str] = []
    for entry in ordered:
        if entry.bucket_start not in by_bucket:
            bucket_order.append(entry.bucket_start)
            by_bucket[entry.bucket_start] = []
        by_bucket[entry.bucket_start].append(entry)

    published: list[CachedObject] = []
    completed_objects = 0
    next_bucket = 0
    retries_used = 0
    while next_bucket < len(bucket_order):
        request_buckets: list[str] = []
        request_entries: list[ObjectEntry] = []
        cursor = next_bucket
        while cursor < len(bucket_order):
            bucket = bucket_order[cursor]
            values = by_bucket[bucket]
            if (
                request_entries
                and len(request_entries) + len(values) > object_request_size
            ):
                break
            request_buckets.append(bucket)
            request_entries.extend(values)
            cursor += 1

        expected_index = 0
        bucket_downloads: dict[str, list[tuple[ObjectEntry, Path]]] = {}
        bucket_received: dict[str, int] = {}
        completed_buckets_this_attempt = 0

        def cleanup_partials() -> None:
            for values in bucket_downloads.values():
                for _, path in values:
                    path.unlink(missing_ok=True)
            bucket_downloads.clear()
            bucket_received.clear()

        def consume(header: dict, source: LimitedReader) -> None:
            nonlocal expected_index, completed_buckets_this_attempt
            if expected_index >= len(request_entries):
                raise ValueError("网关返回了多余对象")
            entry = request_entries[expected_index]
            _validate_object_header(entry, header)
            with cache.partial_file(entry) as (partial, handle):
                written = _copy_limited(source, handle)
            if written != entry.bytes:
                partial.unlink(missing_ok=True)
                raise RuntimeError(
                    f"缓存对象字节数错误: {entry.object_id}: "
                    f"{written} != {entry.bytes}"
                )
            import pyarrow.parquet as pq

            rows = pq.ParquetFile(partial).metadata.num_rows
            if rows != entry.rows:
                partial.unlink(missing_ok=True)
                raise RuntimeError(
                    f"缓存对象行数错误: {entry.object_id}: "
                    f"{rows} != {entry.rows}"
                )
            bucket_downloads.setdefault(entry.bucket_start, []).append(
                (entry, partial)
            )
            bucket_received[entry.bucket_start] = (
                bucket_received.get(entry.bucket_start, 0) + 1
            )
            expected_index += 1
            if bucket_received[entry.bucket_start] == len(
                by_bucket[entry.bucket_start]
            ):
                values = bucket_downloads.pop(entry.bucket_start)
                published.extend(cache.publish_downloads(values))
                bucket_received.pop(entry.bucket_start, None)
                completed_buckets_this_attempt += 1

        try:
            metrics = connection.consume_objects(
                request_entries,
                consume,
            )
            if metrics.objects != len(request_entries) or expected_index != len(
                request_entries
            ):
                raise EOFError(
                    "网关对象数错误: "
                    f"{metrics.objects}/{expected_index} != {len(request_entries)}"
                )
            cleanup_partials()
            next_bucket += len(request_buckets)
            completed_objects += len(request_entries)
            retries_used = 0
        except BaseException as exc:
            cleanup_partials()
            if completed_buckets_this_attempt:
                completed_bucket_names = request_buckets[
                    :completed_buckets_this_attempt
                ]
                completed_now = sum(
                    len(by_bucket[bucket]) for bucket in completed_bucket_names
                )
                next_bucket += completed_buckets_this_attempt
                completed_objects += completed_now
                retries_used = 0
            remaining = len(ordered) - completed_objects
            if remaining == 0:
                break
            if not _is_retryable_transfer_error(exc):
                raise
            connection.close()
            retries_used += 1
            if retries_used > network_retries:
                raise _network_failure(
                    exc,
                    completed=completed_objects,
                    remaining=remaining,
                    retries=network_retries,
                ) from exc
            _retry_wait(retries_used, network_retry_backoff)
    return published


@dataclass(frozen=True)
class BatchStreamResult:
    iterator: Iterator
    completion: threading.Event


def _iter_filtered_batches_from_parquet(
    source,
    request: DataRequest,
    *,
    batch_rows: int,
    memory_check: Callable[[str], None] | None = None,
):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(source)
    schema_names = set(parquet.schema_arrow.names)
    if request.columns is not None:
        missing = sorted(set(request.columns) - schema_names)
        if missing:
            raise ValueError(f"不存在这些列: {missing}")
        columns = list(request.columns)
        if "event_time" not in columns:
            columns.append("event_time")
        if request.daily_start is not None and "time_int" not in columns:
            columns.append("time_int")
    else:
        columns = None
    start = request.start.replace(tzinfo=None)
    end = request.end.replace(tzinfo=None)
    start_scalar = pa.scalar(start, type=pa.timestamp("us"))
    end_scalar = pa.scalar(end, type=pa.timestamp("us"))
    for batch in parquet.iter_batches(
        batch_size=batch_rows,
        columns=columns,
        use_threads=True,
    ):
        if memory_check is not None:
            memory_check("parquet_batch")
        event_time = batch.column(batch.schema.get_field_index("event_time"))
        mask = pc.and_(
            pc.greater_equal(event_time, start_scalar),
            pc.less(event_time, end_scalar),
        )
        if request.daily_start is not None:
            if "time_int" not in batch.schema.names:
                raise ValueError("源数据缺少每日窗口过滤所需的 time_int")
            time_int = batch.column(batch.schema.get_field_index("time_int"))

            def encoded(value) -> int:
                return (
                    value.hour * 10_000_000
                    + value.minute * 100_000
                    + value.second * 1_000
                    + value.microsecond // 1_000
                )

            daily = pc.and_(
                pc.greater_equal(time_int, encoded(request.daily_start)),
                pc.less(time_int, encoded(request.daily_end)),
            )
            mask = pc.and_(mask, daily)
        filtered = batch.filter(mask)
        if request.columns is not None:
            filtered = filtered.select(request.columns)
        if filtered.num_rows:
            yield filtered


def iter_remote_batches(
    connection: GatewayConnection,
    request: DataRequest,
    entries: list[ObjectEntry],
    *,
    batch_rows: int = 131_072,
    queue_parts: int = 2,
    memory_check: Callable[[str], None] | None = None,
    network_retries: int = 3,
    network_retry_backoff: float = 0.25,
    object_request_size: int = 12,
) -> Iterator:
    if network_retries < 0:
        raise ValueError("network_retries 不能为负数")
    if network_retry_backoff < 0:
        raise ValueError("network_retry_backoff 不能为负数")
    if object_request_size < 1:
        raise ValueError("object_request_size 必须 >= 1")
    ordered = sorted(
        entries,
        key=lambda item: (item.bucket_start, item.relative_path),
    )
    output: queue.Queue = queue.Queue(maxsize=max(1, queue_parts))
    done = object()
    stopped = threading.Event()

    def put(value) -> bool:
        while not stopped.is_set():
            try:
                output.put(value, timeout=0.25)
                return True
            except queue.Full:
                continue
        return False

    def producer() -> None:
        next_index = 0
        retries_used = 0
        try:
            while next_index < len(ordered) and not stopped.is_set():
                request_entries = ordered[
                    next_index : next_index + object_request_size
                ]
                expected_index = 0
                completed_before = next_index

                def consume(header: dict, source: LimitedReader) -> None:
                    nonlocal expected_index, next_index
                    if stopped.is_set():
                        raise RuntimeError("direct 消费者已结束")
                    if expected_index >= len(request_entries):
                        raise ValueError("网关返回了多余对象")
                    entry = request_entries[expected_index]
                    _validate_object_header(entry, header)
                    if memory_check is not None:
                        memory_check("remote_object_buffer")
                    buffer = io.BytesIO()
                    written = _copy_limited(source, buffer)
                    if written != entry.bytes:
                        raise RuntimeError(
                            f"direct 对象字节数错误: {entry.object_id}: "
                            f"{written} != {entry.bytes}"
                        )
                    buffer.seek(0)
                    if not put((header, buffer)):
                        raise RuntimeError("direct 消费者已结束")
                    expected_index += 1
                    next_index += 1

                try:
                    metrics = connection.consume_objects(
                        request_entries,
                        consume,
                    )
                    if (
                        metrics.objects != len(request_entries)
                        or expected_index != len(request_entries)
                    ):
                        raise EOFError(
                            "网关对象数错误: "
                            f"{metrics.objects}/{expected_index} != "
                            f"{len(request_entries)}"
                        )
                    retries_used = 0
                except BaseException as exc:
                    if stopped.is_set():
                        return
                    if next_index > completed_before:
                        retries_used = 0
                    remaining = len(ordered) - next_index
                    if remaining == 0:
                        break
                    if not _is_retryable_transfer_error(exc):
                        raise
                    connection.close()
                    retries_used += 1
                    if retries_used > network_retries:
                        raise _network_failure(
                            exc,
                            completed=next_index,
                            remaining=remaining,
                            retries=network_retries,
                        ) from exc
                    _retry_wait(retries_used, network_retry_backoff)
        except BaseException as exc:
            put(exc)
        finally:
            put(done)

    thread = threading.Thread(
        target=producer,
        name="mdapi-direct-producer",
        daemon=True,
    )
    thread.start()
    completed = False
    try:
        while True:
            item = output.get()
            if item is done:
                completed = True
                break
            if isinstance(item, BaseException):
                raise item
            _header, buffer = item
            yield from _iter_filtered_batches_from_parquet(
                buffer,
                request,
                batch_rows=batch_rows,
                memory_check=memory_check,
            )
    finally:
        stopped.set()
        if not completed:
            connection.close()
        thread.join(timeout=5)


def iter_cached_batches(
    objects: list[CachedObject],
    request: DataRequest,
    *,
    batch_rows: int = 131_072,
    memory_check: Callable[[str], None] | None = None,
) -> Iterator:
    for item in objects:
        if memory_check is not None:
            memory_check("cached_object")
        yield from _iter_filtered_batches_from_parquet(
            item.path,
            request,
            batch_rows=batch_rows,
            memory_check=memory_check,
        )
