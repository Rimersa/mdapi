"""Standard-library-only helpers for immutable Parquet byte access."""

from __future__ import annotations

import base64
import struct
import threading
from collections import OrderedDict


CAPABILITIES = ("parquet_footers_v1", "range_bundles_v1", "http_range_v1")
MAX_FOOTER_BYTES = 16 * 1024**2
MAX_METADATA_OBJECTS = 32
MAX_OBJECT_RANGES = 4096


class FooterCache:
    """Bounded, shared cache containing metadata only, never data column chunks."""

    def __init__(self, capacity: int = 32 * 1024**2):
        self.capacity = capacity
        self.bytes = 0
        self._items = OrderedDict()
        self._lock = threading.Lock()

    def get(self, item):
        stat = item.path.stat()
        key = (item.entry.object_id, stat.st_ino, stat.st_mtime_ns, stat.st_size)
        with self._lock:
            if key in self._items:
                self._items.move_to_end(key)
                return self._items[key]
        with item.path.open("rb") as handle:
            if stat.st_size < 12:
                raise ValueError("Parquet 对象过短")
            handle.seek(-8, 2)
            tail = handle.read(8)
            length = struct.unpack("<I", tail[:4])[0]
            if tail[4:] != b"PAR1" or length > MAX_FOOTER_BYTES or length + 12 > stat.st_size:
                raise ValueError("Parquet footer 非法或超过 16MiB 限额")
            handle.seek(-8 - length, 2)
            footer = handle.read(length) + tail
            if len(footer) != length + 8:
                raise EOFError("Parquet footer 读取不完整")
        with self._lock:
            previous = self._items.pop(key, None)
            if previous is not None:
                self.bytes -= len(previous)
            if len(footer) <= self.capacity:
                self._items[key] = footer
                self.bytes += len(footer)
                while self.bytes > self.capacity:
                    _, old = self._items.popitem(last=False)
                    self.bytes -= len(old)
        return footer

    def payload(self, selected):
        if len(selected) > MAX_METADATA_OBJECTS:
            raise OverflowError(f"每次最多读取 {MAX_METADATA_OBJECTS} 个对象的元数据")
        return {
            "format": "market-data-parquet-footers-v1",
            "objects": [{
                "object_id": item.entry.object_id,
                "version": item.entry.version,
                "file_bytes": item.entry.bytes,
                "footer": base64.b64encode(self.get(item)).decode("ascii"),
            } for item in selected],
        }


def validate_ranges(raw, file_bytes):
    """Validate sorted non-overlapping (offset, length) intervals before I/O."""
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_OBJECT_RANGES:
        raise ValueError(f"ranges 必须包含 1 到 {MAX_OBJECT_RANGES} 个字节段")
    result = []
    previous_end = 0
    for value in raw:
        if (not isinstance(value, list) or len(value) != 2
                or any(type(v) is not int for v in value)):
            raise ValueError("每个字节段必须是 [offset, length] 整数数组")
        offset, length = value
        if offset < previous_end or length <= 0 or offset + length > file_bytes:
            raise ValueError("字节段越界、重叠、未排序或长度不为正")
        result.append((offset, length))
        previous_end = offset + length
    return tuple(result)


def parse_http_range(value: str | None, file_bytes: int):
    """Support a standard single HTTP byte range, including suffix ranges."""
    if value is None:
        return 0, file_bytes
    if not value.startswith("bytes=") or "," in value:
        raise ValueError("仅支持单个 bytes 范围；批量读取使用 /v1/ranges")
    first, separator, last = value[6:].partition("-")
    if not separator or not (first or last):
        raise ValueError("Range 格式非法")
    if not first:
        count = int(last)
        if count <= 0:
            raise ValueError("后缀 Range 长度必须为正")
        count = min(file_bytes, count)
        return file_bytes - count, count
    start = int(first)
    end = min(int(last), file_bytes - 1) if last else file_bytes - 1
    if start < 0 or start >= file_bytes or end < start:
        raise ValueError("Range 越界")
    return start, end - start + 1
