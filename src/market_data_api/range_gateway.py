"""Standard-library-only helpers for immutable Parquet byte access."""

from __future__ import annotations

import base64
import struct
import threading
import sqlite3
import time
import zlib
from collections import OrderedDict


CAPABILITIES = ("parquet_footers_v1", "range_bundles_v1", "http_range_v1")
MAX_FOOTER_BYTES = 16 * 1024**2
MAX_METADATA_OBJECTS = 32
MAX_OBJECT_RANGES = 4096


def decompress_footer(payload):
    """Bound decompression before constructing any Arrow metadata objects."""
    decoder = zlib.decompressobj()
    result = decoder.decompress(payload, MAX_FOOTER_BYTES + 9)
    if len(result) > MAX_FOOTER_BYTES + 8 or not decoder.eof or decoder.unused_data:
        raise ValueError("压缩元数据不完整或超过 16MiB 限额")
    return result


class FooterStore:
    """Optional bounded SQLite cache of compressed metadata outside the data root."""

    def __init__(self, path, capacity=512 * 1024**2):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.capacity = capacity
        self._local = threading.local()
        self._connection().execute("""CREATE TABLE IF NOT EXISTS footers (
            cache_key TEXT PRIMARY KEY, payload BLOB NOT NULL, stored_bytes INTEGER NOT NULL,
            created REAL NOT NULL)""")
        self._connection().execute(
            "CREATE INDEX IF NOT EXISTS footer_age ON footers(created)"
        )
        self._puts = 0
        self._lock = threading.Lock()

    def _connection(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            self._local.connection = connection
        return connection

    def get(self, key):
        try:
            row = (
                self._connection()
                .execute("SELECT payload FROM footers WHERE cache_key=?", (repr(key),))
                .fetchone()
            )
            return decompress_footer(row[0]) if row else None
        except (sqlite3.Error, zlib.error, ValueError):
            return (
                None  # A metadata cache failure must fall back to the original footer.
            )

    def put(self, key, footer):
        payload = zlib.compress(footer, level=1)
        try:
            connection = self._connection()
            connection.execute(
                "INSERT OR REPLACE INTO footers VALUES (?, ?, ?, ?)",
                (repr(key), payload, len(payload), time.time()),
            )
            with self._lock:
                self._puts += 1
                if self._puts % 128:
                    return
                total = connection.execute(
                    "SELECT COALESCE(SUM(stored_bytes),0) FROM footers"
                ).fetchone()[0]
                while total > self.capacity:
                    connection.execute(
                        "DELETE FROM footers WHERE cache_key IN (SELECT cache_key FROM footers ORDER BY created LIMIT 128)"
                    )
                    total = connection.execute(
                        "SELECT COALESCE(SUM(stored_bytes),0) FROM footers"
                    ).fetchone()[0]
        except sqlite3.Error:
            pass


class FooterCache:
    """Bounded, shared cache containing metadata only, never data column chunks."""

    def __init__(self, capacity: int = 64 * 1024**2, store=None):
        self.capacity = capacity
        self.bytes = 0
        self._items = OrderedDict()
        self._lock = threading.Lock()
        self.store = store

    def get(self, item):
        stat = item.path.stat()
        key = (
            item.entry.object_id,
            item.entry.version,
            stat.st_ino,
            stat.st_mtime_ns,
            stat.st_size,
        )
        with self._lock:
            if key in self._items:
                self._items.move_to_end(key)
                return self._items[key]
        footer = self.store.get(key) if self.store is not None else None
        if footer is None:
            with item.path.open("rb") as handle:
                if stat.st_size < 12:
                    raise ValueError("Parquet 对象过短")
                handle.seek(-8, 2)
                tail = handle.read(8)
                length = struct.unpack("<I", tail[:4])[0]
                if (
                    tail[4:] != b"PAR1"
                    or length > MAX_FOOTER_BYTES
                    or length + 12 > stat.st_size
                ):
                    raise ValueError("Parquet footer 非法或超过 16MiB 限额")
                handle.seek(-8 - length, 2)
                footer = handle.read(length) + tail
                if len(footer) != length + 8:
                    raise EOFError("Parquet footer 读取不完整")
            if self.store is not None:
                self.store.put(key, footer)
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
            "footer_codec": "zlib",
            "objects": [
                {
                    "object_id": item.entry.object_id,
                    "version": item.entry.version,
                    "file_bytes": item.entry.bytes,
                    "footer": base64.b64encode(
                        zlib.compress(self.get(item), level=1)
                    ).decode("ascii"),
                }
                for item in selected
            ],
        }


def validate_ranges(raw, file_bytes):
    """Validate sorted non-overlapping (offset, length) intervals before I/O."""
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_OBJECT_RANGES:
        raise ValueError(f"ranges 必须包含 1 到 {MAX_OBJECT_RANGES} 个字节段")
    result = []
    previous_end = 0
    for value in raw:
        if (
            not isinstance(value, list)
            or len(value) != 2
            or any(type(v) is not int for v in value)
        ):
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
