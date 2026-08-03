from __future__ import annotations

import contextlib
import datetime as dt
import os
import sqlite3
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

from .catalog import ObjectEntry
from .model import DataRequest, UpdateMode


@dataclass(frozen=True)
class CachedObject:
    object_id: str
    dataset: str
    bucket_start: str
    bucket_end: str
    version: str
    path: Path
    bytes: int
    rows: int


@dataclass(frozen=True)
class CachePlan:
    remote_objects: list[ObjectEntry]
    selected_cached: list[CachedObject]
    missing_bytes: int
    buckets_to_fetch: tuple[str, ...]


class LocalCache:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve(strict=False)
        self.object_root = self.root / "objects"
        self.partial_root = self.root / ".partial"
        self.db_path = self.root / "cache.sqlite3"
        self.root.mkdir(parents=True, exist_ok=True)
        self.object_root.mkdir(parents=True, exist_ok=True)
        self.partial_root.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._initialize()

    def _connection(self) -> sqlite3.Connection:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(
                self.db_path,
                timeout=60,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            self._local.connection = connection
        return connection

    def _initialize(self) -> None:
        connection = self._connection()
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS buckets (
                dataset TEXT NOT NULL,
                bucket_start TEXT NOT NULL,
                bucket_end TEXT NOT NULL,
                version TEXT NOT NULL,
                cached_at TEXT NOT NULL,
                PRIMARY KEY (dataset, bucket_start)
            );
            CREATE TABLE IF NOT EXISTS objects (
                object_id TEXT PRIMARY KEY,
                dataset TEXT NOT NULL,
                bucket_start TEXT NOT NULL,
                bucket_end TEXT NOT NULL,
                version TEXT NOT NULL,
                path TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                rows INTEGER NOT NULL,
                FOREIGN KEY (dataset, bucket_start)
                    REFERENCES buckets(dataset, bucket_start)
                    ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS objects_bucket
                ON objects(dataset, bucket_start);
            """
        )

    @staticmethod
    def _group_remote(entries: list[ObjectEntry]) -> dict[str, list[ObjectEntry]]:
        grouped: dict[str, list[ObjectEntry]] = {}
        for entry in entries:
            grouped.setdefault(entry.bucket_start, []).append(entry)
        return grouped

    def _cached_bucket(self, dataset: str, bucket_start: str) -> sqlite3.Row | None:
        return self._connection().execute(
            """
            SELECT b.dataset, b.bucket_start, b.bucket_end, b.version,
                   (
                       SELECT count(*)
                       FROM objects o
                       WHERE o.dataset = b.dataset
                         AND o.bucket_start = b.bucket_start
                   ) AS object_count
            FROM buckets b
            WHERE b.dataset = ? AND b.bucket_start = ?
            """,
            (dataset, bucket_start),
        ).fetchone()

    def _cached_objects(
        self,
        dataset: str,
        bucket_starts: list[str],
    ) -> list[CachedObject]:
        if not bucket_starts:
            return []
        marks = ",".join("?" for _ in bucket_starts)
        rows = self._connection().execute(
            f"""
            SELECT object_id, dataset, bucket_start, bucket_end,
                   version, path, bytes, rows
            FROM objects
            WHERE dataset = ? AND bucket_start IN ({marks})
            ORDER BY bucket_start, path
            """,
            (dataset, *bucket_starts),
        )
        result = []
        for row in rows:
            path = self.root / row["path"]
            if not path.is_file() or path.stat().st_size != row["bytes"]:
                continue
            result.append(
                CachedObject(
                    object_id=row["object_id"],
                    dataset=row["dataset"],
                    bucket_start=row["bucket_start"],
                    bucket_end=row["bucket_end"],
                    version=row["version"],
                    path=path,
                    bytes=row["bytes"],
                    rows=row["rows"],
                )
            )
        return result

    def plan(
        self,
        request: DataRequest,
        remote_entries: list[ObjectEntry],
    ) -> CachePlan:
        grouped = self._group_remote(remote_entries)
        fetch_buckets: list[str] = []
        keep_buckets: list[str] = []
        cached_metadata: dict[str, sqlite3.Row] = {}
        for bucket_start, entries in sorted(grouped.items()):
            cached = self._cached_bucket(request.dataset, bucket_start)
            remote_versions = {entry.version for entry in entries}
            if len(remote_versions) != 1:
                raise RuntimeError(f"远端同一桶出现多个版本: {bucket_start}")
            remote_version = next(iter(remote_versions))
            if request.update == UpdateMode.FORCE:
                fetch = True
            elif cached is None:
                fetch = True
            elif request.update == UpdateMode.IF_CHANGED:
                fetch = cached["version"] != remote_version
            else:
                fetch = False
            if fetch:
                fetch_buckets.append(bucket_start)
            else:
                keep_buckets.append(bucket_start)
                cached_metadata[bucket_start] = cached

        remote_objects = [
            entry
            for bucket in fetch_buckets
            for entry in grouped[bucket]
        ]
        cached_objects = self._cached_objects(request.dataset, keep_buckets)
        valid_counts: dict[str, int] = {}
        for item in cached_objects:
            valid_counts[item.bucket_start] = (
                valid_counts.get(item.bucket_start, 0) + 1
            )
        unexpectedly_missing = [
            bucket
            for bucket in keep_buckets
            if int(cached_metadata[bucket]["object_count"]) < 1
            or valid_counts.get(bucket, 0)
            != int(cached_metadata[bucket]["object_count"])
        ]
        if unexpectedly_missing:
            fetch_buckets.extend(unexpectedly_missing)
            remote_objects.extend(
                entry
                for bucket in unexpectedly_missing
                for entry in grouped[bucket]
            )
            cached_objects = [
                item
                for item in cached_objects
                if item.bucket_start not in unexpectedly_missing
            ]
        return CachePlan(
            remote_objects=sorted(
                {entry.object_id: entry for entry in remote_objects}.values(),
                key=lambda entry: (entry.bucket_start, entry.relative_path),
            ),
            selected_cached=sorted(
                cached_objects,
                key=lambda item: (item.bucket_start, str(item.path)),
            ),
            missing_bytes=sum(entry.bytes for entry in remote_objects),
            buckets_to_fetch=tuple(sorted(set(fetch_buckets))),
        )

    @contextlib.contextmanager
    def partial_file(self, entry: ObjectEntry):
        fd, name = tempfile.mkstemp(
            prefix=f"{entry.object_id}.",
            suffix=".partial",
            dir=self.partial_root,
        )
        path = Path(name)
        try:
            with os.fdopen(fd, "wb") as handle:
                yield path, handle
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    def publish_downloads(
        self,
        downloaded: list[tuple[ObjectEntry, Path]],
    ) -> list[CachedObject]:
        if not downloaded:
            return []
        grouped: dict[tuple[str, str], list[tuple[ObjectEntry, Path]]] = {}
        for entry, partial in downloaded:
            grouped.setdefault((entry.dataset, entry.bucket_start), []).append(
                (entry, partial)
            )

        published: list[CachedObject] = []
        for (dataset, bucket_start), values in sorted(grouped.items()):
            versions = {entry.version for entry, _ in values}
            if len(versions) != 1:
                raise RuntimeError(f"缓存桶包含多个版本: {bucket_start}")
            ordered_values = sorted(
                values,
                key=lambda value: value[0].relative_path,
            )
            for ordinal, (entry, partial) in enumerate(ordered_values):
                target = (
                    self.object_root
                    / dataset
                    / f"trade_date={entry.trade_date}"
                    / f"bucket={bucket_start[11:16].replace(':', '')}"
                    / f"version={entry.version}"
                    / (
                        f"{ordinal:06d}-"
                        f"{Path(entry.relative_path).name}"
                    )
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(partial, target)
                published.append(
                    CachedObject(
                        object_id=entry.object_id,
                        dataset=entry.dataset,
                        bucket_start=entry.bucket_start,
                        bucket_end=entry.bucket_end,
                        version=entry.version,
                        path=target,
                        bytes=entry.bytes,
                        rows=entry.rows,
                    )
                )

        now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        connection = self._connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            for (dataset, bucket_start), values in sorted(grouped.items()):
                entry = values[0][0]
                connection.execute(
                    """
                    INSERT INTO buckets(
                        dataset, bucket_start, bucket_end, version, cached_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(dataset, bucket_start) DO UPDATE SET
                        bucket_end=excluded.bucket_end,
                        version=excluded.version,
                        cached_at=excluded.cached_at
                    """,
                    (
                        dataset,
                        bucket_start,
                        entry.bucket_end,
                        entry.version,
                        now,
                    ),
                )
                connection.execute(
                    "DELETE FROM objects WHERE dataset = ? AND bucket_start = ?",
                    (dataset, bucket_start),
                )
            for item in published:
                connection.execute(
                    """
                    INSERT INTO objects(
                        object_id, dataset, bucket_start, bucket_end,
                        version, path, bytes, rows
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.object_id,
                        item.dataset,
                        item.bucket_start,
                        item.bucket_end,
                        item.version,
                        str(item.path.relative_to(self.root)),
                        item.bytes,
                        item.rows,
                    ),
                )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        return published

    def selected(self, request: DataRequest) -> list[CachedObject]:
        rows = self._connection().execute(
            """
            SELECT object_id, dataset, bucket_start, bucket_end,
                   version, path, bytes, rows
            FROM objects
            WHERE dataset = ?
              AND bucket_end > ?
              AND bucket_start < ?
            ORDER BY bucket_start, path
            """,
            (request.dataset, request.start.isoformat(), request.end.isoformat()),
        )
        result: list[CachedObject] = []
        for row in rows:
            if not request.bucket_overlaps(
                row["bucket_start"],
                row["bucket_end"],
            ):
                continue
            path = self.root / row["path"]
            if not path.is_file():
                raise RuntimeError(f"缓存对象丢失: {path}")
            result.append(
                CachedObject(
                    object_id=row["object_id"],
                    dataset=row["dataset"],
                    bucket_start=row["bucket_start"],
                    bucket_end=row["bucket_end"],
                    version=row["version"],
                    path=path,
                    bytes=row["bytes"],
                    rows=row["rows"],
                )
            )
        return result
