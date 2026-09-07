from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import tempfile
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping

from .model import (
    BUCKET_SECONDS,
    DATASETS,
    DataRequest,
    iso_shanghai,
    parse_datetime,
)

CATALOG_FORMAT = "market-data-5m-catalog-v1"
CATALOG_INDEX_FORMAT = "market-data-5m-catalog-index-v2"
CATALOG_SHARD_FORMAT = "market-data-5m-catalog-shard-v2"
CATALOG_POINTER_FORMAT = "market-data-5m-catalog-pointer-v2"


@dataclass(frozen=True)
class SourceIdentity:
    relative_path: str
    size: int
    mtime_ns: int


def source_fingerprint(items: Iterable[SourceIdentity]) -> str:
    payload = [
        asdict(item)
        for item in sorted(items, key=lambda item: item.relative_path)
    ]
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class ObjectEntry:
    object_id: str
    dataset: str
    trade_date: str
    bucket_start: str
    bucket_end: str
    relative_path: str
    bytes: int
    rows: int
    uncompressed_bytes: int
    source_fingerprint: str
    version: str

    @classmethod
    def from_dict(cls, raw: dict) -> "ObjectEntry":
        return cls(
            object_id=str(raw["object_id"]),
            dataset=str(raw["dataset"]),
            trade_date=str(raw["trade_date"]),
            bucket_start=str(raw["bucket_start"]),
            bucket_end=str(raw["bucket_end"]),
            relative_path=str(raw["relative_path"]),
            bytes=int(raw["bytes"]),
            rows=int(raw["rows"]),
            uncompressed_bytes=int(raw.get("uncompressed_bytes", raw["bytes"] * 4)),
            source_fingerprint=str(raw["source_fingerprint"]),
            version=str(raw["version"]),
        )

    def as_dict(self) -> dict:
        return asdict(self)

    def overlaps(self, request: DataRequest) -> bool:
        return self.dataset == request.dataset and request.bucket_overlaps(
            self.bucket_start,
            self.bucket_end,
        )


@dataclass
class Catalog:
    generated_at: str
    objects: list[ObjectEntry]
    bucket_seconds: int = BUCKET_SECONDS
    format: str = CATALOG_FORMAT

    @classmethod
    def empty(cls) -> "Catalog":
        now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        return cls(generated_at=now, objects=[])

    @classmethod
    def from_dict(cls, raw: dict) -> "Catalog":
        if raw.get("format") != CATALOG_FORMAT:
            raise ValueError(f"不支持的 catalog 格式: {raw.get('format')!r}")
        if int(raw.get("bucket_seconds", 0)) != BUCKET_SECONDS:
            raise ValueError("catalog 不是 5 分钟切片")
        return cls(
            generated_at=str(raw["generated_at"]),
            objects=[ObjectEntry.from_dict(item) for item in raw["objects"]],
            bucket_seconds=BUCKET_SECONDS,
        )

    @classmethod
    def load(cls, path: Path) -> "Catalog":
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def as_dict(self) -> dict:
        return {
            "format": self.format,
            "generated_at": self.generated_at,
            "bucket_seconds": self.bucket_seconds,
            "objects": [entry.as_dict() for entry in self.objects],
        }

    def selected(self, request: DataRequest) -> list[ObjectEntry]:
        return sorted(
            (entry for entry in self.objects if entry.overlaps(request)),
            key=lambda entry: (entry.bucket_start, entry.relative_path),
        )

    def replace_partition(
        self,
        dataset: str,
        trade_date: str,
        entries: list[ObjectEntry],
    ) -> None:
        self.objects = [
            item
            for item in self.objects
            if not (item.dataset == dataset and item.trade_date == trade_date)
        ]
        self.objects.extend(entries)
        self.objects.sort(key=lambda item: (item.dataset, item.bucket_start, item.relative_path))
        self.generated_at = dt.datetime.now(dt.timezone.utc).isoformat(
            timespec="seconds"
        )


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".partial",
        dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise


def _now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _validated_trade_date(value: str) -> str:
    parsed = dt.date.fromisoformat(value)
    normalized = parsed.isoformat()
    if value != normalized:
        raise ValueError(f"trade_date 必须是 YYYY-MM-DD: {value!r}")
    return normalized


def _validated_version(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValueError(f"version 不能安全用于路径: {value!r}")
    return value


def catalog_shard_path(
    root: Path,
    dataset: str,
    trade_date: str,
    version: str | None = None,
) -> Path:
    if dataset not in DATASETS:
        raise ValueError(f"未知数据集 {dataset!r}")
    date = _validated_trade_date(trade_date)
    partition = (
        root / "catalog" / f"dataset={dataset}" / f"trade_date={date}"
    )
    if version is None:
        return partition / "current.json"
    return partition / f"version={_validated_version(version)}.json"


@dataclass(frozen=True)
class CatalogIndex:
    generated_at: str
    format: str = CATALOG_INDEX_FORMAT
    bucket_seconds: int = BUCKET_SECONDS
    pointer_format: str = CATALOG_POINTER_FORMAT
    shard_format: str = CATALOG_SHARD_FORMAT
    pointer_pattern: str = (
        "catalog/dataset={dataset}/trade_date={trade_date}/current.json"
    )
    version_pattern: str = (
        "catalog/dataset={dataset}/trade_date={trade_date}/"
        "version={version}.json"
    )

    @classmethod
    def from_dict(cls, raw: Mapping) -> "CatalogIndex":
        if raw.get("format") != CATALOG_INDEX_FORMAT:
            raise ValueError(
                f"不支持的 catalog index 格式: {raw.get('format')!r}"
            )
        if int(raw.get("bucket_seconds", 0)) != BUCKET_SECONDS:
            raise ValueError("catalog index 不是 5 分钟切片")
        if raw.get("pointer_format") != CATALOG_POINTER_FORMAT:
            raise ValueError("catalog index 的 pointer_format 不受支持")
        if raw.get("shard_format") != CATALOG_SHARD_FORMAT:
            raise ValueError("catalog index 的 shard_format 不受支持")
        expected_pointer = (
            "catalog/dataset={dataset}/trade_date={trade_date}/current.json"
        )
        expected_version = (
            "catalog/dataset={dataset}/trade_date={trade_date}/"
            "version={version}.json"
        )
        if raw.get("pointer_pattern") != expected_pointer:
            raise ValueError("catalog index 的 pointer_pattern 不受支持")
        if raw.get("version_pattern") != expected_version:
            raise ValueError("catalog index 的 version_pattern 不受支持")
        return cls(generated_at=str(raw["generated_at"]))

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class CatalogShard:
    generated_at: str
    dataset: str
    trade_date: str
    version: str
    objects: tuple[ObjectEntry, ...]
    format: str = CATALOG_SHARD_FORMAT
    bucket_seconds: int = BUCKET_SECONDS

    @classmethod
    def from_dict(cls, raw: Mapping) -> "CatalogShard":
        if raw.get("format") != CATALOG_SHARD_FORMAT:
            raise ValueError(
                f"不支持的 catalog shard 格式: {raw.get('format')!r}"
            )
        if int(raw.get("bucket_seconds", 0)) != BUCKET_SECONDS:
            raise ValueError("catalog shard 不是 5 分钟切片")
        dataset = str(raw["dataset"])
        trade_date = _validated_trade_date(str(raw["trade_date"]))
        version = _validated_version(str(raw["version"]))
        if dataset not in DATASETS:
            raise ValueError(f"未知数据集 {dataset!r}")
        objects = tuple(ObjectEntry.from_dict(item) for item in raw["objects"])
        for entry in objects:
            if (
                entry.dataset != dataset
                or entry.trade_date != trade_date
                or entry.version != version
            ):
                raise ValueError(
                    "catalog shard 中对象的数据集、交易日或版本与分片不一致"
                )
        return cls(
            generated_at=str(raw["generated_at"]),
            dataset=dataset,
            trade_date=trade_date,
            version=version,
            objects=objects,
        )

    def as_dict(self) -> dict:
        return {
            "format": self.format,
            "generated_at": self.generated_at,
            "bucket_seconds": self.bucket_seconds,
            "dataset": self.dataset,
            "trade_date": self.trade_date,
            "version": self.version,
            "objects": [entry.as_dict() for entry in self.objects],
        }


@dataclass(frozen=True)
class CatalogPointer:
    generated_at: str
    dataset: str
    trade_date: str
    version: str
    format: str = CATALOG_POINTER_FORMAT

    @classmethod
    def from_dict(cls, raw: Mapping) -> "CatalogPointer":
        if raw.get("format") != CATALOG_POINTER_FORMAT:
            raise ValueError(
                f"不支持的 catalog pointer 格式: {raw.get('format')!r}"
            )
        dataset = str(raw["dataset"])
        if dataset not in DATASETS:
            raise ValueError(f"未知数据集 {dataset!r}")
        return cls(
            generated_at=str(raw["generated_at"]),
            dataset=dataset,
            trade_date=_validated_trade_date(str(raw["trade_date"])),
            version=_validated_version(str(raw["version"])),
        )

    def as_dict(self) -> dict:
        return asdict(self)


def _request_dates(request: DataRequest) -> Iterable[str]:
    current = request.start.date()
    last = (request.end - dt.timedelta(microseconds=1)).date()
    while current <= last:
        yield current.isoformat()
        current += dt.timedelta(days=1)


class CatalogStore:
    """Read v2 date shards lazily, with bounded LRU caching.

    Legacy v1 catalogs remain readable for migration and compatibility, but
    new publishers should always use the v2 sharded layout.
    """

    def __init__(self, root: Path, *, shard_cache_size: int = 64) -> None:
        if shard_cache_size < 1:
            raise ValueError("shard_cache_size 必须 >= 1")
        self.root = root.resolve(strict=True)
        self.index_path = self.root / "catalog.json"
        self.shard_cache_size = shard_cache_size
        self._lock = threading.RLock()
        self._index_signature: tuple[int, int, int] | None = None
        self._index: CatalogIndex | None = None
        self._legacy: Catalog | None = None
        self._pointers: OrderedDict[
            Path, tuple[tuple[int, int, int], CatalogPointer]
        ] = OrderedDict()
        self._shards: OrderedDict[
            Path, tuple[tuple[int, int, int], CatalogShard]
        ] = OrderedDict()
        self._refresh_index()

    @staticmethod
    def _signature(stat: os.stat_result) -> tuple[int, int, int]:
        return (stat.st_ino, stat.st_mtime_ns, stat.st_size)

    @staticmethod
    def initialize_for_write(root: Path) -> "CatalogStore":
        root = root.resolve(strict=False)
        root.mkdir(parents=True, exist_ok=True)
        index_path = root / "catalog.json"
        if not index_path.exists():
            atomic_write_json(
                index_path,
                CatalogIndex(generated_at=_now_utc()).as_dict(),
            )
        store = CatalogStore(root)
        if store.format == CATALOG_FORMAT:
            raise ValueError(
                "现有目录仍是单文件 catalog v1；请先运行 "
                "migrate_catalog_v2.py"
            )
        return store

    def _refresh_index(self) -> None:
        signature = self._signature(self.index_path.stat())
        with self._lock:
            if signature == self._index_signature:
                return
            with self.index_path.open("r", encoding="utf-8") as handle:
                signature = self._signature(os.fstat(handle.fileno()))
                raw = json.load(handle)
            catalog_format = raw.get("format")
            if catalog_format == CATALOG_FORMAT:
                self._legacy = Catalog.from_dict(raw)
                self._index = None
                self._pointers.clear()
                self._shards.clear()
            elif catalog_format == CATALOG_INDEX_FORMAT:
                self._index = CatalogIndex.from_dict(raw)
                self._legacy = None
            else:
                raise ValueError(
                    f"不支持的 catalog 格式: {catalog_format!r}"
                )
            self._index_signature = signature

    @property
    def format(self) -> str:
        self._refresh_index()
        with self._lock:
            return CATALOG_FORMAT if self._legacy is not None else CATALOG_INDEX_FORMAT

    @property
    def generated_at(self) -> str:
        self._refresh_index()
        with self._lock:
            if self._legacy is not None:
                return self._legacy.generated_at
            assert self._index is not None
            return self._index.generated_at

    @property
    def cached_shards(self) -> int:
        with self._lock:
            return len(self._shards)

    @property
    def cached_pointers(self) -> int:
        with self._lock:
            return len(self._pointers)

    def _load_pointer(
        self,
        dataset: str,
        trade_date: str,
    ) -> CatalogPointer | None:
        path = catalog_shard_path(self.root, dataset, trade_date)
        try:
            signature = self._signature(path.stat())
            with self._lock:
                cached = self._pointers.get(path)
                if cached is not None and cached[0] == signature:
                    self._pointers.move_to_end(path)
                    return cached[1]
            with path.open("r", encoding="utf-8") as handle:
                signature = self._signature(os.fstat(handle.fileno()))
                raw = json.load(handle)
        except FileNotFoundError:
            with self._lock:
                self._pointers.pop(path, None)
            return None
        pointer = CatalogPointer.from_dict(raw)
        if pointer.dataset != dataset or pointer.trade_date != trade_date:
            raise ValueError(f"catalog pointer 路径与内容不一致: {path}")
        with self._lock:
            self._pointers[path] = (signature, pointer)
            self._pointers.move_to_end(path)
            while len(self._pointers) > self.shard_cache_size:
                self._pointers.popitem(last=False)
        return pointer

    def _load_shard(
        self,
        dataset: str,
        trade_date: str,
        version: str | None = None,
    ) -> CatalogShard | None:
        from_pointer = version is None
        if version is None:
            pointer = self._load_pointer(dataset, trade_date)
            if pointer is None:
                return None
            version = pointer.version
        version = _validated_version(version)
        path = catalog_shard_path(self.root, dataset, trade_date, version)
        try:
            signature = self._signature(path.stat())
            with self._lock:
                cached = self._shards.get(path)
                if cached is not None and cached[0] == signature:
                    self._shards.move_to_end(path)
                    return cached[1]
            with path.open("r", encoding="utf-8") as handle:
                signature = self._signature(os.fstat(handle.fileno()))
                raw = json.load(handle)
        except FileNotFoundError:
            with self._lock:
                self._shards.pop(path, None)
            if from_pointer:
                raise FileNotFoundError(f"已发布的 current 指向缺失的版本索引: {path}")
            return None
        shard = CatalogShard.from_dict(raw)
        if (
            shard.dataset != dataset
            or shard.trade_date != trade_date
            or shard.version != version
        ):
            raise ValueError(f"catalog shard 路径与内容不一致: {path}")
        with self._lock:
            self._shards[path] = (signature, shard)
            self._shards.move_to_end(path)
            while len(self._shards) > self.shard_cache_size:
                self._shards.popitem(last=False)
        return shard

    def selected(self, request: DataRequest) -> list[ObjectEntry]:
        self._refresh_index()
        with self._lock:
            legacy = self._legacy
        if legacy is not None:
            return legacy.selected(request)
        entries: list[ObjectEntry] = []
        for trade_date in _request_dates(request):
            shard = self._load_shard(request.dataset, trade_date)
            if shard is None:
                continue
            entries.extend(entry for entry in shard.objects if entry.overlaps(request))
        return sorted(
            entries,
            key=lambda entry: (entry.bucket_start, entry.relative_path),
        )

    def all_current_entries(self) -> list[ObjectEntry]:
        """Scan all current pointers for explicit administrative tooling."""
        self._refresh_index()
        with self._lock:
            legacy = self._legacy
        if legacy is not None:
            return list(legacy.objects)
        entries: list[ObjectEntry] = []
        for path in sorted((self.root / "catalog").glob(
            "dataset=*/trade_date=*/current.json"
        )):
            dataset = path.parent.parent.name.removeprefix("dataset=")
            trade_date = path.parent.name.removeprefix("trade_date=")
            shard = self._load_shard(dataset, trade_date)
            if shard is not None:
                entries.extend(shard.objects)
        return sorted(
            entries,
            key=lambda entry: (
                entry.dataset,
                entry.bucket_start,
                entry.relative_path,
            ),
        )

    def selected_refs(
        self,
        references: list[Mapping[str, str]],
    ) -> list[ObjectEntry]:
        object_ids = [str(item.get("object_id", "")) for item in references]
        if any(not value for value in object_ids):
            raise ValueError("每个对象引用都必须包含 object_id")
        if len(object_ids) != len(set(object_ids)):
            raise ValueError("object_id 不能重复")
        self._refresh_index()
        with self._lock:
            legacy = self._legacy
        if legacy is not None:
            by_id = {entry.object_id: entry for entry in legacy.objects}
        else:
            by_id: dict[str, ObjectEntry] = {}
            grouped: set[tuple[str, str, str]] = set()
            for reference in references:
                dataset = str(reference.get("dataset", ""))
                trade_date = str(reference.get("trade_date", ""))
                version = str(reference.get("version", ""))
                if dataset not in DATASETS:
                    raise ValueError(f"对象引用的数据集非法: {dataset!r}")
                grouped.add(
                    (
                        dataset,
                        _validated_trade_date(trade_date),
                        _validated_version(version),
                    )
                )
            for dataset, trade_date, version in grouped:
                shard = self._load_shard(dataset, trade_date, version)
                if shard is not None:
                    by_id.update(
                        (entry.object_id, entry) for entry in shard.objects
                    )
        missing = [value for value in object_ids if value not in by_id]
        if missing:
            raise ValueError(f"catalog 中不存在这些 object_id: {missing[:5]}")
        entries = [by_id[value] for value in object_ids]
        for reference, entry in zip(references, entries):
            dataset = reference.get("dataset")
            trade_date = reference.get("trade_date")
            if dataset is not None and str(dataset) != entry.dataset:
                raise ValueError(f"对象引用 dataset 不匹配: {entry.object_id}")
            if trade_date is not None and str(trade_date) != entry.trade_date:
                raise ValueError(f"对象引用 trade_date 不匹配: {entry.object_id}")
            version = reference.get("version")
            if version is not None and str(version) != entry.version:
                raise ValueError(f"对象引用 version 不匹配: {entry.object_id}")
        return sorted(
            entries,
            key=lambda entry: (entry.bucket_start, entry.relative_path),
        )

    def replace_partition(
        self,
        dataset: str,
        trade_date: str,
        entries: list[ObjectEntry],
    ) -> bool:
        self._refresh_index()
        if self.format != CATALOG_INDEX_FORMAT:
            raise ValueError("不能向 legacy catalog v1 增量发布")
        date = _validated_trade_date(trade_date)
        if dataset not in DATASETS:
            raise ValueError(f"未知数据集 {dataset!r}")
        ordered = tuple(
            sorted(entries, key=lambda item: (item.bucket_start, item.relative_path))
        )
        if not ordered:
            raise ValueError("不能发布空的catalog分片")
        for entry in ordered:
            if entry.dataset != dataset or entry.trade_date != date:
                raise ValueError("发布对象的数据集或交易日与目标分片不一致")
        versions = {entry.version for entry in ordered}
        if len(versions) != 1:
            raise ValueError("同一catalog分片必须只有一个版本")
        version = _validated_version(next(iter(versions)))
        current = self._load_shard(dataset, date)
        if current is not None and current.objects == ordered:
            return False
        generated_at = _now_utc()
        version_path = catalog_shard_path(self.root, dataset, date, version)
        existing_version = self._load_shard(dataset, date, version)
        if existing_version is not None:
            if existing_version.objects != ordered:
                raise ValueError(
                    f"不可变catalog版本内容冲突: {dataset} {date} {version}"
                )
        else:
            shard = CatalogShard(
                generated_at=generated_at,
                dataset=dataset,
                trade_date=date,
                version=version,
                objects=ordered,
            )
            atomic_write_json(version_path, shard.as_dict())
        pointer_path = catalog_shard_path(self.root, dataset, date)
        pointer = CatalogPointer(
            generated_at=generated_at,
            dataset=dataset,
            trade_date=date,
            version=version,
        )
        atomic_write_json(pointer_path, pointer.as_dict())
        atomic_write_json(
            self.index_path,
            CatalogIndex(generated_at=generated_at).as_dict(),
        )
        with self._lock:
            self._index_signature = None
            self._pointers.pop(pointer_path, None)
            self._shards.pop(version_path, None)
        return True


def migrate_legacy_catalog(root: Path, *, keep_backup: bool = False) -> dict:
    root = root.resolve(strict=True)
    index_path = root / "catalog.json"
    raw = json.loads(index_path.read_text(encoding="utf-8"))
    if raw.get("format") == CATALOG_INDEX_FORMAT:
        return {"status": "already_v2", "shards": 0, "objects": 0}
    legacy = Catalog.from_dict(raw)
    grouped: dict[tuple[str, str], list[ObjectEntry]] = {}
    for entry in legacy.objects:
        grouped.setdefault((entry.dataset, entry.trade_date), []).append(entry)
    generated_at = _now_utc()
    for (dataset, trade_date), entries in sorted(grouped.items()):
        versions = {entry.version for entry in entries}
        if len(versions) != 1:
            raise ValueError(
                f"legacy catalog同一分区有多个版本: {dataset} {trade_date}"
            )
        version = _validated_version(next(iter(versions)))
        shard = CatalogShard(
            generated_at=generated_at,
            dataset=dataset,
            trade_date=trade_date,
            version=version,
            objects=tuple(
                sorted(
                    entries,
                    key=lambda item: (item.bucket_start, item.relative_path),
                )
            ),
        )
        atomic_write_json(
            catalog_shard_path(root, dataset, trade_date, version),
            shard.as_dict(),
        )
        atomic_write_json(
            catalog_shard_path(root, dataset, trade_date),
            CatalogPointer(
                generated_at=generated_at,
                dataset=dataset,
                trade_date=trade_date,
                version=version,
            ).as_dict(),
        )
    if keep_backup:
        backup = root / "catalog.v1.backup.json"
        if not backup.exists():
            atomic_write_json(backup, raw)
    atomic_write_json(
        index_path,
        CatalogIndex(generated_at=generated_at).as_dict(),
    )
    return {
        "status": "migrated",
        "shards": len(grouped),
        "objects": len(legacy.objects),
        "backup": str(root / "catalog.v1.backup.json") if keep_backup else None,
    }


def manifest_summary(entries: list[ObjectEntry]) -> dict:
    return {
        "objects": len(entries),
        "rows": sum(item.rows for item in entries),
        "source_bytes": sum(item.bytes for item in entries),
        "uncompressed_bytes": sum(item.uncompressed_bytes for item in entries),
        "first_bucket": entries[0].bucket_start if entries else None,
        "last_bucket": entries[-1].bucket_end if entries else None,
    }


def bucket_iso(trade_date: str, hhmm: int) -> tuple[str, str]:
    hour, minute = divmod(hhmm, 100)
    start = dt.datetime.fromisoformat(trade_date).replace(
        hour=hour,
        minute=minute,
        tzinfo=parse_datetime(f"{trade_date}T00:00:00").tzinfo,
    )
    end = start + dt.timedelta(seconds=BUCKET_SECONDS)
    return iso_shanghai(start), iso_shanghai(end)
