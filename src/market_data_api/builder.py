from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from .catalog import (
    CatalogStore,
    ObjectEntry,
    SourceIdentity,
    atomic_write_json,
    bucket_iso,
    source_fingerprint,
)
from .model import DATASETS

DEFAULT_SOURCE_ROOT = Path("/data/market_data_lake/lake/curated")
DEFAULT_STAGING_ROOT = Path("/dev/shm/market_data_api_builder")


@dataclass(frozen=True)
class BuildResult:
    dataset: str
    trade_date: str
    source_bytes: int
    output_bytes: int
    rows: int
    objects: int
    version: str
    elapsed_seconds: float
    status: str


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def validate_roots(source_root: Path, dest_root: Path, staging_root: Path) -> None:
    source = source_root.resolve(strict=True)
    if source != DEFAULT_SOURCE_ROOT.resolve(strict=True):
        raise ValueError(
            f"source-root 必须精确等于只读源目录 {DEFAULT_SOURCE_ROOT}，实际为 {source}"
        )
    dest = dest_root.resolve(strict=False)
    staging = staging_root.resolve(strict=False)
    if dest == source or _is_relative_to(dest, source):
        raise ValueError("dest-root 禁止位于原始数据目录中")
    if source == dest or _is_relative_to(source, dest):
        raise ValueError("dest-root 禁止包含原始数据目录")
    if staging == source or _is_relative_to(staging, source):
        raise ValueError("staging-root 禁止位于原始数据目录中")
    if dest == Path("/") or staging == Path("/"):
        raise ValueError("禁止使用根目录作为输出或暂存目录")


def discover_dates(source_root: Path, datasets: tuple[str, ...]) -> list[str]:
    date_sets: list[set[str]] = []
    for dataset in datasets:
        values = {
            item.name.removeprefix("trade_date=")
            for item in (source_root / dataset).glob("trade_date=????-??-??")
            if item.is_dir()
        }
        date_sets.append(values)
    if not date_sets:
        return []
    return sorted(set.intersection(*date_sets))


def source_files(
    source_root: Path,
    dataset: str,
    trade_date: str,
) -> tuple[list[Path], list[SourceIdentity]]:
    partition = source_root / dataset / f"trade_date={trade_date}"
    files = sorted(partition.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"没有源文件: {partition}")
    identities: list[SourceIdentity] = []
    for path in files:
        stat = path.stat()
        identities.append(
            SourceIdentity(
                relative_path=str(path.relative_to(source_root)),
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )
        )
    return files, identities


def _source_rows(files: list[Path]) -> int:
    import pyarrow.parquet as pq

    return sum(pq.ParquetFile(path).metadata.num_rows for path in files)


def _parquet_stats(path: Path) -> tuple[int, int, str]:
    import pyarrow.parquet as pq

    metadata = pq.ParquetFile(path).metadata
    uncompressed = 0
    for index in range(metadata.num_row_groups):
        uncompressed += metadata.row_group(index).total_byte_size
    schema_hash = hashlib.sha256(
        metadata.schema.to_arrow_schema().serialize().to_pybytes()
    ).hexdigest()
    return metadata.num_rows, uncompressed, schema_hash


def _partition_with_polars(
    files: list[Path],
    stage_objects: Path,
    *,
    threads: int,
) -> None:
    os.environ["POLARS_MAX_THREADS"] = str(max(1, threads))
    import polars as pl

    ti = pl.col("time_int").cast(pl.Int64)
    bucket = (
        (ti // 10_000_000) * 100
        + (((ti // 100_000) % 100) // 5) * 5
    ).cast(pl.Int32)
    lazy = (
        pl.scan_parquet(
            [str(path) for path in files],
            hive_partitioning=False,
            rechunk=False,
            low_memory=True,
            cache=False,
        )
        .with_columns(bucket.alias("_bucket_5m"))
    )
    output = pl.PartitionBy(
        stage_objects,
        key="_bucket_5m",
        include_key=False,
        max_rows_per_file=2_000_000,
        approximate_bytes_per_file=256 * 1024**2,
    )
    lazy.sink_parquet(
        output,
        compression="zstd",
        compression_level=1,
        statistics=True,
        row_group_size=131_072,
        maintain_order=False,
        mkdir=True,
        engine="streaming",
    )


def _collect_entries(
    stage_objects: Path,
    *,
    dest_root: Path,
    final_dir: Path,
    dataset: str,
    trade_date: str,
    fingerprint: str,
    version: str,
) -> tuple[list[ObjectEntry], str]:
    entries: list[ObjectEntry] = []
    schema_hashes: set[str] = set()
    for path in sorted(stage_objects.rglob("*.parquet")):
        parent = path.parent.name
        if not parent.startswith("_bucket_5m="):
            raise RuntimeError(f"无法识别分桶目录: {path}")
        hhmm = int(parent.split("=", 1)[1])
        bucket_start, bucket_end = bucket_iso(trade_date, hhmm)
        rows, uncompressed, schema_hash = _parquet_stats(path)
        schema_hashes.add(schema_hash)
        relative_inside = path.relative_to(stage_objects)
        published = final_dir / relative_inside
        relative_path = str(published.relative_to(dest_root))
        object_id = hashlib.sha256(
            f"{version}\0{relative_path}\0{path.stat().st_size}".encode()
        ).hexdigest()
        entries.append(
            ObjectEntry(
                object_id=object_id,
                dataset=dataset,
                trade_date=trade_date,
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                relative_path=relative_path,
                bytes=path.stat().st_size,
                rows=rows,
                uncompressed_bytes=uncompressed,
                source_fingerprint=fingerprint,
                version=version,
            )
        )
    if not entries:
        raise RuntimeError(f"构建没有产生 Parquet: {dataset} {trade_date}")
    if len(schema_hashes) != 1:
        raise RuntimeError(f"同一分区产生了多个 schema: {sorted(schema_hashes)}")
    return entries, next(iter(schema_hashes))


def _safe_remove_own_tree(path: Path, allowed_parent: Path) -> None:
    resolved = path.resolve(strict=False)
    parent = allowed_parent.resolve(strict=False)
    if resolved == parent or not _is_relative_to(resolved, parent):
        raise RuntimeError(f"拒绝清理非专属路径: {resolved}")
    if not path.name.startswith((".build-", ".incoming-")):
        raise RuntimeError(f"拒绝清理非构建临时目录: {resolved}")
    shutil.rmtree(path, ignore_errors=False)


def _load_partition_manifest(final_dir: Path) -> list[ObjectEntry]:
    raw = json.loads((final_dir / "manifest.json").read_text(encoding="utf-8"))
    return [ObjectEntry.from_dict(item) for item in raw["objects"]]


def build_partition(
    *,
    source_root: Path,
    dest_root: Path,
    staging_root: Path,
    dataset: str,
    trade_date: str,
    threads: int,
    catalog: CatalogStore,
) -> BuildResult:
    started = time.monotonic()
    files, identities = source_files(source_root, dataset, trade_date)
    fingerprint = source_fingerprint(identities)
    version = fingerprint[:16]
    final_dir = (
        dest_root
        / "objects"
        / dataset
        / f"trade_date={trade_date}"
        / f"version={version}"
    )
    source_bytes = sum(item.size for item in identities)

    if (final_dir / "manifest.json").is_file():
        entries = _load_partition_manifest(final_dir)
        catalog.replace_partition(dataset, trade_date, entries)
        return BuildResult(
            dataset=dataset,
            trade_date=trade_date,
            source_bytes=source_bytes,
            output_bytes=sum(item.bytes for item in entries),
            rows=sum(item.rows for item in entries),
            objects=len(entries),
            version=version,
            elapsed_seconds=time.monotonic() - started,
            status="already_published",
        )
    if final_dir.exists():
        raise RuntimeError(f"发现不完整的已发布目录，拒绝覆盖: {final_dir}")

    stage_free = shutil.disk_usage(staging_root).free
    required_stage = max(2 * 1024**3, int(source_bytes * 1.5))
    if stage_free < required_stage:
        raise RuntimeError(
            f"暂存空间不足: available={stage_free}, required={required_stage}"
        )
    dest_free = shutil.disk_usage(dest_root).free
    required_dest = max(2 * 1024**3, int(source_bytes * 1.3))
    if dest_free < required_dest:
        raise RuntimeError(
            f"派生盘空间不足: available={dest_free}, required={required_dest}"
        )

    stage_dir = staging_root / f".build-{dataset}-{trade_date}-{uuid.uuid4().hex}"
    incoming = final_dir.parent / f".incoming-{version}-{uuid.uuid4().hex}"
    stage_objects = stage_dir / "objects"
    stage_dir.mkdir(parents=True)
    try:
        _partition_with_polars(files, stage_objects, threads=threads)
        entries, schema_hash = _collect_entries(
            stage_objects,
            dest_root=dest_root,
            final_dir=final_dir,
            dataset=dataset,
            trade_date=trade_date,
            fingerprint=fingerprint,
            version=version,
        )
        source_rows = _source_rows(files)
        _, identities_after = source_files(source_root, dataset, trade_date)
        fingerprint_after = source_fingerprint(identities_after)
        if fingerprint_after != fingerprint:
            raise RuntimeError(
                "构建期间源分区发生变化，本版本不会发布；"
                f"before={fingerprint}, after={fingerprint_after}"
            )
        output_rows = sum(item.rows for item in entries)
        if source_rows != output_rows:
            raise RuntimeError(
                f"行数校验失败: source={source_rows}, output={output_rows}"
            )
        partition_manifest = {
            "format": "market-data-5m-partition-v1",
            "status": "published",
            "dataset": dataset,
            "trade_date": trade_date,
            "source_fingerprint": fingerprint,
            "source_files": [asdict(item) for item in identities],
            "source_rows": source_rows,
            "source_bytes": source_bytes,
            "output_rows": output_rows,
            "output_bytes": sum(item.bytes for item in entries),
            "schema_hash": schema_hash,
            "version": version,
            "published_at": dt.datetime.now(dt.timezone.utc).isoformat(
                timespec="seconds"
            ),
            "objects": [item.as_dict() for item in entries],
        }
        atomic_write_json(stage_dir / "manifest.json", partition_manifest)

        incoming.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(stage_objects, incoming)
        shutil.copy2(stage_dir / "manifest.json", incoming / "manifest.json")
        os.replace(incoming, final_dir)

        catalog.replace_partition(dataset, trade_date, entries)
        return BuildResult(
            dataset=dataset,
            trade_date=trade_date,
            source_bytes=source_bytes,
            output_bytes=sum(item.bytes for item in entries),
            rows=output_rows,
            objects=len(entries),
            version=version,
            elapsed_seconds=time.monotonic() - started,
            status="published",
        )
    finally:
        if incoming.exists():
            _safe_remove_own_tree(incoming, final_dir.parent)
        if stage_dir.exists():
            _safe_remove_own_tree(stage_dir, staging_root)


@contextlib.contextmanager
def builder_lock(dest_root: Path):
    lock_path = dest_root / ".builder.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="只读源湖，增量生成独立的 5 分钟 Parquet 派生层"
    )
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--dest-root", type=Path, required=True)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=DATASETS,
        dest="datasets",
        help="可重复；默认构建全部数据集",
    )
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        help="可重复，格式 YYYY-MM-DD；省略时发现所有共同日期",
    )
    parser.add_argument(
        "--latest",
        type=int,
        help="只处理发现日期中的最近 N 日，适合每日增量任务",
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="真正写入派生目录；不指定时只输出计划",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    datasets = tuple(args.datasets or DATASETS)
    source_root = args.source_root
    dest_root = args.dest_root
    staging_root = args.staging_root
    validate_roots(source_root, dest_root, staging_root)
    if args.threads < 1:
        raise ValueError("threads 必须 >= 1")

    discovered = discover_dates(source_root, datasets)
    dates = sorted(set(args.dates or discovered))
    if args.latest is not None:
        if args.latest < 1:
            raise ValueError("latest 必须 >= 1")
        dates = dates[-args.latest :]
    unknown = sorted(set(dates) - set(discovered))
    if unknown:
        raise ValueError(f"这些日期并非所有所选数据集都存在: {unknown}")
    plan = {
        "source_root": str(source_root),
        "dest_root": str(dest_root),
        "staging_root": str(staging_root),
        "datasets": datasets,
        "dates": dates,
        "partitions": len(datasets) * len(dates),
        "threads": args.threads,
        "execute": args.execute,
    }
    print(json.dumps(plan, ensure_ascii=False))
    if not args.execute:
        return 0

    dest_root.mkdir(parents=True, exist_ok=True)
    staging_root.mkdir(parents=True, exist_ok=True)
    with builder_lock(dest_root):
        catalog = CatalogStore.initialize_for_write(dest_root)
        for trade_date in dates:
            for dataset in datasets:
                result = build_partition(
                    source_root=source_root,
                    dest_root=dest_root,
                    staging_root=staging_root,
                    dataset=dataset,
                    trade_date=trade_date,
                    threads=min(args.threads, os.cpu_count() or args.threads),
                    catalog=catalog,
                )
                print(json.dumps(asdict(result), ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        raise SystemExit(130)
