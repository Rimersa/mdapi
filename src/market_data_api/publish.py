"""Offline publisher for generic immutable daily partitions; never run in reads."""

from __future__ import annotations

import base64
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import uuid

from .derived import NAME_RE, TABLE_FORMAT
from .model import SHANGHAI


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def publish_table(
    root,
    name,
    partitions,
    *,
    description="",
    granularity="daily",
    field_info=None,
    provenance=None,
    expected_version=None,
):
    """Replace supplied dates, retain other partitions; only nullable additions.

    `partitions` maps YYYY-MM-DD to local Parquet paths. Every file must contain
    non-null symbol and Shanghai time. Type changes/removals require a new table.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    if not re.fullmatch(NAME_RE, name):
        raise ValueError("Invalid table name")
    root = Path(root).resolve()
    table_root = root / name
    table_root.mkdir(parents=True, exist_ok=True)
    if table_root.resolve().parent != root:
        raise ValueError("Table path escapes root")
    with (table_root / ".publish.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = table_root / "table.json"
        old = json.loads(current.read_text()) if current.exists() else None
        if (
            expected_version is not None
            and (old or {}).get("version", "absent") != expected_version
        ):
            raise RuntimeError(
                "Table changed while preparing publication; retry from the current version"
            )
        schema = (
            pa.ipc.read_schema(pa.BufferReader(base64.b64decode(old["arrow_schema"])))
            if old
            else None
        )
        if old and old["granularity"] != granularity:
            raise ValueError("Granularity cannot change in place")
        edition = uuid.uuid4().hex
        stage = table_root / (".staging-" + edition)
        final = table_root / "versions" / edition
        stage.mkdir()
        objects = []
        for day, paths in sorted(partitions.items()):
            d = dt.date.fromisoformat(day)
            if not paths:
                raise ValueError("Empty partition list")
            for i, source in enumerate(paths):
                source = Path(source)
                original_stat = source.stat()
                original_signature = (
                    original_stat.st_ino,
                    original_stat.st_size,
                    original_stat.st_mtime_ns,
                )
                pf = pq.ParquetFile(source)
                current_schema = pf.schema_arrow.remove_metadata()
                if not {"symbol", "time"} <= set(current_schema.names):
                    raise ValueError("symbol,time required")
                if len(set(current_schema.names)) != len(current_schema):
                    raise ValueError("Duplicate fields")
                typ = current_schema.field("time").type
                if not pa.types.is_timestamp(typ) or typ.tz != "Asia/Shanghai":
                    raise ValueError("time must be a Shanghai timestamp")
                if not pa.types.is_string(current_schema.field("symbol").type):
                    raise ValueError("symbol must be string")
                for batch in pf.iter_batches(
                    columns=["time", "symbol"], batch_size=131072
                ):
                    t, s = batch.column(0), batch.column(1)
                    if t.null_count or s.null_count:
                        raise ValueError("Null row identity")
                    if len(t):
                        lo, hi = pc.min(t).as_py(), pc.max(t).as_py()
                        if lo.date() != d or hi.date() != d:
                            raise ValueError("Time outside declared partition")
                if schema is None:
                    schema = pa.schema(
                        [
                            pa.field(
                                f.name,
                                f.type,
                                nullable=f.name not in ["time", "symbol"],
                            )
                            for f in current_schema
                        ]
                    )
                else:
                    for field in current_schema:
                        if field.name in schema.names:
                            if schema.field(field.name).type != field.type:
                                raise ValueError(
                                    "Incompatible field type: " + field.name
                                )
                        else:
                            schema = schema.append(
                                pa.field(field.name, field.type, nullable=True)
                            )
                target = stage / ("trade_date=" + day) / f"part-{i:04}.parquet"
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                # Validate the copied snapshot, not a potentially changing source.
                if sha256(target) != sha256(source):
                    raise ValueError("Source changed during copy")
                after = source.stat()
                if original_signature != (
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                ):
                    raise ValueError("Source changed during validation")
                copied = pq.ParquetFile(target)
                if (
                    not copied.schema_arrow.equals(pf.schema_arrow)
                    or copied.metadata.num_rows != pf.metadata.num_rows
                ):
                    raise ValueError("Source schema changed during copy")
                with target.open("rb") as f:
                    os.fsync(f.fileno())
                objects.append(
                    dict(
                        trade_date=day,
                        relative_path=(
                            final.relative_to(root) / target.relative_to(stage)
                        ).as_posix(),
                        bytes=target.stat().st_size,
                        rows=copied.metadata.num_rows,
                        uncompressed_bytes=sum(
                            copied.metadata.row_group(j).total_byte_size
                            for j in range(copied.num_row_groups)
                        ),
                        sha256=sha256(target),
                    )
                )
        if schema is None:
            raise ValueError("No data or existing schema")
        if old:
            objects += [r for r in old["objects"] if r["trade_date"] not in partitions]
        objects.sort(key=lambda r: (r["trade_date"], r["relative_path"]))
        details = {
            c["name"]: {
                k: v for k, v in c.items() if k not in ["name", "type", "nullable"]
            }
            for c in (old or {}).get("columns", [])
        }
        details.update(field_info or {})
        doc = dict(
            format=TABLE_FORMAT,
            name=name,
            description=description or (old or {}).get("description", ""),
            granularity=granularity,
            time_column="time",
            symbol_column="symbol",
            arrow_schema=base64.b64encode(schema.serialize().to_pybytes()).decode(),
            columns=[
                dict(
                    name=f.name,
                    type=str(f.type),
                    nullable=f.nullable,
                    **details.get(f.name, {}),
                )
                for f in schema
            ],
            objects=objects,
            published_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            provenance=provenance or {},
        )
        doc["version"] = hashlib.sha256(
            json.dumps(doc, sort_keys=True).encode()
        ).hexdigest()
        final.parent.mkdir(exist_ok=True)
        os.replace(stage, final)
        temporary = table_root / (".table-" + edition + ".json")
        with temporary.open("w") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2, allow_nan=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, current)
        fd = os.open(table_root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return dict(
            name=name,
            version=doc["version"],
            partitions=len({r["trade_date"] for r in objects}),
            rows=sum(r["rows"] for r in objects),
            columns=schema.names,
        )
