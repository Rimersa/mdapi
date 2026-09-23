"""Producer-side helper for the direct derived-table directory contract.

The gateway never imports this module.  Factor jobs and the flow-base quality
checker use :func:`upsert_daily` to merge their columns into the canonical
``trade_date=YYYY-MM-DD/data.parquet`` file for a table.  Existing columns are
preserved; supplied columns update matching keys and new keys are appended.
There is no manifest update and no publish step.
"""
from __future__ import annotations

import datetime as dt
import fcntl
import json
import os
import re
import uuid
from pathlib import Path

from .model import SHANGHAI

TABLE_NAME_RE = re.compile(r"\A[a-z][a-z0-9_]{0,63}\Z")
DAY_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")
DEFAULT_KEYS = ("time", "symbol")


def _validate_table(table):
    if not isinstance(table, str) or not TABLE_NAME_RE.fullmatch(table):
        raise ValueError("非法派生表名称")
    return table


def _as_table(data):
    import pyarrow as pa
    import pyarrow.parquet as pq

    if isinstance(data, pa.Table):
        return data
    if isinstance(data, (str, os.PathLike)):
        return pq.ParquetFile(Path(data)).read()
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            return pa.Table.from_pandas(data, preserve_index=False)
    except ImportError:
        pass
    if isinstance(data, (list, tuple)) and (not data or isinstance(data[0], dict)):
        return pa.Table.from_pylist(list(data))
    raise TypeError("data 需要是 Arrow Table、Parquet 路径、pandas DataFrame 或字典列表")


def _flat_table(table):
    import pyarrow as pa

    if len(set(table.column_names)) != len(table.column_names):
        raise ValueError("结果包含重复列名")
    arrays, names = [], []
    for field in table.schema:
        column = table.column(field.name)
        typ = field.type
        if pa.types.is_dictionary(typ):
            typ = typ.value_type
            column = column.cast(typ)
        if not (
            pa.types.is_boolean(typ)
            or pa.types.is_integer(typ)
            or pa.types.is_floating(typ)
            or pa.types.is_string(typ)
            or pa.types.is_large_string(typ)
            or pa.types.is_binary(typ)
            or pa.types.is_date(typ)
            or pa.types.is_time(typ)
            or pa.types.is_timestamp(typ)
            or pa.types.is_decimal(typ)
        ):
            raise ValueError(f"派生表暂只支持一维标量列: {field.name} ({typ})")
        arrays.append(column)
        names.append(field.name)
    return pa.Table.from_arrays(arrays, names=names)


def _coerce_time(table, day):
    import pyarrow as pa
    import pyarrow.compute as pc

    target_type = pa.timestamp("us", tz="Asia/Shanghai")
    if "time" not in table.column_names:
        midnight = dt.datetime.combine(dt.date.fromisoformat(day), dt.time(), SHANGHAI)
        table = table.append_column("time", pa.array([midnight] * table.num_rows, type=target_type))
    else:
        index = table.schema.get_field_index("time")
        field = table.schema.field(index)
        column = table.column(index)
        if pa.types.is_timestamp(field.type):
            if field.type == target_type:
                pass
            elif field.type.tz is None:
                column = pc.assume_timezone(column, "Asia/Shanghai").cast(target_type)
            else:
                column = column.cast(target_type)
        elif pa.types.is_date(field.type):
            column = pc.cast(column, pa.timestamp("s"), safe=True)
            column = pc.assume_timezone(column, "Asia/Shanghai").cast(target_type)
        else:
            raise ValueError("time 列必须是 timestamp 或 date 类型")
        table = table.set_column(index, "time", column)
    if table.num_rows:
        lo = pc.min(table["time"]).as_py()
        hi = pc.max(table["time"]).as_py()
        wanted = dt.date.fromisoformat(day)
        if lo is None or hi is None or lo.date() != wanted or hi.date() != wanted:
            raise ValueError(f"time 超出分区日期 {day}: {lo} ~ {hi}")
    return table


def _normalize(table, day, keys):
    import pyarrow as pa
    import pyarrow.compute as pc

    table = _flat_table(table)
    if "symbol" not in table.column_names:
        raise ValueError("派生表结果必须包含 symbol 列")
    symbol_index = table.schema.get_field_index("symbol")
    if not pa.types.is_string(table.schema.field(symbol_index).type):
        table = table.set_column(
            symbol_index, "symbol", pc.cast(table["symbol"], pa.string(), safe=True)
        )
    table = _coerce_time(table, day)
    for key in keys:
        if key not in table.column_names:
            raise ValueError(f"缺少键列: {key}")
    order = [name for name in ("time", "symbol") if name in table.column_names]
    order += [name for name in table.column_names if name not in order]
    table = table.select(order)
    if table.num_rows and table["time"].null_count + table["symbol"].null_count:
        raise ValueError("time/symbol 不能为空")
    if table.num_rows:
        counts = table.select(list(keys)).group_by(list(keys)).aggregate([(keys[0], "count")])
        if pc.max(counts.column(counts.num_columns - 1)).as_py() != 1:
            raise ValueError("同一 time/symbol 在写入结果中出现重复")
    return _key_schema(table, keys)


def _key_schema(table, keys):
    import pyarrow as pa

    key_set = set(keys)
    fields = [
        pa.field(field.name, field.type, nullable=field.name not in key_set)
        for field in table.schema
    ]
    return table.cast(pa.schema(fields, metadata=table.schema.metadata))


def _merge_tables(old, new, keys):

    import pyarrow as pa
    import pyarrow.compute as pc

    new_columns = {}
    for name in new.column_names:
        if name in keys:
            continue
        column = new.column(name)
        if name in old.column_names:
            left_type = old.schema.field(name).type
            if column.type != left_type:
                same_kind = (
                    (pa.types.is_integer(column.type) and pa.types.is_integer(left_type))
                    or (pa.types.is_floating(column.type) and pa.types.is_floating(left_type))
                )
                if not same_kind:
                    raise ValueError(
                        f"列 {name} 类型与已有表不一致: {column.type} -> {left_type}"
                    )
                try:
                    column = column.cast(left_type, safe=True)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError) as exc:
                    raise ValueError(
                        f"列 {name} 类型与已有表不一致: {column.type} -> {left_type}"
                    ) from exc
        new_columns[name] = column
    renamed = new.select(list(keys))
    for name, column in new_columns.items():
        renamed = renamed.append_column(name + "__incoming", column)
    joined = old.join(renamed, keys=list(keys), join_type="full outer", coalesce_keys=True)
    arrays, names = [], []
    for key in keys:
        arrays.append(joined[key])
        names.append(key)
    for name in old.column_names:
        if name in keys:
            continue
        if name in new_columns:
            arrays.append(pc.coalesce(joined[name + "__incoming"], joined[name]))
        else:
            arrays.append(joined[name])
        names.append(name)
    for name in new_columns:
        if name not in old.column_names:
            arrays.append(joined[name + "__incoming"])
            names.append(name)
    return pa.Table.from_arrays(arrays, names=names).sort_by(
        [(key, "ascending") for key in keys]
    )


def _update_metadata(table_root: Path, *, description=None, granularity=None, fields=None):
    # Keep new metadata separate from a possible legacy 0.7 table.json manifest.
    path = table_root / "meta.json"
    current = {}
    if path.is_file():
        try:
            current = json.loads(path.read_text())
        except ValueError:
            current = {}
    changed = False
    if description is not None and current.get("description") != description:
        current["description"] = description
        changed = True
    if granularity is not None and current.get("granularity") != granularity:
        current["granularity"] = granularity
        changed = True
    if fields is not None:
        merged = dict(current.get("fields") or {})
        for name, details in fields.items():
            merged[name] = details
        if merged != current.get("fields"):
            current["fields"] = merged
            changed = True
    if not changed:
        return
    current.setdefault("format", "market-data-derived-table-meta-v1")
    temporary = path.with_name(f".table-{uuid.uuid4().hex}.json")
    with temporary.open("w") as handle:
        json.dump(current, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def upsert_daily(
    root,
    table,
    day,
    data,
    *,
    keys=DEFAULT_KEYS,
    description=None,
    granularity=None,
    fields=None,
):
    """Merge ``data`` into one day partition and return a small receipt."""
    import pyarrow.parquet as pq

    table = _validate_table(table)
    if not isinstance(day, str) or not DAY_RE.fullmatch(day):
        raise ValueError("日期必须是 YYYY-MM-DD")
    dt.date.fromisoformat(day)
    root = Path(root).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    table_root = root / table
    day_root = table_root / ("trade_date=" + day)
    day_root.mkdir(parents=True, exist_ok=True)
    target = day_root / "data.parquet"
    incoming = _normalize(_as_table(data), day, tuple(keys))
    lock_path = day_root / ".lock"
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if target.is_file():
            old = _normalize(pq.ParquetFile(target).read(), day, tuple(keys))
            merged = _merge_tables(old, incoming, tuple(keys))
        else:
            merged = incoming
        merged = _key_schema(merged, tuple(keys))
        temporary = day_root / f".data-{uuid.uuid4().hex}.parquet"
        pq.write_table(
            merged,
            temporary,
            compression="zstd",
            row_group_size=65536,
            store_schema=True,
        )
        os.replace(temporary, target)
        directory_fd = os.open(day_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    _update_metadata(
        table_root,
        description=description,
        granularity=granularity,
        fields=fields,
    )
    return {
        "table": table,
        "date": day,
        "path": str(target),
        "rows": merged.num_rows,
        "columns": merged.column_names,
    }


def read_day(root, table, day):
    import pyarrow.parquet as pq

    table = _validate_table(table)
    if not isinstance(day, str) or not DAY_RE.fullmatch(day):
        raise ValueError("日期必须是 YYYY-MM-DD")
    path = Path(root) / table / ("trade_date=" + day) / "data.parquet"
    return pq.ParquetFile(path).read() if path.is_file() else None


def list_tables(root):
    result = []
    root = Path(root).expanduser()
    if not root.is_dir():
        return result
    for path in sorted(root.iterdir()):
        if path.is_dir() and TABLE_NAME_RE.fullmatch(path.name):
            days = [p for p in path.glob("trade_date=*/data.parquet") if p.is_file()]
            if days:
                result.append({"name": path.name, "dates": len(days)})
    return result
