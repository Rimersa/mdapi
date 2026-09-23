"""Directly-scanned derived tables; the gateway remains standard-library-only.

Layout contract::

    <root>/<table>/trade_date=YYYY-MM-DD/data.parquet

Every file is a flat Parquet file containing ``time`` (Asia/Shanghai
timestamp) and ``symbol`` (string).  Producers write new columns by merging
them into the same day file.  There is no manifest and no publish step: the
gateway rescans directory metadata (footer/stat cached) whenever a request is
planned.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import threading
from collections import OrderedDict
from pathlib import Path

from .catalog import ObjectEntry, _request_dates
from .model import SHANGHAI, is_derived
from .parquet_inspect import inspect_parquet

TABLE_FORMAT = "market-data-derived-table-v2"
NAME_RE = r"[a-z][a-z0-9_]{0,63}"
DAY_RE = re.compile(r"trade_date=(\d{4}-\d{2}-\d{2})\Z")


class DerivedStore:
    """Read-only directory store with automatic schema/date discovery."""

    def __init__(self, root: Path, capacity=8192):
        root = Path(root).expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError("derived-root 必须是实际目录")
        self.root = root
        self.capacity = capacity
        self._entries: OrderedDict[str, tuple[ObjectEntry, Path, tuple]] = OrderedDict()
        self._files: OrderedDict[str, dict] = OrderedDict()
        self._lock = threading.RLock()

    @property
    def generated_at(self):
        return dt.datetime.now(dt.timezone.utc).isoformat()

    @staticmethod
    def _signature(path: Path):
        stat = path.stat()
        return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def _inside(self, path: Path):
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(self.root):
            raise ValueError("派生表路径越界；derived-root 必须是实际存储根目录")
        return resolved

    def _name(self, name: str) -> str:
        if not isinstance(name, str) or not re.fullmatch(NAME_RE, name):
            raise ValueError("非法派生表名称")
        return name

    def _metadata(self, name: str) -> dict:
        base = self.root / name
        path = base / "meta.json"
        if not path.is_file():
            # Legacy 0.7 manifests also carried description/granularity/columns
            # metadata.  Read it only as a fallback; never modify it here.
            path = base / "table.json"
        if not path.is_file():
            return {}
        try:
            doc = json.loads(path.read_text())
        except (OSError, ValueError):
            return {}
        if not isinstance(doc, dict):
            return {}
        fields = doc.get("fields") if isinstance(doc.get("fields"), dict) else {}
        if not fields and isinstance(doc.get("columns"), list):
            fields = {
                item["name"]: {
                    key: item[key]
                    for key in ("description", "unit")
                    if isinstance(item.get(key), str)
                }
                for item in doc["columns"]
                if isinstance(item, dict) and isinstance(item.get("name"), str)
            }
        legacy = doc.get("legacy_arrow_schema")
        return {
            "description": doc.get("description") if isinstance(doc.get("description"), str) else "",
            "granularity": doc.get("granularity") if isinstance(doc.get("granularity"), str) else None,
            "fields": fields,
            "legacy_arrow_schema": legacy if isinstance(legacy, str) else None,
        }

    def _file_info(self, path: Path) -> dict:
        key = str(path)
        signature = self._signature(path)
        with self._lock:
            cached = self._files.get(key)
            if cached is not None and cached.get("signature") == signature:
                self._files.move_to_end(key)
                return cached
        info = inspect_parquet(path)
        info["signature"] = signature
        with self._lock:
            self._files[key] = info
            self._files.move_to_end(key)
            while len(self._files) > self.capacity:
                self._files.popitem(last=False)
        return info

    def _scan(self, name: str):
        name = self._name(name)
        table_root = self.root / name
        if not table_root.is_dir():
            raise ValueError(f"派生表不存在: {name}")
        records = []
        columns = []
        by_name = {}
        for day_dir in sorted(table_root.iterdir()):
            match = DAY_RE.fullmatch(day_dir.name)
            if not match or not day_dir.is_dir():
                continue
            day = match.group(1)
            path = day_dir / "data.parquet"
            if not path.is_file():
                continue
            info = self._file_info(path)
            record = {
                "day": day,
                "path": path,
                "relative_path": path.relative_to(self.root).as_posix(),
                "bytes": info["bytes"],
                "rows": info["rows"],
                "signature": info["signature"],
                "file_columns": info["columns"],
                "arrow_schema": info.get("arrow_schema"),
            }
            records.append(record)
            for column in info["columns"]:
                existing = by_name.get(column["name"])
                if existing is None:
                    existing = dict(column)
                    if existing["name"] in {"time", "symbol"}:
                        existing["nullable"] = False
                    by_name[column["name"]] = existing
                    columns.append(existing)
                elif existing["type"] != column["type"]:
                    raise ValueError(
                        f"派生表 {name} 字段 {column['name']} 类型不一致: "
                        f"{existing['type']} / {column['type']}"
                    )
        if not records:
            raise ValueError(f"派生表 {name} 尚无 trade_date=*/data.parquet 数据")
        metadata = self._metadata(name)
        fields = metadata.get("fields", {})
        for column in columns:
            details = fields.get(column["name"])
            if isinstance(details, dict):
                for key in ("description", "unit"):
                    if isinstance(details.get(key), str):
                        column[key] = details[key]
        records.sort(key=lambda r: r["day"])
        representative = metadata.get("legacy_arrow_schema") or max(
            records,
            key=lambda r: (len(r["file_columns"]), r["day"]),
        ).get("arrow_schema")
        table = {
            "format": TABLE_FORMAT,
            "name": name,
            "time_column": "time",
            "symbol_column": "symbol",
            "columns": columns,
            "granularity": metadata.get("granularity") or "daily",
            "description": metadata.get("description", ""),
            "rows": sum(r["rows"] for r in records),
            "dates": len(records),
        }
        if representative:
            table["arrow_schema"] = representative
        return records, table

    @staticmethod
    def _identity(name: str, record: dict) -> str:
        return hashlib.sha256(
            json.dumps(
                [name, record["relative_path"], list(record["signature"]), record["rows"]],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    def _entry(self, name: str, record: dict) -> ObjectEntry:
        identity = self._identity(name, record)
        start = dt.datetime.combine(dt.date.fromisoformat(record["day"]), dt.time(), SHANGHAI)
        entry = ObjectEntry(
            identity,
            "derived." + name,
            record["day"],
            start.isoformat(),
            (start + dt.timedelta(days=1)).isoformat(),
            record["relative_path"],
            record["bytes"],
            record["rows"],
            max(record["bytes"] * 4, record["rows"] * 64),
            repr(record["signature"]),
            identity,
        )
        with self._lock:
            self._entries[identity] = (entry, record["path"], record["signature"])
            self._entries.move_to_end(identity)
            while len(self._entries) > self.capacity:
                self._entries.popitem(last=False)
        return entry

    def tables(self):
        result = []
        for path in sorted(self.root.iterdir()):
            if not path.is_dir() or not re.fullmatch(NAME_RE, path.name):
                continue
            try:
                self._scan(path.name)
            except (ValueError, FileNotFoundError, OSError):
                continue
            result.append(self.describe(path.name))
        return result

    def describe(self, name):
        records, table = self._scan(name)
        result = {k: v for k, v in table.items() if k != "arrow_schema"}
        result.update(
            dataset="derived." + name,
            dates=len(records),
            first_date=records[0]["day"] if records else None,
            last_date=records[-1]["day"] if records else None,
            rows=sum(r["rows"] for r in records),
        )
        return result

    def _entries_for(self, name: str, records):
        return [self._entry(name, record) for record in records]

    def selection(self, request):
        if not is_derived(request.dataset):
            raise ValueError("需要derived.<表名>")
        name = request.dataset.split(".", 1)[1]
        records, table = self._scan(name)
        wanted = set(_request_dates(request))
        selected = [record for record in records if record["day"] in wanted]
        entries = self._entries_for(name, selected)
        available = {record["day"] for record in selected}
        coverage = {
            "dataset": request.dataset,
            "table": table,
            "dates_without_files": sorted(wanted - available),
            "calendar_note": "未发布日期可能包括休市日；不推定为零值",
        }
        return entries, coverage

    def selected(self, request):
        return self.selection(request)[0]

    def selected_refs(self, references):
        result, seen = [], set()
        for ref in references:
            uid, dataset = ref.get("object_id", ""), ref.get("dataset", "")
            if not re.fullmatch(r"[0-9a-f]{64}", uid) or uid in seen or not is_derived(dataset):
                raise ValueError("非法或重复派生表对象引用")
            seen.add(uid)
            with self._lock:
                saved = self._entries.get(uid)
            if saved is None:
                name = dataset.split(".", 1)[1]
                records, _ = self._scan(name)
                self._entries_for(name, records)
                with self._lock:
                    saved = self._entries.get(uid)
            if saved is None:
                raise ValueError("派生表版本已过期，请重新查询")
            entry = saved[0]
            if any(ref.get(k) != getattr(entry, k) for k in ("dataset", "trade_date", "version")):
                raise ValueError("派生表固定版本引用不一致")
            self.path_for(entry)
            result.append(entry)
        return result

    def path_for(self, entry):
        with self._lock:
            saved = self._entries.get(entry.object_id)
        if saved is None or saved[0] != entry:
            raise ValueError("派生表读取计划已过期，请重新查询")
        _, path, signature = saved
        if self._inside(path) != path or self._signature(path) != signature:
            raise ValueError("已选派生表版本发生变化，停止读取以避免混合版本")
        return path


# ---------------------------------------------------------------------------
# Client-side Arrow helpers.  Importing pyarrow remains a client-only concern.


def _arrow_type(text: str):
    import pyarrow as pa

    aliases = {
        "bool": pa.bool_(),
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "string": pa.string(),
        "large_string": pa.large_string(),
        "binary": pa.binary(),
        "large_binary": pa.large_binary(),
        "date32[day]": pa.date32(),
        "date64[ms]": pa.date64(),
    }
    if text in aliases:
        return aliases[text]
    match = re.fullmatch(r"time32\[(ms|s)\]", text)
    if match:
        return pa.time32(match.group(1))
    match = re.fullmatch(r"time64\[(us|ns)\]", text)
    if match:
        return pa.time64(match.group(1))
    match = re.fullmatch(r"timestamp\[(s|ms|us|ns)(?:, tz=([^\]]+))?\]", text)
    if match:
        return pa.timestamp(match.group(1), tz=match.group(2))
    match = re.fullmatch(r"decimal128\((\d+),\s*(-?\d+)\)", text)
    if match:
        return pa.decimal128(int(match.group(1)), int(match.group(2)))
    raise ValueError(f"派生表字段类型暂不支持: {text}")


def declared_schema(coverage, columns=None):
    """Decode the table's flat column contract from coverage metadata."""
    import pyarrow as pa

    definition = (coverage or {}).get("table", {})
    declared = definition.get("columns")
    if isinstance(declared, list) and declared:
        fields = []
        for column in declared:
            if (
                not isinstance(column, dict)
                or not isinstance(column.get("name"), str)
                or not isinstance(column.get("type"), str)
            ):
                raise ValueError("派生表列定义非法")
            fields.append(
                pa.field(
                    column["name"],
                    _arrow_type(column["type"]),
                    nullable=bool(column.get("nullable", True)),
                )
            )
        schema = pa.schema(fields)
    else:
        encoded = definition.get("arrow_schema", "")
        if not isinstance(encoded, str) or len(encoded) > 2 * 1024**2:
            raise ValueError("无效派生表Arrow结构")
        import base64

        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(encoded, validate=True)))
    if len(schema.names) != len(set(schema.names)) or not {"time", "symbol"} <= set(schema.names):
        raise ValueError("派生表结构缺少唯一的time、symbol字段")
    if not pa.types.is_timestamp(schema.field("time").type):
        raise ValueError("派生表time字段必须为timestamp")
    if not pa.types.is_string(schema.field("symbol").type):
        raise ValueError("派生表symbol字段必须为string")
    if columns is not None:
        missing = set(columns) - set(schema.names)
        if missing:
            raise ValueError("不存在这些列: " + str(sorted(missing)))
        schema = pa.schema([schema.field(c) for c in columns], metadata=schema.metadata)
    return schema


def align_batch(batch, schema):
    import pyarrow as pa

    arrays = []
    for field in schema:
        if field.name in batch.schema.names:
            array = batch.column(batch.schema.get_field_index(field.name))
            if array.type != field.type:
                raise ValueError("派生表字段类型发生不兼容变化: " + field.name)
        elif field.nullable:
            array = pa.nulls(batch.num_rows, type=field.type)
        else:
            raise ValueError("历史分区缺少必需字段: " + field.name)
        arrays.append(array)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
