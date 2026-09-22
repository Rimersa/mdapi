"""Manifest-driven derived tables; the gateway remains standard-library-only."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re

from .catalog import ObjectEntry, _request_dates
from .model import SHANGHAI, is_derived
from .points import PointsStore

TABLE_FORMAT = "market-data-derived-table-v1"
NAME_RE = r"[a-z][a-z0-9_]{0,63}"


class DerivedStore(PointsStore):
    def _definition(self, name):
        if not re.fullmatch(NAME_RE, name):
            raise ValueError("非法派生表名称")
        path = self._inside(self.root / name / "table.json")
        if path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError("派生表清单过大")
        raw = path.read_bytes()
        doc = json.loads(raw)
        if (
            doc.get("format") != TABLE_FORMAT
            or doc.get("name") != name
            or doc.get("time_column") != "time"
            or doc.get("symbol_column") != "symbol"
        ):
            raise ValueError("派生表定义不符合版本1契约")
        if (
            not isinstance(doc.get("arrow_schema"), str)
            or len(doc["arrow_schema"]) > 2 * 1024 * 1024
        ):
            raise ValueError("派生表缺少有效Arrow结构")
        if not isinstance(doc.get("objects"), list) or not re.fullmatch(
            r"[0-9a-f]{64}", doc.get("version", "")
        ):
            raise ValueError("派生表对象或版本声明非法")
        return doc, hashlib.sha256(raw).hexdigest()

    def describe(self, name):
        doc, _ = self._definition(name)
        # Detailed per-file provenance stays in the administrator's manifest;
        # clients only need the schema and interpretation of the table.
        info = {k: v for k, v in doc.items() if k not in {"objects", "provenance"}}
        dates = sorted({r["trade_date"] for r in doc["objects"]})
        return dict(
            info,
            dataset="derived." + name,
            dates=len(dates),
            first_date=dates[0] if dates else None,
            last_date=dates[-1] if dates else None,
            rows=sum(r["rows"] for r in doc["objects"]),
        )

    def tables(self):
        return [
            self.describe(p.parent.name)
            for p in sorted(self.root.glob("*/table.json"))
            if re.fullmatch(NAME_RE, p.parent.name)
        ]

    def _load_table(self, name, dates=None):
        doc, definition_hash = self._definition(name)
        entries, seen = [], set()
        for obj in doc["objects"]:
            day = obj["trade_date"]
            start = dt.datetime.combine(dt.date.fromisoformat(day), dt.time(), SHANGHAI)
            if dates is not None and day not in dates:
                continue
            path = self._inside(self.root / obj["relative_path"])
            if not path.is_relative_to(self.root / name) or path.suffix != ".parquet":
                raise ValueError("派生表对象路径越界")
            if path in seen:
                raise ValueError("重复派生表对象")
            seen.add(path)
            sig = self._signature(path)
            if (
                obj.get("bytes") != sig[2]
                or type(obj.get("rows")) is not int
                or obj["rows"] < 0
            ):
                raise ValueError("派生表文件与发布清单不一致")
            identity = hashlib.sha256(
                json.dumps([name, definition_hash, str(path), sig]).encode()
            ).hexdigest()
            entry = ObjectEntry(
                identity,
                "derived." + name,
                day,
                start.isoformat(),
                (start + dt.timedelta(days=1)).isoformat(),
                path.relative_to(self.root).as_posix(),
                sig[2],
                obj["rows"],
                int(obj.get("uncompressed_bytes", sig[2] * 10)),
                identity,
                doc["version"],
            )
            with self._lock:
                self._entries[identity] = (entry, path, sig)
                self._entries.move_to_end(identity)
                while len(self._entries) > self.capacity:
                    self._entries.popitem(last=False)
            entries.append(entry)
        return entries, doc

    def selection(self, request):
        if not is_derived(request.dataset):
            raise ValueError("需要derived.<表名>")
        dates = set(_request_dates(request))
        entries, doc = self._load_table(request.dataset.split(".", 1)[1], dates)
        available = {e.trade_date for e in entries}
        definition = {k: v for k, v in doc.items() if k not in {"objects", "provenance"}}
        return entries, dict(
            dataset=request.dataset,
            table=definition,
            dates_without_files=sorted(dates - available),
            calendar_note="未发布日期可能包括休市日；不推定为零值",
        )

    def selected_refs(self, references):
        result, seen = [], set()
        for ref in references:
            uid, dataset = ref.get("object_id", ""), ref.get("dataset", "")
            if (
                not re.fullmatch(r"[0-9a-f]{64}", uid)
                or uid in seen
                or not is_derived(dataset)
            ):
                raise ValueError("非法或重复派生表对象引用")
            seen.add(uid)
            with self._lock:
                saved = self._entries.get(uid)
            if saved is None:
                self._load_table(dataset.split(".", 1)[1], {ref.get("trade_date")})
                with self._lock:
                    saved = self._entries.get(uid)
            if saved is None:
                raise ValueError("派生表版本已过期，请重新查询")
            entry = saved[0]
            if any(
                ref.get(k) != getattr(entry, k)
                for k in ["dataset", "trade_date", "version"]
            ):
                raise ValueError("派生表固定版本引用不一致")
            self.path_for(entry)
            result.append(entry)
        return result


def declared_schema(coverage, columns=None):
    """Client-only Arrow decoding; additions are nullable in older partitions."""
    import base64
    import pyarrow as pa

    definition = (coverage or {}).get("table", {})
    encoded = definition.get("arrow_schema", "")
    if not isinstance(encoded, str) or len(encoded) > 2 * 1024 * 1024:
        raise ValueError("无效派生表Arrow结构")
    schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(encoded, validate=True))
    )
    if len(schema.names) != len(set(schema.names)) or not {"time", "symbol"} <= set(
        schema.names
    ):
        raise ValueError("派生表结构缺少唯一的time、symbol字段")
    if not pa.types.is_timestamp(schema.field("time").type):
        raise ValueError("派生表time字段必须为timestamp")
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
