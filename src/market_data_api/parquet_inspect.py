"""Minimal standard-library Parquet footer inspection for derived tables.

Derived tables are plain Parquet files written by producers.  The read-only
gateway must learn row counts and a flat column schema without importing
pyarrow.  This module implements just enough of the Parquet thrift metadata
format to do that, and extracts PyArrow's embedded ``ARROW:schema`` when
present for compatibility with older clients.
"""
from __future__ import annotations

import base64
import binascii
import re
import struct
from pathlib import Path

MAX_FOOTER_BYTES = 16 * 1024**2
_B64_RE = re.compile(r"\A[A-Za-z0-9+/]*={0,2}\Z")


class ParquetInspectionError(ValueError):
    """A derived Parquet file does not satisfy the table contract."""


class _Reader:
    __slots__ = ("data", "pos")

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def byte(self) -> int:
        if self.pos >= len(self.data):
            raise ParquetInspectionError("Parquet footer 提前结束")
        value = self.data[self.pos]
        self.pos += 1
        return value

    def take(self, count: int) -> bytes:
        if count < 0 or self.pos + count > len(self.data):
            raise ParquetInspectionError("Parquet footer 字段越界")
        value = self.data[self.pos : self.pos + count]
        self.pos += count
        return value

    def varint(self) -> int:
        value = shift = 0
        while True:
            byte = self.byte()
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
            shift += 7
            if shift > 70:
                raise ParquetInspectionError("Parquet footer varint 过长")

    def zigzag(self) -> int:
        value = self.varint()
        return (value >> 1) ^ -(value & 1)

    def binary(self) -> bytes:
        return self.take(self.varint())

    def read_struct(self, visitor=None) -> None:
        last = 0
        while True:
            header = self.byte()
            if header == 0:
                return
            field_type = header & 0x0F
            delta = header >> 4
            if delta:
                field_id = last + delta
            else:
                field_id = self.zigzag()
            last = field_id
            if visitor is not None and visitor(self, field_id, field_type):
                continue
            self.skip(field_type)

    def skip(self, field_type: int) -> None:
        if field_type in (1, 2):
            return
        if field_type == 3:
            self.byte()
            return
        if field_type in (4, 5, 6):
            self.varint()
            return
        if field_type == 7:
            self.take(8)
            return
        if field_type == 8:
            self.take(self.varint())
            return
        if field_type in (9, 10):
            header = self.byte()
            size = header >> 4
            element_type = header & 0x0F
            if size == 15:
                size = self.varint()
            for _ in range(size):
                self.skip(element_type)
            return
        if field_type == 11:
            size = self.varint()
            if size:
                types = self.byte()
                key_type, value_type = types >> 4, types & 0x0F
                for _ in range(size):
                    self.skip(key_type)
                    self.skip(value_type)
            return
        if field_type == 12:
            self.read_struct()
            return
        raise ParquetInspectionError(f"未知 thrift compact 类型 {field_type}")

    def list_header(self):
        header = self.byte()
        size = header >> 4
        element_type = header & 0x0F
        if size == 15:
            size = self.varint()
        return size, element_type


def read_footer_bytes(path) -> bytes:
    path = Path(path)
    stat = path.stat()
    if stat.st_size < 12:
        raise ParquetInspectionError("Parquet 文件过短")
    with path.open("rb") as handle:
        handle.seek(-8, 2)
        tail = handle.read(8)
        if len(tail) != 8 or tail[4:] != b"PAR1":
            raise ParquetInspectionError("Parquet 文件尾魔数非法")
        length = struct.unpack("<I", tail[:4])[0]
        if length > MAX_FOOTER_BYTES or length + 12 > stat.st_size:
            raise ParquetInspectionError("Parquet footer 长度非法或超过 16MiB")
        handle.seek(-8 - length, 2)
        payload = handle.read(length + 8)
    if len(payload) != length + 8:
        raise ParquetInspectionError("Parquet footer 读取不完整")
    return payload


def _parse_schema_element(reader: _Reader) -> dict:
    element = {
        "type": None,
        "repetition_type": None,
        "name": None,
        "num_children": 0,
        "converted_type": None,
        "logical": None,
    }

    def visit(inner: _Reader, field_id: int, field_type: int) -> bool:
        if field_id == 1 and field_type in (4, 5, 6):
            element["type"] = inner.zigzag()
            return True
        if field_id == 3 and field_type in (4, 5, 6):
            element["repetition_type"] = inner.zigzag()
            return True
        if field_id == 4 and field_type == 8:
            try:
                element["name"] = inner.binary().decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ParquetInspectionError("Parquet 列名不是UTF-8") from exc
            return True
        if field_id == 5 and field_type in (4, 5, 6):
            element["num_children"] = inner.zigzag()
            return True
        if field_id == 6 and field_type in (4, 5, 6):
            element["converted_type"] = inner.zigzag()
            return True
        if field_id == 10 and field_type == 12:
            element["logical"] = _parse_logical_type(inner)
            return True
        return False

    reader.read_struct(visit)
    if not isinstance(element["name"], str) or not element["name"]:
        raise ParquetInspectionError("Parquet schema 缺少列名")
    return element


def _parse_time_unit(reader: _Reader):
    result = {"unit": None}

    def visit(inner: _Reader, field_id: int, field_type: int) -> bool:
        if field_id in (1, 2, 3) and field_type == 12:
            result["unit"] = {1: "ms", 2: "us", 3: "ns"}[field_id]
            inner.read_struct()
            return True
        return False

    reader.read_struct(visit)
    return result["unit"]


def _parse_logical_type(reader: _Reader):
    result = {"kind": None, "adjusted": None, "unit": None, "scale": None, "precision": None}

    def visit(inner: _Reader, field_id: int, field_type: int) -> bool:
        simple = {1: "STRING", 2: "MAP", 3: "LIST", 4: "ENUM", 6: "DATE"}
        if field_id in simple:
            result["kind"] = simple[field_id]
            if field_type == 12:
                inner.read_struct()
            return True
        if field_id == 5:
            result["kind"] = "DECIMAL"

            def decimal(inner2: _Reader, field_id2: int, field_type2: int) -> bool:
                if field_id2 == 1 and field_type2 in (4, 5, 6):
                    result["scale"] = inner2.zigzag()
                    return True
                if field_id2 == 2 and field_type2 in (4, 5, 6):
                    result["precision"] = inner2.zigzag()
                    return True
                return False

            if field_type == 12:
                inner.read_struct(decimal)
            return True
        if field_id in (7, 8):
            result["kind"] = "TIME" if field_id == 7 else "TIMESTAMP"

            def temporal(inner2: _Reader, field_id2: int, field_type2: int) -> bool:
                if field_id2 == 1:
                    if field_type2 == 1:
                        result["adjusted"] = True
                    elif field_type2 == 2:
                        result["adjusted"] = False
                    else:
                        inner2.skip(field_type2)
                    return True
                if field_id2 == 2 and field_type2 == 12:
                    result["unit"] = _parse_time_unit(inner2)
                    return True
                return False

            if field_type == 12:
                inner.read_struct(temporal)
            return True
        if field_id == 10:
            result["kind"] = "INTEGER"
            if field_type == 12:
                inner.read_struct()
            return True
        return False

    reader.read_struct(visit)
    return result


def _physical_arrow_type(element: dict) -> str:
    physical = element["type"]
    converted = element["converted_type"]
    logical = element["logical"] or {}
    kind = logical.get("kind")
    if physical == 0:
        return "bool"
    if physical == 1:
        if kind == "DATE" or converted == 6:
            return "date32[day]"
        if kind == "TIME":
            unit = logical.get("unit") or ("ms" if converted == 7 else "us")
            return f"time32[{unit}]" if unit == "ms" else f"time64[{unit}]"
        if kind == "DECIMAL" or converted == 5:
            precision, scale = logical.get("precision"), logical.get("scale")
            if isinstance(precision, int) and isinstance(scale, int):
                return f"decimal128({precision}, {scale})"
        unsigned = {11: "uint8", 12: "uint16", 13: "uint32", 14: "uint64"}
        signed = {15: "int8", 16: "int16", 17: "int32", 18: "int64"}
        return unsigned.get(converted) or signed.get(converted) or "int32"
    if physical == 2:
        if kind == "TIMESTAMP" or converted in (9, 10):
            unit = logical.get("unit") or ("ms" if converted == 9 else "us")
            suffix = ", tz=Asia/Shanghai" if (logical.get("adjusted") is True or converted in (9, 10)) else ""
            return f"timestamp[{unit}{suffix}]"
        if kind == "TIME":
            return f"time64[{logical.get('unit') or 'us'}]"
        if kind == "DECIMAL" or converted == 5:
            precision, scale = logical.get("precision"), logical.get("scale")
            if isinstance(precision, int) and isinstance(scale, int):
                return f"decimal128({precision}, {scale})"
        return "uint64" if converted == 14 else "int64"
    if physical == 3:
        return "timestamp[ns, tz=Asia/Shanghai]"
    if physical == 4:
        return "float32"
    if physical == 5:
        return "float64"
    if physical == 6:
        if kind in ("STRING", "ENUM", "JSON") or converted in (0, 4, 19):
            return "string"
        if kind == "DECIMAL" or converted == 5:
            precision, scale = logical.get("precision"), logical.get("scale")
            if isinstance(precision, int) and isinstance(scale, int):
                return f"decimal128({precision}, {scale})"
        return "binary"
    if physical == 7:
        if kind == "DECIMAL" or converted == 5:
            precision, scale = logical.get("precision"), logical.get("scale")
            if isinstance(precision, int) and isinstance(scale, int):
                return f"decimal128({precision}, {scale})"
        return "binary"
    raise ParquetInspectionError(f"不支持的 Parquet 物理类型 {physical}")


def _flat_columns(elements: list[dict]) -> list[dict]:
    if not elements:
        raise ParquetInspectionError("Parquet schema 为空")
    root = elements[0]
    children = int(root.get("num_children") or 0)
    if children <= 0 or len(elements) != 1 + children:
        raise ParquetInspectionError("派生表暂只支持扁平Parquet结构")
    columns, names = [], set()
    for element in elements[1:]:
        if int(element.get("num_children") or 0):
            raise ParquetInspectionError("派生表暂只支持扁平Parquet结构")
        name = element["name"]
        if name in names:
            raise ParquetInspectionError(f"Parquet 存在重复列名: {name}")
        names.add(name)
        columns.append(
            {
                "name": name,
                "type": _physical_arrow_type(element),
                "nullable": element.get("repetition_type") != 0,
            }
        )
    return columns


def extract_arrow_schema(footer: bytes):
    index = footer.find(b"ARROW:schema")
    if index < 0:
        return None
    reader = _Reader(footer)
    reader.pos = index + len(b"ARROW:schema")
    if reader.pos < len(footer) and footer[reader.pos] == 0x18:
        reader.pos += 1
    try:
        value = reader.binary().decode("ascii")
    except (ParquetInspectionError, UnicodeDecodeError):
        return None
    if not value or len(value) > 2 * 1024**2:
        return None
    padded = value + "=" * (-len(value) % 4)
    if not _B64_RE.fullmatch(padded):
        return None
    try:
        base64.b64decode(padded, validate=True)
    except (ValueError, binascii.Error):
        return None
    return value


def inspect_parquet(path) -> dict:
    path = Path(path)
    footer = read_footer_bytes(path)
    metadata = footer[:-8]
    num_rows = None
    elements = []

    def visit(reader: _Reader, field_id: int, field_type: int) -> bool:
        nonlocal num_rows, elements
        if field_id == 2 and field_type in (9, 10):
            size, element_type = reader.list_header()
            if element_type != 12:
                raise ParquetInspectionError("Parquet schema 列表类型非法")
            elements = [_parse_schema_element(reader) for _ in range(size)]
            return True
        if field_id == 3 and field_type in (4, 5, 6):
            num_rows = reader.zigzag()
            return True
        return False

    _Reader(metadata).read_struct(visit)
    if num_rows is None or num_rows < 0:
        raise ParquetInspectionError("Parquet FileMetaData 缺少合法行数")
    columns = _flat_columns(elements)
    names = {c["name"] for c in columns}
    if not {"time", "symbol"} <= names:
        raise ParquetInspectionError("派生表文件必须包含 time 和 symbol 列")
    time_column = next(c for c in columns if c["name"] == "time")
    symbol_column = next(c for c in columns if c["name"] == "symbol")
    if not time_column["type"].startswith("timestamp["):
        raise ParquetInspectionError("派生表 time 列必须是 timestamp 类型")
    if symbol_column["type"] != "string":
        raise ParquetInspectionError("派生表 symbol 列必须是 string 类型")
    return {
        "bytes": path.stat().st_size,
        "rows": int(num_rows),
        "columns": columns,
        "arrow_schema": extract_arrow_schema(footer),
    }
