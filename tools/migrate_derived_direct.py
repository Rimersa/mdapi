"""Copy a legacy 0.7 manifest table into the 0.8 direct day-file layout.

The old ``table.json`` and ``versions/`` files are left untouched, so the 0.7
gateway can keep serving during and after migration.  The script is
idempotent: an existing direct file is verified, never silently overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True, help="derived root, e.g. /data/flow_points/.derived_tables")
    p.add_argument("--table", default="daily_quality")
    p.add_argument("--apply", action="store_true", help="without this flag only report")
    args = p.parse_args()
    root = Path(args.root).resolve(strict=True)
    table_root = (root / args.table).resolve(strict=True)
    if not table_root.is_relative_to(root):
        raise ValueError("table path escapes root")
    manifest_path = table_root / "table.json"
    manifest = json.loads(manifest_path.read_text())
    objects = manifest.get("objects")
    if not isinstance(objects, list) or not objects:
        raise ValueError("旧清单没有 objects")
    days = {}
    results = []
    for obj in objects:
        day = obj.get("trade_date")
        relative = obj.get("relative_path")
        if not isinstance(day, str) or not isinstance(relative, str):
            raise ValueError("旧清单对象缺少日期或相对路径")
        source = (root / relative).resolve(strict=True)
        if not source.is_relative_to(table_root):
            raise ValueError("旧清单对象路径越界")
        expected_bytes = obj.get("bytes")
        if source.stat().st_size != expected_bytes:
            raise ValueError(f"{day} 源文件大小不符")
        if sha256(source) != obj.get("sha256"):
            raise ValueError(f"{day} 源文件SHA256不符")
        rows = pq.ParquetFile(source).metadata.num_rows
        if rows != obj.get("rows"):
            raise ValueError(f"{day} 源文件行数不符")
        if day in days:
            raise ValueError(f"旧清单出现重复日期 {day}")
        days[day] = True
        target = table_root / ("trade_date=" + quote(day, safe="")) / "data.parquet"
        created = False
        if target.exists():
            if target.stat().st_size != expected_bytes or sha256(target) != obj.get("sha256"):
                raise ValueError(f"{day} 已存在的直接文件与源不一致，停止")
        else:
            if not args.apply:
                results.append(dict(date=day, status="would-create", bytes=expected_bytes, rows=rows))
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(source, target)
                created = True
            except OSError:
                shutil.copyfile(source, target)
                created = True
            if sha256(target) != obj.get("sha256") or pq.ParquetFile(target).metadata.num_rows != rows:
                raise RuntimeError(f"{day} 直接文件写入后校验失败")
            directory_fd = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        results.append(dict(date=day, status="created" if created else "already-present", bytes=expected_bytes, rows=rows))
    if args.apply:
        meta_path = table_root / "meta.json"
        current = {}
        if meta_path.exists():
            try:
                current = json.loads(meta_path.read_text())
            except (OSError, ValueError):
                current = {}
        columns = manifest.get("columns") if isinstance(manifest.get("columns"), list) else []
        fields = {
            item["name"]: {k: item[k] for k in ("description", "unit") if isinstance(item.get(k), str)}
            for item in columns
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        }
        meta = dict(current)
        meta["format"] = "market-data-derived-table-meta-v1"
        meta["description"] = manifest.get("description", meta.get("description", ""))
        meta["granularity"] = manifest.get("granularity", meta.get("granularity", "daily"))
        meta["fields"] = {**(current.get("fields") or {}), **fields}
        if isinstance(manifest.get("arrow_schema"), str) and manifest["arrow_schema"]:
            meta["legacy_arrow_schema"] = manifest["arrow_schema"]
        temporary = meta_path.with_name(".meta.json.tmp")
        temporary.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n")
        os.replace(temporary, meta_path)
    summary = {
        "root": str(root),
        "table": args.table,
        "apply": bool(args.apply),
        "dates": len(results),
        "created": sum(r["status"] == "created" for r in results),
        "already_present": sum(r["status"] == "already-present" for r in results),
        "would_create": sum(r["status"] == "would-create" for r in results),
        "rows": sum(r["rows"] for r in results),
        "days": results,
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
