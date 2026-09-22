"""Snapshot every published stock-day quality measurement; never apply a cutoff."""

import argparse
import datetime as dt
import json
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from market_data_api.model import SHANGHAI
from market_data_api.points import PointsStore
from market_data_api.derived import DerivedStore
from market_data_api.model import DataRequest
from market_data_api.publish import publish_table, sha256


FIELD_INFO = {
    "time": {
        "description": "交易日零点，Asia/Shanghai；日汇总所属日期，不表示该时刻已经可得"
    },
    "scope": {"description": "continuous：连续竞价成交量；不含集合竞价"},
    "tick_volume": {
        "unit": "share",
        "description": "v2质量表的兼容列名，与base_volume相同；实际基座主买量加主卖量",
    },
    "base_volume": {
        "unit": "share",
        "description": "实际points订单口径的主买量加主卖量；主质量比较使用此值",
    },
    "buy_volume": {"unit": "share", "description": "实际points订单口径主买量"},
    "sell_volume": {"unit": "share", "description": "实际points订单口径主卖量"},
    "raw_tick_volume": {
        "unit": "share",
        "description": "识别主动订单之前的有效逐笔量，仅保留作为诊断信息",
    },
    "price_mode_volume": {
        "unit": "share",
        "description": "实际points价格档口径独立汇总，用于与订单口径交叉检查，不能相加",
    },
    "volume_basis": {"description": "points_order_mode；单一质量偏差口径"},
    "reference_volume": {
        "unit": "share",
        "description": "独立ClickHouse连续竞价分钟汇总；参考不完整时为null",
    },
    "volume_diff": {"unit": "share", "description": "base_volume - reference_volume"},
    "volume_error_ratio": {
        "unit": "fraction",
        "description": "(tick_volume-reference_volume)/reference_volume；-1代表-100%",
    },
    "volume_error_pct": {
        "unit": "percent",
        "description": "100*(tick_volume-reference_volume)/reference_volume；-100代表-100%",
    },
    "deviation_status": {
        "description": "available / reference_unavailable / zero_reference_volume；后两种百分比为null"
    },
    "possible_issue": {
        "description": "原质量诊断标志；不代表调用方的可用性阈值，不据此过滤数据"
    },
}


def export(points_root, derived_root):
    store = PointsStore(Path(points_root))
    directories = store._directories()
    provenance = {}
    current = Path(derived_root) / "daily_quality/table.json"
    previous = json.loads(current.read_text()) if current.exists() else None
    expected_version = previous["version"] if previous else "absent"
    existing_store = DerivedStore(Path(derived_root)) if previous else None
    with tempfile.TemporaryDirectory(prefix="mdapi-daily-quality-") as temporary:
        parts = {}
        for day, folder in sorted(directories.items()):
            baseline = folder.resolve(strict=True)
            entry, _ = store._load(day, folder)
            qpath = baseline / "quality.parquet"
            before = sha256(qpath)
            q = pq.ParquetFile(qpath).read()
            quality_receipt = json.loads((baseline / "quality.json").read_text())
            if (
                quality_receipt.get("format") != "flow-base-continuous-quality-v2"
                or quality_receipt.get("quality_parquet_sha256") != before
            ):
                raise ValueError(
                    "Publish only completed v2 base-volume quality snapshots: " + day
                )
            rows = q.to_pylist()
            if len({r["symbol"] for r in rows}) != len(rows):
                raise ValueError("Duplicate stock-day: " + day)
            for r in rows:
                if r["date"] != day or r["scope"] != "continuous":
                    raise ValueError("Unexpected quality scope")
                ref = r["reference_volume"]
                usable = r["reference_status"] == "complete" and ref is not None
                ratio = r[
                    "volume_error_ratio"
                ]  # Existing checker value, never rechecked here.
                r.update(
                    time=dt.datetime.combine(
                        dt.date.fromisoformat(day), dt.time(), SHANGHAI
                    ),
                    volume_error_pct=100 * ratio if ratio is not None else None,
                    deviation_status=(
                        "available"
                        if usable and ref > 0
                        else (
                            "zero_reference_volume"
                            if usable
                            else "reference_unavailable"
                        )
                    ),
                    base_version=entry.version,
                )
            schema = pa.schema(
                [("time", pa.timestamp("ns", tz="Asia/Shanghai"))]
                + list(q.schema)
                + [
                    ("volume_error_pct", pa.float64()),
                    ("deviation_status", pa.string()),
                    ("base_version", pa.string()),
                ]
            )
            # Preserve already-published factor columns when refreshing quality.
            if previous:
                request = DataRequest.from_query(
                    dict(dataset="derived.daily_quality", start_date=day, end_date=day)
                )
                entries = existing_store.selected(request)
                extra = [
                    c["name"]
                    for c in previous["columns"]
                    if c["name"] not in schema.names
                ]
                if extra and entries:
                    old_tables = [
                        pq.ParquetFile(existing_store.path_for(e)).read()
                        for e in entries
                    ]
                    old_rows = {}
                    fields = {}
                    for table in old_tables:
                        for r in table.to_pylist():
                            if r["symbol"] in old_rows:
                                raise ValueError("Duplicate prior stock-day")
                            old_rows[r["symbol"]] = r
                        for f in table.schema:
                            if f.name in extra:
                                fields[f.name] = f
                    if set(old_rows) - {r["symbol"] for r in rows}:
                        raise ValueError(
                            "Quality refresh would remove stocks carrying factors; reconcile universe first"
                        )
                    for r in rows:
                        r.update(
                            {
                                name: old_rows.get(r["symbol"], {}).get(name)
                                for name in fields
                            }
                        )
                    schema = pa.schema(list(schema) + list(fields.values()))
            target = Path(temporary) / ("trade_date=" + day) / "quality.parquet"
            target.parent.mkdir()
            table = pa.Table.from_pylist(rows, schema=schema).sort_by(
                [("symbol", "ascending")]
            )
            pq.write_table(table, target, compression="zstd", row_group_size=1024)
            if sha256(qpath) != before or folder.resolve() != baseline:
                raise RuntimeError("Quality changed during export: " + day)
            store.path_for(entry)
            provenance[day] = dict(
                base_version=entry.version,
                quality_sha256=before,
                baseline=str(baseline),
            )
            parts[day] = [target]
        # Do not publish a mixed snapshot if a repair completed during export.
        for day, folder in directories.items():
            p = provenance[day]
            if (
                str(folder.resolve()) != p["baseline"]
                or sha256(folder / "quality.parquet") != p["quality_sha256"]
            ):
                raise RuntimeError("Production changed before publication: " + day)
        return publish_table(
            derived_root,
            "daily_quality",
            parts,
            granularity="daily",
            description="每日每股连续竞价数据质量明细。包含逐笔、参考或已有质量表中出现的股票；未在两侧出现的停牌证券不伪造零值。偏差不设可用性阈值。",
            field_info=FIELD_INFO,
            provenance={"kind": "published_quality_snapshot", "dates": provenance},
            expected_version=expected_version,
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--points-root", required=True)
    p.add_argument("--derived-root", required=True)
    a = p.parse_args()
    print(json.dumps(export(a.points_root, a.derived_root), ensure_ascii=False))


if __name__ == "__main__":
    main()
