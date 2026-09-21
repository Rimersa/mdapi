"""Read-only daily points discovery. No Arrow dependency in the gateway."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import threading
from collections import OrderedDict
from pathlib import Path

from .catalog import ObjectEntry, _request_dates
from .model import SHANGHAI


class PointsStore:
    def __init__(self, root: Path, capacity=4096):
        self.root = root.expanduser().resolve(strict=True)
        if not self.root.is_dir():
            raise ValueError("points-root 必须是实际基座存储目录")
        self.capacity = capacity
        self._entries = OrderedDict()
        self._lock = threading.RLock()

    @property
    def generated_at(self):
        return dt.datetime.now(dt.timezone.utc).isoformat()

    def _inside(self, path):
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(self.root):
            raise ValueError(
                "基座链接越界；points-root 应指定实际存储根目录，例如 /data/flow_points"
            )
        return resolved

    def _directories(self):
        dates = {}
        parents = [self.root, *sorted(self.root.glob("machine=*"))]
        for parent in parents:
            self._inside(parent)
            for folder in sorted(parent.glob("trade_date=*")):
                if not re.fullmatch(r"trade_date=\d{4}-\d{2}-\d{2}", folder.name):
                    continue
                day = folder.name.split("=", 1)[1]
                dt.date.fromisoformat(day)
                if day in dates:
                    raise ValueError(f"基座出现重复日期 {day}，请先消除重复发布")
                dates[day] = folder
        return dates

    @staticmethod
    def _signature(path):
        s = path.stat()
        return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)

    def _load(self, day, folder):
        directory = self._inside(folder)
        path = self._inside(directory / "points.parquet")
        receipt_path = self._inside(directory / "day.json")
        before = self._signature(path)
        raw = receipt_path.read_bytes()
        receipt = json.loads(raw)
        if receipt.get("day") != day:
            raise ValueError(f"基座日期与 day.json 不一致: {day}")
        if receipt.get("status") not in {"complete", "partial"}:
            raise ValueError(f"基座日期尚未完成发布: {day}")
        output = receipt.get("output", {})
        if (
            output.get("bytes") != before[2]
            or type(output.get("rows")) is not int
            or output["rows"] < 0
        ):
            raise ValueError(f"基座文件与 day.json 的大小/行数声明不一致: {day}")
        if self._signature(path) != before:
            raise RuntimeError(f"基座文件正在变化: {day}")
        identity = hashlib.sha256(
            json.dumps([str(path), before, hashlib.sha256(raw).hexdigest()]).encode()
        ).hexdigest()
        start = dt.datetime.combine(dt.date.fromisoformat(day), dt.time(), SHANGHAI)
        entry = ObjectEntry(
            identity,
            "flow_points",
            day,
            start.isoformat(),
            (start + dt.timedelta(days=1)).isoformat(),
            path.relative_to(self.root).as_posix(),
            before[2],
            output["rows"],
            output["rows"] * 80,
            identity,
            identity,
        )
        with self._lock:
            self._entries[identity] = (entry, path, before)
            self._entries.move_to_end(identity)
            while len(self._entries) > self.capacity:
                self._entries.popitem(last=False)
        info = {
            "date": day,
            "rows": entry.rows,
            "bytes": entry.bytes,
            "generation_status": receipt["status"],
            "version": entry.version,
        }
        quality = directory / "quality.json"
        if quality.exists():
            try:
                q = json.loads(self._inside(quality).read_text())
                if not isinstance(q, dict) or not isinstance(
                    q.get("statuses", {}), dict
                ):
                    raise ValueError("quality.json 缺少有效状态字典")
                info["quality_statuses"] = q.get("statuses", {})
            except (ValueError, OSError):
                info["quality_metadata_status"] = "unreadable"
        else:
            info["quality_metadata_status"] = "unavailable"
        return entry, info

    def selection(self, request):
        directories = self._directories()
        entries, available, absent = [], [], []
        for day in _request_dates(request):
            if day not in directories:
                absent.append(day)
                continue
            directory = self._inside(directories[day])
            if not (directory / "day.json").is_file():
                absent.append(day)  # Producer has not published its completion receipt.
                continue
            entry, info = self._load(day, directories[day])
            entries.append(entry)
            available.append(info)
        return entries, {
            "dataset": "flow_points",
            "days": available,
            "dates_without_files": absent,
            "partial_dates": [
                v["date"] for v in available if v["generation_status"] == "partial"
            ],
            "calendar_note": "dates_without_files 未校验交易日历，可能包含休市日；不代表零成交",
            "semantics": "observed identifiable continuous-auction executions",
        }

    def selected(self, request):
        return self.selection(request)[0]

    def selected_refs(self, references):
        entries = []
        seen = set()
        directories = None
        for ref in references:
            uid = ref.get("object_id", "")
            if not re.fullmatch(r"[0-9a-f]{64}", uid) or uid in seen:
                raise ValueError("非法或重复基座对象引用")
            seen.add(uid)
            with self._lock:
                saved = self._entries.get(uid)
            if saved is None:
                directories = (
                    directories if directories is not None else self._directories()
                )
                day = ref.get("trade_date")
                if day not in directories:
                    raise ValueError("基座引用日期不存在")
                self._load(day, directories[day])
                with self._lock:
                    saved = self._entries.get(uid)
            if saved is None:
                raise ValueError("基座版本已变化或过期，请重新查询文件清单")
            entry = saved[0]
            if any(
                ref.get(k) != getattr(entry, k)
                for k in ("dataset", "trade_date", "version")
            ):
                raise ValueError("基座引用与固定版本不一致")
            self.path_for(entry)
            entries.append(entry)
        return entries

    def path_for(self, entry):
        with self._lock:
            saved = self._entries.get(entry.object_id)
        if saved is None or saved[0] != entry:
            raise ValueError("基座读取计划已过期，请重新查询")
        _, path, signature = saved
        if self._inside(path) != path or self._signature(path) != signature:
            raise ValueError("已选基座版本发生变化，停止读取以避免混合版本")
        return path
