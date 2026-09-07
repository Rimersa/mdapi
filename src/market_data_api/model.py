from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from enum import Enum
from zoneinfo import ZoneInfo

DATASETS = ("orders", "trades", "snapshots")
SHANGHAI = ZoneInfo("Asia/Shanghai")
BUCKET_SECONDS = 300


class UpdateMode(str, Enum):
    MISSING_ONLY = "missing_only"
    IF_CHANGED = "if_changed"
    FORCE = "force"


class FetchMode(str, Enum):
    DIRECT = "direct"
    CACHE = "cache"


def parse_datetime(value: str | dt.datetime) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"非法时间 {value!r}，需要 ISO-8601 格式") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(SHANGHAI)


def parse_daily_time(value: str | dt.time | None) -> dt.time | None:
    if value is None:
        return None
    if isinstance(value, dt.time):
        parsed = value
    else:
        try:
            parsed = dt.time.fromisoformat(str(value).strip())
        except ValueError as exc:
            raise ValueError(f"非法每日时间 {value!r}，需要 HH:MM[:SS]") from exc
    if parsed.tzinfo is not None:
        raise ValueError("每日时间不能携带时区；统一使用 Asia/Shanghai")
    return parsed


def iso_shanghai(value: dt.datetime) -> str:
    return parse_datetime(value).isoformat(timespec="milliseconds")


def floor_bucket(value: dt.datetime) -> dt.datetime:
    value = parse_datetime(value)
    return value.replace(
        minute=(value.minute // 5) * 5,
        second=0,
        microsecond=0,
    )


def ceil_bucket(value: dt.datetime) -> dt.datetime:
    value = parse_datetime(value)
    floor = floor_bucket(value)
    return floor if floor == value else floor + dt.timedelta(seconds=BUCKET_SECONDS)


@dataclass(frozen=True)
class DataRequest:
    dataset: str
    start: dt.datetime
    end: dt.datetime
    mode: FetchMode = FetchMode.DIRECT
    update: UpdateMode = UpdateMode.MISSING_ONLY
    columns: tuple[str, ...] | None = None
    daily_start: dt.time | None = None
    daily_end: dt.time | None = None
    symbols: tuple[str, ...] | None = None
    read_strategy: str = "auto"

    def __post_init__(self) -> None:
        if self.dataset not in DATASETS:
            raise ValueError(f"未知数据集 {self.dataset!r}；可选 {DATASETS}")
        start = parse_datetime(self.start)
        end = parse_datetime(self.end)
        if end <= start:
            raise ValueError("end 必须晚于 start；区间采用 [start, end)")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "mode", FetchMode(self.mode))
        object.__setattr__(self, "update", UpdateMode(self.update))
        daily_start = parse_daily_time(self.daily_start)
        daily_end = parse_daily_time(self.daily_end)
        if (daily_start is None) != (daily_end is None):
            raise ValueError("daily_start 和 daily_end 必须同时指定")
        if daily_start is not None and daily_end <= daily_start:
            raise ValueError("daily_end 必须晚于 daily_start；暂不支持跨午夜窗口")
        object.__setattr__(self, "daily_start", daily_start)
        object.__setattr__(self, "daily_end", daily_end)
        if self.columns is not None:
            if isinstance(self.columns, str) or any(
                not isinstance(c, str) or not c for c in self.columns
            ):
                raise ValueError("columns 必须是字段名称数组")
            columns = tuple(dict.fromkeys(self.columns))
            if not columns:
                raise ValueError("columns 不能为空")
            object.__setattr__(self, "columns", columns)
        if self.symbols is not None:
            if isinstance(self.symbols, str):
                raise ValueError("symbols 必须是股票代码数组")
            if not self.symbols or any(
                not isinstance(s, str) or not s.strip() for s in self.symbols
            ):
                raise ValueError("symbols 必须是非空字符串数组；取全部股票时省略此参数")
            object.__setattr__(
                self, "symbols", tuple(dict.fromkeys(s.strip() for s in self.symbols))
            )
        if self.read_strategy not in {"auto", "ranges", "sequential"}:
            raise ValueError("read_strategy 必须是 auto、ranges 或 sequential")
        if self.mode == FetchMode.CACHE and self.read_strategy == "ranges":
            raise ValueError("ranges 只适用于 direct；cache 模式缓存完整对象")

    @classmethod
    def from_values(
        cls,
        *,
        dataset: str,
        start: str | dt.datetime,
        end: str | dt.datetime,
        mode: str | FetchMode = FetchMode.DIRECT,
        update: str | UpdateMode = UpdateMode.MISSING_ONLY,
        columns: list[str] | tuple[str, ...] | None = None,
        daily_start: str | dt.time | None = None,
        daily_end: str | dt.time | None = None,
        symbols: list[str] | tuple[str, ...] | None = None,
        read_strategy: str = "auto",
    ) -> "DataRequest":
        return cls(
            dataset=dataset,
            start=parse_datetime(start),
            end=parse_datetime(end),
            mode=FetchMode(mode),
            update=UpdateMode(update),
            columns=tuple(columns) if columns is not None else None,
            daily_start=parse_daily_time(daily_start),
            daily_end=parse_daily_time(daily_end),
            symbols=symbols,
            read_strategy=read_strategy,
        )

    def bucket_overlaps(
        self,
        bucket_start: str | dt.datetime,
        bucket_end: str | dt.datetime,
    ) -> bool:
        start = parse_datetime(bucket_start)
        end = parse_datetime(bucket_end)
        if end <= self.start or start >= self.end:
            return False
        if self.daily_start is None:
            return True
        start_time = start.timetz().replace(tzinfo=None)
        end_time = end.timetz().replace(tzinfo=None)
        return end_time > self.daily_start and start_time < self.daily_end

    @classmethod
    def from_query(cls, query):
        """Normalize the same strict query contract for HTTP and native clients."""
        raw = dict(query)
        allowed = {
            "dataset",
            "start",
            "end",
            "start_date",
            "end_date",
            "daily_start",
            "daily_end",
            "columns",
            "symbols",
            "mode",
            "update",
            "read_strategy",
        }
        if set(raw) - allowed:
            raise ValueError(f"不支持的请求参数: {sorted(set(raw) - allowed)}")
        if "dataset" not in raw:
            raise ValueError("必须指定 dataset")
        first, last = raw.pop("start_date", None), raw.pop("end_date", None)
        continuous = raw.get("start") is not None or raw.get("end") is not None
        daily = first is not None or last is not None
        if continuous == daily:
            raise ValueError(
                "必须二选一：start/end，或 start_date/end_date + 每日时间窗口"
            )
        if daily:
            if (
                first is None
                or last is None
                or not raw.get("daily_start")
                or not raw.get("daily_end")
            ):
                raise ValueError(
                    "日期区间模式需要 start_date/end_date 和 daily_start/daily_end"
                )
            first, last = dt.date.fromisoformat(first), dt.date.fromisoformat(last)
            if last < first:
                raise ValueError("end_date 不能早于 start_date")
            raw["start"] = f"{first.isoformat()}T00:00:00+08:00"
            raw["end"] = f"{(last + dt.timedelta(days=1)).isoformat()}T00:00:00+08:00"
        if raw.get("start") is None or raw.get("end") is None:
            raise ValueError("start 和 end 必须同时指定")
        return cls.from_values(**raw)


@dataclass(frozen=True)
class LocalResources:
    cpus: int
    total_memory: int
    available_memory: int
    free_disk: int


def detect_local_resources(cache_path: str) -> LocalResources:
    try:
        cpus = len(os.sched_getaffinity(0))
    except AttributeError:
        cpus = os.cpu_count() or 1
    try:
        import psutil

        memory = psutil.virtual_memory()
        total_memory = int(memory.total)
        available_memory = int(memory.available)
        free_disk = int(psutil.disk_usage(cache_path).free)
    except Exception:
        total_memory = int(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
        available_memory = int(
            os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        )
        stat = os.statvfs(cache_path)
        free_disk = int(stat.f_bavail * stat.f_frsize)
    return LocalResources(
        max(1, cpus),
        total_memory,
        available_memory,
        free_disk,
    )
