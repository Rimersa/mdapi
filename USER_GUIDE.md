# Market Data API 0.5.0 用户指南

## 连接与返回格式

Python 推荐使用 `MarketDataClient.connect()`，读取本机已有配置，也可显式提供 `gateway_host`、`gateway_port`、`gateway_token`、`config`、`cache_root`。显式连接其他服务器时，不会把原配置的令牌自动发送到不同地址。

`iter_batches(query)` 是一次性迭代器，每批为 `pyarrow.RecordBatch`。逐批处理并释放可以控制工作内存。`read_table(query)` 把全部结果放入一张 `pyarrow.Table`；完整结果是否保留在内存由调用方决定。

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect(cores=2, io_profile="hdd") as client:
    table = client.read_table({
        "dataset": "snapshots",
        "start": "2026-09-04T09:30:00+08:00",
        "end": "2026-09-04T09:35:00+08:00",
        "symbols": ["000001.SZ"],
        "columns": ["symbol", "event_time", "last_px_i32", "volume_i64"],
    })
    print(table)
    print(client.last_read_stats)
```

需要 Pandas / Polars 时可自行安装并转换；SDK 不自动转换价格单位或业务字段值。

## 股票、字段和时间

`symbols` 是精确股票代码集合；不会模糊匹配或自动补交易所后缀。省略或设为 None 表示全部，空数组报错。不存在的股票返回保留字段类型的空结果。

`columns` 决定最终返回的字段和顺序。过滤所需的 symbol、event_time 等列会在内部读取，但没有显式选择就不额外返回。

时间使用 `[start, end)`。无时区时间按 Asia/Shanghai 解释。日期区间模式的 `end_date` 包含当天，并且必须同时指定每日窗口。

```python
query = {
    "dataset": "trades",
    "start_date": "2026-09-01",
    "end_date": "2026-09-04",
    "daily_start": "09:30",
    "daily_end": "10:00",
    "symbols": ["000001.SZ", "600000.SH"],
    "columns": ["symbol", "event_time", "price_i32", "qty_i32"],
}
```

未提供所有必要参数、拼错参数、字段不存在、空字段数组均明确报错。没有任何已发布分区是 404；已有分区但股票或精确时间没有匹配行则返回空表。已发布 current 指向损坏或缺失版本索引时会报错，不把损坏当成合法的无数据日。API 不内置交易日历；未发布日期与非交易日需由调用方结合自身日历判断。

批次边界不是交易日或五分钟桶边界。结果按源对象和行组顺序读取，不保证全市场事件的全局时间排序。

## 两种存储模式

默认 `mode="direct"`：下载所需数据块，使用后释放，不在本机保存行情 Parquet。元数据有容量上限，连接及缓冲区可复用。

显式 `mode="cache"`：保留原来的完整五分钟对象缓存，第一次仍下载缺失桶的全部股票和字段，后续可复用。股票和字段过滤作用于返回结果。

缓存更新策略：

- `missing_only`：默认，不更新已有桶。
- `if_changed`：跟随服务器版本更新。
- `force`：重新下载所选桶的当前版本。

保留不更新已有桶的语义意味着同一天的不同缓存桶可能来自不同修订版本。需要最新修订时用 `if_changed`；需要研究数据完全可复现时应保存本次输入版本并管理独立数据快照。默认 direct 查询在清单获取时固定对象版本。

`read_strategy="ranges"` 只适用于 direct，不能用于完整对象缓存。

## 读取策略与机械盘

| 选择 | 行为 |
|---|---|
| `auto` | 有股票或字段条件时计划行组/列块读取；结合传输量和寻道成本决定是否采用整文件 |
| `ranges` | 强制按块读取，仍合并相邻块；适合对照测试或特定负载 |
| `sequential` | 完整传输选中的文件，再在本机精确筛选 |

三种策略返回相同逻辑数据。没有可用统计信息时保守保留行组，不猜测并丢弃记录。

```python
client = MarketDataClient.connect(
    cores=2,
    connections=2,
    io_profile="hdd",
    read_options={
        "coalesce_gap_bytes": 512 * 1024,
        "seek_cost_bytes": 1024 * 1024,
        "sequential_threshold": 0.85,
        "bundle_bytes": 16 * 1024 * 1024,
        "metadata_cache_bytes": 64 * 1024 * 1024,
    },
)
```

`ssd` 预设使用更小的合并距离和寻道成本。参数影响计划，不改变结果。成本模型不实时探测所有文件是否命中系统页缓存；需要在自己的网络、存储和负载下做对照。

完整对象通道保留网络和解码流水线；按块通道一次只规划有限对象，并对每组下载/解码所需内存单独检查。大对象不保证能装进任意小内存机器；不足时会拒绝或中止并抛出异常。

## 多用户和生命周期

每位用户使用独立令牌。同一个 Python 进程复用一个客户端即可；它维护有限连接池。可从多个线程调用同一原生客户端，`last_read_stats` 属于当前线程最近一次读取。所有读取结束后再关闭客户端。

服务器默认最多两个传输通道，按用户轮转。内部小段结束会释放通道；队列和等待时间有上限。大量发起并发请求不会让机械盘更快。

提前结束迭代时关闭生成器：

```python
stream = client.iter_batches(query)
try:
    for batch in stream:
        if enough_data(batch):
            break
finally:
    stream.close()
```

可恢复的网络中断只重试未完成的对象。已经完整接收的对象不重复返回；重试预算耗尽后明确报错。API 不为用户已经执行的业务计算提供事务回滚。

## 观察传输效果

原生客户端完成读取后：

```python
print(client.last_read_stats)
```

主要字段：`source_bytes` 是选中完整文件的总大小；`metadata_bytes` 是元数据响应负载；`transfer_bytes` 是实际接收的数据段负载（包括重试收到的字节）；`planned_bytes`、`range_count`、`skipped_objects`、`sequential_objects`、`returned_rows`、`queue_ms` 用于诊断。`versions` 给出各日期实际读取的发布版本；cache 模式同日若出现多个版本会一并列出。

这些数据不包含 TCP/HTTP 头和上行请求体，不能直接当成网卡总字节数。`client.estimate(query)` 默认做轻量清单和资源检查；按块模式的具体传输量随窗口规划，`estimated_transfer_bytes` 为 None，最终看统计值。

本机 HTTP 入口继续使用 `MarketDataClient(base_url)` 和 `/v1/estimate`、`/v1/data`。本机服务可用 `mdapi-local --cores 2 --io-profile hdd`。原生客户端可连接 0.4 网关并以完整对象读取作为兼容路径；强制 ranges 会明确要求升级。

`estimate` 中的 rows 和 estimated_arrow_memory 是未精确过滤的候选数据上界，不是最终股票结果的行数或内存。默认不设置总结果硬上限；工作内存另按读取窗口检查。
