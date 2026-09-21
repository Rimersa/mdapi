# 主动成交基座（flow_points）

本接口按日期区间读取已落盘的每日 `points.parquet`，返回订单级主动成交事件，不重新识别、清洗、分档或计算因子。网关地址和用户令牌与逐笔接口一致。

## 默认行为与选择方式

- `dataset="flow_points"`，必填 `start_date`、`end_date`，**包含首尾两天**；同日查询可令两者相同。
- 默认返回日期区间内已发布文件的全部股票、全部 10 列。
- 可选 `symbols=["000001.SZ", "600000.SH"]`，股票代码需与源文件完全一致；省略为全市场，空数组报错，不存在的股票返回有固定 schema 的空结果。
- 可选 `columns=["symbol", "time", "amount", "volume"]`，返回字段顺序与列表一致。过滤需要的辅助列不会额外出现在结果中。
- `iter_points(...)` / `iter_batches(query)` 逐批返回 Arrow `RecordBatch`，默认每批最多 131,072 行；批次边界与日期、订单边界无关，最后一个批次可能更小。
- 默认 `read_strategy="auto"`；利用 Parquet 行组统计筛选候选股票与时间，并按需要的列块传输，客户端再精确过滤。不能根据统计跳过的行组仍须读取，因此传输量会大于最终结果大小。
- 每个网络段默认约 16 MiB；如果单个行组本身更大，以该行组为下限，并在分配前检查内存额度。整日文件不整体放入内存；日期增多增加总时间，不要求同时载入多日数据。
- 本版基座接口使用 `direct`，不提供整日文件的 `cache` 模式。调用 `read_table` 会在调用者内存中保留全部结果，大范围应使用迭代器边读边消费。

也支持精确时间 `start/end`（半开区间 `[start,end)`），或在日期范围中增加 `daily_start/daily_end`。时间统一按上海时区解释，与原 API 规则一致。`start/end` 和 `start_date/end_date` 两种写法不能混用。原始逐笔数据的日期区间模式仍需明确每日时间窗口，其行为不变。

## Python 请求

升级客户端后，使用现有配置连接到已启用基座的网关：

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect(cores=2) as api:
    # 同一天，全市场，全部10列；日期首尾均包含。
    for batch in api.iter_points("2026-09-02", "2026-09-02"):
        print(batch.num_rows, batch.schema)
        # 在这里聚合、写入自己的文件或交给下游，不累积所有批次。

    # 同一请求传代码列表，减少逐股请求和排队次数。
    query = {
        "dataset": "flow_points",
        "start_date": "2026-09-02",
        "end_date": "2026-09-04",
        "symbols": ["000001.SZ", "600000.SH"],
        "columns": ["symbol", "time", "amount", "volume"],
    }
    print(api.estimate(query)["coverage"])
    for batch in api.iter_batches(query):
        print(batch.num_rows)  # 替换为自己的处理函数。
    print(api.last_read_stats)
```

`connect()` 读取现有 `~/.config/market-data-api/client.json`。新机器可运行发行包内的 `./install-client.sh --native 10.10.10.87`，按提示填写个人令牌。

## HTTP 请求与返回

现有本机 FastAPI 代理同样支持本数据集。代理的接口为 `POST /v1/estimate`（JSON 查询估计/覆盖情况）和 `POST /v1/data`（Arrow IPC 流）：

```bash
curl -X POST http://127.0.0.1:18788/v1/estimate \
  -H 'Content-Type: application/json' \
  -d '{"dataset":"flow_points","start_date":"2026-09-02","end_date":"2026-09-02","symbols":["000001.SZ"],"columns":["time","amount","volume"]}'
```

上述 POST 是**本机代理**的接口。推荐的原生 Python 客户端直接连接远端网关，不需要启动本机代理。远端网关继续使用已有的压缩 Parquet 元数据/字节段协议，其 `GET /v1/manifest?dataset=flow_points&start_date=2026-09-02&end_date=2026-09-02` 返回 JSON 文件清单；不要把网关的二进制文件传输接口当成返回 JSON 行的接口。

事件表保留原始 10 列及类型，不增加聚合行、补量或 NaN：

| 字段 | Arrow 类型 | 含义 |
| --- | --- | --- |
| symbol | string | 股票代码 |
| time | timestamp[ns, Asia/Shanghai] | 事件时间 |
| active_order_id | int64 | 主动方订单号，与股票、时间、方向合用识别事件 |
| side | string | B 主动买，S 主动卖 |
| depth | uint8 | 1–5 档，5 表示五档及以上 |
| mode_mask | uint8 | 1 为 order，2 为 price，3 为两者共享 |
| amount | float64 | 本行金额，元 |
| volume | int64 | 本行股数 |
| order_amount | float64 | 本事件观察到的整单金额，元 |
| order_volume | int64 | 本事件观察到的整单股数 |

order 与 price 两种口径包含同一笔成交，**不能把全表 amount/volume 直接相加**。order 取 `mode_mask in [1,3]`，price 取 `[2,3]`。本接口保持源文件物理顺序，按日期顺序连接，日期内部不保证全市场全局时间排序。

## 缺日和质量信息

`estimate(query)["coverage"]` 和原生客户端读后的 `last_read_stats["coverage"]` 包含：

- `days`：实际读取日期、候选行数、文件大小、固定版本、`generation_status` 与可读取的 `quality_statuses`。
- `dates_without_files`：范围内没有已发布基座的日期，可能含周末或节假日；它不是交易所缺失日认定，也不表示零成交。整个区间均无文件时返回 404。
- `partial_dates`：生产状态为 partial 的日期。仍返回其中有效事件，不替用户丢弃或放大。

质量摘要是**全日原文件的全市场摘要**，不会因股票过滤重新计算。complete 只说明基座生产完成，不是逐笔完整性证明。可识别事件为空的股票没有事件行，不伪造一行零值。质量 JSON 缺失或损坏会单独标注，不阻止读取有效 points；文件本身与 day.json 不一致、版本变化或数据传输无法恢复会明确报错。

读取计划绑定具体文件版本。网络断开时只重取当前尚未返回的片段；已交给调用者的批次不会因内部重试重复。原排队、公平调度及 HTTP 错误处理策略保持不变，503 队列错误仍由调用者处理。取消迭代会释放连接和内存额度。

## 配置与复现测试

网关参数 `--points-root` 或环境变量 `MDAPI_POINTS_ROOT` 用于启用基座数据集。不设置即不启用基座数据集。87 使用真实根目录 `/data/flow_points`，自动发现 `machine=*/trade_date=*`，不依赖手动制作的静态 `flow_points_all` 链接集合；也支持根目录直接放 `trade_date=*`。同一天出现两份目录会报错。

日期目录可通过软链接指向根目录内的不可变版本；链接不能逃到配置根目录以外。因此不要把 `/data/flow_points_all` 这个外部链接视图作为 points-root。元数据缓存必须同时位于逐笔和基座数据根目录之外。网关仍只使用 Python 标准库，CPU 解码和最终过滤在客户端完成。

开发目录中可重跑真实数据测试：

```bash
PYTHONPATH=src python tools/bench_points.py \
  --source /已有基座/trade_date=2024-02-02 \
  --source /已有基座/trade_date=2026-09-02 \
  --work ./dist/一个尚不存在的测试目录 \
  --output ./benchmarks/新测试结果.json
```

测试工具仅在本机建立临时 loopback 网关、只读硬链接样本并在结束时关闭该网关。不会部署或重启正式服务。本次实际结果见 [FLOW_POINTS_BENCHMARK.md](FLOW_POINTS_BENCHMARK.md)。
