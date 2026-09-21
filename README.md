# Market Data API 0.6.0

通过同一客户端读取逐笔行情和已生成的主动成交基座，按批返回 Arrow 数据，支持日期、股票和字段筛选。默认不在客户端保存行情文件。

0.6 新增 **`flow_points`**：按日期区间读取每日主动成交事件，默认全市场、全部 10 列；不会重新识别、清洗或计算因子。整日文件按行组分段传输，股票与字段过滤尽量减少读取量，长区间逐批消费。

| 数据集 | 返回内容 | 时间范围 |
|---|---|---|
| `flow_points` | 已识别、分档的主动成交事件原表 | 日期首尾包含，默认全天，可选每日时间窗口 |
| `orders` / `trades` / `snapshots` | 原委托、成交、快照 | `start/end` 半开区间；按日期请求时需提供每日窗口 |

原三类逐笔接口保持兼容；现有 Parquet、catalog v2 和基座文件均直接只读使用，无需重切数据。服务地址继续使用 `10.10.10.87:18787`，现有用户配置和令牌保留。

## 快速使用

在自己的 Python / Conda 环境安装发行包里的客户端：

```bash
python -m pip install --upgrade 'wheels/market_data_api-0.6.0-py3-none-any.whl[client]'
```

已有客户端配置时，直接读取基座：

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect(cores=2) as api:
    for batch in api.iter_points(
        "2026-09-01", "2026-09-04",  # 包含首尾日期
        symbols=["000001.SZ", "600000.SH"],  # 省略即全市场
        columns=["symbol", "time", "amount", "volume"],  # 省略即全部10列
    ):
        print(batch.num_rows)
    print(api.last_read_stats["coverage"])
```

仍可使用 `iter_batches({"dataset": "flow_points", "start_date": ..., "end_date": ...})`。`read_table()` 会在内存中收齐结果，大区间应使用迭代器。

基座返回 `symbol、time、active_order_id、side、depth、mode_mask、amount、volume、order_amount、order_volume`。order 口径取 `mode_mask=1/3`，price 取 `2/3`，两种口径不能直接混加。缺日与 partial 随覆盖信息报告，已有有效事件照常返回。完整契约见 [FLOW_POINTS.md](FLOW_POINTS.md)。

原来的逐笔请求方式保持不变：

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect() as client:
    query = {
        "dataset": "snapshots",
        "start": "2026-09-04T09:30:00+08:00",
        "end": "2026-09-04T09:35:00+08:00",
        "symbols": ["000001.SZ", "600000.SH"],
        "columns": ["symbol", "event_time", "last_px_i32", "volume_i64"],
    }
    for batch in client.iter_batches(query):
        print(batch.num_rows)
    print(client.last_read_stats)
```

`connect()` 读取 `~/.config/market-data-api/client.json`，复用到网关的 HTTP 连接，无需启动本机 FastAPI。新安装可运行 `./install-client.sh --native 10.10.10.87`，按提示输入个人令牌。

原来 `MarketDataClient()` → 本机 FastAPI 的调用方式仍然保留。使用基座接口时，将客户端及本机代理升级到 0.6；原有逐笔调用无需改写。

## 读取逻辑

```text
日期、时间、股票、字段
  → 逐笔 catalog / 基座日文件收据确定对象版本
  → 元数据判断需要的行组与列块
  → 合并相邻字节段，比较随机读与顺序读成本
  → 87 发送相关压缩字节，或采用整文件顺序传输
  → 客户端精确过滤股票与时间
  → Arrow RecordBatch / Table
```

- 少数股票的多日读取：利用已有 symbol min/max 跳过无关行组。
- 全市场短窗口：只读取重叠的五分钟桶；全字段时保留整文件通道。
- 多人同时读取：按个人令牌公平轮转，空闲时允许借用额外通道。
- 大查询拆成内部小段，不要求用户逐股票或逐文件请求。
- Range 通道可能传输同块内的其他股票，最终结果只包含指定股票。

## 常用参数

| 参数 | 含义 |
|---|---|
| `dataset` | `orders`、`trades`、`snapshots`、可选启用的 `flow_points` |
| `start` / `end` | 上海时区的 `[start, end)`，支持 ISO-8601 时区 |
| `start_date` / `end_date` | 包含首尾日期；逐笔数据需提供每日窗口，flow_points 默认全天；与 start/end 二选一 |
| `daily_start` / `daily_end` | 每天相同的半开时间窗口 |
| `symbols` | 精确股票代码数组；省略表示全部；空数组报错 |
| `columns` | 返回字段及顺序；省略表示全部 |
| `mode` | 默认 `direct`；显式 `cache` 会缓存完整五分钟对象 |
| `read_strategy` | 默认 `auto`；也可强制 `ranges` 或 `sequential` |
| `update` | cache 模式的 `missing_only`、`if_changed`、`force` |

股票、字段、时间过滤可同时使用；过滤所需的辅助列不会额外出现在返回结果中。不存在的股票返回有 schema 的空表。未知参数和字段报错。物理行顺序保持源对象/行组的顺序，不保证全市场按时间全局排序。

## 调优与边界

```python
client = MarketDataClient.connect(
    cores=2,
    connections=2,
    io_profile="hdd",    # 远端是 SSD 时可用 "ssd"
    read_options={"coalesce_gap_bytes": 512 * 1024},
)
```

`auto` 使用可配置的成本模型，不声称能实时判断所有文件的页缓存状态。对照测试可强制 `sequential` / `ranges`；选择股票和字段的结果语义不变。原生客户端的 Arrow 线程预算作用于当前 Python 进程；需要独立处理环境或多个进程统一调度时，可继续使用本机 FastAPI。

服务端仍只依赖 Python 标准库。安装器会在行情目录之外启用压缩元数据 SQLite 缓存，避免重复查询反复读取大量文件尾部。它不缓存或改写业务记录，可以删除重建。第一次访问未预热的历史区间仍可能受机械盘寻道影响。

## 文档与验证

- [快速安装和升级](QUICKSTART.md)
- [主动成交基座接口与返回字段](FLOW_POINTS.md)
- [基座真实数据测试结果](FLOW_POINTS_BENCHMARK.md)
- [完整用户指南](USER_GUIDE.md)
- [服务器部署](DEPLOYMENT.md)
- [数据文件格式](SERVER_DATA_FORMAT.md)
- [字节读取协议](RANGE_PROTOCOL.md)
- [实测性能与测试条件](BENCHMARK.md)
- [本版变更](RELEASE_NOTES.md)

开发与测试：

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python tools/build_release.py
```
