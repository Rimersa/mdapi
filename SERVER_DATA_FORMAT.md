> 0.5.0 兼容说明：现有 Parquet 与 catalog v2 无需改动。新增读取协议见 [RANGE_PROTOCOL.md](RANGE_PROTOCOL.md)。压缩元数据缓存位于行情目录之外。

# 87服务器五分钟数据格式说明

本文定义每日切片程序必须交付给只读网关的数据格式。网关不会自行切片或转换数据，也
不会写入数据根目录。

## 1. 数据根目录

网关接收一个独立的五分钟成品目录，例如：

```text
/data/market_data_5m/
├── catalog.json
├── catalog/
│   ├── dataset=orders/trade_date=YYYY-MM-DD/
│   │   ├── current.json
│   │   └── version=<version>.json
│   ├── dataset=trades/...
│   └── dataset=snapshots/...
└── objects/
    ├── orders/
    ├── trades/
    └── snapshots/
```

这个目录不能与原始数据湖混用。根`catalog.json`是固定大小的小索引，不再保存全部
对象描述。真正定位Parquet的是按数据集、交易日、版本分片JSON中的`relative_path`。

网关不关心上游用什么程序切片。它只要求输入已经是本文定义的五分钟成品格式；
切片、校验和每日原子发布由上游数据程序负责。

当前五日验证目录采用以下推荐布局：

```text
objects/<dataset>/trade_date=YYYY-MM-DD/version=<version>/
    _bucket_5m=HHMM/00000000.parquet
```

## 2. 数据集和切片

当前支持三个固定数据集：

- `orders`：逐笔委托；
- `trades`：逐笔成交；
- `snapshots`：逐笔快照。

每个Parquet对象必须完全属于一个五分钟半开区间
`[bucket_start, bucket_end)`。同一个桶可以有多个Parquet文件，catalog逐个登记即可。
空桶不需要创建空文件，也不需要catalog条目。

## 3. 所有Parquet的强制列

| 列名 | 类型 | 语义 |
|---|---|---|
| `event_time` | 无时区`timestamp[ms]`或`timestamp[us]` | 上海本地时间语义，用于精确过滤 |
| `time_int` | `int32`或可安全比较的整数 | `HHMMSSmmm`，用于多日每日窗口过滤 |

例如`09:25:03.120`编码为`92503120`。

要求：

- 一个对象内所有`event_time`都必须落在catalog声明的五分钟桶内；
- 同一数据集跨日期、版本的列名和类型应保持一致；
- 业务列原样保留，API不会进行价格或数量单位转换；
- 推荐Zstandard压缩、约131072行一个row group；
- 已进入catalog的文件视为不可变对象，不能原地覆盖。

数据修订时应写入新路径、使用新`version`和新`object_id`，最后切换catalog。

## 4. 当前验证数据的参考schema

下面是87五日验证目录实际读取到的schema。服务只强制依赖`event_time`和`time_int`，但
正式上游最好保持这些业务字段及类型稳定，避免用户代码跨日期不一致。

### orders

```text
symbol: large_string
market: large_string
event_time: timestamp[us]
time_int: int32
order_id: int64
exch_order_id: int64
order_type: large_string
side: large_string
price_i32: int32
qty_i32: int32
trade_date: date32[day]
act: large_string
ptype: large_string
cxl_from_trades: bool
```

### trades

```text
symbol: large_string
market: large_string
event_time: timestamp[ms]
time_int: int32
trade_id: int64
trade_code: large_string
order_code: int64
bs_flag: large_string
price_i32: int32
qty_i32: int32
ask_seq_no: int64
bid_seq_no: int64
```

### snapshots

```text
symbol: large_string
market: large_string
event_time: timestamp[us]
time_int: int32
last_px_i32: int32
volume_i64: int64
turnover_i64: int64
trade_count_i32: int32
cum_volume_i64: int64
cum_turnover_i64: int64
high_px_i32: int32
low_px_i32: int32
open_px_i32: int32
prev_close_px_i32: int32
ask_px_1 ... ask_px_10: int32
ask_sz_1 ... ask_sz_10: int64
bid_px_1 ... bid_px_10: int32
bid_sz_1 ... bid_sz_10: int64
weighted_avg_ask_px_i32: int32
weighted_avg_bid_px_i32: int32
total_ask_sz_i64: int64
total_bid_sz_i64: int64
trade_date: date32[day]
cum_turnover_clean: int64
cum_volume_clean: int64
session: large_string
```

## 5. 分片catalog v2

### 5.1 固定大小的根索引

根目录必须有一个UTF-8 `catalog.json`，其大小不随历史数据增长：

```json
{
  "format": "market-data-5m-catalog-index-v2",
  "generated_at": "2026-08-03T10:00:00+00:00",
  "bucket_seconds": 300,
  "pointer_format": "market-data-5m-catalog-pointer-v2",
  "pointer_pattern": "catalog/dataset={dataset}/trade_date={trade_date}/current.json",
  "shard_format": "market-data-5m-catalog-shard-v2",
  "version_pattern": "catalog/dataset={dataset}/trade_date={trade_date}/version={version}.json"
}
```

当前编码后约400字节。它不列出日期，也不包含`objects`数组。网关根据用户请求中的
数据集和日期直接构造分片路径，因此启动和单日请求都不会扫描全部历史。

### 5.2 当天当前版本指针

例如：

```text
catalog/dataset=snapshots/trade_date=2026-05-29/current.json
```

内容很小：

```json
{
  "format": "market-data-5m-catalog-pointer-v2",
  "generated_at": "2026-08-03T10:00:00+00:00",
  "dataset": "snapshots",
  "trade_date": "2026-05-29",
  "version": "a1b2c3d4e5f60708"
}
```

### 5.3 不可变版本分片

指针对应：

```text
catalog/dataset=snapshots/trade_date=2026-05-29/
    version=a1b2c3d4e5f60708.json
```

该文件只描述一个数据集、一个交易日、一个版本：

```json
{
  "format": "market-data-5m-catalog-shard-v2",
  "generated_at": "2026-08-03T10:00:00+00:00",
  "bucket_seconds": 300,
  "dataset": "snapshots",
  "trade_date": "2026-05-29",
  "version": "a1b2c3d4e5f60708",
  "objects": [
    {
      "object_id": "全目录唯一对象ID",
      "dataset": "snapshots",
      "trade_date": "2026-05-29",
      "bucket_start": "2026-05-29T09:15:00.000+08:00",
      "bucket_end": "2026-05-29T09:20:00.000+08:00",
      "relative_path": "objects/snapshots/trade_date=2026-05-29/version=a1b2c3d4e5f60708/_bucket_5m=915/00000000.parquet",
      "bytes": 12345678,
      "rows": 500000,
      "uncompressed_bytes": 87654321,
      "source_fingerprint": "上游分区内容的稳定指纹",
      "version": "a1b2c3d4e5f60708"
    }
  ]
}
```

`version`只允许1至128个ASCII字母、数字、点、下划线或短横线，并且必须以字母或数字
开头。同一个版本文件一经发布不可覆盖。历史修订必须使用新的version文件。

### 5.4 对象字段

| 字段 | 要求 |
|---|---|
| `object_id` | 全目录唯一；文件内容或版本变化时必须改变 |
| `dataset` | `orders`、`trades`或`snapshots` |
| `trade_date` | `YYYY-MM-DD` |
| `bucket_start/end` | 带`+08:00`的ISO-8601五分钟半开区间 |
| `relative_path` | 数据根目录内相对路径，禁止`..`越界 |
| `bytes` | 必须等于实际文件大小；不一致时网关拒绝发送 |
| `rows` | 必须等于Parquet元数据行数，用于cache校验 |
| `uncompressed_bytes` | 推荐填写所有row group的`total_byte_size`之和 |
| `source_fingerprint` | 上游源分区内容或版本的稳定指纹 |
| `version` | 必须等于所在不可变版本分片的version |

### 5.5 实际规模

五日验证目录原单文件catalog包含2409个对象、大小1,281,143字节。迁移后：

- 根索引固定400字节；
- 15个当前指针；
- 15个不可变数据集/交易日分片；
- 实际版本分片约38–143KB。

历史继续增长只会增加独立日分片，不会增加网关启动时必须读取的根索引。

## 6. 每日原子发布顺序

1. 在未发布的临时目录生成当天全部五分钟Parquet；
2. 校验schema、时间边界、行数、文件大小和Parquet可读性；
3. 关闭文件并原子重命名到最终路径；
4. 生成该数据集/交易日的新不可变`version=<version>.json`，校验后原子发布；
5. 生成新的`current.json`临时文件，`flush/fsync`后原子替换当天指针；
6. 最后原子替换约400字节的根`catalog.json`，更新`generated_at`。

必须先发布Parquet和不可变版本分片，再切换`current.json`。网关会在下一次请求自动
重载，无需重启。已经拿到旧manifest的长请求继续按旧version读取；新请求读取新version。

## 7. 从单文件v1迁移

迁移工具默认只打印计划：

```bash
python3 tools/migrate_catalog_v2.py --root /五分钟数据根目录
```

确认后执行：

```bash
python3 tools/migrate_catalog_v2.py \
  --root /五分钟数据根目录 \
  --execute
```

迁移过程只读取旧`catalog.json`并新增catalog元数据分片；不会打开、修改、移动或删除任何
Parquet。所有分片完整写入后才原子切换根索引，默认不留下旧格式副本；中途失败时根索引
尚未切换，原目录仍继续有效。只有确实需要额外归档时才显式增加`--keep-backup`。

## 8. 删除和保留策略

- 原始数据不属于网关目录，任何情况下都不能由本服务删除或修改；
- 新版本发布后建议保留旧派生对象，至少覆盖最长请求持续时间；
- 不要删除仍被当前指针或可能进行中的请求引用的版本JSON和Parquet；
- 如以后确需清理旧派生版本，应由独立维护程序根据catalog引用关系和保留期处理，不能
  让网关自行删除。

## 9. 权限

运行网关的账号只需要：

- 对数据根目录、`catalog.json`和Parquet拥有读取与目录遍历权限；
- 不需要对数据目录拥有写权限；
- 配置和用户令牌放在数据目录之外。

更紧凑的机器契约仍保留在[`DATA_CONTRACT.md`](DATA_CONTRACT.md)。
