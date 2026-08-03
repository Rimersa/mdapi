# 五分钟数据目录契约（catalog v2）

完整字段说明和当前参考schema见
[`SERVER_DATA_FORMAT.md`](SERVER_DATA_FORMAT.md)。

这份契约是每日切片程序与只读网关之间的约定。网关不会切片、转换或修改数据，只读取
已经发布的分片catalog和Parquet。

## 目录结构

```text
DATA_ROOT/
├── catalog.json
├── catalog/
│   └── dataset=<dataset>/trade_date=YYYY-MM-DD/
│       ├── current.json
│       └── version=<version>.json
└── objects/
    ├── orders/trade_date=YYYY-MM-DD/version=<version>/...
    ├── trades/trade_date=YYYY-MM-DD/version=<version>/...
    └── snapshots/trade_date=YYYY-MM-DD/version=<version>/...
```

根`catalog.json`固定为`market-data-5m-catalog-index-v2`，约400字节，不允许包含全历史
`objects`数组。网关按请求日期直接读取对应`current.json`，再读取它指向的不可变
`version=<version>.json`。

## Parquet约束

- `dataset`必须是`orders`、`trades`或`snapshots`；
- 每个对象必须完全落在一个五分钟半开区间`[bucket_start, bucket_end)`；
- 必须有无时区`event_time`列，语义为上海时间；
- 多日每日窗口需要整数`time_int`，编码为`HHMMSSmmm`；
- 推荐Zstandard压缩、约131072行一个row group；
- 已发布文件不可原地覆盖；修订必须写新路径并使用新version。

## 根索引

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

## 当前指针

```json
{
  "format": "market-data-5m-catalog-pointer-v2",
  "generated_at": "2026-08-03T10:00:00+00:00",
  "dataset": "snapshots",
  "trade_date": "2026-05-29",
  "version": "a1b2c3d4e5f60708"
}
```

## 不可变版本分片

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
      "object_id": "全目录唯一且不可复用的对象ID",
      "dataset": "snapshots",
      "trade_date": "2026-05-29",
      "bucket_start": "2026-05-29T09:15:00.000+08:00",
      "bucket_end": "2026-05-29T09:20:00.000+08:00",
      "relative_path": "objects/snapshots/trade_date=2026-05-29/version=a1b2c3d4e5f60708/0915.parquet",
      "bytes": 12345678,
      "rows": 500000,
      "uncompressed_bytes": 87654321,
      "source_fingerprint": "上游分区内容或版本的稳定指纹",
      "version": "a1b2c3d4e5f60708"
    }
  ]
}
```

对象要求：

- 时间使用ISO-8601，桶时间携带`+08:00`；
- `bytes`必须等于文件大小，`rows`必须等于Parquet元数据行数；
- `uncompressed_bytes`推荐为全部row group的`total_byte_size`之和；
- `relative_path`必须位于数据根目录内；
- 一个版本分片只能含一个dataset、trade_date和version；
- version只允许安全的ASCII字母、数字、点、下划线和短横线。

## 每日发布顺序

1. 在未发布目录生成并校验当天Parquet；
2. 原子发布Parquet到新的version路径；
3. 原子发布不可变`version=<version>.json`；
4. 原子替换当天`current.json`；
5. 最后原子替换固定大小的根`catalog.json`以更新时间。

旧版本JSON和Parquet必须保留，至少覆盖最长请求持续时间。这样更新发生时，旧请求继续
读取旧version，新请求读取新version，网关无需重启。

项目内`mdapi-build`只是可选参考实现。已有每日切片程序时，只需满足本契约。
