# 派生表：直接目录契约（0.8.0）

0.8.0 起，`derived.<表名>` 采用**直接目录契约**：生产者把 Parquet 文件写到约定目录，网关扫描目录自动发现，不再需要 `table.json`、版本目录或发布清单。新增日期、新增列、新增表都不需要改网关、重启服务或重新发布客户端。

首次使用派生表功能需要 0.8.0 客户端与配置了 `MDAPI_DERIVED_ROOT` 的 0.8.0 网关；旧逐笔、快照和 `flow_points` 接口继续兼容。

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect() as client:
    tables = client.tables()                     # 发现表
    fields = client.tables("daily_quality")      # 字段类型、粒度、覆盖
    quality = client.read_derived(
        "daily_quality", "2024-02-06", "2024-02-08",
        symbols=["000001.SZ"],
        columns=["date", "symbol", "base_volume", "reference_volume",
                 "volume_diff", "volume_error_pct", "deviation_status"],
    )
```

日期首尾包含。省略 `symbols` 返回全部股票，省略 `columns` 返回当前全部字段。大范围读取使用 `iter_derived()` 逐批消费；`read_table()` / `iter_batches()` 同样接受 `dataset="derived.daily_quality"`。

## 目录与文件格式

一张表一个目录，一天一个文件：

```text
/data/flow_points/.derived_tables/
  daily_quality/
    trade_date=2024-02-06/
      data.parquet
```

每个 `data.parquet` 是扁平 Parquet，必须包含：

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `time` | `timestamp[..., tz=Asia/Shanghai]` | 日表为交易日零点；窗口表为窗口锚点时间；写入工具统一规范为 `timestamp[ns, tz=Asia/Shanghai]`，同表不同日期的单位必须一致 |
| `symbol` | `string` | 股票代码，如 `000001.SZ` |

其他列任意增加。`data.parquet` 内 `time+symbol` 必须唯一。日级数据如果省略 `time`，写入工具会自动补当天 00:00（Asia/Shanghai）。

同一个表的不同日期可以具有不同列集合：网关按所有已存在文件求字段并集；旧日期缺少的新列在读取时返回**有类型的 null**，不是 0。同名字段在不同日期的类型必须一致，否则网关在规划阶段直接报错。

## 质量数据自动写入

`daily_quality` 由 flow-base 自动维护，不需要单独发布：

- `flow-base run` 每天校验完成后自动写入当天质量文件；
- `flow-base check`、`recheck`、`reconcile` 在生产质量文件真正更新后自动写入；
- 同一天重刷质量只会更新质量相关列，**不会覆盖后来手工追加的因子列**；
- API 派生根默认是 `<out-root>/.derived_tables`，也可用 `--derived-root` 或 `FLOW_BASE_DERIVED_ROOT` 指定。

## 因子列与窗口表

### 方式一：0.8.0 管理环境直接插入

写入工具随 flow-base 发布包提供；`market_data_api` 包保持纯读取。

```bash
./flow-base upsert-table \
  --root /data/flow_points/.derived_tables \
  --table daily_quality \
  --date 2026-09-23 \
  --input /path/to/factor.parquet
```

两种方式都按 `time+symbol` 合并：

- 输入里已有的列更新到匹配行；
- 新列追加，已有其他列保留；
- 输入里多出的 key 追加新行；
- 日级结果可只有 `symbol`，自动补 `time`；
- 5 分钟或其他窗口因子必须提供 `time`，换用新表名（例如 `orders_5m`）并声明 `--granularity 5m`。

举例：`factor.parquet` 只有 `symbol` 和 `buy_amount_3`，插入 `daily_quality` 后，`buy_amount_3` 自动出现在 API 中；不需要定义新表、发布清单或重启网关。

已有字段要改类型或改口径时，应新增字段名或新建表，不要静默覆盖历史含义。写入工具只允许同类数值之间的兼容转换（整数之间、浮点之间），跨类型会直接拒绝。

## 旧 0.7 表迁移

如果表仍是 0.7 的 `table.json + versions/` 布局，可用：

```bash
python tools/migrate_derived_direct.py \
  --root /data/flow_points/.derived_tables \
  --table daily_quality \
  --apply
```

迁移脚本只**新增** `trade_date=*/data.parquet` 和 `meta.json`，旧 `table.json`、`versions/` 原样保留，因此 0.7 网关在迁移和切换期间继续可用。脚本会校验旧对象的字节数、行数和 SHA-256；重复执行会跳过已存在的直接文件，不会覆盖。

为保证旧 0.7 客户端读到与旧网关完全一致的 Arrow schema，迁移脚本会把旧清单的 `arrow_schema` 保存到 `meta.json` 的 `legacy_arrow_schema`；新网关仅把它用于旧客户端兼容，新客户端仍按扫描出来的列定义读取。

## 质量值含义

每行是一个股票日，口径为**连续竞价**，不是包含集合竞价的全日成交量。参考为 ClickHouse 分钟数据中 09:31—11:30、13:01—14:59 的 239 个标签。表中包含质量底稿里出现的全部股票，包括基座成交量为 0 的记录；没有在逐笔或参考两侧出现的停牌证券，不虚构为零值。

| 字段 | 含义 |
| --- | --- |
| `base_volume` | 最终基座主买量加主卖量，按订单口径只计一次 |
| `buy_volume`、`sell_volume` | 最终基座主买量、主卖量 |
| `tick_volume` | 兼容列名，v2 中等于 `base_volume` |
| `raw_tick_volume` | 订单识别前的有效逐笔量，仅作差异诊断 |
| `price_mode_volume` | 价格档口径独立汇总，用于交叉核对，不能与订单口径相加 |
| `reference_volume` | 独立连续竞价参考成交量；参考不可用时为 null |
| `volume_diff` | 基座量减参考量；少为负、多为正 |
| `volume_error_ratio` | 差额除参考量；`-1` 即 `-100%` |
| `volume_error_pct` | 比例乘 100；`-100` 即 `-100%`，匹配为 0 |
| `deviation_status` | `available`、`reference_unavailable` 或 `zero_reference_volume` |
| `status`、`possible_issue` | 原始诊断信息，不作为 API 删行规则 |

参考量大于 0、最终基座量为 0，返回 `-100%`。参考缺失、不完整或参考量为 0 时，百分比为 null。API 不设置 1% 或其他可用性阈值；调用方自行筛选，例如 `abs(volume_error_ratio) <= 0.01`。

`time` 是交易日零点，供统一日期过滤，**不是该日结果的可得时刻**。盘中回测应使用数据发布时点或因子自己的可得时间字段，不能把当日汇总当作开盘前已知数据。

## 服务端

网关首次配置 `MDAPI_DERIVED_ROOT=/data/flow_points/.derived_tables` 后重启一次即可。之后文件出现、列增加、新表建立都会在下一次查询自动发现；网关仍是纯标准库只读服务，不执行因子计算，也不修改数据目录。

`GET /v1/tables` 返回表清单，`GET /v1/tables/<name>` 返回字段定义。原生客户端和本机 HTTP API 都支持；`POST /v1/data` 返回 Arrow 流。
