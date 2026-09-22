# 派生表：日级质量与窗口因子

0.7.0 增加统一的 `derived.<表名>` 入口。首张生产表是 `daily_quality`；以后增加因子列或不同窗口表，通过发布数据和表说明接入，无需再次修改客户端、网关或重启服务。客户端首次使用这项功能需要升级到0.7.0；原有逐笔和基座接口继续兼容。

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect() as client:
    tables = client.tables()                     # 已发布的表
    fields = client.tables("daily_quality")      # 字段类型、口径、单位、覆盖
    quality = client.read_derived(
        "daily_quality", "2024-02-06", "2024-02-08",
        symbols=["000001.SZ"],
        columns=["date", "symbol", "tick_volume", "reference_volume",
                 "volume_diff", "volume_error_pct", "deviation_status"],
    )
```

日期首尾包含。省略 `symbols` 返回全部股票，省略 `columns` 返回这张表当前全部字段。大范围读取使用 `iter_derived()` 逐批消费。常规 `read_table()` / `iter_batches()` 同样接受 `dataset="derived.daily_quality"`。

## 质量值含义

每行是一个股票日，口径为**连续竞价**，不是包括集合竞价的全日成交量。参考为ClickHouse分钟数据中09:31—11:30、13:01—14:59的239个标签。本表发布已有基座质量明细中出现的全部股票，包括基座成交量为0的记录；没有在逐笔或参考两侧出现的停牌证券，不虚构为零值。

| 字段 | 含义 |
| --- | --- |
| `base_volume` | 最终基座主买量加主卖量，按订单口径只计一次 |
| `buy_volume`、`sell_volume` | 最终基座的主买量、主卖量 |
| `tick_volume` | 保留的兼容列名，v2中等于`base_volume` |
| `raw_tick_volume` | 订单识别前的有效逐笔量，仅作差异诊断 |
| `price_mode_volume` | 价格档口径的独立汇总，用于交叉核对，不能与订单口径相加 |
| `reference_volume` | 独立连续竞价参考成交量；参考不可用时为null |
| `volume_diff` | 基座量减参考量；少为负、多为正 |
| `volume_error_ratio` | 差额除参考量；`-1`即`-100%` |
| `volume_error_pct` | 比例乘100；`-100`即`-100%`，匹配为0 |
| `deviation_status` | `available`、`reference_unavailable`或`zero_reference_volume` |
| `status`、`possible_issue` | 原始诊断信息，不作为API删行规则 |

参考量大于0、最终基座量为0，返回`-100%`。参考缺失、不完整或参考量为0时，百分比为null；后两种情况不能伪装成0偏差。API不设置1%或其他可用性阈值，调用方自行筛选，例如 `abs(volume_error_ratio) <= 0.01`；无有效分母时由调用方另定策略。

API直接发布已经完成重校验的v2 `quality.parquet`，不在读取请求中重新计算。v2校验读取最终`points.parquet`，分别汇总主买和主卖，按订单口径相加后与独立ClickHouse连续竞价参考量比较；价格档口径单独核对，两种模式不重复相加。

历史740个日期仅重新汇总和更新质量Parquet/JSON，不重跑基座生产。已有已验证的ClickHouse参考量原样复用并记录来源哈希。原有效逐笔量保留在`raw_tick_volume`解释识别损失；`volume_diff`和`volume_error_ratio`只有一个主口径，均基于最终基座量。以`volume_basis=points_order_mode`标识。

`time`是交易日零点，供统一日期过滤，**不是该日结果的可得时刻**。盘中回测应使用数据发布时点或因子自己的可得时间字段，不能把当日汇总当作开盘时已知数据。

## 窗口与字段扩展

表以`symbol`和Asia/Shanghai时区的`time`为基础。日表一行一个股票日；5分钟或其他窗口表可以使用相同接口，声明`granularity`，并增加`window_start`、`window_end`等字段。`daily_start/daily_end`按`time`过滤，每日窗口是左闭右开；表说明应明确`time`表示窗口起点、终点还是其他锚点。

例如，后续可向日表发布`buy_amount_3`、`sell_amount_3`等字段，或新建`orders_5m`。当前客户端自动发现新表和新列；旧分区缺少新增列时返回该类型的null。已有字段改类型或改口径应发布新字段/新表，避免静默改变历史含义。复用`daily_quality`增列时要保留质量字段。质量快照发布器再次运行会按股票日保留已有因子列；准备期间若表被其他发布者更新，会中止而不覆盖。

## 服务与发布

服务端首次配置`MDAPI_DERIVED_ROOT=/data/flow_points/.derived_tables`，重启一次。原认证、用户队列、版本固定和读取预算继续生效。服务端仍只依赖Python标准库，不执行因子计算。

`GET /v1/tables`返回表清单，`GET /v1/tables/<name>`返回字段说明。原生客户端及本机HTTP API均支持。网关的数据传输继续使用清单、Parquet元数据与范围读取协议；本机HTTP API的`POST /v1/data`返回Arrow流。

发布器运行在独立的具备PyArrow的管理环境。输入为`trade_date=YYYY-MM-DD/*.parquet`，至少有`symbol:string`和`time:timestamp[..., tz=Asia/Shanghai]`。新日期或替换日期只需提供相应分区，其他日期自动保留。

```bash
python tools/publish_derived_table.py --root /data/flow_points/.derived_tables \
  --name orders_5m --input /data/prepared/orders_5m --granularity 5m \
  --description '每股5分钟三档订单口径；time为窗口结束时间'

python tools/publish_daily_quality.py \
  --points-root /data/flow_points --derived-root /data/flow_points/.derived_tables
```

发布器复制到不可变版本目录，校验行数、日期、基础字段和兼容类型后，最后原子替换`table.json`。已有读取继续使用固定旧版本；下一次查询看到新版本。读服务不会读取目录中未登记的文件，不会绕过根目录边界。
