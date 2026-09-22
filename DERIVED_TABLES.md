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

## 实际接入：准备、发布、读取

计算程序负责计算因子；发布器负责登记已经计算好的Parquet。GitHub存放程序与文档，因子数据放在数据服务器上，不提交到Git。

### 1. 准备按日期存放的结果

例如新增日表`daily_order_flow`，在服务器准备：

```text
/home/quant/prepared/daily_order_flow/
  trade_date=2026-09-21/part-000.parquet
  trade_date=2026-09-22/part-000.parquet
```

每个Parquet至少包含两列：`symbol`为字符串（例如`000001.SZ`），`time`为带`Asia/Shanghai`时区的时间戳。日表可用所属日期零点；窗口表使用约定的窗口起点或终点。其他列就是已算好的结果，例如`buy_amount_3`、`sell_amount_3`。不要把无时区字符串当作时间戳发布。

日期目录必须与`time`所在的上海日期一致。日表每个股票日一行；窗口表按股票和窗口时间组织。需要拆成多个文件时，同一日期的所有分片都放进该日期目录，避免漏片或重复行。

### 2. 在服务器发布

在解压后的0.7.0安装包目录中运行，`python`须是已安装`market-data-api[client]`、可使用PyArrow的环境；读取服务自身仍只需要系统Python标准库。

```bash
python tools/publish_derived_table.py \
  --root /data/flow_points/.derived_tables \
  --name daily_order_flow \
  --input /home/quant/prepared/daily_order_flow \
  --granularity daily \
  --description '每日每股三档订单口径主买卖金额'
```

当前87主机已经启用上述发布根目录，可用其现有管理解释器：

```bash
PUBLISH_PYTHON=/home/quant/sz_blank_repair_20260922/api-admin-0.7/bin/python
PUBLISH_PACKAGE=/home/quant/sz_blank_repair_20260922/market-data-api-0.7.0
"$PUBLISH_PYTHON" "$PUBLISH_PACKAGE/tools/publish_derived_table.py" \
  --root /data/flow_points/.derived_tables \
  --name daily_order_flow \
  --input /home/quant/prepared/daily_order_flow \
  --granularity daily
```

首次使用某个表名就是新增表。发布成功后，下一次查询自动可见，无须改网关配置或重启。`--field-info fields.json`可附加字段单位和说明，例如：

```json
{
  "buy_amount_3": {"unit": "CNY", "description": "三档订单口径主买金额"},
  "sell_amount_3": {"unit": "CNY", "description": "三档订单口径主卖金额"}
}
```

### 3. 客户端读取

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect() as client:
    print(client.tables())                    # 发现新表
    print(client.tables("daily_order_flow"))  # 查看实际字段和说明
    table = client.read_derived(
        "daily_order_flow", "2026-09-21", "2026-09-22",
        symbols=["000001.SZ"],
        columns=["time", "symbol", "buy_amount_3", "sell_amount_3"],
    )
    frame = table.to_pandas()
```

读取当前已有的质量表，把表名换成`daily_quality`，字段换成`base_volume`、`reference_volume`、`volume_error_pct`即可。可运行随包的`examples/read_derived.py`。

### 四种更新的区别

| 需求 | 操作 | 已有数据的处理 |
| --- | --- | --- |
| 新增交易日 | 相同表名，只提供新日期目录 | 其他日期保留 |
| 修正已有日期 | 相同表名，提供该日期完整结果 | 该日期全部分片被替换 |
| 增加因子列 | 先把原列与新列按股票、时间合并，再发布完整日期 | 未更新的历史日期，新列返回null |
| 新增窗口表 | 用新表名，如`orders_5m`，设置`--granularity 5m` | 原日表保留，客户端通过新表名读取 |

**发布工具不是逐行追加或逐列合并。** 如果同一日期原来有多个文件，重新发布时要提供该日期的全部文件；若向`daily_quality`加列，也必须保留原有质量列。只传新因子列会让替换日期中缺少的旧列返回null。`--granularity`只说明数据粒度，不会替你把逐笔或日数据重新聚合成5分钟。

同表已有字段的数据类型保持不变，改变含义或类型使用新字段名或新表。按这个约定新增数据，只调整计算结果和发布参数，后续无需升级客户端或服务端。
