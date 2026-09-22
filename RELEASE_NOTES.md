# Market Data API 0.7.0

增加通用派生表读取：首张`daily_quality`提供每日每股最终主买加主卖基座量、独立参考量和有符号偏差，不按质量阈值过滤。实际基座量为0且参考量大于0时如实返回-100%；无有效分母时返回null及原因。

- `tables()`发现表和字段，`read_derived()` / `iter_derived()`统一读取日表和窗口表。
- 新增表和可空字段后，同一客户端和网关自动读取，旧分区缺列返回有类型的null；无须重启或重新发布代码。
- 新表复用现有认证、公平队列、版本固定、按股票/字段筛选和有界批读取。
- 附带通用表发布器和全量质量快照发布器；网关仍为纯标准库只读服务。
- 第一次使用派生表功能需要0.7.0客户端和配置`MDAPI_DERIVED_ROOT`的网关。旧逐笔、快照、基座接口兼容。

详细口径与例子见 [DERIVED_TABLES.md](DERIVED_TABLES.md)。

---

# Market Data API 0.6.0

Market Data API 0.6.0 提供只读的五分钟逐笔数据访问，以及每日主动成交基座访问。

## 数据接口

- 逐笔数据集 `orders`、`trades`、`snapshots`：精确时间或按交易日 + 每日窗口读取，支持股票、字段过滤和 `direct`/`cache` 模式。
- 主动成交基座 `flow_points`：按日期区间读取每日已落盘的 `points.parquet`，日期首尾包含，默认全市场、全部 10 列，支持股票、字段和每日时间窗口筛选。
- Arrow `RecordBatch` / `Table` 返回，`iter_batches()` 支持逐批消费；`read_table()` 返回完整表。
- 多用户独立令牌，网关公平调度和并发队列；用户热更新不需要重启。

## 消费端

- `MarketDataClient.connect()` 原生客户端直接连接网关，复用 HTTP 连接池，不要求本机启动 FastAPI。
- 继续支持 `MarketDataClient()` + `mdapi-local` 本机 HTTP API 模式。
- 支持客户端 `estimate()` 覆盖检查、读取统计和版本信息。
- `flow_points` 提供覆盖信息：实际日期、缺日、partial 日期和每日质量摘要；已有有效事件照常返回。

## 服务端

- 单文件网关只依赖 Python 标准库，只读使用五分钟 Parquet、catalog v2 和基座文件。
- 安装器支持用户服务模式和系统服务模式，保留已有用户、令牌、监听地址、端口、并发配置和 `MDAPI_POINTS_ROOT`。
- 压缩 footer 元数据缓存位于行情目录和基座目录之外，可删除重建。
- 随包提供 `mdapi-user` 用户管理工具和健康检查脚本。

## 升级说明

- 客户端与服务端建议同时使用 0.6.0。
- 原有数据格式无需修改；已有 Parquet 和 catalog v2 直接只读使用。
- 服务器启用 `flow_points` 需要存在基座目录，并设置 `MDAPI_POINTS_ROOT` 后重启网关。
- 历史版本说明见 GitHub Releases。
