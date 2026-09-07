# 0.5.0 实测与验收

测试日期：2026-09-07。服务器使用 87 的现有机械盘数据，通过独立的 0.5 网关测试端口读取；客户端运行在本机，单客户端 Arrow 核心预算为 2。未清除系统页缓存，首次与重复读取分别记录。

所有数字来自真实 HTTP 读取。整文件对照采用同版原生客户端的 sequential 策略，包含本机股票过滤；它不是旧本机 FastAPI 的端到端耗时。

## 单股五分钟，全字段

数据日期 2026-08-27，时间 09:30–09:35，股票 000001.SZ。时间为总完成耗时，大小为十进制 MB。

| 数据 | 整桶大小 | 按需数据负载 | 按需首次 | 顺序首次 | 按需重复 | 顺序重复 |
|---|---:|---:|---:|---:|---:|---:|
| 委托 | 324.19 MB | 0.92 MB | 0.206s | 5.946s | 0.057s | 5.012s |
| 成交 | 227.95 MB | 1.27 MB | 0.129s | 3.637s | 0.041s | 3.063s |
| 快照 | 60.20 MB | 1.76 MB | 0.094s | 0.736s | 0.035s | 0.493s |

首次单股委托查询中，测试网关进程的实际磁盘 read_bytes 增量约 1.67 MB，随后顺序对照约 322.53 MB。降低的不仅是网络负载，也包括这组样本实际触发的磁盘读取。它不代表所有随机读取都会按相同比例提速。

元数据响应单独计量：三个单股查询首次分别约 0.191 MB、0.092 MB 及以原始 JSON 记录为准的快照元数据；重复请求命中本机元数据缓存为 0。数据负载不包含 HTTP/TCP 头和上行请求体。

## 10 / 100 股票与全市场

| 场景 | auto 重复耗时 | sequential 重复耗时 | auto 数据负载 |
|---|---:|---:|---:|
| orders_ten_stocks_four_columns | 0.085s | 2.411s | 3.55 MB |
| orders_hundred_stocks_four_columns | 0.712s | 2.527s | 60.22 MB |
| trades_ten_stocks_four_columns | 0.107s | 1.565s | 5.06 MB |
| trades_hundred_stocks_four_columns | 0.850s | 1.648s | 148.80 MB |
| snapshots_ten_stocks_four_columns | 0.029s | 0.179s | 0.23 MB |
| snapshots_hundred_stocks_four_columns | 0.080s | 0.166s | 1.23 MB |
| snapshots_market_auction_all_columns | 0.242s | 0.236s | 7.70 MB |
| snapshots_market_four_columns | 0.091s | 0.164s | 1.48 MB |

100 股票是从当日数据中分散采样的代码集合。成交的 100 股票查询触发了部分对象自动采用顺序读取，验证了成本回退路径。全市场全字段集合竞价仍走原来的整文件通道；auto 与 sequential 的小幅波动不构成显著性能差异。

三个数据集 × 1/10/100 股票共九组结果，均使用 Arrow 表逐值比较通过，不仅比较行数。

## 四用户并发

同一客户端机器上使用四个独立令牌、四个客户端线程，同时混合读取单股委托、五股成交和全市场竞价快照。每用户九次，共 36 次，全部成功；整体约 4.65 秒。

单请求中位数 0.292 秒，p95（线性插值）0.687 秒，最大 0.764 秒。所有用户都有完成记录，没有重试或超时。

这不是四台物理机器的独立压测；四客户端共享本机 CPU、内存和网卡，测试验证了服务器按个人令牌服务多用户的实际路径。

## 多日与内存

2026-08-20–08-26 的快照，取 000001.SZ 的 symbol/event_time/time_int。选中原文件共 8.80 GB，返回 23,806 行。每次实际数据负载 3.35 MB，首次新客户端还接收压缩元数据 17.63 MB。

服务端索引就绪的本次首次读取 3.32 秒；随后四次为 1.58–1.83 秒。连续五次完成后的 RSS 为 215.4–222.9 MB，最后稳定在约 223 MB；每次完成后 Arrow 在用分配为 0。测试没有手动调用全局 GC 或分配器释放。

未预热的冷历史区间仍要首次读取多个原始 footer 并访问相关数据块，可能明显更慢。服务端集中压缩索引和显式 warm_metadata 工具用于降低后续新客户端的首次查询成本，不把缓存命中数字作为任意冷读保证。

## 正确性、兼容与数据保护

- 本机 HTTP API 端到端取回目标股票 100 行，空股票返回 0 行且保留字段类型。
- Python 3.10.19 + PyArrow 24 的真实读取通过；主要自动测试环境为 Python 3.13。
- 48 项自动测试覆盖股票/字段/时间组合、空结果、未知字段、断线恢复、取消、四用户、同一缓存的并发填充、源版本缺失、持久元数据缓存与升级配置保留。
- 原文件检查覆盖最终对照测试涉及的 13 个 Parquet：大小、mtime 和 inode 前后一致；网关运行账号对这些文件均无写权限。测试构造数据另有完整 SHA-256 前后校验。
- direct 验收未产生本地行情 Parquet。源码只以只读方式打开行情对象；持久元数据索引被强制放在行情目录之外。

## 原始记录

- [benchmark_final_matrix.json](benchmarks/2026-09-07/benchmark_final_matrix.json)
- [benchmark_final_concurrency.json](benchmarks/2026-09-07/benchmark_final_concurrency.json)
- [benchmark_repeated_memory_bounded.json](benchmarks/2026-09-07/benchmark_repeated_memory_bounded.json)
- [http_acceptance.json](benchmarks/2026-09-07/http_acceptance.json)
- [python310_acceptance.json](benchmarks/2026-09-07/python310_acceptance.json)
- [source_immutability.json](benchmarks/2026-09-07/source_immutability.json)

复现工具：tools/bench_selective.py。临时账户凭据文件不进入 Git 或发行包。旧版五日测试报告保留在 [BENCHMARK_0_4.md](BENCHMARK_0_4.md)。
