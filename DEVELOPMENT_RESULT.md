# 0.6.0 开发与上线记录

本页记录 `flow_points` 功能的开发、测试和上线结果。正式版为 0.6.0，用户安装与请求方式见 [README.md](README.md) 和 [QUICKSTART.md](QUICKSTART.md)。

## 交付状态

- 开发分支：`feature/flow-points-v0.6`
- 功能提交：`a52b11a`，实现 `flow_points`、股票/字段/时间筛选与有界内存流式读取
- 发布提交：`2019245`，升级版本与文档到正式版 0.6.0
- 基线：`746f003`（v0.5.0）
- 正式上线：2026-09-21 部署到 `10.10.10.87:18787`，基座根目录 `/data/flow_points`
- 原 4 个用户令牌、地址和并发配置保持不变；原 `orders` / `trades` / `snapshots` 接口兼容

## 验证结果

- 完整回归 63 项通过；Python 3.10 相关测试 14 项通过；候选 wheel 与单文件网关联合读取真实数据通过。
- 四天两只股票 328,897 行基座数据与直接 Parquet 读取逐列一致。
- 原三类逐笔接口样本与 0.5.0 结果一致。
- 735 个基座日期可识别；默认全市场读取首批 131,072 行通过。
- 源数据文件 inode、大小和修改时间保持不变。
- 性能测试见 [FLOW_POINTS_BENCHMARK.md](FLOW_POINTS_BENCHMARK.md)，接口说明见 [FLOW_POINTS.md](FLOW_POINTS.md)。

## 回滚与留档

- 上线前备份：`/home/quant/.local/share/market-data-api-server/backups/before-0.6.0-20260921-160855/`
- 87 上的版本包及上线记录：`/data/received/releases/mdapi-0.6.0-1ee5cc5d2823/`

## 本机手动试用（可选）

0.6.0 发布包在本机验证时，先用单文件网关加载样本目录：

```bash
cd /home/quant/market_data_api_flow_points
/usr/bin/python3 dist/market-data-api-0.6.0/bin/mdapi-gateway.pyz \
  --root ./dist/points-benchmark-20260921/ticks \
  --points-root ./dist/points-benchmark-20260921/points \
  --host 127.0.0.1 --port 18987 --token dev-points
```

另开终端用同一分支源码读取：

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect(gateway_host="127.0.0.1", gateway_port=18987,
                              gateway_token="dev-points", cores=2) as api:
    rows = 0
    for batch in api.iter_points("2026-09-02", "2026-09-02", symbols=["000001.SZ"]):
        rows += batch.num_rows
    print(rows)  # 样本中为 49678
```

样本目录仅含 2024-02-02 和 2026-09-02 两个真实日期；测试结束后按 Ctrl+C 关闭临时网关。
