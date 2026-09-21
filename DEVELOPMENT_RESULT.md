# 0.6.0rc1 开发交付

本页保留候选版开发阶段记录。当前正式版为 0.6.0，用户安装与请求请看 [README.md](README.md) 和 [QUICKSTART.md](QUICKSTART.md)。

开发分支：`feature/flow-points-v0.6`。本次未部署或重启 87 正式服务，主分支保持原版本。

- 功能和请求：[FLOW_POINTS.md](FLOW_POINTS.md)
- 真实数据实测：[FLOW_POINTS_BENCHMARK.md](FLOW_POINTS_BENCHMARK.md)
- 完整回归 63 项通过；Python 3.10 的新增相关测试 14 项通过。候选 wheel 与候选单文件网关联合读取真实数据通过。
- 候选包：`dist/market-data-api-0.6.0rc1-easy-install.tar.gz`；本轮不要执行其中会重启服务的安装脚本。
- 包与验收摘要：`benchmarks/points_validation_20260921.json`。

## 在 85 本机手动试用

样本目录已准备好，只含 2024-02-02 和 2026-09-02 两个真实日期；其中间其它日期显示未发布，不是全量历史库。

第一个终端启动候选测试网关（仅本机）：

```bash
cd /home/quant/market_data_api_flow_points
/usr/bin/python3 dist/market-data-api-0.6.0rc1/bin/mdapi-gateway.pyz \
  --root ./dist/points-benchmark-20260921/ticks \
  --points-root ./dist/points-benchmark-20260921/points \
  --host 127.0.0.1 --port 18987 --token dev-points
```

第二个终端使用候选源码和已有开发环境：

```bash
cd /home/quant/market_data_api_flow_points
PYTHONPATH=src /home/quant/market_data_api_v0_5/.venv/bin/python - <<'PYCODE'
from market_data_api import MarketDataClient
with MarketDataClient.connect(gateway_host="127.0.0.1", gateway_port=18987,
                              gateway_token="dev-points", cores=2) as api:
    rows = 0
    for batch in api.iter_points("2026-09-02", "2026-09-02", symbols=["000001.SZ"]):
        rows += batch.num_rows
    print(rows)  # 本次样本为 49678
    print(api.last_read_stats)
PYCODE
```

省略 `symbols` 即为全市场；传 `columns` 可选择字段。日期首尾包含。结束测试后在第一个终端按 Ctrl+C 关闭临时网关。自动验收启动的所有临时网关已关闭。
