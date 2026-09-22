# 安装与快速开始

完整说明见 [README.md](README.md)。本文给出从零部署和使用的短路径。

## 服务端

1. 准备数据根目录，例如 `/data/market_data_5m`，其中必须包含 `catalog.json` 和五分钟 Parquet。主动成交基座目录例如 `/data/flow_points`。

2. 下载 0.7.0 发布包：

```bash
VERSION=0.7.0
curl -LO "https://github.com/Rimersa/mdapi/releases/download/v${VERSION}/market-data-api-${VERSION}-easy-install.tar.gz"
curl -LO "https://github.com/Rimersa/mdapi/releases/download/v${VERSION}/market-data-api-${VERSION}-easy-install.tar.gz.sha256"
sha256sum -c "market-data-api-${VERSION}-easy-install.tar.gz.sha256"
tar -xzf "market-data-api-${VERSION}-easy-install.tar.gz"
cd "market-data-api-${VERSION}"
```

3. 安装网关。无 sudo 安装为当前账号的用户服务；有 sudo 安装为系统服务：

```bash
./install-server.sh /data/market_data_5m
# 或
sudo ./install-server.sh /data/market_data_5m
```

安装器会启动服务并执行健康检查。系统服务模板使用 `quant` 账号，若服务器没有该账号需先创建；用户服务需要 `loginctl enable-linger` 才能无人登录时保持运行。

4. 启用主动成交基座：

```bash
# 用户服务
sed -i 's|^MDAPI_POINTS_ROOT=.*|MDAPI_POINTS_ROOT=/data/flow_points|' \
  ~/.config/market-data-api-server/gateway.env
systemctl --user restart market-data-gateway

# 系统服务
sudo sed -i 's|^MDAPI_POINTS_ROOT=.*|MDAPI_POINTS_ROOT=/data/flow_points|' \
  /etc/market-data-api/gateway.env
sudo systemctl restart market-data-gateway
```

5. 创建用户并保存输出的令牌：

```bash
# 用户服务
~/.local/bin/mdapi-user add alice

# 系统服务
sudo /usr/local/sbin/mdapi-user add alice
```

6. 检查健康接口：

```bash
curl http://10.10.10.87:18787/health
```

确认 `"version":"0.7.0"`、`"flow_points_enabled":true`，能力列表包含 `"daily_points_v1"`。

## 客户端

在目标 Python / Conda 环境中：

```bash
./install-client.sh --native 10.10.10.87
```

按提示输入自己的令牌。脚本会安装客户端 wheel 并写入 `~/.config/market-data-api/client.json`。如果目标解释器不是默认的 `python3`，使用：

```bash
MDAPI_PYTHON=/path/to/conda/env/bin/python ./install-client.sh --native 10.10.10.87
```

也可以继续使用本机 HTTP 代理模式：

```bash
./install-client.sh 10.10.10.87
~/.local/bin/mdapi-local
```

## 第一个请求

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect(cores=2) as client:
    for batch in client.iter_points("2026-09-01", "2026-09-04", symbols=["000001.SZ"]):
        print(batch.num_rows)
    print(client.last_read_stats["coverage"])
```

逐笔数据示例：

```python
query = {
    "dataset": "snapshots",
    "start": "2026-09-04T09:30:00+08:00",
    "end": "2026-09-04T09:35:00+08:00",
    "symbols": ["000001.SZ"],
    "columns": ["symbol", "event_time", "last_px_i32", "volume_i64"],
}
for batch in client.iter_batches(query):
    print(batch.num_rows)
```

大范围查询请使用 `iter_batches()` 逐批消费，不要用 `read_table()` 一次性把全部结果放进内存。基座口径、字段和 coverage 说明见 [FLOW_POINTS.md](FLOW_POINTS.md) 与 [README.md](README.md)。
