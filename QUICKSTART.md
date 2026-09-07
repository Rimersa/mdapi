# 0.5.0 安装与升级

现有行情 Parquet 和 catalog v2 无需修改。要获得按股票、字段减少传输量的能力，网关和客户端都应升级。

## 87 服务器

解压 `market-data-api-0.5.0-easy-install.tar.gz`，进入目录：

```bash
./install-server.sh /data/market_data_5m
```

87 当前采用 quant 的用户服务，延续该模式不加 sudo。系统服务部署才使用 sudo。安装器保留已有用户令牌和地址、端口、并发设置，更新程序并重启服务；不修改行情文件和 catalog。

新增的压缩元数据缓存位于：

- 用户服务：`~/.cache/market-data-api-server/footers.sqlite3`
- 系统服务：`/var/cache/market-data-api/footers.sqlite3`

用户服务需要管理员开启 `loginctl enable-linger quant` 才能保证无人登录时继续运行。

## Python / Notebook 用户

先激活自己使用的 Python / Conda 环境，在发行包目录执行：

```bash
./install-client.sh --native 10.10.10.87
```

按提示输入管理员分配的个人令牌。已有配置的升级，也可以只安装新版 wheel：

```bash
python -m pip install --upgrade 'wheels/market_data_api-0.5.0-py3-none-any.whl[client]'
```

开始读取：

```python
from market_data_api import MarketDataClient

client = MarketDataClient.connect()
query = {
    "dataset": "orders",
    "start_date": "2026-09-01",
    "end_date": "2026-09-04",
    "daily_start": "09:15",
    "daily_end": "09:25",
    "symbols": ["000001.SZ", "600000.SH"],
    "columns": ["symbol", "event_time", "price_i32", "qty_i32"],
}
for batch in client.iter_batches(query):
    print(batch.num_rows)
print(client.last_read_stats)
client.close()
```

不需要启动额外服务，默认不保存行情对象。结果较小时可用 `client.read_table(query)` 得到完整 Arrow 表。

## 继续使用本机 HTTP API

```bash
./install-client.sh 10.10.10.87
~/.local/bin/mdapi-local
```

Notebook 环境也需安装上述客户端 wheel，然后保持原用法：

```python
from market_data_api import MarketDataClient
client = MarketDataClient("http://127.0.0.1:18788")
for batch in client.iter_batches(query):
    print(batch.num_rows)
```

旧本机服务不支持股票过滤；升级后需要重新启动。SDK 会检查股票过滤确认信息，避免新参数被旧服务静默忽略。

## 可选的元数据预热

在已安装客户端的机器上运行，填充的是服务器辅助索引缓存，不是下载行情：

```bash
python tools/warm_metadata.py \
  --config ~/.config/market-data-api/client.json \
  --start-date 2026-09-01 --end-date 2026-09-04
```
