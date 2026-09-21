# Market Data API

Market Data API 是一个只读的 A 股五分钟行情访问服务。服务端运行在数据服务器上，客户端按 HTTP 请求读取逐笔委托、逐笔成交、快照，以及每日主动成交基座数据，结果以 Arrow `RecordBatch` 或 `Table` 返回。

当前版本：**0.6.0**  
项目地址：<https://github.com/Rimersa/mdapi>

## 数据接口

| 数据集 | 内容 | 时间规则 | 读取模式 |
|---|---|---|---|
| `orders` | 逐笔委托 | `start/end` 半开区间；按日期查询需同时提供每日窗口 | `direct` / `cache` |
| `trades` | 逐笔成交 | 同上 | `direct` / `cache` |
| `snapshots` | 快照 | 同上 | `direct` / `cache` |
| `flow_points` | 每日主动成交基座（已识别、分档的事件原表） | `start_date/end_date` 包含首尾日期，默认全天 | `direct` |

服务端只读使用现有五分钟 Parquet、catalog v2 和每日基座文件，不会重切、清洗或改写数据。

## 系统组成

```text
计算/数据生产端                   数据服务器（网关）                   客户端
flow-base 生成每日基座   ──►   /data/flow_points          ──►   Python SDK（MarketDataClient.connect）
                               /data/market_data_5m
                               mdapi-gateway :18787              或 本机 HTTP 代理 :18788
```

- 网关：单文件 Python 程序，只依赖 Python 标准库，按用户令牌做公平并发调度。
- 客户端：Python SDK 原生连接，或启动 `mdapi-local` 使用本机 HTTP API。
- 用户管理：`mdapi-user` 增删改令牌，网关热加载，无需重启。

## 环境要求

服务端：

- Linux x86_64，`systemd`，`/usr/bin/python3` 为 Python 3.10 或更新版本；
- 行情数据根目录必须已经存在，且包含 `catalog.json` 和符合 [SERVER_DATA_FORMAT.md](SERVER_DATA_FORMAT.md) 的五分钟 Parquet；
- 如启用主动成交基座，基座根目录必须已经存在，例如 `/data/flow_points`；
- 默认监听 `10.10.10.87:18787`，可在配置中修改；
- 运行账号需要对数据目录有读取权限。

客户端：

- Python 3.10 或更新版本，以及目标环境中的 `pip`；
- Conda / venv / Notebook 环境需要单独安装客户端 wheel；
- 能通过 HTTP 访问网关地址和端口。

## 从零安装：服务端

### 1. 准备数据目录

行情数据根目录示例：`/data/market_data_5m`。目录中必须至少包含：

```text
/data/market_data_5m/catalog.json
/data/market_data_5m/...（五分钟 Parquet 和版本索引）
```

主动成交基座根目录示例：`/data/flow_points`。网关会自动发现两层结构：

```text
/data/flow_points/machine=*/trade_date=*/points.parquet
```

也支持根目录下直接放 `trade_date=*`。不要把 `flow_points_all` 之类的聚合软链接视图作为基座根目录；基座根目录内的软链接也不能指向根目录之外。

### 2. 下载并校验发布包

```bash
VERSION=0.6.0
curl -LO "https://github.com/Rimersa/mdapi/releases/download/v${VERSION}/market-data-api-${VERSION}-easy-install.tar.gz"
curl -LO "https://github.com/Rimersa/mdapi/releases/download/v${VERSION}/market-data-api-${VERSION}-easy-install.tar.gz.sha256"
sha256sum -c "market-data-api-${VERSION}-easy-install.tar.gz.sha256"
tar -xzf "market-data-api-${VERSION}-easy-install.tar.gz"
cd "market-data-api-${VERSION}"
```

发布包包含单文件网关、客户端 wheel、安装脚本、运维脚本和完整文档。

### 3. 选择安装模式

| | 用户服务模式（不加 `sudo`） | 系统服务模式（加 `sudo`） |
|---|---|---|
| 运行账号 | 当前登录账号 | 模板固定为 `quant`，需先存在 |
| 适用场景 | 当前已有账号、不希望动 root 配置 | 标准服务器常驻服务 |
| 安装命令 | `./install-server.sh /data/market_data_5m` | `sudo ./install-server.sh /data/market_data_5m` |
| 程序路径 | `~/.local/share/market-data-api-server/` | `/opt/market-data-api/` |
| 配置路径 | `~/.config/market-data-api-server/` | `/etc/market-data-api/` |
| 缓存路径 | `~/.cache/market-data-api-server/` | `/var/cache/market-data-api/` |
| 用户管理命令 | `~/.local/bin/mdapi-user` | `/usr/local/sbin/mdapi-user` |

用户服务模式在无人登录或服务器重启后要常驻，需要管理员执行一次：

```bash
sudo loginctl enable-linger "$USER"
```

系统服务模式使用 `/usr/bin/python3` 运行，且 systemd 模板中的运行账号是 `quant`。如果服务器还没有该账号：

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin quant
```

### 4. 执行安装

```bash
# 用户服务
./install-server.sh /data/market_data_5m

# 或系统服务
sudo ./install-server.sh /data/market_data_5m
```

安装过程中会创建空用户库，启动并启用 systemd 服务，然后执行健康检查。也可以直接创建初始用户，用户名追加在数据根目录之后：

```bash
./install-server.sh /data/market_data_5m alice
sudo ./install-server.sh /data/market_data_5m alice bob
```

安装器会输出新用户的令牌。没有用户时健康接口可用，但所有数据接口保持锁定。

### 5. 修改监听地址（服务器不是 10.10.10.87 时）

安装器默认写入 `MDAPI_GATEWAY_HOST=10.10.10.87`、`MDAPI_GATEWAY_PORT=18787`。如果目标服务器 IP 不同，修改配置后重启：

```bash
# 用户服务
sed -i 's/^MDAPI_GATEWAY_HOST=.*/MDAPI_GATEWAY_HOST=0.0.0.0/' \
  ~/.config/market-data-api-server/gateway.env
systemctl --user restart market-data-gateway

# 系统服务
sudo sed -i 's/^MDAPI_GATEWAY_HOST=.*/MDAPI_GATEWAY_HOST=0.0.0.0/' \
  /etc/market-data-api/gateway.env
sudo systemctl restart market-data-gateway
```

`0.0.0.0` 表示监听所有网卡；也可以写服务器自己的固定 IP。

### 6. 启用主动成交基座

基座文件放到服务器后，修改网关配置中的 `MDAPI_POINTS_ROOT` 并重启：

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

安装器生成的配置里默认包含 `MDAPI_POINTS_ROOT=` 行。如果该行为空或指向其他目录，用上面的命令改成真实基座根目录。安装器升级时会保留该配置，但要求元数据缓存目录不能位于基座根目录内。

### 7. 创建用户与令牌

```bash
# 用户服务
~/.local/bin/mdapi-user add alice
~/.local/bin/mdapi-user list
~/.local/bin/mdapi-user rotate alice
~/.local/bin/mdapi-user remove alice

# 系统服务
sudo /usr/local/sbin/mdapi-user add alice
sudo /usr/local/sbin/mdapi-user list
sudo /usr/local/sbin/mdapi-user rotate alice
sudo /usr/local/sbin/mdapi-user remove alice
```

添加或轮换命令会显示令牌，请安全地交给用户。令牌文件原子更新，网关热加载，不需要重启。建议一人一令牌。

### 8. 健康检查

```bash
curl http://10.10.10.87:18787/health
```

正常响应示例：

```json
{
  "status": "ok",
  "version": "0.6.0",
  "configured_users": 1,
  "flow_points_enabled": true,
  "max_streams": 2,
  "capabilities": ["parquet_footers_v1", "range_bundles_v1", "http_range_v1", "daily_points_v1"]
}
```

`flow_points_enabled=true` 与 `daily_points_v1` 表示基座接口已启用。

## 从零安装：客户端

客户端和服务端使用同一个发布包，不需要在客户端机器上启动网关。

### 方式 A：原生客户端（推荐）

在目标 Python / Conda 环境中执行：

```bash
cd market-data-api-0.6.0
./install-client.sh --native 10.10.10.87
```

按提示输入管理员分配的个人令牌。脚本会把 `[client]` wheel 安装到当前 Python 环境，并写入：

```text
~/.config/market-data-api/client.json
```

如果目标环境不是命令默认的 `python3`：

```bash
MDAPI_PYTHON=/path/to/conda/env/bin/python ./install-client.sh --native 10.10.10.87
```

也可以手工只装 wheel：

```bash
python -m pip install --upgrade 'wheels/market_data_api-0.6.0-py3-none-any.whl[client]'
```

然后使用 `scripts/write_client_config.py` 生成配置文件，或直接使用安装脚本。

### 方式 B：本机 HTTP 代理

```bash
./install-client.sh 10.10.10.87
~/.local/bin/mdapi-local
```

终端出现 `Uvicorn running on http://127.0.0.1:18788` 后保持运行，Notebook 中使用：

```python
from market_data_api import MarketDataClient
client = MarketDataClient("http://127.0.0.1:18788")
```

读取基座时，本机代理也必须升级到 0.6.0。

### 验证连接

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect(cores=2) as client:
    print(client.estimate({"dataset": "snapshots",
                           "start": "2026-09-04T09:30:00+08:00",
                           "end": "2026-09-04T09:31:00+08:00"}))
```

## 使用

### 连接

原生客户端读取 `~/.config/market-data-api/client.json`：

```python
from market_data_api import MarketDataClient

with MarketDataClient.connect(cores=2) as client:
    ...
```

也可以显式传入参数：

```python
client = MarketDataClient.connect(
    gateway_host="10.10.10.87",
    gateway_port=18787,
    gateway_token="个人令牌",
    cores=2,
    io_profile="hdd",    # 远端是 SSD 时可用 "ssd"
)
```

### 读取逐笔数据

精确时间区间（`[start, end)`，半开）：

```python
query = {
    "dataset": "snapshots",
    "start": "2026-09-04T09:30:00+08:00",
    "end": "2026-09-04T09:35:00+08:00",
    "symbols": ["000001.SZ", "600000.SH"],
    "columns": ["symbol", "event_time", "last_px_i32", "volume_i64"],
}

for batch in client.iter_batches(query):
    print(batch.num_rows)
```

按交易日和每日窗口查询：

```python
query = {
    "dataset": "trades",
    "start_date": "2026-09-01",
    "end_date": "2026-09-04",   # 包含 09-04
    "daily_start": "09:30",
    "daily_end": "10:00",       # 不包含 10:00
    "symbols": ["000001.SZ"],
    "columns": ["symbol", "event_time", "price_i32", "qty_i32"],
}
```

### 读取主动成交基座

`flow_points` 使用 `start_date` / `end_date`，包含首尾日期；默认全市场、全部 10 列：

```python
with MarketDataClient.connect(cores=2) as api:
    for batch in api.iter_points(
        "2026-09-01", "2026-09-04",
        symbols=["000001.SZ"],       # 省略即全市场
        columns=["symbol", "time", "active_order_id", "side", "depth",
                 "mode_mask", "amount", "volume", "order_amount", "order_volume"],
    ):
        print(batch.num_rows)

    print(api.last_read_stats["coverage"])
```

也可以指定每日时间窗口：

```python
query = {
    "dataset": "flow_points",
    "start_date": "2026-09-01",
    "end_date": "2026-09-05",
    "daily_start": "09:30:00",
    "daily_end": "11:30:00",
}
```

基座返回字段：

| 字段 | 类型 | 含义 |
|---|---|---|
| `symbol` | string | 股票代码 |
| `time` | timestamp[ns, Asia/Shanghai] | 事件时间 |
| `active_order_id` | int64 | 主动方订单号，与股票、时间、方向合用识别事件 |
| `side` | string | `B` 主动买，`S` 主动卖 |
| `depth` | uint8 | 1–5 档，5 表示五档及以上 |
| `mode_mask` | uint8 | 1 为 order，2 为 price，3 为两者共享 |
| `amount` | float64 | 本行金额，元 |
| `volume` | int64 | 本行股数 |
| `order_amount` | float64 | 本事件观察到的整单金额，元 |
| `order_volume` | int64 | 本事件观察到的整单股数 |

`order` 口径取 `mode_mask in (1,3)`，`price` 口径取 `mode_mask in (2,3)`。两种口径记录同一批成交，不能把全表 `amount`/`volume` 直接相加；`order_amount`/`order_volume` 是整单总量，会在多个价位切片行中重复，不能逐行累加。完整契约见 [FLOW_POINTS.md](FLOW_POINTS.md)。

### 覆盖信息与缺失日期

`estimate(query)` 和 `last_read_stats["coverage"]` 返回：

- `days`：实际读取日期、行数、文件大小、版本、`generation_status`；
- `dates_without_files`：范围内没有已发布基座文件的日期，可能含周末或节假日，不表示零成交；
- `partial_dates`：生产状态为 `partial` 的日期，已有有效事件仍会返回。

质量摘要是全日、全市场的，不会因股票过滤重算。做总量统计时不要把缺日当零，也不要把 partial 日当成完整数据。

### 过滤、批次与内存

- `symbols` 精确匹配股票代码，省略或 `None` 表示全部，空数组报错；
- `columns` 决定最终返回字段和顺序，内部过滤列不会额外返回；
- `iter_batches(query)` 逐批返回 `pyarrow.RecordBatch`，适合大范围查询；
- `read_table(query)` 会把全部结果放进一张 `pyarrow.Table`，只用于小结果；
- 批次边界与日期、订单边界无关。

### direct 与 cache

逐笔数据集支持：

- `mode="direct"`（默认）：按需下载数据块，不在本机保存行情 Parquet；
- `mode="cache"`：在本机维护完整五分钟对象缓存，适合经常重复读取的固定区间；
- `update`：`missing_only`、`if_changed`、`force`。

`flow_points` 只支持 `direct`。使用 `cache` 时，第一天仍会完整下载所选桶。

## 服务器运维

### 服务管理

```bash
# 用户服务
systemctl --user status market-data-gateway
systemctl --user restart market-data-gateway
journalctl --user -u market-data-gateway -n 100

# 系统服务
sudo systemctl status market-data-gateway
sudo systemctl restart market-data-gateway
sudo journalctl -u market-data-gateway -n 100
```

用户服务需要 `loginctl enable-linger` 才能无人登录时保持运行。

### 用户与认证

- `mdapi-user add/list/rotate/remove`，命令位置见安装模式表；
- 用户令牌保存在配置目录的 `users.json`；
- 网关热加载令牌；零用户时数据接口锁定；
- `/health` 不要求令牌。

### 元数据缓存

压缩 footer 元数据缓存默认位于：

- 用户服务：`~/.cache/market-data-api-server/footers.sqlite3`
- 系统服务：`/var/cache/market-data-api/footers.sqlite3`

缓存只加快元数据读取，不是数据源，可以删除后重建。缓存路径必须位于行情根目录和基座根目录之外。

### 升级

- 服务端：下载新发布包，保持原来的用户/系统模式，重新执行 `install-server.sh /数据根目录`。安装器保留用户令牌、监听地址、端口、并发配置和基座根目录；
- 客户端：在目标 Python 环境重新执行 `install-client.sh --native 网关地址`，或直接升级 wheel；
- 数据格式不变，不需要迁移行情 Parquet 或 catalog v2。

回滚时替换上一版 `mdapi-gateway.pyz` 和配置并重启即可；`dist/` 里保留上一版发布包。

## 目录、端口与配置

| 项目 | 用户服务 | 系统服务 |
|---|---|---|
| 网关程序 | `~/.local/share/market-data-api-server/mdapi-gateway.pyz` | `/opt/market-data-api/mdapi-gateway.pyz` |
| 配置 | `~/.config/market-data-api-server/gateway.env` | `/etc/market-data-api/gateway.env` |
| 用户令牌 | 配置目录的 `users.json` | 配置目录的 `users.json` |
| 元数据缓存 | `~/.cache/market-data-api-server/footers.sqlite3` | `/var/cache/market-data-api/footers.sqlite3` |
| 用户管理 | `~/.local/bin/mdapi-user` | `/usr/local/sbin/mdapi-user` |
| 服务名 | `market-data-gateway`（user） | `market-data-gateway` |
| 网关默认端口 | `18787` | `18787` |
| 本机代理端口 | `127.0.0.1:18788` | 不适用 |

## 故障排查

- **健康接口无法访问**：确认服务状态、`MDAPI_GATEWAY_HOST/PORT`、防火墙；非 87 服务器默认必须改监听地址。
- **返回 401 / unauthorized**：检查客户端配置中的令牌，或让管理员重新执行 `mdapi-user rotate`。
- **基座接口不存在**：确认 `MDAPI_POINTS_ROOT` 已在配置中，指向真实基座根目录，并重启网关；检查 `/health` 的 `flow_points_enabled`。
- **`ModuleNotFoundError: market_data_api`**：客户端安装到了别的 Python 环境，请在目标 Notebook/Conda 环境重新安装 wheel，或用 `MDAPI_PYTHON` 指定解释器。
- **本机代理读不到基座**：`mdapi-local` 及其安装包必须升级到 0.6.0 并重新启动。
- **内存占用高**：改用 `iter_batches`，加 `symbols`/`columns`/时间窗口限制；不要对大区间使用 `read_table`。

## 文档索引

- [安装与快速开始](QUICKSTART.md)
- [用户指南](USER_GUIDE.md)
- [主动成交基座接口](FLOW_POINTS.md)
- [部署与运维](DEPLOYMENT.md)
- [数据文件格式](SERVER_DATA_FORMAT.md)
- [字节读取协议](RANGE_PROTOCOL.md)
- [数据契约](DATA_CONTRACT.md)
- [版本记录](RELEASE_NOTES.md)
- [基座性能实测](FLOW_POINTS_BENCHMARK.md)
- [0.5.0 性能与验收（历史）](BENCHMARK.md)

## 开发与测试

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python tools/build_release.py
```

`tools/build_release.py` 生成 `dist/market-data-api-<版本>/` 和 `dist/market-data-api-<版本>-easy-install.tar.gz`，包含 wheel、单文件网关、安装脚本、运维脚本、文档和 SHA256SUMS。
