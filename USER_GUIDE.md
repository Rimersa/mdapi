# Market Data API 用户使用手册

本文面向取数用户。用户只连接自己机器上的本地API，不使用SSH，也不需要知道87上的
文件路径。

## 1. 使用前需要什么

- Linux机器和Python 3.10或更高版本；
- 能访问`10.10.10.87:18787`；
- 管理员分配的个人令牌；
- `market-data-api-0.4.1`发布包。

每个用户必须使用自己的令牌。服务器零用户时处于锁定状态；管理员创建用户并交付令牌
后才能取数。

## 2. 一次性安装

```bash
tar -xzf market-data-api-0.4.1-easy-install.tar.gz
cd market-data-api-0.4.1
./install-client.sh 10.10.10.87
```

安装程序会隐藏令牌输入，并自动完成：

- 创建隔离Python环境；
- 安装本机FastAPI、PyArrow及资源检测依赖；
- 把令牌写入权限为`0600`的JSON配置；
- 生成`~/.local/bin/mdapi-local`。

它不会安装系统服务，也不会开机启动。

## 3. 每次使用时启动和停止

打开一个终端运行：

```bash
~/.local/bin/mdapi-local
```

看到以下信息即启动成功：

```text
Uvicorn running on http://127.0.0.1:18788
```

保持这个终端运行，研究程序可以循环请求任意日期。循环期间会复用HTTP连接，不会建立
SSH，也不会在87上反复启动进程。

工作完成后在该终端按`Ctrl+C`，本机API即退出并释放资源。

健康检查：

```bash
curl http://127.0.0.1:18788/health
```

## 4. Python SDK

如果研究代码运行在另一个Python或Conda环境，在该环境安装轻量客户端：

```bash
python -m pip install \
  '/发布包目录/wheels/market_data_api-0.4.1-py3-none-any.whl[client]'
```

推荐逐批消费：

```python
from market_data_api import MarketDataClient

client = MarketDataClient()
query = {
    "dataset": "snapshots",
    "start": "2026-05-29T09:15:00+08:00",
    "end": "2026-05-29T09:25:00+08:00",
    "mode": "direct",
}

print(client.estimate(query))

for batch in client.iter_batches(query):
    # batch是PyArrow RecordBatch；用户自行处理、保留或写入自己的存储
    process(batch)
```

如果确定结果可以装入研究进程内存，也可以一次得到PyArrow Table：

```python
table = client.read_table(query)
```

`read_table()`占用的是用户研究进程内存，不属于本地API的内存保护范围。大结果优先使用
`iter_batches()`。

## 5. 请求方式

### 单个连续区间

```python
query = {
    "dataset": "trades",
    "start": "2026-05-29T09:15:00+08:00",
    "end": "2026-05-29T09:25:00+08:00",
    "mode": "direct",
}
```

时间范围采用半开区间`[start, end)`：包含09:15:00，不包含09:25:00。

### 多日每天相同时间窗口

```python
query = {
    "dataset": "orders",
    "start_date": "2026-05-25",
    "end_date": "2026-05-29",
    "daily_start": "09:15:00",
    "daily_end": "09:25:00",
    "mode": "direct",
}
```

`end_date`包含在内。这个请求只拉五天各自09:15–09:25的数据，不会把日期之间的全天
数据拉回本机。

### 只取部分列

```python
query["columns"] = ["symbol", "event_time", "last_px_i32"]
```

列不存在时返回HTTP 422，不会悄悄忽略。

## 6. direct和cache

| 模式 | 87网络读取 | 用户机器持久化 | 适用场景 |
|---|---:|---:|---|
| `direct` | 每次读取 | 否 | 一次性研究、磁盘紧张 |
| `cache` | 只补缺失或需更新的桶 | 是 | 反复读取相同日期和时段 |

缓存位于`~/.cache/market-data-api`，由SQLite元数据和版本化Parquet对象组成。两种模式
返回完全相同的Arrow Stream格式。

cache更新策略：

- `missing_only`：默认；已有桶不更新，只补缺失桶；
- `if_changed`：87版本发生变化时更新；
- `force`：强制重新拉取所选桶的当前版本。

```python
query["mode"] = "cache"
query["update"] = "if_changed"
```

## 7. 先估算再读取

```python
estimate = client.estimate(query)
print(estimate["source_bytes"])
print(estimate["estimated_arrow_memory"])
print(estimate["estimated_working_memory"])
print(estimate["missing_cache_bytes"])
```

主要字段：

- `source_bytes`：预计从87传输的压缩Parquet字节数；
- `estimated_arrow_memory`：完整结果物化为Arrow时的参考估算；
- `estimated_working_memory`：本地API有界流水线预计工作内存；
- `working_memory_limit`：本次请求动态计算的API工作内存额度；
- `missing_cache_bytes`：cache模式需要补到本机的字节数。

默认不限制总结果大小，但本地API会保护自身流水线：开始前根据当前可用内存预检，运行
期间持续检查紧急余量。

## 8. 并发和循环

- 本机自动检测CPU affinity和可用内存，最多使用8个核心；
- 需要严格单核时运行`mdapi-local --cores 1`；
- 87保持两个顺序数据流，四位用户竞争时按用户轮转；
- 没有其他用户等待时，同一用户可借用第二条流；
- 一个很长的API请求会自动拆成最多12个五分钟对象的传输段；每段结束即重新参与
  按用户轮转，因此年度请求也不会长期霸占一条流。

建议保持一个本机API进程，再在循环中不断调用同一个`MarketDataClient`。

## 9. 网络中断和自动续传

客户端与87之间是长期复用的HTTP连接，没有SSH过程。正常循环不会为每一天重新握手；
只有连接失效时才创建新的TCP连接。

默认对每个未完成对象自动重试3次，退避时间依次约为0.25、0.5、1秒：

- `direct`先把一个完整Parquet对象接收到有界内存，再解码并输出。中途断线时，已经完整
  输出的对象不会重发，只从当前未完成对象继续，因此成功完成的结果没有重复行；
- `cache`按完整五分钟桶校验并立即提交。中途断线时保留已完成桶、删除不完整临时文件，
  只补未完成桶；即使重试耗尽，下一次相同请求也会从缺失桶继续；
- 续传粒度是五分钟对象，不是文件内部的字节偏移。当前对象会从头重传，这换取了简单
  的完整性校验和确定的无重复语义。对象只有五分钟，最坏重传量有界。

如果3次重试仍失败，cache在开始返回Arrow前会得到HTTP 502；direct可能已经向调用者
交付了前面的完整批次，随后SDK抛出`MarketDataAPIError`。需要“全有或全无”的调用方
必须只在迭代正常结束后提交自己的结果，不能把异常前收到的批次当成完整结果。

可在本机配置中调整：

```json
{
  "network_retries": 3,
  "network_retry_backoff": 0.25,
  "object_request_size": 12
}
```

`object_request_size`越小，多用户轮转越及时；默认12约对应单文件布局的一小时数据，持久
连接下额外HTTP开销很小。一般无需修改。

## 10. 本机配置

配置文件：

```text
~/.config/market-data-api/client.json
```

```json
{
  "gateway_host": "10.10.10.87",
  "gateway_port": 18787,
  "gateway_token": "个人令牌",
  "cache_root": "/home/USER/.cache/market-data-api",
  "local_host": "127.0.0.1",
  "local_port": 18788,
  "arrow_compression": "zstd",
  "network_retries": 3,
  "network_retry_backoff": 0.25,
  "object_request_size": 12
}
```

更换令牌最简单的方法是重新运行`install-client.sh`。也可以手工编辑，但必须保持权限为
`0600`。

临时改变端口或核心数：

```bash
~/.local/bin/mdapi-local --port 18888 --cores 1
```

## 11. HTTP接口

- `GET /health`：本机状态；
- `POST /v1/estimate`：估算，不取数据；
- `POST /v1/data`：返回`application/vnd.apache.arrow.stream`。

`/v1/data`返回的是二进制Arrow IPC Stream，不是JSON。非Python程序只要能读取Arrow
Stream即可使用相同接口。

## 12. 常见错误

| HTTP状态 | 含义 | 处理方式 |
|---:|---|---|
| 401或502 `gateway_rejected` | 令牌错误、尚未创建用户或远端拒绝 | 联系管理员核对个人令牌 |
| 502 `network_transfer_failed` | 自动重试后网络传输仍失败 | 检查网络；cache可直接重试并只补缺失桶 |
| 404 | 所选时间没有已发布数据 | 核对日期、数据集和时间段 |
| 413 | 工作内存预检不通过或显式结果上限 | 缩短区间、减少列或释放本机内存 |
| 422 | 参数、列名或时间格式错误 | 根据返回detail修正请求 |
| 503 | 运行期间本机可用内存低于安全余量 | 释放内存后重试 |

如果`mdapi-local`无法启动，先检查18788端口是否被占用；可临时使用
`mdapi-local --port 18888`。如果本机健康但取数失败，再检查到
`10.10.10.87:18787`的网络连通性。
