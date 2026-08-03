# Market Data API

当前发布版本：`0.4.0`。默认使用一键安装，见
[`QUICKSTART.md`](QUICKSTART.md)；手工部署细节见
[`DEPLOYMENT.md`](DEPLOYMENT.md)。

本版本变更见[`RELEASE_NOTES.md`](RELEASE_NOTES.md)。
带`v*`标签的提交会在GitHub Actions中重新通过测试、构建并发布校验过的安装包附件。

普通用户完整手册见[`USER_GUIDE.md`](USER_GUIDE.md)，服务器输入文件格式见
[`SERVER_DATA_FORMAT.md`](SERVER_DATA_FORMAT.md)。

这是一个独立于 acelab 的市场数据服务。它假设 87 上已经存在按 5 分钟切好的、
只读 Parquet 派生数据，并提供：

- 87 常驻只读网关：只选择文件并用 `sendfile` 零拷贝发送，不运行查询；
- 本机 FastAPI：本机资源预检、精确时间过滤以及统一 Arrow Stream；
- `direct`：边拉边用，不持久化；
- `cache`：本地 SQLite 元数据库 + 版本化 Parquet 对象，按 5 分钟桶增量补齐；
- 对象级自动重连续传：默认3次，已完成对象不重拉，cache保留已提交桶；
- 固定根索引 + 数据集/交易日/版本分片catalog，历史增长不拖慢启动；
- 可选离线构建器：只读原始湖，新日期或源指纹变化时发布新版本。

生产网关本身不切数据。现有每日程序只需按
[`SERVER_DATA_FORMAT.md`](SERVER_DATA_FORMAT.md) 产出五分钟成品目录和 catalog；构建器只是
初始化、测试或尚无上游切片程序时的参考实现。

原始数据目录固定为：

```text
/data/market_data_lake/lake/curated
```

构建器拒绝把任何输出或暂存目录放入原始数据目录，也不包含修改、覆盖、移动或删除
原始文件的代码路径。

## 数据流

```text
87 原始湖（只读）
        │
        ▼
5 分钟增量构建器 ──> 独立派生目录/分片catalog v2
                              │
                              ▼
                    87 常驻零拷贝网关
                 （两个按用户公平调度的数据流）
                              │ Parquet bundle
                              ▼
                      本机 FastAPI/SDK
                    ├── direct：内存流水线
                    └── cache：增量本地对象库
                              │
                              ▼
                  application/vnd.apache.arrow.stream
```

87 到本机传输的是已经压缩的 Parquet 对象。精确到秒的边界过滤和 Arrow 编码在本机
完成。网络读取与本机解码使用有界流水线重叠执行。

根`catalog.json`固定约400字节，不保存全历史对象。网关只读取请求涉及日期的
`current.json`及其不可变version分片，并用最多64个分片的LRU限制常驻元数据内存。
每日新增数据只发布当天分片；无需重写或加载全部历史。

## 一键部署

服务器管理员：

```bash
sudo ./install-server.sh /正式五分钟数据根目录 alice bob carol dave
```

每位用户：

```bash
./install-client.sh 10.10.10.87
~/.local/bin/mdapi-local
```

客户端命令在前台运行，`Ctrl+C`停止，不安装systemd。完整说明见`QUICKSTART.md`。

## 开发环境

```bash
cd /home/quant/market_data_api
python -m venv --system-site-packages .venv
.venv/bin/pip install -e '.[api,test]'
```

87 的网关只依赖 Python 标准库，不需要 FastAPI、DuckDB 或 PyArrow。

## 构建与每日增量

五天测试集使用独立目录：

```bash
PYTHONPATH=/home/quant/market_data_api/src \
/home/quant/miniconda/envs/qmt310/bin/python -m market_data_api.builder \
  --dest-root /home/quant/market_data_api_5m_example_v2 \
  --latest 5 \
  --threads 4 \
  --execute
```

不加 `--execute` 只打印计划。每日任务可继续执行同一命令：

- 源文件 size/mtime 列表产生 `source_fingerprint`；
- 相同指纹和已发布版本直接跳过；
- 新日期自动构建；
- 历史日期被上游修订时产生新版本；
- 构建期间源指纹发生变化则拒绝发布；
- 暂存、行数与 schema 校验完成后才原子发布；
- 旧派生版本默认保留，绝不涉及原始湖。

建议上游只传最近若干日期，例如 `--latest 5`，避免每天遍历全部历史数据。

## 开发时手动启动87只读网关

```bash
PYTHONPATH=/home/quant/market_data_api/src \
/usr/bin/python3 -m market_data_api.gateway \
  --root /home/quant/market_data_api_5m_example_v2 \
  --host 10.10.10.87 \
  --port 18787 \
  --max-streams 2 \
  --token-file /path/to/users.json
```

生产环境为每位用户配置独立令牌：

```json
{
  "alice": "独立随机令牌1",
  "bob": "独立随机令牌2",
  "carol": "独立随机令牌3",
  "dave": "独立随机令牌4"
}
```

客户端通过 `--gateway-token` 使用自己的令牌。网关按令牌识别用户：多人等待时每位用户
最多占一条流并按用户轮转；没有其他用户等待时，同一用户可以借用空闲的第二条流。
`MDAPI_GATEWAY_TOKEN_FILE` 可代替 `--token-file`。旧的单一
`MDAPI_GATEWAY_TOKEN` 仍兼容，但只能按客户端 IP 区分公平性。

客户端使用建立后长期复用的 HTTP 连接；不会为每个请求建立 SSH，也不会在 87 上启动
临时查询进程。长API请求默认拆成每段最多12个对象，既能让四位用户及时轮转，也避开
单次网关对象数上限；网络中断时只重拉当前未完成对象。

## 开发时手动启动本机 API

```bash
/home/quant/market_data_api/.venv/bin/mdapi serve-local \
  --config ~/.config/market-data-api/client.json
```

省略 `--cores` 时，根据 CPU affinity 与可用内存自适应，最多使用 8 个本机核心。
`--cores 1` 是严格单核模式。远端数据流默认最多两个。

## 接口

估算但不拉数据：

```http
POST /v1/estimate
Content-Type: application/json
```

返回数据：

```http
POST /v1/data
Content-Type: application/json
Accept: application/vnd.apache.arrow.stream
```

请求示例：

```json
{
  "dataset": "snapshots",
  "start": "2026-05-29T09:15:00+08:00",
  "end": "2026-05-29T09:25:00+08:00",
  "mode": "cache",
  "update": "missing_only",
  "columns": null
}
```

区间采用 `[start, end)`。数据集可为 `orders`、`trades` 或 `snapshots`。

日期区间内每天取相同时间窗口时，可以直接使用更符合业务含义的形式；`end_date`
是包含在内的：

```json
{
  "dataset": "orders",
  "start_date": "2026-05-25",
  "end_date": "2026-05-29",
  "daily_start": "09:15:00",
  "daily_end": "09:25:00",
  "mode": "direct",
  "update": "missing_only"
}
```

该请求只选择五个交易日各自 09:15–09:25 的桶，不会把日期之间的整日数据拉回。

缓存更新模式：

- `missing_only`：默认；已存在的桶不更新，只补新日期或缺失桶；
- `if_changed`：远端版本变化时更新已有桶；
- `force`：强制获取所选桶的当前远端版本。

`direct` 和 `cache` 都返回相同的 Arrow IPC Stream。缓存模式第一次把缺失 Parquet
写入 `.partial`，校验文件大小、行数后原子发布，再提交本地数据库事务。

用户Python代码可以使用包内SDK：

```python
from market_data_api import MarketDataClient

client = MarketDataClient()
for batch in client.iter_batches({
    "dataset": "snapshots",
    "start": "2026-05-29T09:15:00+08:00",
    "end": "2026-05-29T09:25:00+08:00",
    "mode": "direct",
}):
    process(batch)
```

## 资源预检

本机从很小的 Manifest 得到：

- 压缩传输字节数；
- Parquet 解压字节估算；
- 最大 5 分钟桶的工作内存；
- cache 模式缺失字节数。

总结果大小默认不设硬上限。`estimated_arrow_memory` 只作为信息返回；用户收到数据后是
逐批处理、保留在内存还是自行落盘，不属于 API 的内存职责。确有需要时，用户仍可通过
`--max-response-gib` 主动设置一个额外的结果硬上限。

API 只保护自身有界流水线。工作内存按最大 5 分钟桶估算，使用真实样本校准后的保守
倍率：snapshots 24x、orders 16x、trades 10x（相对压缩 Parquet 源字节）。每次请求均
重新读取本机当前可用内存，先保留至少 512MiB 且不少于物理内存 5% 的紧急余量，再把
剩余可用内存的 40% 作为流水线额度。估算工作内存超过额度时，在网络读取开始前返回
HTTP 413。

运行期间还会在接收远端对象和解码 Arrow batch 时复查可用内存。低于紧急余量时主动
终止数据流；能够正常抛出的 Python/PyArrow 内存异常会转换为
`local_memory_exhausted`。操作系统直接触发 OOM Killer 时无法由已被杀死的进程返回
错误，因此运行前预检和有界流水线仍是主要保护。

并发同时受两个条件限制：

- CPU 核心预算；direct 最多使用两个远端长连接，cache 可独立使用本地核心预算；
- 加权内存令牌。

因此不会仅因机器核心数多就同时解压过多大桶。

## 四用户公平调度

87 仍保留两个数据流，以维持机械盘和网络的最高有效吞吐。长API请求会拆成每段最多
12个对象，每个小bundle结束即重新参与按用户调度，不绑定长期HTTP连接：

- 四位用户同时等待时，按用户轮转；
- 同一用户的新请求排在其他等待用户之后；
- 只有一位用户时可以同时使用两条流；
- 用户API不设置总字节或日期范围限制，内部只限制单个公平调度分段；
- 87网关响应和日志记录 `X-MDAPI-Queue-Ms` 排队时间。

因此单次年度请求或年度`for`循环都不会永久占用槽位。

## 性能原则

- Parquet 已压缩，传输时不再重复压缩；
- 87 网关只做 Manifest 选择和 `sendfile`；
- 长时间请求在同一持久连接上使用小bundle分段，不会逐文件重建连接；
- HTTP 连接常驻复用；
- Linux 页缓存承担热数据缓存，不在 Python 再复制一套内存缓存；
- 机械盘冷读默认只允许少量顺序流，避免并发寻道；
- 本机用两级有界流水线重叠网络读取与 Parquet 解码。

`tools/bench_gateway.py` 测网关原始吞吐，`tools/bench_local_api.py` 测最终 Arrow API。
完整五天实测见 [BENCHMARK.md](BENCHMARK.md)。

## 测试

```bash
/home/quant/market_data_api/.venv/bin/pytest -q
```

集成测试覆盖：

- 5 分钟半开区间选择；
- 常驻 HTTP bundle；
- direct/cache Arrow 内容一致；
- 第一次增量缓存和第二次零网络填充；
- 远端版本变化时 `missing_only` 保持旧版本；
- `if_changed` 原子切换到新版本；
- FastAPI 应用创建；
- 四用户按用户轮转及单用户空闲借用；
- 默认无总结果硬上限、可选显式上限；
- 动态工作内存拒绝与运行期内存异常转换；
- 分片catalog固定根大小、按日期懒加载和不可变历史版本续读；
- 传输中途断网后的对象级恢复及缓存断点保留。
