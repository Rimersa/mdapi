# Market Data API 一键安装

## 87服务器：一条安装命令

把发布包复制到87、解压并进入目录后运行：

零用户锁定部署：

```bash
sudo ./install-server.sh /正式五分钟数据根目录
```

没有sudo时去掉`sudo`，会安装为当前账号的用户服务。直接创建用户时把用户名追加到命令
末尾即可。

这条命令会自动完成：

- 安装单文件只读网关；
- 创建空的安全用户库，或生成指定用户的独立令牌；
- 写入服务器配置；
- 安装并启动服务器端 systemd 服务；
- 执行健康检查。

数据根目录必须已经包含符合 [`SERVER_DATA_FORMAT.md`](SERVER_DATA_FORMAT.md) 的
`catalog.json` 和五分钟 Parquet。安装程序不会切片，也不会写入或修改数据目录。
长期运行必须使用固定根索引、按数据集/交易日/版本分片的catalog v2；旧v1迁移命令见
[`DEPLOYMENT.md`](DEPLOYMENT.md)。

零用户时健康接口可用，但所有数据接口保持锁定。

以后新增用户无需重装或重启：

```bash
mdapi-user add alice
mdapi-user list
mdapi-user rotate alice
mdapi-user remove alice
```

用户文件采用原子更新，网关自动热加载。新增或轮换命令会显示需要安全交给该用户的
令牌。

## 用户机器：一条安装命令

用户解压同一个发布包并进入目录后运行：

```bash
./install-client.sh 10.10.10.87
```

安装程序会静默询问该用户自己的令牌，然后自动创建隔离环境、安装依赖、保存配置并
生成 `~/.local/bin/mdapi-local`。

需要取数时运行：

```bash
~/.local/bin/mdapi-local
```

看到 `Uvicorn running on http://127.0.0.1:18788` 后即可使用。该命令在前台运行，
按 `Ctrl+C` 就停止；不会注册 systemd，不会开机自启。

## 取数

研究环境安装轻量SDK：

```bash
python -m pip install '/发布包目录/wheels/market_data_api-0.4.1-py3-none-any.whl[client]'
```

```python
from market_data_api import MarketDataClient

client = MarketDataClient()
query = {
    "dataset": "snapshots",
    "start": "2026-05-29T09:15:00+08:00",
    "end": "2026-05-29T09:25:00+08:00",
    "mode": "direct",
}

for batch in client.iter_batches(query):
    process(batch)
```

`mode="direct"`不落地；`mode="cache"`在用户机器增量缓存。用户循环取多天时只需保持
`mdapi-local`这个终端不退出，整个循环不会建立SSH或反复启动远端进程。

临时断网默认自动重试3次。续传以完整五分钟对象为单位：direct不重复已经返回的对象，
cache保留已完成桶并只补缺失桶。

完整用户说明见[`USER_GUIDE.md`](USER_GUIDE.md)。
