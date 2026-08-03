# Market Data API 0.4.1 部署说明

普通安装请直接使用 [`QUICKSTART.md`](QUICKSTART.md) 的两条命令。本文件解释一键
安装背后的目录、配置、升级和排障方式。

## 组件边界

- 87服务器：`mdapi-gateway.pyz`，只依赖系统Python标准库；
- 用户机器：本机FastAPI、PyArrow处理流水线和可选增量缓存；
- 上游程序：按 [`SERVER_DATA_FORMAT.md`](SERVER_DATA_FORMAT.md) 生成五分钟成品数据；

数据请求不使用SSH。SSH只用于管理员复制发布包、升级和排障。

## 一、87服务器

### 推荐安装

系统服务模式：

```bash
sudo ./install-server.sh /正式五分钟数据根目录
```

没有sudo时可使用用户服务模式：

```bash
./install-server.sh /正式五分钟数据根目录
```

用户服务模式安装到`~/.local/share/market-data-api-server`和
`~/.config/systemd/user`。管理员执行`sudo loginctl enable-linger quant`后，可保证无人
登录和重启后仍自动运行。

安装位置：

```text
/opt/market-data-api/mdapi-gateway.pyz
/etc/market-data-api/gateway.env
/etc/market-data-api/users.json
/etc/systemd/system/market-data-gateway.service
```

用户服务模式的对应位置为：

```text
~/.local/share/market-data-api-server/mdapi-gateway.pyz
~/.config/market-data-api-server/gateway.env
~/.config/market-data-api-server/users.json
~/.config/systemd/user/market-data-gateway.service
~/.local/bin/mdapi-user
```

脚本要求数据目录已经存在并包含`catalog.json`。它只检查该目录，不会创建、切片、
覆盖或删除其中的数据。

正式目录应使用分片catalog v2。旧单文件v1可以先只打印迁移计划：

```bash
python3 tools/migrate_catalog_v2.py --root /五分钟数据根目录
```

确认对象数和分片数后执行：

```bash
python3 tools/migrate_catalog_v2.py \
  --root /五分钟数据根目录 \
  --execute
```

该工具不读取或修改Parquet，只新增版本分片、当前指针和固定大小根索引。默认不留下旧
格式副本；确需额外归档时增加`--keep-backup`。0.4.1网关仍可临时读取旧单文件catalog，
但它会随历史线性膨胀，不应继续用于长期生产。

状态和日志：

```bash
systemctl status market-data-gateway
journalctl -u market-data-gateway -f
curl http://10.10.10.87:18787/health
```

用户服务模式把前两条改为`systemctl --user status ...`和
`journalctl --user -u ...`。

用户令牌保存在`/etc/market-data-api/users.json`。重复运行安装命令时，已有用户令牌
保持不变，只给新用户名生成令牌。

零用户时`users.json`为`{}`，健康接口正常，但所有数据接口返回401。用户管理采用热
加载，不需要重启网关：

```bash
mdapi-user add alice
mdapi-user list
mdapi-user show alice
mdapi-user rotate alice
mdapi-user remove alice
```

系统服务模式下通常使用`sudo mdapi-user ...`；用户服务模式直接运行。令牌文件通过
临时文件、`fsync`和原子重命名更新，已有请求不受影响。

### 每日数据更新

上游程序依次原子发布新Parquet、新的不可变version分片、当天`current.json`，最后只
更新约400字节的根`catalog.json`。网关按需重载涉及日期，无需重启。旧请求继续读取旧
version，新请求立即读取新version。具体字段和顺序见`SERVER_DATA_FORMAT.md`。

### 升级

使用新发布包重复执行原安装命令即可。安装脚本替换网关程序和服务模板，但保留已有
用户令牌：

```bash
sudo ./install-server.sh /正式五分钟数据根目录
```

## 二、用户机器

### 推荐安装

```bash
./install-client.sh 10.10.10.87
```

令牌不写入命令历史；安装程序会隐藏输入。默认位置：

```text
~/.local/share/market-data-api/venv
~/.config/market-data-api/client.json
~/.local/bin/mdapi-local
~/.cache/market-data-api
```

`client.json`权限为`0600`。客户端没有systemd服务，也不会开机启动。

开始工作前运行：

```bash
~/.local/bin/mdapi-local
```

保持该终端开启。用户研究代码访问`http://127.0.0.1:18788`；工作完成后按`Ctrl+C`。

如需改变本机端口或缓存位置，编辑`client.json`。命令行参数优先于配置文件，例如：

```bash
~/.local/bin/mdapi-local --port 18888 --cores 1
```

默认网络恢复参数为：

```json
{
  "network_retries": 3,
  "network_retry_backoff": 0.25,
  "object_request_size": 12
}
```

本机API把长请求拆成小段，通过同一持久HTTP连接读取。断线时重新建TCP连接并从未完成
的五分钟对象继续，不涉及SSH。cache会保留已经原子提交的完整桶，下一次请求也只补
缺失桶。

### SDK

本地API可以从任何语言使用。Python研究环境可额外安装`client`依赖：

```bash
python -m pip install '/发布包目录/wheels/market_data_api-0.4.1-py3-none-any.whl[client]'
```

建议用`MarketDataClient.iter_batches()`逐批消费。`read_table()`会把完整结果物化到用户
进程内存，是否保留或落盘由用户自行决定。

完整示例、参数、错误码和排障见[`USER_GUIDE.md`](USER_GUIDE.md)。

## 三、网络与安全

- 87网关只绑定内网地址，不应暴露到公网；
- 防火墙只允许授权用户机器访问TCP 18787；
- 每位用户使用独立令牌，不能共享；
- 不可信网络应使用VPN或TLS反向代理；
- 本机FastAPI固定默认绑定`127.0.0.1`。

## 四、依赖说明

87单文件网关不需要pip、venv、PyArrow、FastAPI、DuckDB或Polars。

用户安装程序需要Python 3.10或更高版本，并通过用户配置的pip源安装PyArrow、FastAPI、
Uvicorn和psutil。如果用户机器不能访问公共或内部pip源，需要管理员另行提供与该用户
Python版本和操作系统匹配的依赖wheelhouse。
