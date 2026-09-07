# 0.5.0 部署和运维

## 输入与运行要求

网关运行在 Linux，要求 Python 3.10 或更新版本，只使用标准库。输入是已经发布的五分钟 Parquet 与 catalog v2；网关不会改写、重新切片、清洗或删除行情文件。

运行账号需要读取 catalog/current/version JSON、进入对象目录并读取 Parquet 的权限。不要让迁移或维护工具改变这些读取权限。

## 安装和升级

在发行包解压目录中：

```bash
./install-server.sh /data/market_data_5m
```

不加 sudo 延续用户服务；加 sudo 创建系统服务。已有部署升级时保持同一种服务模式，避免创建两套监听相同端口的服务。

安装器保留已有用户令牌、监听地址、端口、并发设置，更新程序和服务配置并重启。数据根参数明确指定新服务读取位置。安装器不改动行情目录内容。

升级前建议保存程序和配置以便回滚。回滚只替换程序与配置并重启，数据格式无需转换。

| 内容 | 用户服务 | 系统服务 |
|---|---|---|
| 程序 | `~/.local/share/market-data-api-server/mdapi-gateway.pyz` | `/opt/market-data-api/mdapi-gateway.pyz` |
| 配置 | `~/.config/market-data-api-server/gateway.env` | `/etc/market-data-api/gateway.env` |
| 用户 | 同配置目录的 users.json | 同配置目录的 users.json |
| 元数据缓存 | `~/.cache/market-data-api-server/footers.sqlite3` | `/var/cache/market-data-api/footers.sqlite3` |

用户服务控制：

```bash
systemctl --user status market-data-gateway
systemctl --user restart market-data-gateway
journalctl --user -u market-data-gateway -n 100
```

系统服务则省略 `--user`，由管理员操作。

用户服务要在无人登录时常驻，需由管理员执行一次：

```bash
sudo loginctl enable-linger quant
```

## 用户和认证

```bash
mdapi-user add alice
mdapi-user list
mdapi-user rotate alice
mdapi-user remove alice
```

用户令牌热更新，不需要重启；每位用户应有独立令牌。零用户时数据接口锁定。健康接口不要求令牌。

```bash
curl http://10.10.10.87:18787/health
```

0.5 返回 version 和 `parquet_footers_v1`、`range_bundles_v1`、`http_range_v1` 能力标志。`metadata_index_enabled` 表示持久元数据缓存启用。

## 元数据缓存

`MDAPI_METADATA_INDEX` 或 `--metadata-index` 可指定压缩 footer SQLite 缓存，路径必须位于行情数据根目录之外。安装器默认启用；手动启动且不指定时只有内存缓存。

首次查询新对象时惰性填充，后续同版本复用。按对象版本、文件 inode、mtime 和大小识别变化。数据库损坏的条目会回退到源 footer；该缓存可以删除重建，不是数据来源。停止网关后可删除整个缓存数据库及同名 WAL/SHM，再启动重建。

系统服务启用 ProtectSystem=strict，仅默认缓存目录允许写入。若自定义缓存路径，需要同时给该目录正确的账号权限及 systemd ReadWritePaths。

可以从安装了客户端的机器预热某个明确日期区间：

```bash
python tools/warm_metadata.py \
  --config ~/.config/market-data-api/client.json \
  --start-date 2026-09-01 --end-date 2026-09-04
```

它只请求 footer，不下载业务列，也不改写行情文件。可在常用数据发布后预热；初次索引大量冷历史文件仍需要付出寻道成本。

## 并发和客户端

默认 `MDAPI_MAX_STREAMS=2`，按个人令牌公平轮转；队列有上限。客户端会将大查询分成有限字节量/对象数量的小段。单个大对象仍可能需要较长读取时间，不保证所有历史随机读都有固定延迟。

用户自己的 Python 环境推荐安装 `[client]` 并使用 `MarketDataClient.connect()`。需要共享本机 HTTP 服务时安装 `[api]` 并运行 `mdapi-local`。本机服务监听默认 127.0.0.1:18788，完整用户用法见 USER_GUIDE.md。

## 验证与发布

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python tools/build_release.py
```

发行包包含 wheel、单文件网关、安装脚本、SHA256SUMS 及文档。`tools/bench_selective.py` 可在开发源码目录进行按需读取与四用户验证；生产数据只读，临时测试账号应放在独立配置中。
