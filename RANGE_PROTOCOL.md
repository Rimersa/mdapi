# 0.5 按块读取协议

服务器不解码 Parquet 业务列。所有路径从认证后的 catalog 对象引用解析，必须位于数据根之内；用户不能传任意文件路径。对象引用包含 `object_id`、`dataset`、`trade_date`、`version`。

现有 `/v1/manifest`、`/v1/objects` 保留。清单包含能力标志 `parquet_footers_v1`、`range_bundles_v1`、`http_range_v1`。清单本身只按日期与桶选对象，股票和字段裁剪由读取器使用 footer 完成。

## POST /v1/metadata

请求 JSON 为 `{"objects": [对象引用, ...]}`，一次最多 32 个对象。响应包含对象 ID、version、file_bytes、footer。

`footer` 是 Base64 编码的文件尾部元数据及最后 8 字节。存在 `footer_codec="zlib"` 时需先解压；省略表示未压缩。客户端在前面补 `PAR1` 可解析元数据，但不能把该小文件当成含业务数据的 Parquet。

单个 footer 解压大小最多 16MiB 加末尾 8 字节。服务端和客户端都检查解压边界。

## POST /v1/ranges

每个对象引用增加 `ranges: [[offset, length], ...]`。字节段必须升序、不重叠、长度为正且在文件内。每对象最多 4096 段，请求体最多 1MiB。

返回沿用 MDPB0001 bundle framing。每部分的 header 中：

- `bytes`：这一部分实际传输的字节总数；
- `file_bytes`：原始不可变对象大小；
- `ranges`：与请求计划完全一致的字节段；
- 对象 ID、dataset、trade_date、version：用于防止错误拼接。

部分 body 按 ranges 顺序连接字节内容。客户端先完整接收并校验一个对象，再构造稀疏可读视图；不会按原文件长度分配内存或补齐未下载区域。Parquet reader 只读取已计划的行组和列。

一个对象中途断线时，只重新请求该未完成对象及后续对象。已完成对象留在本次有界缓冲区中，不重复向消费者返回。

## GET / HEAD /v1/object

查询参数为完整对象引用，支持单个标准 `Range: bytes=start-end`、开放结尾和后缀范围。成功部分读取返回 206、Content-Range、Accept-Ranges 和与 object_id 对应的 ETag。If-Match 不匹配返回 412，非法范围返回 416。

批量读取优先使用 /v1/ranges，以减少往返并共享按用户调度。

## 缓存与调度

可选 `--metadata-index` / `MDAPI_METADATA_INDEX` 指向独立 SQLite 文件，禁止放入行情数据根内。存储压缩 footer，按对象版本和文件签名失效。它是可删除重建的辅助缓存，不参与上游发布，也不存业务数据列。

服务端内存 footer LRU 默认 64MiB；磁盘缓存目标为 512MiB 压缩负载，按写入批次淘汰旧记录，SQLite 文件和 WAL 还会有存储开销。客户端原始 footer LRU 默认 64MiB，解析后的 Arrow 元数据只保留当前窗口。

完整对象、字节段和元数据请求都受按个人令牌的公平传输调度约束。SDK 的按块包目标为 16MiB，完整对象包目标为 64MiB；单个大对象允许超过内部包目标。它们不是完整查询结果的大小上限。
