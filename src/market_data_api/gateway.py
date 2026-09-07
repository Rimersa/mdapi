from __future__ import annotations

import argparse
import hmac
import json
import os
import select
import signal
import socket
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import __version__
from .catalog import CatalogStore, ObjectEntry, manifest_summary
from .model import DataRequest
from .protocol import BUNDLE_MAGIC, encode_bundle_end, encode_part_header
from .scheduler import FairStreamScheduler, StreamQueueTimeout
from .range_gateway import (
    CAPABILITIES,
    FooterCache,
    FooterStore,
    parse_http_range,
    validate_ranges,
)


@dataclass(frozen=True)
class SelectedObject:
    entry: ObjectEntry
    path: Path
    part_header: bytes
    ranges: tuple[tuple[int, int], ...] | None = None

    @property
    def payload_bytes(self):
        return (
            self.entry.bytes if self.ranges is None else sum(n for _, n in self.ranges)
        )


class GatewayState:
    def __init__(
        self,
        root: Path,
        *,
        token: str | None,
        user_tokens: dict[str, str] | None = None,
        auth_required: bool = False,
        token_file: Path | None = None,
        max_streams: int,
        max_objects: int,
        queue_timeout: float,
        metadata_index: Path | None = None,
    ) -> None:
        self.root = root.resolve(strict=True)
        self.catalog_store = CatalogStore(self.root)
        self.token = token
        self.user_tokens = dict(user_tokens or {})
        self.auth_required = auth_required
        self.token_file = token_file.resolve(strict=True) if token_file else None
        if self.token and self.user_tokens:
            raise ValueError("token 和 user_tokens 不能同时设置")
        if len(set(self.user_tokens.values())) != len(self.user_tokens):
            raise ValueError("不同用户不能配置相同令牌")
        self.max_streams = max_streams
        self.max_objects = max_objects
        self.queue_timeout = queue_timeout
        self.stream_scheduler = FairStreamScheduler(max_streams)
        if metadata_index is not None:
            metadata_index = metadata_index.expanduser().resolve()
            if metadata_index.is_relative_to(self.root):
                raise ValueError("元数据缓存必须位于行情数据根目录之外")
        self.footer_cache = FooterCache(
            store=FooterStore(metadata_index) if metadata_index is not None else None
        )
        self._token_lock = threading.Lock()
        if self.token_file:
            token_stat = self.token_file.stat()
            self._token_signature = (
                token_stat.st_ino,
                token_stat.st_mtime_ns,
                token_stat.st_size,
            )
        else:
            self._token_signature = None

    def current_user_tokens(self) -> dict[str, str]:
        if self.token_file is None:
            return self.user_tokens
        stat = self.token_file.stat()
        signature = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
        with self._token_lock:
            if signature != self._token_signature:
                self.user_tokens = _load_user_tokens(self.token_file)
                self._token_signature = signature
            return self.user_tokens

    def authenticate(self, supplied: str, client_ip: str) -> str | None:
        if supplied.startswith("Bearer "):
            supplied = supplied[7:]
        try:
            user_tokens = self.current_user_tokens()
        except (OSError, ValueError, json.JSONDecodeError):
            # Token storage failures must fail closed rather than retaining or
            # falling back to unauthenticated access.
            return None
        if user_tokens:
            matched: str | None = None
            for user_id, expected in user_tokens.items():
                if hmac.compare_digest(supplied, expected):
                    matched = user_id
            return matched
        if self.token and not hmac.compare_digest(supplied, self.token):
            return None
        if not self.token and self.auth_required:
            return None
        # Legacy shared-token and unauthenticated deployments are still made
        # fair across different client machines.  Named per-user tokens are
        # required when multiple users may share one machine or NAT address.
        return f"client:{client_ip}"

    def _materialize(self, entries: list[ObjectEntry]) -> list[SelectedObject]:
        if len(entries) > self.max_objects:
            raise OverflowError(
                f"请求涉及 {len(entries)} 个对象，超过网关上限 {self.max_objects}"
            )
        selected: list[SelectedObject] = []
        for entry in entries:
            path = (self.root / entry.relative_path).resolve(strict=True)
            if path == self.root or not path.is_relative_to(self.root):
                raise RuntimeError(f"catalog 路径越界: {entry.relative_path}")
            size = path.stat().st_size
            if size != entry.bytes:
                raise RuntimeError(
                    f"对象大小与 catalog 不一致: {entry.relative_path}: "
                    f"{size} != {entry.bytes}"
                )
            header = encode_part_header(
                {
                    "object_id": entry.object_id,
                    "dataset": entry.dataset,
                    "trade_date": entry.trade_date,
                    "bucket_start": entry.bucket_start,
                    "bucket_end": entry.bucket_end,
                    "relative_path": entry.relative_path,
                    "bytes": entry.bytes,
                    "rows": entry.rows,
                    "uncompressed_bytes": entry.uncompressed_bytes,
                    "version": entry.version,
                }
            )
            selected.append(SelectedObject(entry=entry, path=path, part_header=header))
        return selected

    def select(self, request: DataRequest) -> list[SelectedObject]:
        return self._materialize(self.catalog_store.selected(request))

    def select_refs(self, references: list[dict[str, str]]) -> list[SelectedObject]:
        return self._materialize(self.catalog_store.selected_refs(references))

    def select_ranges(self, references):
        selected = self.select_refs(references)
        by_id = {value["object_id"]: value for value in references}
        result = []
        for item in selected:
            ranges = validate_ranges(
                by_id[item.entry.object_id].get("ranges"), item.entry.bytes
            )
            header = item.entry.as_dict()
            header.update(
                file_bytes=item.entry.bytes,
                bytes=sum(n for _, n in ranges),
                ranges=ranges,
            )
            result.append(
                SelectedObject(
                    item.entry, item.path, encode_part_header(header), ranges
                )
            )
        return result


class MarketDataGatewayHandler(BaseHTTPRequestHandler):
    server_version = "MarketDataGateway/0.5"
    protocol_version = "HTTP/1.1"
    wbufsize = 0

    @property
    def state(self) -> GatewayState:
        return self.server.gateway_state  # type: ignore[attr-defined]

    def setup(self) -> None:
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024**2)
        self.connection.settimeout(60)

    def log_message(self, fmt: str, *args) -> None:
        message = {
            "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "client": self.client_address[0],
            "message": fmt % args,
        }
        print(json.dumps(message, ensure_ascii=False), flush=True)

    def _principal(self) -> str | None:
        return self.state.authenticate(
            self.headers.get("Authorization", ""),
            self.client_address[0],
        )

    def _json(self, status: HTTPStatus, payload: dict, *, head: bool = False) -> None:
        raw = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head:
            self.wfile.write(raw)

    def _error(self, status: HTTPStatus, code: str, detail: str) -> None:
        if getattr(self, "_streaming_response", False):
            # HTTP status cannot be changed after a stream starts. Close so clients
            # detect truncation immediately and retry the unfinished object.
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            return
        self._json(status, {"error": code, "detail": detail})

    def _parse_request(self, query: dict[str, list[str]]) -> DataRequest:
        extra = set(query) - {"dataset", "start", "end", "daily_start", "daily_end"}
        if extra:
            raise ValueError(
                f"文件清单接口不支持这些参数: {sorted(extra)}；请通过 SDK 或本机 API 筛选股票/字段"
            )

        def one(name: str) -> str:
            values = query.get(name)
            if not values or len(values) != 1:
                raise ValueError(f"参数 {name} 必须恰好出现一次")
            return values[0]

        return DataRequest.from_values(
            dataset=one("dataset"),
            start=one("start"),
            end=one("end"),
            daily_start=(one("daily_start") if "daily_start" in query else None),
            daily_end=one("daily_end") if "daily_end" in query else None,
        )

    def _manifest(self, request: DataRequest, *, head: bool) -> None:
        # Manifest selection is metadata-only and may cover far more objects
        # than one transfer lease.  The client downloads the exact IDs in
        # small, fair, resumable chunks through /v1/objects.
        entries = self.state.catalog_store.selected(request)
        self._json(
            HTTPStatus.OK,
            {
                "format": "market-data-selection-v1",
                "capabilities": CAPABILITIES,
                "catalog_generated_at": self.state.catalog_store.generated_at,
                "request": {
                    "dataset": request.dataset,
                    "start": request.start.isoformat(),
                    "end": request.end.isoformat(),
                    "daily_start": (
                        request.daily_start.isoformat() if request.daily_start else None
                    ),
                    "daily_end": (
                        request.daily_end.isoformat() if request.daily_end else None
                    ),
                },
                "summary": manifest_summary(entries),
                "objects": [entry.as_dict() for entry in entries],
            },
            head=head,
        )

    @staticmethod
    def _bundle_length(selected: list[SelectedObject]) -> int:
        return (
            len(BUNDLE_MAGIC)
            + len(encode_bundle_end())
            + sum(len(item.part_header) + item.payload_bytes for item in selected)
        )

    def _sendfile(self, path: Path) -> None:
        with path.open("rb", buffering=0) as handle:
            if hasattr(os, "posix_fadvise"):
                os.posix_fadvise(
                    handle.fileno(),
                    0,
                    0,
                    os.POSIX_FADV_SEQUENTIAL,
                )
            offset = 0
            remaining = path.stat().st_size
            output_fd = self.connection.fileno()
            while remaining:
                try:
                    sent = os.sendfile(output_fd, handle.fileno(), offset, remaining)
                except BlockingIOError:
                    if not select.select([], [output_fd], [], 60)[1]:
                        raise TimeoutError("数据消费者持续 60 秒未读取")
                    continue
                if sent == 0:
                    raise ConnectionError(f"sendfile 提前结束: {path}")
                offset += sent
                remaining -= sent

    def _sendfile_ranges(self, path: Path, ranges) -> None:
        """Send planned intervals in file-offset order without decoding or rewriting."""
        with path.open("rb", buffering=0) as handle:
            for offset, remaining in ranges:
                while remaining:
                    try:
                        sent = os.sendfile(
                            self.connection.fileno(), handle.fileno(), offset, remaining
                        )
                    except BlockingIOError:
                        if not select.select([], [self.connection.fileno()], [], 60)[1]:
                            raise TimeoutError("数据消费者持续 60 秒未读取")
                        continue
                    if sent == 0:
                        raise ConnectionError(f"范围读取提前结束: {path}")
                    offset += sent
                    remaining -= sent

    def _bundle_selected(
        self,
        selected: list[SelectedObject],
        *,
        head: bool,
        principal: str,
    ) -> None:
        try:
            lease = self.state.stream_scheduler.acquire(
                principal,
                timeout=self.state.queue_timeout,
            )
        except StreamQueueTimeout:
            self._error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "gateway_busy",
                "远端数据流槽位等待超时",
            )
            return
        try:
            length = self._bundle_length(selected)
            self._streaming_response = True
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/x-marketdata-parquet-bundle")
            self.send_header("Content-Length", str(length))
            self.send_header("X-MDAPI-Objects", str(len(selected)))
            self.send_header(
                "X-MDAPI-Transfer-Bytes",
                str(sum(item.payload_bytes for item in selected)),
            )
            self.send_header(
                "X-MDAPI-Source-Bytes",
                str(sum(item.entry.bytes for item in selected)),
            )
            self.send_header("X-MDAPI-Queue-Ms", f"{lease.queue_ms:.3f}")
            self.send_header(
                "X-MDAPI-Borrowed-Stream",
                "1" if lease.borrowed else "0",
            )
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if head:
                return
            self.wfile.write(BUNDLE_MAGIC)
            for item in selected:
                self.wfile.write(item.part_header)
                if item.ranges is None:
                    self._sendfile(item.path)
                else:
                    self._sendfile_ranges(item.path, item.ranges)
            self.wfile.write(encode_bundle_end())
            self._streaming_response = False
        finally:
            self.state.stream_scheduler.release(lease)

    def _bundle(
        self,
        request: DataRequest,
        *,
        head: bool,
        principal: str,
    ) -> None:
        self._bundle_selected(
            self.state.select(request),
            head=head,
            principal=principal,
        )

    def _handle(self, *, head: bool) -> None:
        self._streaming_response = False
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/health":
            queue = self.state.stream_scheduler.snapshot()
            self._json(
                HTTPStatus.OK,
                {
                    "status": "ok",
                    "version": __version__,
                    "capabilities": CAPABILITIES,
                    "footer_cache_bytes": self.state.footer_cache.bytes,
                    "metadata_index_enabled": self.state.footer_cache.store is not None,
                    "configured_users": len(self.state.current_user_tokens()),
                    "catalog_generated_at": self.state.catalog_store.generated_at,
                    "catalog_format": self.state.catalog_store.format,
                    "cached_catalog_shards": (self.state.catalog_store.cached_shards),
                    "cached_catalog_pointers": (
                        self.state.catalog_store.cached_pointers
                    ),
                    "max_streams": self.state.max_streams,
                    **queue,
                },
                head=head,
            )
            return
        principal = self._principal()
        if principal is None:
            self._error(HTTPStatus.UNAUTHORIZED, "unauthorized", "认证失败")
            return
        try:
            query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if parsed.path == "/v1/object":
                self._http_object(query, head=head, principal=principal)
                return
            request = self._parse_request(query)
            if parsed.path == "/v1/manifest":
                self._manifest(request, head=head)
            elif parsed.path == "/v1/data":
                self._bundle(request, head=head, principal=principal)
            else:
                self._error(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")
        except ValueError as exc:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
        except OverflowError as exc:
            self._error(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request_too_large", str(exc)
            )
        except StreamQueueTimeout as exc:
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, "gateway_busy", str(exc))
        except FileNotFoundError as exc:
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, "object_missing", str(exc))
        except BrokenPipeError:
            self.close_connection = True
        except Exception as exc:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "gateway_error", str(exc))

    def _http_object(self, query, *, head, principal):
        allowed = {"object_id", "dataset", "trade_date", "version"}
        if set(query) != allowed or any(len(v) != 1 for v in query.values()):
            raise ValueError("对象请求必须指定 object_id、dataset、trade_date、version")
        item = self.state.select_refs([{k: v[0] for k, v in query.items()}])[0]
        etag = f'"{item.entry.object_id}"'
        if self.headers.get("If-Match") not in (None, "*", etag):
            self._error(
                HTTPStatus.PRECONDITION_FAILED, "version_mismatch", "对象版本不匹配"
            )
            return
        try:
            offset, count = parse_http_range(
                self.headers.get("Range"), item.entry.bytes
            )
        except (ValueError, OverflowError):
            self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            self.send_header("Content-Range", f"bytes */{item.entry.bytes}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with self.state.stream_scheduler.lease(
            principal, timeout=self.state.queue_timeout
        ):
            ranged = self.headers.get("Range") is not None
            self._streaming_response = True
            self.send_response(HTTPStatus.PARTIAL_CONTENT if ranged else HTTPStatus.OK)
            self.send_header("Content-Type", "application/vnd.apache.parquet")
            self.send_header("Content-Length", str(count))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", etag)
            if ranged:
                self.send_header(
                    "Content-Range",
                    f"bytes {offset}-{offset + count - 1}/{item.entry.bytes}",
                )
            self.end_headers()
            if not head:
                self._sendfile_ranges(item.path, ((offset, count),))
            self._streaming_response = False

    def do_GET(self) -> None:
        self._handle(head=False)

    def do_HEAD(self) -> None:
        self._handle(head=True)

    def do_POST(self) -> None:
        self._streaming_response = False
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path not in {"/v1/objects", "/v1/metadata", "/v1/ranges"}:
            self._error(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")
            return
        principal = self._principal()
        if principal is None:
            self._error(HTTPStatus.UNAUTHORIZED, "unauthorized", "认证失败")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > 1024 * 1024:
                raise ValueError("请求体大小必须在 1B 到 1MiB 之间")
            raw = json.loads(self.rfile.read(length))
            if isinstance(raw, dict) and set(raw) != {"objects"}:
                raise ValueError("请求体只接受 objects 字段")
            references = raw.get("objects") if isinstance(raw, dict) else None
            if not isinstance(references, list) or not all(
                isinstance(value, dict) for value in references
            ):
                raise ValueError("objects 必须是对象引用数组")
            normalized: list[dict[str, str]] = []
            for value in references:
                allowed = {"object_id", "dataset", "trade_date", "version"}
                if parsed.path == "/v1/ranges":
                    allowed.add("ranges")
                if set(value) - allowed:
                    raise ValueError(
                        f"对象引用包含未知字段: {sorted(set(value) - allowed)}"
                    )
                normalized.append(
                    {
                        "object_id": str(value.get("object_id", "")),
                        "dataset": str(value.get("dataset", "")),
                        "trade_date": str(value.get("trade_date", "")),
                        "version": str(value.get("version", "")),
                    }
                )
                if parsed.path == "/v1/ranges":
                    normalized[-1]["ranges"] = value.get("ranges")
            if parsed.path == "/v1/metadata":
                selected = self.state.select_refs(normalized)
                with self.state.stream_scheduler.lease(
                    principal, timeout=self.state.queue_timeout
                ):
                    self._json(HTTPStatus.OK, self.state.footer_cache.payload(selected))
                return
            self._bundle_selected(
                (
                    self.state.select_ranges(normalized)
                    if parsed.path == "/v1/ranges"
                    else self.state.select_refs(normalized)
                ),
                head=False,
                principal=principal,
            )
        except ValueError as exc:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
        except OverflowError as exc:
            self._error(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request_too_large", str(exc)
            )
        except StreamQueueTimeout as exc:
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, "gateway_busy", str(exc))
        except FileNotFoundError as exc:
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, "object_missing", str(exc))
        except BrokenPipeError:
            self.close_connection = True
        except Exception as exc:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "gateway_error", str(exc))


class MarketDataGatewayServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, state: GatewayState):
        self.gateway_state = state
        super().__init__(address, handler)


def _load_user_tokens(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("token-file 必须是 {user_id: token} JSON 对象")
    result: dict[str, str] = {}
    for user_id, token in raw.items():
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("token-file 的 user_id 必须是非空字符串")
        if not isinstance(token, str) or not token:
            raise ValueError(f"用户 {user_id!r} 的 token 必须是非空字符串")
        result[user_id.strip()] = token
    if len(set(result.values())) != len(result):
        raise ValueError("token-file 中不同用户不能使用相同 token")
    return result


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="87 侧常驻、只读、零拷贝数据网关")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18787)
    parser.add_argument("--token", default=os.environ.get("MDAPI_GATEWAY_TOKEN"))
    parser.add_argument(
        "--token-file",
        type=Path,
        default=(
            Path(os.environ["MDAPI_GATEWAY_TOKEN_FILE"])
            if os.environ.get("MDAPI_GATEWAY_TOKEN_FILE")
            else None
        ),
        help="每用户独立令牌 JSON：{user_id: token}",
    )
    parser.add_argument("--max-streams", type=int, default=2)
    parser.add_argument("--max-objects", type=int, default=5000)
    parser.add_argument("--queue-timeout", type=float, default=300.0)
    parser.add_argument(
        "--metadata-index",
        type=Path,
        default=(
            Path(os.environ["MDAPI_METADATA_INDEX"])
            if os.environ.get("MDAPI_METADATA_INDEX")
            else None
        ),
        help="可选的压缩元数据 SQLite 缓存，必须在行情数据根目录之外",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.max_streams < 1:
        raise ValueError("max-streams 必须 >= 1")
    if args.max_objects < 1:
        raise ValueError("max-objects 必须 >= 1")
    if args.token and args.token_file:
        raise ValueError("--token 和 --token-file 不能同时使用")
    user_tokens = _load_user_tokens(args.token_file)
    state = GatewayState(
        args.root,
        token=args.token,
        user_tokens=user_tokens,
        auth_required=args.token_file is not None,
        token_file=args.token_file,
        max_streams=args.max_streams,
        max_objects=args.max_objects,
        queue_timeout=args.queue_timeout,
        metadata_index=args.metadata_index,
    )
    server = MarketDataGatewayServer(
        (args.host, args.port),
        MarketDataGatewayHandler,
        state,
    )

    def stop(_signum, _frame):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(
        json.dumps(
            {
                "status": "ready",
                "version": __version__,
                "host": args.host,
                "port": args.port,
                "root": str(state.root),
                "max_streams": args.max_streams,
                "auth_mode": (
                    "per_user_tokens"
                    if user_tokens
                    else "locked"
                    if args.token_file
                    else "shared_token"
                    if args.token
                    else "client_ip"
                ),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
