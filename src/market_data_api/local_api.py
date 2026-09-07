import argparse
import datetime as dt
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from . import __version__
from .client import GatewayError, NetworkTransferError
from .model import DataRequest
from .selective import ReadOptions
from .service import (
    DataService,
    LocalMemoryExhausted,
    NoData,
    RequestRejected,
    ServiceLimits,
)


DEFAULT_CLIENT_CONFIG = Path.home() / ".config" / "market-data-api" / "client.json"


@dataclass(frozen=True)
class LocalAPIConfig:
    gateway_host: str
    gateway_port: int
    gateway_token: str | None
    cache_root: Path
    host: str
    port: int
    cores: int | None
    max_response_gib: float | None
    arrow_compression: str
    network_retries: int
    network_retry_backoff: float
    object_request_size: int
    io_profile: str


def _load_client_config(path: Path, *, required: bool) -> dict:
    path = path.expanduser()
    if not path.exists():
        if required:
            raise FileNotFoundError(f"客户端配置不存在: {path}")
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"客户端配置必须是JSON对象: {path}")
    return raw


def _resolve_config(args: argparse.Namespace) -> LocalAPIConfig:
    explicit_config = args.config is not None
    config_path = args.config or DEFAULT_CLIENT_CONFIG
    raw = _load_client_config(config_path, required=explicit_config)

    def choose(cli_value, env_name: str, config_name: str, default=None):
        if cli_value is not None:
            return cli_value
        env_value = os.environ.get(env_name)
        if env_value not in (None, ""):
            return env_value
        return raw.get(config_name, default)

    gateway_host = str(
        choose(args.gateway_host, "MDAPI_GATEWAY_HOST", "gateway_host", "10.10.10.87")
    )
    gateway_port = int(
        choose(args.gateway_port, "MDAPI_GATEWAY_PORT", "gateway_port", 18787)
    )
    gateway_token = choose(
        args.gateway_token,
        "MDAPI_GATEWAY_TOKEN",
        "gateway_token",
    )
    cache_root = Path(
        choose(
            args.cache_root,
            "MDAPI_CACHE_ROOT",
            "cache_root",
            Path.home() / ".cache" / "market-data-api",
        )
    ).expanduser()
    host = str(choose(args.host, "MDAPI_LOCAL_HOST", "local_host", "127.0.0.1"))
    port = int(choose(args.port, "MDAPI_LOCAL_PORT", "local_port", 18788))
    cores_raw = choose(args.cores, "MDAPI_CORES", "cores")
    cores = int(cores_raw) if cores_raw is not None else None
    max_response_raw = choose(
        args.max_response_gib,
        "MDAPI_MAX_RESPONSE_GIB",
        "max_response_gib",
    )
    max_response_gib = (
        float(max_response_raw) if max_response_raw is not None else None
    )
    arrow_compression = str(
        choose(
            args.arrow_compression,
            "MDAPI_ARROW_COMPRESSION",
            "arrow_compression",
            "zstd",
        )
    )
    network_retries = int(
        choose(
            args.network_retries,
            "MDAPI_NETWORK_RETRIES",
            "network_retries",
            3,
        )
    )
    network_retry_backoff = float(
        choose(
            args.network_retry_backoff,
            "MDAPI_NETWORK_RETRY_BACKOFF",
            "network_retry_backoff",
            0.25,
        )
    )
    object_request_size = int(
        choose(
            args.object_request_size,
            "MDAPI_OBJECT_REQUEST_SIZE",
            "object_request_size",
            12,
        )
    )
    if gateway_port not in range(1, 65536) or port not in range(1, 65536):
        raise ValueError("端口必须在1到65535之间")
    if cores is not None and cores < 1:
        raise ValueError("cores必须>=1")
    if max_response_gib is not None and max_response_gib <= 0:
        raise ValueError("max_response_gib必须>0")
    if arrow_compression not in {"zstd", "lz4", "none"}:
        raise ValueError("arrow_compression必须是zstd、lz4或none")
    if network_retries < 0:
        raise ValueError("network_retries不能为负数")
    if network_retry_backoff < 0:
        raise ValueError("network_retry_backoff不能为负数")
    if object_request_size < 1:
        raise ValueError("object_request_size必须>=1")
    io_profile = str(choose(args.io_profile, "MDAPI_IO_PROFILE", "io_profile", "hdd"))
    ReadOptions.for_profile(io_profile)
    return LocalAPIConfig(
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        gateway_token=(str(gateway_token) if gateway_token is not None else None),
        cache_root=cache_root,
        host=host,
        port=port,
        cores=cores,
        max_response_gib=max_response_gib,
        arrow_compression=arrow_compression,
        network_retries=network_retries,
        network_retry_backoff=network_retry_backoff,
        object_request_size=object_request_size,
        io_profile=io_profile,
    )


def create_app(service: DataService):
    try:
        from fastapi import FastAPI, HTTPException
        from fastapi.responses import StreamingResponse
        from pydantic import BaseModel, Field, ConfigDict
    except ImportError as exc:
        raise RuntimeError(
            "本机 API 依赖未安装；请安装 market-data-api[api]"
        ) from exc

    class DataQuery(BaseModel):
        model_config = ConfigDict(extra="forbid")
        dataset: str
        start: str | None = None
        end: str | None = None
        start_date: str | None = None
        end_date: str | None = None
        mode: str = "direct"
        update: str = "missing_only"
        columns: list[str] | None = Field(default=None)
        daily_start: str | None = None
        daily_end: str | None = None
        symbols: list[str] | None = None
        read_strategy: str = "auto"

    def make_request(query: DataQuery) -> DataRequest:
        return DataRequest.from_query(query.model_dump())

    @asynccontextmanager
    async def lifespan(_app):
        try:
            yield
        finally:
            service.close()

    app = FastAPI(
        title="Market Data API",
        version=__version__,
        description="本机资源预检、87直读与本地增量缓存的统一 API",
        lifespan=lifespan,
    )

    @app.exception_handler(LocalMemoryExhausted)
    def local_memory_exhausted(_request, exc: LocalMemoryExhausted):
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=503,
            content={
                "detail": {
                    "code": "local_memory_exhausted",
                    "message": str(exc),
                    "available_memory": exc.available_memory,
                    "required_reserve": exc.required_reserve,
                    "stage": exc.stage,
                }
            },
        )

    @app.get("/health")
    def health():
        return {
            "status": "ok",
            "version": __version__,
            "local_cpus": service.resources.cpus,
            "total_memory": service.resources.total_memory,
            "available_memory": service.resources.available_memory,
            "workers": service.workers,
            "remote_connections": service.remote_connections,
            "core_budget": service.core_budget,
            "network_retries": service.limits.network_retries,
            "object_request_size": service.limits.object_request_size,
        }

    @app.post("/v1/estimate")
    def estimate(query: DataQuery):
        try:
            request = make_request(query)
            return service.preflight(request).as_dict()
        except RequestRejected as exc:
            raise HTTPException(
                status_code=413,
                detail={
                    "code": exc.code,
                    "message": exc.detail,
                    "estimates": exc.estimates,
                },
            ) from exc
        except NoData as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except GatewayError as exc:
            raise HTTPException(
                status_code=413 if exc.status == 413 else 502,
                detail={
                    "code": "gateway_rejected",
                    "message": exc.detail,
                    "gateway_status": exc.status,
                },
            ) from exc
        except NetworkTransferError as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "network_transfer_failed",
                    "message": str(exc),
                    "completed_objects": exc.completed_objects,
                    "remaining_objects": exc.remaining_objects,
                    "retries": exc.retries,
                },
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/v1/data")
    def data(query: DataQuery):
        try:
            request = make_request(query)
            plan = service.preflight(request)
            _, stream = service.arrow_stream(request, plan)
            # Validate the first output before sending HTTP 200; preserve generator cleanup.
            try:
                first_chunk = next(stream)
            except BaseException:
                stream.close()
                raise
            def primed_stream():
                try:
                    yield first_chunk
                    yield from stream
                finally:
                    stream.close()
            headers = {
                "X-MDAPI-Source-Bytes": str(plan.selection.source_bytes),
                "X-MDAPI-Estimated-Uncompressed-Bytes": str(
                    plan.selection.uncompressed_bytes
                ),
                "X-MDAPI-Estimated-Arrow-Memory": str(
                    plan.estimated_arrow_memory
                ),
                "X-MDAPI-Estimated-Working-Memory": str(
                    plan.estimated_working_memory
                ),
                "X-MDAPI-Working-Memory-Limit": str(
                    plan.working_memory_limit
                ),
                "X-MDAPI-Rows": str(plan.selection.rows),
                "X-MDAPI-Mode": request.mode.value,
                "X-MDAPI-Read-Path": "adaptive_ranges" if plan.selective else "whole_objects",
                "X-MDAPI-Missing-Cache-Bytes": str(plan.missing_cache_bytes),
            }
            return StreamingResponse(
                primed_stream(),
                media_type="application/vnd.apache.arrow.stream",
                headers=headers,
            )
        except RequestRejected as exc:
            raise HTTPException(
                status_code=413,
                detail={
                    "code": exc.code,
                    "message": exc.detail,
                    "estimates": exc.estimates,
                },
            ) from exc
        except NoData as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except GatewayError as exc:
            raise HTTPException(
                status_code=413 if exc.status == 413 else 502,
                detail={
                    "code": "gateway_rejected",
                    "message": exc.detail,
                    "gateway_status": exc.status,
                },
            ) from exc
        except NetworkTransferError as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "network_transfer_failed",
                    "message": str(exc),
                    "completed_objects": exc.completed_objects,
                    "remaining_objects": exc.remaining_objects,
                    "retries": exc.retries,
                },
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="启动用户本机 Market Data API")
    parser.add_argument(
        "--config",
        type=Path,
        help=f"JSON配置文件；默认自动读取 {DEFAULT_CLIENT_CONFIG}",
    )
    parser.add_argument("--gateway-host")
    parser.add_argument("--gateway-port", type=int)
    parser.add_argument("--gateway-token")
    parser.add_argument(
        "--cache-root",
        type=Path,
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--io-profile", choices=("hdd", "ssd"), help="远端存储读盘模式；默认 hdd")
    parser.add_argument(
        "--cores",
        type=int,
        help="本机CPU预算；指定1即为严格单核模式",
    )
    parser.add_argument(
        "--max-response-gib",
        type=float,
        default=None,
        help="可选的结果估算硬上限；默认不限制总响应大小",
    )
    parser.add_argument(
        "--arrow-compression",
        choices=("zstd", "lz4", "none"),
    )
    parser.add_argument(
        "--network-retries",
        type=int,
        help="每个未完成对象的网络自动重试次数；默认3",
    )
    parser.add_argument(
        "--network-retry-backoff",
        type=float,
        help="网络重试的初始退避秒数；默认0.25",
    )
    parser.add_argument(
        "--object-request-size",
        type=int,
        help="每个公平调度分段最多对象数；默认12",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = _resolve_config(args)
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError(
            "本机 API 依赖未安装；请安装 market-data-api[api]"
        ) from exc
    limits = ServiceLimits(
        max_response_uncompressed=(
            int(config.max_response_gib * 1024**3)
            if config.max_response_gib is not None
            else None
        ),
        arrow_compression=(
            None
            if config.arrow_compression == "none"
            else config.arrow_compression
        ),
        user_cores=config.cores,
        network_retries=config.network_retries,
        network_retry_backoff=config.network_retry_backoff,
        object_request_size=config.object_request_size,
        read_options=ReadOptions.for_profile(config.io_profile),
    )
    service = DataService(
        gateway_host=config.gateway_host,
        gateway_port=config.gateway_port,
        gateway_token=config.gateway_token,
        cache_root=config.cache_root,
        limits=limits,
    )
    app = create_app(service)
    uvicorn.run(app, host=config.host, port=config.port, workers=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
