from __future__ import annotations

import contextlib
import json
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping
from typing import Any


class MarketDataAPIError(RuntimeError):
    def __init__(self, status: int | None, detail: Any) -> None:
        message = (
            detail
            if isinstance(detail, str)
            else json.dumps(
                detail,
                ensure_ascii=False,
            )
        )
        super().__init__(f"Market Data API {status or 'network'}: {message}")
        self.status = status
        self.detail = detail


class MarketDataClient:
    """Small synchronous client for a user's local FastAPI process."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:18788",
        *,
        timeout: float = 600.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    @classmethod
    def connect(cls, **kwargs):
        """Connect directly to the gateway using the native, persistent client."""
        from .native import RemoteMarketDataClient

        return RemoteMarketDataClient(**kwargs)

    def _request(
        self,
        path: str,
        payload: Mapping[str, Any] | None = None,
        *,
        accept: str | None = None,
    ):
        body = None
        method = "GET"
        headers = {"Accept-Encoding": "identity"}
        if payload is not None:
            body = json.dumps(dict(payload), ensure_ascii=False).encode("utf-8")
            method = "POST"
            headers["Content-Type"] = "application/json"
        if accept:
            headers["Accept"] = accept
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers=headers,
        )
        try:
            return urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                detail = json.loads(raw)
            except Exception:
                detail = raw.decode("utf-8", "replace")
            raise MarketDataAPIError(exc.code, detail) from exc
        except urllib.error.URLError as exc:
            raise MarketDataAPIError(None, str(exc.reason)) from exc

    def health(self) -> dict[str, Any]:
        with self._request("/health") as response:
            return json.load(response)

    def estimate(self, query: Mapping[str, Any]) -> dict[str, Any]:
        with self._request("/v1/estimate", query) as response:
            result = json.load(response)
            if query.get("symbols") is not None and not result.get("symbols_supported"):
                raise MarketDataAPIError(
                    409,
                    "本机 API 尚未支持股票过滤；请升级并重启 mdapi-local，或使用 MarketDataClient.connect()",
                )
            return result

    @contextlib.contextmanager
    def open_stream(self, query: Mapping[str, Any]):
        response = self._request(
            "/v1/data",
            query,
            accept="application/vnd.apache.arrow.stream",
        )
        reader = None
        try:
            if (
                query.get("symbols") is not None
                and response.headers.get("X-MDAPI-Symbols-Applied") != "true"
            ):
                raise MarketDataAPIError(
                    409,
                    "本机 API 没有确认股票过滤，已拒绝返回可能包含全市场的数据；请升级本机服务",
                )
            if (
                "read_strategy" in query
                and response.headers.get("X-MDAPI-Read-Path") is None
            ):
                raise MarketDataAPIError(
                    409, "本机 API 尚未支持 read_strategy，请升级本机服务"
                )
            content_type = response.headers.get_content_type()
            if content_type != "application/vnd.apache.arrow.stream":
                raw = response.read()
                raise MarketDataAPIError(
                    response.status,
                    f"意外响应类型 {content_type}: {raw[:500]!r}",
                )
            try:
                import pyarrow.ipc as ipc
            except ImportError as exc:
                raise RuntimeError(
                    "读取Arrow数据需要安装 market-data-api[client]"
                ) from exc
            try:
                reader = ipc.open_stream(response)
            except (OSError, EOFError) as exc:
                raise MarketDataAPIError(
                    None,
                    f"Arrow数据流中断或不完整: {exc}",
                ) from exc
            yield reader
        finally:
            if reader is not None:
                reader.close()
            response.close()

    def iter_batches(self, query: Mapping[str, Any]) -> Iterator[Any]:
        with self.open_stream(query) as reader:
            try:
                yield from reader
            except MarketDataAPIError:
                raise
            except Exception as exc:
                raise MarketDataAPIError(
                    None,
                    f"Arrow数据流中断或不完整: {exc}",
                ) from exc

    def read_table(self, query: Mapping[str, Any]):
        with self.open_stream(query) as reader:
            try:
                return reader.read_all()
            except MarketDataAPIError:
                raise
            except Exception as exc:
                raise MarketDataAPIError(
                    None,
                    f"Arrow数据流中断或不完整: {exc}",
                ) from exc
