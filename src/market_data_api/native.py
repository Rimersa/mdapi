"""Native Python client: persistent gateway connections and direct Arrow batches."""

from __future__ import annotations

import contextlib
import itertools
import json
import threading
from dataclasses import replace
from pathlib import Path

from .client import GatewayError
from .model import DataRequest
from .sdk import MarketDataAPIError
from .selective import ReadOptions
from .service import DataService, LocalMemoryExhausted, NoData, RequestRejected, ServiceLimits


class RemoteMarketDataClient:
    """Share one instance between reads; close it after all readers have finished."""

    def __init__(self, *, gateway_host=None, gateway_port=None, gateway_token=None,
                 config=None, cache_root=None, cores=None, read_options=None,
                 network_retries=3, network_retry_backoff=0.25, connections=2, io_profile="hdd"):
        path = Path(config).expanduser() if config else Path.home() / ".config/market-data-api/client.json"
        if config is not None and not path.exists():
            raise FileNotFoundError(f"客户端配置不存在: {path}")
        raw = json.loads(path.read_text()) if path.exists() else {}
        if not isinstance(raw, dict):
            raise ValueError("客户端配置必须是 JSON 对象")
        host = gateway_host if gateway_host is not None else raw.get("gateway_host", "10.10.10.87")
        port = gateway_port if gateway_port is not None else raw.get("gateway_port", 18787)
        same_origin = (host, int(port)) == (raw.get("gateway_host"), int(raw.get("gateway_port", 18787)))
        token = gateway_token if gateway_token is not None else raw.get("gateway_token") if same_origin else None
        cache = Path(cache_root or raw.get("cache_root", Path.home() / ".cache/market-data-api")).expanduser()
        defaults = ReadOptions.for_profile(io_profile)
        options = replace(defaults, **read_options) if isinstance(read_options, dict) else read_options or defaults
        self.service = DataService(gateway_host=host, gateway_port=int(port), gateway_token=token,
            cache_root=cache, limits=ServiceLimits(user_cores=cores, gateway_connections=connections,
                read_options=options, network_retries=network_retries,
                network_retry_backoff=network_retry_backoff))
        self._local = threading.local()
        self._closed = False

    def _check_open(self):
        if self._closed:
            raise RuntimeError("客户端已经关闭")

    @property
    def last_read_stats(self):
        stats = getattr(self._local, "stats", None)
        return stats.as_dict() if stats is not None else None

    @contextlib.contextmanager
    def _errors(self):
        try:
            yield
        except MarketDataAPIError:
            raise
        except RequestRejected as exc:
            raise MarketDataAPIError(413, {"code": exc.code, "message": exc.detail, "estimates": exc.estimates}) from exc
        except NoData as exc:
            raise MarketDataAPIError(404, str(exc)) from exc
        except LocalMemoryExhausted as exc:
            raise MarketDataAPIError(503, str(exc)) from exc
        except GatewayError as exc:
            raise MarketDataAPIError(502, {"gateway_status": exc.status, "message": exc.detail}) from exc
        except (ValueError, TypeError) as exc:
            raise MarketDataAPIError(422, str(exc)) from exc
        except (OSError, EOFError, TimeoutError) as exc:
            raise MarketDataAPIError(None, str(exc)) from exc

    def health(self):
        self._check_open()
        with self._errors(), self.service.pool.connection() as connection:
            response = connection._response("GET", "/health")
            return json.loads(response.read())

    def estimate(self, query):
        self._check_open()
        with self._errors():
            return self.service.preflight(DataRequest.from_query(query)).as_dict()

    def iter_batches(self, query):
        self._check_open()
        batches = None
        with self._errors():
            try:
                request = DataRequest.from_query(query)
                plan = self.service.preflight(request)
                self._local.stats = plan.stats
                _, batches = self.service.batches(request, plan)
                yield from batches
            finally:
                if batches is not None:
                    batches.close()

    @contextlib.contextmanager
    def open_stream(self, query):
        import pyarrow as pa
        batches = self.iter_batches(query)
        reader = None
        try:
            first = next(batches)
            reader = pa.RecordBatchReader.from_batches(first.schema, itertools.chain((first,), batches))
            yield reader
        finally:
            if reader is not None:
                reader.close()
            batches.close()

    def read_table(self, query):
        with self.open_stream(query) as reader:
            return reader.read_all()

    def close(self):
        if not self._closed:
            self.service.close()
            self._closed = True

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, *_):
        self.close()
