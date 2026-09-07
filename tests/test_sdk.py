from __future__ import annotations

import io
from email.message import Message

import pyarrow as pa
import pytest

from market_data_api.sdk import MarketDataClient, MarketDataAPIError


class _ArrowResponse(io.BytesIO):
    status = 200

    def __init__(self, raw: bytes) -> None:
        super().__init__(raw)
        self.headers = Message()
        self.headers["Content-Type"] = "application/vnd.apache.arrow.stream"


def test_sdk_streams_batches_and_can_materialize(monkeypatch) -> None:
    table = pa.table({"symbol": ["A", "B"], "value": [1, 2]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    raw = sink.getvalue().to_pybytes()
    client = MarketDataClient()
    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: _ArrowResponse(raw),
    )

    batches = list(client.iter_batches({"dataset": "snapshots"}))
    assert pa.Table.from_batches(batches).equals(table)
    assert client.read_table({"dataset": "snapshots"}).equals(table)


def test_sdk_rejects_old_proxy_ignoring_symbols(monkeypatch):
    client = MarketDataClient()
    response = _ArrowResponse(b"not read")
    monkeypatch.setattr(client, "_request", lambda *_args, **_kwargs: response)
    with pytest.raises(MarketDataAPIError) as caught:
        client.read_table({"symbols": ["A"]})
    assert caught.value.status == 409
    assert response.closed
