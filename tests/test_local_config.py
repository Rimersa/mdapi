from __future__ import annotations

import json

from market_data_api.local_api import _parse_args, _resolve_config


def test_client_json_config_and_cli_precedence(tmp_path, monkeypatch) -> None:
    path = tmp_path / "client.json"
    path.write_text(
        json.dumps(
            {
                "gateway_host": "10.10.10.87",
                "gateway_port": 19000,
                "gateway_token": "file-token",
                "cache_root": str(tmp_path / "cache"),
                "local_host": "127.0.0.1",
                "local_port": 19001,
                "cores": 3,
                "arrow_compression": "lz4",
                "network_retries": 5,
                "network_retry_backoff": 0.5,
                "object_request_size": 8,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("MDAPI_GATEWAY_TOKEN", "env-token")
    args = _parse_args(
        [
            "--config",
            str(path),
            "--port",
            "19002",
            "--cores",
            "1",
        ]
    )
    config = _resolve_config(args)
    assert config.gateway_host == "10.10.10.87"
    assert config.gateway_port == 19000
    assert config.gateway_token == "env-token"
    assert config.port == 19002
    assert config.cores == 1
    assert config.arrow_compression == "lz4"
    assert config.network_retries == 5
    assert config.network_retry_backoff == 0.5
    assert config.object_request_size == 8


def test_missing_default_client_config_uses_safe_defaults(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("MDAPI_GATEWAY_HOST", raising=False)
    monkeypatch.delenv("MDAPI_GATEWAY_TOKEN", raising=False)
    monkeypatch.setattr(
        "market_data_api.local_api.DEFAULT_CLIENT_CONFIG",
        tmp_path / "missing.json",
    )
    config = _resolve_config(_parse_args([]))
    assert config.gateway_host == "10.10.10.87"
    assert config.host == "127.0.0.1"
    assert config.port == 18788
    assert config.arrow_compression == "zstd"
    assert config.network_retries == 3
    assert config.network_retry_backoff == 0.25
    assert config.object_request_size == 12
