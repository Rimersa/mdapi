from __future__ import annotations

import json
import stat
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_update_users_is_incremental_and_preserves_tokens(tmp_path: Path) -> None:
    path = tmp_path / "users.json"
    tool = PROJECT_ROOT / "scripts" / "update_users.py"
    subprocess.run(
        [sys.executable, str(tool), str(path), "alice", "bob"],
        check=True,
        capture_output=True,
        text=True,
    )
    first = json.loads(path.read_text(encoding="utf-8"))
    subprocess.run(
        [sys.executable, str(tool), str(path), "alice", "carol"],
        check=True,
        capture_output=True,
        text=True,
    )
    second = json.loads(path.read_text(encoding="utf-8"))
    assert second["alice"] == first["alice"]
    assert set(second) == {"alice", "bob", "carol"}
    assert len(set(second.values())) == 3
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


def test_update_users_can_create_locked_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "users.json"
    tool = PROJECT_ROOT / "scripts" / "update_users.py"
    subprocess.run(
        [sys.executable, str(tool), str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(path.read_text(encoding="utf-8")) == {}
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


def test_client_config_is_private_json(tmp_path: Path) -> None:
    path = tmp_path / "client.json"
    tool = PROJECT_ROOT / "scripts" / "write_client_config.py"
    subprocess.run(
        [
            sys.executable,
            str(tool),
            str(path),
            "10.10.10.87",
            "private-token",
        ],
        check=True,
    )
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["gateway_host"] == "10.10.10.87"
    assert raw["gateway_token"] == "private-token"
    assert raw["local_port"] == 18788
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_gateway_upgrade_preserves_operator_settings(tmp_path):
    previous = tmp_path / "gateway.env"
    previous.write_text(
        "MDAPI_GATEWAY_HOST=127.0.0.1\nMDAPI_GATEWAY_PORT=19000\nMDAPI_MAX_STREAMS=3\n"
    )
    output = tmp_path / "new.env"
    tool = PROJECT_ROOT / "scripts" / "write_gateway_config.py"
    subprocess.run(
        [
            sys.executable,
            str(tool),
            str(output),
            str(previous),
            str(tmp_path / "data"),
            str(tmp_path / "users.json"),
            str(tmp_path / "cache" / "footers.sqlite3"),
        ],
        check=True,
    )
    result = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert result["MDAPI_GATEWAY_PORT"] == "19000"
    assert result["MDAPI_MAX_STREAMS"] == "3"
    assert result["MDAPI_DATA_ROOT"] == str(tmp_path / "data")


def test_manage_users_add_rotate_and_remove_preserves_mode(tmp_path: Path) -> None:
    path = tmp_path / "users.json"
    path.write_text("{}\n", encoding="utf-8")
    path.chmod(0o600)
    tool = PROJECT_ROOT / "scripts" / "manage_users.py"

    added = subprocess.run(
        [sys.executable, str(tool), "--file", str(path), "add", "alice"],
        check=True,
        capture_output=True,
        text=True,
    )
    first = json.loads(path.read_text(encoding="utf-8"))["alice"]
    assert first in added.stdout

    listed = subprocess.run(
        [sys.executable, str(tool), "--file", str(path), "list"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert listed.stdout.strip() == "alice"

    subprocess.run(
        [sys.executable, str(tool), "--file", str(path), "rotate", "alice"],
        check=True,
        capture_output=True,
        text=True,
    )
    second = json.loads(path.read_text(encoding="utf-8"))["alice"]
    assert second != first

    subprocess.run(
        [sys.executable, str(tool), "--file", str(path), "remove", "alice"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(path.read_text(encoding="utf-8")) == {}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
