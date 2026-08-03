from __future__ import annotations

import threading
import time

import pytest

from market_data_api.catalog import Catalog, atomic_write_json
from market_data_api.gateway import GatewayState, _load_user_tokens
from market_data_api.scheduler import FairStreamScheduler, StreamQueueTimeout


def _wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("等待条件超时")


def test_single_user_borrows_idle_stream() -> None:
    scheduler = FairStreamScheduler(2)
    first = scheduler.acquire("alice", timeout=1)
    second = scheduler.acquire("alice", timeout=1)
    assert not first.borrowed
    assert second.borrowed
    assert scheduler.snapshot() == {
        "active_streams": 2,
        "active_users": 1,
        "queued_requests": 0,
        "queued_users": 0,
    }
    scheduler.release(second)
    scheduler.release(first)


def test_four_users_rotate_before_existing_users_repeat() -> None:
    scheduler = FairStreamScheduler(2)
    active_a = scheduler.acquire("alice", timeout=1)
    active_b = scheduler.acquire("bob", timeout=1)
    granted: list[tuple[str, bool]] = []
    granted_lock = threading.Lock()
    releases = {
        user_id: threading.Event()
        for user_id in ("carol", "dave", "alice", "bob")
    }

    def queued_request(user_id: str) -> None:
        lease = scheduler.acquire(user_id, timeout=2)
        with granted_lock:
            granted.append((user_id, lease.borrowed))
        releases[user_id].wait(2)
        scheduler.release(lease)

    threads = []
    for index, user_id in enumerate(("carol", "dave", "alice", "bob"), 1):
        thread = threading.Thread(target=queued_request, args=(user_id,))
        thread.start()
        threads.append(thread)
        _wait_for(lambda: scheduler.snapshot()["queued_requests"] == index)

    scheduler.release(active_a)
    _wait_for(lambda: len(granted) >= 1)
    assert granted[0] == ("carol", False)

    scheduler.release(active_b)
    _wait_for(lambda: len(granted) >= 2)
    assert granted[1] == ("dave", False)

    releases["carol"].set()
    _wait_for(lambda: len(granted) >= 3)
    assert granted[2] == ("alice", False)

    releases["dave"].set()
    _wait_for(lambda: len(granted) >= 4)
    assert granted[3] == ("bob", False)

    releases["alice"].set()
    releases["bob"].set()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    assert scheduler.snapshot()["active_streams"] == 0


def test_queue_timeout_removes_waiter() -> None:
    scheduler = FairStreamScheduler(1)
    active = scheduler.acquire("alice", timeout=1)
    with pytest.raises(StreamQueueTimeout):
        scheduler.acquire("bob", timeout=0.01)
    assert scheduler.snapshot()["queued_requests"] == 0
    scheduler.release(active)


def test_per_user_tokens_define_fairness_identity(tmp_path) -> None:
    root = tmp_path / "remote"
    root.mkdir()
    atomic_write_json(root / "catalog.json", Catalog.empty().as_dict())
    token_file = tmp_path / "tokens.json"
    token_file.write_text(
        '{"alice":"secret-a","bob":"secret-b"}',
        encoding="utf-8",
    )
    tokens = _load_user_tokens(token_file)
    state = GatewayState(
        root,
        token=None,
        user_tokens=tokens,
        max_streams=2,
        max_objects=100,
        queue_timeout=1,
    )
    assert state.authenticate("Bearer secret-a", "10.0.0.1") == "alice"
    assert state.authenticate("secret-b", "10.0.0.1") == "bob"
    assert state.authenticate("wrong", "10.0.0.1") is None
