from __future__ import annotations

import collections
import contextlib
import threading
import time
from dataclasses import dataclass


class StreamQueueTimeout(TimeoutError):
    pass


@dataclass(frozen=True)
class StreamLease:
    user_id: str
    queue_ms: float
    borrowed: bool


@dataclass
class _Waiter:
    user_id: str
    queued_at: float
    granted_at: float | None = None
    borrowed: bool = False


class FairStreamScheduler:
    """Work-conserving, per-user round-robin stream admission.

    When at least two users are active or waiting, a user may hold at most one
    stream.  If a single user is alone, it may borrow every otherwise idle
    stream.  Pending requests are rotated by user rather than by connection, so
    opening many connections cannot move one user ahead of everybody else.
    """

    def __init__(
        self,
        max_streams: int,
        *,
        max_pending_per_user: int = 16,
        max_pending: int = 128,
    ) -> None:
        if max_streams < 1:
            raise ValueError("max_streams 必须 >= 1")
        self.max_streams = max_streams
        self.max_pending_per_user = max_pending_per_user
        self.max_pending = max_pending
        self._condition = threading.Condition()
        self._queues: dict[str, collections.deque[_Waiter]] = {}
        self._rotation: collections.deque[str] = collections.deque()
        self._active: collections.Counter[str] = collections.Counter()
        self._active_total = 0

    def _participants_locked(self) -> set[str]:
        return {user_id for user_id, count in self._active.items() if count > 0} | {
            user_id for user_id, values in self._queues.items() if values
        }

    def _eligible_locked(self, user_id: str) -> bool:
        if self._active_total >= self.max_streams:
            return False
        participants = self._participants_locked()
        if len(participants) <= 1:
            return self._active[user_id] < self.max_streams
        return self._active[user_id] == 0

    def _next_user_locked(self) -> str | None:
        for _ in range(len(self._rotation)):
            user_id = self._rotation.popleft()
            values = self._queues.get(user_id)
            if not values:
                continue
            if self._eligible_locked(user_id):
                return user_id
            self._rotation.append(user_id)
        return None

    def _schedule_locked(self) -> None:
        changed = False
        while self._active_total < self.max_streams:
            user_id = self._next_user_locked()
            if user_id is None:
                break
            values = self._queues[user_id]
            waiter = values.popleft()
            if values:
                self._rotation.append(user_id)
            else:
                del self._queues[user_id]
            waiter.borrowed = self._active[user_id] > 0
            self._active[user_id] += 1
            self._active_total += 1
            waiter.granted_at = time.monotonic()
            changed = True
        if changed:
            self._condition.notify_all()

    def _remove_waiter_locked(self, waiter: _Waiter) -> None:
        values = self._queues.get(waiter.user_id)
        if values is None:
            return
        try:
            values.remove(waiter)
        except ValueError:
            return
        if not values:
            del self._queues[waiter.user_id]
            try:
                self._rotation.remove(waiter.user_id)
            except ValueError:
                pass

    def acquire(self, user_id: str, *, timeout: float) -> StreamLease:
        if not user_id:
            raise ValueError("user_id 不能为空")
        if timeout <= 0:
            raise ValueError("timeout 必须 > 0")
        waiter = _Waiter(user_id=user_id, queued_at=time.monotonic())
        deadline = waiter.queued_at + timeout
        with self._condition:
            if (
                len(self._queues.get(user_id, ())) >= self.max_pending_per_user
                or sum(map(len, self._queues.values())) >= self.max_pending
            ):
                raise StreamQueueTimeout("远端等待队列已满，请减少并发请求后重试")
            values = self._queues.setdefault(user_id, collections.deque())
            if not values:
                self._rotation.append(user_id)
            values.append(waiter)
            self._schedule_locked()
            while waiter.granted_at is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._remove_waiter_locked(waiter)
                    self._schedule_locked()
                    raise StreamQueueTimeout(f"用户 {user_id} 等待远端数据流超时")
                self._condition.wait(remaining)
                self._schedule_locked()
        return StreamLease(
            user_id=user_id,
            queue_ms=(waiter.granted_at - waiter.queued_at) * 1000,
            borrowed=waiter.borrowed,
        )

    def release(self, lease: StreamLease) -> None:
        with self._condition:
            if self._active[lease.user_id] < 1:
                raise RuntimeError(f"用户 {lease.user_id} 没有活动数据流")
            self._active[lease.user_id] -= 1
            if self._active[lease.user_id] == 0:
                del self._active[lease.user_id]
            self._active_total -= 1
            self._schedule_locked()
            self._condition.notify_all()

    @contextlib.contextmanager
    def lease(self, user_id: str, *, timeout: float):
        acquired = self.acquire(user_id, timeout=timeout)
        try:
            yield acquired
        finally:
            self.release(acquired)

    def snapshot(self) -> dict[str, int]:
        with self._condition:
            return {
                "active_streams": self._active_total,
                "active_users": len(self._active),
                "queued_requests": sum(len(values) for values in self._queues.values()),
                "queued_users": len(self._queues),
            }
