import datetime
import redis
import os
from redis.lock import Lock as RedisClientLock

from . import CreateLock, Lock, LockResource, FailedToAcquireLock

HOST = os.getenv("REDIS_HOST", "redis")
PORT = int(os.getenv("REDIS_PORT", 6379))
DB = int(os.getenv("REDIS_DB", 0))


class RedisLock(Lock):
    """Redis locker class"""

    def __init__(
        self,
        r: redis.Redis,
        resource: LockResource,
        timeout: datetime.timedelta,
        lock: Lock | None = None,
    ) -> None:
        self.resource = resource
        self.lock = lock or RedisLockFactory()(r, resource, timeout)
        super().__init__()

    def acquire(self) -> bool:
        if not self.lock.acquire():
            raise FailedToAcquireLock

    def release(self) -> bool:
        return self.lock.release()


class RedisLockFactory(CreateLock):
    """Class to create redis locks"""

    def __init__(
        self,
        r: redis.Redis,
    ) -> None:
        self.r = r
        super().__init__()

    def __call__(
        self,
        resource: LockResource,
        timeout: datetime.timedelta,
    ) -> RedisClientLock:
        timeout = timeout.microseconds // 1000 + timeout.seconds * 1000
        return self.r.lock(resource.string, timeout, blocking_timeout=0)
