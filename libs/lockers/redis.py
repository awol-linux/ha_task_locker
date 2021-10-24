import datetime
import os

import redis

from . import (CreateLock, FailedToAcquireLock, FailedToReleaseLock, Lock,
               LockResource)


class RedisLock(Lock):
    """
    Redis lease object used to acquire and release locks.
    This lock should be generating using a RedisLockFactory factory.

    Args:
        r: Redis connection to use for locks
        resource: resource to lock
        timeout: length of lock

    Example:

        Create lock resource and TTL and lock::

            In [8]: ttl = datetime.timedelta(seconds=30)

            In [9]: resource = LockResource('test')

            In [10]: lock = rLocker(resource, ttl)

        Using as a context manager::

            In [17]: with lock as lock:
                ...:     print(lock)
                ...:
            True
    """

    def __init__(
        self,
        r: redis.Redis,
        resource: LockResource,
        timeout: datetime.timedelta,
        lock: Lock | None = None,
    ) -> None:
        self.resource = resource
        timeout = timeout.microseconds * 1000 + timeout.seconds
        self.lock = lock or r.lock(resource.name, timeout, blocking_timeout=0)
        super().__init__()

    def acquire(self) -> bool:
        if not self.lock.acquire():
            raise FailedToAcquireLock
        return True

    def release(self) -> bool:
        try:
            self.lock.release()
            return True
        except redis.exceptions.LockError:
            raise FailedToReleaseLock


class RedisLockFactory(CreateLock):
    """
    Factory to create redis locks

    Args:
        r: Redis connection to use for locks
        resource: resource to lock
        timeout: length of lock
    
    Examples:

        Import all necessary imports:: 

             In [1]: from datetime import timedelta

             In [2]: from libs.lockers.redis import RedisLockFactory

             In [3]: import redis

        Create Redis instance and lock factory::

             In [4]: r = redis.from_url("redis://redis:6379/1")
               ...: ttl = timedelta(seconds=30)
               ...: redisLocker = RedisLockFactory(r)
    """
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
    ) -> RedisLock:
        return RedisLock(
            self.r, resource, timeout
        )
