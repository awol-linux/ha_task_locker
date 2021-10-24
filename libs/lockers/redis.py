import datetime
import redis
import os
from redis.lock import Lock as RedisClientLock

from . import CreateLock, Lock, LockResource, FailedToAcquireLock

HOST = os.getenv("REDIS_HOST", "redis")
PORT = int(os.getenv("REDIS_PORT", 6379))
DB = int(os.getenv("REDIS_DB", 0))


class RedisLock(Lock):
    """ 
    Redis lease object used to acquire and release locks. 
    This lock should be generating using a RedisLockFactory factory. 

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
        self.lock = lock or RedisLockFactory()(r, resource, timeout)
        super().__init__()

    def acquire(self) -> bool:
        if not self.lock.acquire():
            raise FailedToAcquireLock

    def release(self) -> bool:
        return self.lock.release()


class RedisLockFactory(CreateLock):
    """
    Class to create redis locks
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
    ) -> RedisClientLock:
        timeout = timeout.microseconds * 1000 + timeout.seconds
        print(timeout)
        return self.r.lock(resource.string, timeout, blocking_timeout=0)
