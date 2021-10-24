import datetime
from typing import Counter, List
import redis
import os
import logging
from redis.lock import Lock as RedisClientLock

from . import CreateLock, Lock, LockResource, FailedToAcquireLock

LOG = logging.getLogger(__name__)


class QuoromLock(Lock):
    """
    Quorom lease object used to acquire and release locks.
    This lock should be generating using a :class:`libs.lockers.quorom.QuoromLockFactory` factory.

    Args:
        locks: List of lock objects
        resource: resource to lock
        timeout: length of lock 
            this is not used because the lock factory should
            be passing this data to all the locks

    Example:

        Using as a context manager::

            In [17]: with lock as lock:
                ...:     print(lock)
                ...:
            True
    """

    def __init__(
        self,
        locks: List[Lock],
        resource: LockResource,
        timeout: datetime.timedelta,
    ) -> None:
        self.resource = resource
        self.locks = locks
        super().__init__()

    def acquire(self) -> bool:
        """
        Acqure the lock
        It will try to acquire a lock on each of the locks it has.
        If it fails to get majoraty of the locks then it will raise a :class:`libs.lockers.FailedToAcquireLock`
        
        Raises:
            libs.lockers.FailedToAcquireLock
        
        Returns:
            bool
        """
        lock_status: Counter = Counter()
        for lock in self.locks:
            try:
                lock_status.update([lock.acquire()])
            except FailedToAcquireLock as e:
                lock_status.update([False])
                LOG.error(
                    f"Failed to lock {self.resource.string} with {lock.__class__.__name__}"
                )
            if not lock_status[True] > lock_status[False]:
                raise FailedToAcquireLock
        return True

    def release(self) -> bool:
        return self.lock.release()


class QuoromLockFactory(CreateLock):
    """
    Factory to create quorom locks

    Args:
        lockers: list of :class:`libs.lockers.CreateLock` connection to use for locks`

    Examples:
        Create multiple instances of :class:`libs.lockers.CreateLock` 
        in this example we use :class:`libs.lockers.zookeeper.KazooLockFactory` and :class:`libs.lockers.redis.RedisLockFactory`::

            In [1]: lockers = [zkLocker, redisLocker]

            In [2]: qlocker = QuoromLockFactory(lockers)
    """

    def __init__(
        self,
        lockers: List[CreateLock],
    ) -> None:
        self.lockers = lockers
        super().__init__()

    def __call__(
        self,
        resource: LockResource,
        timeout: datetime.timedelta,
    ) -> QuoromLock:
        """
        Create Lock instance

        Args:
            resource: resource to lock
            timeout: length of lock
        
        Returns: A Quorom Lock
        
        Examples:

            Create a lock instance::

                In [3]: lock = qlocker(resource, ttl)

                In [4]: type(lock)
                Out[4]: libs.lockers.quorom.QuoromLock
        """

        return QuoromLock([lock(resource, timeout) for lock in self.lockers], resource, timeout)
