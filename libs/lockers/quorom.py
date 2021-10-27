import datetime
from typing import Counter, List
import redis
import os
import logging
from redis.lock import Lock as RedisClientLock

from . import CreateLock, FailedToReleaseLock, Lock, LockResource, FailedToAcquireLock, UnknownLockStatus

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
        
        Examples:

            Acquire the lock::

                In [50]: lock.acquire()
                Out[50]: True
        """
        lock_status: Counter = Counter()
        for lock in self.locks:
            try:
                lock_status.update([lock.acquire()])
            except FailedToAcquireLock as e:
                lock_status.update([False])
                LOG.error(
                    f"Failed to lock {self.resource.name} with {lock}: {e}"
                )
        if not lock_status[True] > lock_status[False]:
            for lock in self.locks:
                try:
                    lock.acquire()
                except FailedToAcquireLock:
                    pass
            raise FailedToAcquireLock
        return True

    def release(self) -> bool:
        """
        Releaes the lock
        It will try to release a lock on each of the locks it has.
        If it fails to get majoraty of the locks then it will try to reacquire all the locks and try again raise a :class:`libs.lockers.FailedToAcquireLock`
        
        Raises:
            libs.lockers.FailedToAcquireLock
        
        Returns:
            bool
        
        Examples:

            Acquire the lock::

                In [50]: lock.acquire()
                Out[50]: True
        """
        def retry_release():
            lock_status: Counter = Counter()
            for lock in self.locks:
                print(lock)
                try:
                    lock_status.update([lock.release()])
                except FailedToReleaseLock as e:
                    lock_status.update([False])
                    LOG.error(f"Failed to Unlock {self.resource.name} with {lock}: {e}") 
            print(lock_status)
            if not lock_status[True] > lock_status[False]:
                try:
                    self.acquire()
                except FailedToAcquireLock:
                    raise UnknownLockStatus
        try: 
            return retry_release()
        except UnknownLockStatus:
            return retry_release()




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

"""
from libs.lockers import LockResource
from libs.lockers.quorom import QuoromLockFactory
from datetime import timedelta
from example import zkLocker, redisLocker
from example import zkLocker, redisLocker

ttl, resource = timedelta(seconds=30), LockResource('test')
lockers = [zkLocker, redisLocker]

qlocker = QuoromLockFactory(lockers)
lock = qlocker(resource, ttl)
"""