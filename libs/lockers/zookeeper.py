import datetime
import logging

from kazoo.client import KazooClient
from . import CreateLock, FailedToAcquireLock, Lock, LockResource

LOG = logging.getLogger(__name__)

class KazooLease(Lock):
    """ 
    Kazoo lease object used to acquire and release locks. 
    This lock should be generating using a KazooLockFactory factory. 

    Example:
        
        Create lock resource and TTL and lock::

            In [8]: ttl = datetime.timedelta(seconds=30)

            In [9]: resource = LockResource('test')

            In [10]: lock = zkLocker(resource, ttl)
            
        Using as a context manager::

            In [17]: with lock as lock:
                ...:     print(lock)
                ...: 
            True

    
    """
    def __init__(
        self, kz: KazooClient, resource: LockResource, timeout: datetime.timedelta
    ) -> None:
        self.kz = kz
        self.resource = resource
        self.timeout = timeout
        self.timefmt = "%Y-%m-%dT%H:%M:%S"
        self.path = f"/tasks/{self.resource.string}"

    def acquire(self) -> bool :
        """
        Method to acqure lock


        Raises:
            FailedToAcquireLock: Failed to acquire Lock resource
        
        Returns:
            True when the lock was acquired
        

        Examples:

            Acquire the lock ::
        
                In [11]: lock.acquire()
                Out[11]: True
        
            If the lock exists it will raise a FailedToAcquireLock::

                In [12]: lock.acquire()
                 ---------------------------------------------------------------------------
                 FailedToAcquireLock                       Traceback (most recent call last)
                 <ipython-input-13-7d28dc795612> in <module>
                 ----> 1 lock.acquire()

                 /workdir/libs/lockers/zookeeper.py in acquire(self)
                      45                 > now
                      46             ):
                 ---> 47                 raise FailedToAcquireLock
                      48         self.kz.set(
                      49             self.path,

                 FailedToAcquireLock:

        """
        
        now = datetime.datetime.now()
        self.kz.ensure_path(self.path)
        LOG.debug(f"ensuring path {self.path}")
        current_lock, _ = self.kz.get(path=self.path)
        if current_lock != b"":
            if (
                datetime.datetime.strptime(current_lock.decode("utf-8"), self.timefmt)
                > now
            ):
                raise FailedToAcquireLock
        self.kz.set(
            self.path,
            datetime.datetime.strftime(now + self.timeout, self.timefmt).encode(
                "utf-8"
            ),
        )
        return True

    def release(self):
        """
        Release the lock

        Examples:

            Release the Lock::

                In [14]: lock.release()
        """
        self.kz.delete(path=self.path)

class KazooLockFactory(CreateLock):
    """
    Class to create Kazoo locks
    
    Instances of this class are callable and will return a lock

    Example:

        Import all necessary imports:: 

            In [1]: from libs.lockers.zookeeper import KazooLockFactory

            In [2]: from kazoo.client import KazooClient

            In [3]: from libs.lockers import LockResource

            In [4]: import datetime

        Create ZooKeeper instance and lock factory::

            In [5]: zk = KazooClient(hosts='zookeeper:2181')

            In [6]: zk.start()

            In [7]: zkLocker = KazooLockFactory(zk)
        """

    def __init__(
        self,
        client: KazooClient,
    ) -> None:
        self.kz = client
        super().__init__()

    def __call__(
        self,
        resource: LockResource,
        timeout: datetime.timedelta,
    ) -> KazooLease:
        self.kz.ensure_path("/tasks")
        return KazooLease(self.kz, resource, timeout)