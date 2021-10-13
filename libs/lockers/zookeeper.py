import datetime
import logging

from kazoo.client import KazooClient
from . import CreateLock, FailedToAcquireLock, Lock, LockResource

LOG = logging.getLogger(__name__)

class KazooLease(Lock):
    def __init__(
        self, kz: KazooClient, resource: LockResource, timeout: datetime.timedelta
    ) -> None:
        self.kz = kz
        self.resource = resource
        self.timeout = timeout
        self.timefmt = "%Y-%m-%dT%H:%M:%S"
        self.path = f"/tasks/{self.resource.string}"

    def acquire(self):
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
        self.kz.delete(path=self.path)

class KazooLockFactory(CreateLock):
    """Class to create redis locks"""

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