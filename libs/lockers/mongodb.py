import datetime
import logging
from re import T
from pymongo.database import Collection
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from . import CreateLock, FailedToAcquireLock, Lock, LockResource

LOG = logging.getLogger(__name__)


class MongoLock(Lock):
    def __init__(
        self, coll: Collection, resource: LockResource, timeout: datetime.timedelta
    ) -> None:
        self.coll = coll
        self.resource = resource
        self.timeout = timeout
        super().__init__()

    def acquire(self) -> bool:
        if not self.resource.string in self.coll.list_collection_names():
            self.coll.create_collection(self.resource.string)
        collection = self.coll[self.resource.string]
        try:
            collection.insert_one(
                {"_id": self.resource.string, "date": datetime.datetime.utcnow()}
            )
            collection.drop_indexes()
            collection.create_index("date", expireAfterSeconds=self.timeout.seconds)
        except DuplicateKeyError as e:
            raise FailedToAcquireLock
        return True

    def release(self) -> bool:
        return bool(
            self.coll[self.resource.string].find_one_and_delete(
                {"_id": self.resource.string}
            )
        )


class MongoLockFactory(CreateLock):
    """Class to create MongoDB locks"""

    def __init__(
        self,
        client: Collection,
    ) -> None:
        self.coll = client
        super().__init__()

    def __call__(
        self,
        resource: LockResource,
        timeout: datetime.timedelta,
    ) -> MongoLock:
        return MongoLock(self.coll, resource, timeout)
