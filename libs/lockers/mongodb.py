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
        if not self.resource.name in self.coll.list_collection_names():
            self.coll.create_collection(self.resource.name)
        collection = self.coll[self.resource.name]
        try:
            collection.insert_one(
                {"_id": self.resource.name, "date": datetime.datetime.utcnow()}
            )
        except DuplicateKeyError as e:
            item = collection.find_one({"_id": self.resource.name})
            if (item["date"] + self.timeout) < datetime.datetime.utcnow():
                collection.find_one_and_delete({"_id": self.resource.name})
                collection.insert_one(
                    {"_id": self.resource.name, "date": datetime.datetime.utcnow()}
                )
                return True
            raise FailedToAcquireLock
        return True

    def release(self) -> bool:
        return bool(
            self.coll[self.resource.name].find_one_and_delete(
                {"_id": self.resource.name}
            )
        )

    @property
    def status(self) -> bool:
        collection = self.coll[self.resource.name]
        item = collection.find_one({"_id": self.resource.name})
        if item is not None:
            if not (item["date"] + self.timeout) < datetime.datetime.utcnow():
                return True
        return False


class MongoLockFactory(CreateLock):
    """Class to create MongoDB locks"""

    def __init__(self, client: Collection) -> None:
        self.coll = client
        super().__init__()

    def __call__(
        self, resource: LockResource, timeout: datetime.timedelta
    ) -> MongoLock:
        return MongoLock(self.coll, resource, timeout)
