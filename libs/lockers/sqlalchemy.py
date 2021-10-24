from datetime import datetime, timedelta

from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, session
from sqlalchemy.sql.sqltypes import DateTime

from . import CreateLock, FailedToAcquireLock, Lock, LockResource

Base = declarative_base()
meta = MetaData()


class LockTable(Base):
    """
    :meta private:
    """
    __tablename__ = "resources"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    resource_name = Column(String, unique=True)
    expire_at = Column(DateTime)


def _create_all(engine):
    return meta.create_all(engine)


class SQLLock(Lock):
    def __init__(self, session, resource, timeout) -> None:
        self.session = session
        self.resource = resource
        self.timeout = timeout
        super().__init__()

    def acquire(self) -> bool:
        self._clear_expired()
        ttl = datetime.now() + self.timeout
        data = LockTable(resource_name=self.resource.string, expire_at=ttl)
        try:
            self.session.add(data)
            self.session.commit()
        except IntegrityError as error:
            self.session.rollback()
            raise FailedToAcquireLock
        return True

    def release(self) -> bool:
        data = LockTable(resource_name=self.resource.string)
        items = (
            self.session.query(LockTable)
            .filter(LockTable.resource_name == self.resource.string)
            .all()
        )
        for item in items:
            self.session.delete(item)
        self.session.commit()
        return True

    def _clear_expired(self):
        items = (
            self.session.query(LockTable)
            .filter(LockTable.expire_at < datetime.now())
            .all()
        )
        for item in items:
            self.session.delete(item)
        self.session.commit()


class SQLLockFacotory(CreateLock):
    def __init__(
        self,
        client: Session,
    ) -> None:
        self.table = client
        super().__init__()

    def __call__(
        self,
        resource: LockResource,
        timeout: timedelta,
    ) -> SQLLock:
        return SQLLock(self.table, resource, timeout)
