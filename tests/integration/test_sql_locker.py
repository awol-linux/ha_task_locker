from datetime import timedelta
import pytest
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session

from celery import Celery
from sqlalchemy.sql.expression import select
from libs.lockers.sqlalchemy import SQLLockFacotory, _create_all, _drop_all
from libs.scheduler import scheduled_task, shared_scheduled_task
from libs.lockers import FailedToAcquireLock, FailedToReleaseLock
from time import sleep


@pytest.fixture
def app():
    app = Celery()
    app.config_from_object("celeryconfig")
    yield app
    app.close()


@pytest.fixture
def sqllock():
    # Create a zookeeper lock factory for task
    engine = create_engine("postgresql://postgres:postgres@db/postgres")
    session = Session(engine)
    _create_all(engine) 
    sql = SQLLockFacotory(session)
    yield sql
    _drop_all(engine)
    session.close()


def test_zk_scheduled_task_locker(app, sqllock):
    # Create a zk lock factory for task
    ttl = timedelta(seconds=1)

    @scheduled_task(ttl=ttl, capp=app, locker=sqllock)
    def test_zk_scheduled_task():
        return 1 + 1

    test_zk_scheduled_task()
    with pytest.raises(FailedToAcquireLock):
        test_zk_scheduled_task()
    sleep(1)
    test_zk_scheduled_task()


def test_zk_schared_task_locker(sqllock):
    # Create a zk lock factory for task
    ttl = timedelta(seconds=1)

    @shared_scheduled_task(ttl=ttl, locker=sqllock)
    def test_zk_shared_task():
        return 1 + 1

    test_zk_shared_task()
    with pytest.raises(FailedToAcquireLock):
        test_zk_shared_task()
    sleep(1)
    test_zk_shared_task()
