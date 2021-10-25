from datetime import timedelta
from kazoo.client import KazooClient
from pymongo.mongo_client import MongoClient
import pytest

from celery import Celery
from libs.lockers.mongodb import MongoLockFactory
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
def mongodb():
    # Create a zookeeper lock factory for task
    mongo_client = MongoClient("mongodb://mongodb")
    table = mongo_client.lock
    mongo_client.drop_database("lock")
    mo = MongoLockFactory(table)
    yield mo
    mongo_client.close()


def test_zk_scheduled_task_locker(app, mongodb):
    # Create a zk lock factory for task
    ttl = timedelta(seconds=1)

    @scheduled_task(ttl=ttl, capp=app, locker=mongodb)
    def test_zk_scheduled_task():
        return 1 + 1

    test_zk_scheduled_task()
    with pytest.raises(FailedToAcquireLock):
        test_zk_scheduled_task()
    sleep(1)
    test_zk_scheduled_task()


def test_zk_schared_task_locker(app, mongodb):
    # Create a zk lock factory for task
    ttl = timedelta(seconds=1)

    @shared_scheduled_task(ttl=ttl, locker=mongodb)
    def test_zk_shared_task():
        return 1 + 1

    test_zk_shared_task()
    with pytest.raises(FailedToAcquireLock):
        test_zk_shared_task()
    sleep(1)
    test_zk_shared_task()
