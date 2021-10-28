from datetime import timedelta
from kazoo.client import KazooClient
import pytest

from celery import Celery
from libs.lockers.zookeeper import KazooLockFactory
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
def zkfactory():
    # Create a zookeeper lock factory for task
    zk = KazooClient(hosts="zookeeper:2181")
    zk.start()

    zklocker = KazooLockFactory(zk)
    yield zklocker
    zk.stop()
    zk.close()


def test_zk_scheduled_task_locker(app, zkfactory):
    # Create a zk lock factory for task
    ttl = timedelta(seconds=1)

    @scheduled_task(ttl=ttl, capp=app, locker=zkfactory)
    def test_zk_scheduled_task():
        return 1 + 1

    test_zk_scheduled_task()
    with pytest.raises(FailedToAcquireLock):
        test_zk_scheduled_task()
    sleep(1)
    test_zk_scheduled_task()


def test_zk_schared_task_locker(app, zkfactory):
    # Create a zk lock factory for task
    ttl = timedelta(seconds=1)

    @shared_scheduled_task(ttl=ttl, locker=zkfactory)
    def test_zk_shared_task():
        return 1 + 1

    test_zk_shared_task()
    with pytest.raises(FailedToAcquireLock):
        test_zk_shared_task()
    sleep(1)
    test_zk_shared_task()
