import logging
from datetime import timedelta
import sys

print(sys.path)
import redis, pytest

from celery import Celery
from libs.lockers.redis import RedisLockFactory
from libs.scheduler import scheduled_task, shared_scheduled_task
from libs.lockers import FailedToAcquireLock, FailedToReleaseLock, LockResource
from libs.lockers.zookeeper import KazooLease, KazooLockFactory
from libs.lockers.quorom import QuoromLock, QuoromLockFactory
from libs.lockers.mongodb import MongoLock, MongoLockFactory
from pymongo.mongo_client import MongoClient
from unittest import mock

from kazoo.client import KazooClient

from time import sleep


@pytest.fixture
def app():
    app = Celery()
    app.config_from_object("celeryconfig")
    return app


@pytest.fixture
def redislocker():
    # Create a redis lock factory for task
    r = redis.from_url("redis://redis:6379/1")
    r.flushall()
    redisLocker = RedisLockFactory(r)

    yield redisLocker
    r.close()


@pytest.fixture
def zkfactory():
    # Create a zookeeper lock factory for task
    zk = KazooClient(hosts="zookeeper:2181")
    zk.start()

    zklocker = KazooLockFactory(zk)
    yield zklocker
    zk.stop()
    zk.close()


@pytest.fixture
def mongodb():
    # Create a zookeeper lock factory for task
    mongo_client = MongoClient("mongodb://mongodb")
    table = mongo_client.lock
    mongo_client.drop_database("lock")
    mo = MongoLockFactory(table)
    yield mo
    mongo_client.close()


@pytest.fixture
def quorom_lock(zkfactory, redislocker, mongodb):
    return QuoromLockFactory([zkfactory, redislocker, mongodb])


@pytest.fixture
def lock(quorom_lock):
    ttl = timedelta(seconds=1)
    lock: QuoromLock = quorom_lock(LockResource("test"), ttl)
    return lock


def test_quorom_scheduled_task_locker(app, quorom_lock):
    # Create a quorom lock factory for task
    ttl = timedelta(seconds=1)

    @scheduled_task(ttl=ttl, capp=app, locker=quorom_lock)
    def test_quorom_scheduled_task():
        return 1 + 1

    test_quorom_scheduled_task()
    with pytest.raises(FailedToAcquireLock):
        test_quorom_scheduled_task()
    sleep(1)
    test_quorom_scheduled_task()


def test_quorom_schared_task_locker(app, quorom_lock):
    # Create a quorom lock factory for task
    ttl = timedelta(seconds=1)

    @shared_scheduled_task(ttl=ttl, locker=quorom_lock)
    def test_quorom_shared_task():
        return 1 + 1

    test_quorom_shared_task()
    with pytest.raises(FailedToAcquireLock):
        test_quorom_shared_task()
    sleep(1)
    test_quorom_shared_task()


def test_quorom_acquire(quorom_lock):
    ttl = timedelta(seconds=1)
    lock: QuoromLock = quorom_lock(LockResource("test"), ttl)
    lock.acquire()
    with pytest.raises(FailedToAcquireLock):
        lock.acquire()
    sleep(1)
    lock.acquire()
    lock.locks[0].release()
    with pytest.raises(FailedToAcquireLock):
        lock.acquire()
    lock.locks[1].release()
    lock.locks[0].release()
    lock.acquire()


def test_quorom_release(lock, caplog):
    # test base release
    lock.acquire()


def test_mostly_locked(lock, caplog):
    lock.acquire()
    lock.locks[0].release()
    lock.release


def test_mostly_unlocked_release(lock):
    lock.acquire()
    lock.locks[1].release()
    lock.locks[0].release()
    with pytest.raises(FailedToReleaseLock):
        lock.release()


def test_fail_unlock(lock):
    with mock.patch.object(KazooLease, "release") as kazoomock:
        kazoomock.side_effect = FailedToReleaseLock()
        with mock.patch.object(MongoLock, "release") as mongomock:
            mongomock.side_effect = FailedToReleaseLock()
            lock.acquire()
            with pytest.raises(FailedToReleaseLock):
                lock.release()


def test_lock_status(quorom_lock):
    # Create a zk lock factory for task
    ttl = timedelta(seconds=1)
    lock = quorom_lock(resource=LockResource("test"), timeout=ttl)
    lock.acquire()
    assert lock.status
    lock.release()
    assert not lock.status
    lock.acquire()
    sleep(1)
    assert not lock.status
