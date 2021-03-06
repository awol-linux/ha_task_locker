import logging
from datetime import timedelta
import sys

print(sys.path)
import redis, pytest

from celery import Celery
from libs.lockers.redis import RedisLock, RedisLockFactory
from libs.scheduler import scheduled_task, shared_scheduled_task
from libs.lockers import FailedToAcquireLock, FailedToReleaseLock, Lock, LockResource
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
def rlock(redislocker):
    ttl = timedelta(seconds=1)
    lock: Lock = redislocker(resource=LockResource("test"), timeout=ttl)
    return lock


def test_redis_scheduled_task_locker(app, redislocker):
    # Create a redis lock factory for task
    ttl = timedelta(seconds=1)

    @scheduled_task(ttl=ttl, capp=app, locker=redislocker)
    def test_redis_scheduled_task():
        return 1 + 1

    test_redis_scheduled_task()
    with pytest.raises(FailedToAcquireLock):
        test_redis_scheduled_task()
    sleep(1)
    test_redis_scheduled_task()


def test_redis_schared_task_locker(app, redislocker):
    # Create a redis lock factory for task
    ttl = timedelta(seconds=1)

    @shared_scheduled_task(ttl=ttl, locker=redislocker)
    def test_redis_shared_task():
        return 1 + 1

    test_redis_shared_task()
    with pytest.raises(FailedToAcquireLock):
        test_redis_shared_task()
    sleep(1)
    test_redis_shared_task()


def test_lock_status(rlock: RedisLock):
    # Create a zk lock factory for task
    rlock.acquire()
    assert rlock.status
    rlock.release()
    assert not rlock.status
    rlock.acquire()
    sleep(1)
    assert not rlock.status


def test_lock_context_manager(rlock: RedisLock):
    sleep(1)
    with rlock:
        assert rlock.status
    assert not rlock.status


def test_raises_failed_to_release(rlock: RedisLock):
    with pytest.raises(FailedToReleaseLock):
        rlock.release()
