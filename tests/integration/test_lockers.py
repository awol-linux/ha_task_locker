import logging
from datetime import timedelta
import sys
print(sys.path)
import redis, pytest

from celery import Celery
from libs.lockers.redis import RedisLockFactory
from libs.scheduler import scheduled_task,  shared_scheduled_task
from libs.lockers import FailedToAcquireLock, FailedToReleaseLock
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
    redisLocker = RedisLockFactory(r)

    return redisLocker

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
    assert False



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