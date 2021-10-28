import logging
from datetime import timedelta

import redis

from celery import Celery
from kazoo.client import KazooClient
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from libs.lockers.sqlalchemy import SQLLockFacotory
from libs.lockers.redis import RedisLockFactory
from libs.lockers.zookeeper import KazooLockFactory
from libs.lockers.mongodb import MongoLockFactory
from libs.scheduler import scheduled_task, shared_scheduled_task

LOG = logging.getLogger(__name__)
## create a celery object to use for app
app = Celery()
app.config_from_object("celeryconfig")

# Create a redis lock factory for task
r = redis.from_url("redis://redis:6379/1")
ttl = timedelta(seconds=30)
redisLocker = RedisLockFactory(r)


@scheduled_task(ttl=ttl, capp=app, locker=redisLocker)
def test_redis_scheduled_task():
    return 1 + 1


@shared_scheduled_task(ttl=ttl, locker=redisLocker)
def test_redis_shared_task():
    return 1 + 1


# Same thing for zookeeper
zk = KazooClient(hosts="zookeeper:2181")
zk.start()

zkLocker = KazooLockFactory(zk)


@scheduled_task(ttl=ttl, capp=app, locker=zkLocker)
def test_zk_scheduled_task():
    return 1 + 1


@shared_scheduled_task(ttl=ttl, locker=zkLocker)
def test_zk_shared_task():
    return 1 + 1


mongo_client = MongoClient("mongodb://mongodb")
table = mongo_client.lock

mo = MongoLockFactory(table)


@scheduled_task(ttl=ttl, capp=app, locker=mo)
def test_mo_scheduled_task():
    return 1 + 1


@shared_scheduled_task(ttl=ttl, locker=mo)
def test_mo_shared_task():
    return 1 + 1


engine = create_engine("postgresql://postgres:postgres@db/postgres")
session = Session(engine)

sql = SQLLockFacotory(session)


@scheduled_task(ttl=ttl, capp=app, locker=sql)
def test_sql_scheduled_task():
    return 1 + 1


@shared_scheduled_task(ttl=ttl, locker=sql)
def test_sql_shared_task():
    return 1 + 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
