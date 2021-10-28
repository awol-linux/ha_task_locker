[![Documentation Status](https://readthedocs.org/projects/ha-task-locker/badge/?version=latest)](https://ha-task-locker.readthedocs.io/en/latest/?badge=latest)
# Aighly Available Application Ratelimiter

## Creation
___

### Create a celery object to use for app
```python
app = Celery()
app.config_from_object("celeryconfig")
```
### Create a lock factory for task we use redis for this here

```python
r = redis.from_url("redis://redis:6379/1")
ttl = timedelta(seconds=30)
redisLocker = RedisLockFactory(r)
```
### Use python decorators to writing the tasks

```python
@scheduled_task(ttl=ttl, capp=app, locker=redisLocker)
def test_redis_scheduled_task():
    return 1 + 1

@shared_scheduled_task(ttl=ttl, locker=redisLocker)
def test_redis_shared_task():
    return 1 + 1
```
## Usage
___
```python
In [1]: import example

In [2]: example.test_zk_scheduled_task()
Out[2]: 2

In [3]: example.test_zk_scheduled_task()
---------------------------------------------------------------------------
FailedToAcquireLock                       Traceback (most recent call last)
<ipython-input-3-928fa9c615a8> in <module>
----> 1 example.test_zk_scheduled_task()

/workdir/libs/scheduler.py in run_task_if_lock(*args, **kwargs)
     29
     30         def run_task_if_lock(*args, **kwargs):
---> 31             if not lock.acquire():
     32                 LOG.info(
     33                     f"Successfully locked {func.__name__} with locker {locker.__class__.__name__}"

/workdir/libs/lockers/zookeeper.py in acquire(self)
     27                 > now
     28             ):
---> 29                 raise FailedToAcquireLock
     30         self.kz.set(
     31             self.path,

FailedToAcquireLock:

In [4]:
```
