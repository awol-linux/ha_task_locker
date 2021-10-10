redis_url = "redis://redis:6379/1"
redbeat_redis_url = redis_url
broker_url = redis_url
# List of modules to import when the Celery worker starts.
imports = "main"

## Using the database to store task state and results.
result_backend = redis_url
redbeat_lock_timeout = 60
