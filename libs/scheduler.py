"""
Module to task locks Locks
"""
import logging
from datetime import timedelta
from typing import Union

from celery import Celery, shared_task

from .lockers import CreateLock, LockResource

LOG = logging.getLogger(__name__)


class TaskIsLocked(Exception):
    def __init__(self, timeout: timedelta, message: str, *args: object) -> None:
        self.timeout = timeout
        self.message = message
        super().__init__(message, *args)


def scheduled_task(ttl: timedelta, capp: Celery, locker: CreateLock, **lock_kwargs):
    """
    Create a scheduled task locking celery task using a celery app

    Args:
        tts: The length the lock should last for
        capp: The Celery application used to run the task
        locker: The factory used to create lock instances for the object
    """

    def get_task_lock(func):
        LOG.info(
            f"Attempting to run {func.__name__} with locker {locker.__class__.__name__}"
        )
        lock = locker(LockResource(func.__name__), ttl, **lock_kwargs)

        def run_task_if_lock(*args, **kwargs):
            if not lock.acquire():
                LOG.info(
                    f"Successfully locked {func.__name__} with locker {locker.__class__.__name__}"
                )
                raise TaskIsLocked(
                    ttl, f"Not running {func.__name__} since could not acquire lock"
                )
            return capp.task(func)(*args, **kwargs)

        return run_task_if_lock

    return get_task_lock


def shared_scheduled_task(ttl: Union[timedelta], locker: CreateLock, **lock_kwargs):
    """
    Create a scheduled task shared locking celery task

    Args:
        tts: The length the lock should last for
        capp: The Celery application used to run the task
        locker: The factory used to create lock instances for the object
    """

    def get_task_lock(func):
        lock = locker(LockResource(func.__name__), ttl, **lock_kwargs)

        def run_task_if_lock(*args, **kwargs):
            if not lock.acquire():
                LOG.info(
                    f"Successfully locked {func.__name__} with locker {locker.__class__.__name__}"
                )
                raise TaskIsLocked(
                    ttl, f"Not running {func.__name__} since could not acquire lock"
                )
            return shared_task(func)(*args, **kwargs)

        return run_task_if_lock

    return get_task_lock
