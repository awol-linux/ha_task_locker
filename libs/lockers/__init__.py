from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass


class FailedToAcquireLock(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Lock(ABC):
    """base lock class"""

    @abstractmethod
    def acquire(ABC) -> bool:
        """method to get the lock"""

    @abstractmethod
    def release(ABC) -> bool:
        """method to release the lock"""

    def __enter__(self):
        return self.acquire()
    
    def __exit__(self, type, value, traceback):
        return self.release()


class CreateLock(ABC):
    "class for lock"

    @abstractmethod
    def __init__(self) -> Lock:
        """Function to create a lock object using factory pattern"""


@dataclass
class LockResource:
    string: str


@contextmanager
def lock_context_manager(lock: Lock):
    try:
        if lock.acquire():
            yield lock
    finally:
        lock.release()
