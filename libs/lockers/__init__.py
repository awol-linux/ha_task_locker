from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta


class FailedToAcquireLock(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Lock(ABC):
    """
    Base lock class used to acquire and release locks. 
    This lock should be generating using a lock factory. 

    Example:
        This is a axample of acquiring a lock::
        
            if not lock.acquire():
                raise TaskIsLocked(
                    ttl, f"failed to acquire lock"
                )
        A lock can also be used as a context manager::
            
            with lock() as lock:
                print lock

    """
    @abstractmethod
    def acquire(ABC) -> bool:
        """Method to get the lock"""

    @abstractmethod
    def release(ABC) -> bool:
        """Method to release the lock"""

    def __enter__(self):
        return self.acquire()
    
    def __exit__(self, type, value, traceback):
        return self.release()

@dataclass
class LockResource:
    string: str

class CreateLock(ABC):
    """
    Class to create a lock object using factory pattern
    
    Instances of this class are callable and will return a lock
    """
    
    @abstractmethod
    def __call__(
        self,
        resource: LockResource,
        timeout: timedelta,
    ) -> Lock:
        """ 
        Abstract factory used to create the lock 
        
        Args:
            resource: Resource to lock
            timeout: Length of time before the lock gets released
        
        Returns:
            Lock: a callable lock instance
        
        :meta public:
        """






@contextmanager
def lock_context_manager(lock: Lock):
    try:
        if lock.acquire():
            yield lock
    finally:
        lock.release()
