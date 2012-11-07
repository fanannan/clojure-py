from threading import Lock, local, currentThread

from clojure.util.shared_lock import SharedLock, shared_lock, unique_lock

def synchronized(f):
    """ Synchronization decorator. """
    lock = Lock()

    def synchronized_closure(*args, **kw):
        lock.acquire()
        try:
            return f(*args, **kw)
        finally:
            lock.release()
    return synchronized_closure


class ThreadLocal(local):
    def __init__(self):
        pass

    def get(self, defaultfn):
        if not hasattr(self, "value"):
            self.value = defaultfn()
        return self.value

    def set(self, value):
        self.value = value


class AtomicInteger(object):
    def __init__(self, v=0):
        self._v = v
        self._lock = SharedLock()

    def get(self):
        with shared_lock(self._lock): return self._v

    def set(self, v):
        with unique_lock(self._lock): self._v = v

    def getAndIncrement(self):
        with unique_lock(self._lock):
            self._v += 1
            return self._v

    def compareAndSet(self, expected, update):
        with unique_lock(self._lock):
            if self._v == expected:
                self._v = update
                return True
            else:
                return False
