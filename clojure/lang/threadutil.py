from threading import Lock, local, currentThread

from clojure.util.shared_lock import SharedLock

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


class ThreadLocal(local, object):
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
        self._lock.acquire_shared()
        val = self._v
        self._lock.release_shared()
        return val

    def set(self, v):
        self._lock.acquire()
        self._v = v
        self._lock.release()

    def getAndIncrement(self):
        self._lock.acquire()
        self._v += 1
        val = self._v
        self._lock.release()
        return val

    def compareAndSet(self, expected, update):
        self._lock.acquire()
        if self._v == expected:
            self._v = update
            success = True
        else:
            success = False
        self._lock.release()
        return success
