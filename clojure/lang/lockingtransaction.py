from threadutil import AtomicInteger
from threading import local as thread_local
from cljexceptions import IllegalStateException

def runInTransaction(fn):
    return fn()

# Possible status values
class TransactionState:
    Running, Committing, Retry, Killed, Committed = range(5)

class Info:
    def __init__(self, status, startPoint):
        self.status = status
        self.startPoint = AtomicInteger(status)

        # TODO Faking a CountDownLatch(1) with a condition variable + killed var
        # self.latch = Lock()
        # self.latch.acquire()

    def running(self):
        status = self.status.get()
        return status == TransactionState.Running or status == TransactionState.Committing

class LockingTransaction():
    transaction = thread_local()

    @classmethod
    def get(cls):
        """
        Returns the per-thread singleton transaction
        """
        if 'data' in cls.transaction.__dict__:
            return cls.transaction.data
        else:
            return None

    @classmethod
    def ensureGet(cls):
        """
        Returns the per-thread singleton transaction, or raises
        an IllegalStateException if one is not running
        """
        if 'data' in cls.transaction.__dict__:
            return cls.transaction.data
        else:
            raise IllegalStateException("No transaction running")

    @classmethod
    def runInTransaction(cls, fn):
        """
        Runs the desired function in this transaction
        """
        transaction = cls.get()
        if not transaction:
            transaction = LockingTransaction()
            cls.transaction.data = transaction
        # TODO