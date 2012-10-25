from cljexceptions import IllegalStateException

from itertools import count
from threadutil import AtomicInteger
from threading import local as thread_local
from threading import Lock

# Possible status values
class TransactionState:
    Running, Committing, Retry, Killed, Committed = range(5)

class Info:
    def __init__(self, status, startPoint):
        self.status = AtomicInteger(status)
        self.startPoint = status
        # We need to synchronize access to status+latch in stop()
        self.lock = Lock()

        # TODO Faking a CountDownLatch(1) with a condition variable + killed var
        # self.latch = Lock()
        # self.latch.acquire()

    def running(self):
        status = self.status.get()
        return status == TransactionState.Running or status == TransactionState.Committing

class LockingTransaction():
    transaction = thread_local()

    # Global ordering on all transactions---provides a mechanism for determing relativity of transactions
    #  to each other
    transactionCounter = count()

    def _resetData(self):
        self._info = None
        self._vals = {}
        self._sets = set()
        self._commutes = {} # TODO sorted dict

    def __init__(self):
        self._readPoint = -1
        self._resetData()

    def _updateReadPoint(self):
        """
        Update the read point of this transaction to the next transaction counter id"""
        self._readPoint = self.transactionCounter.next()

    @classmethod
    def _getCommitPoint(cls):
        """
        Gets the next transaction counter id, but simply returns it for use instead of
        updating any internal fields.
        """
        return cls.transactionCounter.next()

    def _stop(self, status):
        """
        Stops this transaction, setting the final state to the desired state. Will decrement
        the countdown latch to notify other running transactions that this one has terminated
        """
        if self._info:
            with self._info.lock:
                self._info.status.set(status)
                # TODO countdown latching!
                # self._latch
            self._resetData()

    # def _tryWriteLock(self, ref):

    ### External API
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