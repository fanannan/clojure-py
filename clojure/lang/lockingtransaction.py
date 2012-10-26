from cljexceptions import IllegalStateException, TransactionRetryException

from itertools import count
from threadutil import AtomicInteger
from threading import local as thread_local
from threading import Lock
from time import time

# How many times to retry a transaction before giving up
RETRY_LIMIT = 10000
# How long to wait to acquire a read or write lock for a ref
LOCK_WAIT_SECS = .1 #(100 ms)
# How long a transaction must be alive for before it is considered old enough to survive barging
BARGE_WAIT_SECS = .1 #(10 * 1000000ns)

# Possible status values
class TransactionState:
    Running, Committing, Retry, Killed, Committed = range(5)

class Info:
    def __init__(self, status, startPoint):
        self.status = AtomicInteger(status)
        self.startPoint = startPoint
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
        self._startPoint = None # time since epoch (time.time())
        self._vals = {}
        self._sets = []
        self._commutes = {} # TODO sorted dict
        self._ensures = []

    def __init__(self):
        self._readPoint = -1 # global ordering on transactions (int)
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

    def _tryWriteLock(self, ref):
        """
        Attempts to get a write lock for the desired ref, but only waiting for LOCK_WAIT_SECS
        If acquiring the lock is not possible, throws a retry exception to force a retry for the
        current transaction
        """
        if not ref._lock.acquire(LOCK_WAIT_SECS):
            raise TransactionRetryException

    def _releaseIfEnsured(self, ref):
        """
        Release the given ref from the set of ensured refs, if this ref is ensured
        """
        if ref in self._ensures:
            self._ensures.remove(ref)
            ref._lock.release_shared()

    def _barge(self, other_refinfo):
        """
        Attempts to barge another running transaction, described by that transactions's Info
        object.

        Barging is successful iff:

        1) This transaction is at least BARGE_WAIT_SECS old
        2) This transaction is older than the other transasction
        3) The other transaction is Running and an compareAndSet operation to Killed
            must be successful

        Returns if this barge was successful or not
        """
        # print "Trying to barge: ", time() - self._startPoint, " ", self._startPoint, " < ", other_refinfo.startPoint
        if time() - self._startPoint > BARGE_WAIT_SECS and \
           self._startPoint < other_refinfo.startPoint:
           return other_refinfo.status.compareAndSet(TransactionState.Running, TransactionState.Killed)
        
        return False

    def _blockAndBail(self, other_refinfo):
        """
        This is a time-delayed retry of the current transaction. If we know there was a conflict on a ref
        with other_refinfo's transaction, we give it LOCK_WAIT_SECS to complete before retrying ourselves,
        to reduce contention and re-conflicting with the same transaction in the future.
        """
        self._stop(TransactionState.Retry)
        # TODO Wait for CountdownLatch for LOCK_WAIT_SECS
        raise TransactionRetryException

    def _takeOwnership(self, ref):
        """
        This associates the given ref with this transaction. It is called when a transaction modifies
        a reference in doSet(). It does the following:

        0) Releases any read locks (ensures) on the ref, as a alter/set after an ensure
            undoes the ensure operation
        1) Marks the reference as having been modified in this transaction
        2) Checks if the ref has a newer committed value than the transaction-try start point, and retries this
            transaction if so
        3) Checks if the ref is currently owned by another transaction (has a in-transaction-value in another transaction)
            If so, attempts to barge the other transaction. If it fails, forces a retry
        4) Otherwise, it associates the ref with this transaction by setting the ref's _info to this Info
        5) Returns the most recently committed value for this ref


        This method is called 'lock' in the Clojure/Java implementation
        """
        self._releaseIfEnsured(ref)

        # We might get a retry exception, unlock lock if we have locked it
        unlocked = True
        try:
            self._tryWriteLock(ref)
            unlocked = False

            if ref._tvals and ref._tvals.point > self._readPoint:
                # Newer committed value than when we started our transaction try
                raise TransactionRetryException

            refinfo = ref._tinfo
            if refinfo and refinfo != self._info and refinfo.running():
                # This ref has an in-transaction-value in some *other* transaction
                if not self._barge(refinfo):
                    # We lost the barge attempt, so we retry
                    ref._lock.release()
                    unlocked = True
                    return self._blockAndBail(refinfo)
            # We own this ref
            ref._tinfo = self._info
            return ref._tvals.val if ref._tvals else None
        finally:
            # If we locked the mutex but need to retry, unlock it on our way out
            if not unlocked:
                ref._lock.release()

    def getRef(self, ref):
        """
        Returns a value for the desired ref in this transaction. Ensures that a transaction is running, and
        returns *either* the latest in-transaction-value for this ref (is there is one), or the latest committed
        value that was committed before the start of this transaction.

        If there is no committed value for this ref before this transaction began, it records a fault for the ref,
        and triggers a retry
        """
        if not self._info or not self._info.running():
            raise TransactionRetryException

        # Return in-transaction-value if we have one
        if ref in self._vals:
            return self._vals[ref]

        # Might raise a retry exception
        try:
            ref._lock.acquire_shared()
            if not ref._tvals:
                raise IllegalStateException("Ref in transaction doRef is unbound! ", ref)

            historypoint = ref._tvals
            while True:
                if historypoint.point < self._readPoint:
                    return historypoint.val

                # Get older history value, if we loop around to the front we're done
                historypoint = historypoint.prev
                if historypoint == ref._tvals:
                    break
        finally:
            ref._lock.release_shared()

        # Could not find an old-enough committed value, fault!
        ref._faults.getAndIncrement()
        raise TransactionRetryException

    def doSet(self, ref, val):
        """
        Sets the in-transaction-value of the desired ref to the given value
        """
        if not self._info or not self._info.running():
            raise TransactionRetryException

        # Can't alter after a commute
        if ref in self._commutes:
            raise IllegalStateException("Can't set/alter a ref in a transaction after a commute!")

        if not ref in self._sets:
            self._sets.append(ref)
            self._takeOwnership(ref)

        self._vals[ref] = val
        return val

    def doEnsure(self, ref):
        """
        Ensuring a ref means that no other transactions can change this ref until this transaction is finished.
        """
        if not self._info or not self._info.running():
            raise TransactionRetryException

        # If this ref is already ensured, no more work to do
        if ref in self._ensures:
            return

        # Ensures means we have a read lock (so no one else can write)
        ref._lock.acquire_shared()

        if ref._tvals and ref._tvals.point > self._readPoint:
            # Ref was committed since we started our transaction (since we got our world snapshot)
            # We bail out and retry since we've already 'lost' the ensuring
            ref._lock.release_shared()
            raise TransactionRetryException

        refinfo = ref._tinfo

        if refinfo and refinfo.running():
            # Someone's writing to it (has called _takeOwnership)
            # Let go of our reader lock, ensure means some transaction's already owned it
            ref.lock.release_shared()
            if refinfo != self._info:
                # Not our ref, ensure fails!
                self._blockAndBail(refinfo)
        else:
            self._ensures.append(ref)

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