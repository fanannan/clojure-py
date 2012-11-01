from cljexceptions import IllegalStateException, TransactionRetryException
from clojure.lang.util import TVal
import clojure.lang.rt as RT
from clojure.util.shared_lock import shared_lock, unique_lock

from itertools import count
from threadutil import AtomicInteger
from threading import local as thread_local
from threading import Lock, Event, current_thread
from time import time

# How many times to retry a transaction before giving up
RETRY_LIMIT = 10000
# How long to wait to acquire a read or write lock for a ref
LOCK_WAIT_SECS = 0.1 #(100 ms)
# How long a transaction must be alive for before it is considered old enough to survive barging
BARGE_WAIT_SECS = 0.1 #(10 * 1000000ns)

spew_debug = False

# Possible status values
class TransactionState:
    Running, Committing, Retry, Killed, Committed = range(5)

loglock = Lock()
def log(msg):
    """
    Thread-safe logging, can't get logging module to spit out debug output
    when run w/ nosetests :-/
    """
    if spew_debug:
        with loglock:
            print("Thread: %s (%s): %s" % (current_thread().ident, id(current_thread()), msg))

class Info:
    def __init__(self, status, startPoint):
        self.status = AtomicInteger(status)
        self.startPoint = startPoint

        self.lock = Lock()

        # Faking java's CountdownLatch w/ a simple event---it's only from 1
        self.latch = Event()

    def running(self):
        status = self.status.get()
        return status in (TransactionState.Running, TransactionState.Committing)

class LockingTransaction():
    _transactions = thread_local()
    # Global ordering on all transactions---provides a mechanism for determing relativity of transactions
    #  to each other
    # Start the count at 1 since refs history starts at 0, and a ref created before the first transaction
    #  should be considered "before", and 0 < 0 is false.
    transactionCounter = count(1)

    def _resetData(self):
        self._info       = None
        self._startPoint = -1 # time since epoch (time.time())
        self._vals       = {}
        self._sets       = []
        self._commutes   = {}
        self._ensures    = []
        self._actions    = [] # Deferred agent actions

    def __init__(self):
        self._readPoint = -1 # global ordering on transactions (int)
        self._resetData()

    def _retry(self, debug_msg):
        """
        Raises a retry exception, with additional message for debugging
        """
        log(debug_msg)
        raise TransactionRetryException

    def _updateReadPoint(self, set_start_point):
        """
        Update the read point of this transaction to the next transaction counter id"""
        self._readPoint = self.transactionCounter.next()
        if set_start_point:
            self._startPoint = self._readPoint
            self._startTime = time()


    @classmethod
    def _getCommitPoint(cls):
        """
        Gets the next transaction counter id, but simply returns it for use instead of
        updating any internal fields.
        """
        return cls.transactionCounter.next()

    def _stop_transaction(self, status):
        """
        Stops this transaction, setting the final state to the desired state. Will decrement
        the countdown latch to notify other running transactions that this one has terminated
        """
        if self._info:
            with self._info.lock:
                self._info.status.set(status)
                self._info.latch.set()
            self._resetData()

    def _tryWriteLock(self, ref):
        """
        Attempts to get a write lock for the desired ref, but only waiting for LOCK_WAIT_SECS
        If acquiring the lock is not possible, throws a retry exception to force a retry for the
        current transaction
        """
        if not ref._lock.acquire(LOCK_WAIT_SECS):
            self._retry("RETRY - Failed to acquire write lock in _tryWriteLock. Owned by %s" % ref._tinfo.thread_id if ref._tinfo else None)

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
        # log("Trying to barge: %s %s < %s" % (time() - self._startPoint, self._startPoint, other_refinfo.startPoint))
        if(time() - self._startPoint > BARGE_WAIT_SECS and
            self._startPoint < other_refinfo.startPoint):
            if other_refinfo.status.compareAndSet(TransactionState.Running, TransactionState.Killed):
                # We barged them successfully, set their "latch" to 0 by setting it to true
                # log("BARGED THEM!")
                other_refinfo.latch.set()
                return True

        return False

    def _blockAndBail(self, other_refinfo):
        """
        This is a time-delayed retry of the current transaction. If we know there was a conflict on a ref
        with other_refinfo's transaction, we give it LOCK_WAIT_SECS to complete before retrying ourselves,
        to reduce contention and re-conflicting with the same transaction in the future.
        """
        self._stop_transaction(TransactionState.Retry)
        other_refinfo.latch.wait(LOCK_WAIT_SECS)

        self._retry("RETRY - Blocked and now bailing")

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
                self._retry("RETRY - Newer committed value when taking ownership")

            refinfo = ref._tinfo
            if refinfo and refinfo != self._info and refinfo.running():
                # This ref has an in-transaction-value in some *other* transaction
                if not self._barge(refinfo):
                    # We lost the barge attempt, so we retry
                    ref._lock.release()
                    unlocked = True
                    # log("BARGE FAILED other: %s (%s != %s ? %s (running? %s))" %
                            # (refinfo.thread_id, refinfo, self._info, refinfo != self._info, refinfo.running()))
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
            self._retry("RETRY - Not running in getRef")

        # Return in-transaction-value if we have one
        if ref in self._vals:
            return self._vals[ref]

        # Might raise a retry exception
        with unique_lock(ref._lock):
            if not ref._tvals:
                raise IllegalStateException("Ref in transaction doRef is unbound! ", ref)

            historypoint = ref._tvals
            while True:
                # log("Checking: %s < %s" % (historypoint.point, self._readPoint))
                if historypoint.point < self._readPoint:
                    return historypoint.val

                # Get older history value, if we loop around to the front we're done
                historypoint = historypoint.prev
                if historypoint == ref._tvals:
                    break

        # Could not find an old-enough committed value, fault!
        ref._faults.getAndIncrement()
        self._retry("RETRY - Fault, no new-enough value!")

    def doSet(self, ref, val):
        """
        Sets the in-transaction-value of the desired ref to the given value
        """
        if not self._info or not self._info.running():
            self._retry("RETRY - Not running in doSet!")

        # Can't alter after a commute
        if ref in self._commutes:
            raise IllegalStateException("Can't set/alter a ref in a transaction after a commute!")

        if not ref in self._sets:
            self._sets.append(ref)
            self._takeOwnership(ref)

        self._vals[ref] = val
        return val

    def doCommute(self, ref, fn, args):
        """
        Sets the in-transaction-value of this ref to the given value, but does not require
        other transactions that also change this ref to retry. Commutes are re-computed at
        commit time and apply on top of any more recent changes.
        """
        if not self._info or not self._info.running():
            self._retry("RETRY - Not running in doCommute!")

        # If we don't have an in-transaction-value yet for this ref
        #  get the latest one
        if not ref in self._vals:
            with shared_lock(ref._lock):
                val = ref._tvals.val if ref._tvals else None

            self._vals[ref] = val

        # Add this commute function to the end of the list of commutes for this ref
        self._commutes.setdefault(ref, []).append([fn, args])
        # Save the value we get by applying the fn now to our in-transaction-list
        returnValue = fn(*RT.cons(self._vals[ref], args))
        self._vals[ref] = returnValue

        return returnValue

    def doEnsure(self, ref):
        """
        Ensuring a ref means that no other transactions can change this ref until this transaction is finished.
        """
        if not self._info or not self._info.running():
            self._retry("RETRY - Not running in doEnsure!")

        # If this ref is already ensured, no more work to do
        if ref in self._ensures:
            return

        # Ensures means we have a read lock (so no one else can write)
        ref._lock.acquire_shared()

        if ref._tvals and ref._tvals.point > self._readPoint:
            # Ref was committed since we started our transaction (since we got our world snapshot)
            # We bail out and retry since we've already 'lost' the ensuring
            ref._lock.release_shared()
            self._retry("RETRY - Ref already committed to in doEnsure")

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

    def run(self, fn):
        """Main STM entry point---run the desired 0-args function in a
        transaction, capturing all modifications that happen,
        atomically applying them during the commit step."""

        tx_committed = False

        for i in xrange(RETRY_LIMIT):
            if tx_committed: break

            self._updateReadPoint(i == 0)

            locks, notifications = [], []
            try:
                self._start_transaction()
                returnValue = fn()
                if self.attempt_commit(locks, notifications):
                    tx_committed = True

            except TransactionRetryException:
                pass # Retry after cleanup.
            finally:
                self.release_locks(locks)
                self.release_ensures()
                self._stop_transaction(TransactionState.Committed if tx_committed else TransactionState.Retry)
                self.send_notifications(tx_committed, notifications)

        if tx_committed:
            return returnValue
        else:
            raise CljException("Transaction failed after reaching retry limit :'(")

    def _start_transaction(self):
        # Set the info for this transaction try. We are now Running!
        # if self._info:
        #     log("Setting new INFO, but old: %s %s is running? %s" % (self._info, self._info.thread_id, self._info.running()))
        self._info = Info(TransactionState.Running, self._startPoint)
        self._info.thread_id = "%s (%s)" % (current_thread().ident, id(current_thread()))
        # log("new INFO: %s %s running? %s" % (self._info, self._info.thread_id, self._info.running()))

    def attempt_commit(self, locks, notifications):
        # This will either raise an exception or return True
        if self._info.status.compareAndSet(TransactionState.Running, TransactionState.Committing):
            self.handle_commutes(locks)
            self.acquire_write_locks(self._sets, locks)
            self.validate_changes(self._vals)
            notifications = self.commit_ref_sets()
            self._info.status.set(TransactionState.Committed)
            return True
        return False

    def handle_commutes(self, locks):
        for ref, funcpairs in self._commutes.items():
            # If this ref has been ref-set or alter'ed before the commute, no need to re-apply
            # since we can be sure that the commute happened on the latest value of the ref
            if ref in self._sets: continue

            wasEnsured = ref in self._ensures
            self._releaseIfEnsured(ref)

            # Try to get a write lock---if some other transaction is
            # committing to this ref right now, retry this transaction
            self._tryWriteLock(ref)
            locks.append(ref)
            if wasEnsured and ref._tvals and ref._tvals.point > self._readPoint:
                self._retry("RETRY - was ensured and has newer version while commiting")

            other_refinfo = ref._tinfo
            if other_refinfo and other_refinfo != self._info and other_refinfo.running():
                # Other transaction is currently running, and owns
                # this ref---meaning it set the ref's
                # in-transaction-value already, so we either barge
                # them or retry
                if not self._barge(other_refinfo):
                    self._retry("RETRY - conflicting commutes being commited and barge failed")

            # Ok, no conflicting ref-set or alters to this ref, we can
            # make the change Update the val with the latest
            # in-transaction-version
            val = ref._tvals.val if ref._tvals else None
            self._vals[ref] = val

            # Now apply each commute to the latest value
            for fn, args in funcpairs:
                self._vals[ref] = fn(*RT.cons(self._vals[ref], args))

    def acquire_write_locks(self, sets, locks):
        # Acquire a write lock for all refs that were assigned to.
        # We'll need to change all of their values If we can't,
        # another transaction is committing so we retry
        for ref in sets:
            self._tryWriteLock(ref)
            locks.append(ref)

    def validate_changes(self, vals):
        # Call validators on each ref about to be changed to make sure the change is allowed
        for ref, value in vals.items():
            ref.validate(value)

    def commit_ref_sets(self):
        notifications = []
        commitPoint = self._getCommitPoint()
        for ref in self._vals:
            oldValue      = ref._tvals.val if ref._tvals else None
            newVal        = self._vals[ref]
            historyLength = ref.historyCount()

            # Easy case: ref has no binding, so lets give it one
            if not ref._tvals:
                ref._tvals = TVal(newVal, commitPoint, self._startTime)

            # Add this new value to the tvals history chain. This happens if:
            #  1. historyCount is less than minHistory
            #  2. the ref's faults > 0 and the history chain is < maxHistory
            elif ref._faults.get() > 0 and historyLength < ref.maxHistory() or \
                   historyLength < ref.minHistory():
                ref._tvals = TVal(newVal, commitPoint, self._startTime, ref._tvals)
                ref._faults.set(0)
            # Otherwise, we recycle the oldest ref in the chain, and make it the newest
            else:
                ref._tvals       = ref._tvals.next
                ref._tvals.val   = newVal
                ref._tvals.point = commitPoint
                ref._tvals.msecs = self._startTime

            if len(ref.getWatches()) > 0:
              notifications.append([ref, oldValue, newVal])

        return notifications

    def release_locks(self, locks):
        locks.reverse()
        for ref in locks:
            ref._lock.release()

    def release_ensures(self):
        for ref in self._ensures:
            ref._lock.release_shared()
        self._ensures = []

    def send_notifications(self, tx_committed, notifications):
        try:
            if tx_committed:
                for ref, oldval, newval in notifications:
                    ref.notifyWatches(oldval, newval)
                for action in self._actions:
                    pass # TODO actions when agents are supported
        finally:
            self._actions = []

    ### External API
    @classmethod
    def get(cls):
        """
        Returns the per-thread singleton transaction
        """
        try:
            return cls.ensureGet()
        except IllegalStateException:
            return None

    @classmethod
    def ensureGet(cls):
        """
        Returns the per-thread singleton transaction, or raises
        an IllegalStateException if one is not running
        """
        try:
            transaction = cls._transactions.local
            if not transaction or not transaction._info:
                raise AttributeError
        except AttributeError:
            raise IllegalStateException("No transaction running.")
        return transaction

    @classmethod
    def runInTransaction(cls, fn):
        """
        Runs the desired function in this transaction
        """
        try:
            transaction = cls.ensureGet()
        except IllegalStateException:
            transaction = cls._transactions.local = LockingTransaction()

        if transaction._info:
            # Already running transaction... execute current transaction in it. No nested transactions in the same thread
            return apply(fn)

        return transaction.run(fn)
