from aref import ARef
from cljexceptions import IllegalStateException
from threadutil import AtomicInteger, ms_since_epoch
from clojure.util.shared_lock import SharedLock
from lockingtransaction import LockingTransaction

import clojure.lang.rt as RT

from itertools import count

class TVal:
    def __init__(self, val, point, msecs, prev = None):
        self.val = val
        self.point = point
        self.msecs = msecs

        # If we are passed prev, add ourselves to the end of the linked list
        if prev:
            self.prev = prev
            self.next = prev.next
            self.prev.next = self
            self.next.prev = self
        else:
            self.prev = self
            self.next = self

# TODO only thread-safe in cPython
refids = count()

class Ref(ARef):
    def __init__(self, state, meta=None):
        super(Ref, self).__init__(meta)
        self._id = refids.next()
        self._faults = AtomicInteger(0)
        # NOTE SharedLock is also re-entrant.
        # TODO disable logging + debug when ready
        def xprint(x):
            print(x + "\n")
        self._lock = SharedLock(None, True)
        self._tvals = TVal(state, 0, ms_since_epoch())

    def _currentVal(self):
        """
        Returns the current value of the ref. Safe to be called from
        outside an active transaction"""
        self._lock.acquire_shared()
        try:
            if self._tvals:
                return self._tvals.val
            raise IllegalStateException("Accessing unbound ref in currentVal!")
        finally:
            self._lock.release_shared()

    def deref(self):
        """
        Returns either the in-transaction-value of this ref if there is an active
        transaction, or returns the last committed value of ref"""
        transaction = LockingTransaction.get()
        if transaction:
            return transaction.doGet(self)
        return self._currentVal()

    def refSet(self, state):
        """
        Sets the value of this ref to the desired state, regardless of the current value

        Returns the newly set state"""
        return LockingTransaction.ensureGet().doSet(self, state)

    def alter(self, fn, *args):
        """
        Alters the value of this ref, and returns the new state"""
        transaction = LockingTransaction.ensureGet()
        transaction.doSet(self, apply(fn, RT.cons(transaction.doGet(self), *args)))

    def commute(self, fn, *args):
        """
        Commutes the value of this ref, allowing for it to be updated by other transactions before the
        commuting function is called"""
        LockingTransaction.ensureGet().doCommute(self, fn, *args)

    def touch(self):
        """
        Ensures that this ref cannot be given a new in-transaction-value by any other transactions for the duration
        of this transaction"""
        LockingTransaction.ensureGet().doEnsure(self)

    def isBound(self):
        """
        Returns whether or not this reference has had at least one TVal in the history chain set"""
        try:
            self._lock.acquire_shared()
            return self._tvals != None
        finally:
            self._lock.release_shared()

    def trimHistory(self):
        """
        Shortens the tvals history chain to the newest-item only"""
        try:
            self._lock.acquire()
            if self._tvals != None:
                self._tvals.next = self._tvals
                self._tvals.prev = self._tvals
        finally:
            self._lock.release()

    def _historyCount(self):
        """
        Internal history length counter. Read lock must be acquired"""
        if self._tvals == None:
            return 0
        count = 1
        tval = self._tvals.next
        while tval != self._tvals:
            count += 1
        return count

    def getHistoryCount(self):
        """
        Return the length of the tvals history chain. Requires a traversal and a read lock"""
        try:
            self._lock.acquire_shared()
            return self._historyCount()
        finally:
            self._lock.release_shared()
