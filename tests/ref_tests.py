"""ref_tests.py

Thursday, Oct. 25 2012"""

import unittest
from threading import Thread, current_thread
from threading import local as thread_local
from contextlib import contextmanager
from time import time, sleep
from itertools import count

from clojure.lang.ref import Ref, TVal
from clojure.lang.lockingtransaction import LockingTransaction, TransactionState, Info
from clojure.lang.cljexceptions import IllegalStateException, TransactionRetryException
from clojure.util.shared_lock import SharedLock

import clojure.lang.persistentvector as pv

class TestRef(unittest.TestCase):
    def setUp(self):
        self.refZero = Ref(0, None)
        self.refOne = Ref(pv.vec(range(10)), None)
    ### Internal state
    def testInternalState_PASS(self):
        ## NOTE depends on number of test cases, ugh
        # self.assertEqual(self.refZero._id, 22)
        # self.assertEqual(self.refOne._id, 23)
        self.assertEqual(self.refZero._faults.get(), 0)
        self.assertEqual(self.refOne._faults.get(), 0)
        self.assertIsNone(self.refZero._tinfo)
        self.assertIsInstance(self.refZero._lock, SharedLock)
        self.assertIsInstance(self.refZero._tvals, TVal)
    def testTVal_PASS(self):
        self.assertEqual(self.refZero._tvals.val, 0)
        self.assertEqual(self.refZero._tvals.point, 0)
        self.assertGreater(self.refZero._tvals.msecs, 0)
        self.assertEqual(self.refZero._tvals.next, self.refZero._tvals)
        self.assertEqual(self.refZero._tvals.prev, self.refZero._tvals)
    ### External API
    def testEquality_PASS(self):
        self.assertEqual(self.refZero, self.refZero)
    def testCurrentValPASS(self):
        self.assertEqual(self.refZero._currentVal(), 0)
    def testDeref_PASS(self):
        self.assertEqual(self.refZero.deref(), 0)
    def testDerefVec_PASS(self):
        self.assertEqual(self.refOne.deref(), pv.vec(range(10)))
    def testSetNoTransaction_FAIL(self):
        self.assertRaises(IllegalStateException, self.refOne.refSet, 1)
    def testAlterNoTransaction_FAIL(self):
        self.assertRaises(IllegalStateException, self.refOne.alter, lambda x: x**2)
    def testCommuteNoTransaction_FAIL(self):
        self.assertRaises(IllegalStateException, self.refOne.commute, lambda x: x**2)
    def testTouchNoTransaction_FAIL(self):
        self.assertRaises(IllegalStateException, self.refOne.touch)
    def testBound_PASS(self):
        self.assertTrue(self.refOne.isBound())
    def testHistoryLen_PASS(self):
        self.assertEqual(self.refOne.historyCount(), 1)
    def testTrimHistory_PASS(self):
        self.refOne.trimHistory()
        self.assertEqual(self.refOne.historyCount(), 1)

# TODO refactor to use setupClass and teardownClass, initial attempt didn't
#      seem to work
@contextmanager
def running_transaction(thetest):
    # Fake a running transaction
    LockingTransaction.transaction.data = LockingTransaction()
    LockingTransaction.ensureGet()._readPoint = -1
    LockingTransaction.transactionCounter = count()
    LockingTransaction.ensureGet()._startPoint = time()
    LockingTransaction.ensureGet()._info = Info(TransactionState.Running, LockingTransaction.ensureGet()._startPoint)
    yield
    # Clean up and remove LockingTransaction we created
    LockingTransaction.transaction = thread_local()
    thetest.assertIsNone(LockingTransaction.get())

class TestLockingTransaction(unittest.TestCase):
    def setUp(self):
        self.refZero = Ref(0, None)

    def secondary_op(self, func):
        """
        Utility function, runs the desired function in a secondary thread with
        its own transaction

        func should accept two argument: testclass, and the main thread's LockingTransaction
        """
        def thread_func(testclass, mainTransaction, funcToRun):
            self.assertIsNone(LockingTransaction.get())
            LockingTransaction.transaction.data = LockingTransaction()
            funcToRun(testclass, mainTransaction)
            LockingTransaction.transaction = thread_local()
            self.assertIsNone(LockingTransaction.get())

        t = Thread(target=thread_func, args=[self, LockingTransaction.ensureGet(), func])
        t.start()

    def lockRef(self, ref, reader=False):
        """
        Locks the desired ref's read or write lock. Creates a side thread that never exits, just holds the lock
        """
        def locker(ref, reader):
            if reader:
                ref._lock.acquire_shared()
            else:
                ref._lock.acquire()

        t = Thread(target=locker, args=[ref, reader])
        t.start()

    def testNone_PASS(self):
        self.assertIsNone(LockingTransaction.get())

    def testCreateThreadLocal_PASS(self):
        with running_transaction(self):
            def secondary(testclass, mainTransaction):
                testclass.assertIsInstance(LockingTransaction.get(), LockingTransaction)
                testclass.assertIsInstance(LockingTransaction.ensureGet(), LockingTransaction)
                # Make sure we're getting a unique locking transaction in this auxiliary thread
                testclass.assertNotEqual(LockingTransaction.ensureGet(), mainTransaction)
            self.assertIsInstance(LockingTransaction.get(), LockingTransaction)
            self.assertIsInstance(LockingTransaction.ensureGet(), LockingTransaction)
            self.secondary_op(secondary)

    def testOrdering_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()
            self.assertEqual(t._readPoint, -1)
            t._updateReadPoint()
            self.assertEqual(t._readPoint, 0)
            t._updateReadPoint()
            self.assertEqual(t._readPoint, 1)
            self.assertEqual(t._getCommitPoint(), 2)
            self.assertEqual(t._readPoint, 1)

    def testTransactionInfo_PASS(self):
        with running_transaction(self):
            # NOTE assumes transactions don't actually work yet (_info is never set)
            pass

    def testStop_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()
            # NOTE assumes transactions don't actually work yet (_info is never set)
            t._stop(TransactionState.Killed)
            self.assertIsNone(t._info)

            # Fake running transaction
            t._info = Info(TransactionState.Running, t._readPoint)
            self.assertIsNotNone(t._info)
            self.assertEqual(t._info.status.get(), TransactionState.Running)
            t._stop(TransactionState.Committed)
            # No way to check for proper status==Committed here since it sets the countdownlatch then immediately sets itself to none
            self.assertIsNone(t._info)

    def testTryLock_PASS(self):
        with running_transaction(self):
            LockingTransaction.ensureGet()._tryWriteLock(self.refZero)

    def testBarge_PASS(self):
        with running_transaction(self):
            def secondary(testclass, mainTransaction):
                ourTransaction = LockingTransaction.ensureGet()
                # Barging should fail as this transaction is too young
                ourTransaction._info = Info(TransactionState.Running, ourTransaction._readPoint)
                ourTransaction._startPoint = time()
                testclass.assertFalse(ourTransaction._barge(mainTransaction._info))

                # Barging should still fail, we are the newer transaction
                sleep(.2)
                testclass.assertFalse(ourTransaction._barge(mainTransaction._info))

                # Fake ourselves being older by resetting our time
                # Now barging should be successful
                ourTransaction._startPoint = mainTransaction._startPoint - 2
                testclass.assertTrue(ourTransaction._barge(mainTransaction._info))

                # Make sure we are still running, and we successfully set the other transaction's
                #  state to Killed
                testclass.assertEqual(ourTransaction._info.status.get(), TransactionState.Running)
                testclass.assertEqual(mainTransaction._info.status.get(), TransactionState.Killed)

            t = LockingTransaction.ensureGet()
            # For test purposes, force this transaction status to be the desired state
            t._startPoint = time()
            t._info = Info(TransactionState.Running, t._startPoint)
            # Get two transactions on two different threads
            self.secondary_op(secondary)

    def testTakeOwnershipBasic_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()

            # Will retry since our readPoint is 0
            self.assertRaises(TransactionRetryException, t._takeOwnership, self.refZero)
            # Make sure we unlocked the lock
            self.assertRaises(Exception, self.refZero._lock.release)

            # Now we set the read points synthetically (e.g. saying this transaction-try  is starting *now*)
            #  so it appears there is no newer write since
            # this transaction
            # Taking ownership should work since no other transactions exist
            t._readPoint = time()
            self.assertEqual(t._takeOwnership(self.refZero), 0)
            self.assertRaises(Exception, self.refZero._lock.release)

    def testTakeOwnershipLocked_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()

            # Set a write lock on the ref, check we get a retry
            self.lockRef(self.refZero)
            self.assertRaises(TransactionRetryException, t._takeOwnership, self.refZero)

    def testTakeOwnershipBarging_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()
            sleep(.1)

            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()

            # Give this ref over to another transaction
            def secondary(testclass, mainTransaction):
                t = LockingTransaction.ensureGet()
                t._startPoint = time()
                t._info = Info(TransactionState.Running, t._startPoint)
                # We own the ref now
                testclass.refZero._tinfo = t._info

            # give up the ref
            self.secondary_op(secondary)
            sleep(.1)
            # now try to get it back and successfully barge
            self.assertEqual(t._takeOwnership(self.refZero), 0)
            self.assertEqual(self.refZero._tinfo, t._info)

    def testTakeOwnershipBargeFail_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()

            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()

            def secondary(testclass, mainTransaction):
                t = LockingTransaction.ensureGet()
                t._startPoint = time()
                t._info = Info(TransactionState.Running, t._startPoint)
                # We own the ref now
                testclass.refZero._tinfo = t._info

            # Try again but time time we won't be successful barging
            self.secondary_op(secondary)

            # We fake being newer, so we aren't allowed to barge an older transaction
            sleep(.2)
            t._startPoint = time()
            t._info.startPoint = time()
            self.assertRaises(TransactionRetryException, t._takeOwnership, self.refZero)
            self.assertRaises(Exception, self.refZero._lock.release)

    def testGetRef_PASS(self):
        # No running transaction
        self.assertRaises(TransactionRetryException, LockingTransaction().getRef, self.refZero)
        with running_transaction(self):
            t = LockingTransaction.ensureGet()

            # Ref has a previously committed value and no in-transaction-value, so check it
            # Since we're faking this test, we need our read point to be > 0
            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()
            self.assertEqual(t.getRef(self.refZero), 0)
            # Make sure we unlocked the ref's lock
            self.assertRaises(Exception, self.refZero._lock.release_shared)

            # Give the ref some history and see if it still works. Our read point is 1 and our new value was committed at time 3
            self.refZero._tvals = TVal(100, 3, time(), self.refZero._tvals)
            self.assertEqual(t.getRef(self.refZero), 0)

            # Now we want the latest value
            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()
            self.assertEqual(t.getRef(self.refZero), 100)

            # Force the oldest val to be too new to get a fault
            # Now we have a val at history point 2 and 3
            self.refZero._tvals.next.point = 2
            t._readPoint = 1

            # We should retry and update our faults
            self.assertRaises(TransactionRetryException, t.getRef, self.refZero)
            self.assertEqual(self.refZero._faults.get(), 1)

    def testDoSet_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()
            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()

            # Do the set and make sure it was set in our various transaction state vars
            t.doSet(self.refZero, 200)
            self.assertTrue(self.refZero in t._sets)
            self.assertTrue(self.refZero in t._vals)
            self.assertEqual(t._vals[self.refZero], 200)
            self.assertEqual(self.refZero._tinfo, t._info)

    def testEnsures_PASS(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()

            # Try a normal ensure. Will fail as t has readPoint of -1 and ref has oldest commit point at 0
            self.assertRaises(TransactionRetryException, t.doEnsure, self.refZero)
            self.assertRaises(Exception, self.refZero._lock.release_shared)

            # Now set our transaction to be further in the future, ensure works
            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()
            t.doEnsure(self.refZero)
            self.assertTrue(self.refZero in t._ensures)
            # Try again
            t.doEnsure(self.refZero)
            # Make sure it's read by releasing the lock exactly once w/out error
            self.refZero._lock.release_shared()
            self.assertRaises(Exception, self.refZero._lock.release_shared)

    def testEnsures_FAIL(self):
        with running_transaction(self):
            t = LockingTransaction.ensureGet()
            LockingTransaction.ensureGet()._updateReadPoint()
            LockingTransaction.ensureGet()._updateReadPoint()

            # Make another transaction to simulate a conflict (failed ensure)
            # First, write to it in this thread
            t.doSet(self.refZero, 999)
            def secondary(testclass, mainTransaction):
                t = LockingTransaction.ensureGet()
                LockingTransaction.ensureGet()._updateReadPoint()
                LockingTransaction.ensureGet()._updateReadPoint()

                # Try an ensure that will fail (and cause a bail)
                testclass.assertRaises(TransactionRetryException, t.doEnsure(testclass.refZero))
                self.assertRaises(Exception, self.refZero._lock.release_shared)

    def testRun_PASS(self):
        # Now we'll run a transaction ourselves
        