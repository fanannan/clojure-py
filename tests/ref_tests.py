"""ref_tests.py

Thursday, Oct. 25 2012"""

import unittest

from clojure.lang.ref import Ref, TVal
from clojure.lang.lockingtransaction import LockingTransaction, TransactionState, Info
from clojure.lang.cljexceptions import IllegalStateException
from clojure.util.shared_lock import SharedLock

from threading import Thread
from threading import local as thread_local
import clojure.lang.persistentvector as pv

class TestRef(unittest.TestCase):
    def setUp(self):
        self.refZero = Ref(0, None)
        self.refOne = Ref(pv.vec(range(10)), None)
    ### Internal state
    def testInternalState_PASS(self):
        ## NOTE depends on number of test cases, ugh
        self.assertEqual(self.refZero._id, 16)
        self.assertEqual(self.refOne._id, 17)
        self.assertEqual(self.refZero._faults.get(), 0)
        self.assertEqual(self.refOne._faults.get(), 0)
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
        self.assertEqual(self.refOne.getHistoryCount(), 1)
    def testTrimHistory_PASS(self):
        self.refOne.trimHistory()
        self.assertEqual(self.refOne.getHistoryCount(), 1)

class TestLockingTransaction(unittest.TestCase):
    def testNone_PASS(self):
        self.assertIsNone(LockingTransaction.get())
    def testCreateThreadLocal_PASS(self):
        def f(mainTransaction):
            self.assertIsNone(LockingTransaction.get())
            LockingTransaction.runInTransaction(lambda x: x**2)
            self.assertIsInstance(LockingTransaction.get(), LockingTransaction)
            self.assertIsInstance(LockingTransaction.ensureGet(), LockingTransaction)
            # Make sure we're getting a unique locking transaction in this auxiliary thread
            self.assertNotEqual(LockingTransaction.ensureGet(), mainTransaction)
            # Clean up and remove LockingTransaction we created
            LockingTransaction.transaction = thread_local()
            self.assertIsNone(LockingTransaction.get())
        LockingTransaction.runInTransaction(lambda x: x**2)
        self.assertIsInstance(LockingTransaction.get(), LockingTransaction)
        self.assertIsInstance(LockingTransaction.ensureGet(), LockingTransaction)
        t = Thread(target=f, args=[LockingTransaction.ensureGet()])
        t.start()
        t.join()
        # Clean up and remove LockingTransaction we created
        LockingTransaction.transaction = thread_local()
        self.assertIsNone(LockingTransaction.get())
    def testOrdering_PASS(self):
        try:
            LockingTransaction.runInTransaction(lambda x: x**2)
            self.assertEqual(LockingTransaction.ensureGet()._readPoint, -1)
            LockingTransaction.ensureGet()._updateReadPoint()
            self.assertEqual(LockingTransaction.ensureGet()._readPoint, 0)
            LockingTransaction.ensureGet()._updateReadPoint()
            self.assertEqual(LockingTransaction.ensureGet()._readPoint, 1)
            self.assertEqual(LockingTransaction._getCommitPoint(), 2)
            self.assertEqual(LockingTransaction.ensureGet()._readPoint, 1)
        finally:
            LockingTransaction.transaction = thread_local()
    def testTransactionInfo_PASS(self):
        try:
            LockingTransaction.runInTransaction(lambda x: x**2)

            # NOTE assumes transactions don't actually work yet (_info is never set)
            self.assertIsNone(LockingTransaction.ensureGet()._info)
        finally:
            LockingTransaction.transaction = thread_local()
    def testStop_PASS(self):
        try:
            LockingTransaction.runInTransaction(lambda x: x**2)

            # NOTE assumes transactions don't actually work yet (_info is never set)
            self.assertIsNone(LockingTransaction.ensureGet()._info)
            LockingTransaction.ensureGet()._stop(TransactionState.Killed)
            self.assertIsNone(LockingTransaction.ensureGet()._info)

            # Fake running transaction
            LockingTransaction.ensureGet()._info = Info(TransactionState.Running, LockingTransaction.ensureGet()._readPoint)
            self.assertIsNotNone(LockingTransaction.ensureGet()._info)
            self.assertEqual(LockingTransaction.ensureGet()._info.status.get(), TransactionState.Running)
            LockingTransaction.ensureGet()._stop(TransactionState.Committed)
            # No way to check for proper status==Committed here since it sets the countdownlatch then immediately sets itself to none
            self.assertIsNone(LockingTransaction.ensureGet()._info)
            self.assertIsNone
        finally:
            LockingTransaction.transaction = thread_local()