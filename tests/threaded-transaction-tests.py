"""threaded-transaction-tests.py

These tests exercise the LockingTransaction code
in some multithreaded ways. More comprehensive testing
than the tests in ref-tests.py, which test at a more
granular level.

Friday, Oct. 26 2012"""

import unittest
from threading import Thread, current_thread, Condition, local
from time import time, sleep
from itertools import count

from clojure.lang.ref import Ref, TVal
from clojure.lang.lockingtransaction import LockingTransaction, TransactionState, Info
from clojure.lang.cljexceptions import IllegalStateException, TransactionRetryException
from clojure.util.shared_lock import SharedLock
from clojure.lang.threadutil import AtomicInteger

##
# Basic idea: We want to test the corner cases when different transactions that happen concurrently on different
# threads run into each other at a specified time. e.g. if Transaction 1 (T1) does an in-transaction-write of Ref 1 (R1)
# and Transaction 2 (T2) that was started later tries to do a write on R1, T2 should be retried.
#
# In order to test cross-thread behaviour this granularly we need a bit of leg-work.
# 

# Verbose output for debugging
spew_debug = True

class TestThreadedTransactions(unittest.TestCase):
    spawned_threads = []

    def setUp(self):
        self.main_thread = current_thread()
        self.first_run = local()
        self.first_run.data = True

    def tearDown(self):
        # Join all if not joined with yet
        self.join_all()

    def d(self, str):
        """
        Debug helper
        """
        if spew_debug:
            print(str)

    def runTransactionInThread(self, func, autostart=True, postcommit=None):
        """
        Runs the desired function in a transaction on a secondary thread. Optionally
        allows the caller to start the thread manually, and takes an optional postcommit
        function that is run after the transaction is committed
        """
        def thread_func(transaction_func, postcommit_func):
            self.first_run.data = True
            LockingTransaction.runInTransaction(transaction_func)
            if postcommit_func:
                postcommit_func()

        t = Thread(target=thread_func, args=[func, postcommit])
        if autostart:
            t.start()
        self.spawned_threads.append(t)

        return t

    def wait(self, cond):
        """
        Locks, waits for, and releases the desired condition. If this is not the main thread, 
        it assumes we're in a transaction, and only does so in the initial retry. Unlocks at the end.
        """
        if current_thread() == self.main_thread or self.first_run.data:
            cond.acquire()
            cond.wait()
            cond.release()
            self.first_run.data = False

    def notify(self, cond):
        """
        Locks and notifies, then unlocks the desired condition variable
        """
        cond.acquire()
        cond.notifyAll()
        cond.release()

    def join_all(self):
        """
        Joins all spawned threads to make sure they have all finished before continuing
        """
        for thread in self.spawned_threads:
            thread.join()
        self.spawned_threads = []


    def testSimpleConcurrency(self):
        def t1():
            sleep(.1)
            self.ref0.refSet(1)
        def t2():
            self.ref0.refSet(2)

        # Delaying t1 means it should commit after t2
        self.ref0 = Ref(0, None)
        self.runTransactionInThread(t1)
        self.runTransactionInThread(t2)
        self.join_all()
        self.assertEqual(self.ref0.deref(), 1)

    def testFault(self):
        # We want to cause a fault on one ref, that means no committed value yet
        t1wait = Condition()
        t1launched = Condition()

        def t1():
            # This thread tries to read the value after t2 has written to it, but it starts first
            self.d("* Before wait")
            self.wait(t1wait)
            val = self.refA.deref()
            self.d("* Derefed, asserting fault")
            # Make sure we only successfully got here w/ 1 fault (deref() triggered a retry the first time around)
            self.assertEqual(self.refA._faults.get(), 1)
            self.assertEqual(self.refA.historyCount(), 1)
            # self.assertEqual()
            self.assertEqual(val, 6)

            self.d("* Refsetting after fault")
            # When committed, we should create another tval in the history chain
            self.refA.refSet(7)

        def t2():
            self.wait(t1launched)

            # This thread does the committing after t1 started but before it reads the value of refA
            self.d("** Creating ref")
            self.refA = Ref(5, None)
            self.refA.refSet(6)

        def t2committed():
            self.d("** Notify")
            sleep(.1)
            self.notify(t1wait)

        self.runTransactionInThread(t1)
        self.runTransactionInThread(t2, postcommit=t2committed)

        self.notify(t1launched)

        self.join_all()

        # The write after the fault should have created a new history chain item
        self.assertEqual(self.refA.historyCount(), 2)
        self.d("Len: %s" % self.refA.historyCount())

    def testBarge(self):
        # Barging happens when one transaction tries to do an in-transaction-write to a ref that has
        # an in-transaction-value from another transaction
        t1wait = Condition()
        t2wait = Condition()

        self.t1counter = 0
        self.t2counter = 0

        def t1():
            # We do the first in-transaction-write
            self.refA.refSet(888)

            # Don't commit yet, we want t2 to run and barge us
            self.d("* Notify")
            self.notify(t2wait)
            self.d("* Wait")
            self.wait(t1wait)
            self.d("* Done")

            self.t1counter += 1

        def t2():
            # Wait till t1 has done its write
            self.d("** Wait")
            self.wait(t2wait)

            # Try to write to the ref
            # We should try and succeed to barge them: we were started first
            # and should be long-lived enough
            self.d("** Before barge")
            self.refA.refSet(777)

            self.d("** After barge")
            sleep(.1)
            self.notify(t1wait)

            self.t2counter += 1

        self.refA = Ref(999, None)
        th1 = self.runTransactionInThread(t1, autostart=False)
        th2 = self.runTransactionInThread(t2, autostart=False)

        # Start thread 1 first, give the GIL a cycle so it waits for t2wait
        th2.start()
        sleep(.1)
        th1.start()

        # Wait for the test to finish
        self.join_all()

        # T2 should have successfully barged T1, so T1 should have been re-run
        # The final value of the ref should be 888, as T1 ran last
        self.assertEqual(self.t1counter, 2)
        self.assertEqual(self.t2counter, 1)
        self.assertEqual(self.refA.deref(), 888)

        self.d("Final value of ref: %s and t1 ran %s times" % (self.refA.deref(), self.t1counter))

    def testCommutes(self):
        # Make sure multiple transactions that occur simultaneously each commute the same ref
        # Hard to check for this behaving properly---it should have fewer retries than if each 
        # transaction did an alter, but if transactions commit at the same time one might have to retry
        # anyway

        self.numruns = AtomicInteger()
        self.numalterruns = AtomicInteger()

        def adder(curval):
            return curval + 1

        def t1():
            self.numruns.getAndIncrement()
            self.refA.commute(adder, None)

        def t2():
            self.numalterruns.getAndIncrement()
            self.refB.alter(adder, None)

        self.refA = Ref(0, None)
        self.refB = Ref(0, None)
        for i in range(15):
            self.runTransactionInThread(t1)

        self.join_all()
        print("Commute took %s runs" % self.numruns.get())

        for i in range(15):
            self.runTransactionInThread(t2)

        self.join_all()

        print("Alter took %s" % self.numalterruns.get())
        self.assertTrue(self.numalterruns.get() > self.numruns.get())