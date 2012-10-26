"""threaded-transaction-tests.py

These tests exercise the LockingTransaction code
in some multithreaded ways.

Friday, Oct. 26 2012"""

import unittest
from threading import Thread, current_thread, Condition
from time import time, sleep
from itertools import count

from clojure.lang.ref import Ref, TVal
from clojure.lang.lockingtransaction import LockingTransaction, TransactionState, Info
from clojure.lang.cljexceptions import IllegalStateException, TransactionRetryException
from clojure.util.shared_lock import SharedLock

##
# Basic idea: We want to test the corner cases when different transactions that happen concurrently on different
# threads run into each other at a specified time. e.g. if Transaction 1 (T1) does an in-transaction-write of Ref 1 (R1)
# and Transaction 2 (T2) that was started later tries to do a write on R1, T2 should be retried.
#
# In order to test cross-thread behaviour this granularly we need a bit of leg-work.
# 


class TestThreadedTransactions(unittest.TestCase):
    spawned_threads = []
    # extra_refs = []

    def setUp(self):
        self.main_thread = current_thread()
        self.first_run = True

    def runTransactionInThread(self, func):
        """
        Runs the desired function in a transaction on a secondary thread
        """
        def thread_func(transaction_func):
            LockingTransaction.runInTransaction(transaction_func)
        t = Thread(target=thread_func, args=[func])
        t.start()
        self.spawned_threads.append(t)

    def wait(self, cond):
        """
        Locks, waits for, and releases the desired condition. If this is not the main thread, 
        it assumes we're in a transaction, and only does so in the initial retry. Unlocks at the end.
        """
        if current_thread() == self.main_thread or self.first_run:
            cond.acquire()
            cond.wait()
            cond.release()
            self.first_run = False

    def notify(self, cond):
        """
        Locks and notifies, then unlocks the desired condition variable
        """
        cond.acquire()
        cond.notify()
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
        self.firstRun = True

        # self.refA =
        def t1():
            # This thread tries to read the value after t2 has written to it, but it starts first
            self.wait(t1wait)
            val = self.refA.deref()
            # Make sure we only successfully got here w/ 1 fault (deref() triggered a retry the first time around)
            self.assertEqual(self.refA._faults.get(), 1)

        def t2():
            # This thread does the committing after t1 started but before it reads the value of refA
            self.refA = Ref(5, None)

        self.runTransactionInThread(t1)
        self.runTransactionInThread(t2)

        # Ok t2 ran, now let t1 continue and fault
        self.notify(t1wait)

        self.join_all()
