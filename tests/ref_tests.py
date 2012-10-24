"""ref_tests.py

Thursday, Oct. 25 2012"""

import unittest

from clojure.lang.ref import Ref
from clojure.lang.lockingtransaction import LockingTransaction

class TestRef(unittest.TestCase):
    def setUp(self):
        self.ref = Ref(0, None)
    def testEquality_PASS(self):
        
    # def testGetKey_PASS(self):
    #     self.assertEqual(self.mapEntry.getKey(), "key")
    # def testAssocN_FAIL(self):
    #     self.assertRaises(IndexOutOfBoundsException,
    #                       self.mapEntry.assocN, 3, "yek")

