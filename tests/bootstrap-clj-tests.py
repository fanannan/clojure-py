import os.path
import sys
if (sys.version_info.major, sys.version_info.minor) >= (2, 7):
    import unittest
else:
    import unittest2 as unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from clojure.lang.cljkeyword import Keyword
from clojure.lang.namespace import Namespace, findItem
from clojure.lang.var import Var, threadBindings
from clojure.lang.symbol import Symbol
from clojure.main import requireClj


_NS_ = findItem(Namespace("clojure.core"), Symbol("*ns*"))


def mapTest(ns, var):
    class Test(unittest.TestCase):
        def testVar(self):
            with threadBindings({_NS_: var.ns}):
                var()

    name = ns + str(var)
    tst = Test
    tst.__name__ = name
    globals()[name] = tst


for x in os.listdir(os.path.dirname(__file__)):
    if x.endswith(".clj") and x.find("test") >= 0:
        print "Reading tests from",  x
        requireClj(os.path.join(os.path.dirname(__file__),x))
        folder, file = os.path.split(x)
        ns, ext = os.path.splitext(x)
        module = sys.modules["tests."+ns]

        for idx in dir(module):
            var = getattr(module, idx)
            if isinstance(var, Var) and str(var).endswith("tests"):
                meta = var.meta()
                if meta and meta[Keyword("test")]:
                    mapTest(ns, var) 

