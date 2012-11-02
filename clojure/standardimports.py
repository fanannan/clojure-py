"""Lists the symbols names that should be defined in all clojure namespaces.
"""

from clojure.lang.persistentlist import PersistentList
from clojure.lang.ipersistentlist import IPersistentList
from clojure.lang.ipersistentvector import IPersistentVector
from clojure.lang.ipersistentcollection import IPersistentCollection
from clojure.lang.ipersistentmap import IPersistentMap
from clojure.lang.ilookup import ILookup
from clojure.lang.associative import Associative
from clojure.lang.ideref import IDeref
from clojure.lang.seqable import Seqable
from clojure.lang.atom import Atom
from clojure.lang.ref import Ref
from clojure.lang.iobj import IObj
from clojure.lang.lockingtransaction import LockingTransaction

from clojure.lang.iseq import ISeq
from clojure.lang.var import Var
from clojure.lang.cljexceptions import *
from clojure.lang.sequential import Sequential
import clojure

import dis
import sys

sys.path
