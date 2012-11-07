from abc import abstractmethod

from clojure.lang.cljexceptions import AbstractMethodCall
from clojure.lang.seqable import Seqable

class IPersistentCollection(Seqable):
    @abstractmethod
    def count(self):
        raise AbstractMethodCall(self)

    @abstractmethod
    def cons(self, o):
        raise AbstractMethodCall(self)

    @abstractmethod
    def empty(self):
        raise AbstractMethodCall(self)
