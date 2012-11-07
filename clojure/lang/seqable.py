from abc import ABCMeta, abstractmethod

from clojure.lang.cljexceptions import AbstractMethodCall


class Seqable(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def seq(self):
        raise AbstractMethodCall(self)
