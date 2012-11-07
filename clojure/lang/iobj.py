from abc import ABCMeta, abstractmethod

from clojure.lang.cljexceptions import AbstractMethodCall


class IObj(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def withMeta(self, meta):
        raise AbstractMethodCall(self)
