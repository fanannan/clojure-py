from aref import ARef
from cljexceptions import ArityException

class Ref(ARef):
    def __init__(self, state, meta=None):
        super(Ref, self).__init__(meta)
        self._state = state

    def deref(self):
        return self._state

    def refSet(self, state):
        """
        """
        self._state = state
        return self._state

    def alter(self, fn, *args):
        """
        """
        self._state = fn(self.deref(), *args[0])
        return self.deref()

    def commute(self, fn, *args):
        """
        """
        self._state = fn(self.deref(), *args[0])
        return self.deref()