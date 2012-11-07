import cStringIO

from clojure.lang.aseq import ASeq
from clojure.lang.cljexceptions import ArityException
from clojure.lang.persistentlist import EMPTY
import clojure.lang.rt as RT


class Cons(ASeq):
    def __init__(self, *args):
        """Instantiate a Cons.

        args must be one of:

        * object, ISeq
          head and tail, respectively

        * IPersistentMap, object, ISeq
          meta data, head and tail

        Else an ArityException will be raised."""
        if len(args) == 2:
            self._meta = None
            self._first = args[0]
            self._more = args[1]
        elif len(args) == 3:
            self._meta = args[0]
            self._first = args[1]
            self._more = args[2]
        else:
            raise ArityException()

    def first(self):
        """Return the first item in this cons."""
        return self._first

    def next(self):
        """Return an ISeq or None.

        If there is one item in the cons, return None, else return an ISeq on
        the items after the first."""
        return self.more().seq()

    def more(self):
        """Return an ISeq,

        If there is more than one item in the cons, return the items after
        the first, else return an empty PersistentList."""
        if self._more is None:
            return EMPTY
        return self._more

    def withMeta(self, meta):
        """Return a Cons with meta attached.

        meta -- IPersistentMap

        The returned cons will share the head and tail with this cons."""
        return Cons(meta, self._first, self._more)

    def __len__(self):
        """Return the number of items in this Cons."""
        c = 0
        while self is not None:
            c += 1
            self = self.next()
        return c
