import cStringIO

import clojure.lang.rt as RT
from clojure.lang.obj import Obj
from clojure.lang.iseq import ISeq
from clojure.lang.counted import Counted
from clojure.lang.ihasheq import IHashEq
from clojure.lang.iterable import Iterable
from clojure.lang.iprintable import IPrintable
from clojure.lang.sequential import Sequential
from clojure.lang.ipersistentmap import IPersistentMap
from clojure.lang.ipersistentset import IPersistentSet
from clojure.lang.cljexceptions import AbstractMethodCall


class ASeq(Obj, Sequential, ISeq, IHashEq, Iterable, IPrintable):
    def __eq__(self, other):
        if self is other:
            return True
        if not RT.isSeqable(other) or isinstance(other, IPersistentSet):
            return False
        se = RT.seq(other)
        # XXX: don't think this is used
        # if isinstance(se, RT.NotSeq):
        #     print other, type(other)
        #     return False
        ms = self.seq()
        while se is not None:
            if ms is None or not se.first() == ms.first():
                return False
            ms = ms.next()
            se = se.next()
        return ms is None

    def __ne__(self, other):
        """Return not self.__eq__(other)"""
        return not self == other

    # XXX: This is broken, it should raise IndexOutOfBoundsException.
    #      If the nth protocol is going to be used for clojure-py, this should
    #      act like (0, 1)[42] when called from the Python side.
    def __getitem__(self, idx):
        """Return the item at idx or None if idx >= length self."""
        s = self.seq()
        c = 0
        while s is not None:
            if c == idx:
                return s.first()
            c += 1
            s = s.next()
        return None

    def seq(self):
        """Return this sequence (self)."""
        return self

    # XXX: don't think this is used
    # def count(self):
    #     """Return the number of items in this sequence."""
    #     i = 1
    #     for s in self.interator():
    #         if isinstance(s, Counted):
    #             return i + s.count()
    #         i += 1
    #     return i

    def more(self):
        """Return an ISeq.

        If this sequence has one item in it, return an EmptyList, else return
        the tail of this sequence"""
        s = self.next()
        if s is None:
            from clojure.lang.persistentlist import EMPTY
            return EMPTY
        return s

    # def first(self):
    #     """Raise AbstractMethodCall."""
    #     raise AbstractMethodCall(self)

    def __iter__(self):
        """Return an iterator on this sequence."""
        s = self.seq()
        while s is not None:
            yield s.first()
            s = s.next()

    def hasheq(self):
        """Return the hash of this sequence."""
        ret = 1
        for s in self:
            ret = 31 * ret + hash(s)
        return ret

    def cons(self, other):
        """Return a Cons.

        other -- any object

        The Cons will have object as the head, and this sequence as the
        tail."""
        from clojure.lang.cons import Cons
        return Cons(other, self)

    def __str__(self):
        """Return a string representation of this sequence.

        The list will be formatted as a Python tuple.
        """
        s = []
        for x in self:
            s.append(str(x))
        return "(" + ", ".join(s) + ")"

    def __repr__(self):
        """Return a string representation of this sequence.

        An ASeq has no Python readable representation. The
        *semantic* validity of the resulting list is unknown."""
        sio = cStringIO.StringIO()
        self.writeAsReplString(sio)
        return "<{0}.{1} object at 0x{2:x} {3}>".format(self.__module__,
                                                        type(self).__name__,
                                                        id(self),
                                                        sio.getvalue())

    def writeAsString(self, writer):
        """Write (...) to writer.

        writer -- a write-able object

        Where ... is a single space delimited list of the objects in this
        sequence."""
        writer.write("(")
        s = self
        while s is not None:
            RT.protocols.writeAsString(s.first(), writer)
            if s.next() is not None:
                writer.write(" ")
            s = s.next()
        writer.write(")")

    def writeAsReplString(self, writer):
        """Write (...) to writer.

        writer -- a write-able object

        Where ... is a single space delimited list of the objects in this
        sequence. The string may be read by the clojure-py reader."""
        writer.write("(")
        s = self
        while s is not None:
            RT.protocols.writeAsReplString(s.first(), writer)
            if s.next() is not None:
                writer.write(" ")
            s = s.next()
        writer.write(")")
