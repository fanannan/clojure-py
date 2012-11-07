import clojure.lang.rt as RT
from clojure.lang.obj import Obj
from clojure.lang.iseq import ISeq
from clojure.lang.aseq import ASeq
from clojure.lang.ireduce import IReduce
from clojure.lang.counted import Counted
from clojure.lang.seqable import Seqable
from clojure.lang.sequential import Sequential
from clojure.lang.iprintable import IPrintable
from clojure.lang.ipersistentlist import IPersistentList
from clojure.lang.cljexceptions import ArityException, IllegalStateException


class PersistentList(ASeq, IPersistentList, IReduce, Counted):
    def __init__(self, *args):
        """Instantiate a PersistentList.

        Use persistentlist.create() to instantiate on an existing list.

        args must be 1 or 4 items:

        * object
          first and only item in the list

        * IPersistentMap, object, IPersistentList, int
          IPersistentMap -- the meta data
          object -- the first item to put in the list
          IPersistentList -- the tail of the list
          int -- the total number of items put into the list

          May raise ArityException."""
        if len(args) == 1:
            self._first = args[0]
            self._rest = None
            self._count = 1
        elif len(args) == 4:
            self._meta = args[0]
            self._first = args[1]
            self._rest = args[2]
            self._count = args[3]
        else:
            raise ArityException
        # ???: Why here and not in ASeq
        self._hash = -1

    def next(self):
        """Return an ISeq.

        The sequence will contain all but the first item in this list. If the
        list contains only one item, return None."""
        if self._count == 1:
            return None
        return self._rest

    def first(self):
        """Return the first item in this list."""
        return self._first

    def peek(self):
        """Return the first item in this list."""
        return self.first()

    def pop(self):
        """Return an IPersistentList

        If this list contains only one item, return an EmptyList with this
        list's meta data attached. Else return this list omitting the first
        item."""
        if self._rest is None:
            return EMPTY.withMeta(self._meta)
        return self._rest

    def __len__(self):
        """Return the number of items in this list."""
        return self._count
    count = __len__

    def cons(self, o):
        """Return a new PersistentList.

        o -- any object

        The returned list will contain o as the first item and this list's
        items as the rest. Also, this list's meta data will be attached to the
        new list."""
        return PersistentList(self.meta(), o, self, self._count + 1)

    def empty(self):
        """Return an EmptyList with this list's meta data attached."""
        return EMPTY.withMeta(self.meta())

    def reduce(self, *args):
        """Reduce this list to a single value.

        args must be one of:

        * IFn
        * IFn, object

        If this list contains one item, return it. Else, apply the given
        binary function to the first two items in the list. The result of that
        becomes the first argument of the next application of the function
        with the third item as the second argument. This continues until this
        list is exhausted.

        If the second argument is supplied to reduce, it's treated as the
        first item in this list.

        Return the result of this reduction. May raise ArityException."""
        if len(args) == 1:
            ret = self.first()
        elif len(args) == 2:
            ret = args[0](args[1], self.first())
        else:
            raise ArityException()
        fn = args[0]
        s = self.next()
        while s is not None:
            ret = fn(ret, s.first())
            s = s.next()
        return ret

    def withMeta(self, meta):
        """Return a PersistentList.

        meta -- an IPersistentMap

        If meta is equal to this list's meta return this list else return a
        new PersistentList with this lists contents and meta attached."""
        if meta != self.meta():
            return PersistentList(meta, self._first, self._rest, self._count)
        return self

# ======================================================================
# Creation
# ======================================================================

# XXX: don't think this is required
# def create(lst):
#     """Return a PersistentList with the contents of lst.

#     lst -- any object that implements __len__ and __getitem__"""
#     ret = EMPTY
#     for x in range(len(lst) - 1, -1, -1):
#         # c = lst[x]
#         ret = ret.cons(lst[x])
#     return ret


def creator(*args):
    """Return a PersistentList.

    args -- zero or more objectS"""
    ret = EMPTY
    for x in range(len(args) - 1, -1, -1):
        ret = ret.cons(args[x])
    return ret

# ======================================================================
# EmptyList
# ======================================================================

class EmptyList(Obj, IPersistentList, ISeq, Counted, IPrintable):
    """A list of zero objects."""
    def __init__(self, meta=None):
        """This is a psuedo-singleton class, use persistentlist.EMPTY."""
        self._meta = meta

    def __hash__(self):
        """Return the integer 1"""
        return 1

    def __eq__(self, other):
        """Return True if:

        * other is an instance of Sequential, list, or tuple, and
        * other contains no items

        Else return False."""
        return isinstance(other, (Sequential, list, tuple)) \
            and RT.protocols.seq(other) == None

    def __ne__(self, other):
        """Return not self.__eq__(other).

        other -- any object
        """
        return not self == other

    def __iter__(self):
        """Return None"""
        return

    def withMeta(self, meta):
        """Return an EmptyList with meta attached.

        meta -- an IPersistentMap

        If meta is equal to this list's meta return this list else return a
        new EmptyList with meta attached."""
        if self._meta == meta:
            return self
        return EmptyList(meta)

    def first(self):
        """Return None"""
        return None

    def next(self):
        """Return None"""
        return None

    def more(self):
        """Return this PersistentList."""
        return self

    def cons(self, o):
        """Return a new PersistentList.

        o -- any object

        The returned list will contain o as the first item and this list's
        items as the rest. Also, this list's meta data will be attached to the
        new list."""
        return PersistentList(self.meta(), o, None, 1)

    def empty(self):
        """Return this PersistentList."""
        return self

    def peek(self):
        """Return None."""
        return None

    def pop(self):
        """Raise IllegalStateException."""
        raise IllegalStateException("Can't pop an empty list")

    def count(self):
        """Return 0."""
        return 0

    def seq(self):
        """Return None."""
        return None

    def writeAsString(self, writer):
        """See: EmptyList.__str__"""
        writer.write(str(self))

    def writeAsReplString(self, writer):
        """See: EmptyList.__repr__"""
        writer.write(repr(self))

    def __str__(self):
        """Return the string "()"."""
        return "()"

    def __repr__(self):
        """Return the Python readable string "()"."""
        return "()"

    def __len__(self):
        """Return 0."""
        return 0

# ======================================================================
# Pseudo-Singleton
# ======================================================================

EMPTY = EmptyList()
