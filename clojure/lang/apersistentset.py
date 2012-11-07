"""
March 25, 2012 -- documented
"""

import cStringIO

import clojure.lang.rt as RT
from clojure.lang.ifn import IFn
from clojure.lang.iprintable import IPrintable
from clojure.lang.apersistentmap import createKeySeq
from clojure.lang.ipersistentset import IPersistentSet
from clojure.lang.cljexceptions import ArityException


class APersistentSet(IPersistentSet, IFn, IPrintable):
    """An unordered collection of objects.

    Ordered set implementation:
    http://code.activestate.com/recipes/576694/

    Duplicate items are not permitted."""
    def __init__(self, impl):
        """Instantiate an APersistentSet

        This should not be called directly. See: PersistentHashSet.

        impl -- a PersistentHashMap"""
        self.impl = impl
        self._hash = -1

    def __getitem__(self, item):
        """Return item if found in this set, else None.

        item -- any object"""
        return self.impl[item]

    def __contains__(self, item):
        """Return True if item is found in this set, else False"""
        return item in self.impl

    def __len__(self):
        """Return the number of items in this APersistentSet."""
        return len(self.impl)

    def seq(self):
        """Return a KeySeq containing the items in this set."""
        return createKeySeq(self.impl.seq())

    def __call__(self, *args):
        """Return the single item in args if found in this set, else None.

        args -- must be one object"""
        if len(args) != 1:
            raise ArityException()
        return self.impl[args[0]]

    def __eq__(self, other):
        """Return True if:

        * self is other
        * other is an IPersistentSet and
          * both sets contain the same items"""
        if self is other:
            return True

        if not isinstance(other, IPersistentSet):
            return False

        if len(self) != len(other):
            return False

        for s in self.impl:
            if s not in other or not other[s] == self[s]:
                return False
        return True

    def __ne__(self, other):
        "Return not self.__eq__(other)"
        return not self == other

    def __hash__(self):
        """Return the hash of this set.

        The hash is computed as the sum of the hash of all items. If the set
        is empty, the hash is 0."""
        if self._hash == -1:
            hsh = 0
            s = self.seq()
            while s is not None:
                e = s.first()
                hsh += hash(e)
                s = s.next()
            self._hash = hsh
        return self._hash

    def writeAsString(self, writer):
        """Write #{...} to writer.

        writer -- a write-able object

        Where ... is a single space delimited list of the objects in this
        set."""
        writer.write("#{")
        s = self.seq()
        while s is not None:
            RT.protocols.writeAsString(s.first(), writer)
            if s.next() is not None:
                writer.write(" ")
            s = s.next()
        writer.write("}")

    def writeAsReplString(self, writer):
        """Write #{...} to writer.

        writer -- a write-able object

        Where ... is a single space delimited list of the objects in this
        set. The string may be read by the clojure-py reader."""
        writer.write("#{")
        s = self.seq()
        while s is not None:
            RT.protocols.writeAsReplString(s.first(), writer)
            if s.next() is not None:
                writer.write(" ")
            s = s.next()
        writer.write("}")

    def __str__(self):
        """Return a string representation of this set.

        The set will be formatted as a Python set would be:

        set([contents])"""
        s = []
        sq = self.seq()
        while sq is not None:
            s.append(str(sq.first()))
            sq = sq.next()
        if not s:
            return "set()"
        else:
            return "set([" + ", ".join(s) + "])"

    def __repr__(self):
        """Return a string representation of this set.

        An APersistentSet has no Python readable representation. The
        *semantic* validity of the resulting set is unknown."""
        sio = cStringIO.StringIO()
        self.writeAsReplString(sio)
        return "<{0}.{1} object at 0x{2:x} {3}>".format(self.__module__,
                                                        type(self).__name__,
                                                        id(self),
                                                        sio.getvalue())
