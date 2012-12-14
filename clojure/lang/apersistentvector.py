import cStringIO

import clojure.lang.rt as RT
from clojure.lang.iobj import IObj
from clojure.lang.iprintable import IPrintable
from clojure.lang.indexableseq import IndexableSeq
from clojure.lang.ipersistentmap import IPersistentMap
from clojure.lang.ipersistentset import IPersistentSet
from clojure.lang.ipersistentvector import IPersistentVector
from clojure.lang.cljexceptions import ArityException
from clojure.lang.cljexceptions import IndexOutOfBoundsException


class APersistentVector(IPersistentVector, IPrintable):
    """Pseudo-Abstract class to define a persistent vector.

    For concrete classes see: PersistentVector, MapEntry, and SubVec."""
    def __iter__(self):
        """Return an iterator on this vector."""
        for x in range(len(self)):
            yield self.nth(x)

    def peek(self):
        """Return the last item in this vector or None if empty."""
        if len(self):
            return self.nth(len(self) - 1)
        return None

    def __getitem__(self, index):
        """Return the item at index.

        index -- integer"""
        return self.nth(index)

    def seq(self):
        """Return an IndexableSeq on this vector or None if empty."""
        if not len(self):
            return None
        return IndexableSeq(self, 0)

    def count(self):
        return len(self)

    def __eq__(self, other):
        """Equality test.

        other -- ISeq or something that implements the seq protocol

        ASeq.__eq__ is actually used."""
        return (self is other or
                (RT.isSeqable(other) and
                 not isinstance(other, (IPersistentSet, IPersistentMap)) and
                 self.seq() == RT.seq(other)))

    def __hash__(self):
        """Return the hash on this vector or 1 if the vector is empty.

        See: ASeq.hasheq()"""
        s = self.seq()
        if not s is None:
            return s.hasheq();
        else:
            return 1            # EmptyList.__hash__() => 1

    def __ne__(self, other):
        """Return not self.__eq__(other)"""
        return not self == other

    # Placing these print methods here will cover:
    # MapEntry, PersistentVector, and SubVec

    def writeAsString(self, writer):
        """Write [...] to writer.

        writer -- a write-able object

        Where ... is a single space delimited list of the objects in this
        vector."""
        writer.write("[")
        s = self.seq()
        while s is not None:
            RT.protocols.writeAsString(s.first(), writer)
            if s.next() is not None:
                writer.write(" ")
            s = s.next()
        writer.write("]")

    def writeAsReplString(self, writer):
        """Write [...] to writer.

        writer -- a write-able object

        Where ... is a single space delimited list of the objects in this
        vector. The string may be read by the clojure-py reader."""
        writer.write("[")
        s = self.seq()
        while s is not None:
            RT.protocols.writeAsReplString(s.first(), writer)
            if s.next() is not None:
                writer.write(" ")
            s = s.next()
        writer.write("]")

    def __str__(self):
        """Return a string representation of this vector.

        The vector will be formatted as a Python list.
        """
        s = []
        for x in self:
            s.append(str(x))
        return "[" + ", ".join(s) + "]"

    def __repr__(self):
        """Return a string representation of this vector.

        A persistent vector has no Python readable representation. The
        *semantic* validity of the resulting list is unknown."""
        sio = cStringIO.StringIO()
        self.writeAsReplString(sio)
        return "<{0}.{1} object at 0x{2:x} {3}>".format(self.__module__,
                                                        type(self).__name__,
                                                        id(self),
                                                        sio.getvalue())

# ======================================================================
# SubVec
# ======================================================================

class SubVec(APersistentVector):
    """A fixed *window* on an APersistentVector."""
    def __init__(self, meta, v, start, end):
        """Instantiate a SubVec.

        meta -- IPersistentMap, meta data to attach
        v -- IPersistentVector, parent
        start -- int, start index into v
        end -- int, end index into v"""
        self._meta = meta
        if isinstance(v, SubVec):
            start += v.start
            end += v.start
            v = v.v
        self.v = v
        self.start = start
        self.end = end

    def nth(self, i):
        """Return the i'th item.

        i -- integer >= 0

        May raise an IndexOutOfBoundsException"""
        if i < 0 or self.start + i >= self.end:
            raise IndexOutOfBoundsException()
        return self.v.nth(self.start + i)

    def assocN(self, i, val):
        """Return a PersistentVector or SubVec.

        i -- integer >= 0
        val -- any object

        If i is within bounds of this SubVec, return a SubVec that shares data
        with this vec but with the item at i set to val. If i is equal to the
        length of this SubVec, return a PersistentVector that shares data with
        this SubVec and has val appended. Else, raise
        IndexOutOfBoundsException.  The returned vector will have this
        vector's meta data attached."""
        if i < 0 or self.start + i > self.end:
            raise IndexOutOfBoundsException()
        elif self.start + i == self.end:
            return self.cons(val)
        return SubVec(self._meta,
                      self.v.assocN(self.start + i, val),
                      self.start,
                      self.end)

    def __len__(self):
        """Return the number of items in this SubVec."""
        return self.end - self.start

    def cons(self, o):
        """Return a new SubVec with this vec's contents and o appended."""
        return SubVec(self._meta,
                      self.v.assocN(self.end, o),
                      self.start,
                      self.end + 1)

    def empty(self):
        """Return an empty PersistentVector.

        The new vector will have this vec's meta data attached.'"""
        from clojure.lang.persistentvector import EMPTY as EMPTY_VECTOR
        return EMPTY_VECTOR.withMeta(self.meta())

    def pop(self):
        """Return a vector with all but the last item in this vector omitted.

        If this SubVec contains one item, return an empty Persistentvector.
        Else return a SubVector."""
        from clojure.lang.persistentvector import EMPTY as EMPTY_VECTOR
        if self.end - 1 == self.start:
            return EMPTY_VECTOR
        return SubVec(self._meta, self.v, self.start, self.end - 1)

    def withMeta(self, meta):
        """Return a new SubVec with meta attached.

        meta -- an IPersistentMap

        The new vec will have the same contents as this vector."""
        if self._meta == meta:
            return self
        return SubVec(self._meta, self.v, self.start, self.end)

    def meta(self):
        """Return this vector's meta data, which may be None."""
        return self._meta
