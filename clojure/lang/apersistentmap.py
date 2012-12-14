
import clojure.lang.rt as RT
from clojure.lang.aseq import ASeq
from clojure.lang.mapentry import MapEntry
from clojure.lang.iprintable import IPrintable
from clojure.lang.ipersistentmap import IPersistentMap
from clojure.lang.ipersistentset import IPersistentSet
from clojure.lang.ipersistentvector import IPersistentVector
from clojure.lang.cljexceptions import (ArityException,
                                        InvalidArgumentException)


class APersistentMap(IPersistentMap, IPrintable):
    def cons(self, o):
        if isinstance(o, MapEntry):
            return self.assoc(o.getKey(), o.getValue())
        if isinstance(o, IPersistentVector):
            if len(o) != 2:
                raise InvalidArgumentException("Vector arg to map conj must "
                                               "be a pair")
            return self.assoc(o[0], o[1])
        ret = self
        s = o.seq()
        while s is not None:
            e = s.first()
            ret = ret.assoc(e.getKey(), e.getValue())
            s = s.next()
        return ret

    def toDict(self):
        s = self.seq()
        d = {}
        while s is not None:
            d[s.first().getKey()] = s.first().getValue()
            s = s.next()
        return d

    def __eq__(self, other):
        if self is other:
            return True
        if isinstance(other, (IPersistentSet, IPersistentVector)):
            return False
        try:
            return (len(self) == len(other) and
                    all(s in other and other[s] == self[s] for s in self))
        except TypeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __getitem__(self, item):
        return self.valAt(item)

    def __iter__(self):
        s = self.seq()
        while s is not None:
            if s.first() is None:
                pass
            yield s.first().getKey()
            s = s.next()

    # def __hash__(self):
    #     return mapHash(self)

    def __call__(self, *args, **kwargs):
        return apply(self.valAt, args)

    def __contains__(self, item):
        return self.containsKey(item)

    def writeAsString(self, writer):
        writer.write("{")
        s = self.seq()
        while s is not None:
            e = s.first()
            RT.protocols.writeAsString(e.getKey(), writer)
            writer.write(" ")
            RT.protocols.writeAsString(e.getValue(), writer)
            if s.next() is not None:
                writer.write(", ")
            s = s.next()
        writer.write("}")

    def writeAsReplString(self, writer):
        writer.write("{")
        s = self.seq()
        while s is not None:
            e = s.first()
            RT.protocols.writeAsReplString(e.getKey(), writer)
            writer.write(" ")
            RT.protocols.writeAsReplString(e.getValue(), writer)
            if s.next() is not None:
                writer.write(", ")
            s = s.next()
        writer.write("}")


def mapHash(m):
    return reduce(lambda h, v: h + (0 if v.getKey() is None
                                      else hash(v.getKey()))
                                 ^ (0 if v.getValue() is None
                                      else hash(v.getValue())),
                  m.interator(),
                  0)


class KeySeq(ASeq):
    def __init__(self, *args):
        if len(args) == 1:
            self._seq = args[0]
        elif len(args) == 2:
            self._meta = args[0]
            self._seq = args[1]
        else:
            raise ArityException()

    def first(self):
        return self._seq.first().getKey()

    def next(self):
        return createKeySeq(self._seq.next())

    def withMeta(self, meta):
        return KeySeq(meta, self._seq)

    def __iter__(self):
        s = self
        while s is not None:
            yield s.first()
            s = s.next()


def createKeySeq(s):
    if s is None:
        return None
    return KeySeq(s)


class ValueSeq(ASeq):
    def __init__(self, *args):
        if len(args) == 1:
            self._seq = args[0]
        elif len(args) == 2:
            self._meta = args[0]
            self._seq = args[1]
        else:
            raise ArityException()

    def first(self):
        return self._seq.first().getValue()

    def next(self):
        return createValueSeq(self._seq.next())

    def withMeta(self, meta):
        return ValueSeq(meta, self._seq)

    def __iter__(self):
        s = self
        while s is not None:
            yield s.first()
            s = s.next()


def createValueSeq(s):
    if s is None:
        return None
    return ValueSeq(s)
