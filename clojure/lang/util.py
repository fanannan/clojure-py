from clojure.lang.cljexceptions import (AbstractMethodCall,
                                        InvalidArgumentException)
from clojure.lang.mapentry import MapEntry
import clojure.lang.rt as RT

# Needed by both ref.py and lockingtransaction, so they can't depend on each other
class TVal:
    def __init__(self, val, point, msecs, prev = None):
        self.val = val
        self.point = point
        self.msecs = msecs

        # If we are passed prev, add ourselves to the end of the linked list
        if prev:
            self.prev = prev
            self.next = prev.next
            self.prev.next = self
            self.next.prev = self
        else:
            self.prev = self
            self.next = self

def hashCombine(hash, seed): # FIXME - unused argument?
    seed ^= seed + 0x9e3779b9 + (seed << 6) + (seed >> 2)
    return seed

def hasheq(o):
    raise AbstractMethodCall()

def conjToAssoc(self, o):
    if isinstance(o, MapEntry):
        return self.assoc(o.getKey(), o.getValue())
    if hasattr(o, "__getitem__") and hasattr(o, "__len__"):
        if len(o) != 2:
            raise InvalidArgumentException("Vector arg must be a pair")
        return self.assoc(o[0], o[1])

    s = RT.seq(o)
    map = self
    for s in s.interator():
        m = s.first()
        map = map.assoc(m.getKey(), m.getValue())
    return map


def bitCount(i):
    i -= ((i >> 1) & 0x55555555)
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333)
    return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24


def arrayCopy(src, srcPos, dest, destPos, length):
    dest[destPos:length] = src[srcPos:length]
