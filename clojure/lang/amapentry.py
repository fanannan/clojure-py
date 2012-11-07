from clojure.lang.apersistentvector import APersistentVector
from clojure.lang.persistentvector import create as createVector
from clojure.lang.cljexceptions import IndexOutOfBoundsException


class AMapEntry(APersistentVector):
    """An APersistentVector of exactly two items.

    The items are used as the key/value pairs of a an IPersistentMap.

    This is a pseudo-abstract class and should not be directly
    instantiated. See: MapEntry"""
    def __getitem__(self, i):
        """Return the key or value if i is 0 or 1, respectively.

        Raise IndexOutOfBoundsException otherwise."""
        if i == 0:
            return self.getKey()
        elif i == 1:
            return self.getValue()
        else:
            raise IndexOutOfBoundsException()

    def asVector(self):
        """Return a PersistentVector.

        The vector will contain two objects, the key and value."""
        return createVector(self.getKey(), self.getValue())

    def assocN(self, i, val):
        """Return a PersistentVector with index i set to val.

        i -- int, 0 or 1
        val -- any object"""
        return self.asVector().assocN(i, val)

    def __len__(self):
        """Return 2"""
        return 2

    def __contains__(self, x):
        """Return True if x is 0 or 1, False otherwise."""
        if x == 0 or x == 1:
            return True
        return False

    def seq(self):
        """Return an ISeq on this MapEntry."""
        return self.asVector().seq()

    def cons(self, o):
        """Return a PersistentVector.

        o -- any object

        The returned vector will contain this AMapEntry's key and value with o
        appended."""
        return self.asVector().cons(o)

    def empty(self):
        """Return None."""
        return None

    def pop(self):
        """Return a PersistentVector with one item, this AMapEntry's key."""
        return createVector(self.getKey())
