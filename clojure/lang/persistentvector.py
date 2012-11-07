import clojure.lang.rt as RT
from clojure.lang.atomicreference import AtomicReference
from clojure.lang.apersistentvector import APersistentVector
from clojure.lang.cljexceptions import (ArityException,
                                        IllegalStateException,
                                        IndexOutOfBoundsException)


# Acts sort-of-like supplied-p in Common Lisp.
# Allows one to ascertain if a parameter foo=None was actually given specified
# in the call.
_notSupplied = object()


class PersistentVector(APersistentVector):
    """An indexable array where each operation such as cons and assocN return
    a *new* PersistentVector. The two vectors share old state, but the new
    state is only present in the newly returned vector. This preserves the
    state of the old vector prior to the operation.

    _cnt -- integer, the total number of items in the vector
    _shift -- integer, the depth of the tree, a multiple of 5, >=0
    _root -- Node, the root tree node
    _tail -- list of size 0 to 32
    _meta -- IPersistentHashMap, meta data attached to the vector"""
    def __init__(self, *args):
        """Instantiate a PersistentVector

        args must be one of:

        * int, int, Node, list
        * IPersistentHashMap, int, int, Node, list

        Else an ArityException will be raised.
        """
        if len(args) == 4:
            cnt, shift, root, tail = args
            meta = None
        elif len(args) == 5:
            meta, cnt, shift, root, tail = args
        else:
            raise ArityException()
        self._meta = meta
        self._cnt = cnt
        self._shift = shift
        self._root = root
        self._tail = tail

    def __call__(self, i):
        """Return the item at i.

        i -- integer >= 0

        May raise IndexOutOfBoundsException."""
        return self.nth(i)

    def _arrayFor(self, i):
        """Return the _tail list or a Node._array list.

        i -- integer, vector index >= 0

        The list will be a sublist of this vector where the index of its first
        element is i - i % 32. May raise IndexOutOfBoundsException."""
        if 0 <= i < self._cnt:
            if i >= self._tailoff():
                return self._tail
            node = self._root
            for level in range(self._shift, 0, -5):
                node = node._array[(i >> level) & 0x01f]
            return node._array
        raise IndexOutOfBoundsException()

    def nth(self, i, notFound=_notSupplied):
        """Return the item at index i.

        If i is out of bounds and notFound is supplied, return notFound, else
        raise IndexOutOfBoundsException."""
        if 0 <= i < self._cnt:
            node = self._arrayFor(i)
            return node[i & 0x01f]
        elif notFound is _notSupplied:
            raise IndexOutOfBoundsException()
        else:
            return notFound

    def meta(self):
        """Return this vector's meta data as an IPersistentHashMap."""
        return self._meta

    def assocN(self, i, val):
        """Return a PersistentVector with the item at index i set to val.

        i -- integer >= 0
        val -- any object

        The returned vector will contain all the items in this vector except
        for the newly *changed* value. This function will *append* val if i is
        the length of this vector. If i is > the length, raise
        IndexOutOfBoundsException. The returned vector will have this vector's
        meta data attached."""
        if 0 <= i < self._cnt:
            if i >= self._tailoff():
                newTail = self._tail[:]
                newTail[i & 0x01f] = val

                return PersistentVector(self.meta(), self._cnt, self._shift,
                                        self._root, newTail)

            n = _doAssoc(self._shift, self._root, i, val)
            return PersistentVector(self.meta(), self._cnt, self._shift, n,
                                    self._tail)
        if i == self._cnt:
            return self.cons(val)

        raise IndexOutOfBoundsException()

    def assoc(self, i, val):
        return self.assocN(i, val)

    def __len__(self):
        """Return the number of items in this vector."""
        return self._cnt

    def withMeta(self, meta):
        """Return a PersistentVector.

        meta -- an IPersistentMap

        The returned vector will contain this vectors contents and have meta
        attached."""
        return PersistentVector(meta, self._cnt, self._shift, self._root,
                                self._tail)

    def _tailoff(self):
        """Return the beginning index of the tail.

        This will be a multiple of 32, >= 0."""
        if self._cnt < 32:
            return 0
        return ((self._cnt - 1) >> 5) << 5

    def cons(self, val):
        """Return a new PersistentVector.

        val -- any object

        The returned vector contains all the items in this vector with val
        *appended*. It will also have this vector's meta data attached."""
        # there's room in the _tail for var
        if self._cnt - self._tailoff() < 32:
            newTail = self._tail[:]
            newTail.append(val)
            return PersistentVector(self.meta(), self._cnt + 1, self._shift,
                                    self._root, newTail)
        # _tail is full, have to create a new Node
        tailnode = Node(self._root._edit, self._tail)
        newshift = self._shift
        # no room at this level for the Node, add a new level
        if (self._cnt >> 5) > (1 << self._shift):
            newroot = Node(self._root._edit)
            newroot._array[0] = self._root
            newroot._array[1] = _newPath(self._root._edit, self._shift,
                                         tailnode)
            newshift += 5
        # room at this level for the new Node
        else:
            newroot = self._pushTail(self._shift, self._root, tailnode)

        return PersistentVector(self.meta(), self._cnt + 1, newshift, newroot,
                                [val])

    def _pushTail(self, level, parent, tailnode):
        """Add tailnode to the tree at the given level.

        level -- what level to push to, a multiple of 5, >= 5
        parent -- Node, the old root
        tailnode -- Node to push containing a full _array of the last 32 items
                    in the vector.

        Return the new root Node."""
        # the index of the next empty _array @ the given level
        subidx = ((self._cnt - 1) >> level) & 0x01f
        ret = Node(parent._edit, parent._array[:])

        if level == 5:
            nodeToInsert = tailnode
        else:
            child = parent._array[subidx]
            nodeToInsert = (self._pushTail(level - 5, child, tailnode)
                            if child is not None
                            else _newPath(self._root._edit, level - 5,
                                          tailnode))
        ret._array[subidx] = nodeToInsert
        return ret

    def empty(self):
        """Return an empty PersistentVector.

        The returned vector will have this vector's meta data attached."""
        return EMPTY.withMeta(self.meta())

    def pop(self):
        """Return a new PersistentVector.

        The returned vector will contain all the items it this vector except
        the *last* item. It will have this vector's meta data attached.

        Will raise IllegalStateException if this vector is empty."""
        if not self._cnt:
            raise IllegalStateException("Can't pop empty vector")
        if self._cnt == 1:
            return EMPTY.withMeta(self.meta())
        # pop from the _tail, done
        if self._cnt - self._tailoff() > 1:
            newTail = self._tail[:]
            newTail.pop()
            return PersistentVector(self.meta(), self._cnt - 1, self._shift,
                                    self._root, newTail)
        # the last sublist, post-pop
        newtail = self._arrayFor(self._cnt - 2)
        # pop from the last Node, done
        newroot = self._popTail(self._shift, self._root)
        newshift = self._shift
        if newroot is None:
            newroot = EMPTY_NODE
        if self._shift > 5 and newroot._array[1] is None:
            newroot = newroot._array[0]
            newshift -= 5
        return PersistentVector(self.meta(), self._cnt - 1, newshift, newroot,
                                newtail)

    def _popTail(self, level, node):
        """Return a new root Node or None if the last value was popped.

        level -- integer, 0 or a multiple of 5
        node -- Node to pop from

        Recursively descend the tree, starting at node and remove the value at
        _cnt - 1."""
        subidx = ((self._cnt - 2) >> level) & 0x01f
        if level > 5:
            newchild = self._popTail(level - 5, node._array[subidx])
            if newchild is None and not subidx:
                return None
            else:
                ret = Node(self._root._edit, node._array[:])
                ret._array[subidx] = newchild
                return ret
        elif not subidx:
            return None
        else:
            ret = Node(self._root._edit, node._array[:])
            ret._array[subidx] = None
            return ret

# ======================================================================
# PersistentVector Helpers
# ======================================================================

def _newPath(edit, level, node):
    """Return a Node.

    edit -- currently unused (for Clojure's transient data structures)
    level -- integer, multiple of 5, >= 5, stop recurring when 0
    node -- Node, the new path will lead *to* this node

    Called by PersistentVector.cons() (and indirectly by
    PersistentVector._pushTail()) when the leaves in the tree are full (a new
    level is required)."""
    if not level:
        return node
    ret = Node(edit)
    ret._array[0] = _newPath(edit, level - 5, node)
    return ret


def _doAssoc(level, node, i, val):
    """Return a new root Node with the item at index i set to val.

    level -- integer, multiple of 5, >== 5, stop recurring when 0
    node -- Node, the old root
    i -- integer >= 0, index of item to set
    val -- any object, the item to place at i"""
    ret = Node(node._edit, node._array[:])
    if not level:
        ret._array[i & 0x01f] = val
    else:
        subidx = (i >> level) & 0x01f
        ret._array[subidx] = _doAssoc(level - 5, node._array[subidx], i, val)
    return ret

# ======================================================================
# PersistentVector Tree Node
# ======================================================================

class Node(object):
    """A tree node in a PersistentVector."""
    def __init__(self, edit, array=None):
        """Instantiate a Node.

        edit -- currently unused (for Clojure's transient data structures)
        array -- An optional list of size 32. It will be initialized to [None]
                 * 32 if not supplied."""
        self._edit = edit
        self._array = array if array is not None else [None] * 32

# ======================================================================
# PersistentVector Creation
# ======================================================================

def vec(seq):
    """Return a PersistentVector.

    seq -- ISeq

    If seq is an APersistentVector return seq. Else the returned vector will
    contain the items in seq."""
    if isinstance(seq, APersistentVector):
        return seq
    s = RT.seq(seq)
    v = EMPTY
    while s is not None:
        v = v.cons(RT.first(s))
        s = RT.next(s)
    return v


def create(*args):
    """Return a PersistentVector.

    args -- zero or more objects

    The returned vector will contain all objects found in args."""
    x = EMPTY
    for z in args:
        x = x.cons(z)
    return x

# ======================================================================
# Pseudo-Singletons
# ======================================================================

# currently unused (for Clojure's transient data structures)
NOEDIT = AtomicReference()
# A Node holding no children or vector values
EMPTY_NODE = Node(NOEDIT)
# A PersistentVector containing 0 items
EMPTY = PersistentVector(0, 5, EMPTY_NODE, [])
