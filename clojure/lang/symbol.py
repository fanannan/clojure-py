from clojure.lang.iprintable import IPrintable
from clojure.lang.iobj import IObj
from clojure.lang.cljexceptions import ArityException
from clojure.lang.named import Named


class Symbol(IObj, IPrintable, Named):
    def __init__(self, *args):
        """Symbol initializer.
        
        Valid calls:
        - Symbol(symbol) -- copy,
        - Symbol([[mapping,] str,], str) -- metadata, namespace, name.
        """
        if len(args) == 1:
            arg = args[0]
            if isinstance(arg, Symbol):
                self._meta, self.ns, self.name = arg._meta, arg.ns, arg.name
            else:
                self._meta = None
                idx = arg.rfind("/")
                if idx == -1 or arg == "/":
                    self.ns = None
                    self.name = arg
                else:
                    self.ns = arg[:idx]
                    self.name = arg[idx + 1:]
        elif len(args) == 2:
            self._meta = None
            self.ns, self.name = args
        elif len(args) == 3:
            self._meta, self.ns, self.name = args
        else:
            raise ArityException()

    def getNamespace(self):
        return self.ns

    def getName(self):
        return self.name

    def withMeta(self, meta):
        if meta is self.meta():
            return self
        return Symbol(meta, self.ns, self.name)

    def meta(self):
        return self._meta

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Symbol):
            return False
        return (self.ns == other.ns) and (self.name == other.name)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.name) ^ hash(self.ns)

    def writeAsString(self, writer):
        writer.write(repr(self))

    def writeAsReplString(self, writer):
        writer.write(repr(self))

    def __repr__(self):
        if self.ns is None:
            return self.name
        else:
            return self.ns + "/" + self.name
