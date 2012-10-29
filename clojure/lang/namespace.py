from clojure.lang.cljexceptions import (InvalidArgumentException,
                                        IllegalArgumentException)
from clojure.lang.symbol import Symbol
from clojure.lang.var import Var
import clojure.standardimports as stdimps
import sys
from types import ModuleType


class Namespace(ModuleType):
    def __new__(cls, name):
        """Returns a namespace with a given name, creating it if needed.

        Adds standard imports to a module without clojure.core.
        clojure.core is special-cased: being the first created module, some
        specific Vars must be added "by hand" (currently: *ns*,
        *command-line-args*).
        """
        if isinstance(name, Symbol):
            name = name.name
        if not sys.modules.get(name):
            mod = ModuleType.__new__(cls)
            ModuleType.__init__(mod, name)
            mod.__file__ = "<interactive namespace>"
            for i in dir(stdimps):
                if i.startswith("_"):
                    continue
                setattr(mod, i, getattr(stdimps, i))
            if mod.__name__ == "clojure.core":
                setattr(mod, "*ns*", Var(mod, "*ns*", mod).setDynamic())
                setattr(mod, "*command-line-args*",
                        Var(mod, "*command-line-args*", None).setDynamic())
            sys.modules[name] = mod
        return sys.modules[name]

    def __init__(self, name):
        """Bypasses ModuleType.__init__.
        """


def findNS(name, fromns=None):
    """Finds a namespace, possibly as an defined as an alias in another one.
    """
    symbol_name = Symbol(name)
    if symbol_name in getattr(fromns, "__aliases__", {}):
        return fromns.__aliases__[symbol_name]
    return sys.modules.get(str(name))
    # module =  sys.modules.get(str(name))
    # if module == None:
    #     parts = name.split(".")
    #     clss = parts.pop()
    #     modname = ".".join(parts)
    #     module = sys.modules.get(modname)
    #     if module:
    #         # remodule.__dict__[clss]
    #         print "Got module and name: %s %s" % (module, clss)
    #     return module
    # else:
    #     return module

def remove(ns):
    """Removes a namespace from sys.modules.
    """
    name = findNS(ns).__name__
    if name == "clojure.core":
        raise IllegalArgumentException("Cannot remove clojure.core namespace")
    del sys.modules[name]


def intern(ns, sym):
    """Interns a non-ns-qualified Symbol in a namespace.
    """
    sym = Symbol(sym)
    if sym.ns is not None:
        raise InvalidArgumentException(
            "Can't intern namespace-qualified symbol")
    if not isinstance(ns, ModuleType):
        raise InvalidArgumentException
    v = getattr(ns, str(sym), None)
    if v is not None:
        if not isinstance(v, Var):
            raise Exception(
                "Can't redefine {0} as {1}: is not Var".format(v, sym))
        if ns.__name__ == v.ns.__name__:
            return v
    v = Var(ns, sym)
    setattr(ns, sym.name, v)
    return v


def findItem(ns, sym):
    """Resolves a Symbol, ns-qualified or not, in a namespace.

    None is returned if the Symbol cannot be resolved.
    """
    if sym.ns == ns.__name__:
        return getattr(ns, sym.name, None)
    if sym.ns is not None:
        mod = findNS(sym.ns, fromns=ns)
        return getattr(mod, sym.name, None)
    return getattr(ns, sym.name, None)

