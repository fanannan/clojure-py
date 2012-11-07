from clojure.lang.cljexceptions import AbstractMethodCall


class IPrintable(object):
    """Pseudo-Interface to define an object->string protocol.

    An object that subclasses IPrintable must implement theses two methods:

    * writeAsString
    * writeAsReplString

    The behavior of these methods is described in their doc strings but both
    of these methods should adhere to a few suggestions:

    * Don't flush the writer
    * Don't pretty-print to the writer
    * Don't write leading or trailing white space
      (including a trailing newline)"""

    def writeAsString(self, writer):
        """Write a user-friendly string to writer.

        writer -- a writable stream such as sys.stdout, StringIO, etc.

        This function mimics the Python __str__ method. If the printed
        representation is the same at the Python repl and at the clojure-py
        repl, this method could simply write str(self)."""
        raise AbstractMethodCall(self)

    def writeAsReplString(self, writer):
        """Write a string readable by the clojure-py reader to writer.

        writer -- a writable stream such as sys.stdout, StringIO, etc.

        This function mimics the Python __repl__ method. But, we're writing a
        clojure-py readable string, *not* a Python readable string. If the
        object is unreadable it must have the form:

        #<fully.qualified.Name object at 0xADDRESS foo>

        The last foo is optional and can be any string of printable
        characters.

        An example of this is the Python list. clojure-py has extended the
        IPrintable protocol to include these objectS.

        user=> sys/path
        #<__builtin__.list object at 0xdeadbeef>
        user=>"""
        raise AbstractMethodCall(self)
