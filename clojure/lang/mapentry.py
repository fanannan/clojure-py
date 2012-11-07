from clojure.lang.amapentry import AMapEntry


class MapEntry(AMapEntry):
    """Concrete implementation of an AMapEntry.

    Contains the attributes _key and _value with associated getKey() and
    getValue() methods."""
    def __init__(self, key, value):
        """Instantiate a MapEntry.

        key -- any object       # ???: any hashable object
        value -- any object"""
        self._key = key
        self._value = value

    def getKey(self):
        """Return the key of this MapEntry."""
        return self._key

    def getValue(self):
        """Return the value of this MapEntry."""
        return self._value
