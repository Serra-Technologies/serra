from abc import ABC, abstractmethod

class Reader(ABC):
    """
    Reader base class for ingesting data
    Enforce read method
    """
    @abstractmethod
    def read(self, fmt, path, predicate=None):
        pass

    @property
    def dependencies(self):
        return []