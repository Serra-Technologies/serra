from abc import ABC, abstractmethod

class Writer(ABC):
    """
    Writer base class for loading data
    Enforce write method
    """
    @abstractmethod
    def write(self, df):
        pass