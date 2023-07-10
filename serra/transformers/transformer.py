from abc import ABC, abstractmethod

class Transformer(ABC):
    """
    Transformer base class for data transformations
    Enforce transform method
    """
    @abstractmethod
    def transform(self, df):
        pass